//! Bounded source jobs finish before downstream storage is awaited.

use super::{Active, Control};
use gix::features::zlib::{Decompress, FlushDecompress, Status};
use std::future::Future;
use std::io::{self, Read};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Semaphore;

pub(super) const MIN_BYTES: u64 = 1024 * 1024;
const BUFFER_BYTES: usize = 64 * 1024;
const HEADER_BYTES: usize = 64;
pub(super) struct Locator {
    roots: Vec<PathBuf>,
}
impl Locator {
    pub(super) fn open(store: &gix::odb::Store) -> Self {
        let roots: Vec<_> = std::iter::once(store.path().to_owned())
            .chain(store.alternate_db_paths().unwrap_or_default())
            .collect();
        Self { roots }
    }

    pub(super) fn reader(
        &self,
        oid: &gix::ObjectId,
        size: u64,
        slots: Arc<Semaphore>,
        control: Arc<Control>,
    ) -> io::Result<Option<SourceReader>> {
        let hex = oid.to_string();
        for root in &self.roots {
            let path = root.join(&hex[..2]).join(&hex[2..]);
            if !path.metadata().is_ok_and(|metadata| metadata.is_file()) {
                continue;
            }
            let file = match std::fs::File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let metadata = file.metadata()?;
            if !metadata.is_file() {
                continue;
            }
            return Ok(Some(SourceReader::new(
                file.take(metadata.len()),
                size,
                true,
                slots,
                control,
                oid,
            )));
        }
        Ok(None)
    }
}

struct Inflater {
    source: io::Take<std::fs::File>,
    inflate: Decompress,
    input: Box<[u8]>,
    start: usize,
    end: usize,
    finished: bool,
}
impl Inflater {
    // Consume at most one input-buffer's worth of compressed data per job.
    // A no-output step is progress, not EOF (e.g. many empty deflate blocks).
    fn step(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.finished {
            return Ok(0);
        }
        let initial = self.inflate.total_in();
        loop {
            if self.start == self.end {
                self.end = self.source.read(&mut self.input)?;
                self.start = 0;
                // At compressed EOF the inflater may still hold pending
                // output. Give it empty input before deciding it is truncated.
            }
            let before_in = self.inflate.total_in();
            let before_out = self.inflate.total_out();
            let remaining = BUFFER_BYTES as u64 - (before_in - initial);
            let end = self.end.min(self.start + remaining as usize);
            let status = self
                .inflate
                .decompress(&self.input[self.start..end], output, FlushDecompress::None)
                .map_err(io::Error::other)?;
            let consumed = (self.inflate.total_in() - before_in) as usize;
            let produced = (self.inflate.total_out() - before_out) as usize;
            self.start += consumed;
            self.finished = status == Status::StreamEnd;
            if self.finished || produced != 0 {
                return Ok(produced);
            }
            if consumed == 0 {
                return Err(io::Error::new(
                    if self.end == 0 {
                        io::ErrorKind::UnexpectedEof
                    } else {
                        io::ErrorKind::InvalidData
                    },
                    "truncated or stalled Git zlib stream",
                ));
            }
            if self.inflate.total_in() - initial == BUFFER_BYTES as u64 {
                return Ok(0);
            }
        }
    }
}

struct State {
    inflater: Inflater,
    output: Box<[u8]>,
    cursor: usize,
    filled: usize,
    header: Vec<u8>,
    header_done: bool,
    size: u64,
    seen: u64,
}
impl State {
    fn step(&mut self) -> io::Result<()> {
        self.cursor = 0;
        self.filled = self.inflater.step(&mut self.output)?;
        if !self.header_done {
            if let Some(end) = self.output[..self.filled]
                .iter()
                .position(|byte| *byte == 0)
            {
                if self.header.len() + end + 1 > HEADER_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Git loose header exceeds limit",
                    ));
                }
                self.header.extend_from_slice(&self.output[..=end]);
                let (kind, size, _) =
                    gix::objs::decode::loose_header(&self.header).map_err(io::Error::other)?;
                if kind != gix::objs::Kind::Blob || size != self.size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Git loose header changed kind or size",
                    ));
                }
                self.output.copy_within(end + 1..self.filled, 0);
                self.filled -= end + 1;
                self.header_done = true;
            } else {
                if self.header.len() + self.filled >= HEADER_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unterminated Git loose header",
                    ));
                }
                self.header.extend_from_slice(&self.output[..self.filled]);
                self.filled = 0;
            }
        }
        self.seen = self
            .seen
            .checked_add(self.filled as u64)
            .ok_or_else(|| io::Error::other("Git source length overflow"))?;
        if self.seen > self.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Git source exceeds declared size",
            ));
        }
        if self.inflater.finished && (!self.header_done || self.seen != self.size) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Git source ended before declared size",
            ));
        }
        Ok(())
    }
}

type Job = Pin<Box<dyn Future<Output = io::Result<Box<State>>> + Send>>;

pub(super) struct SourceReader {
    state: Option<Box<State>>,
    job: Option<Job>,
    slots: Arc<Semaphore>,
    control: Arc<Control>,
    cancelled: Arc<AtomicBool>,
    failed: bool,
    #[cfg(test)]
    gate: Option<Arc<tests::Gate>>,
}
impl SourceReader {
    fn new(
        source: io::Take<std::fs::File>,
        size: u64,
        loose: bool,
        slots: Arc<Semaphore>,
        control: Arc<Control>,
        _oid: &gix::ObjectId,
    ) -> Self {
        Self {
            state: Some(Box::new(State {
                inflater: Inflater {
                    source,
                    inflate: Decompress::new(),
                    input: vec![0; BUFFER_BYTES].into_boxed_slice(),
                    start: 0,
                    end: 0,
                    finished: false,
                },
                output: vec![0; BUFFER_BYTES].into_boxed_slice(),
                cursor: 0,
                filled: 0,
                header: Vec::with_capacity(HEADER_BYTES),
                header_done: !loose,
                size,
                seen: 0,
            })),
            job: None,
            slots,
            control,
            cancelled: Arc::new(AtomicBool::new(false)),
            failed: false,
            #[cfg(test)]
            gate: tests::gate(_oid),
        }
    }
}
impl Drop for SourceReader {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}
impl AsyncRead for SourceReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.failed {
            return Poll::Ready(Err(io::Error::other("Git source previously failed")));
        }
        loop {
            if let Some(job) = this.job.as_mut() {
                match job.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => {
                        this.job = None;
                        match result {
                            Ok(state) => this.state = Some(state),
                            Err(error) => {
                                this.failed = true;
                                return Poll::Ready(Err(error));
                            }
                        }
                    }
                }
                if this
                    .state
                    .as_ref()
                    .is_some_and(|state| state.filled == 0 && !state.inflater.finished)
                {
                    // Yield after a compressed-input-only step, rather than
                    // spinning through arbitrary empty blocks in one poll.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
            let state = this.state.as_mut().expect("source owns a state or a job");
            if state.cursor < state.filled {
                let size = output.remaining().min(state.filled - state.cursor);
                output.put_slice(&state.output[state.cursor..state.cursor + size]);
                state.cursor += size;
                return Poll::Ready(Ok(()));
            }
            if state.inflater.finished {
                return Poll::Ready(Ok(()));
            }
            let mut state = this.state.take().expect("state present");
            let slots = this.slots.clone();
            let control = this.control.clone();
            let cancelled = this.cancelled.clone();
            #[cfg(test)]
            let gate = this.gate.take();
            this.job = Some(Box::pin(async move {
                let permit = slots.acquire_owned().await.map_err(io::Error::other)?;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let _active = Active::new(&control);
                    if cancelled.load(Ordering::Relaxed)
                        || control.cancelled.load(Ordering::Relaxed)
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "Git inflation cancelled",
                        ));
                    }
                    #[cfg(test)]
                    if let Some(gate) = gate {
                        gate.park();
                    }
                    state.step()?;
                    Ok(state)
                })
                .await
                .map_err(io::Error::other)?
            }));
        }
    }
}

#[cfg(test)]
mod tests;

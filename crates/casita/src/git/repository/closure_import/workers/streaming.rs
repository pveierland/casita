//! Bounded source jobs finish before downstream storage is awaited.

use super::{Active, Control};
use crate::spill::SpillPayload;

pub(super) mod delta;
use gix::features::zlib::{Decompress, FlushDecompress, Status};
use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Semaphore;

pub(super) const MIN_BYTES: u64 = 1024 * 1024;
const BUFFER_BYTES: usize = 64 * 1024;
const HEADER_BYTES: usize = 64;
pub(super) const READER_BUFFER_BYTES: usize = 2 * BUFFER_BYTES + HEADER_BYTES;
pub(super) const SCRATCH_BUFFER_BYTES: usize = READER_BUFFER_BYTES + BUFFER_BYTES;
// Optional hints cannot add repository-sized work to a single blob import.
// Larger indexes and packs beyond this snapshot use the normal gix path.
const MAX_INDEX_ENTRIES: usize = 256;
const MAX_INDEX_FILES: usize = 32;
const MAX_INDEX_BYTES: u64 = 16 * 1024 * 1024;

pub(super) struct Locator {
    roots: Vec<PathBuf>,
    indexes: OnceLock<Vec<gix::odb::pack::index::File<Box<[u8]>>>>,
    hash: gix::hash::Kind,
}
impl Locator {
    pub(super) fn open(store: &gix::odb::Store) -> Self {
        let roots: Vec<_> = std::iter::once(store.path().to_owned())
            .chain(store.alternate_db_paths().unwrap_or_default())
            .collect();
        Self {
            roots,
            indexes: OnceLock::new(),
            hash: store.object_hash(),
        }
    }

    fn indexes(&self) -> &[gix::odb::pack::index::File<Box<[u8]>>] {
        self.indexes.get_or_init(|| {
            let mut indexes = Vec::new();
            let mut examined = 0;
            let mut bytes = 0;
            for root in &self.roots {
                let Ok(entries) = std::fs::read_dir(root.join("pack")) else {
                    continue;
                };
                for entry in entries {
                    if examined == MAX_INDEX_ENTRIES || indexes.len() == MAX_INDEX_FILES {
                        return indexes;
                    }
                    examined += 1;
                    let Ok(entry) = entry else { continue };
                    let path = entry.path();
                    if path.extension().is_none_or(|ext| ext != "idx") {
                        continue;
                    }
                    let Ok(metadata) = path.metadata() else {
                        continue;
                    };
                    if !metadata.is_file() || metadata.len() > MAX_INDEX_BYTES - bytes {
                        continue;
                    }
                    // Bounded owned snapshots avoid mmap growth/truncation
                    // races and charge invalid indexes against the same limit.
                    bytes += metadata.len();
                    let Ok(mut file) = std::fs::File::open(&path) else {
                        continue;
                    };
                    if !file.metadata().is_ok_and(|m| m.is_file()) {
                        continue;
                    }
                    let mut data = vec![0; metadata.len() as usize].into_boxed_slice();
                    if file.read_exact(&mut data).is_err() {
                        continue;
                    }
                    if let Ok(index) = gix::odb::pack::index::File::from_data(data, path, self.hash)
                    {
                        indexes.push(index);
                    }
                }
            }
            indexes
        })
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
        for index in self.indexes() {
            let Some(position) = index.lookup(oid) else {
                continue;
            };
            let offset = index.pack_offset_at_index(position);
            // Indexes are optional snapshots. Every error before selecting a
            // validated stream is a hint miss: gix can refresh or use a copy.
            if let Ok(Some(reader)) =
                Self::packed_reader(index, offset, oid, size, slots.clone(), control.clone())
            {
                return Ok(Some(reader));
            }
        }
        Ok(None)
    }
    fn packed_reader(
        index: &gix::odb::pack::index::File<Box<[u8]>>,
        offset: u64,
        oid: &gix::ObjectId,
        size: u64,
        slots: Arc<Semaphore>,
        control: Arc<Control>,
    ) -> io::Result<Option<SourceReader>> {
        let path = index.path().with_extension("pack");
        if !path.metadata()?.is_file() {
            return Ok(None);
        }
        let mut file = std::fs::File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Ok(None);
        }
        let hash_len = oid.as_bytes().len();
        let Some(pack_end) = metadata.len().checked_sub(hash_len as u64) else {
            return Ok(None);
        };
        if offset < 12 || offset >= pack_end {
            return Ok(None);
        }
        let mut header = [0; 12];
        file.read_exact(&mut header)?;
        gix::odb::pack::data::header::decode(&header).map_err(io::Error::other)?;
        file.seek(SeekFrom::Start(offset))?;
        let entry = gix::odb::pack::data::Entry::from_read(
            &mut (&mut file).take((pack_end - offset).min(HEADER_BYTES as u64)),
            offset,
            hash_len,
        )?;
        if entry.header != gix::odb::pack::data::entry::Header::Blob
            || entry.decompressed_size != size
            || entry.data_offset >= pack_end
        {
            return Ok(None);
        }
        Ok(Some(SourceReader::new(
            file.take(pack_end - entry.data_offset),
            size,
            false,
            slots,
            control,
            oid,
        )))
    }
}

enum Input {
    Compressed(io::Take<std::fs::File>),
    Spilled(SpillPayload),
}
impl Read for Input {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Compressed(file) => file.read(bytes),
            Self::Spilled(file) => file.read(bytes),
        }
    }
}

struct Inflater {
    source: Input,
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
        if let Input::Spilled(file) = &mut self.source {
            let read = file.read(output)?;
            self.finished = read == 0;
            return Ok(read);
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
    // Last: errors, unwind and unreceived outputs close spill files and release
    // their quota before making the source slot available to a draining window.
    _buffers: Option<Arc<crate::import_buffer::Reservation>>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl State {
    fn new(source: Input, size: u64, loose: bool) -> Self {
        let compressed = matches!(&source, Input::Compressed(_));
        Self {
            inflater: Inflater {
                source,
                inflate: Decompress::new(),
                input: vec![0; if compressed { BUFFER_BYTES } else { 0 }].into_boxed_slice(),
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
            _buffers: None,
            permit: None,
        }
    }
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
        Self::from_input(Input::Compressed(source), size, loose, slots, control, _oid)
    }

    pub(super) fn from_spill(
        source: SpillPayload,
        size: u64,
        slots: Arc<Semaphore>,
        control: Arc<Control>,
        oid: &gix::ObjectId,
    ) -> Self {
        Self::from_input(Input::Spilled(source), size, false, slots, control, oid)
    }

    fn from_input(
        source: Input,
        size: u64,
        loose: bool,
        slots: Arc<Semaphore>,
        control: Arc<Control>,
        _oid: &gix::ObjectId,
    ) -> Self {
        let mut state = State::new(source, size, loose);
        state._buffers = control.buffer_guard();
        Self {
            state: Some(Box::new(state)),
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
                state.permit = Some(slots.acquire_owned().await.map_err(io::Error::other)?);
                let cpu = control.cpu.clone();
                crate::import_cpu::run(cpu.as_ref(), move || {
                    let active = Active::new(&control);
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
                    drop(active);
                    if matches!(&state.inflater.source, Input::Compressed(_)) {
                        // Release the private source slot after this step.
                        // State still owns the independent shared buffer reservation
                        // through result receipt or disposal.
                        drop(state.permit.take());
                    }
                    Ok(state)
                })
                .await
                .map_err(io::Error::other)?
                .map(|mut state| {
                    // A spilled result holds its slot through receipt or disposal.
                    // Release before returning to the reader's next acquisition.
                    drop(state.permit.take());
                    state
                })
            }));
        }
    }
}

#[cfg(test)]
mod tests;

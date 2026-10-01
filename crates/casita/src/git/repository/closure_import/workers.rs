//! Bounded source workers deliver objects and streams to verified staging.

use super::{Result, source_error};
use crate::ObjectKey;
use crate::blob::BlobStore;
use crate::format::FormatLimits;
use crate::git::{GitObjectFormat, GitObjectKind, git_key_parts};
use crate::importers::GitClosureImport;
use crate::metadata::MetadataStore;
use crate::repository::{
    MutationSession, NativeSeal, NativeVerifier, RepositoryError, StagedObject,
};
use futures::{StreamExt, TryStreamExt};
use gix::objs::FindExt;
use gix::odb::HeaderExt;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

mod streaming;
use crate::spill::SpillArea;
use streaming::delta::Plan as DeltaPlan;

const PACK_CACHE_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct SourcePool {
    sources: Vec<gix::odb::Handle>,
    decoded_bytes: u64,
    spill_area: SpillArea,
    delta_spilling: bool,
    payload_limit: u64,
    source_files: Arc<tokio::sync::Semaphore>,
    locator: Arc<OnceLock<streaming::Locator>>,
    slots: Arc<tokio::sync::Semaphore>,
}

struct Pending {
    key: ObjectKey,
    oid: gix::ObjectId,
    kind: GitObjectKind,
    size: u64,
    delta: Option<DeltaPlan>,
}

struct Decoded {
    key: ObjectKey,
    body: Body,
    seal: Option<NativeSeal>,
}

enum Body {
    Buffered(Box<[u8]>),
    Stream {
        reader: streaming::SourceReader,
        size: u64,
    },
}
impl Body {
    fn size(&self) -> u64 {
        match self {
            Self::Buffered(bytes) => bytes.len() as u64,
            Self::Stream { size, .. } => *size,
        }
    }
}

fn stream_blob(
    source: &gix::odb::Handle,
    locator: &OnceLock<streaming::Locator>,
    slots: &Arc<tokio::sync::Semaphore>,
    control: &Arc<Control>,
    kind: GitObjectKind,
    oid: &gix::ObjectId,
    size: u64,
) -> Result<Option<Body>> {
    if kind != GitObjectKind::Blob || size < streaming::MIN_BYTES {
        return Ok(None);
    }
    let locator = locator.get_or_init(|| streaming::Locator::open(source.store_ref()));
    Ok(locator
        .reader(oid, size, slots.clone(), control.clone())
        .map_err(source_error)?
        .map(|reader| Body::Stream { reader, size }))
}

struct Window {
    pub source: SourcePool,
    pub pending: VecDeque<ObjectKey>,
    pub decoded: Vec<Decoded>,
    pub bytes: u64,
    pub peak_workers: usize,
    groups: Vec<Vec<Pending>>,
}

pub(super) struct StagedWindow<'hold> {
    pub source: SourcePool,
    pub pending: VecDeque<ObjectKey>,
    pub objects: Vec<StagedObject<'hold>>,
    pub bytes: u64,
    pub peak_workers: usize,
    pub spilled_delta_objects: usize,
}

#[derive(Default)]
struct Control {
    cpu: Option<crate::import_cpu::ImportCpuBudget>,
    cancelled: AtomicBool,
    active: AtomicUsize,
    peak: AtomicUsize,
    spilled_delta_objects: AtomicUsize,
}
impl Control {
    fn with_cpu_budget(mut self, cpu: Option<crate::import_cpu::ImportCpuBudget>) -> Self {
        self.cpu = cpu;
        self
    }
}
struct Cancellation(Arc<Control>);
impl Drop for Cancellation {
    fn drop(&mut self) {
        self.0.cancelled.store(true, Ordering::Relaxed);
    }
}
struct Active<'a>(&'a Control);
impl<'a> Active<'a> {
    fn new(control: &'a Control) -> Self {
        let active = control.active.fetch_add(1, Ordering::Relaxed) + 1;
        control.peak.fetch_max(active, Ordering::Relaxed);
        Self(control)
    }
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}

impl SourcePool {
    fn open(
        path: PathBuf,
        format: GitObjectFormat,
        limit: u64,
        workers: usize,
        spill_area: SpillArea,
        delta_spilling: bool,
    ) -> Result<Self> {
        let options = gix::odb::store::init::Options {
            object_hash: match format {
                GitObjectFormat::Sha1 => gix::hash::Kind::Sha1,
                GitObjectFormat::Sha256 => gix::hash::Kind::Sha256,
            },
            alloc_limit_bytes: usize::try_from(limit).ok(),
            ..Default::default()
        };
        let handle = gix::odb::at_opts(path, [], options).map_err(source_error)?;
        let cache_bytes = PACK_CACHE_BYTES / workers;
        let sources = (0..workers)
            .map(|_| {
                handle.clone().with_pack_cache(move || {
                    Box::new(gix::odb::pack::cache::lru::MemoryCappedHashmap::new(
                        cache_bytes,
                    ))
                })
            })
            .collect();
        Ok(Self {
            sources,
            decoded_bytes: 0,
            spill_area,
            delta_spilling,
            payload_limit: limit,
            source_files: Arc::new(tokio::sync::Semaphore::new(
                streaming::delta::MAX_SOURCE_FILES,
            )),
            locator: Arc::new(OnceLock::new()),
            slots: Arc::new(tokio::sync::Semaphore::new(workers)),
        })
    }

    fn probe(
        &self,
        kind: GitObjectKind,
        oid: &gix::ObjectId,
        control: &Arc<Control>,
    ) -> std::io::Result<(u64, Option<DeltaPlan>)> {
        if self.delta_spilling && kind == GitObjectKind::Blob {
            let locator = self
                .locator
                .get_or_init(|| streaming::Locator::open(self.sources[0].store_ref()));
            if let Some(plan) = DeltaPlan::probe(
                locator,
                oid,
                self.payload_limit,
                control.clone(),
                self.source_files.clone(),
            )? {
                return Ok((plan.size(), Some(plan)));
            }
        }
        Ok((
            self.sources[0]
                .header(oid)
                .map_err(std::io::Error::other)?
                .size(),
            None,
        ))
    }

    fn decode_serial(
        &mut self,
        pending: &mut VecDeque<ObjectKey>,
        count: usize,
        budget: u64,
        payload_limit: u64,
        metadata_limit: u64,
        control: &Arc<Control>,
    ) -> Result<Vec<Decoded>> {
        let mut decoded = Vec::new();
        let mut bytes = 0u64;
        while decoded.len() < count {
            let Some(key) = pending.front() else { break };
            #[cfg(test)]
            tests::pause_before_decode(key);
            let (_, kind, oid) = git_key_parts(key)?;
            let oid = gix::ObjectId::from_bytes_or_panic(oid);
            let (size, delta) = self.probe(kind, &oid, control).map_err(delta_error)?;
            let limit = if kind == GitObjectKind::Blob {
                payload_limit
            } else {
                payload_limit.min(metadata_limit)
            };
            if size > limit {
                return Err(RepositoryError::LimitExceeded(format!(
                    "Git object {key} has {size} bytes, limit is {limit}"
                ))
                .into());
            }
            if !decoded.is_empty() && (bytes > budget || size > budget.saturating_sub(bytes)) {
                break;
            }
            if let Some(delta) = delta {
                let body = spill_delta(delta, &oid, &self.spill_area, &self.slots, control)?;
                bytes = bytes
                    .checked_add(size)
                    .ok_or_else(|| source_error("decoded byte count overflow"))?;
                decoded.push(Decoded {
                    key: pending.pop_front().expect("front exists"),
                    body,
                    seal: None,
                });
                continue;
            }
            if let Some(body) = stream_blob(
                &self.sources[0],
                &self.locator,
                &self.slots,
                control,
                kind,
                &oid,
                size,
            )? {
                bytes = bytes
                    .checked_add(size)
                    .ok_or_else(|| source_error("decoded byte count overflow"))?;
                decoded.push(Decoded {
                    key: pending.pop_front().expect("front exists"),
                    body,
                    seal: None,
                });
                continue;
            }
            let mut body = Vec::new();
            let object = self.sources[0]
                .find(&oid, &mut body)
                .map_err(source_error)?;
            let actual_kind = match object.kind {
                gix::objs::Kind::Blob => GitObjectKind::Blob,
                gix::objs::Kind::Tree => GitObjectKind::Tree,
                gix::objs::Kind::Commit => GitObjectKind::Commit,
                gix::objs::Kind::Tag => GitObjectKind::Tag,
            };
            if actual_kind != kind || body.len() as u64 != size {
                return Err(source_error(format!(
                    "Git object {key} disagrees with its expected kind or size"
                )));
            }
            bytes = bytes
                .checked_add(size)
                .ok_or_else(|| source_error("decoded byte count overflow"))?;
            decoded.push(Decoded {
                key: pending.pop_front().expect("front exists"),
                body: Body::Buffered(body.into_boxed_slice()),
                seal: None,
            });
        }
        self.decoded_bytes = self.decoded_bytes.saturating_add(bytes);
        Ok(decoded)
    }

    fn plan(
        &self,
        missing: &mut VecDeque<ObjectKey>,
        count: usize,
        budget: u64,
        limits: &FormatLimits,
        control: &Arc<Control>,
    ) -> Result<Vec<Pending>> {
        let mut plan = Vec::new();
        let mut bytes = 0u64;
        while plan.len() < count {
            let Some(key) = missing.front() else { break };
            let (_, kind, oid) = git_key_parts(key)?;
            let oid = gix::ObjectId::from_bytes_or_panic(oid);
            let (size, delta) = match self.probe(kind, &oid, control) {
                Ok(probed) => probed,
                // Planned readers own their handles until decoding. Drain this
                // window instead of waiting while its descriptors remain held.
                Err(error) if streaming::delta::handle_window_full(&error) && !plan.is_empty() => {
                    break;
                }
                Err(error) => return Err(delta_error(error)),
            };
            let limit = if kind == GitObjectKind::Blob {
                limits.max_payload_bytes
            } else {
                limits.max_payload_bytes.min(limits.max_metadata_bytes)
            };
            if size > limit {
                return Err(RepositoryError::LimitExceeded(format!(
                    "Git object {key} has {size} bytes, limit is {limit}"
                ))
                .into());
            }
            if !plan.is_empty() && (bytes > budget || size > budget.saturating_sub(bytes)) {
                break;
            }
            bytes = bytes
                .checked_add(size)
                .ok_or_else(|| source_error("decoded byte count overflow"))?;
            plan.push(Pending {
                key: missing.pop_front().expect("front exists"),
                oid,
                kind,
                size,
                delta,
            });
        }
        Ok(plan)
    }
}

fn delta_error(error: std::io::Error) -> super::GitClosureImportError {
    if let Some(crate::error::Error::LimitExceeded(message)) = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<crate::error::Error>())
    {
        return RepositoryError::LimitExceeded(message.clone()).into();
    }
    source_error(error)
}

fn spill_delta(
    plan: DeltaPlan,
    oid: &gix::ObjectId,
    area: &SpillArea,
    slots: &Arc<tokio::sync::Semaphore>,
    control: &Arc<Control>,
) -> Result<Body> {
    let size = plan.size();
    let spool = plan.reconstruct(area).map_err(delta_error)?;
    control
        .spilled_delta_objects
        .fetch_add(1, Ordering::Relaxed);
    Ok(Body::Stream {
        reader: streaming::SourceReader::from_spill(
            spool,
            size,
            slots.clone(),
            control.clone(),
            oid,
        ),
        size,
    })
}

// Borrow the window-owned resources independently; the worker retains no session.
struct DecodeResources<'a> {
    control: &'a Arc<Control>,
    locator: &'a OnceLock<streaming::Locator>,
    slots: &'a Arc<tokio::sync::Semaphore>,
    area: &'a SpillArea,
}

fn decode(
    source: &mut gix::odb::Handle,
    batch: Vec<Pending>,
    verifier: Option<&NativeVerifier>,
    resources: DecodeResources<'_>,
    mut emit: impl FnMut(Decoded) -> Result<()>,
) -> Result<()> {
    let DecodeResources {
        control,
        locator,
        slots,
        area,
    } = resources;
    let _active = Active::new(control);
    for pending in batch {
        if control.cancelled.load(Ordering::Relaxed) {
            return Err(source_error("Git decoding cancelled"));
        }
        #[cfg(test)]
        tests::pause_before_decode(&pending.key);
        if let Some(delta) = pending.delta {
            let body = spill_delta(delta, &pending.oid, area, slots, control)?;
            emit(Decoded {
                key: pending.key,
                body,
                seal: None,
            })?;
            continue;
        }

        if let Some(body) = stream_blob(
            source,
            locator,
            slots,
            control,
            pending.kind,
            &pending.oid,
            pending.size,
        )? {
            emit(Decoded {
                key: pending.key,
                body,
                seal: None,
            })?;
            continue;
        }
        let mut body = Vec::new();
        let object = source.find(&pending.oid, &mut body).map_err(source_error)?;
        let kind = match object.kind {
            gix::objs::Kind::Blob => GitObjectKind::Blob,
            gix::objs::Kind::Tree => GitObjectKind::Tree,
            gix::objs::Kind::Commit => GitObjectKind::Commit,
            gix::objs::Kind::Tag => GitObjectKind::Tag,
        };
        if kind != pending.kind || body.len() as u64 != pending.size {
            return Err(source_error(format!(
                "Git object {} disagrees with its expected kind or size",
                pending.key
            )));
        }
        let seal = verifier
            .map(|verifier| verifier.verify(&pending.key, &body))
            .transpose()?;
        emit(Decoded {
            key: pending.key,
            body: Body::Buffered(body.into_boxed_slice()),
            seal,
        })?;
    }
    Ok(())
}

async fn stage<'hold, PS: BlobStore, SS: MetadataStore>(
    session: &'hold MutationSession<'_, PS, SS>,
    object: Decoded,
) -> Result<StagedObject<'hold>> {
    Ok(match object.body {
        Body::Stream { mut reader, size } => {
            session
                .stage_object_reader_with_size(object.key, size, &mut reader)
                .await?
        }
        Body::Buffered(body) => match object.seal {
            Some(seal) => session.stage_native_seal(seal, &body).await?,
            None => session.stage_object(object.key, &body).await?,
        },
    })
}

pub(super) async fn stage_window<'hold, PS: BlobStore, SS: MetadataStore>(
    session: &'hold MutationSession<'_, PS, SS>,
    source: Option<SourcePool>,
    mut pending: VecDeque<ObjectKey>,
    request: &GitClosureImport,
    format: GitObjectFormat,
    limits: FormatLimits,
    spill_area: SpillArea,
) -> Result<StagedWindow<'hold>> {
    let verifier = session.native_verifier();
    let count = request
        .concurrency
        .get()
        .min(limits.max_batch_objects)
        .min(super::FRONTIER);
    let workers = request.decode_workers.get().min(count);
    let path = request.objects_dir.clone();
    let budget = request.max_buffered_bytes.get();
    let serial = request.decode_workers.get() == 1;
    let delta_spilling = request.delta_spilling;
    let control = Arc::new(Control::default().with_cpu_budget(request.cpu.clone()));
    // Dropping the import cancels parallel decoding between objects and
    // streamed inflation between bounded steps. An in-flight gix decode or
    // admitted buffered serial window may finish. Running jobs retain their
    // buffers and permits; source workers never wait for destination operations.
    let _cancellation = Cancellation(control.clone());
    let first_control = control.clone();
    let first_verifier = verifier.clone();
    let mut window = crate::import_cpu::run(request.cpu.as_ref(), move || {
        let mut source = match source {
            Some(source)
                if source.decoded_bytes < super::super::MAX_GIT_SOURCE_MAPPING_WINDOW_BYTES =>
            {
                source
            }
            _ => SourcePool::open(
                path,
                format,
                limits.max_payload_bytes,
                workers,
                spill_area,
                delta_spilling,
            )?,
        };
        if serial {
            // Keep the existing one-worker path: interleave headers and
            // decoding, then verify on the staging runtime. Planning all
            // headers first and moving verification regressed serial controls.
            let decoded = source.decode_serial(
                &mut pending,
                count,
                budget,
                limits.max_payload_bytes,
                limits.max_metadata_bytes,
                &first_control,
            )?;
            let bytes = decoded.iter().map(|object| object.body.size()).sum();
            return Ok(Window {
                source,
                pending,
                decoded,
                bytes,
                peak_workers: 1,
                groups: Vec::new(),
            });
        }
        let plan = source.plan(&mut pending, count, budget, &limits, &first_control)?;
        let bytes = plan.iter().map(|object| object.size).sum();
        source.decoded_bytes = source.decoded_bytes.saturating_add(bytes);
        let mut decoded = Vec::new();
        let mut groups = Vec::new();
        if workers == 1 || plan.len() == 1 {
            // Preserve one blocking-pool handoff for single-object frontiers.
            decode(
                &mut source.sources[0],
                plan,
                first_verifier.as_ref(),
                DecodeResources {
                    control: &first_control,
                    locator: &source.locator,
                    slots: &source.slots,
                    area: &source.spill_area,
                },
                |object| {
                    decoded.push(object);
                    Ok(())
                },
            )?;
        } else {
            groups = (0..workers).map(|_| Vec::new()).collect();
            let width = plan.len().div_ceil(workers).max(1);
            for (index, object) in plan.into_iter().enumerate() {
                groups[index / width].push(object);
            }
        }
        Ok::<_, super::GitClosureImportError>(Window {
            source,
            pending,
            decoded,
            bytes,
            peak_workers: 0,
            groups,
        })
    })
    .await
    .map_err(source_error)??;
    let result = if window.groups.is_empty() {
        futures::stream::iter(std::mem::take(&mut window.decoded))
            .map(|object| stage(session, object))
            .buffer_unordered(count)
            .try_collect::<Vec<_>>()
            .await
    } else {
        let (sender, receiver) = decoded_channel(workers, request.cpu.is_some());
        let sources = std::mem::take(&mut window.source.sources);
        // Spawn eagerly: waiting for the receiver before spawning these jobs
        // would leave it waiting for senders that have never started.
        let jobs: Vec<_> = sources
            .into_iter()
            .zip(std::mem::take(&mut window.groups))
            .map(|(mut source, batch)| {
                if batch.is_empty() {
                    return (Some(source), None);
                }
                let verifier = verifier.clone();
                let control = control.clone();
                let sender = sender.clone();
                let locator = window.source.locator.clone();
                let slots = window.source.slots.clone();
                let area = window.source.spill_area.clone();
                // No staging reader is polled until all jobs are spawned, so
                // one slot per nonempty group is available here. CPU jobs own
                // the slot through cancellation and release it before readers
                // submit bounded inflater steps.
                let permit = slots
                    .clone()
                    .try_acquire_owned()
                    .expect("previous source window drained");
                (
                    None,
                    Some(crate::import_cpu::spawn(request.cpu.clone(), move || {
                        let _permit = permit;
                        let result = decode(
                            &mut source,
                            batch,
                            verifier.as_ref(),
                            DecodeResources {
                                control: &control,
                                locator: &locator,
                                slots: &slots,
                                area: &area,
                            },
                            |object| {
                                sender
                                    .blocking_send(Ok(object))
                                    .map_err(|_| source_error("Git staging receiver closed"))
                            },
                        );
                        if let Err(error) = result {
                            // A receiver closed by a staging failure already
                            // holds the error to return; do not replace it.
                            let _ = sender.blocking_send(Err(error));
                        }
                        source
                    })),
                )
            })
            .collect();
        drop(sender);
        // Staging capacity covers the whole admitted count. It can receive all
        // bodies without waiting for storage, including when storage and source
        // decoding share a runtime with only one blocking thread.
        let result = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        })
        .map_ok(|object| stage(session, object))
        .try_buffer_unordered(count)
        .try_collect::<Vec<_>>()
        .await;
        // The consumed stream (and receiver) is dropped before joining jobs.
        // This releases blocked senders on failure. No next window is admitted
        // until every source job and successful destination writer has drained.
        if result.is_err() {
            control.cancelled.store(true, Ordering::Relaxed);
            // Do not await admission held by an unrelated import after failure.
            // Submitted blocking jobs still drain through their private slots.
            for (_, job) in &jobs {
                if let Some(job) = job {
                    job.cancel_waiter();
                }
            }
        }
        let mut join_error = None;
        for result in futures::future::join_all(jobs.into_iter().map(|(ready, job)| async move {
            match job {
                Some(job) => job.join().await,
                None => Ok(ready.expect("idle source")),
            }
        }))
        .await
        {
            match result {
                Ok(source) => window.source.sources.push(source),
                Err(error) => {
                    join_error.get_or_insert_with(|| source_error(error));
                }
            }
        }
        match (result, join_error) {
            (Err(error), _) | (_, Some(error)) => Err(error),
            (Ok(objects), None) => Ok(objects),
        }
    };
    if result.is_err() {
        control.cancelled.store(true, Ordering::Relaxed);
        // Staging readers have been dropped and decode groups joined. Waiting
        // for every slot also drains inflater jobs whose reader was dropped
        // after submission. Keep the original error, including on job panic.
        let _drained = window
            .source
            .slots
            .clone()
            .acquire_many_owned(workers as u32)
            .await
            .expect("private source semaphore remains open");
    }
    let objects = result?;
    Ok(StagedWindow {
        source: window.source,
        pending: window.pending,
        objects,
        spilled_delta_objects: control.spilled_delta_objects.load(Ordering::Relaxed),
        bytes: window.bytes,
        peak_workers: window
            .peak_workers
            .max(control.peak.load(Ordering::Relaxed)),
    })
}

// With global CPU admission, sending must never wait while holding a CPU
// permit. The unbounded channel is confined to one already-admitted window
// (at most FRONTIER objects); no new window starts before it drains.
#[derive(Clone)]
enum DecodedSender {
    Bounded(tokio::sync::mpsc::Sender<Result<Decoded>>),
    Window(tokio::sync::mpsc::UnboundedSender<Result<Decoded>>),
}
enum DecodedReceiver {
    Bounded(tokio::sync::mpsc::Receiver<Result<Decoded>>),
    Window(tokio::sync::mpsc::UnboundedReceiver<Result<Decoded>>),
}
impl DecodedSender {
    fn blocking_send(&self, value: Result<Decoded>) -> std::result::Result<(), ()> {
        match self {
            Self::Bounded(sender) => sender.blocking_send(value).map_err(|_| ()),
            Self::Window(sender) => sender.send(value).map_err(|_| ()),
        }
    }
}
impl DecodedReceiver {
    async fn recv(&mut self) -> Option<Result<Decoded>> {
        match self {
            Self::Bounded(receiver) => receiver.recv().await,
            Self::Window(receiver) => receiver.recv().await,
        }
    }
}
fn decoded_channel(workers: usize, shared: bool) -> (DecodedSender, DecodedReceiver) {
    if shared {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (
            DecodedSender::Window(sender),
            DecodedReceiver::Window(receiver),
        )
    } else {
        let (sender, receiver) = tokio::sync::mpsc::channel(workers);
        (
            DecodedSender::Bounded(sender),
            DecodedReceiver::Bounded(receiver),
        )
    }
}

#[cfg(test)]
mod tests;

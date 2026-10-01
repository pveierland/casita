//! CPU workers stream verified objects into staging within one admitted window.

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
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

const PACK_CACHE_BYTES: usize = 16 * 1024 * 1024;

pub(super) struct SourcePool {
    sources: Vec<gix::odb::Handle>,
    decoded_bytes: u64,
}

struct Pending {
    key: ObjectKey,
    oid: gix::ObjectId,
    kind: GitObjectKind,
    size: u64,
}

pub(super) struct Decoded {
    pub key: ObjectKey,
    pub body: Box<[u8]>,
    pub seal: Option<NativeSeal>,
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
}

#[derive(Default)]
struct Control {
    cancelled: AtomicBool,
    active: AtomicUsize,
    peak: AtomicUsize,
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
    fn open(path: PathBuf, format: GitObjectFormat, limit: u64, workers: usize) -> Result<Self> {
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
        })
    }

    fn decode_serial(
        &mut self,
        pending: &mut VecDeque<ObjectKey>,
        count: usize,
        budget: u64,
        payload_limit: u64,
        metadata_limit: u64,
    ) -> Result<Vec<Decoded>> {
        let mut decoded = Vec::new();
        let mut bytes = 0u64;
        while decoded.len() < count {
            let Some(key) = pending.front() else { break };
            let (_, kind, oid) = git_key_parts(key)?;
            let oid = gix::ObjectId::from_bytes_or_panic(oid);
            let header = self.sources[0].header(oid).map_err(source_error)?;
            let size = header.size();
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
                body: body.into_boxed_slice(),
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
    ) -> Result<Vec<Pending>> {
        let mut plan = Vec::new();
        let mut bytes = 0u64;
        while plan.len() < count {
            let Some(key) = missing.front() else { break };
            let (_, kind, oid) = git_key_parts(key)?;
            let oid = gix::ObjectId::from_bytes_or_panic(oid);
            let size = self.sources[0].header(oid).map_err(source_error)?.size();
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
            });
        }
        Ok(plan)
    }
}

fn decode(
    source: &mut gix::odb::Handle,
    batch: Vec<Pending>,
    verifier: Option<&NativeVerifier>,
    control: &Control,
    mut emit: impl FnMut(Decoded) -> Result<()>,
) -> Result<()> {
    let _active = Active::new(control);
    for pending in batch {
        if control.cancelled.load(Ordering::Relaxed) {
            return Err(source_error("Git decoding cancelled"));
        }
        #[cfg(test)]
        tests::pause_before_decode(&pending.key);
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
            body: body.into_boxed_slice(),
            seal,
        })?;
    }
    Ok(())
}

async fn stage<'hold, PS: BlobStore, SS: MetadataStore>(
    session: &'hold MutationSession<'_, PS, SS>,
    object: Decoded,
) -> Result<StagedObject<'hold>> {
    Ok(match object.seal {
        Some(seal) => session.stage_native_seal(seal, &object.body).await?,
        None => session.stage_object(object.key, &object.body).await?,
    })
}

pub(super) async fn stage_window<'hold, PS: BlobStore, SS: MetadataStore>(
    session: &'hold MutationSession<'_, PS, SS>,
    source: Option<SourcePool>,
    mut pending: VecDeque<ObjectKey>,
    request: &GitClosureImport,
    format: GitObjectFormat,
    limits: FormatLimits,
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
    let control = Arc::new(Control::default());
    // Dropping the import cancels parallel decoding between objects. An
    // in-flight gix decode or the admitted serial window may finish; source
    // workers never write destination data.
    let _cancellation = Cancellation(control.clone());
    let first_control = control.clone();
    let first_verifier = verifier.clone();
    let mut window = tokio::task::spawn_blocking(move || {
        let mut source = match source {
            Some(source)
                if source.decoded_bytes < super::super::MAX_GIT_SOURCE_MAPPING_WINDOW_BYTES =>
            {
                source
            }
            _ => SourcePool::open(path, format, limits.max_payload_bytes, workers)?,
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
            )?;
            let bytes = decoded.iter().map(|object| object.body.len() as u64).sum();
            return Ok(Window {
                source,
                pending,
                decoded,
                bytes,
                peak_workers: 1,
                groups: Vec::new(),
            });
        }
        let plan = source.plan(&mut pending, count, budget, &limits)?;
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
                &first_control,
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
    let objects = if window.groups.is_empty() {
        futures::stream::iter(std::mem::take(&mut window.decoded))
            .map(|object| stage(session, object))
            .buffer_unordered(count)
            .try_collect()
            .await?
    } else {
        let (sender, receiver) = tokio::sync::mpsc::channel(workers);
        let sources = std::mem::take(&mut window.source.sources);
        // Spawn eagerly: waiting for the receiver before spawning these jobs
        // would leave it waiting for senders that have never started.
        let jobs: Vec<_> = sources
            .into_iter()
            .zip(std::mem::take(&mut window.groups))
            .map(|(mut source, batch)| {
                if batch.is_empty() {
                    return futures::future::Either::Left(futures::future::ready(Ok(source)));
                }
                let verifier = verifier.clone();
                let control = control.clone();
                let sender = sender.clone();
                futures::future::Either::Right(tokio::task::spawn_blocking(move || {
                    let result =
                        decode(&mut source, batch, verifier.as_ref(), &control, |object| {
                            sender
                                .blocking_send(Ok(object))
                                .map_err(|_| source_error("Git staging receiver closed"))
                        });
                    if let Err(error) = result {
                        // A receiver closed by a staging failure already
                        // holds the error to return; do not replace it.
                        let _ = sender.blocking_send(Err(error));
                    }
                    source
                }))
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
        }
        let mut join_error = None;
        for result in futures::future::join_all(jobs).await {
            match result {
                Ok(source) => window.source.sources.push(source),
                Err(error) => {
                    join_error.get_or_insert_with(|| source_error(error));
                }
            }
        }
        let objects = result?;
        if let Some(error) = join_error {
            return Err(error);
        }
        objects
    };
    Ok(StagedWindow {
        source: window.source,
        pending: window.pending,
        objects,
        bytes: window.bytes,
        peak_workers: window
            .peak_workers
            .max(control.peak.load(Ordering::Relaxed)),
    })
}

#[cfg(test)]
mod tests;

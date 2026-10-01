//! CPU workers share an admission window, then finish before storage starts.

use super::{Result, decoded_mismatch, kind_mismatch, native_kind, source_error};
use crate::ObjectKey;
use crate::format::FormatLimits;
use crate::git::{GitObjectFormat, GitObjectKind, git_key_parts};
use crate::importers::GitClosureImport;
use crate::repository::{NativeSeal, NativeVerifier, RepositoryError};
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
    /// Canonically ordered selected roots, for classifying type mismatches.
    roots: Arc<[ObjectKey]>,
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
    pub body: Vec<u8>,
    pub seal: Option<NativeSeal>,
}

pub(super) struct Window {
    pub source: SourcePool,
    pub pending: VecDeque<ObjectKey>,
    pub decoded: Vec<Decoded>,
    pub bytes: u64,
    pub peak_workers: usize,
    groups: Vec<Vec<Pending>>,
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
    fn open(
        path: PathBuf,
        format: GitObjectFormat,
        limit: u64,
        workers: usize,
        roots: Arc<[ObjectKey]>,
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
            roots,
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
            // Reject a wrong type from the header alone: its size limit and
            // decoding cost belong to a different kind than the one selected.
            let actual = native_kind(header.kind());
            if actual != kind {
                return Err(kind_mismatch(&self.roots, key, kind, actual));
            }
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
            if let Some(error) = decoded_mismatch(key, kind, size, object.kind, &body) {
                return Err(error);
            }
            bytes = bytes
                .checked_add(size)
                .ok_or_else(|| source_error("decoded byte count overflow"))?;
            decoded.push(Decoded {
                key: pending.pop_front().expect("front exists"),
                body,
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
            let header = self.sources[0].header(oid).map_err(source_error)?;
            let actual = native_kind(header.kind());
            if actual != kind {
                return Err(kind_mismatch(&self.roots, key, kind, actual));
            }
            let size = header.size();
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
) -> Result<Vec<Decoded>> {
    let _active = Active::new(control);
    let mut output = Vec::with_capacity(batch.len());
    for pending in batch {
        if control.cancelled.load(Ordering::Relaxed) {
            return Err(source_error("Git decoding cancelled"));
        }
        let mut body = Vec::new();
        let object = source.find(&pending.oid, &mut body).map_err(source_error)?;
        if let Some(error) =
            decoded_mismatch(&pending.key, pending.kind, pending.size, object.kind, &body)
        {
            return Err(error);
        }
        let seal = verifier
            .map(|verifier| verifier.verify(&pending.key, &body))
            .transpose()?;
        output.push(Decoded {
            key: pending.key,
            body,
            seal,
        });
    }
    Ok(output)
}

pub(super) async fn decode_window(
    source: Option<SourcePool>,
    mut pending: VecDeque<ObjectKey>,
    request: &GitClosureImport,
    format: GitObjectFormat,
    limits: FormatLimits,
    verifier: Option<NativeVerifier>,
) -> Result<Window> {
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
    // A reopened pool keeps its roots; only the first window copies them.
    let roots = match &source {
        Some(source) => source.roots.clone(),
        None => request.roots.as_slice().into(),
    };
    let mut window = tokio::task::spawn_blocking(move || {
        let mut source = match source {
            Some(source)
                if source.decoded_bytes < super::super::MAX_GIT_SOURCE_MAPPING_WINDOW_BYTES =>
            {
                source
            }
            _ => SourcePool::open(path, format, limits.max_payload_bytes, workers, roots)?,
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
            decoded = decode(
                &mut source.sources[0],
                plan,
                first_verifier.as_ref(),
                &first_control,
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
    if !window.groups.is_empty() {
        let sources = std::mem::take(&mut window.source.sources);
        let jobs = sources
            .into_iter()
            .zip(std::mem::take(&mut window.groups))
            .map(|(mut source, batch)| {
                let verifier = verifier.clone();
                let control = control.clone();
                async move {
                    if batch.is_empty() {
                        return Ok((source, Vec::new()));
                    }
                    tokio::task::spawn_blocking(move || {
                        let decoded = decode(&mut source, batch, verifier.as_ref(), &control)?;
                        Ok::<_, super::GitClosureImportError>((source, decoded))
                    })
                    .await
                    .map_err(source_error)?
                }
            });
        // Join every source job even when one fails. No destination writer is
        // active yet, and successful results cannot escape a failed window.
        for result in futures::future::join_all(jobs).await {
            let (source, decoded) = result?;
            window.source.sources.push(source);
            window.decoded.extend(decoded);
        }
    }
    window.peak_workers = window
        .peak_workers
        .max(control.peak.load(Ordering::Relaxed));
    Ok(window)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::GitError;
    use crate::importers::GitClosureImportError;
    use std::io::Write;
    use std::process::{Command, Stdio};

    /// A bare SHA-1 object directory holding these loose blobs, and their keys.
    fn loose_blobs(bodies: &[&[u8]]) -> (tempfile::TempDir, Vec<ObjectKey>) {
        let directory = tempfile::tempdir().unwrap();
        let initialized = Command::new("git")
            .args(["init", "--bare", "-q", "--object-format=sha1"])
            .arg(directory.path())
            .output()
            .unwrap();
        assert!(initialized.status.success());
        let mut keys = Vec::new();
        for body in bodies {
            let mut child = Command::new("git")
                .arg("-C")
                .arg(directory.path())
                .args(["hash-object", "-w", "--stdin"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(body).unwrap();
            assert!(child.wait_with_output().unwrap().status.success());
            keys.push(
                crate::git::git_object_key_for_body(
                    GitObjectFormat::Sha1,
                    GitObjectKind::Blob,
                    body,
                )
                .unwrap(),
            );
        }
        (directory, keys)
    }

    fn open(directory: &tempfile::TempDir, limit: u64, roots: &[ObjectKey]) -> SourcePool {
        SourcePool::open(
            directory.path().join("objects"),
            GitObjectFormat::Sha1,
            limit,
            1,
            roots.into(),
        )
        .unwrap()
    }

    #[test]
    fn an_oversized_serial_body_does_not_admit_an_empty_sibling() {
        let (directory, keys) = loose_blobs(&[b"oversized", b""]);
        let mut keys = VecDeque::from(keys);
        let mut source = open(&directory, 16, &[]);
        let first = source.decode_serial(&mut keys, 2, 1, 16, 16).unwrap();
        assert_eq!(
            first.len(),
            1,
            "oversized bodies must occupy their own window"
        );
        assert_eq!(keys.len(), 1);
        let second = source.decode_serial(&mut keys, 2, 1, 16, 16).unwrap();
        assert_eq!(second.len(), 1);
        assert!(second[0].body.is_empty());
        assert!(keys.is_empty());
    }

    #[test]
    fn a_wrong_type_is_rejected_from_its_header_before_size_limits_or_decoding() {
        let body = [b'x'; 64];
        let (directory, blobs) = loose_blobs(&[&body]);
        let (format, _, oid) = git_key_parts(&blobs[0]).unwrap();
        let tree = crate::git::git_object_key(format, GitObjectKind::Tree, oid.to_vec()).unwrap();
        let limits = FormatLimits {
            max_payload_bytes: 1024,
            max_metadata_bytes: 16,
            ..Default::default()
        };
        // The blob exceeds the metadata limit a tree would be held to. Only the
        // type error describes the request; neither path decodes the body.
        for (roots, category) in [
            (
                vec![tree.clone()],
                crate::RepositoryErrorCategory::InvalidInput,
            ),
            (Vec::new(), crate::RepositoryErrorCategory::InvalidData),
        ] {
            for serial in [true, false] {
                let mut source = open(&directory, 1024, &roots);
                let mut pending = VecDeque::from([tree.clone()]);
                let error = if serial {
                    source
                        .decode_serial(&mut pending, 1, 1024, 1024, 16)
                        .err()
                        .unwrap()
                } else {
                    source.plan(&mut pending, 1, 1024, &limits).err().unwrap()
                };
                assert_eq!(error.category(), category, "{error}");
                match error {
                    GitClosureImportError::RootKind { root, actual } => {
                        assert_eq!(root, tree);
                        assert_eq!(actual, GitObjectKind::Blob);
                    }
                    GitClosureImportError::Git(GitError::InvalidObject(message)) => {
                        assert!(message.contains("linked as a tree but stored as a blob"));
                    }
                    other => panic!("unexpected error: {other}"),
                }
                assert_eq!(source.decoded_bytes, 0);
                assert_eq!(pending.len(), 1);
            }
        }
    }
}

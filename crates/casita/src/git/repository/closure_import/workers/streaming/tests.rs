use super::*;
use crate::blob::{
    BlobBatchGuard, BlobReader, BlobStore, BlobWriter, MemoryBlobStore, PayloadPublication,
};
use crate::git::{GitObjectFormat, GitObjectKind, git_object_key};
use crate::importers::GitClosureImport;
use crate::metadata::{BackendWriteScope, DataPinLease, MemoryMetadataStore, MetadataStore};
use crate::repository::Repository;
use crate::{BlobId, error::Error};
use futures::FutureExt;
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Condvar, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;

static GATES: Mutex<BTreeMap<gix::ObjectId, Arc<Gate>>> = Mutex::new(BTreeMap::new());
pub(super) struct Gate {
    entered: AtomicBool,
    released: Mutex<bool>,
    wake: Condvar,
    ready: tokio::sync::Notify,
}
impl Gate {
    pub(super) fn park(&self) {
        self.entered.store(true, Ordering::Release);
        self.ready.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
    }
    async fn entered(&self) {
        if !self.entered.load(Ordering::Acquire) {
            self.ready.notified().await;
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
pub(super) fn gate(oid: &gix::ObjectId) -> Option<Arc<Gate>> {
    GATES.lock().unwrap().get(oid).cloned()
}
struct Registration {
    oid: gix::ObjectId,
    gate: Arc<Gate>,
}
impl Registration {
    fn new(oid: gix::ObjectId) -> Self {
        let gate = Arc::new(Gate {
            entered: AtomicBool::new(false),
            released: Mutex::new(false),
            wake: Condvar::new(),
            ready: tokio::sync::Notify::new(),
        });
        assert!(GATES.lock().unwrap().insert(oid, gate.clone()).is_none());
        Self { oid, gate }
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.gate.release();
        GATES.lock().unwrap().remove(&self.oid);
    }
}
fn git(path: &std::path::Path, args: &[&str], bytes: &[u8]) -> String {
    use std::process::{Command, Stdio};
    let mut child = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(bytes).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
fn fixture() -> (tempfile::TempDir, gix::ObjectId, Vec<u8>) {
    let source = tempfile::tempdir().unwrap();
    git(source.path(), &["init", "--bare", "-q"], b"");
    let mut bytes = vec![b'x'; 2 * 1024 * 1024];
    let unique = source.path().to_str().unwrap().as_bytes();
    bytes[..unique.len()].copy_from_slice(unique);
    let oid = gix::ObjectId::from_hex(
        git(source.path(), &["hash-object", "-w", "--stdin"], &bytes).as_bytes(),
    )
    .unwrap();
    (source, oid, bytes)
}
fn reader(
    source: &std::path::Path,
    oid: &gix::ObjectId,
    size: u64,
    slots: Arc<Semaphore>,
) -> SourceReader {
    let handle = gix::odb::at(source.join("objects")).unwrap();
    Locator::open(handle.store_ref())
        .reader(oid, size, slots, Arc::new(Control::default()))
        .unwrap()
        .unwrap()
}
struct FailingStore {
    inner: MemoryBlobStore,
    gate: Arc<Gate>,
    failed: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl BlobStore for FailingStore {
    fn write_scope(&self) -> BackendWriteScope {
        self.inner.write_scope()
    }
    fn begin_pinned_batch(&self, pin: DataPinLease) -> Result<BlobBatchGuard, Error> {
        self.inner.begin_pinned_batch(pin)
    }
    fn publication(&self) -> PayloadPublication<'_> {
        self.inner.publication()
    }
    async fn has(&self, id: &BlobId) -> Result<bool, Error> {
        self.inner.has(id).await
    }
    async fn open_read(&self, id: &BlobId) -> Result<Option<Box<dyn BlobReader>>, Error> {
        self.inner.open_read(id).await
    }
    async fn open_write(&self) -> Box<dyn BlobWriter> {
        self.inner.open_write().await
    }
    async fn put_slice(&self, _: &[u8]) -> Result<BlobId, Error> {
        self.gate.entered().await;
        self.failed.notify_one();
        Err(Error::Msg("injected sibling staging failure".into()))
    }
}

#[tokio::test]
async fn error_return_drains_inflater_jobs_for_serial_and_grouped_windows() {
    for workers in [1, 2] {
        let (source, oid, _) = fixture();
        let small = git(
            source.path(),
            &["hash-object", "-w", "--stdin"],
            b"fail this sibling",
        );
        let registration = Registration::new(oid);
        let store = FailingStore {
            inner: MemoryBlobStore::new(),
            gate: registration.gate.clone(),
            failed: tokio::sync::Notify::new(),
        };
        let keys = [oid, gix::ObjectId::from_hex(small.as_bytes()).unwrap()].map(|oid| {
            git_object_key(
                GitObjectFormat::Sha1,
                GitObjectKind::Blob,
                oid.as_bytes().to_vec(),
            )
            .unwrap()
        });
        let repository = Repository::new(store, MemoryMetadataStore::new().unwrap());
        let request = GitClosureImport::new(source.path().join("objects"), keys.clone())
            .with_concurrency(2.try_into().unwrap())
            .with_decode_workers(workers.try_into().unwrap());
        let mut importing = Box::pin(repository.import(request));
        // Poll until the sibling has actually failed while an inflater is parked.
        let failed = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = &mut importing => panic!("import returned before parked inflater release: {result:?}"),
                _ = repository.payloads().failed.notified() => {}
            }
        }).await;
        let premature = importing.as_mut().now_or_never();
        registration.gate.release();
        failed.expect("sibling did not reach its staged failure");
        assert!(
            premature.is_none(),
            "error returned with source work still running"
        );
        let error = tokio::time::timeout(Duration::from_secs(10), importing)
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected sibling staging failure")
        );
        assert!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .object_batch(&keys)
                .await
                .unwrap()
                .iter()
                .all(Option::is_none)
        );
    }
}

#[tokio::test]
async fn cancellation_retains_submitted_job_permit_but_drops_waiting_job() {
    let (source, oid, bytes) = fixture();
    let slots = Arc::new(Semaphore::new(1));
    let occupied = slots.clone().acquire_owned().await.unwrap();
    let registration = Registration::new(oid);
    let mut waiting = reader(source.path(), &oid, bytes.len() as u64, slots.clone());
    let mut buffer = [0; 32];
    assert!(waiting.read(&mut buffer).now_or_never().is_none());
    drop(waiting);
    drop(occupied);
    assert_eq!(slots.available_permits(), 1);
    assert!(!registration.gate.entered.load(Ordering::Acquire));
    let mut submitted = reader(source.path(), &oid, bytes.len() as u64, slots.clone());
    assert!(submitted.read(&mut buffer).now_or_never().is_none());
    tokio::time::timeout(Duration::from_secs(10), registration.gate.entered())
        .await
        .unwrap();
    drop(submitted);
    assert_eq!(
        slots.available_permits(),
        0,
        "dropping a reader must not free a running job's capacity"
    );
    registration.gate.release();
    let _drained = tokio::time::timeout(Duration::from_secs(10), slots.acquire())
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn inflater_buffers_remain_fixed_and_declared_lengths_are_enforced() {
    let (source, oid, bytes) = fixture();
    for expected in [
        bytes.len() as u64 - 1,
        bytes.len() as u64,
        bytes.len() as u64 + 1,
    ] {
        let mut reader = reader(source.path(), &oid, expected, Arc::new(Semaphore::new(1)));
        let state = reader.state.as_mut().unwrap();
        let mut actual = Vec::new();
        let result: io::Result<()> = (|| {
            while !state.inflater.finished {
                let before = state.inflater.inflate.total_in();
                state.step()?;
                assert!(state.inflater.inflate.total_in() - before <= BUFFER_BYTES as u64);
                assert_eq!(state.inflater.input.len(), BUFFER_BYTES);
                assert_eq!(state.output.len(), BUFFER_BYTES);
                actual.extend_from_slice(&state.output[..state.filled]);
            }
            Ok(())
        })();
        if expected == bytes.len() as u64 {
            result.unwrap();
            assert_eq!(actual, bytes);
        } else {
            assert!(result.is_err());
        }
    }
}

#[test]
fn loose_lookup_does_not_initialize_optional_pack_indexes() {
    let (source, oid, bytes) = fixture();
    let handle = gix::odb::at(source.path().join("objects")).unwrap();
    let locator = Locator::open(handle.store_ref());
    assert!(
        locator
            .reader(
                &oid,
                bytes.len() as u64,
                Arc::new(Semaphore::new(1)),
                Arc::new(Control::default())
            )
            .unwrap()
            .is_some()
    );
    assert!(
        locator.indexes.get().is_none(),
        "a loose read must not initialize pack hints"
    );
}

#[test]
fn stale_pack_hints_are_misses_before_stream_selection() {
    let (source, oid, bytes) = fixture();
    let prefix = source.path().join("objects/pack/pack");
    let id = git(
        source.path(),
        &["pack-objects", "--window=0", prefix.to_str().unwrap()],
        format!("{oid}\n").as_bytes(),
    );
    let path = prefix.with_file_name(format!("pack-{id}.idx"));
    let data = std::fs::read(&path).unwrap().into_boxed_slice();
    let index =
        gix::odb::pack::index::File::from_data(data, path.clone(), gix::hash::Kind::Sha1).unwrap();
    let offset = index.pack_offset_at_index(index.lookup(oid).unwrap());
    let locator = Locator {
        roots: Vec::new(),
        indexes: OnceLock::new(),
        hash: gix::hash::Kind::Sha1,
    };
    assert!(locator.indexes.set(vec![index]).is_ok());
    for contents in [vec![0; 128], {
        let mut malformed =
            gix::odb::pack::data::header::encode(gix::odb::pack::data::Version::V2, 1).to_vec();
        malformed.extend_from_slice(&[0xff; 100]);
        malformed
    }] {
        std::fs::remove_file(path.with_extension("pack")).unwrap();
        std::fs::write(path.with_extension("pack"), contents).unwrap();
        assert!(
            locator
                .reader(
                    &oid,
                    bytes.len() as u64,
                    Arc::new(Semaphore::new(1)),
                    Arc::new(Control::default())
                )
                .unwrap()
                .is_none(),
            "stale hint at {offset} must defer to gix"
        );
    }
}

#[test]
fn optional_index_snapshots_obey_entry_count_and_byte_caps() {
    let (source, oid, _) = fixture();
    let prefix = source.path().join("objects/pack/pack");
    let id = git(
        source.path(),
        &["pack-objects", "--window=0", prefix.to_str().unwrap()],
        format!("{oid}\n").as_bytes(),
    );
    let index_bytes = std::fs::read(prefix.with_file_name(format!("pack-{id}.idx"))).unwrap();
    for count in [MAX_INDEX_FILES - 1, MAX_INDEX_FILES, MAX_INDEX_FILES + 1] {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("pack")).unwrap();
        for i in 0..count {
            std::fs::write(root.path().join(format!("pack/{i:03}.idx")), &index_bytes).unwrap();
        }
        let locator = Locator {
            roots: vec![root.path().to_owned()],
            indexes: OnceLock::new(),
            hash: gix::hash::Kind::Sha1,
        };
        assert_eq!(locator.indexes().len(), count.min(MAX_INDEX_FILES));
    }
    for overhead in [-1i64, 0, 1] {
        let primary = tempfile::tempdir().unwrap();
        let alternate = tempfile::tempdir().unwrap();
        for root in [&primary, &alternate] {
            std::fs::create_dir(root.path().join("pack")).unwrap();
        }
        // A malformed index still consumes the attempted-byte budget. The
        // next root's valid index is admitted only if its entire snapshot fits.
        let size = (MAX_INDEX_BYTES - index_bytes.len() as u64) as i64 + overhead;
        let file = std::fs::File::create(primary.path().join("pack/invalid.idx")).unwrap();
        file.set_len(size as u64).unwrap();
        std::fs::write(alternate.path().join("pack/valid.idx"), &index_bytes).unwrap();
        let locator = Locator {
            roots: vec![primary.path().to_owned(), alternate.path().to_owned()],
            indexes: OnceLock::new(),
            hash: gix::hash::Kind::Sha1,
        };
        assert_eq!(locator.indexes().len(), usize::from(overhead <= 0));
    }
    for count in [
        MAX_INDEX_ENTRIES - 1,
        MAX_INDEX_ENTRIES,
        MAX_INDEX_ENTRIES + 1,
    ] {
        let primary = tempfile::tempdir().unwrap();
        let alternate = tempfile::tempdir().unwrap();
        for root in [&primary, &alternate] {
            std::fs::create_dir(root.path().join("pack")).unwrap();
        }
        for i in 0..count {
            std::fs::write(primary.path().join(format!("pack/ignored-{i}")), b"").unwrap();
        }
        std::fs::write(alternate.path().join("pack/valid.idx"), &index_bytes).unwrap();
        let locator = Locator {
            roots: vec![primary.path().to_owned(), alternate.path().to_owned()],
            indexes: OnceLock::new(),
            hash: gix::hash::Kind::Sha1,
        };
        assert_eq!(
            locator.indexes().len(),
            usize::from(count < MAX_INDEX_ENTRIES)
        );
    }
}

/// Permanent threshold corpus for optional pack lookup. This measures locator
/// setup/selection, not end-to-end import or isolated memory use.
#[test]
#[ignore = "run through benchmark run git-source-locator"]
fn benchmark_git_source_locator() {
    let (source, oid, bytes) = fixture();
    let prefix = source.path().join("objects/pack/pack");
    let id = git(
        source.path(),
        &["pack-objects", "--window=0", prefix.to_str().unwrap()],
        format!("{oid}\n").as_bytes(),
    );
    let index_bytes = std::fs::read(prefix.with_file_name(format!("pack-{id}.idx"))).unwrap();
    let hex = oid.to_string();
    std::fs::remove_file(
        source
            .path()
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..]),
    )
    .unwrap();
    for dimension in ["index-files", "index-bytes", "directory-entries"] {
        for side in [-1i64, 0, 1] {
            let padding = tempfile::tempdir().unwrap();
            std::fs::create_dir(padding.path().join("pack")).unwrap();
            let (threshold, value, expected_stream) = match dimension {
                "index-files" => {
                    let count = (MAX_INDEX_FILES as i64 + side) as usize;
                    for i in 0..count {
                        // The snapshots contain this OID but have no pack file:
                        // all optional hints miss until the later source root.
                        std::fs::write(
                            padding.path().join(format!("pack/{i:03}.idx")),
                            &index_bytes,
                        )
                        .unwrap();
                    }
                    (MAX_INDEX_FILES as u64, count as u64, side < 0)
                }
                "index-bytes" => {
                    let value = (MAX_INDEX_BYTES as i64 + side) as u64;
                    let file =
                        std::fs::File::create(padding.path().join("pack/invalid.idx")).unwrap();
                    file.set_len(value - index_bytes.len() as u64).unwrap();
                    (MAX_INDEX_BYTES, value, side <= 0)
                }
                _ => {
                    let count = (MAX_INDEX_ENTRIES as i64 + side) as usize;
                    for i in 0..count {
                        std::fs::write(padding.path().join(format!("pack/ignored-{i}")), b"")
                            .unwrap();
                    }
                    (MAX_INDEX_ENTRIES as u64, count as u64, side < 0)
                }
            };
            // Initialize from a later root containing exactly one index. Add
            // its pack only after discovery so directory order cannot decide
            // the 255/256-entry boundary. File installation is not timed.
            let usable = tempfile::tempdir().unwrap();
            std::fs::create_dir(usable.path().join("pack")).unwrap();
            std::fs::write(usable.path().join("pack/valid.idx"), &index_bytes).unwrap();
            let start = std::time::Instant::now();
            let locator = Locator {
                roots: vec![padding.path().to_owned(), usable.path().to_owned()],
                indexes: OnceLock::new(),
                hash: gix::hash::Kind::Sha1,
            };
            locator.indexes();
            let initialize_nanos = start.elapsed().as_nanos();
            std::fs::hard_link(
                prefix.with_file_name(format!("pack-{id}.pack")),
                usable.path().join("pack/valid.pack"),
            )
            .unwrap();
            let start = std::time::Instant::now();
            let reader = locator
                .reader(
                    &oid,
                    bytes.len() as u64,
                    Arc::new(Semaphore::new(1)),
                    Arc::new(Control::default()),
                )
                .unwrap();
            let nanos = initialize_nanos + start.elapsed().as_nanos();
            let streamed = reader.is_some();
            assert_eq!(streamed, expected_stream, "{dimension} {value}");
            let actual = if let Some(mut reader) = reader {
                let state = reader.state.as_mut().unwrap();
                let mut actual = Vec::new();
                while !state.inflater.finished {
                    state.step().unwrap();
                    actual.extend_from_slice(&state.output[..state.filled]);
                }
                actual
            } else {
                use gix::objs::FindExt;
                let handle = gix::odb::at(source.path().join("objects")).unwrap();
                let mut actual = Vec::new();
                handle.find(&oid, &mut actual).unwrap();
                actual
            };
            assert_eq!(actual, bytes);
            println!(
                "git_source_locator_sample {}",
                serde_json::json!({
                    "dimension": dimension, "threshold": threshold, "value": value,
                    "side": side, "streamed": streamed, "wall_nanos": nanos,
                    "correctness": "asserted stream selection and exact stream/fallback payload",
                    "operation": "optional-pack-lookup", "index_bytes": index_bytes.len(),
                    "body_bytes": bytes.len(), "oid": oid.to_string()
                })
            );
        }
    }
}

#[tokio::test]
async fn cancelled_spool_reader_keeps_quota_until_submitted_job_stops() {
    use crate::spill::{SpillArea, SpillLimits};
    let (_source, oid, _) = fixture();
    let area = SpillArea::new(
        None,
        SpillLimits {
            max_memory_objects: 1,
            max_spill_bytes: 4,
        },
    );
    let slots = Arc::new(Semaphore::new(1));
    let registration = Registration::new(oid);
    let make_reader = || {
        let mut payload = area.payload(4).unwrap();
        payload.append(b"test").unwrap();
        payload.rewind().unwrap();
        SourceReader::from_spill(
            payload,
            4,
            slots.clone(),
            Arc::new(Control::default()),
            &oid,
        )
    };
    let occupied = slots.clone().acquire_owned().await.unwrap();
    let mut waiting = make_reader();
    let mut buffer = [0; 4];
    assert!(waiting.read(&mut buffer).now_or_never().is_none());
    assert!(area.payload(1).is_err());
    drop(waiting);
    assert!(area.payload(4).is_ok());
    assert!(!registration.gate.entered.load(Ordering::Acquire));
    drop(occupied);
    let mut submitted = make_reader();
    assert!(submitted.read(&mut buffer).now_or_never().is_none());
    tokio::time::timeout(Duration::from_secs(10), registration.gate.entered())
        .await
        .unwrap();
    drop(submitted);
    assert!(
        area.payload(1).is_err(),
        "running job must keep its file reservation"
    );
    registration.gate.release();
    let _drained = tokio::time::timeout(Duration::from_secs(10), slots.acquire())
        .await
        .unwrap()
        .unwrap();
    assert!(
        area.payload(4).is_ok(),
        "drained job must release its file reservation"
    );
}

#[test]
fn completed_unreceived_spool_keeps_its_source_slot() {
    use crate::spill::{SpillArea, SpillLimits};
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let (_source, oid, _) = fixture();
            let area = SpillArea::new(
                None,
                SpillLimits {
                    max_memory_objects: 1,
                    max_spill_bytes: 4,
                },
            );
            let mut payload = area.payload(4).unwrap();
            payload.append(b"test").unwrap();
            payload.rewind().unwrap();
            let slots = Arc::new(Semaphore::new(1));
            let control = Arc::new(Control::default());
            let registration = Registration::new(oid);
            let mut reader =
                SourceReader::from_spill(payload, 4, slots.clone(), control.clone(), &oid);
            let mut buffer = [0; 4];
            assert!(reader.read(&mut buffer).now_or_never().is_none());
            tokio::time::timeout(Duration::from_secs(10), registration.gate.entered())
                .await
                .unwrap();
            assert_eq!(control.active.load(Ordering::Relaxed), 1);
            registration.gate.release();
            // A fence on the sole blocking thread proves that the preceding job has
            // stored its output. An activity counter reaching zero is too early.
            tokio::time::timeout(Duration::from_secs(10), tokio::task::spawn_blocking(|| ()))
                .await
                .unwrap()
                .unwrap();
            // The blocking operation has returned, but its output has not been
            // received. Its file remains live and must still participate in draining.
            assert!(area.payload(1).is_err());
            assert_eq!(slots.available_permits(), 0);
            drop(reader);
            let _drained = tokio::time::timeout(Duration::from_secs(10), slots.acquire())
                .await
                .unwrap()
                .unwrap();
            assert!(area.payload(4).is_ok());
        });
}

#[test]
fn cancelled_queued_spool_job_keeps_quota_until_it_runs() {
    use crate::spill::{SpillArea, SpillLimits};
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let area = SpillArea::new(
                None,
                SpillLimits {
                    max_memory_objects: 1,
                    max_spill_bytes: 4,
                },
            );
            let mut payload = area.payload(4).unwrap();
            payload.append(b"test").unwrap();
            payload.rewind().unwrap();
            let slots = Arc::new(Semaphore::new(1));
            let (release, blocked) = std::sync::mpsc::channel::<()>();
            let (entered, started) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                let _ = blocked.recv();
            });
            started.await.unwrap();
            let oid = gix::ObjectId::from_bytes_or_panic(&[9; 20]);
            let mut reader = SourceReader::from_spill(
                payload,
                4,
                slots.clone(),
                Arc::new(Control::default()),
                &oid,
            );
            assert!(reader.read(&mut [0; 4]).now_or_never().is_none());
            drop(reader);
            assert_eq!(slots.available_permits(), 0);
            assert!(area.payload(1).is_err());
            drop(release);
            blocker.await.unwrap();
            let _drained = tokio::time::timeout(Duration::from_secs(10), slots.acquire())
                .await
                .unwrap()
                .unwrap();
            assert!(area.payload(4).is_ok());
        });
}

#[tokio::test]
async fn failed_spool_read_releases_quota_before_the_drain_barrier() {
    use crate::spill::{SpillArea, SpillLimits};
    let area = SpillArea::new(
        None,
        SpillLimits {
            max_memory_objects: 1,
            max_spill_bytes: 4,
        },
    );
    let slots = Arc::new(Semaphore::new(1));
    let mut payload = area.payload(4).unwrap();
    payload.append(b"test").unwrap();
    payload.rewind().unwrap();
    let oid = gix::ObjectId::from_bytes_or_panic(&[8; 20]);
    let mut reader = SourceReader::from_spill(
        payload,
        3,
        slots.clone(),
        Arc::new(Control::default()),
        &oid,
    );
    assert!(reader.read(&mut [0; 4]).await.is_err());
    let _drained = slots.acquire().await.unwrap();
    assert!(area.payload(4).is_ok());
}

#[test]
fn unreceived_compressed_result_does_not_block_another_reader() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let (source, oid, bytes) = fixture();
            let slots = Arc::new(Semaphore::new(1));
            let registration = Registration::new(oid);
            let mut first = reader(source.path(), &oid, bytes.len() as u64, slots.clone());
            let mut buffer = [0; 32];
            assert!(first.read(&mut buffer).now_or_never().is_none());
            tokio::time::timeout(Duration::from_secs(10), registration.gate.entered())
                .await
                .unwrap();
            registration.gate.release();
            // The first result is stored but deliberately not received. Its CPU
            // step is done, so another ordinary reader must be able to advance.
            tokio::time::timeout(Duration::from_secs(10), tokio::task::spawn_blocking(|| ()))
                .await
                .unwrap()
                .unwrap();
            let mut second = reader(source.path(), &oid, bytes.len() as u64, slots);
            tokio::time::timeout(Duration::from_secs(10), second.read_exact(&mut buffer))
                .await
                .expect("completed ordinary work must not retain execution admission")
                .unwrap();
            assert_eq!(&buffer, &bytes[..32]);
            first.read_exact(&mut buffer).await.unwrap();
            assert_eq!(&buffer, &bytes[..32]);
        });
}

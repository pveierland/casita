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

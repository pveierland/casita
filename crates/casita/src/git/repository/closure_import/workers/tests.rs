use super::*;
use crate::BlobId;
use crate::blob::{
    BlobBatchGuard, BlobReader, BlobStore, BlobWriter, MemoryBlobStore, PayloadPublication,
};
use crate::error::Error;
use crate::git::{GitError, git_object_key};
use crate::importers::{BackendImporter, GitClosureImportError};
use crate::metadata::{BackendWriteScope, DataPinLease, MemoryMetadataStore, MetadataStore};
use crate::repository::Repository;
use std::collections::BTreeMap;
use std::io::Write;
use std::num::NonZeroUsize;
use std::process::{Command, Stdio};
use std::result::Result;
use std::sync::{Condvar, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

static GATES: Mutex<BTreeMap<ObjectKey, Arc<Gate>>> = Mutex::new(BTreeMap::new());
const STAGING_FAILURE: &str = "injected streaming-window staging failure";

struct Gate {
    arrived: Mutex<Option<oneshot::Sender<()>>>,
    released: Mutex<bool>,
    wake: Condvar,
}
impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
struct Registration {
    key: ObjectKey,
    gate: Arc<Gate>,
}
impl Registration {
    fn new(key: ObjectKey) -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        let gate = Arc::new(Gate {
            arrived: Mutex::new(Some(sender)),
            released: Mutex::new(false),
            wake: Condvar::new(),
        });
        assert!(
            GATES
                .lock()
                .unwrap()
                .insert(key.clone(), gate.clone())
                .is_none()
        );
        (Self { key, gate }, receiver)
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.gate.release();
        GATES.lock().unwrap().remove(&self.key);
    }
}

pub(super) fn pause_before_decode(key: &ObjectKey) {
    let gate = GATES.lock().unwrap().get(key).cloned();
    if let Some(gate) = gate {
        if let Some(sender) = gate.arrived.lock().unwrap().take() {
            let _ = sender.send(());
        }
        let mut released = gate.released.lock().unwrap();
        while !*released {
            released = gate.wake.wait(released).unwrap();
        }
    }
}

struct ObservedStore {
    inner: MemoryBlobStore,
    selected: Vec<u8>,
    completed: Mutex<Option<oneshot::Sender<()>>>,
    fail: bool,
}
#[async_trait::async_trait]
impl BlobStore for ObservedStore {
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
    async fn put_slice(&self, bytes: &[u8]) -> Result<BlobId, Error> {
        let result = if bytes == self.selected && self.fail {
            Err(Error::Msg(STAGING_FAILURE.into()))
        } else {
            self.inner.put_slice(bytes).await
        };
        if bytes == self.selected {
            if let Some(sender) = self.completed.lock().unwrap().take() {
                let _ = sender.send(());
            }
        }
        result
    }
}

fn git(path: &std::path::Path, args: &[&str], input: &[u8]) -> String {
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
    child.stdin.take().unwrap().write_all(input).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

async fn exercise(format: GitObjectFormat, fail: bool) {
    let source = tempfile::tempdir().unwrap();
    let flag = match format {
        GitObjectFormat::Sha1 => "--object-format=sha1",
        GitObjectFormat::Sha256 => "--object-format=sha256",
    };
    git(source.path(), &["init", "--bare", "-q", flag], b"");
    // Unique fixture bytes keep the private gate isolated from parallel tests.
    let fast = format!("ready payload from {}", source.path().display()).into_bytes();
    let slow = format!("paused payload from {}", source.path().display()).into_bytes();
    let key = |bytes: &[u8]| {
        let oid = git(source.path(), &["hash-object", "-w", "--stdin"], bytes);
        git_object_key(
            format,
            GitObjectKind::Blob,
            data_encoding::HEXLOWER.decode(oid.as_bytes()).unwrap(),
        )
        .unwrap()
    };
    let fast_key = key(&fast);
    let slow_key = key(&slow);
    let (registration, mut arrived) = Registration::new(slow_key.clone());
    let (completed, mut completion) = oneshot::channel();
    let repository = Repository::new(
        ObservedStore {
            inner: MemoryBlobStore::new(),
            selected: fast.clone(),
            completed: Mutex::new(Some(completed)),
            fail,
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let request = GitClosureImport::new(
        source.path().join("objects"),
        [fast_key.clone(), slow_key.clone()],
    )
    .with_concurrency(NonZeroUsize::new(2).unwrap())
    .with_decode_workers(NonZeroUsize::new(2).unwrap());
    let mut importing = Box::pin(request.import_into(&repository));
    let mut finished = None;
    let paused = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            result = &mut importing => { finished = Some(result); false }
            result = &mut arrived => result.is_ok(),
        }
    })
    .await
    .unwrap_or(false);
    let overlapped = if paused && finished.is_none() {
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut importing => { finished = Some(result); false }
                result = &mut completion => result.is_ok(),
            }
        })
        .await
        .unwrap_or(false)
    } else {
        false
    };
    // An error return must join source work too, not abandon a blocked sender.
    let mut returned_before_release = false;
    if fail && overlapped && finished.is_none() {
        use futures::FutureExt;
        if let Some(result) = importing.as_mut().now_or_never() {
            returned_before_release = true;
            finished = Some(result);
        }
    }
    // Cleanup precedes all behavioral assertions, including the expected RED.
    registration.gate.release();
    let result = match finished {
        Some(result) => result,
        None => tokio::time::timeout(Duration::from_secs(10), importing)
            .await
            .expect("import did not settle after releasing its source"),
    };
    assert!(paused, "the selected source decoder did not reach its gate");
    if fail {
        assert!(
            result.unwrap_err().to_string().contains(STAGING_FAILURE),
            "source cleanup must preserve the original staging failure"
        );
        let snapshot = repository.metadata().snapshot().await.unwrap();
        assert!(
            snapshot
                .object_batch(&[fast_key, slow_key])
                .await
                .unwrap()
                .into_iter()
                .all(|record| record.is_none()),
            "a failed window must not publish its staged records"
        );
        assert!(
            !returned_before_release,
            "error returned before its source worker finished"
        );
    } else {
        let imported = result.unwrap();
        assert_eq!(imported.report.imported_objects, 2);
        assert_eq!(
            imported.report.source_bytes,
            (fast.len() + slow.len()) as u64
        );
        for (key, expected) in [(fast_key, fast), (slow_key, slow)] {
            let (_, mut payload) = imported.reader.open_payload(&key).await.unwrap().unwrap();
            let mut actual = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut payload, &mut actual)
                .await
                .unwrap();
            assert_eq!(actual, expected);
        }
    }
    assert!(
        overlapped,
        "destination staging waited for the last source decoder"
    );
}

#[tokio::test]
async fn streaming_window_stages_before_all_sha1_decoders_finish() {
    exercise(GitObjectFormat::Sha1, false).await;
}
#[tokio::test]
async fn streaming_window_stages_before_all_sha256_decoders_finish() {
    exercise(GitObjectFormat::Sha256, false).await;
}
#[tokio::test]
async fn streaming_window_preserves_staging_errors_and_joins_source_jobs() {
    exercise(GitObjectFormat::Sha1, true).await;
}

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
            crate::git::git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, body)
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
        crate::spill::SpillArea::new(None, crate::spill::SpillLimits::default()),
        false,
        roots.into(),
    )
    .unwrap()
}

#[test]
fn an_oversized_serial_body_does_not_admit_an_empty_sibling() {
    let (directory, keys) = loose_blobs(&[b"oversized", b""]);
    let mut keys = VecDeque::from(keys);
    let mut source = open(&directory, 16, &[]);
    let control = Arc::new(Control::default());
    let first = source
        .decode_serial(&mut keys, 2, 1, 16, 16, &control)
        .unwrap();
    assert_eq!(
        first.len(),
        1,
        "oversized bodies must occupy their own window"
    );
    assert_eq!(keys.len(), 1);
    let second = source
        .decode_serial(&mut keys, 2, 1, 16, 16, &control)
        .unwrap();
    assert_eq!(second.len(), 1);
    assert!(matches!(&second[0].body, Body::Buffered(bytes) if bytes.is_empty()));
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
                    .decode_serial(&mut pending, 1, 1024, 1024, 16, &Arc::default())
                    .err()
                    .unwrap()
            } else {
                source
                    .plan(&mut pending, 1, 1024, &limits, &Arc::default())
                    .err()
                    .unwrap()
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

async fn shared_source_and_compression(workers: usize) {
    use crate::blob::ChunkedBlobStore;
    use crate::import_cpu::ImportCpuBudget;
    let source = tempfile::tempdir().unwrap();
    git(source.path(), &["init", "--bare", "-q"], b"");
    let mut keys = Vec::new();
    for index in 0..4 {
        let bytes = format!("shared cpu source {} #{index}", source.path().display());
        let oid = git(
            source.path(),
            &["hash-object", "-w", "--stdin"],
            bytes.as_bytes(),
        );
        keys.push(
            git_object_key(
                GitObjectFormat::Sha1,
                GitObjectKind::Blob,
                data_encoding::HEXLOWER.decode(oid.as_bytes()).unwrap(),
            )
            .unwrap(),
        );
    }
    let (registration, mut arrived) = Registration::new(keys[0].clone());
    let budget = ImportCpuBudget::new(NonZeroUsize::MIN);
    let repository = Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
    let request = GitClosureImport::new(source.path().join("objects"), keys)
        .with_decode_workers(NonZeroUsize::new(workers).unwrap())
        .with_cpu_budget(budget.clone());
    let mut importing = Box::pin(request.import_into(&repository));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            result = &mut importing => panic!("import finished before the source gate: {result:?}"),
            result = &mut arrived => result.unwrap(),
        }
    })
    .await
    .unwrap();
    let store = ChunkedBlobStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::default(),
        1024,
    );
    // This small fresh blob reuses its whole-blob digest. It exercises the
    // prehashed compression path, independently of the multi-chunk hash test.
    let bytes = b"fresh destination compression shares source execution admission";
    let mut writing = Box::pin(budget.scope(store.put_slice(bytes)));
    let early = tokio::time::timeout(Duration::from_millis(100), writing.as_mut()).await;
    let bypassed = early.is_ok();
    registration.gate.release();
    let digest = match early {
        Ok(result) => result.unwrap(),
        Err(_) => tokio::time::timeout(Duration::from_secs(10), writing)
            .await
            .unwrap()
            .unwrap(),
    };
    let outcome = tokio::time::timeout(Duration::from_secs(10), importing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.report.imported_objects, 4);
    assert_eq!(digest, BlobId::new(blake3::hash(bytes).into()));
    assert!(
        !bypassed,
        "compression ran while a source job occupied the shared CPU slot"
    );
}

#[tokio::test]
async fn shared_cpu_serial_source_and_compression_do_not_overlap() {
    shared_source_and_compression(1).await;
}

#[tokio::test]
async fn shared_cpu_parallel_source_and_compression_do_not_overlap() {
    shared_source_and_compression(4).await;
}

struct CpuCaptureStore {
    inner: crate::blob::ChunkedBlobStore,
    budget: crate::import_cpu::ImportCpuBudget,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    wait: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}
#[async_trait::async_trait]
impl BlobStore for CpuCaptureStore {
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
        // Capture the real import's context before parking unrelated work on
        // its coordinator. The test never explicitly scopes this writer.
        let writer = self.inner.open_write().await;
        let budget = self.budget.clone();
        let wait = self
            .wait
            .lock()
            .unwrap()
            .take()
            .expect("one fresh blob writer");
        let entered = self.entered.lock().unwrap().take().unwrap();
        let (ready, started) = oneshot::channel();
        tokio::spawn(async move {
            budget
                .run(move || {
                    let _ = ready.send(());
                    let _ = wait.recv();
                })
                .await
                .unwrap();
        });
        started.await.unwrap();
        let _ = entered.send(());
        writer
    }
}

#[tokio::test]
async fn shared_cpu_import_propagates_to_its_actual_chunked_destination() {
    use crate::import_cpu::ImportCpuBudget;
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let source = tempfile::tempdir().unwrap();
    git(source.path(), &["init", "--bare", "-q"], b"");
    let bytes = b"automatic source-to-destination CPU scope";
    let oid = git(source.path(), &["hash-object", "-w", "--stdin"], bytes);
    let key = git_object_key(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        data_encoding::HEXLOWER.decode(oid.as_bytes()).unwrap(),
    )
    .unwrap();
    let budget = ImportCpuBudget::new(NonZeroUsize::MIN);
    let (release, wait) = std::sync::mpsc::channel();
    let release = Release(Some(release));
    let (entered, mut started) = oneshot::channel();
    let repository = Repository::new(
        CpuCaptureStore {
            inner: crate::blob::ChunkedBlobStore::new(
                Arc::new(object_store::memory::InMemory::new()),
                object_store::path::Path::default(),
                1024,
            ),
            budget: budget.clone(),
            entered: Mutex::new(Some(entered)),
            wait: Mutex::new(Some(wait)),
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let request = GitClosureImport::new(source.path().join("objects"), [key.clone()])
        .with_cpu_budget(budget.clone());
    let mut importing = Box::pin(request.import_into(&repository));
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            result = &mut importing => panic!("import finished before destination admission: {result:?}"),
            result = &mut started => result.unwrap(),
        }
    }).await.unwrap();
    let early = tokio::time::timeout(Duration::from_millis(100), importing.as_mut()).await;
    let bypassed = early.is_ok();
    drop(release);
    let outcome = match early {
        Ok(result) => result.unwrap(),
        Err(_) => tokio::time::timeout(Duration::from_secs(10), importing)
            .await
            .unwrap()
            .unwrap(),
    };
    budget.run(|| ()).await.unwrap();
    assert_eq!(outcome.report.imported_objects, 1);
    let (_, mut payload) = outcome.reader.open_payload(&key).await.unwrap().unwrap();
    let mut actual = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut payload, &mut actual)
        .await
        .unwrap();
    assert_eq!(actual, bytes);
    assert!(
        !bypassed,
        "the import's own chunked writer did not inherit CPU admission"
    );
}

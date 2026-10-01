use super::*;
use crate::BlobId;
use crate::blob::{
    BlobBatchGuard, BlobReader, BlobStore, BlobWriter, MemoryBlobStore, PayloadPublication,
};
use crate::error::Error;
use crate::git::git_object_key;
use crate::importers::BackendImporter;
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

#[test]
fn an_oversized_serial_body_does_not_admit_an_empty_sibling() {
    let directory = tempfile::tempdir().unwrap();
    let initialized = Command::new("git")
        .args(["init", "--bare", "-q", "--object-format=sha1"])
        .arg(directory.path())
        .output()
        .unwrap();
    assert!(initialized.status.success());
    let mut keys = VecDeque::new();
    for body in [b"oversized".as_slice(), b""] {
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
        keys.push_back(
            crate::git::git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, body)
                .unwrap(),
        );
    }
    let mut source = SourcePool::open(
        directory.path().join("objects"),
        GitObjectFormat::Sha1,
        16,
        1,
    )
    .unwrap();
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

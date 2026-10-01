use super::{Source, key};
use casita::BlobId;
use casita::experimental::{
    BackendWriteScope, BlobBatchGuard, BlobReader, BlobStore, BlobWriter, DataPinLease, Error,
    GitObjectFormat, GitObjectKind, MemoryBlobStore, MemoryMetadataStore, MetadataStore,
    PayloadPublication, Repository,
};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::task::{Context, Poll};

struct ObservedWriter {
    inner: Box<dyn BlobWriter>,
    written: Arc<AtomicU64>,
}
impl tokio::io::AsyncWrite for ObservedWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, bytes);
        if let Poll::Ready(Ok(count)) = result {
            self.written.fetch_add(count as u64, Ordering::Relaxed);
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
#[async_trait::async_trait]
impl BlobWriter for ObservedWriter {
    async fn close(&mut self) -> Result<(BlobId, u64), Error> {
        self.inner.close().await
    }
}

struct ObservedStore {
    inner: MemoryBlobStore,
    written: Arc<AtomicU64>,
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
        Box::new(ObservedWriter {
            inner: self.inner.open_write().await,
            written: self.written.clone(),
        })
    }
    async fn put_slice(&self, bytes: &[u8]) -> Result<BlobId, Error> {
        self.inner.put_slice(bytes).await
    }
}

// A complete-body decoder fails at the trailer before it can start staging.
// Incremental inflation must deliver an earlier prefix, then fail without
// publishing a native object record when the corrupt trailer is reached.
#[tokio::test]
async fn loose_blob_stages_a_prefix_before_rejecting_a_late_inflate_error() {
    for (name, format) in [
        ("sha1", GitObjectFormat::Sha1),
        ("sha256", GitObjectFormat::Sha256),
    ] {
        for workers in [1, 4] {
            let source = Source::new(name);
            let body = vec![b'x'; 8 * 1024 * 1024];
            let oid = source.blob(&body);
            let root = key(format, GitObjectKind::Blob, &oid);
            let path = source
                .0
                .path()
                .join("objects")
                .join(&oid[..2])
                .join(&oid[2..]);
            let mut compressed = std::fs::read(&path).unwrap();
            *compressed.last_mut().unwrap() ^= 1;
            // Git creates loose objects read-only; replace this private fixture entry.
            std::fs::remove_file(&path).unwrap();
            std::fs::write(path, compressed).unwrap();
            let written = Arc::new(AtomicU64::new(0));
            let repository = Repository::new(
                ObservedStore {
                    inner: MemoryBlobStore::new(),
                    written: written.clone(),
                },
                MemoryMetadataStore::new().unwrap(),
            );
            let result = repository
                .import(
                    source
                        .request(vec![root.clone()])
                        .with_decode_workers(std::num::NonZeroUsize::new(workers).unwrap()),
                )
                .await;
            assert!(
                result.is_err(),
                "a corrupt zlib trailer must fail the import"
            );
            assert!(
                repository
                    .metadata()
                    .snapshot()
                    .await
                    .unwrap()
                    .object(&root)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                written.load(Ordering::Relaxed) > 0,
                "source inflation must deliver a prefix before decoding the complete blob"
            );
            assert!(
                written.load(Ordering::Relaxed) <= body.len() as u64,
                "the stream must never deliver more than the declared body"
            );
        }
    }
}

#[tokio::test]
async fn streaming_and_buffered_blobs_roundtrip_across_threshold_and_alternates() {
    use tokio::io::AsyncReadExt;
    for (name, format) in [
        ("sha1", GitObjectFormat::Sha1),
        ("sha256", GitObjectFormat::Sha256),
    ] {
        {
            let source = Source::new(name);
            let alternate = Source::new(name);
            std::fs::write(
                alternate.0.path().join("objects/info/alternates"),
                format!("{}\n", source.0.path().join("objects").display()),
            )
            .unwrap();
            let mut blobs = Vec::new();
            for size in [
                0,
                1024 * 1024 - 1,
                1024 * 1024,
                1024 * 1024 + 1,
                4 * 1024 * 1024,
            ] {
                let body: Vec<_> = (0..size).map(|i| ((i * 31 + i / 7) % 251) as u8).collect();
                let oid = source.blob(&body);
                blobs.push((oid, body));
            }
            let roots: Vec<_> = blobs
                .iter()
                .map(|(oid, _)| key(format, GitObjectKind::Blob, oid))
                .collect();
            for workers in [1, 4] {
                // Covers oversized isolation and an empty object in the same selection.
                for budget in [1024 * 1024 - 1, 1024 * 1024, 16 * 1024 * 1024] {
                    let repository =
                        Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
                    let imported = repository
                        .import(
                            alternate
                                .request(roots.clone())
                                .with_decode_workers(workers.try_into().unwrap())
                                .with_max_buffered_bytes((budget as u64).try_into().unwrap()),
                        )
                        .await
                        .unwrap();
                    assert_eq!(imported.report.imported_objects, blobs.len());
                    assert!(
                        imported.report.peak_source_bytes <= (budget as u64).max(4 * 1024 * 1024)
                    );
                    assert!(imported.report.peak_decode_workers <= workers);
                    for (root, (_, expected)) in roots.iter().zip(&blobs) {
                        let (_, mut payload) =
                            imported.reader.open_payload(root).await.unwrap().unwrap();
                        let mut actual = Vec::new();
                        payload.read_to_end(&mut actual).await.unwrap();
                        assert_eq!(&actual, expected);
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn streamed_native_mismatch_short_and_excess_payloads_never_publish() {
    for (name, format) in [
        ("sha1", GitObjectFormat::Sha1),
        ("sha256", GitObjectFormat::Sha256),
    ] {
        for length in [2 * 1024 * 1024 - 1, 2 * 1024 * 1024, 2 * 1024 * 1024 + 1] {
            let source = Source::new(name);
            let original = source.blob(&vec![b'x'; 2 * 1024 * 1024]);
            let replacement = source.blob(&vec![b'y'; length]);
            let objects = source.0.path().join("objects");
            source.remove(&original);
            std::fs::copy(
                objects.join(&replacement[..2]).join(&replacement[2..]),
                objects.join(&original[..2]).join(&original[2..]),
            )
            .unwrap();
            let root = key(format, GitObjectKind::Blob, &original);
            let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
            assert!(
                repository
                    .import(source.request(vec![root.clone()]))
                    .await
                    .is_err()
            );
            assert!(
                repository
                    .metadata()
                    .snapshot()
                    .await
                    .unwrap()
                    .object(&root)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[test]
fn streamed_windows_progress_with_one_blocking_thread_and_local_storage() {
    let source = Source::new("sha1");
    let roots = (0..16u8)
        .map(|i| {
            key(
                GitObjectFormat::Sha1,
                GitObjectKind::Blob,
                &source.blob(&vec![i; if i % 2 == 0 { 2 * 1024 * 1024 } else { 32768 }]),
            )
        })
        .collect::<Vec<_>>();
    let destination = tempfile::tempdir().unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let repository = Repository::local(destination.path()).await.unwrap();
            let imported = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                repository.import(
                    source
                        .request(roots)
                        .with_decode_workers(2.try_into().unwrap())
                        .with_max_buffered_bytes((32 * 1024 * 1024).try_into().unwrap()),
                ),
            )
            .await
            .expect("stream readers and storage must share one blocking thread")
            .unwrap();
            assert_eq!(imported.report.imported_objects, 16);
            assert_eq!(imported.report.peak_decode_workers, 1);
            drop(imported);
            repository.flush().await.unwrap();
        });
}

#[tokio::test]
async fn valid_zlib_with_wrong_declared_length_and_truncated_zlib_never_publish() {
    use std::io::Write;
    for (name, format) in [
        ("sha1", GitObjectFormat::Sha1),
        ("sha256", GitObjectFormat::Sha256),
    ] {
        for alteration in ["short", "excess", "truncated"] {
            let source = Source::new(name);
            let size = 2 * 1024 * 1024;
            let oid = source.blob(&vec![b'x'; size]);
            let root = key(format, GitObjectKind::Blob, &oid);
            let path = source
                .0
                .path()
                .join("objects")
                .join(&oid[..2])
                .join(&oid[2..]);
            let mut encoder = gix::features::zlib::stream::deflate::Write::new(Vec::new());
            encoder
                .write_all(format!("blob {size}\0").as_bytes())
                .unwrap();
            encoder
                .write_all(&vec![
                    b'x';
                    match alteration {
                        "short" => size - 1,
                        "excess" => size + 1,
                        _ => size,
                    }
                ])
                .unwrap();
            encoder.flush().unwrap();
            let mut bytes = encoder.into_inner();
            if alteration == "truncated" {
                bytes.truncate(bytes.len() - 4);
            }
            source.remove(&oid);
            std::fs::write(path, bytes).unwrap();
            let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
            let error = repository
                .import(source.request(vec![root.clone()]))
                .await
                .unwrap_err();
            assert!(
                repository
                    .metadata()
                    .snapshot()
                    .await
                    .unwrap()
                    .object(&root)
                    .await
                    .unwrap()
                    .is_none(),
                "{error}"
            );
        }
    }
}

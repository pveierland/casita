#![cfg(all(feature = "native", feature = "experimental"))]

use casita::experimental::{
    BlobFormat, DirectLinkView, FormatError, FormatLimits, FormatRegistry, MemoryBlobStore,
    MemoryMetadataStore, MetadataStore, ObjectFormat, Repository, VerificationContext,
    VerifiedObject,
};
use casita::{NamespaceId, ObjectRecord};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[path = "support/counting_blob_store.rs"]
mod counting_blob_store;
use counting_blob_store::CountingBlobStore;

#[tokio::test]
async fn unrooted_verified_raw_blobs_receive_closure_witnesses_without_reads() {
    let reads = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let repository = Repository::new(
        CountingBlobStore {
            inner: MemoryBlobStore::new(),
            reads: reads.clone(),
            writes: writes.clone(),
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let session = repository.mutation_session().await.unwrap();
    let mut keys = Vec::new();
    for size in [0, 1, 65535, 65536, 65537] {
        let object = session.stage_blob(&vec![b'x'; size]).await.unwrap();
        let key = object.record().key().clone();
        session.publish_unrooted(vec![object]).await.unwrap();
        assert_eq!(
            repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .validated_closures(std::slice::from_ref(&key))
                .await
                .unwrap(),
            [true]
        );
        keys.push(key);
    }
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(writes.load(Ordering::SeqCst), 5);
    drop(session);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    let collected = repository.collect_logical().await.unwrap();
    assert_eq!(collected.removed.logical_objects, keys.len());
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&keys)
            .await
            .unwrap(),
        vec![false; keys.len()]
    );
}

#[tokio::test]
async fn rooting_a_staged_verified_raw_blob_does_not_reopen_its_payload() {
    let reads = Arc::new(AtomicUsize::new(0));
    let repository = Repository::new(
        CountingBlobStore {
            inner: MemoryBlobStore::new(),
            reads: reads.clone(),
            writes: Arc::new(AtomicUsize::new(0)),
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let session = repository.mutation_session().await.unwrap();
    let object = session.stage_blob(b"verified raw bytes").await.unwrap();
    let key = object.record().key().clone();
    session
        .publish_rooted(vec![object], "file".try_into().unwrap(), key.clone())
        .await
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    let (_, mut reader) = repository.open_payload(&key).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, b"verified raw bytes");
}

struct RejectRaw {
    inner: BlobFormat,
    calls: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl ObjectFormat for RejectRaw {
    fn namespace(&self) -> &NamespaceId {
        self.inner.namespace()
    }
    async fn verify(
        &self,
        context: VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<VerifiedObject, FormatError> {
        self.inner.verify(context, limits).await
    }
    async fn verify_links(
        &self,
        _context: VerificationContext<'_>,
        _object: &ObjectRecord,
        _links: &dyn DirectLinkView,
        _limits: &FormatLimits,
    ) -> Result<(), FormatError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(FormatError::InvalidPayload {
            namespace: self.namespace().clone(),
            message: "custom raw relation rejected".into(),
        })
    }
}

#[tokio::test]
async fn raw_blob_shortcut_does_not_apply_to_custom_registries() {
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = FormatRegistry::new([Arc::new(RejectRaw {
        inner: BlobFormat::default(),
        calls: calls.clone(),
    }) as Arc<dyn ObjectFormat>])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        registry,
        FormatLimits::default(),
    );
    let session = repository.mutation_session().await.unwrap();
    let object = session.stage_blob(b"native body seal").await.unwrap();
    let key = object.record().key().clone();
    session.publish_unrooted(vec![object]).await.unwrap();
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(std::slice::from_ref(&key))
            .await
            .unwrap(),
        [false]
    );
    assert!(
        session
            .publish_rooted(Vec::new(), "rejected".try_into().unwrap(), key.clone())
            .await
            .is_err()
    );
    assert!(calls.load(Ordering::SeqCst) > 0);
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .root(&"rejected".try_into().unwrap())
            .await
            .unwrap(),
        None
    );
    assert_eq!(snapshot.validated_closures(&[key]).await.unwrap(), [false]);
}

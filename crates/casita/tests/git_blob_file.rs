#![cfg(all(feature = "native", feature = "git", feature = "experimental"))]

use casita::experimental::{
    GitObjectFormat, GitObjectKind, MemoryBlobStore, MemoryMetadataStore, MetadataStore, Repository,
};

#[path = "support/counting_blob_store.rs"]
mod counting_blob_store;
use counting_blob_store::CountingBlobStore;

#[tokio::test]
async fn git_blob_file_registration_preserves_content_without_payload_reads() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
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
    let body = b"shared git and plain file bytes";
    let git = casita::experimental::git_object_key_for_body(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        body,
    )
    .unwrap();
    let native_session = repository.mutation_session().await.unwrap();
    let native = native_session
        .stage_object(git.clone(), body)
        .await
        .unwrap();
    native_session.publish_unrooted(vec![native]).await.unwrap();
    let session = repository.mutation_session().await.unwrap();
    let staged = session.stage_git_blob_file(&git).await.unwrap();
    let file = staged.record().key().clone();
    assert_eq!(staged.record().payload_size(), 31);
    session
        .publish_rooted(vec![staged], "file".try_into().unwrap(), file.clone())
        .await
        .unwrap();
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "neither registration nor rooting should reread verified blob content"
    );
    assert_eq!(
        writes.load(Ordering::SeqCst),
        1,
        "only the original native ingestion writes payload bytes"
    );
    assert!(session.stage_git_blob_file(&file).await.is_err());
    let (_, mut opened) = repository.open_payload(&file).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut opened, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, b"shared git and plain file bytes");
    drop(native_session);
}
/// Permanent workload: benchmark run git-blob-file. Both strategies start
/// with an already durable native blob; setup and audits are outside timing.
#[tokio::test]
#[ignore = "run through benchmark run git-blob-file"]
async fn benchmark_git_blob_file() {
    let backend = std::env::var("CASITA_GIT_ALIAS_BACKEND").unwrap();
    if backend == "local" {
        let destination = tempfile::tempdir().unwrap();
        let repository = Repository::local(destination.path()).await.unwrap();
        alias_benchmark(&repository).await;
        repository.flush().await.unwrap();
    } else {
        assert_eq!(backend, "memory");
        let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
        alias_benchmark(&repository).await;
    }
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
}

async fn alias_benchmark<PS: casita::experimental::BlobStore, SS: MetadataStore>(
    repository: &Repository<PS, SS>,
) {
    use casita::{BlobId, Digest, ObjectKey};
    let bytes: usize = std::env::var("CASITA_GIT_ALIAS_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    let strategy = std::env::var("CASITA_GIT_ALIAS_STRATEGY").unwrap();
    let backend = std::env::var("CASITA_GIT_ALIAS_BACKEND").unwrap();
    let mut body = vec![0u8; bytes];
    let mut state = 0x9e3779b97f4a7c15u64;
    for part in body.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        part.copy_from_slice(&state.to_le_bytes()[..part.len()]);
    }
    let git = casita::experimental::git_object_key_for_body(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        &body,
    )
    .unwrap();
    let expected = ObjectKey::blob(BlobId::new(Digest::hash(&body)));
    let native_session = repository.mutation_session().await.unwrap();
    let native = native_session
        .stage_object(git.clone(), &body)
        .await
        .unwrap();
    native_session.publish_unrooted(vec![native]).await.unwrap();

    let start = std::time::Instant::now();
    let session = repository.mutation_session().await.unwrap();
    let staged = match strategy.as_str() {
        "reread" => {
            let record = repository
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .object(&git)
                .await
                .unwrap()
                .unwrap();
            session
                .stage_existing(ObjectKey::blob(record.payload()), record.payload())
                .await
                .unwrap()
        }
        "alias" => session.stage_git_blob_file(&git).await.unwrap(),
        _ => panic!("unknown alias strategy"),
    };
    assert_eq!(staged.record().key(), &expected);
    assert_eq!(staged.record().payload_size(), bytes as u64);
    session
        .publish_rooted(vec![staged], "file".try_into().unwrap(), expected.clone())
        .await
        .unwrap();
    let wall_nanos = start.elapsed().as_nanos();

    assert_eq!(
        repository.verify_closure(&expected).await.unwrap(),
        casita::experimental::ClosureStatus::Complete { objects: 1 }
    );
    let (_, mut reader) = repository.open_payload(&expected).await.unwrap().unwrap();
    let mut actual = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual)
        .await
        .unwrap();
    assert_eq!(actual, body);
    println!(
        "git_blob_file_sample {}",
        serde_json::json!({
            "strategy": strategy, "backend": backend, "file_bytes": bytes,
            "wall_nanos": wall_nanos, "root": expected.to_string(),
            "correctness": "exact identity, length, closure and byte-for-byte readback",
        })
    );
}

#[tokio::test]
async fn aliases_support_both_git_hashes_and_empty_payloads() {
    use casita::{BlobId, Digest, ObjectKey};
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    for format in [GitObjectFormat::Sha1, GitObjectFormat::Sha256] {
        for body in [b"".as_slice(), b"native contents"] {
            let session = repository.mutation_session().await.unwrap();
            let git =
                casita::experimental::git_object_key_for_body(format, GitObjectKind::Blob, body)
                    .unwrap();
            let native = session.stage_object(git.clone(), body).await.unwrap();
            session.publish_unrooted(vec![native]).await.unwrap();
            let alias = session.stage_git_blob_file(&git).await.unwrap();
            assert_eq!(
                alias.record().key(),
                &ObjectKey::blob(BlobId::new(Digest::hash(body)))
            );
            assert_eq!(alias.record().payload_size(), body.len() as u64);
            session.publish_unrooted(vec![alias]).await.unwrap();
        }
        let tree = casita::experimental::git_object_key_for_body(format, GitObjectKind::Tree, b"")
            .unwrap();
        let missing = casita::experimental::git_object_key_for_body(
            format,
            GitObjectKind::Blob,
            b"not stored",
        )
        .unwrap();
        let session = repository.mutation_session().await.unwrap();
        assert!(session.stage_git_blob_file(&tree).await.is_err());
        assert!(session.stage_git_blob_file(&missing).await.is_err());
    }
}

#[tokio::test]
async fn staged_alias_protects_reused_bytes_across_collection() {
    let body = b"protected by the receiving mutation";
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let git = casita::experimental::git_object_key_for_body(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        body,
    )
    .unwrap();
    let native_session = repository.mutation_session().await.unwrap();
    let native = native_session
        .stage_object(git.clone(), body)
        .await
        .unwrap();
    native_session.publish_unrooted(vec![native]).await.unwrap();
    let receiving = repository.mutation_session().await.unwrap();
    let alias = receiving.stage_git_blob_file(&git).await.unwrap();
    let key = alias.record().key().clone();
    drop(native_session);
    repository.collect().await.unwrap();
    receiving
        .publish_rooted(vec![alias], "file".try_into().unwrap(), key.clone())
        .await
        .unwrap();
    drop(receiving);
    repository.collect().await.unwrap();
    let (_, mut reader) = repository.open_payload(&key).await.unwrap().unwrap();
    let mut actual = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual)
        .await
        .unwrap();
    assert_eq!(actual, body);
}

/// Delivers one previously valid snapshot, as if collection completed between
/// the initial metadata read and admission of the alias's data protections.
struct StaleOnceMetadata {
    inner: MemoryMetadataStore,
    stale: std::sync::Mutex<Option<std::sync::Arc<dyn casita::experimental::MetadataSnapshot>>>,
}
#[async_trait::async_trait]
impl MetadataStore for StaleOnceMetadata {
    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<casita::experimental::RepositoryLease>, casita::experimental::MetadataError>
    {
        self.inner.try_collection_lease().await
    }
    async fn pin_store(
        &self,
    ) -> Result<
        std::sync::Arc<dyn casita::experimental::PinStore>,
        casita::experimental::MetadataError,
    > {
        self.inner.pin_store().await
    }
    async fn snapshot(
        &self,
    ) -> Result<
        std::sync::Arc<dyn casita::experimental::MetadataSnapshot>,
        casita::experimental::MetadataError,
    > {
        let stale = self.stale.lock().unwrap().take();
        if let Some(snapshot) = stale {
            return Ok(snapshot);
        }
        self.inner.snapshot().await
    }
    async fn commit(
        &self,
        expected: &casita::RepositoryRevision,
        mutation: casita::experimental::MetadataMutation,
    ) -> Result<casita::experimental::CommitResult, casita::experimental::MetadataError> {
        self.inner.commit(expected, mutation).await
    }
}

#[tokio::test]
async fn alias_rechecks_records_after_admitting_protection() {
    let repository = Repository::new(
        MemoryBlobStore::new(),
        StaleOnceMetadata {
            inner: MemoryMetadataStore::new().unwrap(),
            stale: std::sync::Mutex::new(None),
        },
    );
    let key = casita::experimental::git_object_key_for_body(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        b"collected",
    )
    .unwrap();
    let native = repository.mutation_session().await.unwrap();
    let staged = native
        .stage_object(key.clone(), b"collected")
        .await
        .unwrap();
    native.publish_unrooted(vec![staged]).await.unwrap();
    let old = repository.metadata().snapshot().await.unwrap();
    drop(native);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    repository.collect().await.unwrap();
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&key)
            .await
            .unwrap()
            .is_none()
    );
    let receiving = repository.mutation_session().await.unwrap();
    *repository.metadata().stale.lock().unwrap() = Some(old);
    assert!(
        receiving.stage_git_blob_file(&key).await.is_err(),
        "a record collected before pin admission cannot authenticate live payload bytes"
    );
}

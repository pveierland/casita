#![cfg(all(feature = "native", feature = "experimental"))]

use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use casita::experimental::{
    CommitResult, MemoryBlobStore, MemoryMetadataStore, MetadataError, MetadataMutation,
    MetadataSnapshot, MetadataStore, ObjectKey, Repository, RepositoryError, RepositoryRevision,
    RootChange, RootName,
};

struct SnapshotCheckingStore {
    inner: MemoryMetadataStore,
    latest: Mutex<Option<Weak<dyn MetadataSnapshot>>>,
    commits: AtomicUsize,
    race_once: bool,
}

#[async_trait]
impl MetadataStore for SnapshotCheckingStore {
    async fn try_collection_lease(
        &self,
    ) -> Result<Option<casita::experimental::RepositoryLease>, MetadataError> {
        self.inner.try_collection_lease().await
    }
    fn coordinates_payload_catalog(&self) -> bool {
        self.inner.coordinates_payload_catalog()
    }
    async fn pin_store(
        &self,
    ) -> Result<
        std::sync::Arc<dyn casita::experimental::PinStore>,
        casita::experimental::MetadataError,
    > {
        self.inner.pin_store().await
    }

    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        let snapshot = self.inner.snapshot().await?;
        *self.latest.lock().unwrap() = Some(Arc::downgrade(&snapshot));
        Ok(snapshot)
    }

    async fn commit(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        assert!(
            self.latest
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .is_none(),
            "publication kept its validation snapshot alive during commit"
        );
        if self.commits.fetch_add(1, Ordering::SeqCst) == 0 && self.race_once {
            self.inner.commit(expected, MetadataMutation::new()).await?;
        }
        self.inner.commit(expected, mutation).await
    }
}

fn store(race_once: bool) -> Arc<SnapshotCheckingStore> {
    Arc::new(SnapshotCheckingStore {
        inner: MemoryMetadataStore::new().unwrap(),
        latest: Mutex::default(),
        commits: AtomicUsize::new(0),
        race_once,
    })
}

#[tokio::test]
async fn publication_releases_its_snapshot_before_commit_and_each_retry() {
    for race in [false, true] {
        let state = store(race);
        let repository = Repository::new(MemoryBlobStore::new(), state.clone());
        let independent_reader = state.snapshot().await.unwrap();
        let session = repository.mutation_session().await.unwrap();
        let staged = session.stage_blob(b"snapshot lifetime").await.unwrap();
        let key = staged.record().key().clone();
        let name = RootName::try_from("published").unwrap();
        session
            .publish_rooted(vec![staged], name.clone(), key.clone())
            .await
            .unwrap();
        assert_eq!(
            state.commits.load(Ordering::SeqCst),
            if race { 2 } else { 1 }
        );
        let snapshot = state.inner.snapshot().await.unwrap();
        assert_eq!(snapshot.root(&name).await.unwrap(), Some(key.clone()));
        assert!(snapshot.object(&key).await.unwrap().is_some());
        assert_eq!(independent_reader.root(&name).await.unwrap(), None);
        assert_eq!(independent_reader.object(&key).await.unwrap(), None);
    }
}

#[tokio::test]
async fn releasing_the_snapshot_preserves_exact_revision_conflicts() {
    let state = store(true);
    let repository = Repository::new(MemoryBlobStore::new(), state.clone());
    let expected = state.snapshot().await.unwrap().revision();
    let session = repository.mutation_session().await.unwrap();
    let staged = session
        .stage_blob(b"must remain unpublished")
        .await
        .unwrap();
    let key: ObjectKey = staged.record().key().clone();
    let name = RootName::try_from("conflicted").unwrap();
    let error = session
        .publish_at_revision(
            expected,
            vec![staged],
            vec![RootChange::Set {
                name: name.clone(),
                target: key.clone(),
            }],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RepositoryError::Metadata(MetadataError::StaleRevision { .. })
    ));
    assert_eq!(state.commits.load(Ordering::SeqCst), 1);
    let snapshot = state.inner.snapshot().await.unwrap();
    assert_eq!(snapshot.root(&name).await.unwrap(), None);
    assert_eq!(snapshot.object(&key).await.unwrap(), None);
}

#[path = "support/counting_blob_store.rs"]
mod counting_blob_store;

#[tokio::test]
async fn checked_closures_revalidate_after_a_refused_commit() {
    use casita::{Directory, Node, PathComponent};
    use std::collections::BTreeSet;
    let state = store(true);
    let reads = Arc::new(AtomicUsize::new(0));
    let repository = Repository::new(
        counting_blob_store::CountingBlobStore {
            inner: MemoryBlobStore::new(),
            reads: reads.clone(),
            writes: Arc::new(AtomicUsize::new(0)),
        },
        state.clone(),
    );
    let session = repository.mutation_session().await.unwrap();
    let child = Directory::new();
    let parent = Directory::try_from_iter([(
        PathComponent::try_from("child").unwrap(),
        Node::Directory {
            digest: child.digest(),
            size: child.size(),
        },
    )])
    .unwrap();
    let child = session.stage_directory(&child).await.unwrap();
    let parent = session.stage_directory(&parent).await.unwrap();
    let keys = BTreeSet::from([child.record().key().clone(), parent.record().key().clone()]);
    reads.store(0, Ordering::SeqCst);
    session
        .publish_closures(vec![child, parent], keys)
        .await
        .unwrap();
    assert_eq!(state.commits.load(Ordering::SeqCst), 2);
    assert_eq!(
        reads.load(Ordering::SeqCst),
        8,
        "each attempt must recheck both directories and the parent-child relation",
    );
}

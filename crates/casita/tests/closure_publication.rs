#![cfg(all(feature = "native", feature = "experimental"))]

use casita::experimental::{
    ClosureStatus, FormatLimits, FormatRegistry, MemoryBlobStore, MemoryMetadataStore,
    MetadataStore, Repository, RepositoryError,
};
use casita::{BlobId, Digest, Directory, Node, ObjectKey, PathComponent};
use futures::TryStreamExt;
use std::collections::BTreeSet;

#[tokio::test]
async fn unnamed_directory_publication_records_requested_closure_witnesses() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let session = repository.mutation_session().await.unwrap();
    let staged = session.stage_directory(&Directory::new()).await.unwrap();
    let key = staged.record().key().clone();
    session
        .publish_closures(vec![staged], BTreeSet::from([key.clone()]))
        .await
        .unwrap();
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&[key])
            .await
            .unwrap(),
        [true],
        "a checked unnamed directory needs a reusable closure witness"
    );
}

#[tokio::test]
async fn only_requested_directory_targets_are_marked_and_none_are_permanent_roots() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
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
    let child_key = child.record().key().clone();
    let parent = session.stage_directory(&parent).await.unwrap();
    let parent_key = parent.record().key().clone();
    session
        .publish_closures(vec![child, parent], BTreeSet::from([parent_key.clone()]))
        .await
        .unwrap();
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert_eq!(
        snapshot
            .validated_closures(&[child_key.clone(), parent_key.clone()])
            .await
            .unwrap(),
        [false, true]
    );
    assert!(
        snapshot
            .roots()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    drop(snapshot);
    assert_eq!(
        repository
            .try_collect()
            .await
            .unwrap()
            .removed
            .logical_objects,
        0
    );
    drop(session);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    assert_eq!(
        repository.collect().await.unwrap().removed.logical_objects,
        2
    );
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_payload_batch(&[child_key, parent_key])
            .await
            .unwrap(),
        [None, None]
    );
}

#[path = "support/counting_blob_store.rs"]
mod counting_blob_store;

#[tokio::test]
async fn existing_targets_acquire_reusable_witnesses_without_new_objects() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let reads = Arc::new(AtomicUsize::new(0));
    let repository = Repository::new(
        counting_blob_store::CountingBlobStore {
            inner: MemoryBlobStore::new(),
            reads: reads.clone(),
            writes: Arc::new(AtomicUsize::new(0)),
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let session = repository.mutation_session().await.unwrap();
    let directory = session.stage_directory(&Directory::new()).await.unwrap();
    let key = directory.record().key().clone();
    session.publish_unrooted(vec![directory]).await.unwrap();
    reads.store(0, Ordering::SeqCst);
    session
        .publish_closures(Vec::new(), BTreeSet::from([key.clone()]))
        .await
        .unwrap();
    assert!(
        reads.load(Ordering::SeqCst) > 0,
        "an unchecked directory needs normal verification"
    );
    reads.store(0, Ordering::SeqCst);
    session
        .publish_closures(Vec::new(), BTreeSet::from([key]))
        .await
        .unwrap();
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "a recorded witness stops subsequent traversal"
    );
}

#[tokio::test]
async fn missing_descendants_reject_every_record_and_witness_in_the_batch() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let session = repository.mutation_session().await.unwrap();
    let valid = session.stage_directory(&Directory::new()).await.unwrap();
    let valid_key = valid.record().key().clone();
    let missing = BlobId::new(Digest::hash(b"absent"));
    let broken = Directory::try_from_iter([(
        PathComponent::try_from("missing").unwrap(),
        Node::File {
            digest: missing,
            size: 6,
            executable: false,
        },
    )])
    .unwrap();
    let broken = session.stage_directory(&broken).await.unwrap();
    let broken_key = broken.record().key().clone();
    assert!(matches!(
        session
            .publish_closures(
                vec![valid, broken],
                BTreeSet::from([valid_key.clone(), broken_key.clone()])
            )
            .await,
        Err(RepositoryError::RootNotPublishable {
            status: ClosureStatus::Missing { .. },
            ..
        })
    ));
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert!(snapshot.object(&valid_key).await.unwrap().is_none());
    assert!(snapshot.object(&broken_key).await.unwrap().is_none());
    assert_eq!(
        snapshot
            .validated_closures(&[valid_key, broken_key])
            .await
            .unwrap(),
        [false, false]
    );
}

#[tokio::test]
async fn complete_records_with_invalid_directory_relations_are_rejected() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let session = repository.mutation_session().await.unwrap();
    let child = Directory::new();
    let wrong = Directory::try_from_iter([(
        PathComponent::try_from("child").unwrap(),
        Node::Directory {
            digest: child.digest(),
            size: 1,
        },
    )])
    .unwrap();
    let child = session.stage_directory(&child).await.unwrap();
    let child_key = child.record().key().clone();
    let wrong = session.stage_directory(&wrong).await.unwrap();
    let key = wrong.record().key().clone();
    assert!(matches!(
        session
            .publish_closures(vec![child, wrong], BTreeSet::from([child_key, key.clone()]))
            .await,
        Err(RepositoryError::RootNotPublishable {
            status: ClosureStatus::Invalid { .. },
            ..
        })
    ));
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
}

#[tokio::test]
async fn requested_targets_and_staged_objects_each_obey_batch_limits() {
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::builtin(),
        FormatLimits {
            max_batch_objects: 1,
            ..Default::default()
        },
    );
    let session = repository.mutation_session().await.unwrap();
    let first = session.stage_blob(b"first").await.unwrap();
    let second = session.stage_blob(b"second").await.unwrap();
    let keys = BTreeSet::from([first.record().key().clone(), second.record().key().clone()]);
    assert!(matches!(
        session.publish_closures(Vec::new(), keys).await,
        Err(RepositoryError::LimitExceeded(_))
    ));
    assert!(matches!(
        session
            .publish_closures(vec![first, second], BTreeSet::new())
            .await,
        Err(RepositoryError::LimitExceeded(_))
    ));
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .object(&ObjectKey::blob(BlobId::new(Digest::hash(b"first"))))
            .await
            .unwrap()
            .is_none()
    );
}

struct RejectingBlob(casita::experimental::BlobFormat);
#[async_trait::async_trait]
impl casita::experimental::ObjectFormat for RejectingBlob {
    fn namespace(&self) -> &casita::NamespaceId {
        self.0.namespace()
    }
    async fn verify(
        &self,
        context: casita::experimental::VerificationContext<'_>,
        limits: &FormatLimits,
    ) -> Result<casita::experimental::VerifiedObject, casita::experimental::FormatError> {
        self.0.verify(context, limits).await
    }
    async fn verify_links(
        &self,
        _context: casita::experimental::VerificationContext<'_>,
        _object: &casita::experimental::ObjectRecord,
        _links: &dyn casita::experimental::DirectLinkView,
        _limits: &FormatLimits,
    ) -> Result<(), casita::experimental::FormatError> {
        Err(casita::experimental::FormatError::InvalidPayload {
            namespace: self.namespace().clone(),
            message: "custom relation rejects closure".into(),
        })
    }
}

#[tokio::test]
async fn custom_format_relations_cannot_be_bypassed_by_checked_publication() {
    use std::sync::Arc;
    let formats =
        FormatRegistry::new([Arc::new(RejectingBlob(Default::default()))
            as Arc<dyn casita::experimental::ObjectFormat>])
        .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let session = repository.mutation_session().await.unwrap();
    let staged = session
        .stage_blob(b"valid body with a rejecting custom relation")
        .await
        .unwrap();
    let key = staged.record().key().clone();
    assert!(matches!(
        session
            .publish_closures(vec![staged], BTreeSet::from([key.clone()]))
            .await,
        Err(RepositoryError::RootNotPublishable {
            status: ClosureStatus::Invalid { .. },
            ..
        })
    ));
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert!(snapshot.object(&key).await.unwrap().is_none());
    assert_eq!(snapshot.validated_closures(&[key]).await.unwrap(), [false]);
}

#[tokio::test]
async fn checking_existing_targets_retains_their_entire_graph_in_a_new_session() {
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let seed = repository.mutation_session().await.unwrap();
    let file = seed
        .stage_blob(b"child kept by the checked parent")
        .await
        .unwrap();
    let file_key = file.record().key().clone();
    let tree = Directory::try_from_iter([(
        PathComponent::try_from("file").unwrap(),
        Node::File {
            digest: file.record().payload(),
            size: file.record().payload_size(),
            executable: false,
        },
    )])
    .unwrap();
    let tree = seed.stage_directory(&tree).await.unwrap();
    let key = tree.record().key().clone();
    seed.publish_unrooted(vec![file, tree]).await.unwrap();
    let previous = repository.owned_retention_hold().await.unwrap();
    drop(seed);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    let checked = repository.mutation_session().await.unwrap();
    checked
        .publish_closures(Vec::new(), BTreeSet::from([key.clone()]))
        .await
        .unwrap();
    drop(previous);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    assert_eq!(
        repository
            .try_collect()
            .await
            .unwrap()
            .removed
            .logical_objects,
        0
    );
    assert!(matches!(
        repository.verify_closure(&key).await.unwrap(),
        ClosureStatus::Complete { objects: 2 }
    ));
    drop(checked);
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    assert_eq!(
        repository.collect().await.unwrap().removed.logical_objects,
        2
    );
    let snapshot = repository.metadata().snapshot().await.unwrap();
    assert!(snapshot.object(&file_key).await.unwrap().is_none());
    assert_eq!(snapshot.validated_closures(&[key]).await.unwrap(), [false]);
}

#[tokio::test]
async fn postorder_targets_reuse_only_completed_closure_checks() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let reads = Arc::new(AtomicUsize::new(0));
    let repository = Repository::new(
        counting_blob_store::CountingBlobStore {
            inner: MemoryBlobStore::new(),
            reads: reads.clone(),
            writes: Arc::new(AtomicUsize::new(0)),
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let session = repository.mutation_session().await.unwrap();
    let mut directory = Directory::new();
    let mut staged = Vec::new();
    let mut targets = BTreeSet::new();
    const DEPTH: usize = 16;
    for _ in 0..DEPTH {
        let object = session.stage_directory(&directory).await.unwrap();
        targets.insert(object.record().key().clone());
        staged.push(object);
        directory = Directory::try_from_iter([(
            PathComponent::try_from("child").unwrap(),
            Node::Directory {
                digest: directory.digest(),
                size: directory.size(),
            },
        )])
        .unwrap();
    }
    reads.store(0, Ordering::SeqCst);
    session
        .publish_closures(staged, targets.clone())
        .await
        .unwrap();
    // Each directory needs its own relation check, including reading the
    // immediate child's declared size. Descendants already checked as targets
    // need no additional closure walk in this publication attempt.
    assert!(
        reads.load(Ordering::SeqCst) < 2 * DEPTH,
        "postorder closure checks reopened {} payloads for {DEPTH} directories",
        reads.load(Ordering::SeqCst),
    );
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&targets.into_iter().collect::<Vec<_>>())
            .await
            .unwrap()
            .into_iter()
            .all(|valid| valid)
    );
}

#[tokio::test]
async fn overlapping_root_changes_check_shared_descendants_once() {
    use casita::experimental::{RootChange, RootName};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let reads = Arc::new(AtomicUsize::new(0));
    let repository = Repository::new(
        counting_blob_store::CountingBlobStore {
            inner: MemoryBlobStore::new(),
            reads: reads.clone(),
            writes: Arc::new(AtomicUsize::new(0)),
        },
        MemoryMetadataStore::new().unwrap(),
    );
    let session = repository.mutation_session().await.unwrap();
    let mut directory = Directory::new();
    let mut staged = Vec::new();
    let mut roots = Vec::new();
    const DEPTH: usize = 16;
    for level in 0..DEPTH {
        let object = session.stage_directory(&directory).await.unwrap();
        roots.push(RootChange::Set {
            name: RootName::try_from(format!("level-{level}").as_str()).unwrap(),
            target: object.record().key().clone(),
        });
        staged.push(object);
        directory = Directory::try_from_iter([(
            PathComponent::try_from("child").unwrap(),
            Node::Directory {
                digest: directory.digest(),
                size: directory.size(),
            },
        )])
        .unwrap();
    }
    // Publish the outermost root first: its closure contains every later root.
    roots.reverse();
    reads.store(0, Ordering::SeqCst);
    session.publish(staged, roots.clone()).await.unwrap();
    // The first walk proves every directory. Later roots inside that closure
    // need no walk of their own in the same publication attempt.
    assert!(
        reads.load(Ordering::SeqCst) < 2 * DEPTH,
        "overlapping root checks reopened {} payloads for {DEPTH} directories",
        reads.load(Ordering::SeqCst),
    );
    let snapshot = repository.metadata().snapshot().await.unwrap();
    for change in roots {
        let RootChange::Set { name, target } = change else {
            unreachable!()
        };
        assert_eq!(snapshot.root(&name).await.unwrap(), Some(target.clone()));
        assert_eq!(
            snapshot
                .validated_closures(std::slice::from_ref(&target))
                .await
                .unwrap(),
            [true]
        );
    }
}

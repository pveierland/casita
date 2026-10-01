#![cfg(all(feature = "git", feature = "experimental"))]

use casita::experimental::{
    DirectLinkView, FormatError, FormatLimits, FormatRegistry, GitNativeObjectFormat,
    GitObjectFormat, GitObjectKind, MemoryBlobStore, MemoryMetadataStore, MetadataStore,
    ObjectFormat, Repository, VerificationContext, VerifiedObject, git_object_key_for_body,
};
use casita::import::GitClosureImport;
use casita::{NamespaceId, ObjectRecord};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct CheckedNative {
    inner: GitNativeObjectFormat,
    calls: Arc<AtomicUsize>,
    reject: bool,
}

#[async_trait::async_trait]
impl ObjectFormat for CheckedNative {
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
        context: VerificationContext<'_>,
        object: &ObjectRecord,
        links: &dyn DirectLinkView,
        limits: &FormatLimits,
    ) -> Result<(), FormatError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reject {
            Err(FormatError::InvalidPayload {
                namespace: self.namespace().clone(),
                message: "custom link verification rejected".into(),
            })
        } else {
            self.inner
                .verify_links(context, object, links, limits)
                .await
        }
    }
}

#[tokio::test]
async fn closure_import_does_not_bypass_registered_link_verification() {
    let calls = Arc::new(AtomicUsize::new(0));
    let formats = FormatRegistry::new([Arc::new(CheckedNative {
        inner: GitNativeObjectFormat::new(GitObjectFormat::Sha1, GitObjectKind::Blob),
        calls: calls.clone(),
        reject: true,
    }) as Arc<dyn ObjectFormat>])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let body = b"verified bytes with additional publication rules";
    let root = git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, body).unwrap();
    let session = repository.mutation_session().await.unwrap();
    let staged = session.stage_object(root.clone(), body).await.unwrap();
    session.publish_unrooted(vec![staged]).await.unwrap();
    let result = repository
        .import(GitClosureImport::new("/missing", [root.clone()]))
        .await;
    assert!(
        result.is_err(),
        "a body seal does not prove custom relational rules"
    );
    assert!(calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&[root])
            .await
            .unwrap(),
        [false]
    );
}

#[tokio::test]
async fn closure_import_reuses_a_proof_after_custom_link_verification() {
    let calls = Arc::new(AtomicUsize::new(0));
    let formats = FormatRegistry::new([Arc::new(CheckedNative {
        inner: GitNativeObjectFormat::new(GitObjectFormat::Sha1, GitObjectKind::Blob),
        calls: calls.clone(),
        reject: false,
    }) as Arc<dyn ObjectFormat>])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let body = b"verified bytes with additional publication rules";
    let root = git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, body).unwrap();
    let session = repository.mutation_session().await.unwrap();
    let staged = session.stage_object(root.clone(), body).await.unwrap();
    session.publish_unrooted(vec![staged]).await.unwrap();
    let result = repository
        .import(GitClosureImport::new("/missing", [root.clone()]))
        .await;
    let imported = result.unwrap();
    let checked = calls.load(Ordering::SeqCst);
    assert!(checked > 0);
    let warm = repository
        .import(GitClosureImport::new("/missing", [root.clone()]))
        .await
        .unwrap();
    assert_eq!(warm.report.imported_objects, 0);
    assert_eq!(calls.load(Ordering::SeqCst), checked);
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(&[root])
            .await
            .unwrap(),
        [true]
    );
    drop(imported);
}

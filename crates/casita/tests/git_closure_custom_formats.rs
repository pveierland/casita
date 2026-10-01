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
    verify_thread: Option<std::thread::ThreadId>,
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
        if let Some(expected) = self.verify_thread {
            assert_eq!(
                std::thread::current().id(),
                expected,
                "custom async verifiers must remain on their calling runtime"
            );
        }
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
        verify_thread: None,
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
        verify_thread: None,
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

#[tokio::test]
async fn parallel_source_workers_keep_custom_verifiers_on_the_async_runtime() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let source = tempfile::tempdir().unwrap();
    assert!(
        Command::new("git")
            .args(["init", "--bare", "--quiet"])
            .arg(source.path())
            .status()
            .unwrap()
            .success()
    );
    let mut roots = Vec::new();
    for i in 0..8u8 {
        let body = vec![i; 65536];
        let mut child = Command::new("git")
            .arg("-C")
            .arg(source.path())
            .args(["hash-object", "-w", "--stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&body).unwrap();
        assert!(child.wait().unwrap().success());
        roots.push(
            git_object_key_for_body(GitObjectFormat::Sha1, GitObjectKind::Blob, &body).unwrap(),
        );
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let formats = FormatRegistry::new([Arc::new(CheckedNative {
        inner: GitNativeObjectFormat::new(GitObjectFormat::Sha1, GitObjectKind::Blob),
        calls: calls.clone(),
        reject: false,
        verify_thread: Some(std::thread::current().id()),
    }) as Arc<dyn ObjectFormat>])
    .unwrap();
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits::default(),
    );
    let outcome = repository
        .import(
            GitClosureImport::new(source.path().join("objects"), roots)
                .with_decode_workers(4.try_into().unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(outcome.report.imported_objects, 8);
    assert!(calls.load(Ordering::SeqCst) >= 8);
}

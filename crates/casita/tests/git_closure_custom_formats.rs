#![cfg(all(feature = "git", feature = "experimental"))]

use casita::experimental::{
    ClosureStatus, DirectLinkView, FormatError, FormatLimits, FormatRegistry,
    GitNativeObjectFormat, GitObjectFormat, GitObjectKind, MemoryBlobStore, MemoryMetadataStore,
    MetadataStore, ObjectFormat, Repository, VerificationContext, VerifiedObject,
    git_object_key_for_body,
};
use casita::import::GitClosureImport;
use casita::{NamespaceId, ObjectKey, ObjectRecord};
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

/// Every native Git kind, each audited through a counting link verifier.
fn counting_registry(calls: &Arc<AtomicUsize>) -> FormatRegistry {
    FormatRegistry::new(
        [
            GitObjectKind::Blob,
            GitObjectKind::Tree,
            GitObjectKind::Commit,
            GitObjectKind::Tag,
        ]
        .into_iter()
        .map(|kind| {
            Arc::new(CheckedNative {
                inner: GitNativeObjectFormat::new(GitObjectFormat::Sha1, kind),
                calls: calls.clone(),
                reject: false,
                verify_thread: None,
            }) as Arc<dyn ObjectFormat>
        }),
    )
    .unwrap()
}

/// A packed linear SHA-1 history whose commits each add a distinct tree and
/// blob, so it holds exactly three objects per commit.
fn linear_history(commits: usize) -> (tempfile::TempDir, ObjectKey) {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let directory = tempfile::tempdir().unwrap();
    let git = |args: &[&str]| {
        let mut command = Command::new("git");
        command
            .arg("-C")
            .arg(directory.path())
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null");
        command
    };
    assert!(
        git(&["init", "--bare", "-q", "--object-format=sha1"])
            .status()
            .unwrap()
            .success()
    );
    let mut stream = Vec::new();
    for i in 0..commits {
        let message = format!("commit {i}\n");
        let contents = format!("contents {i}\n");
        write!(
            stream,
            "commit refs/heads/main\nmark :{}\ncommitter Casita <casita@example.com> 1700000000 +0000\ndata {}\n{message}",
            i + 1,
            message.len()
        )
        .unwrap();
        if i > 0 {
            writeln!(stream, "from :{i}").unwrap();
        }
        write!(
            stream,
            "M 100644 inline file\ndata {}\n{contents}\n",
            contents.len()
        )
        .unwrap();
    }
    let mut importer = git(&["fast-import", "--quiet"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    importer.stdin.take().unwrap().write_all(&stream).unwrap();
    assert!(importer.wait().unwrap().success());
    let head = git(&["rev-parse", "refs/heads/main"]).output().unwrap();
    assert!(head.status.success());
    let oid = String::from_utf8(head.stdout).unwrap();
    let root = casita::experimental::git_object_key(
        GitObjectFormat::Sha1,
        GitObjectKind::Commit,
        data_encoding::HEXLOWER
            .decode(oid.trim().as_bytes())
            .unwrap(),
    )
    .unwrap();
    (directory, root)
}

#[tokio::test]
async fn custom_registries_audit_each_object_of_a_history_once() {
    const COMMITS: usize = 32;
    let (source, root) = linear_history(COMMITS);
    // One witness batch holding every commit, and six smaller batches.
    for max_batch_objects in [4_096, 16] {
        let calls = Arc::new(AtomicUsize::new(0));
        let repository = Repository::with_formats(
            MemoryBlobStore::new(),
            MemoryMetadataStore::new().unwrap(),
            counting_registry(&calls),
            FormatLimits {
                max_batch_objects,
                ..Default::default()
            },
        );
        let request = GitClosureImport::new(source.path().join("objects"), [root.clone()]);
        let imported = repository.import(request.clone()).await.unwrap();
        assert_eq!(imported.report.imported_objects, 3 * COMMITS);
        // Each commit's closure contains every older commit. Auditing each
        // witness target separately would repeat those walks quadratically.
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3 * COMMITS,
            "{max_batch_objects}-object batches must audit each object exactly once"
        );
        let warm = repository.import(request).await.unwrap();
        assert_eq!(warm.report.imported_objects, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 3 * COMMITS);
        assert_eq!(
            repository.verify_closure(&root).await.unwrap(),
            ClosureStatus::Complete {
                objects: 3 * COMMITS
            }
        );
    }
}

/// Imports a linear history once, timing the cold import and recording how
/// many custom link audits it needed. Warm reuse and the exhaustive closure
/// audit are correctness gates outside the timed region.
#[tokio::test]
#[ignore = "run through benchmark run git-closure-audit"]
async fn benchmark_git_closure_audit() {
    let variable = |name: &str| std::env::var(name).unwrap();
    let commits: usize = variable("CASITA_GIT_AUDIT_COMMITS").parse().unwrap();
    let registry = variable("CASITA_GIT_AUDIT_REGISTRY");
    let max_batch_objects: usize = variable("CASITA_GIT_AUDIT_BATCH_OBJECTS").parse().unwrap();
    let (source, root) = linear_history(commits);
    let calls = Arc::new(AtomicUsize::new(0));
    let formats = match registry.as_str() {
        "builtin" => FormatRegistry::builtin(),
        "custom" => counting_registry(&calls),
        other => panic!("unknown registry {other}"),
    };
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        formats,
        FormatLimits {
            max_batch_objects,
            ..Default::default()
        },
    );
    let request = GitClosureImport::new(source.path().join("objects"), [root.clone()]);
    let start = std::time::Instant::now();
    let imported = repository.import(request.clone()).await.unwrap();
    let nanos = start.elapsed().as_nanos();
    let link_audits = calls.load(Ordering::SeqCst);
    let objects = 3 * commits;
    assert_eq!(imported.report.imported_objects, objects);
    assert_eq!(imported.report.reused_objects, 0);
    let warm = repository.import(request).await.unwrap();
    assert_eq!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { objects }
    );
    println!(
        "git_closure_audit_sample {}",
        serde_json::json!({
            "commits": commits, "registry": registry,
            "publication_batch_objects": max_batch_objects, "objects": objects,
            "imported_objects": imported.report.imported_objects,
            "reused_objects": imported.report.reused_objects,
            "warm_imported_objects": warm.report.imported_objects,
            "warm_source_bytes": warm.report.source_bytes,
            "link_audits": link_audits, "wall_nanos": nanos, "root": root.to_string(),
            "correctness": "exact import counts, exhaustive closure verification and source-free warm reuse"
        })
    );
}

#[tokio::test]
async fn parallel_source_workers_keep_custom_verifiers_on_the_async_runtime() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let source = tempfile::tempdir().unwrap();
    assert!(
        Command::new("git")
            .args(["init", "--bare", "--quiet", "--object-format=sha1"])
            .arg(source.path())
            .status()
            .unwrap()
            .success()
    );
    let mut roots = Vec::new();
    for i in 0..8u8 {
        // Exercise both buffered and streamed source bodies under the custom registry.
        let size = if i % 2 == 0 { 1024 * 1024 } else { 65536 };
        let body = vec![i; size];
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

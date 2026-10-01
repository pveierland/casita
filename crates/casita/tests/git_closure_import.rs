#![cfg(all(feature = "git", feature = "experimental"))]

use std::io::Write;
use std::process::{Command, Stdio};

use casita::ObjectKey;
use casita::experimental::{
    ClosureStatus, GitObjectFormat, GitObjectKind, MemoryBlobStore, MemoryMetadataStore,
    MetadataStore, Repository, git_object_key,
};
use casita::import::GitClosureImport;

struct Source(tempfile::TempDir);
impl Source {
    fn new(format: &str) -> Self {
        let source = Self(tempfile::tempdir().unwrap());
        source.git(
            &["init", "--bare", "-q", &format!("--object-format={format}")],
            b"",
        );
        source
    }
    fn git(&self, args: &[&str], input: &[u8]) -> String {
        let mut child = Command::new("git")
            .arg("-C")
            .arg(self.0.path())
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
    fn blob(&self, bytes: &[u8]) -> String {
        self.git(&["hash-object", "-w", "--stdin"], bytes)
    }
    fn tree(&self, entries: &str) -> String {
        self.git(&["mktree", "--missing"], entries.as_bytes())
    }
    fn remove(&self, oid: &str) {
        std::fs::remove_file(
            self.0
                .path()
                .join("objects")
                .join(&oid[..2])
                .join(&oid[2..]),
        )
        .unwrap();
    }
    fn request(&self, roots: Vec<ObjectKey>) -> GitClosureImport {
        GitClosureImport::new(self.0.path().join("objects"), roots)
    }
}
fn key(format: GitObjectFormat, kind: GitObjectKind, oid: &str) -> ObjectKey {
    git_object_key(
        format,
        kind,
        data_encoding::HEXLOWER.decode(oid.as_bytes()).unwrap(),
    )
    .unwrap()
}
fn tree(oid: &str) -> ObjectKey {
    key(GitObjectFormat::Sha1, GitObjectKind::Tree, oid)
}

#[tokio::test]
async fn imports_only_selected_closures_and_repeats_without_source_access() {
    let source = Source::new("sha1");
    let blob = source.blob(b"selected");
    let unused = source.blob(b"unselected");
    let root = tree(&source.tree(&format!("100644 blob {blob}\tfile\n")));
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let imported = repository
        .import(source.request(vec![root.clone()]))
        .await
        .unwrap();
    assert_eq!(imported.report.imported_objects, 2);
    assert_eq!(imported.report.reused_objects, 0);
    assert!(
        imported
            .reader
            .object(&key(GitObjectFormat::Sha1, GitObjectKind::Blob, &unused))
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { objects: 2 }
    ));
    let warm = repository
        .import(GitClosureImport::new(
            source.0.path().join("nonexistent"),
            [root.clone()],
        ))
        .await
        .unwrap();
    assert_eq!(warm.report.imported_objects, 0);
    assert_eq!(warm.report.reused_objects, 1);
    assert_eq!(warm.report.source_bytes, 0);
    use futures::TryStreamExt;
    assert!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .roots()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    drop(imported);
    repository.collect().await.unwrap();
    let (_, mut payload) = warm
        .reader
        .open_payload(&key(GitObjectFormat::Sha1, GitObjectKind::Blob, &blob))
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut payload, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, b"selected");
}

#[tokio::test]
async fn reuses_subtrees_across_unrelated_roots_without_reading_old_source_objects() {
    let source = Source::new("sha1");
    let blob = source.blob(b"shared contents");
    let shared = source.tree(&format!("100644 blob {blob}\tfile\n"));
    let root1 = tree(&source.tree(&format!("040000 tree {shared}\tfirst\n")));
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let first = repository
        .import(source.request(vec![root1]))
        .await
        .unwrap();
    assert_eq!(first.report.imported_objects, 3);
    source.remove(&blob);
    source.remove(&shared);
    let new = source.blob(b"new contents");
    let root2 = tree(&source.tree(&format!(
        "040000 tree {shared}\trenamed\n100755 blob {new}\tnew\n"
    )));
    let second = repository
        .import(source.request(vec![root2.clone()]))
        .await
        .unwrap();
    assert_eq!(second.report.imported_objects, 2);
    assert_eq!(second.report.reused_objects, 1);
    assert!(matches!(
        repository.verify_closure(&root2).await.unwrap(),
        ClosureStatus::Complete { objects: 4 }
    ));
}

#[tokio::test]
async fn an_existing_parent_is_not_a_complete_closure_and_can_be_repaired() {
    use casita::experimental::MetadataStore;
    let source = Source::new("sha1");
    let blob = source.blob(b"missing child");
    let oid = source.tree(&format!("100644 blob {blob}\tfile\n"));
    let root = tree(&oid);
    // Read binary tree bodies without the textual fixture helper.
    let bytes = Command::new("git")
        .arg("-C")
        .arg(source.0.path())
        .args(["cat-file", "tree", &oid])
        .output()
        .unwrap()
        .stdout;
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let mutation = repository.mutation_session().await.unwrap();
    let staged = mutation.stage_object(root.clone(), &bytes).await.unwrap();
    mutation.publish_unrooted(vec![staged]).await.unwrap();
    source.remove(&oid);
    source.remove(&blob);
    assert!(
        repository
            .import(source.request(vec![root.clone()]))
            .await
            .is_err()
    );
    assert_eq!(
        repository
            .metadata()
            .snapshot()
            .await
            .unwrap()
            .validated_closures(std::slice::from_ref(&root))
            .await
            .unwrap(),
        [false]
    );
    assert_eq!(source.blob(b"missing child"), blob);
    let repaired = repository
        .import(source.request(vec![root.clone()]))
        .await
        .unwrap();
    assert_eq!(repaired.report.imported_objects, 1);
    assert!(matches!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { objects: 2 }
    ));
}

#[tokio::test]
async fn supports_sha256_and_rejects_wrong_types() {
    let source = Source::new("sha256");
    let blob = source.blob(b"sha256 data");
    let root = key(
        GitObjectFormat::Sha256,
        GitObjectKind::Tree,
        &source.tree(&format!("100644 blob {blob}\tfile\n")),
    );
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let imported = repository
        .import(source.request(vec![root.clone()]))
        .await
        .unwrap();
    assert_eq!(imported.report.imported_objects, 2);
    assert!(matches!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { objects: 2 }
    ));
    let wrong = key(GitObjectFormat::Sha256, GitObjectKind::Tree, &blob);
    assert!(
        repository
            .import(source.request(vec![wrong]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn closure_witnesses_survive_reopening_a_local_repository() {
    let source = Source::new("sha1");
    let blob = source.blob(b"persistent contents");
    let root = tree(&source.tree(&format!("100644 blob {blob}\tfile\n")));
    let destination = tempfile::tempdir().unwrap();
    {
        let repository = Repository::local(destination.path()).await.unwrap();
        let imported = repository
            .import(source.request(vec![root.clone()]))
            .await
            .unwrap();
        let session = repository.mutation_session().await.unwrap();
        session
            .publish_rooted(Vec::new(), "selected".try_into().unwrap(), root.clone())
            .await
            .unwrap();
        drop(imported);
        drop(session);
        repository.flush().await.unwrap();
    }
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
    let repository = Repository::local(destination.path()).await.unwrap();
    let imported = repository
        .import(GitClosureImport::new(
            source.0.path().join("missing"),
            [root],
        ))
        .await
        .unwrap();
    assert_eq!(imported.report.imported_objects, 0);
    assert_eq!(imported.report.reused_objects, 1);
    assert_eq!(imported.report.source_bytes, 0);
}

/// Permanent workload: benchmark run git-closure-import. Correctness audits
/// deliberately run outside the timed region.
#[tokio::test]
#[ignore = "run through benchmark run git-closure-import"]
async fn benchmark_git_closure_import() {
    use casita::experimental::{FormatLimits, SpillLimits};
    let backend = std::env::var("CASITA_GIT_CLOSURE_BACKEND").unwrap_or("memory".into());
    if backend == "local" {
        let destination = tempfile::tempdir().unwrap();
        let repository = Repository::local(destination.path())
            .await
            .unwrap()
            .with_spill_limits(SpillLimits {
                max_memory_objects: 64,
                ..Default::default()
            });
        run_git_closure_benchmark(&repository).await;
        repository.flush().await.unwrap();
    } else {
        assert_eq!(backend, "memory");
        let repository = Repository::with_formats(
            MemoryBlobStore::new(),
            MemoryMetadataStore::new().unwrap(),
            casita::experimental::FormatRegistry::builtin(),
            FormatLimits {
                max_batch_objects: 64,
                ..Default::default()
            },
        )
        .with_spill_limits(SpillLimits {
            max_memory_objects: 64,
            ..Default::default()
        });
        run_git_closure_benchmark(&repository).await;
    }
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
}

async fn run_git_closure_benchmark<PS: casita::experimental::BlobStore, SS: MetadataStore>(
    repository: &Repository<PS, SS>,
) {
    let backend = std::env::var("CASITA_GIT_CLOSURE_BACKEND").unwrap_or("memory".into());
    let file_bytes: usize = std::env::var("CASITA_GIT_CLOSURE_FILE_BYTES")
        .unwrap_or("1024".into())
        .parse()
        .unwrap();
    let concurrency: usize = std::env::var("CASITA_GIT_CLOSURE_CONCURRENCY")
        .unwrap_or("16".into())
        .parse()
        .unwrap();
    let decode_workers: usize = std::env::var("CASITA_GIT_CLOSURE_DECODE_WORKERS")
        .unwrap_or("1".into())
        .parse()
        .unwrap();
    let content = std::env::var("CASITA_GIT_CLOSURE_CONTENT").unwrap_or("repeated".into());
    let count: usize = std::env::var("CASITA_GIT_CLOSURE_FILES")
        .unwrap()
        .parse()
        .unwrap();
    let budget: u64 = std::env::var("CASITA_GIT_CLOSURE_BYTES")
        .unwrap()
        .parse()
        .unwrap();
    let packed = std::env::var("CASITA_GIT_CLOSURE_PACKED").unwrap() == "1";
    let source = Source::new("sha1");
    let mut entries = String::new();
    for i in 0..count {
        let size = if content == "mixed" && i % 16 != 0 {
            1024
        } else {
            file_bytes
        };
        let mut body = vec![b'x'; size];
        if content == "random" || content == "mixed" {
            // Stable incompressible bytes, independent of the importer and its hashes.
            let mut state = (i as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
            for part in body.chunks_mut(8) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                part.copy_from_slice(&state.to_le_bytes()[..part.len()]);
            }
        }
        body[..8].copy_from_slice(&(i as u64).to_le_bytes());
        let blob = source.blob(&body);
        entries.push_str(&format!("100644 blob {blob}\tfile{i:08}\n"));
    }
    let subtree = source.tree(&entries);
    let first = tree(&source.tree(&format!("040000 tree {subtree}\tshared\n")));
    // Keep packing independent of commit history: Git permits tree-valued
    // refs outside refs/heads, and repack includes their reachable objects.
    if packed {
        let (_, _, oid) = casita::experimental::git_key_parts(&first).unwrap();
        source.git(
            &[
                "update-ref",
                "refs/benchmark/base",
                &data_encoding::HEXLOWER.encode(oid),
            ],
            b"",
        );
        source.git(&["repack", "-adf", "--window=16"], b"");
    }
    let mut holds = Vec::new();
    for operation in ["cold", "warm", "subtree-delta", "wide-delta"] {
        let root = match operation {
            "cold" | "warm" => first.clone(),
            "subtree-delta" => {
                let blob = source.blob(b"new leaf");
                tree(&source.tree(&format!(
                    "040000 tree {subtree}\trenamed\n100644 blob {blob}\tnew\n"
                )))
            }
            "wide-delta" => {
                let blob = source.blob(b"wide new leaf");
                tree(&source.tree(&format!("{entries}100644 blob {blob}\tnew\n")))
            }
            _ => unreachable!(),
        };
        let request = source
            .request(vec![root.clone()])
            .with_max_buffered_bytes(budget.try_into().unwrap())
            .with_concurrency(concurrency.try_into().unwrap())
            .with_decode_workers(decode_workers.try_into().unwrap());
        let start = std::time::Instant::now();
        let imported = repository.import(request).await.unwrap();
        let nanos = start.elapsed().as_nanos();
        let (new, reused, reachable) = match operation {
            "cold" => (count + 2, 0, count + 2),
            "warm" => (0, 1, count + 2),
            "subtree-delta" => (2, 1, count + 3),
            "wide-delta" => (2, count, count + 2),
            _ => unreachable!(),
        };
        assert_eq!(imported.report.imported_objects, new);
        assert_eq!(imported.report.reused_objects, reused);
        assert_eq!(
            repository.verify_closure(&root).await.unwrap(),
            ClosureStatus::Complete { objects: reachable }
        );
        println!(
            "git_closure_sample {}",
            serde_json::json!({
                "operation": operation, "files": count, "packed": packed,
                "backend": backend, "file_bytes": file_bytes, "content": content,
                "concurrency": concurrency, "publication_batch_objects": repository.limits().max_batch_objects,
                "max_buffered_bytes": budget, "wall_nanos": nanos,
                "imported_objects": imported.report.imported_objects,
                "reused_objects": imported.report.reused_objects,
                "source_bytes": imported.report.source_bytes,
                "decode_workers": decode_workers,
                "peak_source_bytes": imported.report.peak_source_bytes,
                "peak_decode_workers": imported.report.peak_decode_workers,
                "root": root.to_string(),
                "correctness": "exact imported/reused counts and exhaustive closure verification"
            })
        );
        holds.push(imported);
    }
}

#[tokio::test]
async fn packed_alternates_import_tag_and_commit_closures_without_following_gitlinks() {
    let source = Source::new("sha1");
    let blob = source.blob(b"packed alternate content");
    let gitlink = "1111111111111111111111111111111111111111";
    let tree_oid = source.tree(&format!(
        "100644 blob {blob}\tfile\n160000 commit {gitlink}\tsubmodule\n"
    ));
    let commit = source.git(
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit-tree",
            &tree_oid,
        ],
        b"commit\n",
    );
    let tag = source.git(&["mktag"], format!("object {commit}\ntype commit\ntag v1\ntagger Test <test@example.com> 1 +0000\n\nrelease\n").as_bytes());
    source.git(&["update-ref", "refs/tags/v1", &tag], b"");
    source.git(&["repack", "-adf", "--window=16"], b"");
    let alternate = Source::new("sha1");
    std::fs::write(
        alternate.0.path().join("objects/info/alternates"),
        format!("{}\n", source.0.path().join("objects").display()),
    )
    .unwrap();
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let root = key(GitObjectFormat::Sha1, GitObjectKind::Tag, &tag);
    let imported = repository
        .import(alternate.request(vec![root.clone()]))
        .await
        .unwrap();
    assert_eq!(imported.report.imported_objects, 4);
    assert_eq!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { objects: 4 }
    );
    assert!(
        imported
            .reader
            .object(&key(GitObjectFormat::Sha1, GitObjectKind::Commit, gitlink))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn failed_import_checkpoints_never_claim_an_incomplete_tree_is_validated() {
    use casita::experimental::{FormatLimits, FormatRegistry};
    let source = Source::new("sha1");
    let blob = source.blob(b"temporarily missing");
    let tree_oid = source.tree(&format!("100644 blob {blob}\tfile\n"));
    let root = tree(&tree_oid);
    source.remove(&blob);
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::builtin(),
        FormatLimits {
            max_batch_objects: 1,
            ..Default::default()
        },
    );
    assert!(
        repository
            .import(source.request(vec![root.clone()]))
            .await
            .is_err()
    );
    let hold = repository.owned_retention_hold().await.unwrap();
    assert!(
        hold.object(&root).await.unwrap().is_some(),
        "the parent was checkpointed before its missing child was discovered"
    );
    assert_eq!(
        hold.snapshot()
            .validated_closures(std::slice::from_ref(&root))
            .await
            .unwrap(),
        [false]
    );
    assert_eq!(source.blob(b"temporarily missing"), blob);
    source.remove(&tree_oid);
    let resumed = repository
        .import(source.request(vec![root.clone()]))
        .await
        .unwrap();
    assert_eq!(resumed.report.imported_objects, 1);
    assert_eq!(
        repository.verify_closure(&root).await.unwrap(),
        ClosureStatus::Complete { objects: 2 }
    );
}

#[tokio::test]
async fn parallel_decode_preserves_closures_and_bounds_each_window() {
    for format in ["sha1", "sha256"] {
        let source = Source::new(format);
        let hash = if format == "sha1" {
            GitObjectFormat::Sha1
        } else {
            GitObjectFormat::Sha256
        };
        let mut entries = String::new();
        for i in 0..12u8 {
            let oid = source.blob(&vec![i; 128 * 1024]);
            entries.push_str(&format!("100644 blob {oid}\tfile{i:02}\n"));
        }
        let root = key(hash, GitObjectKind::Tree, &source.tree(&entries));
        for workers in [1, 2, 4, 8] {
            for budget in [128 * 1024 - 1, 128 * 1024, 256 * 1024] {
                let repository =
                    Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
                let imported = repository
                    .import(
                        source
                            .request(vec![root.clone()])
                            .with_decode_workers(workers.try_into().unwrap())
                            .with_max_buffered_bytes((budget as u64).try_into().unwrap()),
                    )
                    .await
                    .unwrap();
                assert_eq!(imported.report.imported_objects, 13);
                assert!(imported.report.peak_source_bytes <= budget.max(128 * 1024) as u64);
                assert!(imported.report.peak_decode_workers <= workers);
                if budget <= 128 * 1024 {
                    assert_eq!(imported.report.peak_decode_workers, 1);
                }
                assert_eq!(
                    repository.verify_closure(&root).await.unwrap(),
                    ClosureStatus::Complete { objects: 13 }
                );
                let warm = repository
                    .import(
                        GitClosureImport::new("/absent", [root.clone()])
                            .with_decode_workers(workers.try_into().unwrap()),
                    )
                    .await
                    .unwrap();
                assert_eq!(warm.report.imported_objects, 0);
                assert_eq!(warm.report.peak_decode_workers, 0);
                assert_eq!(warm.report.peak_source_bytes, 0);
            }
        }
    }
}

#[tokio::test]
async fn parallel_native_verification_rejects_corrupt_source_identity() {
    let source = Source::new("sha1");
    let original = source.blob(&vec![b'x'; 65536]);
    let replacement = source.blob(&vec![b'y'; 65536]);
    let objects = source.0.path().join("objects");
    // Git writes loose objects read-only; replace this private fixture entry.
    std::fs::remove_file(objects.join(&original[..2]).join(&original[2..])).unwrap();
    std::fs::copy(
        objects.join(&replacement[..2]).join(&replacement[2..]),
        objects.join(&original[..2]).join(&original[2..]),
    )
    .unwrap();
    let root = key(GitObjectFormat::Sha1, GitObjectKind::Blob, &original);
    let sibling = key(GitObjectFormat::Sha1, GitObjectKind::Blob, &replacement);
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    assert!(
        repository
            .import(
                source
                    .request(vec![root.clone(), sibling])
                    .with_decode_workers(4.try_into().unwrap())
            )
            .await
            .is_err()
    );
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

#[test]
fn parallel_windows_progress_with_one_blocking_thread() {
    let source = Source::new("sha1");
    let roots = (0..8u8)
        .map(|i| {
            key(
                GitObjectFormat::Sha1,
                GitObjectKind::Blob,
                &source.blob(&vec![i; 32768]),
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
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                repository.import(
                    source
                        .request(roots)
                        .with_decode_workers(8.try_into().unwrap())
                        .with_max_buffered_bytes(65536.try_into().unwrap()),
                ),
            )
            .await
            .expect("source workers must not occupy a blocking thread waiting for storage")
            .unwrap();
            assert_eq!(result.report.imported_objects, 8);
            assert_eq!(result.report.peak_decode_workers, 1);
            drop(result);
            repository.flush().await.unwrap();
        });
}

#[tokio::test]
async fn excessive_worker_requests_are_bounded_by_the_metadata_frontier() {
    use casita::experimental::{FormatLimits, FormatRegistry};
    let source = Source::new("sha1");
    let root = tree(&source.tree(""));
    let repository = Repository::with_formats(
        MemoryBlobStore::new(),
        MemoryMetadataStore::new().unwrap(),
        FormatRegistry::builtin(),
        FormatLimits {
            max_batch_objects: usize::MAX,
            ..Default::default()
        },
    );
    let imported = repository
        .import(
            source
                .request(vec![root])
                .with_decode_workers(usize::MAX.try_into().unwrap())
                .with_concurrency(usize::MAX.try_into().unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(imported.report.imported_objects, 1);
    assert_eq!(imported.report.peak_decode_workers, 1);
}

#[tokio::test]
async fn an_oversized_source_body_does_not_admit_an_empty_sibling() {
    let source = Source::new("sha1");
    let empty = source.blob(b"");
    let mut large = source.blob(&vec![0; 4 * 1024 * 1024]);
    for byte in 1..=255u8 {
        if large < empty {
            break;
        }
        large = source.blob(&vec![byte; 4 * 1024 * 1024]);
    }
    assert!(
        large < empty,
        "the oversized object must sort before its empty sibling"
    );
    let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
    let imported = repository
        .import(
            source
                .request(vec![
                    key(GitObjectFormat::Sha1, GitObjectKind::Blob, &large),
                    key(GitObjectFormat::Sha1, GitObjectKind::Blob, &empty),
                ])
                .with_decode_workers(2.try_into().unwrap())
                .with_max_buffered_bytes(1.try_into().unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(imported.report.imported_objects, 2);
    assert_eq!(imported.report.peak_source_bytes, 4 * 1024 * 1024);
    assert_eq!(
        imported.report.peak_decode_workers, 1,
        "oversized bodies run alone, including zero-byte siblings"
    );
}

struct FalseDigestStore(MemoryBlobStore);

#[async_trait::async_trait]
impl casita::experimental::BlobStore for FalseDigestStore {
    fn write_scope(&self) -> casita::experimental::BackendWriteScope {
        self.0.write_scope()
    }
    fn begin_pinned_batch(
        &self,
        pin: casita::experimental::DataPinLease,
    ) -> Result<casita::experimental::BlobBatchGuard, casita::experimental::Error> {
        self.0.begin_pinned_batch(pin)
    }
    fn publication(&self) -> casita::experimental::PayloadPublication<'_> {
        self.0.publication()
    }
    async fn has(&self, id: &casita::BlobId) -> Result<bool, casita::experimental::Error> {
        self.0.has(id).await
    }
    async fn open_read(
        &self,
        id: &casita::BlobId,
    ) -> Result<Option<Box<dyn casita::experimental::BlobReader>>, casita::experimental::Error>
    {
        self.0.open_read(id).await
    }
    async fn open_write(&self) -> Box<dyn casita::experimental::BlobWriter> {
        self.0.open_write().await
    }
    async fn put_slice(&self, bytes: &[u8]) -> Result<casita::BlobId, casita::experimental::Error> {
        self.0.put_slice(bytes).await?;
        Ok(casita::BlobId::new(casita::Digest::hash(
            b"false backend digest",
        )))
    }
}

#[tokio::test]
async fn worker_seals_reject_false_backend_digests_before_publication() {
    use casita::experimental::{GitClosureImportError, RepositoryError};
    let source = Source::new("sha1");
    let oid = source.blob(b"verified source bytes");
    let root = key(GitObjectFormat::Sha1, GitObjectKind::Blob, &oid);
    let repository = Repository::new(
        FalseDigestStore(MemoryBlobStore::new()),
        MemoryMetadataStore::new().unwrap(),
    );
    let result = repository
        .import(
            source
                .request(vec![root.clone()])
                .with_decode_workers(std::num::NonZeroUsize::new(2).unwrap()),
        )
        .await;
    assert!(matches!(
        result,
        Err(GitClosureImportError::Repository(
            RepositoryError::PayloadIdentityMismatch { .. }
        ))
    ));
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

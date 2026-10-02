#![recursion_limit = "256"]
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
    let imports: usize = std::env::var("CASITA_GIT_CLOSURE_IMPORTS")
        .unwrap_or("1".into())
        .parse()
        .unwrap();
    assert!(imports > 0);
    if backend == "local" {
        let destinations: Vec<_> = (0..imports).map(|_| tempfile::tempdir().unwrap()).collect();
        let mut repositories = Vec::new();
        for destination in &destinations {
            let mut repository = Repository::local(destination.path())
                .await
                .unwrap()
                .with_spill_limits(SpillLimits {
                    max_memory_objects: 64,
                    ..Default::default()
                });
            let chunk_concurrency: usize = std::env::var("CASITA_GIT_CLOSURE_CHUNK_CONCURRENCY")
                .unwrap_or("32".into())
                .parse()
                .unwrap();
            repository =
                repository.with_chunk_upload_concurrency(chunk_concurrency.try_into().unwrap());
            repositories.push(repository);
        }
        run_git_closure_benchmark(&repositories).await;
        for repository in &repositories {
            repository.flush().await.unwrap();
        }
    } else {
        assert_eq!(backend, "memory");
        let repositories: Vec<_> = (0..imports)
            .map(|_| {
                Repository::with_formats(
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
                })
            })
            .collect();
        run_git_closure_benchmark(&repositories).await;
    }
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
}

async fn run_git_closure_benchmark<PS: casita::experimental::BlobStore, SS: MetadataStore>(
    repositories: &[Repository<PS, SS>],
) {
    let source_buffer_bytes: usize = std::env::var("CASITA_GIT_CLOSURE_SOURCE_BUFFER_BYTES")
        .unwrap_or("0".into())
        .parse()
        .unwrap();
    let destination_buffer_bytes: usize =
        std::env::var("CASITA_GIT_CLOSURE_DESTINATION_BUFFER_BYTES")
            .unwrap_or("0".into())
            .parse()
            .unwrap();
    assert_eq!(source_buffer_bytes == 0, destination_buffer_bytes == 0);
    let chunk_concurrency: usize = std::env::var("CASITA_GIT_CLOSURE_CHUNK_CONCURRENCY")
        .unwrap_or("32".into())
        .parse()
        .unwrap();
    let imports = repositories.len();
    let shared_cpu_limit: usize = std::env::var("CASITA_GIT_CLOSURE_SHARED_CPU_LIMIT")
        .unwrap_or("0".into())
        .parse()
        .unwrap();
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
    let delta_spilling = std::env::var("CASITA_GIT_CLOSURE_DELTA_SPILL").as_deref() == Ok("1");
    let cpu_metrics = std::env::var("CASITA_GIT_CLOSURE_CPU_METRICS").as_deref() == Ok("1");
    let delta_metrics = std::env::var("CASITA_GIT_CLOSURE_DELTA_METRICS").as_deref() == Ok("1");
    let mut fixture_blob_deltas = 0;
    let bounded = std::env::var("CASITA_GIT_CLOSURE_BOUNDED_FIXTURE").as_deref() == Ok("1");
    let pack_window: usize = std::env::var("CASITA_GIT_CLOSURE_PACK_WINDOW")
        .unwrap_or("16".into())
        .parse()
        .unwrap();
    let mut expected = Vec::new();
    let source = Source::new("sha1");
    let mut entries = String::new();
    for i in 0..count {
        let size = if content == "mixed" && i % 16 != 0 {
            1024
        } else {
            file_bytes
        };
        let blob = if bounded {
            let (oid, hash) = bounded_fixture::blob(&source, size, i, &content);
            expected.push(bounded_fixture::expected(&oid, hash, size));
            oid
        } else {
            let mut body = vec![b'x'; size];
            if content == "random" || content == "mixed" || content == "clustered" {
                // Stable incompressible bytes, independent of the importer and its hashes.
                let family = if content == "clustered" { i % 8 } else { i };
                let mut state = (family as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
                for part in body.chunks_mut(8) {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    part.copy_from_slice(&state.to_le_bytes()[..part.len()]);
                }
            }
            body[..8].copy_from_slice(&(i as u64).to_le_bytes());
            source.blob(&body)
        };
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
        source.git(&["repack", "-adf", &format!("--window={pack_window}")], b"");
        if delta_metrics
            || delta_spilling
            || (content == "clustered" && count > 8 && file_bytes >= 65536)
        {
            fixture_blob_deltas = std::fs::read_dir(source.0.path().join("objects/pack"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "idx"))
                .map(|path| {
                    source
                        .git(&["verify-pack", "-v", path.to_str().unwrap()], b"")
                        .lines()
                        .filter(|line| {
                            let fields: Vec<_> = line.split_whitespace().collect();
                            fields.len() == 7 && fields[1] == "blob"
                        })
                        .count()
                })
                .sum();
            if content == "clustered" && count > 8 && file_bytes >= 65536 {
                assert!(
                    fixture_blob_deltas > 0,
                    "clustered packed fixture must contain blob deltas"
                );
            }
        }
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
        let mut request = source
            .request(vec![root.clone()])
            .with_max_buffered_bytes(budget.try_into().unwrap())
            .with_concurrency(concurrency.try_into().unwrap())
            .with_decode_workers(decode_workers.try_into().unwrap())
            .with_delta_spilling(delta_spilling);
        if let Some(limit) = std::num::NonZeroUsize::new(shared_cpu_limit) {
            request = request.with_cpu_concurrency(limit);
        }
        if source_buffer_bytes > 0 {
            request = request
                .with_buffer_limits(source_buffer_bytes, destination_buffer_bytes)
                .unwrap();
        }
        let hwm_before = bounded.then(bounded_fixture::parent_hwm).flatten();
        let io_before = delta_metrics.then(observations::process_io).flatten();
        let cpu_before = cpu_metrics.then(observations::process_cpu).flatten();
        let start = std::time::Instant::now();
        let outcomes = futures::future::join_all(
            repositories
                .iter()
                .map(|repository| repository.import(request.clone())),
        )
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
        let nanos = start.elapsed().as_nanos();
        let cpu_after = cpu_metrics.then(observations::process_cpu).flatten();
        let process_cpu = observations::io_delta(cpu_before, cpu_after);
        let io_after = delta_metrics.then(observations::process_io).flatten();
        let process_io = observations::io_delta(io_before, io_after);
        let hwm_after = bounded.then(bounded_fixture::parent_hwm).flatten();
        // Observe release before readback or audits can hide late cleanup.
        let (
            source_buffer_capacity,
            destination_buffer_capacity,
            peak_source_buffer_bytes,
            peak_destination_buffer_bytes,
            reserved_source_buffer_bytes,
            reserved_destination_buffer_bytes,
        ) = request
            .buffer_budget()
            .map_or((0, 0, 0, 0, 0, 0), |buffers| {
                (
                    buffers.source_capacity(),
                    buffers.destination_capacity(),
                    buffers.peak_source_bytes(),
                    buffers.peak_destination_bytes(),
                    buffers.reserved_source_bytes(),
                    buffers.reserved_destination_bytes(),
                )
            });
        assert_eq!(source_buffer_capacity, source_buffer_bytes / 65536 * 65536);
        assert_eq!(
            destination_buffer_capacity,
            destination_buffer_bytes / 65536 * 65536
        );
        assert!(peak_source_buffer_bytes <= source_buffer_capacity);
        assert!(peak_destination_buffer_bytes <= destination_buffer_capacity);
        assert_eq!(reserved_source_buffer_bytes, 0);
        assert_eq!(reserved_destination_buffer_bytes, 0);
        assert_eq!(
            peak_source_buffer_bytes == 0,
            source_buffer_bytes == 0 || operation == "warm"
        );
        assert_eq!(
            peak_destination_buffer_bytes == 0,
            destination_buffer_bytes == 0 || operation == "warm" || backend == "memory"
        );
        let (new, reused, reachable) = match operation {
            "cold" => (count + 2, 0, count + 2),
            "warm" => (0, 1, count + 2),
            "subtree-delta" => (2, 1, count + 3),
            "wide-delta" => (2, count, count + 2),
            _ => unreachable!(),
        };
        let expected_spilled = if delta_spilling && operation == "cold" {
            fixture_blob_deltas
        } else {
            0
        };
        let mut report = casita::GitClosureImportReport::default();
        for (repository, imported) in repositories.iter().zip(outcomes) {
            if bounded {
                bounded_fixture::audit(&imported.reader, &expected).await;
            }
            assert_eq!(imported.report.imported_objects, new);
            assert_eq!(imported.report.reused_objects, reused);
            assert_eq!(
                repository.verify_closure(&root).await.unwrap(),
                ClosureStatus::Complete { objects: reachable }
            );
            assert_eq!(imported.report.spilled_delta_objects, expected_spilled);
            if expected_spilled > 0 {
                assert!(imported.report.peak_spill_bytes > 0);
            }
            report.imported_objects += imported.report.imported_objects;
            report.reused_objects += imported.report.reused_objects;
            report.source_bytes += imported.report.source_bytes;
            report.spilled_delta_objects += imported.report.spilled_delta_objects;
            // Per-import maxima are not simultaneous aggregate observations.
            report.peak_source_bytes = report
                .peak_source_bytes
                .max(imported.report.peak_source_bytes);
            report.peak_spill_bytes = report
                .peak_spill_bytes
                .max(imported.report.peak_spill_bytes);
            report.peak_decode_workers = report
                .peak_decode_workers
                .max(imported.report.peak_decode_workers);
            holds.push(imported);
        }
        let peak_cpu_jobs = request.cpu_budget().map_or(0, |budget| budget.peak_jobs());
        assert!(peak_cpu_jobs <= shared_cpu_limit);
        assert_eq!(
            peak_cpu_jobs == 0,
            shared_cpu_limit == 0 || operation == "warm"
        );
        println!(
            "git_closure_sample {}",
            serde_json::json!({
                "imports": imports, "audited_imports": imports,
                "shared_cpu_limit": shared_cpu_limit, "peak_cpu_jobs": peak_cpu_jobs,
                "source_buffer_capacity": source_buffer_capacity, "destination_buffer_capacity": destination_buffer_capacity,
                "peak_source_buffer_bytes": peak_source_buffer_bytes, "peak_destination_buffer_bytes": peak_destination_buffer_bytes,
                "reserved_source_buffer_bytes": reserved_source_buffer_bytes, "reserved_destination_buffer_bytes": reserved_destination_buffer_bytes,
                "chunk_upload_concurrency": chunk_concurrency,
                "timing_scope": "combined concurrent import makespan",
                "operation": operation, "files": count, "packed": packed,
                "bounded_fixture": bounded, "pack_window": pack_window,
                "parent_hwm_before_import_bytes": hwm_before,
                "parent_hwm_after_import_bytes": hwm_after,
                "payload_correctness": if bounded { Some("independent BLAKE3 and exact streaming readback") } else { None },
                "backend": backend, "file_bytes": file_bytes, "content": content,
                "concurrency": concurrency, "publication_batch_objects": repositories[0].limits().max_batch_objects,
                "max_buffered_bytes": budget, "wall_nanos": nanos,
                "imported_objects": report.imported_objects,
                "reused_objects": report.reused_objects,
                "source_bytes": report.source_bytes,
                "decode_workers": decode_workers,
                "delta_spilling": delta_spilling,
                "fixture_blob_deltas": fixture_blob_deltas,
                "import_process_io": process_io,
                "import_process_cpu": process_cpu,
                "spilled_delta_objects": report.spilled_delta_objects,
                "peak_spill_bytes": report.peak_spill_bytes,
                "peak_source_bytes": report.peak_source_bytes,
                "peak_decode_workers": report.peak_decode_workers,
                "root": root.to_string(),
                "correctness": "exact imported/reused counts and exhaustive closure verification"
            })
        );
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
    // Each worker emits eight objects into a two-slot channel. Storage also
    // needs the sole blocking thread: receiving must progress under pressure.
    let roots = (0..16u8)
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
                        .with_decode_workers(2.try_into().unwrap())
                        .with_max_buffered_bytes(524288.try_into().unwrap()),
                ),
            )
            .await
            .expect("source workers must not occupy a blocking thread waiting for storage")
            .unwrap();
            assert_eq!(result.report.imported_objects, 16);
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

#[tokio::test]
async fn parallel_import_verifies_objects_from_primary_and_alternate_packs() {
    for (name, format) in [
        ("sha1", GitObjectFormat::Sha1),
        ("sha256", GitObjectFormat::Sha256),
    ] {
        let primary = Source::new(name);
        let alternate = Source::new(name);
        let mut entries = String::new();
        for (group, source) in [("a", &primary), ("b", &alternate)] {
            let mut pack_entries = String::new();
            for index in 0..8 {
                let mut body = vec![group.as_bytes()[0]; 65536];
                body[..8].copy_from_slice(&(index as u64).to_le_bytes());
                let oid = source.blob(&body);
                pack_entries.push_str(&format!("100644 blob {oid}\t{group}{index}\n"));
            }
            let root = source.tree(&pack_entries);
            source.git(&["update-ref", "refs/benchmark/packed", &root], b"");
            source.git(&["repack", "-adf", "--window=16"], b"");
            entries.push_str(&pack_entries);
        }
        std::fs::write(
            primary.0.path().join("objects/info/alternates"),
            format!("{}\n", alternate.0.path().join("objects").display()),
        )
        .unwrap();
        let root = key(format, GitObjectKind::Tree, &primary.tree(&entries));
        let repository = Repository::<MemoryBlobStore, MemoryMetadataStore>::memory().unwrap();
        let imported = repository
            .import(
                primary
                    .request(vec![root.clone()])
                    .with_concurrency(16.try_into().unwrap())
                    .with_decode_workers(4.try_into().unwrap())
                    .with_max_buffered_bytes(1048576.try_into().unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(imported.report.imported_objects, 17);
        assert_eq!(
            repository.verify_closure(&root).await.unwrap(),
            ClosureStatus::Complete { objects: 17 }
        );
    }
}

#[path = "git_closure_import/inflation.rs"]
mod inflation;

#[path = "git_closure_import/bounded_fixture.rs"]
mod bounded_fixture;
#[path = "git_closure_import/observations.rs"]
mod observations;

#[path = "git_closure_import/delta_spill.rs"]
mod delta_spill;

// Keep the matched predecessor fixture identical; its importer ignores this
// opt-in option, and its report compatibility fields remain zero.
#[allow(dead_code)]
trait BaselineDeltaSpilling {
    fn with_delta_spilling(self, enabled: bool) -> Self;
}
impl BaselineDeltaSpilling for GitClosureImport {
    fn with_delta_spilling(self, _enabled: bool) -> Self {
        self
    }
}

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

#[path = "git_worker_matrix/adapter.rs"]
mod adapter;
#[path = "git_closure_import/bounded_fixture.rs"]
mod bounded_fixture;
#[path = "git_worker_matrix/observations.rs"]
mod observations;

/// Permanent workload: benchmark run git-worker-matrix. Correctness audits
/// deliberately run outside the timed region.
#[tokio::test]
#[ignore = "run through benchmark run git-worker-matrix"]
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
    let bounded = std::env::var("CASITA_GIT_CLOSURE_BOUNDED_FIXTURE").as_deref() == Ok("1");
    assert!(bounded, "worker matrix requires bounded fixtures");
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
        if content == "clustered" && count > 8 && file_bytes >= 65536 {
            let deltas: usize = std::fs::read_dir(source.0.path().join("objects/pack"))
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
            assert!(
                deltas > 0,
                "clustered packed fixture must contain blob deltas"
            );
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
        let request = source
            .request(vec![root.clone()])
            .with_max_buffered_bytes(budget.try_into().unwrap())
            .with_concurrency(concurrency.try_into().unwrap());
        let request = adapter::select_workers(request, decode_workers);
        let hwm_before = bounded.then(bounded_fixture::parent_hwm).flatten();
        let cpu_before = observations::process_cpu();
        let start = std::time::Instant::now();
        let imported = repository.import(request).await.unwrap();
        let nanos = start.elapsed().as_nanos();
        let cpu = observations::cpu_delta(cpu_before, observations::process_cpu());
        let hwm_after = bounded.then(bounded_fixture::parent_hwm).flatten();
        if bounded {
            bounded_fixture::audit(&imported.reader, &expected).await;
        }
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
        let (peak_source_bytes, peak_decode_workers) = adapter::source_metrics(&imported.report);
        println!(
            "git_closure_sample {}",
            serde_json::json!({
                "operation": operation, "files": count, "packed": packed,
                "bounded_fixture": bounded, "pack_window": pack_window,
                "parent_hwm_before_import_bytes": hwm_before,
                "parent_hwm_after_import_bytes": hwm_after,
                "import_process_cpu": cpu,
                "payload_correctness": if bounded { Some("independent BLAKE3 and exact streaming readback") } else { None },
                "backend": backend, "file_bytes": file_bytes, "content": content,
                "concurrency": concurrency, "publication_batch_objects": repository.limits().max_batch_objects,
                "max_buffered_bytes": budget, "wall_nanos": nanos,
                "imported_objects": imported.report.imported_objects,
                "reused_objects": imported.report.reused_objects,
                "source_bytes": imported.report.source_bytes,
                "decode_workers": decode_workers,
                "peak_source_bytes": peak_source_bytes,
                "peak_decode_workers": peak_decode_workers,
                "root": root.to_string(),
                "correctness": "exact imported/reused counts and exhaustive closure verification"
            })
        );
        holds.push(imported);
    }
}

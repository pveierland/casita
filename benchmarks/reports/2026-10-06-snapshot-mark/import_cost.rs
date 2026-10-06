//! The cost of importing a large Git tree (nixpkgs) into a fresh local
//! repository, by stage: the native object import, the store tree built
//! from it, and its NAR hash. Ignored by default; run with
//! `MNOS_BENCH_DIR=<scratch> MNOS_BENCH_REPO=<repository> cargo test
//! --release -p mnos-eval-cas --test import_cost -- --ignored --nocapture`.

use mnos_eval_cas::{Cas, GitObjectFormat, GitObjectKind, GitObjectRef, GitOid, GitTreeOptions};
use mnos_eval_prim::texts;
use mnos_eval_store::hash::{AlgorithmPrefix, HashFormat};
use std::path::PathBuf;
use std::time::Instant;

/// Linux resource counters are process-wide; the benchmark runs one test.
fn resources(directory: &std::path::Path) -> serde_json::Value {
    let counter = |file: &str, name: &str| -> Option<u64> {
        std::fs::read_to_string(file)
            .ok()?
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key == name)
                    .then(|| value.split_whitespace().next()?.parse().ok())
                    .flatten()
            })
    };
    serde_json::json!({
        "peak_rss_bytes": counter("/proc/self/status", "VmHWM").map(|kb| kb * 1024),
        "rss_bytes": counter("/proc/self/status", "VmRSS").map(|kb| kb * 1024),
        "read_bytes": counter("/proc/self/io", "read_bytes"),
        "write_bytes": counter("/proc/self/io", "write_bytes"),
        "wal_bytes": std::fs::metadata(directory.join("casita.sqlite-wal")).ok().map(|m| m.len()),
    })
}

fn report_stage(directory: &std::path::Path, revision: &BenchText, stage: &str, start: Instant) {
    eprintln!(
        "git_ingest_sample {}",
        serde_json::json!({
            "revision": revision.raw(), "stage": stage,
            "wall_seconds": start.elapsed().as_secs_f64(), "resources": resources(directory),
        })
    );
}

texts! {
    /// An environment variable's value, or Git's output.
    BenchText;
}

fn env_path(name: BenchText) -> Option<PathBuf> {
    std::env::var_os(name.raw()).map(PathBuf::from)
}

/// `git -C <repo> rev-parse <spec>`.
fn rev_parse(repo: &std::path::Path, spec: BenchText) -> BenchText {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["rev-parse", spec.raw()])
        .output()
        .expect("git rev-parse");
    assert!(output.status.success(), "git rev-parse {spec}");
    BenchText::from(String::from_utf8(output.stdout).expect("UTF-8").trim())
}

/// Import revision `spec` of `repo` and build and measure its store tree,
/// printing each stage's time.
fn import_revision(
    cas: &Cas,
    directory: &std::path::Path,
    repo: &std::path::Path,
    objects_dir: &std::path::Path,
    spec: BenchText,
) -> mnos_eval_cas::HeldObject {
    let tree = rev_parse(repo, BenchText::from(format!("{spec}^{{tree}}")));
    let oid = GitOid::from(
        data_encoding::HEXLOWER
            .decode(tree.as_bytes())
            .expect("hex"),
    );
    let reference = GitObjectRef {
        format: GitObjectFormat::Sha1,
        kind: GitObjectKind::Tree,
        oid: oid.clone(),
    };
    eprintln!("revision {spec}:");
    eprintln!(
        "git_ingest_start {}",
        serde_json::json!({
            "revision": spec.raw(), "resources": resources(directory),
        })
    );

    let start = Instant::now();
    let summary = cas
        .import_git(objects_dir, std::slice::from_ref(&reference))
        .expect("import");
    let imported = start.elapsed();
    report_stage(directory, &spec, "import_git", start);
    eprintln!("import_git: {imported:.2?} ({:?} objects)", summary.objects);

    let start = Instant::now();
    let built = cas
        .git_tree(GitObjectFormat::Sha1, &oid, &GitTreeOptions::default())
        .expect("git tree");
    let converted = start.elapsed();
    report_stage(directory, &spec, "git_tree", start);
    eprintln!("git_tree: {converted:.2?}");

    let start = Instant::now();
    let nar = built.object.nar().expect("nar");
    let measured = start.elapsed();
    report_stage(directory, &spec, "nar", start);
    eprintln!(
        "nar: {measured:.2?} ({} bytes, {})",
        nar.size.raw(),
        nar.hash
            .to_text(HashFormat::Nix32, AlgorithmPrefix::Include)
    );
    eprintln!("root: {}", built.object.root());
    eprintln!(
        "git_ingest_identity {}",
        serde_json::json!({
            "revision": spec.raw(), "git_tree": tree.raw(),
            "root": built.object.root().to_string(), "nar_hash": nar.hash.to_sri().raw(),
            "nar_bytes": nar.size.raw(), "imported_objects": summary.objects.raw(),
        })
    );
    eprintln!("total: {:.2?}", imported + converted + measured);
    built.object
}

/// `MNOS_BENCH_REV` (default `HEAD`) into a fresh repository, then
/// `MNOS_BENCH_REV2` when set (a later revision sharing most subtrees).
/// Set `MNOS_BENCH_RETAIN_PREVIOUS=1` to keep the first tree alive during
/// the second import, as a browser retaining an older revision would.
#[test]
#[ignore = "benchmark: needs MNOS_BENCH_DIR and a large Git repository"]
fn import_large_git_tree() {
    let Some(bench) = env_path(BenchText::from("MNOS_BENCH_DIR")) else {
        eprintln!("MNOS_BENCH_DIR is not set");
        return;
    };
    let repo = env_path(BenchText::from("MNOS_BENCH_REPO"))
        .unwrap_or_else(|| PathBuf::from(std::env::var("HOME").expect("HOME")).join("dev/nixpkgs"));
    let objects_dir = match repo.join(".git/objects") {
        working if working.is_dir() => working,
        _ => repo.join("objects"),
    };
    let directory = bench.join(format!("cas-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("bench directory");
    let cas = Cas::open(&directory).expect("open");
    let spec = std::env::var("MNOS_BENCH_REV").unwrap_or_else(|_| "HEAD".to_owned());
    let retain_previous = std::env::var("MNOS_BENCH_RETAIN_PREVIOUS").as_deref() == Ok("1");
    eprintln!(
        "git_ingest_configuration {}",
        serde_json::json!({"retain_previous": retain_previous})
    );
    let first = import_revision(&cas, &directory, &repo, &objects_dir, BenchText::from(spec));
    let first = retain_previous.then_some(first);
    if let Ok(second) = std::env::var("MNOS_BENCH_REV2") {
        import_revision(
            &cas,
            &directory,
            &repo,
            &objects_dir,
            BenchText::from(second),
        );
    }
    drop(first);
    let start = Instant::now();
    drop(cas);
    eprintln!("close: {:.2?}", start.elapsed());
}

/// Repeated reads through held evaluator files expose reader-admission costs.
/// Run with MNOS_BENCH_DIR and optionally MNOS_BENCH_READS (default 4096).
#[test]
#[ignore = "benchmark: needs MNOS_BENCH_DIR"]
fn retained_blob_read_cost() {
    use mnos_eval_cas::{BlobInput, ContentBytes};
    use mnos_eval_store::tree::Executable;

    let Some(bench) = env_path(BenchText::from("MNOS_BENCH_DIR")) else {
        eprintln!("MNOS_BENCH_DIR is not set");
        return;
    };
    let reads: usize = std::env::var("MNOS_BENCH_READS")
        .map(|value| value.parse().expect("read count"))
        .unwrap_or(4096);
    assert!(reads > 0);
    for size in [128, 4096] {
        let directory = bench.join(format!("reads-{size}-{}", std::process::id()));
        let cas = Cas::open(&directory).expect("repository");
        let contents: Vec<_> = (0..128u64)
            .map(|index| {
                let mut bytes = vec![42; size];
                bytes[..8].copy_from_slice(&index.to_le_bytes());
                bytes
            })
            .collect();
        let files: Vec<_> = contents
            .iter()
            .map(|bytes| BlobInput {
                contents: ContentBytes::from_raw(bytes),
                executable: Executable::No,
            })
            .collect();
        let objects = cas.ingest_blobs(&files).expect("fixture");
        let trees: Vec<_> = objects.iter().map(|object| object.tree()).collect();
        for index in 0..objects.len() {
            assert_eq!(
                trees[index]
                    .read_node(objects[index].root())
                    .expect("warm read")
                    .raw(),
                contents[index]
            );
        }
        let before = resources(&directory);
        let start = Instant::now();
        for iteration in 0..reads {
            let index = iteration % objects.len();
            assert_eq!(
                trees[index]
                    .read_node(objects[index].root())
                    .expect("read")
                    .raw(),
                contents[index]
            );
        }
        let seconds = start.elapsed().as_secs_f64();
        let after = resources(&directory);
        let delta = |key: &str| {
            before[key]
                .as_u64()
                .zip(after[key].as_u64())
                .map(|(before, after)| after.saturating_sub(before))
        };
        eprintln!(
            "retained_read_sample {}",
            serde_json::json!({
                "files": objects.len(), "payload_bytes": size, "reads": reads,
                "wall_seconds": seconds,
                "read_bytes": delta("read_bytes"), "write_bytes": delta("write_bytes"),
                "resources": after, "correctness": "passed",
            })
        );
    }
}

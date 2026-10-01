use super::{GitClosureImport, Source, key};
use casita::experimental::{
    GitObjectFormat, GitObjectKind, MemoryBlobStore, MemoryMetadataStore, Repository, SpillLimits,
};

// A matched predecessor can compile this fixture. Its ordinary delta decoder
// ignores the experimental selection, exposing the missing spill-budget gate.
#[allow(dead_code)]
trait BaselineDeltaSpill {
    fn with_delta_spilling(self, enabled: bool) -> Self;
}
impl BaselineDeltaSpill for GitClosureImport {
    fn with_delta_spilling(self, _enabled: bool) -> Self {
        self
    }
}

fn delta_fixture(source: &Source, ofs: bool) -> (Vec<(String, blake3::Hash, usize)>, Vec<String>) {
    let mut ids = Vec::new();
    let mut expected = Vec::new();
    for index in 0u64..4 {
        let mut bytes = vec![b'x'; 2 * 1024 * 1024];
        bytes[..8].copy_from_slice(&index.to_le_bytes());
        let oid = source.blob(&bytes);
        expected.push((oid.clone(), blake3::hash(&bytes), bytes.len()));
        ids.push(oid);
    }
    let prefix = source.0.path().join("objects/pack/pack");
    let mut args = vec!["pack-objects", "--window=16", prefix.to_str().unwrap()];
    if ofs {
        args.push("--delta-base-offset");
    }
    let pack = source.git(&args, (ids.join("\n") + "\n").as_bytes());
    for oid in &ids {
        source.remove(oid);
    }
    let index = prefix.with_file_name(format!("pack-{pack}.idx"));
    let listing = source.git(&["verify-pack", "-v", index.to_str().unwrap()], b"");
    let deltas: Vec<_> = listing
        .lines()
        .filter_map(|line| {
            let columns: Vec<_> = line.split_whitespace().collect();
            (columns.len() == 7 && columns[1] == "blob").then(|| columns[0].to_owned())
        })
        .collect();
    assert!(
        !deltas.is_empty(),
        "the fixture must contain actual delta blobs"
    );
    (expected, deltas)
}

#[tokio::test]
async fn delta_reconstruction_reserves_spill_capacity_before_writing() {
    for (name, format) in [
        ("sha1", GitObjectFormat::Sha1),
        ("sha256", GitObjectFormat::Sha256),
    ] {
        let source = Source::new(name);
        let (_, deltas) = delta_fixture(&source, false);
        let oid = &deltas[0];
        let root = key(format, GitObjectKind::Blob, oid);
        for workers in [1, 4] {
            let repository =
                Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap())
                    .with_spill_limits(SpillLimits {
                        max_memory_objects: 250_000,
                        max_spill_bytes: 1,
                    });
            let result = repository
                .import(
                    source
                        .request(vec![root.clone()])
                        .with_delta_spilling(true)
                        .with_decode_workers(workers.try_into().unwrap()),
                )
                .await;
            let error = result.expect_err("delta reconstruction must reserve disk capacity before allocating or writing its base");
            assert!(error.to_string().contains("spill"), "{error}");
            assert!(
                matches!(
                    error,
                    casita::experimental::GitClosureImportError::Repository(
                        casita::experimental::RepositoryError::LimitExceeded(_)
                    )
                ),
                "spill exhaustion must retain the repository resource-limit type"
            );
            assert!(!matches!(
                repository.verify_closure(&root).await.unwrap(),
                casita::experimental::ClosureStatus::Complete { .. }
            ));
        }
    }
}

#[tokio::test]
async fn spilled_ref_and_offset_deltas_verify_and_read_back_in_both_hash_formats() {
    for (name, format) in [
        ("sha1", GitObjectFormat::Sha1),
        ("sha256", GitObjectFormat::Sha256),
    ] {
        for ofs in [false, true] {
            let source = Source::new(name);
            let (expected, deltas) = delta_fixture(&source, ofs);
            let expected: Vec<_> = expected
                .into_iter()
                .map(|(oid, hash, size)| (key(format, GitObjectKind::Blob, &oid), hash, size))
                .collect();
            let alternate = Source::new(name);
            std::fs::write(
                alternate.0.path().join("objects/info/alternates"),
                format!("{}\n", source.0.path().join("objects").display()),
            )
            .unwrap();
            for workers in [1, 4] {
                for input in [&source, &alternate] {
                    let repository = Repository::new(
                        MemoryBlobStore::new(),
                        MemoryMetadataStore::new().unwrap(),
                    );
                    let roots: Vec<_> = expected.iter().map(|(key, _, _)| key.clone()).collect();
                    let imported = repository
                        .import(
                            input
                                .request(roots.clone())
                                .with_delta_spilling(true)
                                .with_decode_workers(workers.try_into().unwrap()),
                        )
                        .await
                        .unwrap();
                    assert_eq!(imported.report.imported_objects, expected.len());
                    assert_eq!(imported.report.spilled_delta_objects, deltas.len());
                    assert!(imported.report.peak_spill_bytes >= 4 * 1024 * 1024);
                    super::bounded_fixture::audit(&imported.reader, &expected).await;
                    for root in &roots {
                        assert_eq!(
                            repository.verify_closure(root).await.unwrap(),
                            casita::experimental::ClosureStatus::Complete { objects: 1 }
                        );
                    }
                    let warm = repository
                        .import(input.request(roots).with_delta_spilling(true))
                        .await
                        .unwrap();
                    assert_eq!(warm.report.imported_objects, 0);
                    assert_eq!(warm.report.spilled_delta_objects, 0);
                    assert_eq!(warm.report.peak_spill_bytes, 0);
                }
            }
        }
    }
}

#[test]
fn spilling_progresses_with_one_blocking_thread() {
    let source = Source::new("sha1");
    let (expected, deltas) = delta_fixture(&source, true);
    let expected: Vec<_> = expected
        .into_iter()
        .map(|(oid, hash, size)| {
            (
                key(GitObjectFormat::Sha1, GitObjectKind::Blob, &oid),
                hash,
                size,
            )
        })
        .collect();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let repository =
                Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
            let roots = expected.iter().map(|(key, _, _)| key.clone()).collect();
            let imported = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                repository.import(
                    source
                        .request(roots)
                        .with_delta_spilling(true)
                        .with_decode_workers(4.try_into().unwrap()),
                ),
            )
            .await
            .expect("spooling and staging must share a single blocking thread without deadlock")
            .unwrap();
            assert_eq!(imported.report.spilled_delta_objects, deltas.len());
            super::bounded_fixture::audit(&imported.reader, &expected).await;
        });
}

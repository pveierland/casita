//! Compare existing verified payload paths under the same retained snapshot.
use super::*;
use futures::{StreamExt, TryStreamExt, stream};
use serde_json::json;
use std::time::Instant;

async fn read_batch(
    hold: &OwnedRetentionHold<impl BlobStore, impl MetadataStore>,
    fixtures: &[(ObjectRecord, Vec<u8>)],
    scoped: bool,
    concurrency: usize,
) -> Vec<Vec<u8>> {
    stream::iter(fixtures)
        .map(|(record, expected)| async move {
            let payloads = hold.repository.payloads();
            let mut reader = if scoped {
                let proof = payloads
                    .open_proof_scoped(
                        &record.payload(),
                        record.payload_size(),
                        hold._protection._pin.as_ref().unwrap().clone(),
                        hold.snapshot().payload_catalog(),
                    )
                    .await?
                    .expect("published payload");
                crate::verified::stream::decode(proof, record.payload(), record.payload_size())
            } else {
                payloads
                    .open_verified(&record.payload(), record.payload_size())
                    .await?
                    .expect("published payload")
            };
            let mut actual = Vec::with_capacity(expected.len());
            reader.read_to_end(&mut actual).await?;
            Ok::<_, crate::error::Error>(actual)
        })
        .buffered(concurrency)
        .try_collect()
        .await
        .unwrap()
}

fn check(actual: &[Vec<u8>], fixtures: &[(ObjectRecord, Vec<u8>)]) {
    assert_eq!(actual.len(), fixtures.len());
    for (actual, (_, expected)) in actual.iter().zip(fixtures) {
        assert_eq!(actual, expected);
    }
}

#[test]
#[ignore = "permanent benchmark; run benchmark run retained-verified-paths"]
fn benchmark_retained_verified_paths() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(64)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            for size in [0, 4096, 16383, 16384, 16385, 524289] {
                let directory = tempfile::tempdir().unwrap();
                let repository = Repository::local_with_pack_options(
                    directory.path(),
                    crate::PackOptions {
                        target_size: 128 * 1024,
                        cache_capacity: 0,
                    },
                )
                .await
                .unwrap();
                let session = repository.mutation_session().await.unwrap();
                let mut fixtures: Vec<(ObjectRecord, Vec<u8>)> = Vec::new();
                let mut staged = Vec::new();
                for ordinal in 0..64u64 {
                    if size == 0 && ordinal > 0 {
                        fixtures.push(fixtures[0].clone());
                        continue;
                    }
                    let mut bytes = vec![0; size];
                    blake3::Hasher::new()
                        .update(&ordinal.to_le_bytes())
                        .finalize_xof()
                        .fill(&mut bytes);
                    let object = session.stage_blob(&bytes).await.unwrap();
                    assert_eq!(object.record().payload().digest(), crate::Digest::hash(&bytes));
                    fixtures.push((object.record().clone(), bytes));
                    staged.push(object);
                }
                session.publish(staged, Vec::new()).await.unwrap();
                drop(session);
                crate::metadata::flush_repository_leases().await.unwrap();
                let hold = repository.owned_read_hold().await.unwrap();
                assert!(hold.snapshot().payload_catalog().is_some());
                let mut chunk_counts = BTreeSet::new();
                for (record, _) in &fixtures {
                    let hex = record.payload().digest().to_hex();
                    let manifest = directory.path().join(format!("blobs/blobs/b3/{}/{}", &hex[..2], hex));
                    match std::fs::read(manifest) {
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            assert!(size > 0 && size <= 16385);
                            chunk_counts.insert(1);
                        }
                        Ok(bytes) => {
                            assert!(size == 0 || size == 524289);
                            let chunks = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                            assert_eq!(bytes.len() as u64, 8 + 40 * chunks);
                            assert!(if size == 0 { chunks == 0 } else { chunks >= 2 });
                            chunk_counts.insert(chunks);
                        }
                        Err(error) => panic!("manifest fixture: {error}"),
                    }
                }
                let pin = hold._protection._pin.as_ref().unwrap();
                let ledger = repository.metadata().pin_store().await.unwrap();
                assert!(matches!(
                    ledger.inventory().await.unwrap().pins[pin.token()].scope,
                    crate::metadata::PinScope::Snapshot { .. }
                ));
                for concurrency in [1, 64] {
                    // Warm both paths, including catalog resolution and per-pin
                    // resource admission. First-use costs are outside this screen.
                    for scoped in [false, true] {
                        check(&read_batch(&hold, &fixtures, scoped, concurrency).await, &fixtures);
                    }
                    let warmed_revision = ledger.inventory().await.unwrap().revision;
                    for (sample, scoped) in [false, true, true, false, false, true]
                        .into_iter()
                        .enumerate()
                    {
                        let started = Instant::now();
                        let actual = read_batch(&hold, &fixtures, scoped, concurrency).await;
                        let elapsed = started.elapsed();
                        check(&actual, &fixtures);
                        assert_eq!(ledger.inventory().await.unwrap().revision, warmed_revision);
                        println!("retained_verified_sample {}", json!({
                            "size": size, "concurrency": concurrency, "sample": sample,
                            "path": if scoped { "scoped" } else { "unscoped" },
                            "reads": fixtures.len(), "distinct_payloads": if size == 0 { 1 } else { 64 },
                            "layout": if size > 0 && size <= 16385 { "bare" } else { "flat-manifest" },
                            "chunk_counts": chunk_counts,
                            "elapsed_ns": elapsed.as_nanos(),
                            "correctness": "passed"
                        }));
                    }
                }
                drop(hold);
                crate::metadata::flush_repository_leases().await.unwrap();
                assert!(ledger.inventory().await.unwrap().pins.is_empty());
            }
        });
}

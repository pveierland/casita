//! Isolate the reported 512-blob publication slowdown after a WAL3 checkpoint.

use super::*;
use crate::metadata::Wal3MetadataStore;
use crate::{Digest, MemoryBlobStore};
use std::time::Instant;

type ProbeRepository = Repository<MemoryBlobStore, Wal3MetadataStore>;

fn body(index: usize) -> [u8; 8] {
    (index as u64).to_le_bytes()
}

/// Stage outside the measurement, as in the original publication probe.
async fn publish(repository: &ProbeRepository, start: usize, batch: usize, prefix: &str) -> u64 {
    let session = repository.mutation_session().await.unwrap();
    let mut staged = Vec::with_capacity(batch);
    for index in start..start + batch {
        staged.push(session.stage_blob(&body(index)).await.unwrap());
    }
    repository.metadata().reset_read_stats();
    let started = Instant::now();
    let result = session
        .publish_filesystem_constructed(staged, Vec::new())
        .await
        .unwrap();
    let nanos = started.elapsed().as_nanos();
    assert_eq!(result.objects_inserted, batch);
    let stats = repository.metadata().read_stats();
    println!(
        "{prefix}_nanos {nanos} {prefix}_shard_gets {} {prefix}_shard_get_bytes {} \
         {prefix}_shard_cache_hits {} {prefix}_shard_puts {} {prefix}_fragment_gets {} \
         {prefix}_fragment_puts {} {prefix}_manifest_gets {} {prefix}_tail_deltas {}",
        stats.logical_shard_get_requests,
        stats.logical_shard_get_bytes,
        stats.logical_shard_cache_hits,
        stats.logical_shard_put_requests,
        stats.fragment_get_requests,
        stats.fragment_put_requests,
        stats.manifest_load_requests,
        stats.tail_deltas,
    );
    stats.logical_shard_put_requests
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "run through benchmark run wal3-publication-checkpoints"]
async fn benchmark_wal3_publication_checkpoints() {
    // Both sides of the reported 512-object batch size; no partial batches.
    for batch in [511, 512, 513] {
        for reopened in [false, true] {
            let temperature = if reopened { "reopened" } else { "warm" };
            let prefix = format!("wal3_b{batch}_{temperature}");
            let directory = tempfile::tempdir().unwrap();
            let storage = Arc::new(chroma_storage::Storage::Local(
                chroma_storage::local::LocalStorage::new(directory.path().to_str().unwrap()),
            ));
            let payloads = MemoryBlobStore::new();
            let state = Wal3MetadataStore::open(storage.clone(), "probe/state", "first")
                .await
                .unwrap();
            let mut repository = Repository::new(payloads.clone(), state);
            assert_eq!(
                publish(&repository, 0, batch, &format!("{prefix}_before")).await,
                0,
                "the control must precede object-shard checkpointing"
            );

            repository.metadata().reset_read_stats();
            let mut checkpoint_commits = None;
            // Empty metadata edits cross the tail-count limit without changing
            // the object corpus. Stop at the first actual object-shard write.
            for commits in 1..=16 {
                let revision = repository.metadata().snapshot().await.unwrap().revision();
                repository
                    .metadata()
                    .commit(&revision, MetadataMutation::new())
                    .await
                    .unwrap();
                let stats = repository.metadata().read_stats();
                if stats.logical_shard_put_requests > 0 {
                    assert_eq!(stats.tail_deltas, 0);
                    checkpoint_commits = Some(commits);
                    break;
                }
            }
            println!(
                "{prefix}_checkpoint_commits {}",
                checkpoint_commits.expect("checkpoint reached")
            );
            if reopened {
                drop(repository);
                crate::flush_repository_leases().await.unwrap();
                let state = Wal3MetadataStore::open(storage, "probe/state", "reopened")
                    .await
                    .unwrap();
                repository = Repository::new(payloads, state);
            }
            publish(&repository, batch, batch, &format!("{prefix}_after")).await;

            let keys: Vec<_> = (0..2 * batch)
                .map(|index| ObjectKey::blob(BlobId::new(Digest::hash(&body(index)))))
                .collect();
            let snapshot = repository.metadata().snapshot().await.unwrap();
            let records = snapshot.object_batch(&keys).await.unwrap();
            assert_eq!(records.len(), 2 * batch);
            assert!(records.into_iter().all(|record| record.is_some()));
            assert!(
                snapshot
                    .validated_closures(&keys)
                    .await
                    .unwrap()
                    .into_iter()
                    .all(|stored| !stored),
                "raw blobs must not acquire stored closure witnesses"
            );
            drop(snapshot);
            let fsck = repository.fsck().await.unwrap();
            assert_eq!(fsck.objects_checked, 2 * batch);
            assert_eq!(fsck.payloads_checked, 2 * batch);
            assert_eq!(fsck.issues.len(), 2 * batch, "{fsck:?}");
            assert!(
                fsck.issues.iter().all(|issue| {
                    issue.kind == FsckIssueKind::UnrootedObject
                        && issue.disposition == FsckDisposition::Collectible
                }),
                "only the deliberately unrooted objects may be reported: {fsck:?}"
            );
            println!("{prefix}_validated 1");
            drop(repository);
            crate::flush_repository_leases().await.unwrap();
        }
    }
}

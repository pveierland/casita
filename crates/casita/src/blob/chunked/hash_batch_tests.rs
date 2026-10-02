//! Observe actual hash-job inputs without replacing hashing or storage.

use super::ChunkedBlobStore;
use crate::blob::{BlobStore, ChunkMeta};
use crate::digest::{BlobId, ChunkId};
use object_store::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;

struct Observation {
    digests: Vec<ChunkId>,
    jobs: Mutex<Vec<(usize, usize)>>,
}
static OBSERVATIONS: Mutex<Vec<Arc<Observation>>> = Mutex::new(Vec::new());
struct Registration(Arc<Observation>);
impl Drop for Registration {
    fn drop(&mut self) {
        OBSERVATIONS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|entry| !Arc::ptr_eq(entry, &self.0));
    }
}

pub(super) fn record_hash_job<'a>(chunks: impl IntoIterator<Item = &'a [u8]>) {
    let observations = OBSERVATIONS.lock().unwrap().clone();
    if observations.is_empty() {
        return;
    }
    let digests: Vec<_> = chunks
        .into_iter()
        .map(|bytes| (ChunkId::new(blake3::hash(bytes).into()), bytes.len()))
        .collect();
    for observation in observations.iter() {
        let matched: Vec<_> = digests
            .iter()
            .filter(|(digest, _)| observation.digests.contains(digest))
            .collect();
        if !matched.is_empty() {
            observation
                .jobs
                .lock()
                .unwrap()
                .push((matched.len(), matched.iter().map(|(_, bytes)| bytes).sum()));
        }
    }
}

fn data(size: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x752a_1cb3_e465_a80du64 ^ seed;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

#[tokio::test]
async fn writer_hashes_ready_chunks_in_fewer_jobs_without_changing_content() {
    let bytes = data(256 * 1024, 1);
    let expected: Vec<_> = fastcdc::v2020::FastCDC::new(&bytes, 512, 1024, 2048)
        .map(|chunk| ChunkMeta {
            digest: ChunkId::new(
                blake3::hash(&bytes[chunk.offset..chunk.offset + chunk.length]).into(),
            ),
            size: chunk.length as u64,
        })
        .collect();
    assert!(expected.len() > 16);
    let observed = Registration(Arc::new(Observation {
        digests: expected.iter().map(|chunk| chunk.digest).collect(),
        jobs: Mutex::new(Vec::new()),
    }));
    OBSERVATIONS.lock().unwrap().push(observed.0.clone());
    let store = ChunkedBlobStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        Path::default(),
        1024,
    )
    .with_chunk_upload_concurrency(16.try_into().unwrap())
    .with_chunk_memory_budget_bytes(1024 * 1024);
    let id = store.put_slice(&bytes).await.unwrap();
    assert_eq!(id, BlobId::new(blake3::hash(&bytes).into()));
    assert_eq!(store.chunks(&id).await.unwrap().unwrap(), expected);
    let mut actual = Vec::new();
    store
        .open_read(&id)
        .await
        .unwrap()
        .unwrap()
        .read_to_end(&mut actual)
        .await
        .unwrap();
    assert_eq!(actual, bytes);
    let jobs = observed.0.jobs.lock().unwrap().clone();
    assert_eq!(
        jobs.iter().map(|(count, _)| count).sum::<usize>(),
        expected.len(),
        "observe every real chunk hash"
    );
    assert!(
        jobs.iter()
            .all(|(count, bytes)| { (1..=4).contains(count) && *bytes <= 1024 * 1024 })
    );
    // Source readiness and upload completions legitimately flush partial
    // batches. A fixed percentage reduction is not a scheduling invariant;
    // the permanent benchmark measures the size of the improvement.
    assert!(
        jobs.len() < expected.len(),
        "ready chunks should amortize blocking-pool handoffs: {} jobs for {} chunks",
        jobs.len(),
        expected.len()
    );
}

#[test]
fn partial_hash_batches_progress_with_small_budgets_and_one_blocking_thread() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let bytes = data(32768, 3);
        for budget in [65536, 131072, 262144] {
            for concurrency in [1, 3, 4, 5] {
                let store = ChunkedBlobStore::new(
                    Arc::new(object_store::memory::InMemory::new()),
                    Path::default(),
                    1024,
                )
                .with_chunk_upload_concurrency(concurrency.try_into().unwrap())
                .with_chunk_memory_budget_bytes(budget);
                for _ in 0..2 {
                    let id = tokio::time::timeout(
                        std::time::Duration::from_secs(3),
                        store.put_slice(&bytes),
                    )
                    .await
                    .expect("partial batches must release admission")
                    .unwrap();
                    assert_eq!(id, BlobId::new(blake3::hash(&bytes).into()));
                    let mut actual = Vec::new();
                    store
                        .open_read(&id)
                        .await
                        .unwrap()
                        .unwrap()
                        .read_to_end(&mut actual)
                        .await
                        .unwrap();
                    assert_eq!(actual, bytes);
                    assert_eq!(store.chunk_memory_budget.free_bytes(), budget);
                }
            }
        }
    });
}

#[tokio::test]
async fn hash_jobs_bound_count_and_bytes_and_isolate_oversized_chunks() {
    use super::hash_batch::HashBatch;
    use crate::byte_budget::ByteBudget;
    let sizes = [1, 2, 3, 4, 524288, 524288, 524289, 524288, 1048577, 7];
    let inputs: Vec<_> = sizes
        .iter()
        .enumerate()
        .map(|(i, size)| data(*size, 100 + i as u64))
        .collect();
    let digests: Vec<_> = inputs
        .iter()
        .map(|bytes| ChunkId::new(blake3::hash(bytes).into()))
        .collect();
    let observed = Registration(Arc::new(Observation {
        digests: digests.clone(),
        jobs: Mutex::new(Vec::new()),
    }));
    OBSERVATIONS.lock().unwrap().push(observed.0.clone());
    let budget = ByteBudget::new(8 * 1024 * 1024);
    let mut batch = HashBatch::default();
    let mut receivers = Vec::new();
    for bytes in inputs {
        let permit = budget.reserve(bytes.len()).await;
        receivers.push(batch.push(bytes, permit));
    }
    batch.flush();
    for (receiver, digest) in receivers.into_iter().zip(digests) {
        let hashed = receiver.await.unwrap();
        assert_eq!(hashed.digest, digest);
    }
    let jobs = observed.0.jobs.lock().unwrap().clone();
    assert_eq!(
        jobs.iter().map(|(count, _)| count).sum::<usize>(),
        sizes.len()
    );
    assert!(
        jobs.iter()
            .all(|(count, bytes)| *count <= 4 && (*bytes <= 1048576 || *count == 1))
    );
    assert!(
        jobs.contains(&(4, 10)),
        "count boundary must batch four small chunks"
    );
    assert!(
        jobs.contains(&(2, 1048576)),
        "exact byte boundary must remain one job"
    );
    assert!(
        jobs.contains(&(1, 1048577)),
        "oversized admission must run alone"
    );
    assert_eq!(budget.free_bytes(), 8 * 1024 * 1024);
}

#[test]
fn cancelled_hash_group_keeps_all_queued_chunk_permits() {
    use super::hash_batch::HashBatch;
    use crate::byte_budget::ByteBudget;
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let budget = ByteBudget::new(4 * 65536);
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let blocker = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            wait.recv().unwrap();
        });
        started.await.unwrap();
        let mut batch = HashBatch::default();
        let mut receivers = Vec::new();
        for _ in 0..4 {
            receivers.push(batch.push(vec![19; 65536], budget.reserve(65536).await));
        }
        assert_eq!(budget.free_bytes(), 0);
        drop(receivers);
        drop(batch);
        assert_eq!(
            budget.free_bytes(),
            0,
            "all four queued buffers still own admission"
        );
        drop(release);
        blocker.await.unwrap();
        let permit =
            tokio::time::timeout(std::time::Duration::from_secs(3), budget.reserve(4 * 65536))
                .await
                .unwrap();
        drop(permit);
        assert_eq!(budget.free_bytes(), 4 * 65536);
    });
}

#[tokio::test]
async fn shared_cpu_writer_hashes_wait_for_other_import_work() {
    use crate::import_cpu::ImportCpuBudget;
    use std::num::NonZeroUsize;
    use std::time::Duration;
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let bytes = data(32768, 0xace193);
    let observed = Registration(Arc::new(Observation {
        digests: fastcdc::v2020::FastCDC::new(&bytes, 512, 1024, 2048)
            .map(|chunk| {
                ChunkId::new(blake3::hash(&bytes[chunk.offset..chunk.offset + chunk.length]).into())
            })
            .collect(),
        jobs: Mutex::new(Vec::new()),
    }));
    OBSERVATIONS.lock().unwrap().push(observed.0.clone());
    let budget = ImportCpuBudget::new(NonZeroUsize::MIN);
    let occupied = budget.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let release = Release(Some(release));
    let running = tokio::spawn(async move {
        occupied
            .run(move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
            })
            .await
            .unwrap();
    });
    started.await.unwrap();
    let store = ChunkedBlobStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        Path::default(),
        1024,
    );
    let mut writing = Box::pin(budget.scope(store.put_slice(&bytes)));
    let early = tokio::time::timeout(Duration::from_millis(100), writing.as_mut()).await;
    let jobs_before_release = observed.0.jobs.lock().unwrap().len();
    drop(release);
    running.await.unwrap();
    let digest = match early {
        Ok(result) => result.unwrap(),
        Err(_) => tokio::time::timeout(Duration::from_secs(10), writing)
            .await
            .unwrap()
            .unwrap(),
    };
    assert_eq!(digest, BlobId::new(blake3::hash(&bytes).into()));
    assert!(
        !observed.0.jobs.lock().unwrap().is_empty(),
        "the real chunk hash path was not exercised"
    );
    assert_eq!(
        jobs_before_release, 0,
        "chunk hashing bypassed shared CPU admission"
    );
}

#[tokio::test]
async fn shared_cpu_cancelled_hash_dispatch_releases_bytes_before_foreign_job() {
    use crate::byte_budget::ByteBudget;
    use crate::import_cpu::ImportCpuBudget;
    use std::time::Duration;
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let chunks: Vec<_> = (0..4).map(|i| data(65536, 0xa900 + i)).collect();
    let observed = Registration(Arc::new(Observation {
        digests: chunks
            .iter()
            .map(|bytes| ChunkId::new(blake3::hash(bytes).into()))
            .collect(),
        jobs: Mutex::new(Vec::new()),
    }));
    OBSERVATIONS.lock().unwrap().push(observed.0.clone());
    let cpu = ImportCpuBudget::new(std::num::NonZeroUsize::MIN);
    let occupied = cpu.clone();
    let (release, wait) = std::sync::mpsc::channel();
    let release = Release(Some(release));
    let (entered, started) = tokio::sync::oneshot::channel();
    let running = tokio::spawn(async move {
        occupied
            .run(move || {
                entered.send(()).unwrap();
                let _ = wait.recv();
            })
            .await
            .unwrap();
    });
    started.await.unwrap();
    let memory = ByteBudget::new(4 * 65536);
    let mut batch = super::hash_batch::HashBatch::new(Some(cpu));
    let mut receivers = Vec::new();
    for chunk in chunks {
        receivers.push(batch.push(chunk, memory.reserve(65536).await));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let premature_hashes = observed.0.jobs.lock().unwrap().len();
    drop(receivers);
    drop(batch);
    let recovered =
        tokio::time::timeout(Duration::from_millis(100), memory.reserve(4 * 65536)).await;
    let recovered_before_release = recovered.is_ok();
    drop(recovered);
    drop(release);
    running.await.unwrap();
    let _drained = tokio::time::timeout(Duration::from_secs(10), memory.reserve(4 * 65536))
        .await
        .unwrap();
    assert_eq!(
        premature_hashes, 0,
        "hash dispatch bypassed global CPU admission"
    );
    assert!(
        recovered_before_release,
        "cancelled dispatcher waited for unrelated CPU work before releasing bytes"
    );
}

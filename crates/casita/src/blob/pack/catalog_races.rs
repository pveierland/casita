//! Deterministic catalog/flush interleavings; no sleeps or scheduler assumptions.
use super::*;
use object_store::memory::InMemory;

async fn fixture() -> (Arc<PackedChunks>, Vec<u8>) {
    let initial = PackedChunks::empty_state_catalog().unwrap();
    let packed = PackedChunks::open_with_state_catalog(
        Arc::new(InMemory::new()),
        Path::from("catalog-race"),
        u64::MAX,
        0,
        &initial,
    )
    .await
    .unwrap();
    let mut root = decode_delta_catalog(&initial).unwrap();
    root.generation += 1;
    let next = encode_delta_catalog(&root).unwrap().to_vec();
    (packed, next)
}

fn pause(
    slot: &StdMutex<Option<CatalogRaceHook>>,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (resume, resume_rx) = tokio::sync::oneshot::channel();
    *slot.lock().unwrap() = Some(CatalogRaceHook {
        reached: reached_tx,
        resume: resume_rx,
    });
    (reached, resume)
}

fn chunk() -> (ChunkMeta, Bytes) {
    let data = b"a newly durable pack must survive catalog synchronization";
    (
        ChunkMeta {
            digest: ChunkId::new(blake3::hash(data).into()),
            size: data.len() as u64,
        },
        zstd::bulk::compress(data, 3).unwrap().into(),
    )
}

async fn assert_pack_survives(packed: &Arc<PackedChunks>, meta: &ChunkMeta, bytes: &Bytes) {
    assert_eq!(
        packed.get(&meta.digest).await.unwrap().as_ref(),
        Some(bytes)
    );
    let catalog = packed.prepare_state_catalog().await.unwrap().unwrap();
    packed.finish_state_catalog(true).unwrap();
    let reopened = PackedChunks::open_with_state_catalog(
        packed.object_store.clone(),
        packed.base.clone(),
        u64::MAX,
        0,
        &catalog,
    )
    .await
    .unwrap();
    assert_eq!(
        reopened.get(&meta.digest).await.unwrap().as_ref(),
        Some(bytes)
    );
}

#[tokio::test]
async fn synchronization_sees_a_flush_after_catalog_decode() {
    let (packed, next) = fixture().await;
    let (reached, resume) = pause(&packed.catalog_sync_hook);
    let sync = tokio::spawn({
        let packed = packed.clone();
        async move { packed.synchronize_state_catalog(Some(&next)).await }
    });
    reached.await.unwrap();
    let (meta, bytes) = chunk();
    packed.put(meta.clone(), bytes.clone()).await.unwrap();
    packed.flush().await.unwrap();
    resume.send(()).unwrap();
    sync.await.unwrap().unwrap();
    assert_pack_survives(&packed, &meta, &bytes).await;
}

#[tokio::test]
async fn indexed_pack_is_dirty_before_synchronization_can_replace_it() {
    let (packed, next) = fixture().await;
    let (meta, bytes) = chunk();
    packed.put(meta.clone(), bytes.clone()).await.unwrap();
    let (reached, resume) = pause(&packed.flush_indexed_hook);
    let flush = tokio::spawn({
        let packed = packed.clone();
        async move { packed.flush().await }
    });
    reached.await.unwrap();
    packed.synchronize_state_catalog(Some(&next)).await.unwrap();
    resume.send(()).unwrap();
    flush.await.unwrap().unwrap();
    assert_pack_survives(&packed, &meta, &bytes).await;
}

//! Batched object handles retain metadata provenance without holding SQL cursors.
#![cfg(all(feature = "native", feature = "experimental"))]
use casita::Repository;
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn batched_handles_preserve_visibility_order_and_collection_protection() {
    let directory = tempfile::tempdir().unwrap();
    for repository in [
        Repository::memory().unwrap(),
        Repository::local(directory.path()).await.unwrap(),
    ] {
        let key = repository
            .import(casita::import::BlobImport::new(
                &b"held payload"[..],
                "old".parse().unwrap(),
            ))
            .await
            .unwrap();
        let snapshot = repository.retained_reader().await.unwrap();
        let reader = snapshot.object_reader().unwrap();
        drop(snapshot);
        let future = repository
            .import(casita::import::BlobImport::new(
                &b"future payload"[..],
                "future".parse().unwrap(),
            ))
            .await
            .unwrap();
        assert!(reader.objects(&[]).await.unwrap().is_empty());
        for count in [63, 64, 65, 255, 256, 257] {
            let keys: Vec<_> = [key.clone(), future.clone(), key.clone()]
                .into_iter()
                .cycle()
                .take(count)
                .collect();
            let handles = reader.objects(&keys).await.unwrap();
            assert_eq!(handles.len(), count);
            for (handle, expected) in handles.iter().zip(&keys) {
                if *expected == future {
                    assert!(handle.is_none());
                } else {
                    assert_eq!(handle.as_ref().unwrap().record().key(), expected);
                }
            }
        }
        let handles = reader.objects(&[key.clone(), key.clone()]).await.unwrap();
        drop(reader);
        repository
            .remove_root(&"old".parse().unwrap(), &key)
            .await
            .unwrap();
        repository.collect().await.unwrap();
        assert!(repository.object(&key).await.unwrap().is_some());
        repository.flush().await.unwrap();
        let mut opened = handles[0].as_ref().unwrap().open_verified().await.unwrap();
        drop(handles);
        repository.collect().await.unwrap();
        assert!(repository.object(&key).await.unwrap().is_some());
        let mut bytes = Vec::new();
        opened.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"held payload");
        drop(opened);
        repository.collect().await.unwrap();
        assert!(repository.object(&key).await.unwrap().is_none());
        repository.flush().await.unwrap();
    }
}

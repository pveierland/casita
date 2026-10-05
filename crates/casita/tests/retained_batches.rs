//! Batched retained reads preserve the snapshot, key ordering and GC protection.
#![cfg(all(feature = "native", feature = "experimental"))]

use casita::{BlobId, Digest, ObjectKey, Repository, RootName};

#[tokio::test]
async fn retained_batches_preserve_order_missing_duplicates_and_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    for repository in [
        Repository::memory().unwrap(),
        Repository::local(directory.path()).await.unwrap(),
    ] {
        let mut keys = Vec::new();
        let mut names = Vec::new();
        for index in 0..3u8 {
            let bytes = vec![index; 7];
            let name = RootName::try_from(format!("blob-{index}")).unwrap();
            keys.push(
                repository
                    .import(casita::import::BlobImport::new(
                        bytes.as_slice(),
                        name.clone(),
                    ))
                    .await
                    .unwrap(),
            );
            names.push(name);
        }
        let held = repository.retained_reader().await.unwrap();
        let records = futures::future::try_join_all(keys.iter().map(|key| held.object(key)))
            .await
            .unwrap();
        let missing = ObjectKey::blob(BlobId::new(Digest::hash(b"absent")));
        let later = repository
            .import(casita::import::BlobImport::new(
                &b"later"[..],
                "later".try_into().unwrap(),
            ))
            .await
            .unwrap();
        assert!(held.object_batch(&[]).await.unwrap().is_empty());
        for count in [1, 255, 256, 257, 513] {
            let pattern = [
                keys[2].clone(),
                missing.clone(),
                keys[0].clone(),
                keys[2].clone(),
                later.clone(),
            ];
            let expected = [
                records[2].clone(),
                None,
                records[0].clone(),
                records[2].clone(),
                None,
            ];
            let requested: Vec<_> = pattern.into_iter().cycle().take(count).collect();
            let actual = held.object_batch(&requested).await.unwrap();
            assert_eq!(
                actual,
                expected.into_iter().cycle().take(count).collect::<Vec<_>>()
            );
        }
        assert!(repository.remove_root(&names[0], &keys[0]).await.unwrap());
        repository.collect().await.unwrap();
        assert_eq!(
            held.object_batch(&[keys[0].clone()]).await.unwrap(),
            vec![records[0].clone()]
        );
        drop(held);
        repository.collect().await.unwrap();
        assert!(repository.object(&keys[0]).await.unwrap().is_none());
        repository.flush().await.unwrap();
    }
    casita::experimental::flush_repository_leases()
        .await
        .unwrap();
}

//! Immutable object readers reuse admitted protection without keeping SQL views.
#![cfg(all(feature = "native", feature = "experimental"))]

use casita::{BlobId, Digest, ObjectKey, Repository, RootName};
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn object_readers_preserve_birth_bound_order_and_payload_protection() {
    let directory = tempfile::tempdir().unwrap();
    for repository in [
        Repository::memory().unwrap(),
        Repository::local(directory.path()).await.unwrap(),
    ] {
        let mut keys = Vec::new();
        for index in 0..3u8 {
            let name: RootName = format!("old-{index}").parse().unwrap();
            keys.push(
                repository
                    .import(casita::import::BlobImport::new(&[index; 128][..], name))
                    .await
                    .unwrap(),
            );
        }
        let snapshot = repository.retained_reader().await.unwrap();
        let reader = snapshot.object_reader().unwrap();
        assert_eq!(reader.generation(), snapshot.generation().unwrap());
        let records = snapshot.object_batch(&keys).await.unwrap();
        drop(snapshot);
        let future = repository
            .import(casita::import::BlobImport::new(
                &b"future"[..],
                "future".parse().unwrap(),
            ))
            .await
            .unwrap();
        let missing = ObjectKey::blob(BlobId::new(Digest::hash(b"missing")));
        assert!(reader.object_batch(&[]).await.unwrap().is_empty());
        for count in [1, 255, 256, 257, 513] {
            let pattern = [
                keys[2].clone(),
                future.clone(),
                keys[0].clone(),
                keys[2].clone(),
                missing.clone(),
            ];
            let expected = [
                records[2].clone(),
                None,
                records[0].clone(),
                records[2].clone(),
                None,
            ];
            let query: Vec<_> = pattern.into_iter().cycle().take(count).collect();
            assert_eq!(
                reader.object_batch(&query).await.unwrap(),
                expected.into_iter().cycle().take(count).collect::<Vec<_>>()
            );
        }
        assert!(reader.open_verified(&future).await.unwrap().is_none());
        assert!(reader.open(&missing).await.unwrap().is_none());
        let mut opened = reader.open_verified(&keys[0]).await.unwrap().unwrap();
        let mut prefix = [0; 3];
        opened.read_exact(&mut prefix).await.unwrap();
        repository
            .remove_root(&"old-0".parse().unwrap(), &keys[0])
            .await
            .unwrap();
        // Neither the object handle nor an opened physical stream holds SQL.
        repository.flush().await.unwrap();
        let clone = reader.clone();
        let mut seekable = clone.open(&keys[1]).await.unwrap().unwrap();
        repository
            .remove_root(&"old-1".parse().unwrap(), &keys[1])
            .await
            .unwrap();
        drop(clone);
        drop(reader);
        repository.collect().await.unwrap();
        // Metadata presence checks ensure protection, even if a small payload
        // has already been buffered by the physical reader.
        assert!(repository.object(&keys[0]).await.unwrap().is_some());
        assert!(repository.object(&keys[1]).await.unwrap().is_some());
        let mut plain = Vec::new();
        seekable.read_to_end(&mut plain).await.unwrap();
        assert_eq!(plain, [1; 128]);
        let mut bytes = prefix.to_vec();
        opened.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, [0; 128]);
        drop(opened);
        drop(seekable);
        repository.collect().await.unwrap();
        assert!(repository.object(&keys[0]).await.unwrap().is_none());
        assert!(repository.object(&keys[1]).await.unwrap().is_none());
        repository.flush().await.unwrap();
    }
}

#[tokio::test]
async fn idle_object_readers_and_open_payloads_do_not_block_wal_truncation() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let key = repository
        .import(casita::import::BlobImport::new(
            &b"old bytes"[..],
            "old".parse().unwrap(),
        ))
        .await
        .unwrap();
    let snapshot = repository.retained_reader().await.unwrap();
    let reader = snapshot.object_reader().unwrap();
    drop(snapshot);
    let mut payload = reader.open_verified(&key).await.unwrap().unwrap();
    repository
        .import(casita::import::BlobImport::new(
            &b"new bytes"[..],
            "new".parse().unwrap(),
        ))
        .await
        .unwrap();
    repository.flush().await.unwrap();
    assert_eq!(
        std::fs::metadata(directory.path().join("casita.sqlite-wal"))
            .unwrap()
            .len(),
        0
    );
    let mut bytes = Vec::new();
    payload.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"old bytes");
    drop(payload);
    drop(reader);
    repository.flush().await.unwrap();
}

#[tokio::test]
async fn failed_object_decoding_releases_the_implicit_read_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let key = repository
        .import(casita::import::BlobImport::new(
            &b"corrupt metadata, intact payload"[..],
            "record".parse().unwrap(),
        ))
        .await
        .unwrap();
    let snapshot = repository.retained_reader().await.unwrap();
    let reader = snapshot.object_reader().unwrap();
    drop(snapshot);
    let path = directory.path().join("casita.sqlite");
    let database = turso::Builder::new_local(path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    connection
        .execute(
            "UPDATE objects SET record = X'FF' WHERE namespace = ?1 AND native_id = ?2",
            turso::params![key.namespace().as_str(), key.native_id()],
        )
        .await
        .unwrap();
    for count in [1, 3] {
        assert_eq!(
            reader
                .object_batch(&vec![key.clone(); count])
                .await
                .unwrap_err()
                .kind(),
            casita::ErrorKind::Corrupt
        );
        repository
            .flush()
            .await
            .expect("failed decodes must not leave pooled cursors pinning WAL");
        assert_eq!(
            std::fs::metadata(directory.path().join("casita.sqlite-wal"))
                .unwrap()
                .len(),
            0
        );
    }
    drop(reader);
    drop(connection);
    drop(database);
}

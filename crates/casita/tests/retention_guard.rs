//! Data protection can outlive a SQL snapshot without blocking checkpoints.
#![cfg(all(feature = "native", feature = "experimental"))]

use casita::{ErrorKind, Repository, RootName};
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn object_retention_survives_snapshot_release_and_collection() {
    let directory = tempfile::tempdir().unwrap();
    for repository in [
        Repository::memory().unwrap(),
        Repository::local(directory.path()).await.unwrap(),
    ] {
        let name: RootName = "sentinel".parse().unwrap();
        let bytes = b"protected independently of a metadata snapshot";
        let key = repository
            .import(casita::import::BlobImport::new(&bytes[..], name.clone()))
            .await
            .unwrap();
        let snapshot = repository.retained_reader().await.unwrap();
        let retention = snapshot.retain_objects();
        let cloned = retention.clone();
        drop(snapshot);
        drop(retention);
        repository.remove_root(&name, &key).await.unwrap();
        repository.collect().await.unwrap();
        let fresh = repository.retained_reader().await.unwrap();
        let mut opened = fresh.open(&key).await.unwrap().unwrap();
        drop(fresh);
        drop(cloned);
        repository.collect().await.unwrap();
        let mut actual = Vec::new();
        opened.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, bytes);
        drop(opened);
        repository.collect().await.unwrap();
        assert!(repository.object(&key).await.unwrap().is_none());
        repository.flush().await.unwrap();
    }
}

#[tokio::test]
async fn object_retention_allows_wal_truncation_while_a_snapshot_blocks_it() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Repository::local(directory.path()).await.unwrap();
    let key = repository
        .import(casita::import::BlobImport::new(
            &b"old"[..],
            "old".parse().unwrap(),
        ))
        .await
        .unwrap();
    repository.flush().await.unwrap();
    let snapshot = repository.retained_reader().await.unwrap();
    assert!(snapshot.object(&key).await.unwrap().is_some());
    let retention = snapshot.retain_objects();
    repository
        .import(casita::import::BlobImport::new(
            &b"new"[..],
            "new".parse().unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(
        repository.flush().await.unwrap_err().kind(),
        ErrorKind::Busy
    );
    let wal = directory.path().join("casita.sqlite-wal");
    assert!(std::fs::metadata(&wal).unwrap().len() > 0);
    drop(snapshot);
    repository.flush().await.unwrap();
    assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
    let fresh = repository.retained_reader().await.unwrap();
    assert!(fresh.object(&key).await.unwrap().is_some());
    drop(fresh);
    drop(retention);
    repository.flush().await.unwrap();
}

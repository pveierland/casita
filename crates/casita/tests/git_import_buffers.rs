#![cfg(feature = "native")]

#[path = "../src/import_buffer.rs"]
mod import_buffer;
use import_buffer::{ImportBufferBudget, Partition as Budget};

const UNIT: usize = 64 * 1024;

fn budget(bytes: usize) -> Budget {
    Budget::new(bytes).unwrap()
}

#[test]
fn reservations_never_round_capacity_up_or_saturate_oversized_requests() {
    for bytes in [2 * UNIT - 1, 2 * UNIT, 2 * UNIT + 1] {
        let part = budget(bytes);
        let capacity = bytes / UNIT * UNIT;
        assert_eq!(part.capacity(), capacity);
        assert!(part.try_reserve(capacity).is_some());
        assert!(part.try_reserve(capacity + 1).is_none());
    }
    let budget = budget(2 * UNIT + 1);
    assert!(
        budget.try_reserve(2 * UNIT + 1).is_none(),
        "a strict partition must reject a reservation whose rounded size exceeds its capacity"
    );
}

#[tokio::test]
async fn impossible_reservations_fail_promptly_and_exact_capacity_is_usable() {
    let budget = budget(2 * UNIT + UNIT - 1);
    assert_eq!(budget.capacity(), 2 * UNIT);
    assert!(budget.reserve(2 * UNIT + 1).await.is_err());
    let held = budget.reserve(2 * UNIT).await.unwrap();
    assert!(budget.try_reserve(1).is_none());
    drop(held);
    assert!(budget.try_reserve(2 * UNIT).is_some());
    assert!(Budget::new(UNIT - 1).is_err());
}

#[tokio::test]
async fn a_full_source_partition_preserves_destination_progress_room() {
    let budget = ImportBufferBudget::new(UNIT, UNIT).unwrap();
    assert_eq!(budget.source_capacity(), UNIT);
    assert_eq!(budget.destination_capacity(), UNIT);
    let source = budget.source.reserve(UNIT).await.unwrap();
    let destination = budget.destination.reserve(UNIT).await.unwrap();
    assert_eq!(budget.reserved_source_bytes(), UNIT);
    assert_eq!(budget.reserved_destination_bytes(), UNIT);
    drop((source, destination));
    assert_eq!(budget.reserved_source_bytes(), 0);
    assert_eq!(budget.reserved_destination_bytes(), 0);
    assert_eq!(budget.peak_source_bytes(), UNIT);
    assert_eq!(budget.peak_destination_bytes(), UNIT);
}

#[tokio::test]
async fn clones_share_capacity_and_cancelled_waiters_recover() {
    let budget = ImportBufferBudget::new(UNIT, UNIT).unwrap();
    let held = budget.source.reserve(UNIT).await.unwrap();
    let clone = budget.clone();
    let mut waiting = Box::pin(clone.source.reserve(UNIT));
    assert!(futures::poll!(&mut waiting).is_pending());
    drop(waiting);
    drop(held);
    assert!(budget.source.try_reserve(UNIT).is_some());
    assert_eq!(budget.reserved_source_bytes(), 0);
}

#[tokio::test]
async fn unreceived_blocking_outputs_keep_their_buffer_admission() {
    use std::sync::Arc;
    let budget = ImportBufferBudget::new(UNIT, UNIT).unwrap();
    let held = budget.source.reserve(UNIT).await.unwrap();
    let last = Arc::downgrade(&held);
    let (send, receive) = tokio::sync::oneshot::channel();
    tokio::task::spawn_blocking(move || {
        // Tuple field order drops the actual bytes before their reservation.
        drop(send.send((vec![19; UNIT], held)));
    })
    .await
    .unwrap();
    assert!(last.upgrade().is_some());
    assert_eq!(budget.reserved_source_bytes(), UNIT);
    assert!(budget.source.try_reserve(1).is_none());
    drop(receive);
    assert!(last.upgrade().is_none());
    assert_eq!(budget.reserved_source_bytes(), 0);
}

#[tokio::test]
async fn cancelled_running_jobs_keep_their_buffer_admission() {
    let budget = ImportBufferBudget::new(UNIT, UNIT).unwrap();
    let held = budget.source.reserve(UNIT).await.unwrap();
    let (release, wait) = std::sync::mpsc::channel();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (ended, finished) = tokio::sync::oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        let bytes = vec![19; UNIT];
        entered.send(()).unwrap();
        wait.recv().unwrap();
        drop(bytes);
        drop(held);
        ended.send(()).unwrap();
    });
    started.await.unwrap();
    task.abort();
    drop(task);
    assert_eq!(budget.reserved_source_bytes(), UNIT);
    release.send(()).unwrap();
    finished.await.unwrap();
    assert_eq!(budget.reserved_source_bytes(), 0);
}

#[tokio::test]
async fn explicit_scopes_share_clones_and_unconfigured_imports_mask_the_parent() {
    let budget = ImportBufferBudget::new(UNIT, UNIT).unwrap();
    budget
        .scope(async {
            let current = import_buffer::current().unwrap();
            let held = current.destination.reserve(UNIT).await.unwrap();
            assert!(budget.destination.try_reserve(1).is_none());
            import_buffer::scope(None, async {
                assert!(import_buffer::current().is_none());
            })
            .await;
            assert!(import_buffer::current().is_some());
            drop(held);
        })
        .await;
    assert!(import_buffer::current().is_none());
}

#[cfg(all(feature = "git", feature = "experimental"))]
mod pipeline {
    use casita::ObjectKey;
    use casita::experimental::{
        BlobStore, ChunkedBlobStore, GitObjectFormat, GitObjectKind, Repository, git_object_key,
    };
    use casita::import::{GitClosureImport, ImportBufferBudget};
    use std::io::Write;
    use std::process::{Command, Stdio};
    use std::sync::Arc;

    struct Source(tempfile::TempDir);
    impl Source {
        fn new() -> Self {
            let source = Self(tempfile::tempdir().unwrap());
            source.git(&["init", "--bare", "-q"], b"");
            source
        }
        fn git(&self, args: &[&str], input: &[u8]) -> String {
            let mut child = Command::new("git")
                .arg("-C")
                .arg(self.0.path())
                .args(args)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(input).unwrap();
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        }
        fn blob(&self, body: &[u8]) -> ObjectKey {
            let oid = self.git(&["hash-object", "-w", "--stdin"], body);
            git_object_key(
                GitObjectFormat::Sha1,
                GitObjectKind::Blob,
                data_encoding::HEXLOWER.decode(oid.as_bytes()).unwrap(),
            )
            .unwrap()
        }
        fn request(&self, roots: Vec<ObjectKey>) -> GitClosureImport {
            GitClosureImport::new(self.0.path().join("objects"), roots)
        }
    }
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap()
    }

    #[test]
    fn impossible_writer_envelope_fails_before_accepting_payload() {
        runtime().block_on(async {
            let store = ChunkedBlobStore::new(
                Arc::new(object_store::memory::InMemory::new()),
                object_store::path::Path::default(),
                262144,
            );
            let budget = ImportBufferBudget::new(1048576, 65536).unwrap();
            let result = budget
                .scope(store.put_slice(b"not enough writer room"))
                .await;
            assert!(
                result.is_err(),
                "the selected destination cannot hold even one writer envelope"
            );
            assert_eq!(budget.reserved_destination_bytes(), 0);
        });
    }

    #[test]
    fn impossible_source_envelope_fails_before_decoding() {
        let source = Source::new();
        let root = source.blob(b"not enough source room");
        let destination = tempfile::tempdir().unwrap();
        runtime().block_on(async {
            let repository = Repository::local(destination.path()).await.unwrap();
            let budget = ImportBufferBudget::new(65536, 8 * 1048576).unwrap();
            let result = repository
                .import(
                    source
                        .request(vec![root])
                        .with_buffer_budget(budget.clone()),
                )
                .await;
            assert!(
                result.is_err(),
                "the source partition cannot hold fixed reader and scratch overhead"
            );
            assert_eq!(budget.reserved_source_bytes(), 0);
        });
    }

    #[test]
    fn writer_capacity_boundary_preserves_one_complete_progress_envelope() {
        runtime().block_on(async {
            let probe_store = ChunkedBlobStore::new(
                Arc::new(object_store::memory::InMemory::new()),
                object_store::path::Path::default(),
                262144,
            )
            .with_chunk_upload_concurrency(std::num::NonZeroUsize::MIN);
            let roomy = ImportBufferBudget::new(1048576, 64 * 1048576).unwrap();
            let mut probe = roomy.scope(probe_store.open_write()).await;
            let minimum = roomy.peak_destination_bytes();
            assert!(minimum > 0);
            probe.close().await.unwrap();
            drop(probe);
            assert_eq!(roomy.reserved_destination_bytes(), 0);
            println!("buffer_boundary writer_minimum={minimum}");
            let body = vec![37; 1048577];
            for capacity in [minimum - 1, minimum, minimum + 1] {
                let store = ChunkedBlobStore::new(
                    Arc::new(object_store::memory::InMemory::new()),
                    object_store::path::Path::default(),
                    262144,
                );
                let budget = ImportBufferBudget::new(1048576, capacity).unwrap();
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    budget.scope(store.put_slice(&body)),
                )
                .await
                .unwrap();
                assert_eq!(result.is_ok(), capacity >= minimum);
                assert!(budget.peak_destination_bytes() <= capacity);
                assert_eq!(budget.reserved_destination_bytes(), 0);
                if let Ok(id) = result {
                    assert_eq!(store.read_to_vec(&id).await.unwrap().unwrap(), body);
                }
            }
        });
    }

    #[test]
    fn source_capacity_boundary_and_oversized_buffered_fallback_are_explicit() {
        let source = Source::new();
        let tiny = source.blob(b"source boundary");
        let too_big = source.blob(&vec![17; 131072]);
        runtime().block_on(async {
            let minimum = 9 * 65536;
            for capacity in [minimum - 1, minimum, minimum + 1] {
                let destination = tempfile::tempdir().unwrap();
                let repository = Repository::local(destination.path()).await.unwrap();
                let budget = ImportBufferBudget::new(capacity, 16 * 1048576).unwrap();
                let result = repository
                    .import(
                        source
                            .request(vec![tiny.clone()])
                            .with_buffer_budget(budget.clone()),
                    )
                    .await;
                assert_eq!(result.is_ok(), capacity >= minimum);
                assert!(budget.peak_source_bytes() <= capacity);
                assert_eq!(budget.reserved_source_bytes(), 0);
            }
            let destination = tempfile::tempdir().unwrap();
            let repository = Repository::local(destination.path()).await.unwrap();
            let budget = ImportBufferBudget::new(minimum, 16 * 1048576).unwrap();
            let error = repository
                .import(
                    source
                        .request(vec![too_big])
                        .with_buffer_budget(budget.clone()),
                )
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("source body allowance"),
                "{error}"
            );
            assert_eq!(budget.reserved_source_bytes(), 0);
            println!("buffer_boundary source_minimum={minimum}");
        });
    }

    #[test]
    fn cancelled_writer_keeps_admission_with_queued_blocking_bytes() {
        struct Release(Option<std::sync::mpsc::Sender<()>>);
        impl Drop for Release {
            fn drop(&mut self) {
                if let Some(send) = self.0.take() {
                    let _ = send.send(());
                }
            }
        }
        runtime().block_on(async {
            let (release, wait) = std::sync::mpsc::channel();
            let release = Release(Some(release));
            let (entered, started) = tokio::sync::oneshot::channel();
            let parked = tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
            });
            started.await.unwrap();
            let store = ChunkedBlobStore::new(
                Arc::new(object_store::memory::InMemory::new()),
                object_store::path::Path::default(),
                262144,
            );
            let budget = ImportBufferBudget::new(1048576, 11 * 1048576).unwrap();
            let body = vec![29; 100000];
            let mut write = Box::pin(budget.scope(store.put_slice(&body)));
            for _ in 0..4 {
                assert!(futures::poll!(&mut write).is_pending());
                tokio::task::yield_now().await;
            }
            assert!(budget.reserved_destination_bytes() > 0);
            drop(write);
            let retained = budget.reserved_destination_bytes() > 0;
            drop(release);
            parked.await.unwrap();
            let next = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                budget.scope(store.open_write()),
            )
            .await
            .unwrap();
            drop(next);
            assert!(
                retained,
                "queued compression lost its writer reservation on caller cancellation"
            );
            assert_eq!(budget.reserved_destination_bytes(), 0);
        });
    }

    #[test]
    fn concurrent_imports_release_source_allowances_between_windows() {
        let source = Source::new();
        let expected: Vec<_> = (0..13u8)
            .map(|n| {
                let body = vec![n; if n == 0 { 2 * 1048576 + 1 } else { 1024 }];
                (source.blob(&body), body)
            })
            .collect();
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        runtime().block_on(async {
            let repositories = [
                Repository::local(first.path()).await.unwrap(),
                Repository::local(second.path()).await.unwrap(),
            ];
            let budget = ImportBufferBudget::new(1048576, 11 * 1048576).unwrap();
            let request = source
                .request(expected.iter().map(|(key, _)| key.clone()).collect())
                .with_buffer_budget(budget.clone())
                .with_decode_workers(4.try_into().unwrap())
                .with_cpu_concurrency(std::num::NonZeroUsize::MIN);
            let outcomes = tokio::time::timeout(
                std::time::Duration::from_secs(20),
                futures::future::join_all(
                    repositories
                        .iter()
                        .map(|repository| repository.import(request.clone())),
                ),
            )
            .await
            .expect("window admission must not survive in the reusable source pool");
            for outcome in outcomes {
                let outcome = outcome.unwrap();
                assert_eq!(outcome.report.imported_objects, expected.len());
                for (key, body) in &expected {
                    let (_, mut reader) = outcome.reader.open_payload(key).await.unwrap().unwrap();
                    let mut actual = Vec::new();
                    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual)
                        .await
                        .unwrap();
                    assert_eq!(&actual, body);
                }
            }
            assert_eq!(budget.reserved_source_bytes(), 0);
            assert_eq!(budget.reserved_destination_bytes(), 0);
            assert!(budget.peak_source_bytes() <= budget.source_capacity());
            assert!(budget.peak_destination_bytes() <= budget.destination_capacity());
        });
    }

    #[test]
    fn shared_buffers_and_one_cpu_progress_and_release_after_real_streamed_import() {
        let source = Source::new();
        let mut body = vec![0; 2 * 1048576 + 1];
        blake3::Hasher::new()
            .update(b"shared producer buffer progress")
            .finalize_xof()
            .fill(&mut body);
        let root = source.blob(&body);
        let destination = tempfile::tempdir().unwrap();
        runtime().block_on(async {
            let repository = Repository::local(destination.path()).await.unwrap();
            let budget = ImportBufferBudget::new(1048576, 11 * 1048576).unwrap();
            let request = source
                .request(vec![root.clone()])
                .with_buffer_budget(budget.clone())
                .with_cpu_concurrency(std::num::NonZeroUsize::MIN)
                .with_decode_workers(4.try_into().unwrap());
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(20),
                repository.import(request),
            )
            .await
            .expect("one CPU and one blocking thread must make progress")
            .unwrap();
            assert_eq!(result.report.imported_objects, 1);
            assert!(
                budget.peak_source_bytes() > 0,
                "source did not capture selected buffer admission"
            );
            assert!(
                budget.peak_destination_bytes() > 0,
                "writer did not capture selected buffer admission"
            );
            assert!(budget.peak_source_bytes() <= budget.source_capacity());
            assert!(budget.peak_destination_bytes() <= budget.destination_capacity());
            assert_eq!(budget.reserved_source_bytes(), 0);
            assert_eq!(budget.reserved_destination_bytes(), 0);
            let (_, mut reader) = result.reader.open_payload(&root).await.unwrap().unwrap();
            let mut actual = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual)
                .await
                .unwrap();
            assert_eq!(actual, body);
        });
    }
}

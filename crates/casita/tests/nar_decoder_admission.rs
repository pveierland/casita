//! NAR decoding leaves blocking capacity available for durable storage.
#![cfg(feature = "native")]

use casita::{NarRequirements, Repository, import::NarImport, scrub_nar};
use sha2::{Digest, Sha256};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

#[path = "../../../benchmarks/fixtures/nar_decoder.rs"]
mod fixture;

struct Reap(Child);
impl Drop for Reap {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn isolated(name: &str, work: impl FnOnce()) {
    if std::env::var("CASITA_NAR_DECODER_CHILD").as_deref() == Ok(name) {
        work();
        return;
    }
    let mut child = Reap(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture", "--test-threads=1"])
            .env("CASITA_NAR_DECODER_CHILD", name)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "{name}: {status}");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "NAR intake exceeded process watchdog"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn durable_intake_crosses_the_buffer_window_with_one_worker() {
    isolated(
        "durable_intake_crosses_the_buffer_window_with_one_worker",
        || {
            for threads in [1, 2] {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .max_blocking_threads(threads)
                    .enable_all()
                    .build()
                    .unwrap();
                for size in [8 * 1024 * 1024, 32 * 1024 * 1024] {
                    let archive = fixture::archive(size);
                    runtime.block_on(async {
                        let directory = tempfile::tempdir().unwrap();
                        let repo = Repository::local(directory.path()).await.unwrap();
                        let report = repo
                            .import(NarImport::new(archive.as_slice()))
                            .await
                            .unwrap();
                        assert_eq!(report.nar_size(), archive.len() as u64);
                        assert_eq!(report.nar_sha256(), Sha256::digest(&archive).as_slice());
                        assert_eq!(report.stats().hash_payload_bytes, size as u64);
                        let scrub =
                            scrub_nar(report.reader(), report.root(), &NarRequirements::default())
                                .await
                                .unwrap();
                        assert_eq!(scrub.nar_sha256(), report.nar_sha256());
                        drop(scrub);
                        drop(report);
                        repo.flush().await.unwrap();
                    });
                }
            }
        },
    );
}

#[test]
fn concurrent_repositories_import_large_archives_with_one_worker() {
    isolated(
        "concurrent_repositories_import_large_archives_with_one_worker",
        || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let archive = fixture::archive(32 * 1024 * 1024);
            runtime.block_on(async {
                let import = || async {
                    let directory = tempfile::tempdir().unwrap();
                    let repo = Repository::local(directory.path()).await.unwrap();
                    let report = repo
                        .import(NarImport::new(archive.as_slice()))
                        .await
                        .unwrap();
                    assert_eq!(report.nar_sha256(), Sha256::digest(&archive).as_slice());
                    let scrub =
                        scrub_nar(report.reader(), report.root(), &NarRequirements::default())
                            .await
                            .unwrap();
                    assert_eq!(scrub.nar_sha256(), report.nar_sha256());
                    drop(scrub);
                    drop(report);
                    repo.flush().await.unwrap();
                };
                futures::join!(import(), import());
            });
        },
    );
}

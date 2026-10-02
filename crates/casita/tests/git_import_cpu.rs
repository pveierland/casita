#![cfg(all(feature = "native", feature = "git", feature = "experimental"))]

mod pipeline {
    use casita::ObjectKey;
    use casita::experimental::{GitObjectFormat, GitObjectKind, Repository, git_object_key};
    use casita::import::GitClosureImport;
    use std::io::Write;
    use std::process::{Command, Stdio};
    struct Source(tempfile::TempDir);
    impl Source {
        fn new(format: &str) -> Self {
            let source = Self(tempfile::tempdir().unwrap());
            source.git(
                &["init", "--bare", "-q", &format!("--object-format={format}")],
                b"",
            );
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
        fn blob(&self, bytes: &[u8]) -> String {
            self.git(&["hash-object", "-w", "--stdin"], bytes)
        }
        fn request(&self, roots: Vec<ObjectKey>) -> GitClosureImport {
            GitClosureImport::new(self.0.path().join("objects"), roots)
        }
    }
    fn key(format: GitObjectFormat, kind: GitObjectKind, oid: &str) -> ObjectKey {
        git_object_key(
            format,
            kind,
            data_encoding::HEXLOWER.decode(oid.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn shared_cpu_parallel_streams_and_chunks_progress_with_one_blocking_thread() {
        let source = Source::new("sha1");
        let expected: Vec<_> = (0..16u8)
            .map(|i| {
                let size = if i < 2 { 1048577 } else { 32768 };
                let mut state = u64::from(i) + 1;
                let body: Vec<u8> = (0..size)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        state as u8
                    })
                    .collect();
                let key = key(
                    GitObjectFormat::Sha1,
                    GitObjectKind::Blob,
                    &source.blob(&body),
                );
                (key, body)
            })
            .collect();
        let destination = tempfile::tempdir().unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap()
            .block_on(async {
                let repository = Repository::local(destination.path()).await.unwrap();
                let request = source
                    .request(expected.iter().map(|(key, _)| key.clone()).collect())
                    .with_decode_workers(4.try_into().unwrap())
                    .with_cpu_concurrency(std::num::NonZeroUsize::MIN);
                let observation = request.cpu_budget().unwrap().clone();
                let imported = tokio::time::timeout(
                    std::time::Duration::from_secs(20),
                    repository.import(request),
                )
                .await
                .expect("shared CPU admission must progress with one blocking thread")
                .unwrap();
                assert_eq!(imported.report.imported_objects, expected.len());
                assert_eq!(observation.peak_jobs(), 1);
                for (key, expected) in &expected {
                    let (_, mut reader) = imported.reader.open_payload(key).await.unwrap().unwrap();
                    let mut actual = Vec::new();
                    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut actual)
                        .await
                        .unwrap();
                    assert_eq!(&actual, expected);
                }
                drop(imported);
                repository.flush().await.unwrap();
            });
    }
}

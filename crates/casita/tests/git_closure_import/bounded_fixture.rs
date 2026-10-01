//! Fixture generation and audits use a fixed buffer, never a whole blob Vec.
use super::{Source, key};
use casita::ObjectKey;
use casita::experimental::{
    BlobStore, GitObjectFormat, GitObjectKind, MetadataStore, OwnedRetentionHold,
};
use std::io::Write;
use std::process::{Command, Stdio};
use tokio::io::AsyncReadExt;

pub(super) fn blob(
    source: &Source,
    size: usize,
    index: usize,
    content: &str,
) -> (String, blake3::Hash) {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(source.0.path())
        .args(["hash-object", "-w", "--stdin"])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut buffer = [0; 65536];
    let family = if content == "clustered" {
        index % 8
    } else {
        index
    };
    let mut state = (family as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
    let mut hash = blake3::Hasher::new();
    let mut offset = 0;
    while offset < size {
        let n = (size - offset).min(buffer.len());
        let bytes = &mut buffer[..n];
        bytes.fill(b'x');
        if content == "random" || content == "mixed" || content == "clustered" {
            for part in bytes.chunks_mut(8) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                part.copy_from_slice(&state.to_le_bytes()[..part.len()]);
            }
        }
        if offset == 0 {
            bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
        }
        hash.update(bytes);
        input.write_all(bytes).unwrap();
        offset += n;
    }
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (
        String::from_utf8(output.stdout).unwrap().trim().to_owned(),
        hash.finalize(),
    )
}

pub(super) fn parent_hwm() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:").and_then(|value| {
                value
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
                    .map(|kib| kib * 1024)
            })
        })
}

pub(super) fn expected(
    oid: &str,
    hash: blake3::Hash,
    size: usize,
) -> (ObjectKey, blake3::Hash, usize) {
    (
        key(GitObjectFormat::Sha1, GitObjectKind::Blob, oid),
        hash,
        size,
    )
}

pub(super) async fn audit<PS: BlobStore, SS: MetadataStore>(
    reader: &OwnedRetentionHold<PS, SS>,
    expected: &[(ObjectKey, blake3::Hash, usize)],
) {
    let mut buffer = [0; 65536];
    for (key, expected_hash, size) in expected {
        let (_, mut payload) = reader.open_payload(key).await.unwrap().unwrap();
        let mut hash = blake3::Hasher::new();
        let mut count = 0;
        loop {
            let n = payload.read(&mut buffer).await.unwrap();
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
            count += n;
        }
        assert_eq!(count, *size);
        assert_eq!(&hash.finalize(), expected_hash);
    }
}

// Process counters bracket only import. Logical character counts include all
// import threads and these tiny snapshots; physical I/O can lag writeback.
// The fixture and audit run outside this interval.
pub(super) fn process_io() -> Option<std::collections::BTreeMap<String, u64>> {
    let text = std::fs::read_to_string("/proc/self/io").ok()?;
    let mut values = std::collections::BTreeMap::new();
    for line in text.lines() {
        let (name, value) = line.split_once(':')?;
        if matches!(
            name,
            "rchar" | "wchar" | "read_bytes" | "write_bytes" | "cancelled_write_bytes"
        ) {
            values.insert(name.to_owned(), value.trim().parse().ok()?);
        }
    }
    (values.len() == 5).then_some(values)
}

pub(super) fn io_delta(
    before: Option<std::collections::BTreeMap<String, u64>>,
    after: Option<std::collections::BTreeMap<String, u64>>,
) -> Option<std::collections::BTreeMap<String, u64>> {
    let (before, after) = before.zip(after)?;
    after
        .into_iter()
        .map(|(name, value)| {
            let difference = value.checked_sub(*before.get(&name)?)?;
            Some((name, difference))
        })
        .collect()
}

// Linux process totals include all threads, excluding child processes. Keep raw
// ticks: the runner records SC_CLK_TCK and must not imply finer resolution.
// Parse after the final ')' because the command name may contain spaces or ')'.
pub(super) fn process_cpu() -> Option<std::collections::BTreeMap<String, u64>> {
    let text = std::fs::read_to_string("/proc/self/stat").ok()?;
    let (_, fields) = text.rsplit_once(") ")?;
    let mut fields = fields.split_whitespace();
    let user = fields.nth(11)?.parse().ok()?;
    let system = fields.next()?.parse().ok()?;
    Some(std::collections::BTreeMap::from([
        ("user_ticks".to_owned(), user),
        ("system_ticks".to_owned(), system),
    ]))
}

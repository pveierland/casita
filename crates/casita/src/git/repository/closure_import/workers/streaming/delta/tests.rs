use super::*;
use crate::spill::SpillLimits;
use std::io::Write;
use std::sync::OnceLock;

fn oid(byte: u8) -> gix::ObjectId {
    gix::ObjectId::from_bytes_or_panic(&[byte; 20])
}
fn ref_header(byte: u8) -> Header {
    Header::RefDelta { base_id: oid(byte) }
}

// Deliberately synthetic indexes: parser-valid sorted IDs/offsets, with no
// claim that fabricated IDs identify these malformed objects. Plan-only tests
// must reject their metadata before any gix traversal or native body lookup.
fn fixture(entries: Vec<(u8, Header, Vec<u8>)>) -> (tempfile::TempDir, Locator) {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("pack")).unwrap();
    let path = directory.path().join("pack/test");
    let mut pack = b"PACK\0\0\0\x02".to_vec();
    pack.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    let mut positions = Vec::new();
    for (id, header, body) in entries {
        positions.push((oid(id), pack.len() as u32));
        header.write_to(body.len() as u64, &mut pack).unwrap();
        let mut encoder = gix::features::zlib::stream::deflate::Write::new(Vec::new());
        encoder.write_all(&body).unwrap();
        encoder.flush().unwrap();
        pack.extend_from_slice(&encoder.into_inner());
    }
    pack.extend_from_slice(&[0; 20]);
    std::fs::write(path.with_extension("pack"), pack).unwrap();
    positions.sort_by_key(|(id, _)| *id);
    let mut index = b"\xfftOc\0\0\0\x02".to_vec();
    for first in 0..=255u8 {
        index.extend_from_slice(
            &(positions
                .iter()
                .filter(|(id, _)| id.as_bytes()[0] <= first)
                .count() as u32)
                .to_be_bytes(),
        );
    }
    for (id, _) in &positions {
        index.extend_from_slice(id.as_bytes());
    }
    index.resize(index.len() + positions.len() * 4, 0);
    for (_, offset) in &positions {
        index.extend_from_slice(&offset.to_be_bytes());
    }
    index.extend_from_slice(&[0; 40]);
    std::fs::write(path.with_extension("idx"), index).unwrap();
    let locator = Locator {
        roots: vec![directory.path().to_owned()],
        indexes: OnceLock::new(),
        hash: gix::hash::Kind::Sha1,
    };
    assert_eq!(locator.indexes().len(), 1, "fixture index must parse");
    (directory, locator)
}
fn probe(locator: &Locator) -> io::Result<Option<Plan>> {
    Plan::probe(
        locator,
        &oid(1),
        u64::MAX,
        Arc::new(Control::default()),
        Arc::new(tokio::sync::Semaphore::new(MAX_SOURCE_FILES)),
    )
}
fn encode_size(mut size: u64) -> Vec<u8> {
    let mut encoded = Vec::new();
    loop {
        let byte = (size & 0x7f) as u8;
        size >>= 7;
        encoded.push(byte | if size == 0 { 0 } else { 0x80 });
        if size == 0 {
            return encoded;
        }
    }
}

#[test]
fn selected_invalid_metadata_does_not_fall_back_on_a_missing_ref_hint() {
    let (_directory, locator) = fixture(vec![
        (1, ref_header(2), vec![2, 1, 1, b'a']),
        (2, ref_header(3), vec![1, 3, 3, b'a', b'b', b'c']),
    ]);
    assert!(
        probe(&locator).is_err(),
        "a proven parent/base size mismatch must not fall back when a later REF hint is missing"
    );
}

#[test]
fn declared_work_boundary_is_checked_before_ref_hint_fallback() {
    for total in [MAX_WORK_BYTES - 1, MAX_WORK_BYTES, MAX_WORK_BYTES + 1] {
        let result = total - 7;
        let mut delta = vec![0];
        delta.extend(encode_size(result));
        assert_eq!(delta.len(), 7);
        let (_directory, locator) = fixture(vec![(1, ref_header(2), delta)]);
        let outcome = probe(&locator);
        if total <= MAX_WORK_BYTES {
            assert!(
                outcome.unwrap().is_none(),
                "an unresolved hint within limits remains a miss"
            );
        } else {
            assert!(
                outcome.is_err(),
                "known excessive declared work must fail before missing-REF fallback"
            );
        }
    }
}

#[test]
fn delta_chain_depth_and_reference_cycles_are_bounded() {
    for depth in [63, 64, 65] {
        let mut entries: Vec<_> = (1..=depth)
            .map(|id| (id, ref_header(id + 1), vec![1, 1, 0x90, 1]))
            .collect();
        entries.push((depth + 1, Header::Blob, vec![b'a']));
        let (_directory, locator) = fixture(entries);
        let outcome = probe(&locator);
        if depth <= 64 {
            assert!(outcome.unwrap().is_some());
        } else {
            assert!(outcome.is_err());
        }
    }
    let (_directory, locator) = fixture(vec![
        (1, ref_header(2), vec![1, 1, 0x90, 1]),
        (2, ref_header(1), vec![1, 1, 0x90, 1]),
    ]);
    assert!(probe(&locator).is_err());
}

fn interpreter_plan(delta: Vec<u8>, base: Vec<u8>) -> (tempfile::TempDir, Plan) {
    let (directory, locator) = fixture(vec![(1, ref_header(2), delta), (2, Header::Blob, base)]);
    let mut plan = probe(&locator).unwrap().unwrap();
    // Isolate the instruction interpreter. Integration cases use real Git IDs
    // and exercise native verification; these literal fixtures test copying.
    for node in &mut plan.nodes {
        node.oid = None;
    }
    (directory, plan)
}
fn area(bytes: u64) -> SpillArea {
    SpillArea::new(
        None,
        SpillLimits {
            max_memory_objects: 1,
            max_spill_bytes: bytes,
        },
    )
}

#[test]
fn delta_interpreter_copies_backwards_and_inserts_literal_bytes() {
    let (_directory, plan) =
        interpreter_plan(vec![6, 5, 0x91, 3, 2, 0x90, 2, 1, b'!'], b"abcdef".to_vec());
    let area = area(11);
    let mut result = plan.reconstruct(&area).unwrap();
    let mut actual = Vec::new();
    result.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, b"deab!");
    assert_eq!(area.metrics().peak_bytes, 11);
    drop(result);
    assert!(
        area.payload(11).is_ok(),
        "completed spools must release their full reservation"
    );
}

#[test]
fn delta_zero_copy_length_means_65536_and_spill_boundaries_hold() {
    let mut delta = encode_size(65536);
    delta.extend(encode_size(65536));
    delta.push(0x80);
    for budget in [131071, 131072, 131073] {
        let (_directory, plan) = interpreter_plan(delta.clone(), vec![b'x'; 65536]);
        let area = area(budget);
        let result = plan.reconstruct(&area);
        if budget < 131072 {
            assert!(result.is_err());
        } else {
            let mut actual = Vec::new();
            result.unwrap().read_to_end(&mut actual).unwrap();
            assert_eq!(actual, vec![b'x'; 65536]);
        }
        assert!(
            area.payload(budget).is_ok(),
            "success and quota failures must release base and result reservations"
        );
    }
}

#[test]
fn malformed_delta_instructions_release_all_spill_capacity() {
    for delta in [
        vec![6, 1, 0],             // reserved opcode
        vec![6, 1, 0x91],          // truncated copy
        vec![6, 1, 0x91, 6, 1],    // out-of-range base copy
        vec![6, 1, 2, b'a', b'b'], // excess output
        vec![6, 2, 1, b'a'],       // short output
        vec![6, 2, 2, b'a'],       // truncated literal
    ] {
        let (_directory, plan) = interpreter_plan(delta, b"abcdef".to_vec());
        let area = area(100);
        assert!(plan.reconstruct(&area).is_err());
        assert!(area.payload(100).is_ok());
    }
}

#[test]
fn size_varints_reject_overflow_and_truncation() {
    assert_eq!(varint(&mut std::io::Cursor::new([0])).unwrap(), 0);
    let mut maximum = vec![0xff; 9];
    maximum.push(1);
    assert_eq!(
        varint(&mut std::io::Cursor::new(maximum)).unwrap(),
        u64::MAX
    );
    let mut overflow = vec![0xff; 9];
    overflow.push(2);
    for bytes in [overflow, vec![0x80], vec![0xff; 11]] {
        assert!(varint(&mut std::io::Cursor::new(bytes)).is_err());
    }
}

#[test]
#[cfg(target_os = "linux")]
fn planned_chains_do_not_keep_one_file_descriptor_per_delta() {
    let mut entries: Vec<_> = (1..=64)
        .map(|id| (id, ref_header(id + 1), vec![1, 1, 0x90, 1]))
        .collect();
    entries.push((65, Header::Blob, vec![b'a']));
    let (directory, locator) = fixture(entries);
    let path = directory.path().join("pack/test.pack");
    let plans: Vec<_> = (0..4).map(|_| probe(&locator).unwrap().unwrap()).collect();
    let open_handles = || {
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_link(entry.path()).ok())
            .filter(|target| target == &path)
            .count()
    };
    let count = open_handles();
    assert!(
        count <= plans.len(),
        "{count} source descriptors retained by {} plans for one pack",
        plans.len()
    );
    drop(plans);
    assert_eq!(
        open_handles(),
        0,
        "dropping planned work must close all source descriptors"
    );
}

#[test]
fn source_handle_admission_is_bounded_and_recovers_after_a_window() {
    let (_directory, locator) = fixture(vec![
        (1, ref_header(2), vec![1, 1, 0x90, 1]),
        (2, Header::Blob, vec![b'a']),
    ]);
    let slots = Arc::new(tokio::sync::Semaphore::new(MAX_SOURCE_FILES));
    let control = Arc::new(Control::default());
    let mut plans = Vec::new();
    for _ in 0..MAX_SOURCE_FILES {
        plans.push(
            Plan::probe(&locator, &oid(1), u64::MAX, control.clone(), slots.clone())
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(slots.available_permits(), 0);
    let failure = Plan::probe(&locator, &oid(1), u64::MAX, control.clone(), slots.clone());
    assert!(matches!(failure, Err(ref error) if handle_window_full(error)));
    drop(plans.pop());
    assert!(
        Plan::probe(&locator, &oid(1), u64::MAX, control, slots.clone())
            .unwrap()
            .is_some()
    );
    drop(plans);
    assert_eq!(slots.available_permits(), MAX_SOURCE_FILES);
}

#[test]
fn handle_window_marker_does_not_hide_operating_system_errors() {
    assert!(!handle_window_full(&io::Error::from(
        io::ErrorKind::WouldBlock
    )));
    assert!(handle_window_full(&io::Error::new(
        io::ErrorKind::WouldBlock,
        HandleWindowFull
    )));
}

#[test]
fn full_plan_window_preserves_and_retries_the_pending_key() {
    use super::super::super::SourcePool;
    use crate::format::FormatLimits;
    use crate::git::{GitObjectKind, git_object_key};
    use std::collections::VecDeque;
    let (directory, _) = fixture(vec![
        (1, ref_header(2), vec![1, 1, 0x90, 1]),
        (2, Header::Blob, vec![b'a']),
    ]);
    let pool = SourcePool::open(
        directory.path().to_owned(),
        GitObjectFormat::Sha1,
        1024,
        4,
        area(1024),
        true,
        Vec::new().into(),
    )
    .unwrap();
    let key = git_object_key(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        oid(1).as_bytes().to_vec(),
    )
    .unwrap();
    // Duplicate roots deliberately exercise the planner in isolation: public
    // traversal deduplicates roots before admission.
    let mut pending = VecDeque::from(vec![key.clone(); MAX_SOURCE_FILES + 1]);
    let limits = FormatLimits::default();
    let control = Arc::new(Control::default());
    let plan = pool
        .plan(&mut pending, MAX_SOURCE_FILES + 1, 1024, &limits, &control)
        .unwrap();
    assert_eq!(plan.len(), MAX_SOURCE_FILES);
    assert_eq!(pending, VecDeque::from([key]));
    drop(plan);
    let plan = pool
        .plan(&mut pending, MAX_SOURCE_FILES + 1, 1024, &limits, &control)
        .unwrap();
    assert_eq!(plan.len(), 1);
    assert!(pending.is_empty());
    drop(plan);
    assert_eq!(pool.source_files.available_permits(), MAX_SOURCE_FILES);
}

#[test]
fn a_blob_key_resolving_to_a_tree_delta_is_a_type_mismatch() {
    use super::super::super::SourcePool;
    use crate::format::FormatLimits;
    use crate::git::{GitObjectKind, git_object_key};
    use std::collections::VecDeque;
    let (directory, _) = fixture(vec![
        (1, ref_header(2), vec![1, 1, 0x90, 1]),
        (2, Header::Tree, vec![b'a']),
    ]);
    let key = git_object_key(
        GitObjectFormat::Sha1,
        GitObjectKind::Blob,
        oid(1).as_bytes().to_vec(),
    )
    .unwrap();
    let limits = FormatLimits::default();
    let control = Arc::new(Control::default());
    // The chain's tree base leaves delta spilling to the header fallback, which
    // reports the stored type instead of a source failure.
    for serial in [true, false] {
        let mut pool = SourcePool::open(
            directory.path().to_owned(),
            GitObjectFormat::Sha1,
            1024,
            1,
            area(1024),
            true,
            Vec::new().into(),
        )
        .unwrap();
        let mut pending = VecDeque::from([key.clone()]);
        let error = if serial {
            pool.decode_serial(&mut pending, 1, 1024, 1024, 1024, &control)
                .err()
                .unwrap()
        } else {
            pool.plan(&mut pending, 1, 1024, &limits, &control)
                .err()
                .unwrap()
        };
        assert_eq!(
            error.category(),
            crate::RepositoryErrorCategory::InvalidData,
            "{error}"
        );
        assert!(
            error
                .to_string()
                .contains("linked as a blob but stored as a tree"),
            "{error}"
        );
        assert_eq!(pending, VecDeque::from([key.clone()]));
    }
}

#[test]
fn known_native_base_and_result_identities_are_both_verified() {
    for format in [GitObjectFormat::Sha1, GitObjectFormat::Sha256] {
        for corrupt in [None, Some(0), Some(1)] {
            let (_directory, mut plan) =
                interpreter_plan(vec![6, 5, 0x91, 3, 2, 0x90, 2, 1, b'!'], b"abcdef".to_vec());
            for (index, bytes) in [b"deab!".as_slice(), b"abcdef".as_slice()]
                .into_iter()
                .enumerate()
            {
                let mut hasher =
                    NativeHasher::new(format, format!("blob {}\0", bytes.len()).as_bytes());
                hasher.update(if corrupt == Some(index) {
                    b"wrong"
                } else {
                    bytes
                });
                plan.nodes[index].oid = Some(gix::ObjectId::from_bytes_or_panic(
                    &hasher.finish().unwrap(),
                ));
            }
            let area = area(11);
            let outcome = plan.reconstruct(&area);
            if corrupt.is_none() {
                let mut actual = Vec::new();
                outcome.unwrap().read_to_end(&mut actual).unwrap();
                assert_eq!(actual, b"deab!");
            } else {
                assert!(
                    matches!(outcome, Err(ref error) if error.to_string().contains("native identity mismatch"))
                );
            }
            assert!(area.payload(11).is_ok());
        }
    }
}

#[test]
fn tiny_delta_result_still_reserves_its_full_base() {
    let base_size = 1024 * 1024;
    let mut instructions = encode_size(base_size);
    instructions.extend([1, 0x90, 1]);
    for budget in [base_size, base_size + 1, base_size + 2] {
        let (_directory, plan) =
            interpreter_plan(instructions.clone(), vec![b'x'; base_size as usize]);
        let area = area(budget);
        let result = plan.reconstruct(&area);
        if budget == base_size {
            assert!(result.is_err());
        } else {
            let mut bytes = Vec::new();
            result.unwrap().read_to_end(&mut bytes).unwrap();
            assert_eq!(bytes, b"x");
        }
        assert!(area.payload(budget).is_ok());
    }
}

#[test]
fn source_path_replacement_does_not_change_an_admitted_plan() {
    let (directory, plan) =
        interpreter_plan(vec![6, 5, 0x91, 3, 2, 0x90, 2, 1, b'!'], b"abcdef".to_vec());
    let pack = directory.path().join("pack/test.pack");
    std::fs::rename(&pack, directory.path().join("old.pack")).unwrap();
    std::fs::write(pack, b"replacement").unwrap();
    let mut bytes = Vec::new();
    plan.reconstruct(&area(11))
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    assert_eq!(bytes, b"deab!");
}

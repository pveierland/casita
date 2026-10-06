# Rotation within one mutation admission

A long writer retains object and payload pins for every publication. Opening a
separate session to release them also repeats mutation-start maintenance and
discovery. `MutationSession::rotate(&mut self)` prepares a fresh staging pin and
payload batch within the already admitted operation, preserving catalog
protection and synchronization. It replaces the old session only after all
fallible preparation succeeds.

Callers must acquire independent retention for published objects before rotating.
The API does not transfer those objects automatically. Mutable borrowing prevents
using staged objects across rotation; cancellation or failure preserves the old
session. Independent operations still use `Repository::mutation_session()`.

## Correctness

The focused tests cover maintenance/discovery admission counts, released staging
resources, failed and cancelled preparation, collection during catalog handoff,
and real packed-store readback after collection. The documentation includes a
compile-fail staged-borrow example. Validation: 857 native tests passed, 53
ignored; 32 doctests passed; 44 Python parser/CLI/all-suite checks passed. The
consumer's full CAS suite passed 76 top-level tests and eight subprocess checks.

The initial API test failed because `rotate` did not exist. Parser review exposed
libtest-prefixed output and weak JSON-type/error handling; regression tests first
failed, then passed after the parser fix. The initial suite-registration failure
and its corrected run are retained as well.

## Controlled benchmark

The permanent `mutation-rotation` suite is registered in the manifest, revision
builder and `benchmark all`. It compares long sessions, independently admitted
sessions and rotation, with 7/8/9/16/17 publications of 64 unique 256-byte blobs.
Four forward/reverse repetitions in each of two injected maintenance-eligibility
states produce 120 independently audited processes. The eligible hook performs
real collection per admission; this models eligibility deterministically and does
not measure the local 60-second cooldown.

At 17 publications, the long session reaches 1,088 object resources. Rotation and
separate sessions each reach at most 512 per writer; rotation admits once while
separate sessions admit three times. Every case verifies exact bytes after real
collection, final object reclamation and an empty pin inventory.

Timing includes staging, publication, retention handoff, admission and release
drain. Inventory inspection and final correctness audits are excluded. Eligible
9/16/17-publication rotation cases were 12.73%/8.49%/19.24% faster than independent
sessions. These are small controlled timings: the identical pre-rotation flow at
7/8 publications varies by up to 7.4%. They establish the resource/admission
mechanism, not general repository throughput or an ingestion speedup. Counts are
per writer, not bytes or an aggregate memory bound; dependencies and delayed pin
release matter in other workloads.

## Consumer qualification

Mnos uses the API between groups of eight Git conversion publications. Its
paired native tests show the following median differences against the existing
long writer. Four balanced pairs per row verified both Git-root and NAR identity.

| Scenario | Total wall | Peak RSS | Update total |
|---|---:|---:|---:|
| Near | -0.38% | -0.93% | +1.29% |
| Week, initial drifting set | +1.34% | -6.64% | +5.87% |
| Week, confirmation | +0.12% | -7.48% | +0.76% |
| Release | -0.16% | -4.48% | +0.30% |

The initial week set is retained: controls drifted from 57.666 to 47.157 seconds.
Confirmation does not invalidate it. Confirmed update medians increased by about
57–80 ms; there is no universal nonregression claim. Near RSS was mixed, including
one +1.11% pair, while every week/release pair decreased. All 32 runs observed one
pressure-marker update under faster ambient conditions than the earlier rejected
separate-session experiment. This does not demonstrate removal of its observed
cooldown-related latency event. Admission counts are independently tested.

## Reproduction and evidence

Build an optimized library test binary, then run:

```sh
cargo test --release -p casita --no-default-features --features native,git,experimental --lib --no-run --message-format=json
benchmark run mutation-rotation --profile smoke --repetitions 4 --probe-binary /path/to/casita-lib-test --no-build --output rotation.json --report rotation.md
```

The recorded build used `RUSTFLAGS='-C link-arg=-fuse-ld=mold'`; the benchmark used
`RUST_TEST_THREADS=1`. The sidecar records the exact compiler, features, source
identity and configuration. `artifacts.json` hashes every compressed and
uncompressed artifact. Raw process stdout, all medians, source snapshots, build
metadata, safety-test logs and consumer audits are retained beside this report.

# NAR decoder admission outside the storage blocking pool

NAR decoding now uses a separate, process-wide allowance of 16 native threads.
A decoder waiting on bounded input/output pipes leaves Tokio's blocking pool
available for the storage work needed by its consumer. The Mnos I/O blocking
pool default remains 64. This change addresses a demonstrated liveness failure;
it does not establish a production throughput or memory improvement.

## Baseline and candidate

The baseline is Casita `966e740113f613036f0d5a61e8cf043100f75313` with Mnos
`e82112466b3f819720d8b9003bbfeb36ed9b8d53`. Existing Turso WAL work remains part
of that baseline. One frozen System-allocator debug executable tests fresh
persistent repositories through both direct NAR ingestion and the actual raw
server Substitute protocol. Each successful case verifies every payload byte.

| Regular file size | One blocking worker, direct/server | Two blocking workers, direct/server |
| --- | --- | --- |
| 31 bytes | Both pass | Both pass |
| 2 MiB | Both pass | Both pass |
| 8 MiB | Both pass | Both pass |
| 32 MiB | Both exceed the 8-second watchdog | Both pass |

The two timed-out cases receive SIGTERM and exit with -15; neither requires
SIGKILL. Baseline executable SHA-256 is
`f73f14b1761b7e8b7a46a1d884e1c2ee36da0a06e93480bf4253303a7f9636d2`.
With the candidate, all eight combinations of 8/32 MiB, one/two workers and
direct/server ingestion complete and verify. Its 32 MiB, one-worker cases take
about 2.52 seconds direct and 2.62 seconds through the server. These durations
are liveness observations, not balanced performance comparisons.

The source dependency is a synchronous parser occupying `spawn_blocking` while
waiting on `SyncIoBridge` pipes or the event queue. The async consumer can need
that same pool for hashing, compression or durable metadata work. A bounded
pipe alone does not predict the failing input size: downstream buffering lets
smaller files complete. No stack sample identifies the exact stopped operation,
no infinite wait is proven by a finite watchdog, and no exact threshold between
8 and 32 MiB is claimed. Two workers succeeding for one import is not a general
concurrency guarantee.

## Worker lifetime

A private helper asynchronously acquires one of 16 shared semaphore permits,
then launches a fallible named native thread. It captures the Tokio handle
before admission and enters that context inside the panic boundary. The handle
provides context, not runtime ownership. The original parser, pump, consumer,
validation and error precedence remain in place.

Queued cancellation launches no thread. An active worker owns its permit until
its callback and abandoned result cleanup finish. Dropping the caller closes
its pipe peers; there is no synchronous join in async Drop. A stalled input
holds a slot until it completes or is abandoned. Thread launch, completion
channel and panic failures become storage errors. Sixteen is a resource bound,
not a measured optimum. This lane is separate from Mnos acquisition admission,
which can already be occupied by the NAR download that invokes the parser.

## Validation

Final test results and executable hashes are recorded in `results.json` and the
archived raw logs. The targeted suites cover:

- 54 NAR library tests, including cancellation, malformed/late input, publication,
  crash/reopen and the three new worker lifecycle tests. The shared-capacity
  test uses a local 16-slot semaphore with the same helper across two runtime
  contexts; it is not 16 concurrent public repository imports. It checks queued
  cancellation, active abandonment, retained capacity and later queued progress.
  The teardown test requires an independent successful EOF acknowledgement;
  panic recovery also checks that subsequent work can acquire capacity.
- Two public durable-import integration tests: 8/32 MiB under one/two blocking
  workers, and two concurrent 32 MiB imports into different repositories with
  one blocking worker. NAR size/hash, payload accounting and stored-tree scrub
  are checked as applicable; retained reports are dropped before flush.
- Eight Mnos server admission tests, including a permanent 32 MiB one-worker
  Substitute regression that checks the registered path and all stored bytes.
- Eighteen existing Mnos substitution/knowledge/latency tests with the explicit
  installed Nix 2.34.8 reference. One pre-existing external-network test remains
  ignored. The reference's binary SHA-256 and source/derivation are recorded;
  this does not claim a different unavailable flake package was tested.
- Four permanent Criterion cases in correctness-test mode. They are part of
  the existing registered `nar_import` benchmark and therefore benchmark all:
  `nar_decoder_pool/workers-{1,2}/{8388608,33554432}`. Setup, SHA/size checks,
  scrub and flush are outside the import timer. This run validates the corpus;
  it does not compare benchmark throughput before and after the change.

Run the permanent tests with the repository environment:

```sh
cargo test -p casita --test nar_decoder_admission
# Bound a standalone benchmark invocation; benchmark all already includes it.
timeout 600 cargo bench --bench nar_import -- nar_decoder_pool
# In Mnos's evaluator workspace, using this Casita revision:
cargo test -p mnos-eval-server --test acquire_admission
```

Actual recorded Casita native builds use locked offline release artifacts with
`native,git,experimental,cli`, debug level 1 and LTO disabled. Mnos uses its
normal debug test profile. Their lockfiles use Tokio 1.53.2 and 1.53.1,
respectively. No cross-profile performance inference is made.

## Provenance and limits

`artifacts.json` contains original/compressed SHA-256 pairs. The archived audit
script recomputes `results.json` from those artifacts alone. Exact source maps,
lockfiles, build logs, process watchdog runners and executable hashes are
included; large binaries and persistent diagnostic stores remain outside Git.
Run the extracted audit with `python3 audit-nar-decoder-admission.py REPORT_DIR`.

The initial candidate protocol binds three Casita runtime source files. Its
complete Mnos source snapshot was saved after that matrix and before removing
the temporary probe, rather than being bound by the pre-run protocol. The probe
matches the baseline; the only added test was the permanent large-file case.
Final rebuilt tests qualify the permanent source separately. After the test
builds, rustfmt moves the private `mod decoder` declaration before the other
module declarations in `nar.rs`. Both tested and delivered source maps are
retained; the audit checks this exact formatting-only transform and equality
of every other file. Tests are not repeated for that module-order formatting
change. Initial/final helper differences are also formatting only.

The first new native integration run failed at fixture cleanup: its scrub
report still retained a reader when flush attempted a WAL checkpoint. Dropping
both reports before flush fixed the tests and benchmark. That failure and the
initial invalid `cargo bench --release` invocation are retained. Cleanup journals
record removal of this task's inactive build caches/executables; frozen failure
controls and diagnostic CAS stores were preserved.

This is targeted validation, not a full-workspace run. Actual OS thread-launch
failure was not injected. No universal one-worker safety, optimum decoder count,
smaller global blocking default, production allocation ownership, historical OOM
resolution or completion of the broader ingestion investigation is claimed.

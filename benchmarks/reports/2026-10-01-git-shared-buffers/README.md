# Partitioned Git producer-buffer admission

> Historical evidence from original commit `31d297386e494cad370ec273a7e494ab669a189a`. These measurements and archived build artifacts describe the original source and dependency set, not the newly extracted branch. Reproduce the historical measurements using the predecessor, exact patches and lockfiles recorded below; validate the extracted branch separately.

**Retain as an opt-in memory control.** One cloneable `ImportBufferBudget` shares
separate source and chunked-writer allowances across selected imports. Defaults
remain disabled. The
benefit is a substantial reduction in concurrent import memory, with generally
slower imports. This is not a general throughput optimization.

## Decision and measurements

The report contains **1,040 audited samples in 104 setting/phase cells**, each
with five alternating pairs. It compares neither coordinator, CPU only, buffers
only and both; one/two/four concurrent imports; existing source/chunk tuning;
local and memory destinations; random and clustered packed objects; and small,
large and source-streaming-boundary files. All imports in each concurrent group
receive independent identity and streaming payload audits outside timing.

These are **cold pre-audit process HWM** observations. Later phases inherit
previous peaks and do not establish phase-local memory savings. Except where
noted, each import contains 16 files of 1 MiB + 1 byte, with four source workers
and 16 MiB in each shared partition. Positive time reductions mean faster;
negative values mean slower. Paired medians are not ratios of marginal medians.

| Comparison | Imports | Baseline → candidate HWM (MiB) | Median paired saving (MiB) | Minimum saving | Paired time reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| Buffers only | 1 | 80.81 → 56.92 | 23.75 | 22.77 | -27.2% |
| Buffers only | 2 | 150.23 → 84.93 | 63.93 | 57.72 | -23.2% |
| Buffers only | 4 | 258.07 → 138.59 | 121.39 | 114.49 | -46.0% |
| Same executable, admission off/on | 4 | 259.41 → 136.78 | 123.38 | 109.20 | -40.7% |
| Add buffers to CPU limit four on both sides | 4 | 222.50 → 136.75 | 85.75 | 78.16 | -34.6% |
| Existing controls: one decoder, 4 MiB window, one upload | 4 | 184.27 → 144.27 | 39.40 | 33.26 | -39.6% |
| Existing controls: one decoder, 4 MiB window, four uploads | 4 | 183.91 → 146.27 | 38.09 | 32.02 | -32.2% |
| Clustered deltas | 4 | 283.56 → 176.32 | 102.27 | 99.43 | -25.4% |
| Memory destination | 4 | 143.23 → 94.14 | 47.41 | 42.18 | +6.1% |

Every cold pair in these rows saves memory. The same-executable comparison
supports an admission effect beyond build-layout differences. The CPU-controlled
comparison shows incremental value beyond the already retained CPU coordinator.
The lower-concurrency controls establish additional aggregate value even after
reducing existing per-import/per-writer limits. Memory storage bypasses the
chunked destination, so source admission contributes independently; these data
do not isolate the destination partition's contribution on its own.

There are important unfavorable cases. Four imports of 64 × 1 KiB files save
only 3.54 MiB at the paired median while taking 109.2% longer. Four imports of
4 × 4 MiB files save 28.02 MiB and take 33.7% longer. This is why tight shared
allowances remain explicit rather than enabled automatically.

## Disabled overhead and host activity

Disabled small-file cold medians improve 30.1% for one import and 1.6% for four,
but the one-import subtree change regresses 14.0% (10.29 → 11.97 ms marginal
medians). Disabled large-file cold improves 6.9% for one import and initially
regresses 10.3% for four. The latter does not repeat: the additional five pairs
have a +1.4% reduction, ranging from -7.8% to +3.5%.

The repeat still has mixed incremental timings: warm -10.8%, subtree change
-5.3%, and wide change -1.6% paired reductions. Their marginal medians are
24.12 → 24.54 ms, 32.78 → 34.90 ms, and 34.73 → 33.52 ms respectively. These
results do not establish a zero-cost disabled path or a uniform regression.
No unfavorable samples were discarded.

No builds or other benchmarks from this task overlapped timing. **Unrelated
builds did overlap parts of the matrix**, including the initial disabled and
buffer-only cases and existing-concurrency controls. Raw host observations and
[host-summary.csv](host-summary.csv) preserve this limitation; latency changes
must be interpreted in that context. The incremental-over-CPU, tiny, large,
clustered, memory-backend and disabled-repeat cases have no recorded competing
build intervals. Thus the decision also has a quiet local comparison saving
85.75 MiB in addition to CPU admission, with every pair saving at least 78.16 MiB.
Absence of a recorded build is not a claim that the machine was otherwise idle.

## Admission, progress and thresholds

Capacities round down to 64 KiB and reservations round up. An impossible
reservation fails rather than saturating a semaphore request. Source and
destination partitions never borrow: a retained source window cannot consume
the allowance needed to drain it into a writer.

Source admission reserves retained body allowance, per-object reader buffers,
and source-worker/planning scratch before decode dispatch. Oversized buffered
fallbacks fail before their retained body is decoded; eligible oversized streams
can run using the fixed reader allowance. No guard lives in a reusable source
pool. Guards accompany queued/running work and completed unreceived outputs.

Each writer reserves its complete conservative envelope before opening: duplex,
head, chunker and Bao buffers plus plaintext/compression allowances for all
effective in-flight chunks. Concurrency is clamped using total partition
capacity, never racing currently free bytes. This avoids a second shared wait
inside an intermittently polled writer, but can serialize writers and cause
head-of-line blocking. Existing chunk-budget waits continue to drive uploads.

At the default 256 KiB chunk average, one upload needs a rounded writer allowance
of 10,944,512 bytes. The permanent boundary tests exercise minimum-1/at/+1,
source minimum-1/at/+1, capacity rounding, oversized fallback, cancellation and
one-CPU/one-blocking-thread progress. The minimum source case is specifically
configured for one reader/worker; it is not a universal minimum for every request.

At the throughput fixture's 64 MiB logical source window, four decoders and
32 uploads per writer, complete allowances are 67 MiB per source window and
57 MiB per writer. The standard corpus covers one byte below/at/above the
134 MiB source / 114 MiB destination transition. All 15 cold gates observe one
complete allowance below it and two at/above it; every import releases both
partitions before readback. See [concurrency-boundary-gates.json](concurrency-boundary-gates.json).
The below/at/above cold comparisons save median 135.07/110.69/117.41 MiB, with
paired time reductions -40.6%/-26.9%/-15.7%. At and above have the same rounded
capacities; their different timings are observations, not another threshold.

The guarantee concerns **reserved envelopes for enumerated producer payload
buffers**, not allocator measurements or process RSS. Gix workspace/cache,
verification buffers, manifest/index metadata, codec workspace, allocator
overhead/reallocation transients and backend-owned payloads are excluded.
Encoded bytes transferred to ObjectStore or pack staging become backend-owned;
detached puts, persistent memory storage and pack caches may outlive producer
cancellation. A reservation must not follow a payload into permanent backend
storage, which could prevent the flush needed to release it. Ordinary uploads
conservatively retain the reservation through completion.

## Correctness and final integration

- 14 buffer/coordinator and actual pipeline tests; seven existing CPU tests.
- 38 Git-closure unit tests, including three new reader-lifetime tests.
- 98 chunked-writer unit tests, plus 20 isolated batching-test repetitions.
- Both matched executables: 24 Git integration tests. Additional custom-verifier,
  blob-alias and mutation-session targets: 11 tests.
- Strict Clippy with and without Git, Rust formatting, and 36 relevant Python
  runner tests. Both new suites pass through `benchmark all` (64 throughput
  smoke samples and six diagnostic boundary cases).

The broader unit run exposed a preexisting scheduling-sensitive assertion:
requiring at least a 50% hash-job reduction failed at 102/189 and 108/189 jobs.
Payload correctness passed, and isolated runs alternated between pass and fail.
The separately committed test correction (`15c735b`) requires actual batching
and count/byte bounds while preserving exact chunk identities and readback.
The full chunked group and 20 repeats pass. Production behavior was not changed.
The initial failure, isolated reproduction and corrected results are archived.
An earlier fixture-only JSON macro recursion-limit error was corrected before
freezing candidate v2; the failed build is archived and was not measured.
One existing unused `fuzz_decode` warning remains in the default-feature unit
build; the strict native lint configurations pass.

## Reproduce and verify provenance

Both timed libraries and probes were freshly compiled from predecessor
`5e73034e2ecb528c7bb22d81f163018ca5d5499f` in distinct source/target directories,
with identical fixtures, lockfile, Rust 1.96.0, features and flags. Baseline and
candidate source patches, build manifests, compiler artifact records and logs
are in [artifacts](artifacts). The later test-only correction does not change
any production code in the measured candidate; its patch and source-copy
comparison are retained separately.

Create two checkouts at that predecessor. Apply `baseline.patch.gz` to the
baseline and `candidate.patch.gz` to the candidate after decompression. Restore
the archived `Cargo.lock` in both. Build with separate target directories:

```sh
cargo test --offline --locked --release -p casita --no-default-features \
  --features native,git,experimental --test git_closure_import \
  --no-run --message-format=json
```

Use the adjacent delta-spill report's `freeze-probe.py` helper to retain each
executable, fixture/lockfile fingerprints and source identity. Then, from this
repository with its benchmark Python modules available:

```sh
python3 benchmarks/reports/2026-10-01-git-shared-buffers/matrix.py \
  --baseline BASELINE --candidate CANDIDATE --output-dir NEW_RESULTS
python3 benchmarks/reports/2026-10-01-git-shared-buffers/repeat.py \
  --baseline BASELINE --candidate CANDIDATE --output-dir NEW_RESULTS
```

The exact argument ledgers, raw JSON, host activity, [summary.csv](summary.csv)
and boundary gates are retained. `summarize.py` reads results beside itself;
copy it alongside a reproduced result set to summarize that set. Add the
archived test-only correction when reproducing final unit validation, using
normal default features for the library tests. Boundary integrations build with
`--no-default-features --features native,git,experimental --test git_import_buffers`.
The suites are permanently registered as `git-shared-buffers` and
`git-shared-buffer-limits`. `SHA256SUMS` covers retained report evidence except
itself and generated Python caches; large logs and source patches are compressed.

## Extraction provenance

`ORIGINAL-SHA256SUMS` retains the original checksum entries for included artifacts. `SHA256SUMS` covers their current retained contents, including this notice and neutralized unrelated host-process labels. `EXTRACTION.json` records original and extracted hashes for edited host records and the original checksum-list hash. Numerical measurements and compressed source/build artifacts for this feature are unchanged. Unrelated downstream project artifacts and claims are omitted from this extracted report.

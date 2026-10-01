# Shared Git import CPU admission

> Historical evidence from original commit `5e73034e2ecb528c7bb22d81f163018ca5d5499f`. These measurements and archived build artifacts describe the original source and dependency set, not the newly extracted branch. Reproduce the historical measurements using the predecessor, exact patches and lockfiles recorded below; validate the extracted branch separately.

**Retain shared admission as an opt-in resource control.** A cloneable
`ImportCpuBudget` coordinates source decoding, bounded inflation/reconstruction,
and supported destination chunk hashing/compression across selected imports.
Unconfigured requests preserve unrestricted dispatch; the default source worker
count remains one. There is no automatic CPU-count heuristic.

The hard property limits admitted blocking jobs, not total CPU utilization,
retained bytes, or process RSS. Inline FastCDC, Bao hashing, asynchronous native
verification, storage operations and unrelated jobs are excluded. Waiting jobs
can retain inputs; byte admission is a separate concern. Custom backends that
spawn writer creation must explicitly propagate the selected scope.

## Decision and measurements

**1,000 audited samples in 100 setting/phase cells**, each with five alternating
pairs, cover cold/warm/subtree-change/wide-change imports, one/two/four concurrent
imports, source workers one/four, local and memory destinations, random/clustered
packed data, and shared limits zero/one/three/four/five. File sizes below/at/above
1 MiB cover the source-streaming boundary; 1 KiB and 4 MiB are controls. Every
sample measures combined completion time for the entire concurrent group and
audits every destination outside timing. No division by import count is used.

The initial 840 samples use two freshly compiled executables based on
`df8bc19307e78ff3662801e36a5034d79e942d56`, separate source/target directories,
identical fixtures, lockfiles, Rust 1.96.0, features and flags. Supplemental
controls add 160 samples, including admission off/on in the same candidate
executable. Exact source patches, compiler artifact records, hashes, logs and
commands accompany the raw results. No builds or other benchmarks from this task
overlapped any measurements. All runs used CPU affinity 0,1,2,3.

The following are **cold pre-audit process HWM** observations. Later phases
inherit earlier high-water marks and cannot establish phase-local memory savings.
Each import in these rows has 16 files of 1 MiB + 1 byte; all rows use four
concurrent imports and CPU limit four. Positive time reductions mean a faster
candidate; paired reductions need not equal ratios of separate medians.

| Control | Source workers per import | Baseline → candidate HWM (MiB) | Median paired HWM saving (MiB) | Minimum saving | Median paired time reduction |
|---|---:|---:|---:|---:|---:|
| Local random | 4 | 275.43 → 233.91 | 41.52 | 27.27 | -2.0% |
| Local random repeat | 4 | 268.22 → 229.36 | 41.39 | 4.23 | +8.2% |
| Same executable, off/on | 4 | 270.04 → 233.03 | 39.45 | 7.53 | +3.6% |
| Existing serial-source control | 1 | 253.77 → 228.94 | 25.67 | 14.34 | +13.2% |
| Local clustered deltas | 4 | 288.27 → 248.57 | 37.33 | 35.56 | +4.8% |
| Memory destination | 4 | 145.81 → 105.68 | 40.13 | 36.20 | -2.2% |

Every cold pair in these six cells saves memory. The same-executable comparison
supports a scheduling effect beyond build-layout differences. The serial-source
control shows useful savings even with existing source-worker tuning. The memory
backend bypasses chunked destination work, so source scheduling contributes on
its own; these results do not isolate the destination gate's incremental benefit.

One- and two-import local cases save median 5.21/4.85 MiB, with one negative
memory pair in the latter. Four-import limits 1/3/5 save median
56.76/52.35/54.20 MiB. These are observations, not guaranteed byte limits.
The serial-source 4 MiB case saves only 0.20 MiB at the paired median. There is
no universal memory or latency improvement for every workload.

## Costs and uncertainty

Enabled tiny-file cold imports regress 11.4%/22.0% at one/four source workers.
The disabled one-import, one-worker tiny cold control regresses 13.6% initially
and 4.4% on repeat; its wide-change regression persists at 8.5% then 10.6%.
The repeated cold medians are 14.99 → 15.65 ms; wide-change medians are
10.67 → 12.27 ms. Retaining the feature accepts this measured overhead against
its concurrent memory benefit and explicit admission property. Do not claim a
zero-cost disabled path or enable this automatically for tiny workloads.

Warm phases execute no observed blocking jobs, and their timing varies too.
The initial four-import local cold result has two faster pairs out of five;
its repeat and same-executable control have three. This evidence supports
resource control and the repeated memory reduction, not a general speedup.
Process CPU observations have 10 ms resolution; full paired ranges and added
CPU ticks remain in `summary.csv` and the raw data.

The host was shared. The initial disabled and shared-four matrices recorded
no competing builds, but the CPU/source-boundary, tiny, large, clustered and
supplemental cases encountered substantial competing compilation. For example,
all 59 intervals of the local repeat and all 59 of the same-executable control
observed competing builds. `host-summary.csv` and raw host samples preserve
that context. The quiet initial shared-four memory result also repeated under
contention; small timing changes should not be generalized.

## Progress and cancellation

A first implementation awaited contended CPU admission inside intermittently
polled compression futures. A duplex writer could stop polling that future
while awaiting source input; semaphore capacity assigned to the unpolled waiter
then prevented the source from progressing. A real one-CPU, one-blocking-thread
pipeline and a direct unpolled-caller regression reproduced the deadlock.

The corrected implementation eagerly dispatches contended admission in an
owned asynchronous task. Dropping its owner cancels waiting work; submitted
blocking jobs retain their private slots and buffer guards until completion.
CPU permits end inside the blocking closure, before completed output or storage
waits. Partial hash batches cancel when all receivers close. Source completion
uses a nonblocking per-window channel when shared admission is selected; the
existing object-count and byte window still bound its contents. Cleanup drains
only the import's private source slots, never another import's shared budget.

All 1,000 samples pass exact import/reuse/closure counts, independent BLAKE3 and
exact streamed readback, paired root identity, real delta-count checks,
all-destination audit counts, and observed shared CPU limits. Additional checks:

- 10 shared source/writer admission unit tests, including paused storage.
- 7 coordinator and actual pipeline tests, including one-thread progress,
  queued cancellation, submitted/unreceived outputs and an unpolled dispatcher.
- 35 Git closure unit tests and 106 chunked-writer unit tests (one ignored each).
- Both variants: 24 Git integration tests (one ignored).
- 30 relevant Python corpus/runner tests, strict Clippy with/without Git, and
  formatting checks. The new suite passes through `benchmark all`.

## Reproduce

Create two separate checkouts at the predecessor above. Apply
`artifacts/baseline.patch.gz` or `artifacts/candidate-v2.patch.gz` respectively,
and restore each archived `Cargo.lock`. The patches contain the identical new
fixture; the candidate patch also includes the implementation and tests. Use
separate Cargo targets and the compiler/flags recorded in each build manifest:

```sh
cargo test --offline --locked --release -p casita --no-default-features \
  --features native,git,experimental --test git_closure_import \
  --no-run --message-format=json > build.jsonl
python3 benchmarks/reports/2026-10-01-git-delta-spill/freeze-probe.py \
  "$PWD" build.jsonl "$FROZEN_BINARY"
```

The freezer requires a freshly compiled artifact and saves its source and build
fingerprints. Once all builds have finished, use the retained runner scripts:

```sh
python3 benchmarks/reports/2026-10-01-git-shared-cpu/matrix.py \
  --baseline "$BASELINE" --candidate "$CANDIDATE" --output-dir "$NEW_RESULTS"
python3 benchmarks/reports/2026-10-01-git-shared-cpu/repeat.py \
  --baseline "$BASELINE" --candidate "$CANDIDATE" --output-dir "$NEW_RESULTS"
```

Copy `summarize.py` into the result directory and run it there. Both command
ledgers retain exact arguments. The benchmark is permanently registered as
`git-shared-cpu` in the manifest, revision runner and `benchmark all`; the guide
at `benchmarks/git-shared-cpu.md` describes broader profiles. `SHA256SUMS` covers
all retained evidence, including decompressed-source provenance through the
archived patch hashes. Executables are identified by hash rather than committed.

## Extraction provenance

`ORIGINAL-SHA256SUMS` retains the original checksum entries for included artifacts. `SHA256SUMS` covers their current retained contents, including this notice and neutralized unrelated host-process labels. `EXTRACTION.json` records original and extracted hashes for edited host records and the original checksum-list hash. Numerical measurements and compressed source/build artifacts for this feature are unchanged. Unrelated downstream project artifacts and claims are omitted from this extracted report.

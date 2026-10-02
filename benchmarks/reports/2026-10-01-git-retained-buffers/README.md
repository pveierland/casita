# Historical retained-buffer evidence

These original measurements used the recorded post-streaming, opt-in-delta-spill
source and dependencies on 2026-10-01. The representation change is extracted
earlier here, before those source-reader features. Its original timings are not
measurements of this reordered branch. Archived commands target the original
checkout and may use later options absent from this stage. Numeric observations
remain unchanged; extraction-provenance.json records limited process-name
redactions and the compatibility change to the observation helper.

# Release excess Git decode capacity

**Retain the normalization change for its cold-import memory benefit.** Completed
Git bodies use boxed slices before waiting in serial windows or parallel staging
queues. The pinned gix decoder reserves space for two maximum-size base/result
buffers plus delta instructions, then truncates length without shrinking capacity.
The eight-line runtime change releases that excess allocation capacity at the
handoff. It changes no identities, verification, chunk boundaries, or public API.

This is a memory improvement with mixed latency effects, not a general speedup.
Normalization may reallocate/copy. It does not bound decode-time workspace,
conversion transients, allocator-held pages, caches, or process RSS. The default
remains one source worker; delta spilling remains separately opt-in.

## Matched evidence

There are **840 audited matched import samples** in 84 phase/setting cells.
The initial matrix contains 720 samples; two targeted negative-control repeats
add 120. Each cell has five alternating pairs. Baseline is committed predecessor
`f06efa07a5b14f9799336e018b83edc51ccc299e`; the candidate has only the body
normalization runtime patch. Both use the same fixture, Rust 1.96.0, features,
flags and lockfile, separate source/target directories and freshly compiled,
frozen executables. No builds or other benchmarks from this task overlapped
timing. Exact source and executable fingerprints accompany the raw evidence.

Cold HWM observations are taken before streamed readback/audits. **Only cold
samples support memory conclusions:** later phases inherit the process high-water
mark. The table reports separate variant medians; paired time reductions are
computed within each pair, so they need not equal ratios of separate medians.
Positive time reductions mean faster candidate imports.

| Case | Workers | Baseline → candidate HWM (MiB) | Median paired time reduction |
|---|---:|---:|---:|
| 16 × 1 KiB | 1 | 22.8 → 22.4 | +5.2% |
| 16 × 1 KiB | 4 | 22.9 → 22.7 | +5.0% |
| 16 × (1 MiB − 1) | 1 | 128.6 → 125.9 | +2.6% |
| 16 × (1 MiB − 1) | 4 | 124.0 → 118.8 | -3.0% |
| 16 × 1 MiB | 1 | 93.1 → 85.9 | -3.0% |
| 16 × 1 MiB | 4 | 98.8 → 90.2 | +8.4% |
| 16 × (1 MiB + 1) | 1 | 95.8 → 86.4 | +1.3% |
| 16 × (1 MiB + 1) | 4 | 99.1 → 92.0 | +12.7% |
| 16 × 4 MiB | 1 | 254.7 → 223.5 | -4.3% |
| 16 × 4 MiB | 4 | 241.5 → 211.7 | +5.9% |
| 64 × 1 MiB | 1 | 206.0 → 176.7 | +7.3% |
| 64 × 1 MiB | 4 | 198.3 → 182.3 | +6.0% |
| 64 × 1 MiB repeat | 4 | 207.4 → 179.0 | -8.8% |

For 16 × 4 MiB, all ten cold pairs reduce memory. Median paired savings are
32.55/29.79 MiB at one/four workers; minimum savings are 21.27/15.82 MiB.
For dense sources, serial median paired savings are 38.01 MiB (minimum 5.21).
The initial four-worker dense case is less consistent: median 7.64 MiB, minimum
−9.37 MiB. Its repeat saves a median 29.92 MiB, minimum 16.04 MiB, across all
five pairs. Ordinary random loose/packed controls show no consistent memory
benefit, as expected when large delta workspace is absent.

The clustered threshold cases contain both buffered deltas and eligible base
objects. Crossing 1 MiB changes streaming eligibility for the base objects;
it is **not** a clean whole-fixture buffered-versus-streamed switch. Clean
source-streaming boundary controls also remain in the permanent source-inflation
corpus. The retained-buffer suite covers both sides of the boundary and its
standard profile includes tiny/small/large files and 16/64-file sources.

## Costs and negative results

The serial ordinary packed cold control initially regresses 37.6%; its repeat
improves 1.5%. The dense four-worker cold case changes from +6.0% to −8.8% on
repeat. Neither supports a universal latency conclusion. The repeated dense
wide-change regression persists: −17.6% initially and −8.2% on repeat, with
median paired additions of 3.36 ms and 0.89 ms. Serial dense subtree changes
regress 6.3% (0.79 ms), with all five pairs slower. These costs are retained and
accepted against the useful memory reduction, not dismissed as zero overhead.

CPU ticks cover all process threads around import and exclude Git children.
Resolution is 10 ms. Dense four-worker repeated cold imports add a median four
CPU ticks; repeated wide changes add a median zero ticks, which is too coarse
to establish equal CPU cost for that short phase. All wall/CPU ranges, phases,
failed-sign pairs, and I/O observations remain in `summary.csv` and raw JSON.

The host was shared. Competing builds appeared in 264/269 monitored intervals
for the 4 MiB case, 295/359 for the original dense case, and all 24 ordinary-control
intervals. Neither repeat observed competing builds, but the host was still not
isolated. `host-summary.csv` and raw host observations retain the full context;
small timing differences should not be generalized.

## Remaining benefit of spilling

The final initial-matrix comparison uses the **same normalized binary** with
spilling off/on. Cold 16 × 4 MiB HWM falls from 228.0 to 122.5 MiB with one
worker and from 210.9 to 113.0 MiB with four. All ten pairs improve memory;
minimum paired savings are 98.52/91.26 MiB. Median paired time reductions are
−16.3%/−31.5%, with roughly 64 MiB of additional logical writes. Spilling still
has a substantial independent memory benefit after normalization, with latency
and I/O costs that support keeping it opt-in.

## Correctness and packaging

All timed samples pass exact closure/import-reuse gates, independent BLAKE3 and
streamed payload readback, paired root identity, actual delta-count checks, and
requested worker/spilling-setting checks. Existing native-identity, ownership
and cancellation regressions cover the representation-only runtime change.
Both variants pass all 24 integration tests (one ignored); the candidate passes
48 Git unit tests (three ignored), strict Clippy, formatting, and 43 relevant
Python runner tests. The new suite passes through `benchmark all` and is
registered in the manifest and revision runner.

A review found that help-only use of the observation helper created an empty
host-evidence file. A regression first reproduced that behavior, then passed
after the fix; a normal four-sample helper smoke also passes. The exact helper
used during the matched matrix is archived as `artifacts/profile-measured.py.gz`.
No benchmark implementation changed while measurements were running.

## Reproduce

Create two separate source checkouts at
`f06efa07a5b14f9799336e018b83edc51ccc299e`. Apply
`artifacts/candidate.patch.gz` to the candidate and restore each archived
`Cargo.lock`. Use distinct Cargo target directories, Rust 1.96.0 and the
features/flags recorded in the build manifests. In each checkout:

```sh
cargo test --offline --locked --release -p casita --no-default-features \
  --features native,git,experimental --test git_closure_import \
  --message-format=json > build.log
python3 benchmarks/reports/2026-10-01-git-delta-spill/freeze-probe.py \
  "$PWD" build.log "$FROZEN_BINARY"
```

The freezer requires a newly compiled artifact and retains its patch, lockfile,
fixture, compiler and executable hashes. Keep the frozen baseline and candidate
in separate paths. Once all builds have ended, replay the exact matrix:

```sh
python3 benchmarks/reports/2026-10-01-git-retained-buffers/matrix.py \
  --baseline "$BASELINE" --candidate "$CANDIDATE" --output-dir "$NEW_RESULTS"
```

Repeat the two targeted controls into the same fresh results directory with
`matrix.py --repeat-controls` and the same baseline/candidate/output arguments.
`matrix-commands.json` and `supplemental-commands.json` preserve every executed
comparison command. `profile.py` records
unrelated host activity without stopping other users' processes. The final case
uses the same normalized binary for both variants, changing only spilling.
Copy `summarize.py` into the new result directory and run it there to recompute
the phase/worker-specific CSVs. The script checks complete paired samples,
correctness gates, CPU/I/O observations, matching roots, and actual delta counts.
`SHA256SUMS` covers the retained evidence; build logs and patches are compressed
with deterministic gzip metadata.

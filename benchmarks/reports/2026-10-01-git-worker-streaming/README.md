# Historical worker-streaming evidence

These results describe the original recorded binaries and environment from
2026-10-01, before extraction onto updated upstream dependencies. Archived patches
and commands reproduce that historical experiment; timings are not measurements
of this extracted branch. All original data files remain unchanged.

# Streaming parallel Git decode results into staging

Decision: retain for parallel decode windows. Three of four uniformly large-file
configurations improve in all five pairs: 10.1–17.1% median paired import-time
reductions with four decode workers on both sides. This is a workload-specific
throughput improvement, not a general speedup. Small-file and incremental costs
remain visible below; no isolated memory improvement is claimed.

## Change and correctness

Each worker now sends a verified decoded object to asynchronous staging as soon
as it finishes. The existing admitted byte/count window remains fixed; no next
window starts until current workers and successful writers finish. Source
handles are returned after joining every job. A failed staging stream drops its
receiver before joining blocked senders, preserving the original error and
preventing metadata publication for the failed window. Custom verifiers remain
on the async runtime. The one-worker and single-object decode paths retain their
existing admission and decoding sequence. Each individual body is still fully
inflated before handoff; this is not incremental object inflation.

Three deterministic unit tests failed at the intended overlap assertion before
the change, then passed on the final source. They cover both Git hash formats and
staging-error cleanup with a paused source worker. All 26 integration tests
passed (three benchmark probes ignored), including custom verifiers, native
identity rejection, aliases, verified streams and a strengthened one-blocking-
thread test admitting 16 objects through a two-slot channel. All 31 runner tests
passed. The registered `git-worker-streaming` suite passed through `benchmark
all` with 128 audited smoke samples. Smoke timings overlap compilation and are
excluded from performance evidence.

## Measurements

The main matrix has 1,440 samples: 36 configurations, four operations, five
alternating baseline/candidate pairs. A focused local 64 KiB repeat adds 80
samples; a focused serial packed/local repeat adds 40. All 1,560 samples pass
exact import/reuse counters and exhaustive closure verification; paired roots
match. No experiment compiler was running during these measurements. Both
variants use CPUs 0–3, the same Rust 1.96 compiler, flags, features and Cargo.lock.
The shared host is not isolated; several timing ranges are wide.

The following cold imports contain sixteen distinct 4 MiB random files, use a
64 MiB admitted window and concurrency 16. Cold means a fresh destination, not
an OS-cold source: fixtures are created before timing. Time includes import;
fixture creation and audits are excluded. Reduction percentages are medians of
paired reductions, not ratios of the separate time medians.

| Destination | Source | Baseline median ms | Candidate median ms | Paired reduction | Pair range |
|---|---|---:|---:|---:|---:|
| memory | loose | 320.11 | 297.79 | 7.4% | -20.1–59.7% |
| local | loose | 385.92 | 309.87 | 17.1% | 5.6–45.8% |
| memory | packed | 179.43 | 119.45 | 17.0% | 11.0–38.6% |
| local | packed | 534.27 | 467.73 | 10.1% | 3.1–32.3% |

Loose memory results are inconclusive despite a positive median. Mixed-size
imports (four large files among 64 files) also have both faster and slower pairs
in every configuration; their medians range from a 3.6% regression to a 9.5%
reduction. No consistent gain is claimed for them.

## Costs and controls

- Loose local 64 KiB cold imports initially regress by a 13.4% paired median.
  The targeted repeat regresses by 5.3%; combined across ten pairs the median is
  a 7.7% regression, with reductions ranging from -94.9% to +30.6%. This cost is
  retained in the evidence; the parallel path is not a universal small-file win.
- Packed local 64 KiB cold imports initially regress by 1.4%, then improve by
  5.9% in the repeat (all five repeat reductions positive). Results are sensitive
  to the run and do not establish a stable small-file benefit.
- Initial one-worker cold-control medians range from -4.8% to +6.3% reduction,
  with mixed signs within every configuration. Default decoding remains serial;
  these controls do not prove identical performance after the staging refactor.
- Serial packed/local wide-delta imports initially regress in every pair, with
  a 26.6% median regression (separate medians 27.01 ms to 31.70 ms). The targeted
  repeat has mixed signs and a 1.9% median reduction (23.35 ms to 25.14 ms).
  Combined across ten pairs the paired median still regresses by 15.9%; the
  repeat does not consistently reproduce the original effect. The repeat's
  subtree-delta operation instead regresses in every pair by a 19.1% median
  (22.47 ms to 25.68 ms). These incremental-control costs are not hidden or
  attributed confidently to streaming, which is inactive with one worker.
- Four-worker packed/local wide-delta imports regress in every original pair
  by a 5.3% median (26.35 ms to 27.52 ms). A packed-memory boundary wide-delta
  case regresses by a 29.6% median (1.15 ms to 1.32 ms); a serial loose-memory
  warm case regresses by 14.4% (0.160 ms to 0.173 ms). All operations, including
  these small absolute effects, are retained in the CSV and raw reports.

The decision weighs repeatable large-file gains against measured small-file
costs and noisy incremental controls. It does not establish statistical
significance or a universal regression bound. The reported source-byte peak is
the admitted window size, not total process memory; process RSS also includes
Git fixture creation, pack caches and audits.

## Reproduction and source identity

`commands.sh`, `repeat-small.sh` and `repeat-serial.sh` retain the exact commands;
substitute local checkout and frozen executable paths. `build-environment.sh`
records the original compiler and sysroot. See
[the corpus guide](../../git-worker-streaming.md) for build and runner commands.
The mixed-size suite is registered in the manifest, revision builds and
`benchmark all`. It crosses the 64 KiB and 4 MiB oversized-object thresholds;
the uniform worker suite also covers both sides of the two-object 128 KiB
window boundary.

The baseline is Casita commit `1699a944bc90608200e5655b8f61dc6d247a88ae`.
`candidate.patch.gz` applied to that revision reconstructs the measured Rust
candidate, including its new unit tests. Restore `Cargo.lock.gz` for both builds
(the checkout ignores Cargo.lock), and use no default features with
`native,git,experimental`. Build sidecars record executable, lock and source
fingerprints. The fixture file hashes differ because the existing backpressure
test was strengthened; the timed benchmark function is unchanged. Cargo JSON
confirms the candidate library and all four test executables were freshly built.

All seven raw JSON reports, paired summaries, build sidecars, exact dependency
lock, candidate patch and correctness logs are permanent. The JSON preserves
each pair, environment metadata and process output. Initial compile-error and
interrupted builds are excluded; only the genuine behavioral RED and final
GREEN are correctness evidence.

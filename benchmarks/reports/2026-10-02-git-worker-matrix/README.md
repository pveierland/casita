# Git decode-worker speed and memory

## Results

**Shared-host evidence:** unrelated builds were sampled during 504/823 intervals
of the large sweep, 1075/1184 of the delta sweep, and 237/502 of the same-binary
large sweep. Peak sampled external CPU use approached 94% of the whole host.
The final same-binary tiny and local-four repeats sampled no competing builds,
with maximum external CPU fractions about 11%. These observations limit timing
precision and do not establish performance on an isolated host.

Parallel decoding helps the larger synthetic imports, but adds overhead on tiny
files. The local/loose four-worker comparison is inconsistent across runs.
These results support retaining one decoder by default and exposing higher
counts as a workload-dependent option. They do not establish universal speedups
or end-to-end application improvements. B1 does not depend on these workers.

### Same executable: one versus two/four/eight decoders

Sixteen 4 MiB incompressible blobs, 64 MiB admission budget, staging concurrency
16, five alternating pairs per cell. Both variants are B2. Positive values are
median **paired duration reductions**; negative values are slowdowns. These are
not ratios of the two independent duration medians.

| Destination / source | 2 decoders | 4 decoders | 8 decoders |
|---|---:|---:|---:|
| Memory / loose | +39.3% | +42.8% | +39.9% |
| Memory / packed | +15.5% | +34.2% | +33.1% |
| Local / loose | +45.7% | -34.1% | +45.6% |
| Local / packed | +21.9% | +27.3% | +28.1% |

The local/loose four-worker follow-up used ten pairs: +10.8%
paired reduction, ranging from -36.6% to +47.2%.
Its independent medians were 0.4691 → 0.4820 seconds
(2.7% longer). Median process CPU was
0.485 → 1.035 seconds. The original negative result
and the repeat are both retained. Treat this configuration as inconclusive;
neither the favorable paired median nor the unfavorable independent medians
alone establish its effect. The CPU observation includes storage work, not just
source decoding.

### Parent process peak memory

Median pre-audit cold VmHWM in MiB for that same-executable large-object sweep.
The one-decoder column pools its fifteen baseline runs; each parallel column
contains five runs. The counter is approximate and includes destination data,
Git caches and allocator retention.

| Destination / source | 1 decoder | 2 decoders | 4 decoders | 8 decoders |
|---|---:|---:|---:|---:|
| Memory / loose | 200.7 | 200.8 | 201.4 | 201.9 |
| Memory / packed | 264.6 | 264.6 | 264.9 | 265.1 |
| Local / loose | 297.6 | 289.9 | 285.3 | 296.4 |
| Local / packed | 358.0 | 351.5 | 360.5 | 357.8 |

In the local/loose four-worker repeat, memory was 300.2 →
304.7 MiB, reversing the apparent saving in the first sweep.
Do not interpret lower local medians as a demonstrated memory reduction.
All large cold B2 cases admitted at most 64 MiB of source body lengths; that
budget does not cover spare vector capacity, caches, delta workspace or storage.

### Direct comparison with B1

The same large workload compared with the pre-worker importer. The one-worker
column is a control, not a claimed optimization; the preserved serial execution
strategy does not imply identical runtime cost. Full pair ranges and independent
medians are in `summary.csv`.

| Destination / source | B2: 1 decoder | B2: 2 | B2: 4 | B2: 8 |
|---|---:|---:|---:|---:|
| Memory / loose | -2.6% | +42.8% | +43.6% | +44.9% |
| Memory / packed | +20.8% | +37.5% | +29.9% | +42.5% |
| Local / loose | +3.5% | +31.6% | +39.4% | +43.2% |
| Local / packed | -5.0% | +18.5% | +32.2% | +36.3% |

### Tiny files and packed deltas

The ten-pair same-B2-binary tiny-file repeat (32 × 1 KiB, memory backend)
recorded these paired duration reductions:

| Source | 2 decoders | 4 decoders | 8 decoders |
|---|---:|---:|---:|
| Loose | -0.8% | -6.3% | -11.5% |
| Packed | -15.1% | -10.5% | -32.1% |

Every pair was slower for four decoders on tiny loose objects and for eight
decoders on tiny packed objects. The separate ten-pair B1→B2 one-decoder controls
were −0.8% loose and −3.5% packed, with ranges crossing zero. The initial tiny
loose serial-control slowdown of 52.0% was not stable.

For actual packed deltas (16 × 4 MiB, eight content families), the B1→B2
cold-import reductions were:

| Destination | 2 decoders | 4 decoders | 8 decoders |
|---|---:|---:|---:|
| Memory | +39.0% | +41.9% | +40.9% |
| Local | +34.7% | +19.0% | -5.7% |

Delta memory medians for B2 at 1/2/4/8 decoders were 280.6/280.7/277.0/289.7 MiB
(memory backend) and 365.0/362.9/340.5/346.5 MiB (local). Local figures remain
noisy; these do not establish a general memory saving. The eight-worker local
delta time range spanned −24.3% to +33.5%, so its negative median is not a
consistent slowdown across every pair.

### Coverage and validation

- 5,136 audited import samples across 1,284 processes; 396 paired phase/case cells.
- Cold, warm, subtree-delta and wide-delta results retained without outlier removal.
- 15/16/17 objects and two-body admission budgets 131071/131072/131073 covered.
- B1 integration: seven passing tests; B2 integration: thirteen passing tests.
- Benchmark harness: 436 tests run, two skipped; no failures; strict probe Clippy and formatting pass.
- All three registered worker corpus entries pass `benchmark all --profile smoke`.

`summary.csv` includes time ranges, separate medians, CPU time, observed worker
peaks and memory observations. `host-summary.csv` records sampled competition.
The host was shared and unrelated work was present; these ranges are not
confidence intervals. No warm-import worker speedup is claimed: warm imports
perform zero source work.

## Method and reproduction
This report compares the serial closure importer (`0220d4a`, B1) with the
bounded-worker change (`d04f4ee`, B2) at requested decoder counts 1/2/4/8. Neither measured library contains completed-decode staging overlap (B3,
`1710a64`) nor buffer-capacity normalization (B4, `ea0ebdc`). The benchmark lives on the later decoder branch
for permanent corpus maintenance; its checkout revision is not the measured
implementation revision. Use each executable's build manifest as source identity.

Both builds use identical release features (`native,git,experimental`, no default
features), Rust 1.96.0, Git 2.54.0, flags, dependency lock, fixture and observation
helpers.
Only the small API adapter differs: B1 asserts serial operation and reports null
source/worker counters; B2 calls its real worker setter and reports real peaks.
No importer logic is patched. Exact source patches, lockfile, build manifests,
compiler output and executable hashes are retained. Binaries remain in the local
frozen artifact directory and are reproducible from these inputs.

Each primary timing cell uses five alternating baseline/candidate pairs. The
tiny controls and local-four follow-up use ten pairs. Cold means a fresh Casita destination;
source filesystem caches are warmed by fixture generation. Cold, warm,
subtree-delta and wide-delta samples are all retained with independent readback
and closure audits. The main comparison covers tiny, medium, large, mixed and
actual packed-delta fixtures. Tiny/medium use Git pack window 16. The large,
mixed and same-binary large ordinary cases use window 0, ensuring non-delta
packing; the dedicated clustered-delta case keeps window 16. An initial large
run was stopped because Git delta search on random blobs consumed tens of CPU
seconds per fixture outside import timing. Its incomplete raw report, host
observations and original commands are retained as `initial-*` evidence and
excluded from the final complete-cell summary. Complete tiny/medium results
were retained; the large case was rerun in full. No slow pairs were discarded. A same-B2-binary comparison isolates scaling from
one to two/four/eight workers. A one-repetition boundary sweep is correctness
coverage, not speed evidence.

All measured processes are assigned CPUs 0–3: four logical CPU slots with
distinct reported core IDs and the same maximum frequency (about 5.16 GHz).
Eight requested decoders therefore oversubscribe those four CPU slots. SMT siblings
and other CPUs remain available to unrelated host work. Task builds finish before
measurement. Host activity is sampled throughout; unrelated process names are
hashed while process identities, CPU ticks and competing-build counts are kept.
These are shared-host observations, not controlled laboratory results.

The [measurement guide](../../git-worker-matrix.md) explains memory and CPU
accounting. Cold parent-process VmHWM before audits avoids fixture subprocess and
readback peaks. VmHWM is approximate; raw decreases remain visible and are counted
in summaries, rather than clamped or discarded. It includes destination storage, caches and allocator retention.
Warm/incremental high-water marks inherit previous peaks. CPU counters have clock
tick resolution; tiny operations may record zero. Pair ranges are descriptive,
not confidence intervals. Positive time reduction means faster.

The timer brackets the import call. Fixture generation, payload audits, the
final explicit repository flush and lease teardown are outside it; complete
process output and process resource usage remain in the raw reports.

## Reproduce

From the repository root, use a fresh directory for each replay:

```sh
report=benchmarks/reports/2026-10-02-git-worker-matrix
. "$report/build-environment.sh"
python3 "$report/build-probes.py" --directory /tmp/worker-rebuild \
  --target-dir /tmp/worker-build-target
python3 "$report/matrix.py" --baseline /tmp/worker-rebuild/bin/baseline \
  --candidate /tmp/worker-rebuild/bin/candidate --output-dir /tmp/worker-results
PYTHONPATH=. python3 "$report/summarize.py" /tmp/worker-results
python3 "$report/matrix.py" --baseline /tmp/worker-rebuild/bin/baseline \
  --candidate /tmp/worker-rebuild/bin/candidate --repeat-controls \
  --output-dir /tmp/worker-repeat-results
PYTHONPATH=. python3 "$report/summarize.py" /tmp/worker-repeat-results
```

The recorded Nix compiler paths describe this host. To use another toolchain,
provide equivalent `cargo`, `rustc`, linker and sysroot settings, and treat it as
a new measurement. New build manifests record those differences. The replay
uses the archived dependency lock and fixture patches; it does not substitute
whatever importer is checked out in the working tree.

# Historical delta-reconstruction evidence

These original 2026-10-01 measurements and source artifacts describe the recorded
experiment, including rejected intermediate candidates and the final opt-in
runtime. This extraction retains the final runtime with earlier capacity fixes,
updated dependencies and reordered commits. Archived timings do not measure this
extracted branch. Numeric observations remain unchanged. Limited unrelated
process-name redactions and original hashes are recorded in
extraction-provenance.json; the current checksum manifest covers this extraction.

# File-backed Git delta reconstruction

Retain explicit, default-disabled file-backed delta reconstruction as a process
memory tradeoff. The final candidate reduces cold-import HWM by 54–56% on the
16 × 4 MiB workload, with 64 MiB extra logical writes and slower imports. Reject
automatic enablement: small and ordinary workloads do not justify the cost.
This is not a throughput optimization or a universal source-memory ceiling.

The original default-disabled slowdown triggered a deterministic progress fix,
a task-result representation experiment, and CPU-aware controls. The final cold
controls do not reproduce that slowdown. Short warm/incremental results still
include small regressions; retention rests on the large memory benefit, not a
claim that all default-path overhead has been disproved.

## Evidence

The evidence contains 2,000 audited import samples: 1,120 in the primary matrix
and supplemental controls, 400 in the admission experiment, and 480 in the final
CPU-aware experiment. The table below describes the initial candidate. Comparisons use five alternating pairs, with ten pairs in the disabled
repeat, and matched source-worker
counts, local destination storage and CPU affinity 0–3. Raw results preserve
cold, warm, changed-subtree and wide-tree phases, exact paired closure roots,
actual packed delta counts, complete payload audits, memory and import I/O.

| Cold packed workload | Workers | Baseline HWM MiB | Candidate HWM MiB | Baseline seconds | Candidate seconds | Extra logical writes MiB |
|---|---:|---:|---:|---:|---:|---:|
| 16 × 64 KiB clustered | 1 | 30.2 | 29.3 | 0.033 | 0.040 | 1.0 |
| 16 × 64 KiB clustered | 4 | 30.7 | 29.8 | 0.027 | 0.030 | 1.0 |
| 16 × 1 MiB clustered | 1 | 94.7 | 77.7 | 0.168 | 0.208 | 16.0 |
| 16 × 1 MiB clustered | 4 | 98.7 | 79.7 | 0.129 | 0.173 | 16.0 |
| 16 × 16 MiB clustered | 1 | 394.1 | 131.1 | 1.868 | 2.365 | 256.6 |
| 16 × 16 MiB clustered | 4 | 437.8 | 134.9 | 1.420 | 1.819 | 256.5 |
| 64 × 1 MiB clustered | 1 | 208.5 | 142.8 | 0.557 | 0.706 | 90.0 |
| 64 × 1 MiB clustered | 4 | 194.4 | 128.3 | 0.370 | 0.556 | 90.0 |
| 16 × 16 MiB compressible | 1 | 244.9 | 34.0 | 1.894 | 2.636 | 480.1 |
| 16 × 16 MiB compressible | 4 | 284.4 | 34.3 | 1.084 | 1.874 | 480.1 |

Values are separate medians; paired percentage reductions and their full ranges
are in summary.csv. Large clustered objects reduce process HWM by about 67–69%,
and large compressible objects by about 86–88%. These are repeatable memory
reductions, with additional logical writes and slower imports. The 1 MiB
clustered candidate is slower in all five pairs at both worker settings. Small
64 KiB deltas save less than 1 MiB; 1 KiB deltas show no memory benefit.

Below/at/above the one-payload source budget, cold HWM falls from roughly
59–65 MiB to 43–47 MiB, retaining correct results on both sides. The no-delta
controls show broadly unchanged memory and mixed timing, including regressions:
serial loose −44.0%, serial packed −6.5%, four-worker packed −32.7%, and
four-worker loose +3.2% median paired time reduction. The feature should remain
disabled for those ordinary workloads.

The primary quiet-host gate timed out. No builds from this investigation ran
during timing; unrelated builds and other host activity were sampled. Dense
deltas had no observed competing builds but did have other activity. Timing
claims are exploratory, not statistically isolated effects. The first disabled
control is especially noisy: serial candidate times range 0.182–0.538 seconds,
versus 0.165–0.304 for the baseline, with a −70.6% median paired reduction but
two faster candidate pairs. That triggered the separately retained ten-pair
repeat; it must not be hidden or described as evidence of zero default overhead.
The repeat still regresses: −18.4% median paired reduction with one worker
(8/10 slower), and −5.5% with four workers (7/10 slower). A deterministic regression demonstrated that holding an ordinary reader's source
slot until result receipt could block another reader whose predecessor had
already finished. The admission revision fixes this: ordinary success releases
at blocking completion, while spill results retain cleanup ownership through
receipt or disposal. This fixes the progress dependency but has not established
an explanation of the end-to-end slowdown.

The admission revision adds 400 audited samples in `admission-check/`. Its cold
16 × 4 MiB comparison still reduces HWM: 255.2→120.6 MiB (one worker) and
244.8→110.7 MiB (four), with 64 MiB additional logical writes and paired time
reductions of −10.9% and −25.4%. Against the predecessor with spilling disabled,
ten-pair timing reductions remain −6.3% and −9.7%, while HWM and logical I/O
are effectively unchanged. An isolated comparison against the initial candidate
is mixed (+5.8%/−5.0% for one/four workers). Warm controls also show regressions
but do not use the changed reader jobs. Host activity remains a confounder;
these results do not establish zero default cost.

The retained implementation puts the cleanup permit last in the boxed reader
state, restoring a pointer-sized blocking-task result. The isolated 4 MiB
comparison against the admission revision improved all five one-worker pairs:
median paired wall reduction 43.5%, CPU reduction 40.4%. Four-worker results were
mixed (three of five faster; paired wall median +25.3%, CPU +18.8%). This is a
measured layout variant, not an established general explanation for scheduling
costs. CPU-aware fixtures and source identities are in `cpu-check/`.

Final disabled controls against the predecessor use ten pairs per cell:

| File size | Workers | Paired wall reduction | Median paired CPU tick change | Baseline → candidate HWM MiB |
|---|---:|---:|---:|---:|
| 1 MiB | 1 | +1.2% | −1 | 96.0 → 93.1 |
| 1 MiB | 4 | +2.0% | −0.5 | 98.6 → 99.1 |
| 4 MiB | 1 | +1.8% | −1 | 257.8 → 260.7 |
| 4 MiB | 4 | +9.1% | −13.5 | 244.3 → 243.5 |

One tick is 10 ms of process CPU here. Positive wall reductions mean faster.
Separate median times must not be divided to substitute for paired effects.
The 1 MiB warm controls still regress by 7.9%/6.9% (one/four workers), with
median paired additions of 0.67/1.10 ms; their paired differences range across
both signs (−16.2…+12.4 ms and −13.5…+10.1 ms). Median CPU tick changes are zero,
which is below useful resolution and does not establish zero CPU cost. The
4 MiB warm controls instead improve by 3.0%/2.2%. Incremental phases remain
mixed; raw samples and summaries retain every result. Thus the earlier blanket
5% regression screen cannot establish a zero-overhead guarantee for these short,
contended phases. The concrete progress regression is fixed, cold controls no
longer show its earlier pattern, and the substantial memory reduction justifies
retaining the explicit option with these timing limitations documented.

The final enabled 4 MiB comparison has five pairs per worker count:

| Workers | Baseline → candidate HWM MiB | Wall seconds, separate medians | Paired wall reduction | CPU seconds, separate medians | Paired CPU reduction |
|---|---:|---:|---:|---:|---:|
| 1 | 259.5 → 118.5 | 0.667 → 0.743 | −11.5% | 0.80 → 0.91 | −7.5% |
| 4 | 248.6 → 109.6 | 0.448 → 0.643 | −31.4% | 0.95 → 1.16 | −26.3% |

The final comparison adds approximately 64 MiB logical writes. All five memory
pairs improve at each worker setting; only one serial timing pair improves,
and no four-worker timing pair does. No investigation builds overlapped any
measurement. Unrelated builds were observed in 51/166, 522/617, and 331/331 host
intervals for the isolated, disabled, and enabled comparisons respectively.
CPU measurements narrow the observations but do not remove host contention or
frequency/cache variation as confounders.

A separate follow-up will measure normalizing gix's oversized returned vectors
before staging. That can release retained delta workspace, but cannot prevent
its decode-time allocation. These results compare against the identified
predecessor, not that as-yet unmeasured alternative.

## Implementation and limits

The opt-in `GitClosureImport::with_delta_spilling(true)` path reconstructs
located blob deltas into anonymous files. It inflates instructions incrementally
and copies bounded blocks from a file-backed base; it does not allocate complete
base, instruction or result vectors. Selected chains are limited to 64 deltas
and 64 GiB of aggregate declared/actual work, including probing, compressed
input, inflation and reconstructed output. Repository payload limits also apply
to every base, instruction stream and result. Missing optional locator hints
still use gix, so the feature does not provide a universal source-memory bound.
Invalid or over-limit selected chains fail closed.

Payload reservations share the import's spill quota with traversal state and
are acquired before files are created. The reservation lives until the file
closes, including queued, running and completed-but-unreceived blocking jobs.
Plans retain at most 128 stable source handles, plus one transient reader handle
per active source job; a full planning window drains and retries its pending
object. Delta chains reuse handles within each plan and use independent seek
positions across plans. Native SHA-1/SHA-256 identities, complete zlib endings,
exact lengths and independent backend digests remain checked. The final root
is hashed during reconstruction and again during ordinary verified staging;
removing that duplicate CPU work is a separate possible optimization, not a
measured benefit of this feature.

## Measurement limits

Memory values are Linux parent-process high-water marks immediately after
import and before audits. Fixed-buffer fixture construction/readback avoid
whole-payload fixture allocations; Git child RSS is excluded. Initialization,
allocator retention, caches and destination work still contribute. Only cold
phase HWM isolates the import from preceding phases. These are not live source
allocation counts or bounds on machine/cgroup memory: filesystem cache pages
can retain spilled data. Temporary files and destination storage used ZFS here.

Import I/O snapshots use `/proc/self/io` immediately around the timed import,
before correctness readback. Logical writes include all import writes, not just
delta reconstruction. Physical writeback may lag; unlinked temporary files can
cancel pending writes. Report logical `wchar` differences separately from
`write_bytes` and `cancelled_write_bytes`. Missing platform observations are
never converted to zeros. The baseline's added `peak_spill_bytes` field is a
zero-only compatibility shim, not an observation of its traversal reservations;
the comparison summary deliberately leaves that baseline metric empty.

## Correctness and reproduction

The retained implementation passes 24 integration tests (one ignored benchmark),
all 48 Git unit tests (three ignored benchmarks), 41 benchmark-runner tests, and
strict Clippy for the measured integration configuration. The Git tests include
all 25 focused streaming/delta unit tests. Coverage
includes SHA-1/SHA-256, REF/OFS chains, alternate object stores, one/four workers,
one blocking thread, exact streamed payload and closure audits, malformed
instructions/zlib/native identities, depth/work/quota/handle boundaries,
large-base/tiny-result reservations, and cancellation ownership. Regression logs
retain the predecessor failures: ignored spill quota, known invalid metadata
falling back, excessive stable handles, and a cancellation drain returning
before its spill reservation was released.

Permanent `git-delta-spill`, `git-delta-disabled` and `git-delta-limits` suites
are registered in the manifest, revision runner and `benchmark all`. The admission
revision and final-candidate smoke executions pass all three. The limits suite times complete correctness test processes, with
fixture construction and assertions included; its timings do not demonstrate
import throughput. The [corpus guide](../../git-delta-spill.md) documents options.

All measured integration executables were freshly compiled (`fresh:false`) in separate
source and target directories with identical compiler/features/flags/lockfiles
and fixture hashes. Apply the exact `artifacts/baseline.patch.gz` or
`artifacts/candidate.patch.gz` to commit
`22fa2ab3e1d78c2828be49dc5d1a858584dd3676`, restore its matching Cargo.lock, build
with the manifest's options and freeze before measuring. The baseline adds only
report compatibility fields and a no-op extension method for the common
fixture's spilling option. Its spill count must remain zero. The unit artifacts
use default features plus git/experimental; their separate manifests record this.

The final CPU experiment uses `artifacts/cpu-baseline.patch.gz`,
`artifacts/cpu-admission.patch.gz`, and `artifacts/cpu-state.patch.gz`, with their
matching manifests and lockfiles. Their shared fixture hash is
`a934bf2f7443537e73a4e9d1b2e653b230622513612dd5bcc2c71460cd4471d4`.
The predecessor's unfiltered test invocation predictably fails the three new
feature-specific tests; its 21 applicable integration tests pass separately.
Both logs are retained. The final candidate passes all 24. `retained-source`
is identical to the measured `cpu-state` Rust patch; its manifest records this.

`matrix-commands.json` records the primary measurements; supplemental controls
have their own command/start-host records. `matrix.py` and `profile.py` reproduce
the host-observed comparisons with replacement binary paths. `summarize.py`
audits all complete samples and regenerates the comparison/host CSVs. Exact
measured patches remain unchanged even if later documentation, harness or lint
fixes are made. `cpu-check/matrix.py --baseline BASELINE --admission ADMISSION
--candidate CANDIDATE --output-dir NEW_DIRECTORY` replays the final comparisons;
copy its `summarize.py` into that new output directory to regenerate CSVs.
CPU counters are import-interval Linux process ticks across threads, excluding
children; snapshots slightly widen the interval. The report preserves the tick
rate and raw zeros, and never divides by a zero CPU baseline. SHA256SUMS covers retained evidence except itself.

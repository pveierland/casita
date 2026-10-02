# Historical source-inflation evidence

These original measurements from 2026-10-01 tested loose and packed source
inflation together, with the recorded source, dependencies and fixture. This
extraction introduces loose and packed inflation in separate commits. Archived
commands and patches describe that original combined experiment; its timings
are not measurements of the updated extracted branch. Numeric observations remain unchanged. extraction-provenance.json
records limited unrelated process-name redactions and original file hashes.

# Incremental Git source inflation: retained for memory reduction

Keep bounded incremental inflation for loose and non-delta packed blobs of at
least 1 MiB. The measured large-object imports substantially reduce cold parent
peak memory. Throughput is mixed; no general latency improvement or statistical
significance is claimed. Delta reconstruction and shared source/destination
resource admission remain separate work.

Each admitted reader owns fixed 64 KiB compressed and decoded buffers plus
inflater state. Bounded blocking steps share source-worker admission with ordinary
decode jobs and release their permits before destination storage awaits. Exact
length, complete zlib termination, native Git identity, and destination digests
remain checked. Normal errors join submitted jobs; dropping an import leaves
only bounded submitted work owning its buffers and permits until completion.
Declared-object byte admission remains conservative and does not become a
resident-memory guarantee.

## Results and decision

There are **1,360 audited paired import samples** and **45 locator-boundary
samples**. Import comparisons use five alternating baseline/candidate pairs,
matched one/four worker settings, local destination storage, and CPU affinity
0–3. Cold, warm, changed-subtree, and wide-tree phases remain in the raw results.

| Cold workload | Layout | Workers | Baseline peak MiB | Candidate peak MiB |
|---|---|---:|---:|---:|
| 1 × 16 MiB random | loose | 1 | 90.2 | 51.6 |
| 1 × 16 MiB random | packed | 1 | 107.2 | 53.1 |
| 1 × 64 MiB random | loose | 1 | 217.9 | 105.0 |
| 1 × 64 MiB random | packed | 1 | 300.3 | 109.3 |
| 4 × 16 MiB random | loose | 1 | 265.6 | 114.6 |
| 4 × 16 MiB random | loose | 4 | 279.9 | 115.9 |
| 4 × 16 MiB random | packed | 1 | 329.1 | 111.7 |
| 4 × 16 MiB random | packed | 4 | 351.5 | 114.6 |
| 1 × 64 MiB repeated | loose | 1 | 108.6 | 28.4 |
| 1 × 64 MiB repeated | packed | 1 | 107.1 | 26.0 |

Values are medians of Linux parent-process high-water marks immediately after
import and before audits. Fixture generation and readback use fixed 64 KiB
buffers; child Git processes are excluded. Initialization, allocator retention,
destination work, caches and earlier phases still contribute. These are not
live source-allocation measurements. Only the cold phase supports the isolated
comparison used above; later high-water marks inherit earlier work.

The 16 × 64 KiB controls stay around 30–31 MiB. Four blobs immediately below
1 MiB remain on the buffered path; at/above that threshold their medians fall
from roughly 46–50 MiB to 37–41 MiB. Budgets immediately below/at/above one blob
also retain the saving and correctness. The clustered packed corpus asserts
actual deltas, but also contains ordinary base objects: its lower memory does
not demonstrate streaming delta reconstruction.

Latency is inconsistent. For single 64 MiB random blobs, median paired time
reduction is +21.1% loose and −44.5% packed; the packed pairs range from −565.2%
to +16.2%. Four 16 MiB packed blobs with four workers show +10.5%, ranging from
−16.4% to +38.5%. At the 1 MiB threshold the four-worker packed case shows
−20.2%. These regressions and spreads remain in the evidence. Ratios of separate
wall-time medians differ from medians of paired ratios; summary.csv includes
the separate wall-time medians and paired reductions.
Streaming verification runs through asynchronous staging, while the buffered
parallel path can verify on source workers, so scheduling costs can also differ.

The quiet-host gate timed out. Competing builds were present throughout the
initial, threshold and large-random series and portions of later series. No
builds from this investigation ran during timing. The small-file and delta
series had no observed competing builds; their timing changes are modest.
Host samples and summaries qualify every timing claim. The large, repeated
memory reduction is the reason to retain this change, with CPU scheduling still
an open optimization.

## Scope and bounds

Optional pack hints are lazy: loose hits do not load them. Discovery examines
at most 256 directory entries, retains at most 32 valid indexes, and attempts
at most 16 MiB of owned snapshots, including invalid attempts. A large eligible
index can outweigh savings on small blobs; these comparisons use small indexes.
The existing gix index/cache footprint is separate. Unrepresented, stale,
oversized or missing hints and all deltas fall back to gix. This is a best-effort
optimization, not universally bounded Git-source memory.

The locator corpus covers below/at/above all three caps and verifies selected
stream or fallback payloads. Padding is in an earlier root; the usable index is
the sole entry in a later root. Its pack is linked after discovery, outside
timing, to avoid directory-order ambiguity. Time sums discovery and subsequent
selection. Locator whole-process RSS includes its fixture and is not used for
source-memory claims.

## Correctness and reproduction

The final integration suite passes 21 tests (one benchmark ignored), including SHA-1
and SHA-256, loose/packed/alternate sources, actual delta fallback, threshold and
budget boundaries, malformed length/native identity/zlib trailers, full payload
readback, and mixed workloads with one blocking thread. Nine focused unit tests
pass (one benchmark ignored), including cancellation ownership, ordinary-error
drain barriers, fixed buffers, stale hints and locator caps. Late-trailer tests
prove that a valid prefix stages before a later failure without publishing a
record. The baseline failed that streaming-prefix assertion before implementation.

The permanent suites are registered in the manifest, revision runner and
benchmark all. The latter passes both smoke suites using frozen artifacts;
35 benchmark-runner and revision-registration tests pass. Strict Clippy with warnings denied, Rust formatting and whitespace checks pass;
see retained logs. Earlier failed setup/lint attempts are retained separately;
clippy-verified.log and final-integration.log record the successful final checks.

Baseline and candidate integration artifacts use separate checkouts and target
directories, identical compiler/features/flags/lockfiles and all three fixture
files. Each was freshly compiled (Cargo fresh:false) before freezing. Exact
patches, lockfiles and manifests are under artifacts/. Apply a patch to a fresh
checkout at 98ea56bb61059d7127e9811270c23bf9bfe841e9, restore its lockfile, and build
with its recorded compiler/options. The integration candidate predates only the
private locator microbenchmark correction, final documentation edits and three
Clippy expression simplifications plus two redundant clones in correctness tests. These do not change runtime semantics. The separate unit artifact includes the corrected
locator fixture. The final source patch is archived separately and linted; exact measured patches
remain archived without modification.

matrix-commands.json records the executed matrix commands. matrix.py and
profile.py reproduce the host-observed matrix with replacement binary paths;
commands.sh includes initial, locator and verification commands. The guide at
../../git-source-inflation.md explains bounded fixture and suite options.
summarize.py regenerates summary.csv and host-summary.csv from complete audited
results. SHA256SUMS covers retained evidence files except itself.

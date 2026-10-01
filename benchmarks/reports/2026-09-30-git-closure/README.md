# Historical Git closure import evidence

These original results describe the recorded executable and environment from
2026-09-30. They predate this extraction and updated upstream dependencies.
The raw JSON remains unchanged; its timings are not current-branch measurements.

# Git closure import optimization evidence

The initial release baseline passed seven integration tests. Its workload-matrix
validation contains 64 audited samples: eight files, memory/local backends,
loose/packed sources, 1 KiB/256 KiB deterministic random files, and staging
concurrency 1/16. These single samples validate the harness; they do not establish
speedup or statistical significance.

```sh
python3 -m benchmarks.suites.git_closure_import --counts 8 --max-buffered-bytes 67108864 --backend both --layout both --file-bytes 1024,262144 --concurrency 1,16 --content random --probe-binary /path/to/closure-baseline --no-build --output baseline-validation.json
```

Build the executable with `cargo test --release -p casita --features
git,experimental --test git_closure_import --no-run`. The JSON records its
SHA-256, platform, all process output and exact fixture dimensions. Its temporary
binary path is descriptive; a fresh build may have a different hash.

Further experiments must retain paired before/after runs with at least five
repetitions and independent correctness gates before a production optimization
is accepted.

## Bounded object workers

The first prototype decodes and intrinsically verifies independent native objects
on CPU workers. A bounded window admits source bodies before allocation, and all
source jobs finish before destination writes begin. Built-in format verification
produces private, repository-bound seals; storage must independently reproduce
its physical digest. Custom verifiers stay on the caller's async runtime.
Source handles share the Git object database; their configured aggregate pack
cache target remains 16 MiB. The body budget excludes cache allocator overhead,
delta workspace, verifier metadata, and destination buffers.

`workers-large-paired.json` contains 480 audited samples (five pairs per case),
comparing 2/4/8 workers against one worker in the same executable. Each cold
fixture holds sixteen 4 MiB incompressible blobs. `workers-small-paired.json`
contains 160 samples comparing the initial one/four-worker prototype against the
pre-worker importer with identical dependency locks. `workers-delta-paired.json`
contains 80 samples of sixteen 4 MiB near-identical packed blobs; Git may choose
delta compression. `workers-mixed-paired.json` contains 160 samples, two 512 KiB
blobs interspersed with thirty 1 KiB blobs. `workers-boundaries.json` is a
144-sample correctness sweep around 15/16/17 objects and source byte windows
131071/131072/131073; its single repetitions are not performance evidence.

Initial prototype: cold import reduction with four versus one worker:

| Workload | Backend | Layout | Median paired reduction | Pair range |
|---|---|---|---:|---:|
| 4 MiB random | memory | loose | 46.7% | 42.2–52.8% |
| 4 MiB random | local | loose | 39.6% | 32.3–51.3% |
| 4 MiB random | memory | packed | 21.4% | 12.8–26.3% |
| 4 MiB random | local | packed | 22.9% | -29.4–49.2% |
| 4 MiB near-identical | memory | packed | 39.3% | 12.9–65.9% |
| 4 MiB near-identical | local | packed | 10.3% | -9.7–81.5% |
| Mixed sizes | memory | loose | 23.4% | -47.1–36.2% |
| Mixed sizes | local | loose | 15.7% | -1.0–26.4% |
| Mixed sizes | memory | packed | 22.6% | 12.1–26.3% |
| Mixed sizes | local | packed | 20.5% | -6.7–55.5% |

The one-worker prototype is not the old importer: it plans headers in advance
and moves built-in verification into the blocking source job. The independent
control `workers-serial-large.json` (160 samples, eight 1 MiB random blobs)
found regressions: packed memory cold imports slowed a median 17.2%, with every
pair negative (-91.8% to -1.8% reduction). These results reject changing the
serial default to that implementation, and the same-executable speedups above
must not be presented as improvements over the original importer.

The refined candidate restores serial header/decode interleaving and staging
verification when one worker is requested. Parallel workers remain opt-in.
An additional regression exposed a zero-byte sibling admitted behind an
oversized body; the planner now treats that body as exclusive even when the next
object is empty. `workers-first-candidate.patch` preserves the measured initial
implementation against ef4e021, including its probe. Final refined-candidate
validation and comparison against the original importer are required before
adoption.

All runs used CPU affinity 0–3 on a heterogeneous host. Unrelated work continued;
part of the large comparison and later controls overlapped task compilation
restricted to other cores. Ranges are retained and are not statistical confidence
intervals. Peak worker counts confirm concurrent execution. Warm operations
perform no source work, and their small/noisy differences establish no worker
speedup. No default worker-count increase is justified by these measurements.

```sh
python3 -m benchmarks.suites.git_closure_import --counts 16 --file-bytes 4194304 --max-buffered-bytes 67108864 --content random --decode-workers 2,4,8 --baseline-decode-workers 1 --probe-binary WORKERS --baseline-binary WORKERS --no-build --repetitions 5 --cpu-affinity 0,1,2,3 --output /tmp/workers-large-paired.json
python3 -m benchmarks.suites.git_closure_import --counts 32 --file-bytes 1024 --max-buffered-bytes 67108864 --content random --layout loose --decode-workers 1,4 --probe-binary WORKERS --baseline-binary ORIGINAL --no-build --repetitions 5 --cpu-affinity 0,1,2,3 --output /tmp/workers-small-paired.json
python3 -m benchmarks.suites.git_closure_import --counts 16 --file-bytes 4194304 --max-buffered-bytes 67108864 --content repeated --layout packed --decode-workers 4 --baseline-decode-workers 1 --probe-binary WORKERS --baseline-binary WORKERS --no-build --repetitions 5 --cpu-affinity 0,1,2,3 --output /tmp/workers-delta-paired.json
python3 -m benchmarks.suites.git_closure_import --counts 32 --file-bytes 524288 --max-buffered-bytes 67108864 --content mixed --layout both --decode-workers 4 --baseline-decode-workers 1 --probe-binary WORKERS --baseline-binary WORKERS --no-build --repetitions 5 --cpu-affinity 0,1,2,3 --output /tmp/workers-mixed-paired.json
python3 -m benchmarks.suites.git_object_workers --counts 15,16,17 --file-bytes 65536 --max-buffered-bytes 131071,131072,131073 --content random --decode-workers 4 --probe-binary WORKERS --no-build --repetitions 1 --cpu-affinity 0,1,2,3 --output /tmp/workers-boundaries.json
python3 -m benchmarks.suites.git_closure_import --counts 8 --file-bytes 1048576 --max-buffered-bytes 67108864 --content random --decode-workers 1 --probe-binary WORKERS --baseline-binary ORIGINAL --no-build --repetitions 5 --cpu-affinity 0,1,2,3 --output /tmp/workers-serial-large.json
```

### Refined workers: retain as an opt-in

`workers-refined-large.json` (320 audited samples) and
`workers-refined-small.json` (160 samples) compare the refined importer directly
against the pre-worker importer, using identical dependency locks and five
alternating pairs per case. The large fixtures contain eight 1 MiB random blobs;
the small fixtures contain thirty-two 1 KiB random blobs.

| Workload | Backend | Layout | Workers | Median paired cold reduction | Pair range |
|---|---|---|---:|---:|---:|
| 1 MiB | memory | loose | 1 | 5.0% | -44.6–26.4% |
| 1 MiB | memory | loose | 4 | 48.6% | 41.7–57.3% |
| 1 MiB | local | loose | 1 | 13.9% | -2.1–42.9% |
| 1 MiB | local | loose | 4 | 22.0% | -25.2–56.5% |
| 1 MiB | memory | packed | 1 | 25.5% | -45.6–62.9% |
| 1 MiB | memory | packed | 4 | 38.5% | 27.7–46.8% |
| 1 MiB | local | packed | 1 | -0.8% | -46.8–45.0% |
| 1 MiB | local | packed | 4 | 34.8% | 17.3–55.4% |
| 1 KiB | memory | loose | 1 | -1.2% | -38.4–3.2% |
| 1 KiB | memory | loose | 4 | 23.0% | 5.6–45.3% |
| 1 KiB | local | loose | 1 | 20.8% | -6.4–36.2% |
| 1 KiB | local | loose | 4 | -3.2% | -70.5–13.2% |

Decision: retain opt-in parallel workers and leave the default at one. Four
workers show substantial gains in every pair for large memory imports (loose
and packed) and local packed imports. Small local imports are near neutral;
there is no claim that parallelism improves every workload. Restoring the serial
execution strategy removes the consistent packed-memory cold regression observed
in the first prototype. Its noisy positive control medians are not claimed as
serial speedups.

Warm and incremental measurements remain noisy on this shared host. Some paired
medians are negative by more than 5%, including local packed warm (-12.4%) and
wide-delta (-10.1%) serial controls. The local loose subtree-delta comparison has
independent medians of 620 ms and 136 ms yet a negative paired median, illustrating
the scheduling/I/O variability. Earlier same-executable worker-count comparisons
also vary substantially on warm operations that execute no workers. These results
do not establish broad non-regression or a causal warm/incremental slowdown.
The adoption decision rests on repeatable cold gains, unchanged default
parallelism, preserved serial work order, and correctness/resource gates; an
isolated host is needed to resolve small-path timing differences.

Twenty-eight targeted integration tests pass across closure imports, custom
verifiers, aliases, raw-blob proofs and verified streaming. They include both
Git hash formats, loose/packed alternates, custom-verifier runtime affinity,
progress with one blocking thread, excessive caller limits, corrupt native
identities, source-byte bounds, oversized/empty sibling exclusivity, collection
retention and zero source work on warm imports. The benchmark harness tests pass. A focused unit test rejects native verification
seals from another repository before writing any payload, and the import suite
rejects a backend returning a false payload digest before metadata publication.

```sh
python3 -m benchmarks.suites.git_closure_import --counts 8 --file-bytes 1048576 --max-buffered-bytes 67108864 --content random --decode-workers 1,4 --probe-binary REFINED --baseline-binary ORIGINAL --no-build --repetitions 5 --cpu-affinity 0,1,2,3 --output /tmp/workers-refined-large.json
python3 -m benchmarks.suites.git_closure_import --counts 32 --file-bytes 1024 --max-buffered-bytes 67108864 --content random --layout loose --decode-workers 1,4 --probe-binary REFINED --baseline-binary ORIGINAL --no-build --repetitions 5 --cpu-affinity 0,1,2,3 --output /tmp/workers-refined-small.json
```

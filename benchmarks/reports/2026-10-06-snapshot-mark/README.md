# Snapshot-pin collection marking — 2026-10-06

Snapshot marking now skips metadata lookups for dependencies already marked by
its generation scan. Filtering applies only to the initial scan dependencies;
later closure roots, resources and descendants keep the previous traversal.
After two consecutive filtered batches avoid no reads, filtering stops. In the
all-new distinct-target fixture, only the first two batches incur probes. A
shared new target gets a second batch after its first fetch marks it. Occasional
hits can keep filtering active; this is not a general cap for mostly-new graphs.

This is a proven marking-component improvement with measured tradeoffs. The
release-ingestion comparison does not establish a stable speedup or exclude a
small regression. The broader ingestion investigation remains open. Existing
Turso WAL PR #9485 was already in the baseline; this work neither adds nor
duplicates it.

## Component results

Milliseconds, median of four independent process means, each with three warm
iterations. First iterations are retained separately. Matching legacy/current
processes run adjacently in balanced AB/BA order on CPU 2. Both strategies are
compiled into the same executable. Warm iterations are subsamples, not separate
repetitions. OS caches are not explicitly dropped.

| Full snapshot, 8,192 parents | Legacy | Adaptive | Change |
|---|---:|---:|---:|
| Shared leaf, memory | 33.545 | 16.505 | −50.8% |
| Distinct leaves, memory | 60.369 | 36.768 | −39.1% |
| Single-root chain, memory | 46.424 | 19.279 | −58.5% |
| Shared leaf, spill | 368.902 | 281.242 | −23.8% |
| Distinct leaves, spill | 564.572 | 473.205 | −16.2% |
| Single-root chain, spill | 385.385 | 298.221 | −22.6% |

Full snapshots scan the same records in both strategies, but subsequent metadata
record lookups fall from 8,192 to zero. Half-covered snapshots improve 4.2–20.8%
across the same shape/storage cases. Sparse snapshots range from 2.8% faster to
0.4% slower. The closure-only pin control calls the same production function
under both labels; it is not a separate implementation.

The adverse `snapshot-forward` fixture publishes old parents before their
leaves, then pins only the old generation. With distinct leaves every
membership probe misses. At 8,192 parents, its memory result is 42.825 → 42.865 ms
(+0.1%, paired changes −0.3% to +0.2%). The spill result is 487.721 → 502.535 ms
(+3.0%; pairs +2.7%, +4.5%, +0.8%, −0.3%). This residual spill delta is retained;
it is not evidence of universal nonregression. The matching spilled pin
control is noisy, including one +26.7% pair, but that does not erase the
adverse-case observation.

Shared and chain forward dependencies retain substantial gains: respectively
47.0% and 56.0% in memory, and 9.5% and 24.6% in spill storage. A single missed
batch cannot trigger fallback because all references to a new shared leaf
miss in the first batch and hit after that batch fetches the leaf.

The quiet 511/512/513-parent matrix brackets two complete 256-key batches.
No warm case regresses more than 4%. Distinct-forward spill medians range from
−0.5% to +0.2%; memory changes range from −0.3% to +3.0%, with the largest change
1.683 → 1.733 ms. Small all-new graphs still pay the initial probes. The fallback
is a heuristic: two early misses followed by many hits can forgo later savings.
It changes which optimization runs, not which objects are retained.

Memory limits are 250,000 keys (the production default, no spill for these
fixtures) and 256 keys (deliberate spill stress). Timings include marking and
queue cleanup, but exclude fixture setup, exact-set audits and destruction of
the returned mark set. Process RSS includes setup and audits; it does not
isolate traversal memory. Raw paired results, exact runners, source snapshots
and build manifests are retained through [summary.json](summary.json).

## Release ingestion: no established end-to-end gain

Four balanced adjacent control/candidate pairs use the same evaluator source,
fixture, dependency lockfile and build settings. Both revisions remain retained,
as in a browser keeping an older revision. Ordinary pressure collection remains
enabled. Each of the eight runs passes both Git-tree/root/NAR identity checks
and all six stage checks; all 16 identities match the recorded references.

| Median per independent run | Control | Adaptive | Change |
|---|---:|---:|---:|
| Total wall time | 79.532 s | 80.078 s | +0.7% |
| Fresh ingestion, three stages | 42.284 s | 41.719 s | −1.3% |
| Update ingestion, three stages | 36.667 s | 38.024 s | +3.7% |
| Update store-tree conversion | 15.675 s | 16.959 s | +8.2% |
| Peak RSS | 1,331,595,264 B | 1,342,126,080 B | +0.8% |

Paired total changes are −0.6%, +3.0%, −6.6% and +3.6%. Stage medians are
computed independently and do not sum to the median total. The update slowdown
is reported, not relabeled as a gain. These observations do not demonstrate a
repeatable overall ingestion effect in either direction. They also do not
resolve the previously identified pressure-collection cost.

CPU affinity is unrestricted and recorded for these real-ingestion runs; OS
caches are managed normally. No owned build, test suite or other benchmark ran
concurrently. Read-only 100 ms marker polling observes two pressure-marker
updates per run. These are not exact collection counts or durations. The
closed stores and inventories are retained; the report archives raw stages,
resource counters, identities and marker observations.

The fixture publishes zero named roots. Although the candidate includes the
previous named-root optimization, that change cannot explain these results.
The comparison covers the fixed release revision pair only; it makes no claim
for nearby/week updates, browser responsiveness or all ingestion workloads.
The component change is retained for its repeatable full/partial-snapshot
benefit, with the adverse distinct-forward spill result and this inconclusive
ingestion result kept visible.

## Rejected candidates

The first candidate filtered every frontier whenever a snapshot pin existed.
Although full snapshots improved, the sparse spilled chain slowed 29.2%, with
all four pairs worse. It was rejected before ingestion measurement.

Filtering only the initial scan-dependency prefix removed that chain regression,
but distinct-forward spill slowed 4.8% with all four pairs worse. The matched
pin control was stable. That candidate was not accepted. Its release-ingestion
comparison passed all 16 identities but remained inconclusive: median total
84.302 → 81.876 seconds (−2.9%), with paired changes −2.4%, −8.5%, −0.3%, +4.8%.
The raw observations remain evidence of that earlier candidate, not the final
adaptive implementation.

## Correctness and integration

The generation scan still marks records and enqueues dependencies of newly
marked objects. FIFO order keeps that initial dependency prefix ahead of later
pins and descendants, including when the traversal queue spills. Each batch
consumes the original prefix length before filtering. Filtered-empty batches
continue draining the queue. Unpublished missing records are not marked or
counted against limits; later-published dependencies and their descendants
remain reachable. A failed pass aborts before pruning.

Tests cover exact marked keys and read/scan counts, in-memory and spilled
storage, multiple repeated frontiers, mixed snapshot/closure/resource pins,
unpublished inputs, exact and one-below object limits, and the 255/256/257 and
511/512/513 boundaries. The full native/Git/experimental library suite passes:
851 tests, zero failures, 52 ignored. Thirty-six Python harness tests pass.
The adaptive timing matrices contain 960 processes and 3,840 exact-set audits.
The registered smoke run additionally passes 576 processes and 1,152 audits,
and every smoke observation normalizes for the dashboard. Smoke ran alongside
correctness tests and is not timing evidence.

The permanent suite includes all six modes, all three graph shapes and both
storage limits. Smoke covers 127, 128, 255, 256, 257, 511, 512 and 513 parents;
standard adds 8,192. Repeat `--mode` and `--shape` to narrow a run.

```console
benchmark run collection-mark --profile standard --repetitions 4 --output results/snapshot-mark.json
benchmark all --suites collection-mark --profile smoke --repetitions 1 --output results/snapshot-mark-smoke
```

For an existing optimized build, use `--probe-binary PATH --no-build`, retaining
its `.build.json` sidecar. The component build uses release mode, no default
features, native/git/experimental features, and
`RUSTFLAGS='-C link-arg=-fuse-ld=mold'`. Ingestion uses its separately matched
release evaluator build with normal flags. The frozen benchmark's legacy pin
implementation is copied from `360539b`; the unchanged named-root reference
remains `3576508`.

# Named-root collection marking — 2026-10-06

Named-root traversal now inserts each key into the mark set before fetching its
record, so repeated edges do not reread metadata. The shared in-memory graph is
31% faster in the main same-executable comparison. Distinct graphs and deep
chains show changes near the unchanged-path controls. Pin traversal is unchanged.

This is a component improvement, **not an ingestion speedup claim**. The release
ingestion fixture publishes unrooted objects and retains snapshot pins. Four
retained stores each contain 247,671 objects and zero named roots. The planned
named-only ingestion comparison was therefore not run; snapshot-pin traversal
requires a separate investigation. Existing Turso WAL PR #9485 was already in
the baseline and is not added or duplicated here.

## Measured result

Milliseconds, median of four independent process means, with three warm
iterations per process. Each case also retains its first iteration separately.
Warm iterations are subsamples, not independent repetitions. These are traversal
times, excluding fixture setup, exact-set audits and returned mark-set cleanup.

| 8,192-parent case | Legacy named | Current named | Change | Unchanged pin control change |
|---|---:|---:|---:|---:|
| Shared leaf, memory | 49.914 | 34.327 | −31.2% | −3.3% |
| Distinct leaves, memory | 58.128 | 58.402 | +0.5% | +1.9% |
| Shared leaf, spill | 349.423 | 334.489 | −4.3% | +0.7% |
| Distinct leaves, spill | 510.921 | 507.579 | −0.7% | +0.6% |
| Single-root chain, memory | 88.188 | 89.266 | +1.2% | +1.1% |
| Single-root chain, spill | 219.390 | 222.334 | +1.3% | +3.5% |

The 8,192-parent shared graph performs 8,193 metadata record lookups instead of
16,384. The four paired warm changes in the main in-memory shared case are
−31.6%, −31.1%, −30.8% and −33.3%. A preceding same-executable screen measured
−29.9%. The spilled shared result is substantially noisier; no comparable broad
spill speedup is claimed.

The 127/128/255/256/257-parent shared cases improve about 25–32% with a
250,000-key memory limit. Around the 256-key spill threshold, gains mostly
disappear. Both sides of that threshold and of the 256-key frontier boundary
remain permanent benchmark cases. The chain uses one root and one-element
frontiers, exposing allocation overhead that broad graphs amortize.

## Comparison design and limits

The legacy named-root implementation is copied from `3576508` into a test-only
reference. Both algorithms use the same executable, dependencies and reopened
Turso fixture. Each process runs just one strategy and traversal mode. Matching
strategy processes run adjacently; four repetitions give equal AB/BA order,
with a fixed shuffle of case pairs. Pins enter identical code under both labels
and never run after named marking in the same process.

All timed runs used CPU 2, a performance core on the measured hybrid CPU. This
controls placement for the component comparison; it is not a deployment
recommendation. OS caches are not explicitly dropped. Process RSS includes
fixture construction and audits and is not a traversal-memory measurement.
The default 250,000-key limit keeps these fixtures in memory; the 256-key limit
deliberately exercises spill behavior.

The broad matrix uses the v2 probe, and the later chain matrix uses v3 with the
additional shape. Both compare strategies within their own executable. Production
collection code is identical between those builds. Immutable build manifests,
source snapshots, exact runners, raw process receipts, per-pair audits and
artifact hashes are linked from [summary.json](summary.json).

Earlier two-executable comparisons are retained as exploratory evidence. An
initial filter added membership checks and within-frontier deduplication to
both named and pin paths. It was discarded. Those runs also measured pins after
named marking in the same process, and even unchanged pins shifted between
builds. They do not isolate algorithm effects and are excluded from the table.

## Correctness and implementation

Successful named-root marking now reads each distinct record once. Provisional
marks cannot escape an error because the entire pass returns an error before
pruning. A filtered-empty frontier continues draining the queue. The object
limit admits at most one provisional key beyond the limit; an earlier missing
record still takes precedence over a later object-limit error. Ordering between
unrelated failures, such as spill I/O failure and missing metadata, can differ;
both abort collection before pruning. Tolerant unpublished pin inputs retain
their previous semantics.

Regression tests check exact marked keys and record counts in memory and spill
storage, continuation after 512 repeated root aliases, exact object limits,
and missing-root versus later-limit precedence. The full native/Git/experimental
library suite passes: 847 tests, zero failures, 52 ignored. Thirty-five Python
harness tests pass. The timed screen and matrices contain 608 successful
processes and 2,432 exact-set/cardinality/spill/revision audits. The registered
`benchmark all` smoke additionally passes all three shapes, and all final raw
results normalize for the dashboard. Smoke ran alongside correctness testing
and is used only as an integration check, not as timing evidence.

```console
benchmark run collection-mark --profile standard --repetitions 4 --output results/collection-mark.json
benchmark all --suites collection-mark --profile smoke --repetitions 1 --output results/collection-mark-smoke
```

The suite accepts `--shape shared|distinct|chain` (repeatable), `--mode named|pins`,
`--strategy legacy|current`, `--parents`, `--memory-limits`, and `--iterations`.
To reuse an exact optimized build, pass `--probe-binary PATH --no-build`. The
saved builds use release mode, native/git/experimental features, no default
features, and `RUSTFLAGS='-C link-arg=-fuse-ld=mold'`.

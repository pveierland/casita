# Shared Git import CPU admission

Run `python3 -m benchmarks.cli run git-shared-cpu --profile smoke --output RESULT.json`. The suite is registered in
`benchmarks/manifest.json` and `benchmark all`. Standard cases include one,
two and four concurrent imports into independent destinations, source workers
1/4, and shared limits 0/1/3/4/5. Zero disables shared admission. On a four-CPU
measurement allocation, 3/4/5 cover both sides of CPU capacity; 1 tests serialized
progress. File sizes 1048575/1048576/1048577 cover the source inflation boundary.

Each sample measures the combined elapsed time until every concurrent import
finishes. It never divides this makespan by the number of imports. Setup and
independent audits of every destination are outside timing. All destinations
must report exact imported/reused totals, complete closures, and independent
BLAKE3 plus exact streamed payload readback. Limited samples must observe a
positive non-warm peak within the configured limit; warm reuse requires no jobs.
The coordinator observes executing blocking jobs across source and destination.
Inline FastCDC, Bao hashing, async native verification, storage, and unrelated
operations are outside this scope.

Use bounded fixtures and process CPU metrics. Only cold pre-audit parent HWM
supports memory comparisons: later phases inherit earlier high-water marks.
`peak_source_bytes`, `peak_spill_bytes`, and `peak_decode_workers` are maxima over individual imports,
not simultaneous aggregate observations. The shared CPU peak is aggregate. `files` and `file_bytes` describe each source
fixture; total imported work grows with `imports`.

For comparisons, pass `--probe-binary CANDIDATE --baseline-binary BASELINE
--no-build --repetitions 5 --output RESULT.json`, with a selected explicit matrix.
Use separate source and target directories, identical benchmark fixtures, lock
files, compiler, features and flags; preserve compiler artifact records proving
fresh builds. Record hashes, exact commands, CPU affinity and competing work.
Do not build or run other benchmarks during measurements. Alternate baseline and
candidate order using the common runner. Compare against matched source worker
limits, single-import controls, and memory-backend controls. Add clustered packed
delta cases with explicit `--content clustered --delta-metrics` and evaluate
spilling separately. Preserve raw samples, provenance, and a retain/reject decision.

Use one `ImportCpuBudget` and pass its clones through
`GitClosureImport::with_cpu_budget` when independently constructed requests should
share admission. `with_cpu_concurrency` creates a new coordinator shared by that
request and its clones. Both are opt-in. The retained experiment and measured
small-workload costs are in
`benchmarks/reports/2026-10-01-git-shared-cpu/README.md`.

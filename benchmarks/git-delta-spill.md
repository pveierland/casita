# Git delta spill corpus

`benchmark run git-delta-spill` compares packed Git imports with opt-in
file-backed delta reconstruction. `benchmark run git-delta-limits` runs exact
resource-boundary tests. `benchmark run git-delta-disabled` checks the default
path while still recording actual deltas and import I/O. All three suites are
registered in `benchmark all`.

The import suite uses deterministic, streamed fixtures and independent BLAKE3
plus exact streamed readback. Each fixture records its actual packed blob delta
count. The candidate must report reconstructing those deltas on cold import;
warm and revision-reuse phases must not reconstruct already imported objects.
Paired variants must produce identical closure roots and exact import counts.

```sh
python3 -m benchmarks.cli run git-delta-spill --profile smoke --output /tmp/delta-smoke.json
python3 -m benchmarks.cli run git-delta-limits --output /tmp/delta-limits.json
python3 -m benchmarks.cli all --profile smoke --suites git-delta-spill,git-delta-disabled,git-delta-limits
```

Use `--probe-binary` and `--no-build` for an already compiled integration test
executable. Add `--baseline-binary` for alternating paired measurements. Use
separate source and target directories when compiling variants, and retain
exact patches, compiler options, lockfile and executable fingerprints. Neither
variant should be built while timing runs. See the retained report for the
actual commands and artifact manifests.

The default import corpus includes 16/64 files, 1 KiB/64 KiB/1 MiB/16 MiB file sizes,
and one/four source workers. Useful controls include `--content repeated`,
`--content random --pack-window 0 --layout both`, and budgets immediately
below/at/above one payload with `--max-buffered-bytes 1048575,1048576,1048577`.
Use `--no-delta-spilling --delta-metrics` for a disabled-feature control that
still records actual delta counts and I/O. The spill-limit suite covers below/at/above chain depth, declared work, spill
capacity, tiny-result/large-base reservation and stable source handle limits;
it also verifies draining and retrying a full planning window. Its time and RSS
include fixture construction and assertions and are diagnostic only.

Import process high-water marks are captured immediately before and after
import, before payload audits. Fixture generation and readback use fixed-size
buffers; Git child processes do not contribute to parent RSS. Only cold-phase
comparisons isolate the import from earlier high-water marks. Filesystem cache
memory is outside this process metric, so spilling is not a bound on total
machine or cgroup memory. `/proc/self/io` snapshots bracket the import; logical
write counts include all import writes, while physical writeback can lag and
writes to deleted temporary files can be cancelled. Unsupported systems report
missing I/O observations, never synthetic zeros.

The feature is disabled by default and enabled with
`GitClosureImport::with_delta_spilling(true)`. It applies only when bounded
source hints locate the complete chain; other objects retain the existing gix
path. Every selected base/result/instruction stream remains subject to payload
limits, aggregate work limits and the operation's shared spill quota. Native
Git identity, complete zlib termination, exact lengths and independent backend
digests remain checked. Invalid selected chains fail instead of falling back.

The measured decision, all comparison cells, host activity, correctness logs
and exact source artifacts are retained in
[the investigation report](reports/2026-10-01-git-delta-spill/README.md).

Import CPU observations can be required with `--cpu-metrics` on these suites.
The probe samples Linux `/proc/self/stat` immediately around import; the report
records raw `user_ticks` and `system_ticks` plus the runner's `SC_CLK_TCK` rate
in `configuration.process_cpu_ticks_per_second`. Totals include all process
threads and exclude child processes, fixture creation, and audits. Snapshot
work slightly widens the CPU interval relative to the wall-clock interval.
Unavailable or malformed counters fail a requested CPU measurement rather
than being treated as zero. Tick quantization makes short or warm imports
unsuitable for small percentage comparisons; retain zero observations and do
not divide by a zero baseline. CPU time excludes scheduling and storage waits;
stable CPU with slower wall time does not by itself identify the cause.

For example, using matched frozen binaries with this fixture:

```sh
python3 -m benchmarks.suites.git_delta_disabled --no-build --cpu-metrics \
  --probe-binary "$CANDIDATE" --baseline-binary "$BASELINE" \
  --file-bytes 4194304 --repetitions 5 --output cpu-control.json
```

# Shared Git producer-buffer admission

Run `python3 -m benchmarks.cli run git-shared-buffers --profile smoke --output RESULT.json`.
The suite is registered in the manifest, revision runner and `benchmark all`.
`git-shared-buffer-limits` permanently exercises rounding, both sides of minimum
source/writer allowances, oversized fallback rejection, and one-thread progress.
The standard throughput corpus also covers one byte below, at and above the
134 MiB source / 114 MiB destination transition to two complete allowances with
the default 64 MiB source window, four decoders and 32 uploads per writer.
Its boundary-test timings include fixture creation and correctness checks and are diagnostic.

`--buffer-budget SOURCE:DEST` selects separate shared capacities in bytes;
comma-separated pairs form a matrix and `0:0` disables admission. Capacities round
down to 64 KiB and reservations round up. Use `--baseline-buffer-budget` for the
paired baseline. `--shared-cpu-limit` and `--baseline-shared-cpu-limit` independently
select CPU admission, allowing neither/CPU-only/buffers-only/both comparisons.
`--chunk-upload-concurrency` and `--baseline-chunk-upload-concurrency` compare the
existing writer tuning; source worker and per-import logical window controls
remain available too.

For application use, create one `ImportBufferBudget::new(source_bytes,
destination_bytes)` and pass clones through `GitClosureImport::with_buffer_budget`
to the imports that should share admission. `with_buffer_limits` creates a fresh
budget; only clones of that request share it. CPU admission remains independently
selected with `with_cpu_budget`. Repositories and caller-owned mutation sessions
use the same request options. Custom backends that spawn writer creation must
explicitly propagate the budget's `scope`; task locals do not cross spawned tasks.

Every sample measures the combined makespan of all concurrent imports into
independent destinations. Each destination receives independent closure and
streaming payload audits outside timing. Gates check aggregate reservation peaks
against rounded capacities and require both partitions to be released immediately
after imports, before readback. Use cold pre-audit process HWM for memory claims;
later phases inherit earlier peaks. Tracked reserved envelopes are not measured
allocation counts or process RSS.

The implementation reserves complete conservative envelopes before decoding a source
window or opening a chunked writer. Source windows account for retained bodies,
each reader's fixed buffers, and concurrent reconstruction/probing scratch.
Writers reserve duplex/head/chunker/Bao buffers and every effective in-flight
chunk's plaintext and compression allowance. Effective upload concurrency is
clamped to the configured window and what the complete destination partition can
hold. This can serialize writers and cause head-of-line blocking; measure that
cost rather than assuming a memory limit is free.

The scope is enumerated producer payload buffers. Gix workspace/cache, locator
and manifest/index metadata, native verification buffers, codec workspace,
allocator overhead/reallocation transients, and backend-owned encoded payloads
are excluded. Transfer to ObjectStore or pack staging is an ownership boundary;
detached writes and retained backend storage can outlive producer cancellation.
This is not an end-to-end upload-memory or RSS guarantee.

Use matched fresh builds in separate source/target directories, identical fixture
and lockfile hashes, and at least five alternating pairs. Preserve exact patches,
commands, CPU affinity, host activity, correctness failures and negative results.
Do not overlap builds or other benchmarks with measurements. Retain/reject the
runtime change only after the CPU and buffer controls have been compared.

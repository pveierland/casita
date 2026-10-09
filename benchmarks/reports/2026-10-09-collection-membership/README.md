# Collection inventory membership batching

Bounded membership batches reduce the cost of collection inventory classification.
In six paired server trials with seven older revisions retained and read during
an update, the aged-collection workload improved **20.71% in wall time** and
**22.54% in sampled server CPU**, using median paired ratios. Recent-collection
updates increased **1.34% in wall time**, within the preset 5% limit. All ten
server gates and all microbenchmark gates passed.

## Server results

Each trial prepares an independent store. Recent cases keep the collection
timestamp recent; aged cases make collection eligible under the same disk
pressure. Every case verifies eight imported revisions, seven concurrent older
root read/hash overlaps, 28 total read/hash pairs, expected NAR identities and
the appropriate collection timestamp behavior. All 24 cases passed with
unchanged inputs and clean process cleanup in 4,623.9 seconds.

| Metric | Recent paired change | Aged paired change | Preset limit |
|---|---:|---:|---:|
| Target wall time | +1.34% | −20.71% | Recent +5%, aged −10% or better |
| Sampled server CPU | +1.38% | −22.54% | +5% |
| Whole-case sampled family RSS | +1.05% | +1.64% | +5% |
| Whole-case sampled physical WAL length | +2.13% | +0.03% | +10% |
| Target server OS write bytes | −0.22% | +0.71% | +5% |

The aged baseline and candidate wall-time medians are 66.763 and 52.970 seconds;
the recent medians are 4.852 and 4.854 seconds. A ratio of these medians differs
from the median of paired ratios used for every gate. Individual pairs are
retained: recent wall changes range from −7.99% to +28.18%, and one aged RSS
pair increases 12.35%. This is not a per-run regression bound or a confidence
interval claim. The six aged wall reductions range from 19.55% to 21.14%.

CPU uses server user/system ticks over enclosing samples and excludes
waited-child CPU. Process I/O can include reaped children. RSS sums owned
processes and can double-count shared mappings; RSS and WAL peaks are sampled.
There were no unavailable-process sampling events in the successful matrix.

## Implementation and correctness

Logical inventory classification, physical-manifest presence, stale payload
classification and chunk inventory classification now use batches of at most
256 keys. Frozen spill sets forward to the existing bounded membership API.
The spilled path still executes one SQL lookup per key; it shares connections,
statement preparation and blocking work across a batch. Payload-read concurrency,
pin protection, traversal, insertion order and sweep policy remain unchanged.

Batching can read ahead and change which planning error is encountered first.
Failed planning still prevents deletion. A started blocking batch may finish
after cancellation; this change does not establish a cancellation latency bound.
The qualified candidate passed 28 focused native tests again after the server
comparison, covering spill behavior, 255/256/257 boundaries, 8,192-file spilled
and in-memory inventories, injected failures and retry, pins, shared chunks and
durability. Five Python harness tests and the real CLI smoke's 15 cases passed.

The micro comparison retains 120 observations across ten inventory/memory-limit
configurations. The primary 8,192-file, 17-key-memory case improves 13.22% in
median paired plan time. All secondary wall/RSS gates pass. One small-case pair
is 2.262 times its baseline and remains included. Setup, a separate collecting
pass and exhaustive retained-data audits are outside plan timing; process RSS
includes them. The permanent suite covers 255/256/257 frontiers and both sides
of the live-set spill threshold and is registered in `benchmark all`.

## Reproduce and inspect

```console
benchmark run collection-inventory --profile smoke --repetitions 1 --output results/collection-inventory.json
benchmark all --suites collection-inventory --profile smoke --repetitions 1 --output results/collection-inventory-all
benchmark run server-ingest-pressure --configuration CONFIG.json --profile standard --repetitions 6 --output /tmp/pressure.json
python3 benchmarks/reports/2026-10-09-collection-membership/verify.py
```

Use the server benchmark configuration described in the main benchmark README,
including the qualified server build, corpus, independent read fixtures and WAL
observer. Keep the server output path short enough for Unix sockets. The exact
comparison runners, build manifests, schedule, raw receipts and source snapshots
are in [evidence.tar.gz](evidence.tar.gz), indexed by [artifacts.json](artifacts.json).
[Server results](server-analysis.json) and [micro results](micro-analysis.json)
retain every paired observation. `verify.py` checks the archive and recomputes
metrics without extracting files. External executables, SDKs, corpus checkouts
and CAS stores are hash-referenced, not embedded; the saved absolute-path runners
require their original environment or equivalent path restoration.

The server binaries share the qualified allocator, engine and evaluator settings.
Two Python-only benchmark fixes landed after the candidate build; the included
source qualification reconstructs its original source identity and proves that
native inputs did not change. The original binary manifests remain intact.
Earlier attempts are retained separately: an inventory fixture setup failure,
a server socket-path startup failure, and a partial server matrix interrupted
by a process-sampling permissions race. They do not contribute to the successful
matrix's ratios. The original failed process state is unknown; a real zombie
race reproducer motivated the sampler fix.

Existing upstream Turso WAL PR #9485 is separate from this change. These results
qualify collection membership batching for the measured ingestion workload;
the broader streaming-loader and cancelled-ingestion memory investigations
remain open.

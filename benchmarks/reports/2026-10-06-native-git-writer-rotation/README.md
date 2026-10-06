# Native Git import writer rotation

A repository-owned Git import previously retained one mutation writer throughout
all object publications. Its object-resource pin set therefore grew with the
imported closure. The importer now replaces that writer after eight publications
and a fully published decoded group. Caller-owned sessions retain their existing
lifetime contract and do not rotate.

Before replacement, the importer acquires independent generation and physical
payload protection while both the previous writer and previous protection remain
live. It retains that complete read hold, then uses the existing
admission-preserving rotation API. The original input snapshot stays live; one
current rotation hold remains between publications, with temporary overlap
during replacement. Final output-reader acquisition overlaps the writer
and retained protection. No public importer signature changes.

Accept the full read hold as a writer-resource, memory and I/O optimization. The retained nearby-revision matrix has a 4.73% longer median wall time, so this is not a universal speedup. The data-only alternative is not selected: its adverse initial week matrix and slower direct three-way result outweigh removing one extra logical snapshot. The original input snapshot, input-sized conversion maps and complete directory plan remain separate work.

## Correctness and limits

The selected-leaf regression uses real memory-store pins and a fully forwarding
metadata observer. It verifies protection at snapshot admission, exact writer
counts at boundaries, payload readback after collection while held, and final
reclamation after release. Cases cover 55/56/57/112/113 objects with batch seven,
concurrency one or three and byte budgets one/1024, both owned and borrowed.
Failure and cancellation after rotation leave no false closure witnesses, retain
partial output correctly and permit complete resumption. The selected full guard
adds one logical snapshot at publication boundaries after the first rotation; it is replaced
rather than accumulated.

The long-writer baseline failed the 57-object resource assertion and the
post-rotation failure test. An experimental weak-reference assertion that
rotation add no logical snapshot failed for the full guard at publication nine: two snapshots versus one initially.
The data-only guard satisfied that hypothesis, but its performance did not justify
selecting it. The hypothesis and both implementations remain archived. Both
variants passed 16 Git integration tests (one benchmark ignored), 857 native
library tests (53 ignored), and the production native/git configuration check.
Both retention-guard integration tests additionally passed with the data-only
refinement. Thirteen Python benchmark tests passed; documentation build and offline link validation passed.
A missing exact linker/rustfmt version initially prevented linking; that log is
retained separately from behavioral failures. Exact versions were restored.

Eight publications bound writer lifetime, not total heap. A decoded group may add
a partial publication before rotation. Wide parents pin their dependencies, and
final closure proof retains every selected root. The resource assertion excludes
that final proof intentionally; final output retention is checked separately.
No fixed input-independent memory bound or optimality of eight is claimed.

## Threshold benchmark

The permanent registered `git-closure-import` standard suite now includes
509/510/511 and 1022/1023 files, covering cold-object totals around 512 and 1024
with its two trees. It remains included in `benchmark all`; default byte budgets
also cover decoded-group boundaries. A regression exercises standard selection.
Both experimental variants were measured in 100 processes each: five balanced
pairs for each of five counts and two byte budgets, memory backend, loose Git
objects, concurrency three, 1024-byte files. Every process audits cold, warm,
subtree-delta and wide-delta operations, exact import/reuse counts, witness policy
and exhaustive closure identity: 400 operations per variant. Small timing changes
are mixed. Whole-process RSS includes fixture creation and audits and is not an
isolated import-memory measurement.

## Native ingestion

Each row contains four balanced control/candidate pairs. Each process imports
fresh A then B while retaining A, under ordinary pressure collection. All 24 runs
per variant pass 48 independent Git-root and NAR identity checks. The scenarios
use the saved pinned local nixpkgs mirror, not the latest branch head. Both sides
use accepted Mnos conversion rotation and the existing upstream Turso PR 9485 WAL
fix. Only Casita's native import writer policy changes.

Selected full guard, initial median candidate difference versus control:

| Scenario | Wall | Peak RSS | Written bytes | Peak WAL | Update total |
|---|---:|---:|---:|---:|---:|
| Near | +4.73% | -1.56% | -9.77% | +0.17% | +7.56% |
| Week | -14.55% | -5.59% | -13.27% | +0.23% | -24.02% |
| Release | -0.99% | -4.46% | -5.67% | +0.53% | +0.39% |

The separately frozen data-only alternative released the additional logical
snapshot while preserving generation and payload protection:

| Scenario | Wall | Peak RSS | Written bytes | Peak WAL | Update total |
|---|---:|---:|---:|---:|---:|
| Near | +1.60% | -0.90% | -9.82% | +0.26% | +0.59% |
| Week | +29.01% | +6.86% | +0.94% | +0.13% | +79.96% |
| Release | +1.33% | -3.23% | -9.21% | +0.29% | -3.35% |

The adverse data-only week result triggered a predeclared three-way confirmation:
all six permutations of control/full guard/data only, 18 fresh processes and 36
independent root/NAR checks. No runs were excluded. Median differences:

| Comparison | Wall | CPU | Peak RSS | Written bytes |
|---|---:|---:|---:|---:|
| Full guard vs control | -21.51% | -9.34% | -8.91% | -17.05% |
| Data only vs control | -10.92% | -4.38% | -6.55% | -12.96% |
| Data only vs full guard | +13.48% | +5.48% | +2.59% | +4.93% |

The control observed two pressure-marker updates in five of six runs, full guard
in one of six, and data only in three of six. A marker records collection
completion after collection/root eviction, under a 60-second cooldown; it does
not identify start or duration. This is a combined ordinary-pressure result,
not an isolated checkpoint or rotation-time gain. The earlier adverse week
matrix remains material and is not invalidated by confirmation.

The initial matrices are separate paired experiments; only the confirmation
interleaves all three variants. The first matrix contains substantial timing
drift and different observed pressure-marker counts. All runs and outliers remain included. Native
processes use the system allocator; these RSS values cannot be compared directly
with prior browser/mimalloc measurements. Component results and profiler timings
do not establish end-to-end throughput.

Process write counters are cumulative. Fresh import includes process/repository
startup; update import and conversion deltas subtract their preceding stage.
WAL is polled every 50 ms, giving observed lower-bound peaks. Phase labels follow
stdout and can lag by a poll. One final observer timestamp is 0.2 ms after
recorded process completion because the parent joins an in-flight poll afterward;
the unchanged raw sample is retained. Marker updates are not exact GC counts or durations.
OS caches are unmanaged. No owned builds/tests ran concurrently with measurements.
Possible checkpoint effects are inferences; checkpoint scheduling was not traced.

## Live heap diagnostic

For the same fresh release-A fixture, sampled live payload-heap peak changed from
284,526,983 to 234,269,164 bytes (-17.66%). The control
reuses the retained profile of the byte-identical accepted baseline executable;
the candidate uses the same Massif options and independently verifies root/NAR
identity. The data-only alternative is also profiled separately and retained.
Both diagnostics defer advisory pressure collection after its first marker. Emergency collection is unchanged.

This profile separates live allocation stacks from process RSS. It excludes
untracked mappings, allocator retention and thread stacks. Category maxima occur
at different times and cannot be added; hidden frames remain unresolved. It is
not an ordinary-GC throughput or browser-memory comparison. Raw trees, source,
checksums, category audits and the baseline profile are retained. The optional
`ms_print` formatter emitted deep-recursion warnings while producing its text
report; the authoritative raw trees passed the independent parser and allocation
sum checks.

## Reproduction

Build matching release integration probes with schema-2 manifests. The retained
Casita builds use `RUSTFLAGS='-C link-arg=-fuse-ld=mold'`; Mnos leaves RUSTFLAGS
unset so repository configuration applies. Compiler, flag origin/value, features,
lockfiles, target and configuration are matched; fixture identities are checked.

```sh
cargo test --release -p casita --no-default-features --features native,git,experimental --test git_closure_import --no-run --message-format=json
python -m benchmarks.suites.git_closure_import --probe-binary CANDIDATE --baseline-binary CONTROL --no-build --counts 509,510,511,1022,1023 --max-buffered-bytes 1024,65536 --file-bytes 1024 --concurrency 3 --backend memory --layout loose --repetitions 5 --output component.json
```

Native runners, fixture revisions, pair order, exact build commands, source
snapshots, raw process logs, all I/O/WAL observations and independent audits are
archived beside this report. Adapt their absolute checkout/evidence paths when
reproducing elsewhere. `artifacts.json` verifies compressed and uncompressed
checksums; frozen executables themselves remain in the local evidence directory.

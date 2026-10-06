# Native Git import writer rotation

The accepted conversion rotation leaves the global heap peak in native Git
import, where the writer's object-resource tree occupied about 60 MiB in the
retained profile. Test whether bounding that writer lifetime lowers the peak
without unacceptable ingestion time or I/O cost. Turso PR 9485 is already in
the baseline.

Repository-owned imports may replace their writer after eight actual
publications. Caller-owned mutation-session imports retain their existing
writer and publication behavior: that session promises to protect the result.
A private import writer represents these two ownership cases; no public API
or importer-trait signature changes are needed.

Before replacement, drain staging futures and publish every remaining staged
object in the decoded group. Acquire a fresh owned read hold while the prior
writer and any previous protection are still live, retain the complete hold,
rotate, then replace the previous hold. Keep this protection through final
output-reader acquisition. One current rotation hold remains between
publications, with temporary overlap during replacement. Delay
rotation until another nonempty decoded group needs staging, so an import
ending exactly at a boundary creates no empty writer. An awkward decoded group
can cause one additional partial publication; measure this cost.

Keep the native importer's initial metadata snapshot unchanged. A separate
Mnos conversion experiment released its input SQL snapshot after loading cache
hints and increased writes by 14–20%. That is a different lifetime from both
the native importer's input and its additional rotation guards; do not mix the
experiments. Eight publications bound writer lifetime, not total memory: wide
objects can pin many dependency keys in one publication.
Final closure proof also retains every selected root in the final writer;
the selected-leaf test measures its resource bound during object publication,
and separately checks final retention and reclamation.

Alternatives are independent mutation sessions (repeat admission policy),
per-object resource removal (requires stronger dependency/lifetime tracking),
or retaining the current long writer. Rotation reuses the already validated
admission-preserving API; retain the long writer if measurements reject it.

Correctness gates cover both ownership paths, threshold boundaries, awkward
batch/concurrency and byte-budget combinations, exact payload readback after
collection, no incomplete closure witnesses on cancellation/failure, and
continuous protection of published keys through every snapshot acquisition.
Use real memory-store pins with a forwarding observer, not simulated pins.
A separately measured data-only variant used weak references to verify release
of the additional logical snapshot. That hypothesis is retained as experimental
evidence, not a requirement of the selected implementation. The full hold had
better combined performance in the predeclared three-way week comparison and
lower sampled global heap. The original matrices include slower nearby imports
and an adverse data-only week result; retain all outcomes. These measurements
select a memory and I/O tradeoff, not a universal speedup or proof that eight
publications is optimal. See the retained qualification report in
`benchmarks/reports/2026-10-06-native-git-writer-rotation/`.

Extend the permanent Git closure benchmark to cover memory-backend boundaries
around eight publications of 64 objects (510/511 files plus two trees); include an additional
group and retain existing cold/warm/subtree/wide-delta correctness gates.
Freeze matched schema-2 builds before balanced measurements. Run the actual
Mnos near/week/release workloads with independent root/NAR identities; retain
time, RSS, writes and raw logs. A component result alone is not an ingestion
speedup. Review each proven commit with pm-review-work and fix findings with
pm-fix-no-commit. Rejected candidates retain evidence only.

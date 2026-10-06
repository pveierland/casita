# Mutation rotation implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task.

**Goal:** Bound a long ingestion writer's staging resources without admitting additional logical mutations and their advisory maintenance.

**Architecture:** Add `MutationSession::rotate(&mut self)` to prepare a fresh pin and payload batch, preserving catalog admission but reusing the original discovery/maintenance admission. Replace the old session only after preparation succeeds. Callers protect committed outputs with an independently acquired generation hold before rotation.

**Tech stack:** Rust, Tokio, Casita pin ledger, Python permanent benchmark harness.

**Spec:** The architecture and invariants below define this bounded experiment. Acceptance requires measured Mnos ingestion results. The earlier candidate using independent sessions remains rejected; the rotation variant is accepted as a memory optimization with the timing tradeoffs recorded below.

## Constraints and review focus

- Initial and independent sessions still run `before_mutation`; rotation skips it and discovery refresh only.
- Catalog synchronization and protection must run for the new pin, including catalog-backed repositories.
- Failure or cancellation during preparation leaves the original session and its published objects protected.
- A live staged object must prevent rotation through Rust's mutable borrowing rules.
- Retention handoff precedes rotation; all outputs survive actual collection, and released writers leave no permanent pins.
- Keep frozen benchmark sources unchanged until the running release comparison finishes. Never add Codex coauthor trailers.

## Task 1: Casita API and safety

Files: `crates/casita/src/repository/{mutation.rs,mod.rs,mutation_rotation_tests.rs}`.

- [x] Add a focused test using real memory metadata and pins: publish a blob, retain its generation, rotate, collect, and read its exact bytes. A counted mutation-start hook must run once across rotations and again for an independent session. Compile before implementation to verify the missing API failure.
- [x] Extract the existing pin/batch/catalog admission into private `Repository::prepare_mutation_session(discovery_refreshed: bool)`; the existing public constructor calls it with the maintenance result.
- [x] Implement `rotate` as `let replacement = self.repository.prepare_mutation_session(true).await?; *self = replacement; Ok(())`. Document that it does not transfer published-object retention.
- [x] Add failure/cancellation gates during preparation, catalog protection and collection coverage, plus a compile-fail staged-borrow example. Run focused tests, doctests and the complete native library tests.

## Task 2: Permanent measurement

Files: the new Rust test module, `benchmarks/suites/mutation_rotation.py`, `benchmarks/tests/test_mutation_rotation.py`, `benchmarks/{manifest.json,all.py}`.

- [x] Register a probe comparing one long session, independent sessions and rotation. Measure publication and retention handoff with 7, 8, 9, 16 and 17 publications; groups contain eight publications and each publication contains 64 unique blobs.
- [x] Exercise maintenance both ineligible and eligible using a deterministic injected hook; explicitly distinguish this from measuring the local wall-clock cooldown.
- [x] Verify exact bytes after GC, peak per-writer object resources, admission counts and final pin release. Emit the configuration and correctness gates with each timing.
- [x] Reject incomplete/mismatched results in the Python parser. Register the suite in `benchmark all`, run parser/registration checks and a quiet balanced comparison. Preserve raw process output and binary hashes.

## Task 3: Mnos experiment and acceptance

Files: `evaluator/crates/mnos-eval-cas/src/git/mod.rs`, existing `repository/git_publication_tests.rs`, evidence/report files.

- [x] Reuse one logical session across alias and directory groups; acquire the next generation hold before each needed rotation. Keep the original publication sizes and children-before-parents ordering.
- [x] Run the existing pin-continuity/collection test, cancellation tests and full CAS suite. Freeze an optimized matched binary and its source manifest.
- [x] Measure fresh plus near/week/release updates with independent root/NAR checks and ordinary GC. Retain all samples, including the earlier week outlier and incomplete harness attempt. Re-profile allocation retention if acceptance depends on the refined variant's heap behavior.
- [x] Review both proposed commits with `$pm-review-work`; resolve findings with `$pm-fix-no-commit`. Preserve adverse tradeoffs and rejected experiments.
- [ ] Create the separate reviewed Casita and Mnos commits (record their identities in the execution ledger).

## Acceptance outcome

Safety and correctness validation passed: 857 native Casita tests, 32 doctests,
44 Python harness checks, and 76 Mnos CAS tests plus eight subprocess checks.
The permanent benchmark completed 120 processes across both sides of the writer
boundary. Rotation preserved one admission while bounding each writer's retained
object resources; independent sessions admitted again at each boundary.

All 32 native ingestion runs preserved 64 independently checked root/NAR
identities. Median peak RSS fell 4.48% for release and 7.48% for the confirming
week set; near RSS was mixed. Confirmed update medians rose about 57–80 ms.
The initial drifting week set remains reported, including its 5.87% update
increase. This is a memory optimization with timing tradeoffs, not an ingestion
speedup or a universal nonregression claim.

The sampled heap peak while Converter allocations were live decreased by
44,249,766 bytes (15.80%); the global heap peak did not decrease. Ordinary runs
all observed one pressure-marker update, so they do not prove removal of the
earlier independent-session latency event. Both final commits passed the six
review criteria and independent source/evidence review. The broad ingestion
optimization goal remains open.

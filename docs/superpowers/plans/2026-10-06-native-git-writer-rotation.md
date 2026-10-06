# Native Git Writer Rotation Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans inline. User authorizes autonomous experiments and separate commits only for proven work.

**Goal:** Measure and, if justified, reduce native Git import writer-resource memory.

**Architecture:** An internal writer distinguishes owned from borrowed sessions. Owned imports retain prior publications before rotating at a drained staging boundary; borrowed imports preserve their session's lifetime contract.

**Tech Stack:** Rust/Tokio, Casita memory and local metadata, Python benchmark harness, Mnos ingestion probe.

**Spec:** `docs/superpowers/specs/2026-10-06-native-git-writer-rotation-design.md`

## Global Constraints

- Eight actual publications per writer, then finish the decoded group.
- Caller-owned sessions do not rotate; no new public API.
- Preserve the initial SQL snapshot and final reader handoff.
- Permanent benchmark cases and independent root/NAR identity gates.
- Separate proven commits; pm-review-work and pm-fix-no-commit for findings.
- No Codex coauthor trailers.

## Review Focus

- Boundary-ending imports must not create an empty trailing writer.
- Staged objects from the current decoded group cannot outlive their writer.
- Published incomplete parents must survive rotation until their dependencies arrive.
- Failed/cancelled discovery must not create false completeness witnesses.
- Caller-owned sessions must protect all imported objects after the engine returns.

## Task 1: Safety and ownership integration

Files: `crates/casita/src/git/repository/closure_import.rs`,
`crates/casita/src/importers/git_closure.rs`,
`crates/casita/tests/git_closure_import.rs`, and a focused test support module.

Interfaces: consume `MutationSession::rotate`, `Repository::owned_read_hold`;
produce an internal owned/borrowed writer with a shared session accessor.

- [x] Add a forwarding metadata observer that records staging pin identities,
      maximum object resources and verifies protection at snapshot admission.
- [x] Add selected-blob fixtures at 55/56/57/112/113 objects with batch seven,
      concurrency one; assert <=56 resources and ceil(objects/56) owned writers.
      Repeat with borrowed sessions, expecting one writer retaining all objects.
      Add concurrency three and byte budgets one/1024 with a bound of 58.
      Collect and compare every payload before releasing the result; then prove
      final collection removes the unrooted objects.
- [x] Run `cargo test -p casita --no-default-features --features native,git,experimental --test git_closure_import native_import_writer -- --nocapture`;
      expect the owned 57-object case to exceed 56 before implementation.
- [x] Add the owned/borrowed writer and staged-object lifetime barrier:

```rust
// Before the next decoded group, only when owned and the threshold was met:
drop(staged);
writer.rotate().await?; // acquire fresh hold before replacing the old writer
staged = Vec::new();
publications = 0;
// After finishing a decoded group at the threshold, publish any partial tail.
```

- [x] Rerun the focused tests, all Git closure integration tests (including
      failed and cancelled discovery), and native library tests. Require no
      failures; inspect diagnostics and preserve logs.

## Task 2: Permanent benchmark and matched measurement

Files: `benchmarks/suites/git_closure_import.py`, corresponding Python tests,
benchmark manifest/documentation as needed, and retained evidence reports.

Interfaces: existing `benchmark_git_closure_import` emits exact import/reuse
counts and audited root identity; schema-2 manifests prove matched builds.

- [x] Add default memory rotation-boundary counts 509/510/511 and 1022/1023 to
      the registered standard suite (files plus two trees). Exercise default
      selection with a fake probe and assert every requested count ran.
- [x] Freeze baseline and candidate release integration probes with matched
      flags/configuration/dependencies. Run balanced paired threshold cases,
      recording all four operations and exact correctness gates.
- [x] Freeze matched Mnos import-cost probes; use retained near/week/release
      scripts and independent root/NAR tables, adding process-write counters.
      Record every run, outlier, and observed GC marker without exclusion.
- [x] Profile identical accepted-baseline/candidate workloads if native RSS
      supports the hypothesis; distinguish total peak from component maxima.
- [x] Accept only supported tradeoffs, otherwise restore runtime changes and
      retain reproducible rejected-experiment evidence. Review with
      pm-review-work and an independent reviewer, fix material findings, then
      create the separate proven commit without coauthor trailers.

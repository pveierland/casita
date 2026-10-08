# Retained verified read paths

Do not route every retained verified read through the existing scoped path.
This warmed screen found lower elapsed time through 16 KiB, a small increase
at 16 KiB + 1, and a substantial increase for concurrent multi-chunk reads.
Production routing is unchanged. Small-payload routing remains a candidate for
separate correctness and native-ingestion qualification, not an accepted fix.

Each observation reads 64 prepared payloads through authenticated EOF. Both paths
share one retained snapshot, its exact catalog and its existing process reader
pin. The scoped variant calls `open_proof_scoped` followed by the same decoder
used by `open_verified`. Timing includes opening, proof production, verification,
and output buffering. Metadata lookup, public-reader wrapping, setup, warming,
and final byte comparisons are excluded. Both paths warm first; each shape then
runs unscoped/scoped/scoped/unscoped/unscoped/scoped.

| Payload bytes | Concurrency | Unscoped median ms | Scoped median ms | Change |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | 3.679 | 1.617 | −56.06% |
| 0 | 64 | 1.615 | 0.864 | −46.52% |
| 4096 | 1 | 4.714 | 2.623 | −44.37% |
| 4096 | 64 | 2.601 | 1.311 | −49.58% |
| 16383 | 1 | 7.639 | 5.240 | −31.40% |
| 16383 | 64 | 5.019 | 3.217 | −35.91% |
| 16384 | 1 | 9.498 | 5.623 | −40.80% |
| 16384 | 64 | 4.477 | 2.631 | −41.23% |
| 16385 | 1 | 11.371 | 11.914 | +4.78% |
| 16385 | 64 | 6.400 | 6.583 | +2.85% |
| 524289 | 1 | 92.757 | 100.138 | +7.96% |
| 524289 | 64 | 61.190 | 112.468 | +83.80% |

All three fixed pairs for the concurrent multi-chunk case were adverse
(+94.07%, +67.32%, +85.25%). This supports rejecting a blanket routing change
without treating a single short process run as a statistical or production
performance result. There are three observations per path and shape, with no
confidence interval. Host activity and OS cache contents were uncontrolled.

The local fixture uses a 128 KiB pack target and disables the compressed chunk
cache. Runtime configuration is four workers and at most 64 blocking threads.
Nonempty fixtures have 64 distinct deterministic payloads; empty reads share one
payload. The probe checks actual bare/flat manifests and reports chunk counts.
Every sample validates complete bytes; warmed reads must leave the pin-ledger
revision unchanged, and final teardown must release the pin. Both sides of the
16 KiB proof boundary are included. Scoped bare reads buffer and hash earlier;
manifest reads construct pinned plans and use another proof-data reader. The
difference cannot be attributed solely to removing filesystem existence checks.

The benchmark is permanent at
`crates/casita/src/repository/retained_verified_bench.rs`, registered as
`benchmark run retained-verified-paths` and included in `benchmark all`.
The actual recorded run exercised the `benchmark all` prebuilt-binary path.
All 72 samples passed; the guarded process family exited successfully with no
watchdog stop, forced cleanup, unexpected/adopted child, or survivor. Its
wait4-reported maximum RSS was 284,528,640 bytes, including setup and runner
bookkeeping; this is not a simultaneous sum across family members or a measure
of per-path allocation ownership or peak memory.

The source base is Casita `b3712df3b8768f65f9bcced87aaaa76f35ffe923` with the
retained benchmark patch. The qualified full-source fingerprint is
`f13d6086d2f7ac1c1513516db7e493596bb6a44ee8b7acc5ba3eb4ca381ca881`.
Executed ELF SHA256:
`136e6c5db40509a4af7684127384367a313cd34167145b5240bf0bcb86e92807`.
The release diagnostic used debug information and LTO disabled; it is not a
production CLI build or an ingestion speed comparison.

Build and review history is retained. Initial compilation succeeded in 11m47s,
but its ownership guard rejected one child adopted after Cargo exited. That
child exited zero and no process remained. The failed guard result was not
rewritten or accepted as a successful qualification. After runner-only fixes,
a fresh Cargo invocation reported the unit artifact cached, with identical ELF
bytes and clean ownership. The initial runner suite failed its registry check;
after correction all 36 runner/CLI tests passed. The result parser additionally
rejected 14 changed-order, membership, duration, correctness and layout cases.
The unused first qualification script is retained as an unexecuted draft.

No owned build, test, compression or analysis overlapped the performance run.
Time/resource guards were 360 seconds, 2 GiB sampled family RSS, and 256 MiB
minimum sampled free disk, with 512 MiB required at launch. They are sampled
cooperative guards, not OS-enforced ceilings. This experiment does not close the
original concurrent cancelled-server OOM, last-interest termination, or broader
ingestion-performance goal. The upstream Turso WAL PR remains separate.

`artifacts.json` identifies original and compressed evidence bytes.
`external-inputs.json` lists three large executable/archive identities whose
bytes are not included. Run `python3 verify.py` here to verify the retained bytes,
cross-document identities and all twelve numerical summaries without the live
worktrees. This replays evidence checks, not the performance workload or external
binary bytes. Raw samples and every fixed-pair change remain in the archive.

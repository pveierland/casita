# Git source reopening: transient resident memory

The candidate lowers the shared decoded-byte source reopening trigger from 128 MiB to 32 MiB. The production evaluator comparison below measures a local Git mirror; the native-view and focused ingestion results qualify their separate paths and allocator configurations.

This is a reopening trigger, not an RSS cap. A closure decode group or oversized object can pass the trigger before the next reopening. Detached native-view bodies own their bytes while staging continues. Both builds use the existing cache capacities, validation, writer rotation, publication rules and Turso/WAL baseline.

The runtime patch changes one constant in `crates/casita/src/git/repository/mod.rs`. It affects the inventory-free closure importer used by Mnos (16 MiB pack cache) and native Git-view imports (64 MiB object cache). Reopening discards source mappings and cache state; performance in both paths is therefore qualified.

The final production comparison used four balanced pairs of fresh processes and stores, with all eight complete JSON outputs equal to the retained pinned reference. It uses the real MiMalloc evaluator binary. Rows report medians; percentages compare those medians.

| Production Git metric | 128 MiB | 32 MiB | Change |
|---|---:|---:|---:|
| Wall seconds | 47.322 | 47.320 | -0.01% |
| CPU seconds | 41.863 | 41.715 | -0.35% |
| wait4 peak RSS MiB | 1101.91 | 1029.16 | -6.60% |
| Process output MiB | 378.68 | 378.88 | +0.05% |
| Observed WAL peak MiB | 42.20 | 41.67 | -1.28% |

Individual production pairs remain visible rather than being replaced by the median:

| Pair | Wall change | CPU change | RSS change | Output change | Observed WAL change |
|---|---:|---:|---:|---:|---:|
| 1 | +0.53% | -1.96% | -5.93% | +0.04% | -0.33% |
| 2 | +0.53% | +0.12% | -8.27% | +0.07% | -1.99% |
| 3 | -0.01% | +0.44% | -7.59% | -0.52% | -2.54% |
| 4 | -2.10% | +1.22% | -6.14% | +0.04% | +2.78% |

The earlier two-pair production screen and two instrumented mapping runs are retained separately. All 93 raw smaps snapshots were reparsed. At each run’s sampled RSS peak, local pack/index residency was about 392.80 MiB with the control and 261.02 MiB with the candidate. Those peaks occurred at different times; these observations do not attribute unnamed memory to an allocator or establish timing effects. The source is a large local historical mirror, so this mapping cost does not represent a shallow remote fetch.

The focused native probe uses matched older evaluator scaffolding and the system allocator. It retains the older revision while importing the update, with ordinary pressure collection enabled. Each scenario has four balanced pairs and 16 exact tree/NAR identity checks. These values are separate from production MiMalloc measurements.

| Native scenario | Fresh total change | Update total change | Process wall change | CPU change | Peak RSS change | Output change | Observed WAL change |
|---|---:|---:|---:|---:|---:|---:|---:|
| near | -11.10% | -21.48% | -9.77% | -5.08% | -1.09% | +0.05% | +0.48% |
| week | -3.75% | -4.82% | -3.64% | +1.21% | +0.96% | +0.00% | +0.04% |
| release | -12.68% | -4.96% | -8.26% | +4.51% | -8.05% | +0.32% | -0.08% |

Individual stage medians include the adverse measurements:

| Native scenario | Fresh import | Fresh conversion | Fresh NAR | Update import | Update conversion | Update NAR |
|---|---:|---:|---:|---:|---:|---:|
| near | -7.94% | -10.56% | -22.94% | -7.69% | -11.02% | -22.51% |
| week | +1.82% | -9.20% | +1.68% | -20.45% | -29.79% | +67.60% |
| release | -16.48% | -11.65% | +2.47% | -14.15% | -4.44% | +0.84% |

In the week-apart case, update NAR rose from 7.457 s to 12.498 s (+67.60%), while the update total fell 4.82% and peak RSS rose 0.96%. Long NAR stages occurred in both variants, and the per-run conversion and NAR times varied. Pressure-marker timing is consistent with collection work moving between stages, but does not establish that cause or rule out a stage-specific regression. The 50 ms process-exit polling interval cannot explain this multi-second stage difference.

CPU changes use the sum of retained wait4 user and system seconds. Per-pair native resources remain visible:

| Scenario/pair | Wall change | CPU change | Peak RSS change | Final process writes change | Observed WAL change | Marker updates control/candidate |
|---|---:|---:|---:|---:|---:|---:|
| near/1 | -0.96% | +2.08% | -5.42% | -0.07% | -0.20% | 2/2 |
| near/2 | +2.58% | -1.42% | +0.27% | +0.31% | +1.49% | 2/2 |
| near/3 | -18.76% | -7.25% | -3.87% | -0.69% | +1.13% | 2/1 |
| near/4 | -16.75% | -3.49% | +1.93% | +0.09% | -1.07% | 2/1 |
| week/1 | -3.58% | -1.60% | -3.53% | +0.25% | +0.56% | 2/2 |
| week/2 | -6.70% | -0.95% | -0.56% | -0.12% | -0.08% | 2/2 |
| week/3 | +1.21% | +2.93% | +0.43% | -0.11% | +0.26% | 2/2 |
| week/4 | -2.16% | +4.44% | +2.18% | -0.30% | -0.72% | 2/2 |
| release/1 | -6.67% | +1.81% | -6.91% | +0.76% | +0.20% | 2/2 |
| release/2 | -17.80% | +1.80% | -7.22% | +0.15% | -0.10% | 2/2 |
| release/3 | -9.36% | +7.54% | -7.95% | +7.98% | +0.19% | 2/2 |
| release/4 | -0.79% | +2.16% | -8.24% | +0.15% | -0.40% | 2/2 |

The matrix preserves the ordinary pressure policy, not an identical collection schedule. Marker updates can vary between runs and are not exact collection counts; wall times include any policy effects. The existing local policy has a 60-second collection cooldown, so small timing differences can change when another attempt becomes eligible. A timing difference cannot be attributed specifically to source reopening from these observations alone.

Native-view qualification used the permanent delta-packed 2,049 × 64 KiB fixture. The first two-pair screen showed large adverse concurrency-16 wall times; two additional pairs reversed that result. A separate controlled experiment then generated the fixture once, fsynced only its own files and directories before timing, and timed each baseline/candidate operation adjacently before checkout/fsck. All 48 operations across the three experiments passed the identity and content checks. Do not pool the controlled timings with the earlier screens.

| Native-view experiment | Concurrency | Operation | Wall change | CPU change | RSS change | Output change |
|---|---:|---|---:|---:|---:|---:|
| screen | 1 | initial-import | +4.79% | -2.56% | -29.79% | +0.00% |
| screen | 1 | incremental-import | -13.07% | -1.88% | -8.64% | -0.02% |
| screen | 16 | initial-import | +46.92% | -3.03% | -29.19% | +0.01% |
| screen | 16 | incremental-import | +57.87% | +3.03% | -6.67% | +0.14% |
| confirmation | 1 | initial-import | -21.39% | -9.06% | -29.99% | +0.02% |
| confirmation | 1 | incremental-import | -15.42% | -12.57% | -8.57% | +0.03% |
| confirmation | 16 | initial-import | -19.97% | -2.42% | -29.06% | +0.01% |
| confirmation | 16 | incremental-import | -20.98% | +2.34% | -5.95% | -0.17% |
| paired | 16 | initial-import | -0.40% | -3.81% | -28.95% | -0.01% |
| paired | 16 | incremental-import | +0.38% | -1.46% | -6.69% | +0.00% |

The controlled experiment’s first pair was slower in both variants than its later pairs; all pairs are retained. The large original slowdown did not repeat. The observed RSS reduction is consistent, but these finite measurements do not prove performance equivalence or identify the cause of the earlier wall-time variation.

Correctness validation passed 857 native unit tests and 33 Git integration tests. Existing ignored tests remain ignored; the targeted benchmark probes were run explicitly. The Python benchmark checks passed 40 tests. Four native measurement-runner fault tests cover observer failure, thread-start failure, a late post-exit poll, and a stalled active-process poll.

Eight permanent cases are registered in `benchmarks/manifest.json`, `benchmark all`, and revision builds. They cover both paths below and above 32/128 MiB, plus 33/129 MiB oversized blobs. The actual corpus run passed 68 operations. Native-view byte inventories come independently from Git; closure counters come from the tested importer. The audit accepted the valid corpus and rejected six corrupted variants covering duplicate operations/processes, wrong layouts and wrong binary fingerprints.

| Registered case | Operations | Cold reachable/decoded bytes by file count |
|---|---:|---|
| git-closure-source-window-32 | 12 | 31: 32507129, 32: 33555745, 33: 34604361 |
| git-closure-source-window-128 | 12 | 127: 133174265, 128: 134222881, 129: 135271497 |
| git-closure-source-window-oversized-32 | 4 | 2: 69206129 |
| git-closure-source-window-oversized-128 | 4 | 2: 270532721 |
| git-view-source-window-32 | 12 | 511: 33506468, 512: 33572038, 513: 33637608 |
| git-view-source-window-128 | 16 | 2046: 134156418, 2047: 134221988, 2048: 134287558, 2049: 134353128 |
| git-view-source-window-oversized-32 | 4 | 2: 69206282 |
| git-view-source-window-oversized-128 | 4 | 2: 270532874 |

Build provenance is retained for every frozen binary. The Casita base is `90435a1145c10a7492136110f07a3db04df88e7a`; the corpus and native-view candidate were built with the one-line runtime patch plus the benchmark changes. The production binary’s Casita fingerprint predates those benchmark-only edits; its frozen runtime bytes match. The native-view control has the same benchmark overlay and dependency lock, with the 128 MiB runtime constant. Its tracked sources differ from the candidate only at that constant. The ignored Casita `Cargo.lock` is archived as `source-window-casita-Cargo.lock`; use it for the recorded `--locked` build commands. The production baseline is evaluator `d27fb6d` and the candidate is `bbb4d06`, with byte-matched evaluator runtime sources; the intervening commits contain evidence.

To reproduce analysis, run the archived `verify-source-window-archive.py` with this report directory. It extracts to a fresh temporary directory, verifies compressed and original SHA-256 hashes, validates all seven frozen protocol hash maps and the CLI lockfile, reruns the standalone auditors, and requires byte-identical audit outputs and aggregate results. `artifacts.json` maps archived paths to original paths and hashes; `results.json` retains the complete summaries and adverse pairs. The measurement runners, build/freezing scripts, benchmark overlays, exact commands, raw logs, snapshots and reference inputs are included. Executables and full stores stay in the task’s retained scratch space, outside the report.

For measurement reproduction, restore the recorded checkouts, benchmark overlay and lockfile; use the toolchain/build settings in the schema-2 sidecars. Run the eight registered source-window suites with `python -m benchmarks.cli all --bin-dir <frozen-binaries> --profile smoke --repetitions 1 --suites <comma-separated eight IDs above> --output <fresh-directory>`. The archived protocols give the exact comparison order and commands. Complete builds/tests first, then run measurements sequentially without owned builds, tests, profiles or compression competing.

Limits: process RSS includes reclaimable file-backed pages; a lower process peak is not proof of lower cgroup pressure or resolution of the historical 10 GiB OOM. Mappings are sampled and non-atomic. Native WAL polls give lower-bound peaks and lagging stage labels; marker updates do not count collections. Native process exit observation can add up to one 50 ms poll. Source caches and recorded CPU affinity follow each protocol; unrelated host activity remains uncontrolled. Unique object-byte inventories do not count repeated delta-base decoding or OS reads. No remote-transport, browser-session, cancellation or universal-optimality claim follows from these experiments.

# Git closure audit reuse

`paired-audit.json` compares closure imports of packed linear Git histories
before and after publication audits reused objects proven earlier in the same
attempt. The candidate is the `git_closure_custom_formats` probe built from
commit `42b46fd`. The baseline is the same tree with
`crates/casita/src/repository/mutation.rs` restored from its parent `931fca9`;
the report's recorded revision is that parent with the change still
uncommitted. Both were release builds from one `Cargo.lock` and toolchain, but
the report has no build manifests to prove dependency equality.

Each configuration ran five alternating pairs on 2026-10-02 with the memory
backend. Every sample passed exact import counts, a source-free warm import
and exhaustive closure verification. Link-audit counts are deterministic and
identical across repetitions.

| Commits | Batch | Baseline audits | Candidate audits | Baseline median | Candidate median |
|---:|---:|---:|---:|---:|---:|
| 16 | 64 | 456 | 48 | 13.2 ms | 6.2 ms |
| 16 | 4096 | 456 | 48 | 12.2 ms | 7.3 ms |
| 64 | 64 | 4,224 | 192 | 54.8 ms | 8.1 ms |
| 64 | 4096 | 6,432 | 192 | 67.5 ms | 7.9 ms |
| 256 | 64 | 18,378 | 768 | 235 ms | 37.1 ms |
| 256 | 4096 | 99,456 | 768 | 1.00 s | 41.4 ms |
| 1024 | 64 | 64,224 | 3,072 | 771 ms | 136 ms |
| 1024 | 4096 | 1,577,472 | 3,072 | 14.3 s | 120 ms |

Rows use the custom registry. The candidate audits each of the three objects
per commit exactly once. The baseline grows quadratically with the commits
sharing one witness batch. Smaller batches let earlier batches' witnesses cut
later walks short, which bounds but does not remove the repetition. Built-in
registries perform no link audits in either variant; their paired medians
differ by -13% to 19%, within the noise of the shared host.

The host ran unrelated builds during measurement. The 16-commit, 64-object
custom case ranges from a 197% slowdown to a 69% reduction at millisecond
scale; its audit counts, not its timings, show the effect.

```sh
python3 -m benchmarks.suites.git_closure_audit --commits 16,64,256,1024 --registry both --publication-batch-objects 64,4096 --repetitions 5 --baseline-binary /path/to/baseline --probe-binary /path/to/candidate --no-build --output paired-audit.json
```

Build each probe with `cargo test --release -p casita --no-default-features
--features native,git,experimental --test git_closure_custom_formats --no-run`
and copy it out of the target directory before building the other tree.

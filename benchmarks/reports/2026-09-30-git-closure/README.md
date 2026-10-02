# Git closure import harness validation

`baseline-validation.json` is a single-sample validation of the
`git-closure-import` harness against the original closure importer. It was
captured on 2026-09-30 from a dirty worktree based on
`c57185a19b2009bec39858101d7621c505a01cb9`, before this feature was extracted
onto its current base. The raw report and executable fingerprint are unchanged;
its timings do not measure this branch.

The run audits 64 samples: four operations (cold, warm, subtree delta and wide
delta) for eight random files of 1 KiB or 256 KiB, memory and local backends,
loose and packed sources, and staging concurrency 1 and 16. Every sample passed
the suite's exact imported/reused counts, source-free warm import and
exhaustive closure verification. One sample per configuration validates the
harness and correctness gates only; it establishes no speedup or significance.

```sh
python3 -m benchmarks.suites.git_closure_import --counts 8 --max-buffered-bytes 67108864 --backend both --layout both --file-bytes 1024,262144 --concurrency 1,16 --content random --probe-binary /path/to/closure-baseline --no-build --output baseline-validation.json
```

Build the probe with `cargo test --release -p casita --no-default-features
--features native,git,experimental --test git_closure_import --no-run`. The JSON
records its SHA-256, platform, every process output and the exact fixture
dimensions. Its executable path is descriptive; a fresh build has a different
hash. Performance claims need paired baseline and candidate runs with at least
five repetitions, as described in the benchmark README.

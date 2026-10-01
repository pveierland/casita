# Historical Git closure import evidence

These original results describe the recorded executable and environment from
2026-09-30. They predate this extraction and updated upstream dependencies.
The raw JSON remains unchanged; its timings are not current-branch measurements.

# Git closure import optimization evidence

The initial release baseline passed seven integration tests. Its workload-matrix
validation contains 64 audited samples: eight files, memory/local backends,
loose/packed sources, 1 KiB/256 KiB deterministic random files, and staging
concurrency 1/16. These single samples validate the harness; they do not establish
speedup or statistical significance.

```sh
python3 -m benchmarks.suites.git_closure_import --counts 8 --max-buffered-bytes 67108864 --backend both --layout both --file-bytes 1024,262144 --concurrency 1,16 --content random --probe-binary /path/to/closure-baseline --no-build --output baseline-validation.json
```

Build the executable with `cargo test --release -p casita --features
git,experimental --test git_closure_import --no-run`. The JSON records its
SHA-256, platform, all process output and exact fixture dimensions. Its temporary
binary path is descriptive; a fresh build may have a different hash.

Further experiments must retain paired before/after runs with at least five
repetitions and independent correctness gates before a production optimization
is accepted.

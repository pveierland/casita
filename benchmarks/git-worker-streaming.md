# Git worker streaming corpus

`git-worker-streaming` adds mixed-size workloads to the uniform-size
`git-object-workers` corpus. Every sixteenth file has the selected large size;
other files are 1 KiB. Both suites run memory/local destinations and loose/packed
sources, and check exact imported/reused counts and exhaustive closure validity.
Fixture creation and correctness audits are outside import timing.

The mixed-size smoke profile crosses the 64 KiB oversized-object threshold,
then admits a wider window. The standard profile also crosses 4 MiB and tests
16, 17 and 64 files. Both include one-worker controls and four-worker windows.
The uniform-size worker corpus additionally crosses the two-object 128 KiB
window boundary at 128 KiB minus one byte, exactly 128 KiB and plus one byte.

```sh
python3 -m benchmarks.suites.git_worker_streaming --profile smoke --output mixed.json
python3 -m benchmarks.suites.git_object_workers --profile smoke --output uniform.json
python3 -m benchmarks.all --suites git-worker-streaming,git-object-workers --profile smoke
```

Build comparison probes from separate source checkouts with the same Cargo.lock,
compiler, flags and features. Copy the chosen lockfile into both checkouts first;
this repository does not track Cargo.lock. Dependency downloads remain allowed:

```sh
cargo test --locked --release -p casita --no-default-features \
  --features native,git,experimental --test git_closure_import --no-run --message-format=json
```

Copy each reported executable before another build can replace it. The runner
records executable hashes and validates optional `.build.json` sidecars. Compare
equal worker counts explicitly; the baseline defaults to one worker:

```sh
python3 -m benchmarks.suites.git_worker_streaming \
  --probe-binary /path/to/candidate --baseline-binary /path/to/baseline --no-build \
  --baseline-decode-workers 4 --decode-workers 4 --counts 64 \
  --file-bytes 4194304 --max-buffered-bytes 67108864 --repetitions 5 \
  --output paired-mixed.json
```

A streaming window hands off each completed, verified object to asynchronous
staging. It still inflates an individual object completely, and admits no next
window until current source workers and destination writes finish. The reported
source-byte limit covers admitted object bodies; pack caches, delta workspaces,
and destination buffers are additional. Whole-process RSS includes fixture
creation and audits, so it cannot isolate importer memory use.

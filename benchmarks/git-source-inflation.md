# Incremental Git source inflation

`git-source-inflation` compares loose blobs around the
1 MiB streaming threshold and at larger sizes, with matched one/four decoder
counts and byte-admission budgets. It reuses the closure import corpus's cold,
warm, changed-subtree and wide-tree operations. The smoke matrix uses loose sources. Packed objects continue through the existing
gix decoder; `--layout packed` remains available as a buffered control.

```sh
python3 -m benchmarks.suites.git_source_inflation --profile smoke --output smoke.json
python3 -m benchmarks.all --suites git-source-inflation --profile smoke
```

Build separate baseline/candidate checkouts with the same test fixture and lockfile.
Copy the chosen ignored Cargo.lock into both checkouts before these comparison builds:

```sh
cargo test --locked --release -p casita --no-default-features \
  --features native,git,experimental --test git_closure_import --no-run --message-format=json
python3 -m benchmarks.suites.git_source_inflation \
  --probe-binary /path/to/candidate --baseline-binary /path/to/baseline --no-build \
  --counts 1,4 --file-bytes 67108864 --max-buffered-bytes 67108864 \
  --repetitions 5 --output paired-large.json
```

Fixture generation writes 64 KiB buffers to Git and independently hashes the
payload with BLAKE3. Full readback uses another fixed buffer and checks lengths
and hashes, in addition to exact import/reuse counts and closure validation.
Packing and audits are outside the import timer. Every bounded-fixture sample
must pass these gates; an old probe that ignores this mode is rejected.

`parent_hwm_after_import_bytes` comes from Linux `/proc/self/status` immediately
after the timed import, before readback or closure audit. It excludes child Git
processes, unlike the process wrapper's whole-run RSS measurement. It remains a
parent-process high-water mark, including repository initialization and fixture
or earlier-phase work; use the cold phase and compare its before/after values.
It is not a direct live-allocation counter. This memory corpus requires Linux.

The byte-admission budget remains based on declared object sizes. Streaming
readers add two 64 KiB buffers plus inflater state per admitted reader; source
permits bound executing jobs, not the number of admitted readers. Pack caches,
gix delta workspaces, and destination buffers are additional.

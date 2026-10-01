# Incremental Git source inflation

`git-source-inflation` compares loose and non-delta packed blobs around the
1 MiB streaming threshold and at larger sizes, with matched one/four decoder
counts and byte-admission budgets. It reuses the closure import corpus's cold,
warm, changed-subtree and wide-tree operations. Source packing uses window zero
by default; `--content clustered --pack-window 16 --counts 16` exercises actual delta fallback.

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
index hints, gix delta workspaces, and destination buffers are additional.

## Optional pack locator bounds

Loose hits do not initialize pack hints. On a loose miss the locator examines
at most 256 directory entries, retains at most 32 indexes, and attempts to read
at most 16 MiB of index snapshots in total. Invalid snapshots consume the byte
budget. Snapshots own their bytes so concurrent file truncation cannot invalidate
an index mapping. Oversized, missing, stale, or unrepresented indexes fall back
to gix; the source inflater optimization is therefore best effort. Delta entries
also use gix. The existing gix source handle and its indexes have separate costs.

The `git-source-locator` corpus covers below/at/above each cap and asserts which
path was selected. Padding lives in an earlier root and the usable index is the sole entry in a
later root. The pack is linked into that later root after index discovery,
outside timing. Reported time sums initialization and subsequent selection;
this isolates the exact boundary from directory enumeration order.
File/entry values count preceding hints; byte values include the usable index.
Missing packs and malformed padding indexes are intentional. Each selected
stream or gix fallback is checked against the complete original payload.

```sh
python3 -m benchmarks.suites.git_source_locator --profile smoke --output locator.json
python3 -m benchmarks.all --suites git-source-locator --profile smoke
```

This companion suite measures optional locator setup and selection only. It
uses a library test probe with `git,experimental` and default features. Its
whole-process RSS includes fixture allocations and is not an inflater-memory
measurement. End-to-end import claims must use the separate inflation corpus.

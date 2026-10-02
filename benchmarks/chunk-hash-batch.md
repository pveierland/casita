# Chunk hash batching corpus

`chunk-hash-batch` measures `ChunkedBlobStore::put_slice` for an empty destination
and then the same content in the same destination. The duplicate measurement
follows a complete cold-write audit. Both memory and local object stores are
available. Fixture construction, ordinary BLAKE3 hashing, reference FastCDC,
and full verified readback are outside the timed interval. Fixtures occupy a
whole payload buffer, matching the existing decoded Git blob staging path;
process memory measurements do not isolate writer memory.

Every sample requires the ordinary BLAKE3 blob identity, exact reference chunk
boundaries and hashes, exhaustive verified readback, and a real object-store
counter proving zero chunk writes on the duplicate pass. Paired variants must
produce identical ordered chunk metadata. The reported `manifest_hash` hashes
that canonical metadata, rather than the on-disk manifest encoding.

The permanent standard cases include upload concurrency 1/3/4/5/16, memory for
one/three/four 64 KiB admission units, a 1 KiB many-chunk configuration, the
256 KiB default average, periodic duplicate-heavy input, sizes immediately
below/at/above the small-file cutoff, average chunk sizes below/at/above 512 KiB
(where maximum chunks cross the 1 MiB job cap), oversized chunks, and
single-upload writes whose chunks fall below, across and above the 4 KiB bound
under which a group holding one chunk is hashed without a blocking job.
Actual content-defined cuts need not hit the cap exactly; unit tests cover
exact group-byte and group-count boundaries independently.

Paired against per-chunk blocking hash jobs on the memory backend (nine
repetitions on four pinned CPUs), duplicate writes of 1 KiB chunks took 48.5%
less time at concurrency 1 and 56.9% less with a one-chunk budget, and cold
writes at concurrency 16 took 13.2% less; the 256 KiB default average was
within noise. Most of the concurrency 1 gain comes from hashing a lone small
chunk inline: against grouping alone, `inline-2048` duplicate writes took 51.6%
less time, `inline-4096` 12.4% less, and `inline-8196` was unchanged.

```sh
python3 -m benchmarks.suites.chunk_hash_batch --profile smoke --output hash-smoke.json
python3 -m benchmarks.all --suites chunk-hash-batch --profile smoke
```

For comparisons, build the identical fixture and lockfile in separate source
checkouts **and separate Cargo target directories**. Freeze each freshly built
executable before any subsequent Cargo command. Use matching compiler, features,
and flags, and preserve source patches and executable hashes.

```sh
CARGO_TARGET_DIR=/tmp/hash-baseline-target \
  cargo test --offline --locked --release -p casita --no-default-features \
  --features native,experimental --test chunk_hash_batch --no-run --message-format=json
# Repeat in the candidate checkout with its own target directory, then freeze binaries.
python3 -m benchmarks.suites.chunk_hash_batch --profile standard \
  --baseline-binary /path/to/baseline --probe-binary /path/to/candidate --no-build \
  --repetitions 5 --cpu-affinity 0,1,2,3 --output hash-paired.json
```

`--cases many-4,default-4` selects named cases from the suite's `CASES` map.
Each process emits cold and duplicate samples. Variant order alternates per
repetition. The runner reports paired reductions for each phase separately;
one smoke repetition is correctness evidence, not a performance conclusion.

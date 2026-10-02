# Bounded chunk hash jobs: retained with workload limits

> Historical evidence from the original `git-import-perf` investigation, preserved
> from commit `59a91d9ac13d02ea6c9b464bf8310a69b2372b1e`. These timings and test counts
> describe the original source variants recorded below and in the raw artifacts.
> They do not measure this extracted branch on upstream `6e9642e`. Reproduce
> historical Git controls using that original source tree and its Git benchmark
> harness; this independent chunk pipeline branch carries only its own suites.
> `ORIGINAL-SHA256SUMS` preserves the original checksum list. `SHA256SUMS`
> updates only this README after the provenance notice; raw artifacts are unchanged.

Keep the bounded hash-batching implementation. At a 1 KiB average chunk size and
upload concurrency 16, duplicate writes improve by about 21% on both tested
backends, with all ten paired observations faster. This is a specific writer
throughput benefit, not evidence of general Git import acceleration. Serial
many-chunk memory writes and oversized cold memory writes regress; retention
accepts those measured tradeoffs. Default chunk-size cold writes remain mixed.

## Change

The writer groups already-admitted chunks into CPU jobs of at most four chunks
and 1 MiB total. An individually oversized admitted chunk runs alone. Each
result is sent independently through a nonblocking oneshot channel into its
existing pin, deduplication, compression, and upload path. Duplicates still skip
compression and chunk writes. Chunk concurrency and source ordering count
individual chunks, not groups. Small/single-chunk prehash reuse remains intact.

Partial groups flush before memory admission waits, upload/reorder waits, EOF,
and pending source reads. While input is pending, the writer continues polling
admitted uploads. The latter also fixes an existing progress problem: a producer
could pause below the upload limit while already-admitted chunks never reached
storage. Both changes are present in the timed candidate; the timings cannot
attribute every improvement solely to fewer hash jobs.

Each queued/running job owns every input buffer and its shared byte-budget
permit. Cancellation never releases a permit while CPU work still owns its
bytes. Sending results never waits for storage. A failed detached job closes
unfulfilled result channels, producing an upload error before publication; the
original panic detail is not retained. Global CPU admission and compression
batching are separate future experiments.

## Measurements

Positive reductions mean faster candidate writes; negative values mean a
regression. Five alternating-order pairs per case were followed by five fresh
pairs for promising cases and regression controls. `paired.json` contains 760
samples across all 19 permanent cases; `repeat.json` contains 280 more.
`summary.csv` preserves separate runs and `combined.csv` combines their pairs.

| Configuration | Phase/backend | Combined median reduction | Positive pairs | Range |
|---|---|---:|---:|---:|
| 1 KiB average, concurrency 16 | duplicate / memory | 20.9% | 10/10 | 10.6% to 27.5% |
| 1 KiB average, concurrency 16 | duplicate / local | 21.1% | 10/10 | 11.6% to 26.3% |
| 1 KiB average, concurrency 16 | cold / memory | 8.0% | 8/10 | -2.6% to 14.3% |
| 1 KiB average, concurrency 16 | cold / local | 12.1% | 9/10 | -1.4% to 16.2% |
| 1 KiB average, concurrency 1 | cold / memory | -5.1% | 3/10 | -37.8% to 11.4% |
| 1 KiB average, concurrency 1 | duplicate / memory | -4.5% | 2/10 | -22.3% to 12.2% |
| 1 MiB average, oversized chunks | cold / memory | -1.9% | 2/10 | -20.3% to 5.0% |
| 256 KiB average, concurrency 4 | cold / memory | -0.7% | 5/10 | -13.8% to 11.3% |
| 256 KiB average, concurrency 4 | cold / local | -1.1% | 4/10 | -24.4% to 10.8% |
| 256 KiB average, concurrency 4 | duplicate / memory | 2.9% | 9/10 | -3.7% to 15.9% |
| 256 KiB average, concurrency 4 | duplicate / local | 4.9% | 8/10 | -8.3% to 10.0% |

The serial many-chunk memory regression repeated. Oversized memory cold writes
regressed in all five repeat pairs (median -2.4%). The initial oversized local
cold regression did not repeat; combined median is -0.6%, with 4/10 positive.
No threshold was selected after measurement to hide these costs. The corpus
retains concurrency 3/4/5, admission capacities 1/3/4, averages around the byte
cap, oversized chunks, and all three small-file boundary sizes. Exact grouping
boundaries are covered by unit tests because FastCDC cuts depend on content.

The standalone probe times `put_slice` with a whole deterministic input buffer;
reference hashes, chunking, and full readback are excluded. Duplicate timing
follows the cold audit and benefits from that warmed state. It uses real memory
or local object stores and real duplicate-write counters, but omits repository
ledger/pin/publication overhead. No memory saving is claimed. Fixture buffers,
runtime, backend storage and correctness audits are part of process RSS.

## Complete Git controls

`git-workers-1.json` and `git-workers-4.json` each contain 80 samples: eight packed
random files, 1 KiB and 4 MiB bodies, local destination, import concurrency 16,
64 MiB source budget, and matched decode-worker counts. The small-file cases
exercise prehashed storage. The 4 MiB cases exercise many default-sized chunks
inside actual repository mutation and publication. Cold, warm, subtree-delta and
wide-delta imports all pass exact import/reuse counters, full closure/readback
audits, and paired root identity checks.

These timings are too variable to establish broad benefit or non-regression.
Large-blob cold median reductions were 2.0% with one decoder and 7.1% with four,
but both include substantial slower pairs. The initial serial wide-delta median
was -15.5%; `git-serial-repeat.json` adds 40 samples and gives +7.9% on repetition.
Combined serial wide-delta is -0.4%, 5/10 positive, range -201.0% to +41.5%.
Large-blob serial cold combines to +1.1%, 6/10 positive, range -75.8% to +26.5%.
`git-summary.csv` and `git-combined.csv` preserve all phases and spreads. The
cause of the large variance was not isolated. Warm closure reuse does not hash
payload chunks and is a control, not a batching speedup claim.

## Validation and source identity

- Initial behavioral tests failed at 189 hash jobs for 189 chunks and at paused
  source progress. The corrected candidate passes both.
- 103 chunked-backend unit tests pass (one existing ignored test), including
  exact four-chunk/1 MiB bounds, exclusive oversized jobs, queued four-permit
  cancellation, one blocking thread, and partial groups under small budgets.
- 18 release integration regressions pass: Git closures, completion ordering,
  bounded manifest metadata, and streaming manifests. The benchmark fixture
  correctness test also passes on both frozen variants, including the even
  FastCDC minimum on the byte-boundary cases.
- 23 Python runner/revision tests pass, including mandatory audit/configuration,
  paired chunk identity, duplicate-write failures, and corpus registration.
- `benchmark all` runs the registered smoke suite successfully: 24 candidate
  samples. `all-smoke-execution.json` and `all-smoke.json` retain its evidence.
- All 1,264 retained performance/smoke samples pass their correctness gates.

Both variants start at `05f224bcdbe6a8dfb700c6b94f66f6f0a5a19653`. Baseline adds
only the identical new probe. Candidate adds the hash batching implementation
and tests. Builds used separate Cargo target directories, Rust 1.96.0, identical
lockfile/features/flags, and freshly compiled integration executables frozen
immediately after the build. Each `*.build.json` records executable, fixture,
lockfile, source patch and source-tree hashes. `*.patch.gz` reconstruct exact
Rust inputs from the source head; `Cargo.lock.gz` preserves the common lockfile.
The hash probes' fixture SHA256 is
`07d6786b17a6505ef42f512d4c5a63cb556567803098df8994a6ba9bc0a1100a`.
No compilation ran during paired measurements. CPU affinity was 0,1,2,3. This is
one host, without a claim of whole-host isolation or formal significance testing.

## Reproduction

See `benchmarks/chunk-hash-batch.md` for the permanent corpus and isolated build
procedure. Apply each archived Rust patch to a separate checkout of the source
head, restore the lockfile, build its probe in a distinct target directory with
`--no-default-features --features native,git,experimental --release --locked`,
and freeze the resulting executable. Substitute their paths below.

```sh
python3 -m benchmarks.suites.chunk_hash_batch --profile standard \
  --baseline-binary BASELINE --probe-binary CANDIDATE --no-build \
  --repetitions 5 --cpu-affinity 0,1,2,3 --output paired.json
python3 -m benchmarks.suites.chunk_hash_batch \
  --cases many-1,many-4,many-16,default-4,default-16,oversized,small-+1 \
  --baseline-binary BASELINE --probe-binary CANDIDATE --no-build \
  --repetitions 5 --cpu-affinity 0,1,2,3 --output repeat.json
for workers in 1 4; do
  python3 -m benchmarks.suites.git_closure_import --counts 8 \
    --file-bytes 1024,4194304 --content random --layout packed --backend local \
    --concurrency 16 --max-buffered-bytes 67108864 \
    --decode-workers "$workers" --baseline-decode-workers "$workers" \
    --baseline-binary GIT_BASELINE --probe-binary GIT_CANDIDATE --no-build \
    --repetitions 5 --cpu-affinity 0,1,2,3 --output "git-workers-$workers.json"
done
# Repeat the workers=1 Git command with --file-bytes 4194304 for git-serial-repeat.json.
```

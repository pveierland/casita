# Incremental chunk manifests

> Historical evidence from the original `git-import-perf` investigation, preserved
> from commit `cbbe63f5f00ff46501fd0818f62df4d6c20a5cdb`. These timings and test counts
> describe the original source variants recorded below and in the raw artifacts.
> They do not measure this extracted branch on upstream `6e9642e`. Reproduce
> historical Git controls using that original source tree and its Git benchmark
> harness; this independent chunk pipeline branch carries only its own suites.

Decision: retain incremental manifest construction for its bounded metadata and measured peak-memory reduction on streams with many chunks. The measurements do not establish a general Git throughput improvement or universal non-regression.

The writer keeps at most 64 completed entries behind an earlier upload, plus its existing upload concurrency window. Contiguous entries feed canonical 64-entry leaf pages immediately. The page builder retains fewer than 64 references per tree level. This bounds manifest bookkeeping; it does not bound the complete process, source object buffers, backend storage, caches, or pin inventories.

Both revisions use completion-order uploads and the same dependency lock, features, Rust compiler and flags. Executables were preserved before testing. Every case has five alternating pairs on CPUs 0–3, with no concurrent build started by this investigation. Other host work remained possible.

## Streaming stress workload

The fixture repeats a deterministic 64 KiB block and configures 1 KiB average chunks with four uploads and a 1 MiB byte budget. Fixture generation and exhaustive verified readback use fixed-size buffers. An independent streamed BLAKE3 prehash precedes timing. Peak RSS is sampled immediately after write/close, before readback. The periodic source limits unique memory-backend payloads while manifest entry count grows. Every pair must produce identical blob and manifest hashes.

Positive timing percentages mean improvement; positive RSS differences mean lower candidate peak RSS. RSS is the process high-water mark, not a direct count of manifest allocations.

| Input bytes | Backend | Median time reduction | Pair range | Median RSS reduction MiB | RSS pair range MiB |
|---:|---|---:|---:|---:|---:|
| 65536 | memory | -6.5% | -20.3–16.9% | 0.20 | -0.07–0.32 |
| 65536 | local | 9.8% | -0.5–24.6% | 0.18 | -0.54–0.32 |
| 16777216 | memory | 3.7% | -45.4–5.2% | 0.62 | -0.81–0.75 |
| 16777216 | local | -25.9% | -62.9–25.1% | -1.13 | -1.34–1.57 |
| 67108864 | memory | -0.7% | -13.0–6.7% | 1.42 | 0.56–1.64 |
| 67108864 | local | -1.5% | -4.2–8.4% | 1.24 | 0.55–1.89 |
| 268435456 | memory | 0.8% | -86.1–5.1% | 7.88 | 5.46–9.82 |
| 268435456 | local | -24.6% | -52.5–53.4% | 8.45 | 4.68–14.27 |

Small inputs do not consistently reduce RSS. At 256 MiB every pair reduces RSS: median savings are 7.88 MiB in memory and 8.45 MiB locally. Local stress cases at 16 MiB and 256 MiB have median time regressions of 25.9% and 24.6%, respectively, with pair ranges crossing zero. These throughput costs and variability are accepted alongside the memory benefit; this is not a speed optimization claim.

## End-to-end Git control

The local backend imports two incompressible loose Git blobs per source using the normal 256 KiB average chunk size. The 8 MiB inputs cannot exceed 64 chunks at the minimum chunk size; 32 MiB plus one byte requires more than 64 at the maximum chunk size. Those local cases cover flat and paged storage without the 1 KiB stress configuration. Git memory cases use MemoryBlobStore rather than ChunkedBlobStore and serve as unchanged-backend controls, not evidence about manifest performance. Every sample checks exact imported/reused counts and exhaustively verifies the resulting closure.

| Bytes per file | Backend | Operation | Median time reduction | Pair range |
|---:|---|---|---:|---:|
| 8388608 | memory | cold | 5.8% | -2.2–17.0% |
| 8388608 | memory | warm | -5.2% | -19.4–32.5% |
| 8388608 | memory | subtree-delta | 21.8% | -24.5–32.6% |
| 8388608 | memory | wide-delta | -6.9% | -56.4–4.5% |
| 33554433 | memory | cold | 2.7% | -95.5–51.9% |
| 33554433 | memory | warm | -1.5% | -144.8–45.2% |
| 33554433 | memory | subtree-delta | 8.7% | -109.1–35.3% |
| 33554433 | memory | wide-delta | -3.8% | -62.0–44.6% |
| 8388608 | local | cold | 2.0% | 0.4–4.6% |
| 8388608 | local | warm | 6.6% | -19.4–61.8% |
| 8388608 | local | subtree-delta | 8.5% | -23.6–49.2% |
| 8388608 | local | wide-delta | 15.0% | -1.2–44.0% |
| 33554433 | local | cold | 2.3% | -6.4–12.2% |
| 33554433 | local | warm | 10.0% | -127.8–18.7% |
| 33554433 | local | subtree-delta | 0.2% | -11.9–9.0% |
| 33554433 | local | wide-delta | -7.8% | -15.7–-0.5% |

Local cold-import medians improve by 2.0% and 2.3%; the larger case has a pair range crossing zero. Unchanged memory-backend controls are especially noisy at 32 MiB. The 32 MiB local wide-delta case regresses in every pair (median 7.8%). Its new tree and 13-byte blob take the unchanged single-chunk writer path, but the preceding cold import used the changed path and may affect backend, cache or allocator state. A candidate-induced regression is not ruled out. This is an accepted unresolved tradeoff against bounded bookkeeping and measured memory savings, not a dismissed noise result. No universal non-regression or general Git speedup is claimed.

## Correctness and reproducibility

The library suite passes 88 tests (one benchmark ignored). Release integration passes 17 tests across manifest streaming, upload completion, and Git closure import (three benchmarks ignored). Benchmark harness/registration checks pass 31 tests. Tests compare exact manifest bytes at 0, 1, 63, 64, 65, 4095, 4096, 4097 and 10000 entries; they also cover bounded reordering, small byte budgets, failed publication and corruption.

The first implementation exposed a pin-gate deadlock: a page write could wait for a lock held by an unpolled chunk upload. A deterministic real-lease regression timed out before the fix. Page writes now poll existing uploads while awaiting completion, without admitting additional source chunks. That test passes, and independent follow-up review cleared the finding.

The permanent chunk-manifest-stream suite is registered in benchmarks/manifest.json and benchmark all. Its standard corpus includes all stress sizes measured here. Raw reports retain samples, correctness results, build fingerprints and paired ranges.

The baseline source is 204fb1c plus the identical new chunk_manifest_stream.rs fixture, without production changes. The candidate is the incremental-manifest implementation in this commit. Build both with cargo test --offline --locked --release -p casita --no-default-features --features native,git,experimental --test chunk_manifest_stream --test git_closure_import --no-run; preserve each executable before building the other revision. Raw artifact metadata records the exact compiler, flags and lock hash. Reproduce with separately preserved, lock-matched executables:

```sh
python3 -m benchmarks.suites.chunk_manifest_stream --file-bytes 65536,16777216,67108864 --backend both --repetitions 5 --cpu-affinity 0,1,2,3 --baseline-binary BUFFERED --probe-binary INCREMENTAL --no-build --output /tmp/manifest-paired.json
python3 -m benchmarks.suites.chunk_manifest_stream --file-bytes 268435456 --backend both --repetitions 5 --cpu-affinity 0,1,2,3 --baseline-binary BUFFERED --probe-binary INCREMENTAL --no-build --output /tmp/manifest-large.json
python3 -m benchmarks.suites.git_closure_import --counts 2 --file-bytes 8388608,33554433 --max-buffered-bytes 67108864 --content random --layout loose --decode-workers 1 --repetitions 5 --cpu-affinity 0,1,2,3 --baseline-binary GIT_BUFFERED --probe-binary GIT_INCREMENTAL --no-build --output /tmp/manifest-git.json
```

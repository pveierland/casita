# Chunk upload completion scheduling

> Historical evidence from the original `git-import-perf` investigation, preserved
> from commit `204fb1c2d54aeb8ee4c2bd152c193d5bd26a9992`. These timings and test counts
> describe the original source variants recorded below and in the raw artifacts.
> They do not measure this extracted branch on upstream `6e9642e`. Reproduce
> historical Git controls using that original source tree and its Git benchmark
> harness; this independent chunk pipeline branch carries only its own suites.

Decision: retain completion-order scheduling. With one in eight chunk puts delayed by 8 ms (the remaining puts delayed by 1 ms), every measured pair improves. Median payload-write reductions range from 27.1% to 45.6%. This is a controlled straggler experiment, not evidence that ordinary local disks or every Git workload improve by that amount.

The unchanged FastCDC boundaries, physical BLAKE3 root, manifest order, and Bao-verified full readback are checked for every sample. All uploads complete before manifest publication. Source offsets restore canonical order after completion; upload concurrency and byte permits are unchanged. The implementation still retains one metadata entry per chunk and does not claim bounded manifest memory.

The 160 samples use five alternating baseline/candidate pairs per case, identical dependency locks and feature configuration, immutable executables, and CPU affinity 0–3. Both executables contain the same refined Git workers. The unrelated build was confined to CPUs 4–5; unrelated host work remained possible.

| Bytes | Budget | Delay ms | Median paired reduction | Pair range |
|---:|---:|---:|---:|---:|
| 65536 | 196607 | 0 | 9.9% | -62.2–55.7% |
| 65536 | 196607 | 8 | 27.1% | 7.1–29.5% |
| 65536 | 196608 | 0 | 2.0% | -78.8–20.1% |
| 65536 | 196608 | 8 | 29.1% | 26.8–29.7% |
| 65536 | 196609 | 0 | -24.0% | -218.5–9.4% |
| 65536 | 196609 | 8 | 41.1% | 36.6–42.9% |
| 65536 | 1048576 | 0 | -2.9% | -48.7–14.4% |
| 65536 | 1048576 | 8 | 41.7% | 39.4–42.9% |
| 1048576 | 196607 | 0 | -9.9% | -23.3–18.5% |
| 1048576 | 196607 | 8 | 29.4% | 28.9–30.1% |
| 1048576 | 196608 | 0 | 2.9% | -5.4–20.3% |
| 1048576 | 196608 | 8 | 27.2% | 24.2–33.4% |
| 1048576 | 196609 | 0 | 2.5% | -64.3–31.7% |
| 1048576 | 196609 | 8 | 45.6% | 32.4–48.1% |
| 1048576 | 1048576 | 0 | 8.6% | -41.5–21.4% |
| 1048576 | 1048576 | 8 | 45.4% | 44.7–48.4% |

Zero-delay controls are noisy: medians range from -24.0% to +9.9%, and every range crosses zero. They do not establish a general throughput improvement or universal non-regression. Adoption is justified by the repeatable straggler benefit and removal of head-of-line admission blocking, with canonical identities preserved.

Byte permits round to 64 KiB. Budgets 196607 and 196608 admit three small chunks; 196609 admits four. The regression fixture uses a 1 MiB budget and identifies the first source chunk by its digest, independent of backend arrival order. That chunk waits for an upload beyond the initial four to start: the ordered implementation times out; completion-order scheduling passes. An earlier 64 KiB fixture admitted only one chunk and was not valid evidence of ordering; it was corrected before these measurements.

The permanent `chunk-upload-completion` corpus is registered in `benchmarks/manifest.json` and `benchmark all`. Its standard configuration includes both sides of 64 KiB, 192 KiB, and 256 KiB byte-budget boundaries, plus chunk-size boundaries. Two focused Rust tests pass, covering straggler progress and independent canonical-content audits.

Reproduce after building each revision with the same lock and features and preserving separate executables:

```sh
python3 -m benchmarks.suites.chunk_upload_completion --file-bytes 65536,1048576 --budgets 196607,196608,196609,1048576 --delays-ms 0,8 --repetitions 5 --cpu-affinity 0,1,2,3 --baseline-binary ORDERED --probe-binary UNORDERED --no-build --output /tmp/chunk-completion-paired.json
```

The existing chunk-storage library suite also passes: 76 tests, including upload concurrency limits, progress below the full upload-window budget, publication failures, verified reads, corruption, and paged metadata. One permanent benchmark is ignored in that test run.

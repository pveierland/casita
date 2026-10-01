# Verified Git blob-file registration

Historical evidence preserved from the original implementation at `f9e67a2fbe4c548ec3556e0eb405966412a57ae8`.
The raw report and executable fingerprint are unchanged. These measurements
predate this extraction onto upstream `6e9642e`; they do not measure the new base.
Both strategies run in the same executable and share raw-blob closure witnesses.

All alias comparisons use seven alternating pairs per payload size and backend,
with both strategies in the same executable. The 168 total samples
(84 per strategy) each check exact identity, length, exhaustive closure validity and
byte-for-byte readback. The timed operation includes registration and rooted
publication. The reread strategy also benefits from the built-in raw-blob
closure proof, so the comparison isolates registration savings.

```sh
python3 -m benchmarks.suites.git_blob_file --backend both --repetitions 7 --cpu-affinity 0,1,2,3 --probe-binary /path/to/git-blob-file-probe --no-build --output /tmp/blob-alias.json
```

Build with `cargo test --release -p casita --no-default-features --features
native,git,experimental --test git_blob_file --no-run`. Reports retain executable
fingerprints and every process output. Percentages are medians of paired
reductions, not ratios of independent medians or statistical-significance claims.
The host ran other work; small timings and large ranges must be interpreted with
that limitation.

The retained alias combines native-object, raw-object and payload protections in
one update, then checks the current native record before issuing the seal. Its
collection regression supplies a formerly valid snapshot after collection has
removed the native record; the operation must reject it.
`blob-alias-combined-pins.json` contains the final 168 audited samples:

| Backend | Size | Median paired time reduction | Pair range |
|---|---:|---:|---:|
| Memory | 0 B | 0.2% | -10.4–43.9% |
| Memory | 1 B | 0.7% | -6.6–43.3% |
| Memory | 65535 B | 48.9% | 43.2–52.9% |
| Memory | 65536 B | 45.4% | 40.4–51.3% |
| Memory | 65537 B | 42.4% | 39.7–45.3% |
| Memory | 4 MiB | 94.2% | 92.4–94.4% |
| Local | 0 B | 0.8% | -9.0–11.8% |
| Local | 1 B | 0.4% | -22.3–4.7% |
| Local | 65535 B | 4.6% | 0.9–17.2% |
| Local | 65536 B | 3.0% | -6.6–13.9% |
| Local | 65537 B | -2.6% | -183.2–20.6% |
| Local | 4 MiB | 43.8% | 41.3–47.7% |

Decision: retain. Large-file registration improves in every pair on both
backends, with no case's median regression exceeding 5%. Small local-file
results are near neutral and noisy; no speedup is claimed for them. Tests prove
zero payload reads and zero additional writes for alias registration and
publication, both Git hash formats, empty files, invalid selectors, collection
retention, and custom-verifier publication rules.

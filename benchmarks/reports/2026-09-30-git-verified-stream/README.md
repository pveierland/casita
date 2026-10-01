# Historical one-pass verified ingestion

This section and its original JSON were recorded on 2026-09-30. The JSON
is preserved byte-for-byte with its original executable fingerprint and
environment. These timings describe that historical binary; they are not
measurements of the extracted branch or its updated dependencies. The
runnable permanent corpus now uses the shared affinity helper.

`verified-stream.json` retains 168 audited samples: seven alternating pairs for
six sizes on memory and local backends. Both strategies use the same executable.
The baseline streams bytes into storage and then reopens them for verification;
the candidate lets the selected verifier drive source reads and forwards each
read to the writer. Timing covers staging only, with fixture creation, mutation
setup, rooted publication and exhaustive audits outside the timer.

| Backend | Size | Median paired time reduction | Pair range |
|---|---:|---:|---:|
| Memory | 0 B | 25.6% | 18.2–82.2% |
| Memory | 1 B | 22.1% | 20.6–39.5% |
| Memory | 65535 B | 12.0% | -5.1–15.1% |
| Memory | 65536 B | 15.2% | 4.2–31.6% |
| Memory | 65537 B | 3.1% | -16.6–9.4% |
| Memory | 4 MiB | 0.4% | -14.0–4.7% |
| Local | 0 B | 17.8% | -305.2–65.4% |
| Local | 1 B | 44.5% | 37.2–54.9% |
| Local | 65535 B | 30.5% | -107.7–35.4% |
| Local | 65536 B | 30.2% | 22.9–64.2% |
| Local | 65537 B | 35.7% | 21.5–50.8% |
| Local | 4 MiB | 6.2% | -7.8–12.6% |

Decision: retain. Local 64 KiB and 64 KiB+1 staging improves in every pair, with
all workload medians nonnegative. Large-body results do not establish a
substantial gain. These measurements compare streaming ingestion APIs.
They are not end-to-end Git import speedups or memory
measurements. Whole-process RSS includes the in-memory fixture and audit buffer.

Six correctness tests cover both Git hash formats, threshold sizes, exact native
and physical identities, length mismatch, metadata links, source limits, false
EOF from an empty verifier read, backend write/close failures and false backend
length/digest results. A counting backend confirms no verification rereads.

```sh
cargo test --release -p casita --no-default-features --features native,git,experimental --test verified_stream --no-run
python3 -m benchmarks.suites.git_verified_stream --backend both --repetitions 7 --cpu-affinity 0,1,2,3 --probe-binary /path/to/verified-stream-probe --no-build --output /tmp/verified-stream.json
```

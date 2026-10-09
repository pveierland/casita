# Server ingestion: pressure collection attribution

The same fresh single-file Git update took 4.681 s with a recent collection
timestamp and 66.248 s with an expired timestamp. The latter stamp advanced
61.304 s into the import. Both cases retained the same seven historical roots,
verified all eight imported revision/NAR identities, completed 28 fresh
read/hash pairs (seven overlapping the target), and left no owned processes.
This controlled pair implicates pressure-triggered collection in the alternating
43–62 s updates observed during the preceding 80-minute server workload.

This is attribution, not an adopted optimization. Internal collection phase
costs remain to be profiled, and one pair does not establish a general timing
distribution. Production behavior is unchanged. The existing upstream Turso
WAL work remains [PR 9485](https://github.com/tursodatabase/turso/pull/9485).

| Target observation | Recent stamp | Expired stamp |
| --- | ---: | ---: |
| Wall time | 4.681 s | 66.248 s |
| Sampled server CPU | 13.91 s | 85.66 s |
| `rchar` delta | 427,413,931 B | 23,009,145,990 B |
| `write_bytes` delta | 2,727,936 B | 629,276,672 B |
| Collection stamp | unchanged | advanced during target |

The enclosing resource samples add at most 4.2 ms after the target and 0.3 ms
before it. Counters are process-wide and may include reaped children; `rchar`
measures syscall bytes, not physical reads. Both cases reported zero
`read_bytes`. The standard seed indices were 0/15/16/31/32/47/48; target 49 was
new. Preparation explicitly suppressed automatic pressure collection using an
owned advisory stamp. The measured recent/expired controls ran on independent
fresh repositories with identical qualified inputs, under the existing 80%
filesystem pressure policy. No other owned workload overlapped measurement.

The permanent `server-ingest-sustained` and `server-ingest-pressure` cases are
registered in `benchmark all`; configuration, guards, qualification requirements
and commands are documented in [the benchmark README](../../README.md).
The migrated sustained smoke passed four imports plus four reopens, one
witnessed cancellation/recovery, 22 read/hash pairs and empty cleanup. The
original full observation remains separate evidence; it was not rerun merely
to package the driver. Missing external configuration records explicit skips
and leaves the all-suite completion ledger incomplete.

Validation: 13 targeted/storage tests passed, including changed binary/read
fixtures and actual stopped-driver/separate-session-child timeout and SIGTERM
regressions. Initial failing cleanup tests exposed two outer-runner defects;
both were fixed and re-reviewed across all six PM dimensions. The benchmark
now adopts and cleans detached descendants, including when its driver stops
responding. The cleanup fixes postdate the measured pair; archived driver
sources preserve the exact version used for that evidence.

`evidence.tar.gz` contains the measurement protocols, logs, wire/resource/helper
streams, terminal results, original driver snapshots, analyzer and analysis,
configuration, missing-input ledger and a SHA-256 inventory. It excludes the
large CAS payloads, external executables, native libraries and corpus. Their
paths and hashes remain in the protocols. Those originals remain under
`/tmp/mnos-ingest/evidence/{sp1,server-ingest-port-smoke-v1-artifacts}` and the
qualified corpus/build directories recorded there. Full F3/F5/F7 closure and
the original old-Nix memory failure remain outside this benchmark-port commit.

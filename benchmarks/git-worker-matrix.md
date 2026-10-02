# Git decoder matrix

`git-worker-matrix` is a permanent Linux benchmark entrypoint, registered in
`benchmark all`. The additional `git-worker-matrix-mixed` and
`git-worker-matrix-delta` entries retain mixed-size and actual packed-delta
coverage at every worker count. They share the same integration executable.
It measures 1/2/4/8 requested source decoders independently of
staging concurrency (16 by default). The default importer still uses one decoder.

```sh
benchmark run git-worker-matrix --profile smoke --output /tmp/workers-smoke.json
benchmark all --suites git-worker-matrix --profile smoke --output /tmp/workers-all
python3 -m benchmarks.suites.git_worker_matrix --counts 16 --file-bytes 4194304 \
  --max-buffered-bytes 67108864 --decode-workers 1,2,4,8 --repetitions 5 \
  --probe-binary CANDIDATE --baseline-binary BASELINE --no-build \
  --cpu-affinity 0,1,2,3 --output /tmp/workers-paired.json
```

The standalone `git_worker_matrix` integration executable shares the bounded
fixture helper with `git_closure_import`. Generation and independent BLAKE3
readback use 64 KiB buffers. Git hashing and repacking run in child processes.
Cold means an empty Casita destination, not cold filesystem caches: fixture
generation has just written the Git source objects.
The four timed operations are cold import, repeated root, changed root retaining
a complete subtree, and a wide tree retaining individual blobs. Every operation
checks exact import/reuse counts, complete closure, native identity, byte length,
and independent payload digest. Packed clustered fixtures must actually contain
blob deltas. Ordinary worker fixtures default to pack window 0 to avoid expensive
delta search during setup; clustered fixtures default to window 16. Explicit
`--pack-window` overrides are retained in each report. Roots and build fingerprints must match in paired comparisons.

The smoke profile covers the two-64-KiB-body admission boundary at byte budgets
131071/131072/131073. Standard adds 15/16/17 objects, 1 KiB/64 KiB/1 MiB/4 MiB
bodies and a 64 MiB admission budget. Counts straddle the 16-object staging batch;
byte budgets straddle two-body admission. Reports retain the observed worker
peak: the requested worker count is a cap, not guaranteed concurrency.

Memory is the **parent process VmHWM**, sampled before import and immediately
after import, before payload audits. This excludes fixture subprocess peaks.
It is still an approximate process high-water mark, not a resettable
per-operation peak. Linux documents this counter as
[inaccurate](https://www.man7.org/linux/man-pages/man5/proc_pid_status.5.html);
the smoke run observed a small decrease between reads. Both raw values are
retained without clamping, and summaries count decreasing observations.
Small differences must not be interpreted as precise allocation savings.
Cold measurements are most useful; subsequent phases inherit earlier peaks and
retain earlier results. Destination storage, caches, allocator retention, and
delta reconstruction are included. `peak_source_bytes` is only the admitted body
budget; it is not RSS and excludes caches and reconstruction workspace.

The timer brackets the import call. Fixture generation, payload audits, the
final explicit repository flush and lease teardown are outside it; complete
process output and process resource usage remain in the raw reports.

CPU is the process user/system tick delta bracketing import, including its
threads and excluding children. The runner records `SC_CLK_TCK`; short imports
may quantize to zero. Missing Linux observations fail the suite. Pre-worker
baselines explicitly report null source/worker counters through a small API
adapter; missing memory or CPU observations never become invented zeroes.

A paired result reports the median of per-pair percentage changes as well as
independent medians and pair ranges. Those are different statistics. Five pairs
are a minimum diagnostic sample, not a confidence interval. The host activity
sampler records competing processes; shared-host results are not isolated-host
claims. See the [isolated B2 measurement procedure](reports/2026-10-02-git-worker-matrix/README.md).

# Retained Git body buffers

`git-retained-buffers` measures decoded-body retention using buffered delta
reconstruction. It uses bounded fixture construction and independent streamed
readback so the parent process never constructs a whole-file fixture buffer.
The smoke profile covers 1 MiB − 1, at, and + 1 byte with one/four source workers;
the standard profile adds tiny/small/large files and 16/64-file sources.

```sh
python3 -m benchmarks.cli run git-retained-buffers --profile smoke --output result.json
python3 -m benchmarks.cli all --profile smoke --suites git-retained-buffers \
  --output all-results
python3 -m benchmarks.suites.git_retained_buffers --no-build \
  --probe-binary "$CANDIDATE" --baseline-binary "$BASELINE" \
  --counts 16 --file-bytes 4194304 --repetitions 5 --output paired.json
```

Paired binaries must use identical fixture hashes, compiler, features, flags,
and dependency lockfiles. Timing excludes fixture construction and correctness
audits. Parent HWM is observed before readback, includes the whole import process,
and remains affected by allocator/cache/destination memory; only cold HWM isolates
it from earlier operations. Exact payload hashes,
streamed readback, imported/reused counts, and exhaustive closure checks gate
every sample. Raw samples preserve failed runs and negative results.

Boxing completed bodies discards excess vector capacity at the allocation-layout
level. It may copy/reallocate; temporary overlap and allocator-held pages remain
possible. It does not remove or bound gix's workspace during delta reconstruction.

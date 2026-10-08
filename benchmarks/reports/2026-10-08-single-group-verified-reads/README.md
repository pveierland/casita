# Single-group verified reads: retain coverage, reject optimization

The permanent `verified_manifest_reads` target now includes 20 bare-blob cases
at 1/16383/16384/16385/32768 bytes, memory-loose/local-packed storage and reader
widths 1/64. The existing target is registered in the manifest and `benchmark all`.
Each timed iteration reads 64 distinct prepared blobs through authenticated EOF;
fixture construction, flush and output comparisons are outside timing. Backend
and packing vary together. The original 24 empty/flat/paged cases remain.

The investigated optimization bypassed redundant proof encoding/decoding for
already-resolved nonempty bare blobs no larger than one 16 KiB Bao group. Many
eligible microbenchmark cases improved, but larger packed controls regressed
(16385 bytes, width64: +46.52% median of run-level medians). These adverse
controls were retained, not attributed to noise or used to select another run.

Six qualified native near-update runs in fixed baseline/candidate,
candidate/baseline, baseline/candidate order failed three required gates:
median NAR wall changed +0.0963%, the third fixed pair was slower, and median
NAR-interval process CPU increased 7.2506%. Full wall changed +0.4724% and peak
RSS fell 9.3193%; those passing gates cannot compensate for the failed ones.
The two experimental production files were restored to accepted
`892fccde0b21854239ef28fa62c94df63889a029`. This commit retains only tests,
benchmarks and documentation. No production optimization is retained.

Boundary tests verify proof shape/decoding, incorrect sizes, authentication
failure before output and sticky errors. A paused-storage RAII counter proves
that dropping the owning pending read releases its operation immediately,
before the storage fault is disarmed, on both sides of the 16 KiB boundary.
The restored source passed 108 chunked tests plus one existing ignore, the ten
tests selected by `verified::`, and all 44 benchmark test-mode cases. Two new
tests appear in both unit filters, so those counts are not additive. The rebuilt
benchmark executable is byte-identical to the original baseline. Test mode
does not measure throughput.

```sh
cargo bench -p casita --features native,experimental --bench verified_manifest_reads -- --test
cargo bench -p casita --features native,experimental --bench verified_manifest_reads -- verified_bare_group_reads
```

Full raw evidence and the rejected patch are in repository **mnos-nix-xp**,
commit `4eb9c74fed22848b2c59c0d97c1248b1559722cc`, under
`docs/reports/evidence/git-ingestion/2026-10-08-single-group-verified-reads`.
The verified package contains 2015 compressed artifacts, 454 external inputs
and six historical executable mappings. Independent reviews and all original
failed preflights remain. The exact source/build commands and retained fixture
identities are recorded there; replay scripts require fresh output destinations.

The correctness/screen build used release debug1 with LTO off; native results
used the matched production thin-LTO/mimalloc build. Both variants have identical
benchmark source. The tested restored snapshot precedes this report and README
link; their addition changes no benchmark, test or production implementation.

These are warmed local retained-store near updates, not cold/new/week/release
qualification or an explanation of the historical OOM. CPU within the NAR
interval includes concurrent evaluator work. All NAR spans complete; three
traces also retain two explicitly censored background spans. The original seed
is unchanged and all full revision/NAR outputs match. Existing Turso WAL PR #9485
and the broader ingestion/memory investigation remain separate.

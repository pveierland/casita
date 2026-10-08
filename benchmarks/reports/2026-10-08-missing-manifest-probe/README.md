# Verified reads with one missing-manifest probe

When a bounded descriptor read returns NotFound, proof_reader now checks for the
bare chunk directly instead of issuing another HEAD for the same manifest. The
internal Missing/Flat/Paged distinction preserves existing Option wrappers,
non-NotFound errors, flat header/length/count validation and flat HEAD-NotFound
fallback. Bao authentication and successful-EOF requirements are unchanged.
Absence is observed per open, never cached; a fresh read after publication must
find newly published metadata.

The qualified near-update workload uses three baseline and three candidate runs
from identical retained near-A stores, in fixed order B,C,C,B,B,C. Median NAR
wall improves from 5.878 s to 4.696 s (20.11%), and full-command wall from 10.069 s
to 8.858 s (12.03%). Every fixed pair improves. NAR-interval process CPU falls
from 34.117 s to 25.484 s (25.30%). These are warmed copied-store observations,
not cold-storage or universal repository claims. Full-command time includes
copied-store recovery/setup, and interval CPU includes other process work.

The predeclared gates require at least 5% better median NAR wall, improvement in
every pair, no median NAR CPU regression, full-wall regression no more than 3%,
and median peak RSS increase no greater than max(5%,16 MiB). All pass. First-pair
peak RSS rises 7.82%, although the group median falls 1.97%; no general memory
improvement or OOM closure is claimed. Five traces report two unfinished spans,
one reports zero; all required NAR intervals complete, and no records are dropped.
Every full revision/NAR result agrees. See `results.json` for every observation,
range, fixed pair, exact gate and implementation file hash.

## Evidence and reproducibility

The exact raw evidence, both failed and successful attempts, source/build bindings,
16 runner fault cases, independent reviews and verified package are committed in
repository **mnos-nix-xp**, commit
`51c8c1d6df38b4cdde090d637a6c344603bb1205`, at
`docs/reports/evidence/git-ingestion/2026-10-08-missing-manifest-probe`.
The evidence package has 338 compressed artifacts, 470 decisive external inputs
(including all 441 seed files), and verified retention for historical executables.
Its byte verifier and independent semantic review serve different purposes.

The tested baseline is Casita `a37c5465dd92570e38075298461f6bc7697454ac`.
The recorded candidate snapshot includes the implementation, tests and permanent
benchmark before this documentation was added. The qualified production evaluator
uses identical evaluator source, lockfile, release/thin-LTO profile, native bindings
and mimalloc configuration on both sides. Test executables use a separately
recorded no-LTO profile and are not used as production performance proxies.
Existing Turso WAL work, including upstream PR #9485, remains separate.

## Permanent benchmark and correctness

`verified_manifest_reads` is registered in Cargo, `benchmarks/manifest.json` and
`benchmark all`'s core corpus. Its 24 cases pair memory-loose/local-packed storage
with concurrency 1/64 and explicit chunk counts 0/1/2/63/64/65. Shape assertions
require an elided single-chunk manifest, a real empty flat manifest, and both
sides of the 64-entry flat/page boundary. Local fixtures use local_packed and
flush before timing. Each warmed iteration reads 64 prepared blobs, with one
repeated identity in the empty case. Backend and packing vary together, so the
matrix does not independently attribute their costs.

Fixture hash computation and full-output comparisons are outside timing;
authentication hashing, reads and output allocations are inside. All output is
consumed through successful verified EOF. The recorded 24-case test-mode run is
a correctness gate, not a Criterion baseline/candidate throughput comparison.
Any future revision comparison must use this same corrected harness on both sides.

```sh
cargo bench -p casita --features native,experimental --bench verified_manifest_reads -- --test
cargo bench -p casita --features native,experimental --bench verified_manifest_reads
```

The original implementation fails the two missing-manifest request tests with
one extra HEAD. The final candidate passes 106 chunked-store tests, eight
verification tests, all 24 benchmark cases, and 21 benchmark-runner tests. The
existing small-blob-pins benchmark remains ignored as a unit test. Coverage
includes outboards, wrong sizes, storage errors, malformed metadata, legacy flat
removal, deterministic publication overlap and fresh-read discovery. The initial
benchmark constructor error and its failed executable/logs remain retained.

This change resolves one measured verified-read cost. The wider ingestion work,
finer Casita attribution and historical 10 GiB memory investigation remain open.

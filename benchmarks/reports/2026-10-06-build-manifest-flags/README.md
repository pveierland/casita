# Distinguish absent and empty Rust flag overrides

Build manifests previously serialized both an unset `RUSTFLAGS` and an explicitly
empty override as the same empty string. They also discarded whether a value
came from `RUSTFLAGS` or `CARGO_ENCODED_RUSTFLAGS`. Identical manifests could
therefore qualify builds that used different compiler flags. This surfaced while
preparing the Mnos rotation benchmark: an empty override bypassed its configured
linker flags. That build was interrupted before measurement; the accepted runs
used the intended unset environment and repository configuration.

Schema 2 records `rustflags_source` alongside the value. The source follows
[Cargo's documented precedence](https://doc.rust-lang.org/cargo/reference/config.html#buildrustflags),
including present-but-empty overrides. Configuration files and their environment
overrides remain fingerprinted separately. Legacy schema 1 remains readable for
individual artifacts, but paired comparisons require rebuilding rather than
inventing missing historical provenance. Existing archived sidecars are unchanged.

Strict paired suites compare the source as well as all existing matched fields.
Revision comparisons independently check schema eligibility and matching flag
source/value before timing. They still allow other changes across revisions,
such as dependency or checked-in configuration changes, and record them for the
reader to interpret. These sidecars are local provenance records, not signed
attestations or proof of every possible compiler input.

Regression tests first demonstrated that the writer accepted unset/empty and
plain/encoded mismatches and that legacy pairs were accepted. Review then found
the revision runner bypassed the paired validator; three direct-consumer tests
failed before sharing the flag check with that path. The complete Python suite
initially exposed an outdated expected suite list and four sandbox socket
permission errors. The list was corrected for `collection-mark` and
`mutation-rotation`, and the complete suite was run with loopback access.
Final result: **506 tests run, 2 skipped, no failures or errors**. The suite's
mocked failure scenarios intentionally print diagnostic errors; the final
unittest result reports the actual outcome. No Rust runtime behavior changed.

A tiny dependency-free Cargo fixture also confirmed the root cause: setting
`[build].rustflags = ["--cfg", "configured_flag", "--check-cfg", "cfg(configured_flag)"]`
and printing `cfg!(configured_flag)` yielded `true` with both environment
variables unset, and `false` with either variable explicitly empty. The two
empty cases warned about the cfg name because they also removed its configured
`--check-cfg` declaration. Raw results are retained.

Reproduce the permanent harness checks from the repository root:

```sh
python3 -m unittest discover -s benchmarks/tests
```

`artifacts.json` includes SHA-256 hashes for the compressed and uncompressed
red/green logs, Cargo characterization output and exact implementation patch.
This is a benchmark-correctness fix; it makes no ingestion performance claim.

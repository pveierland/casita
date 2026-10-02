#!/usr/bin/env bash
set -euo pipefail
# Run from the Casita checkout with the compiler recorded in build-environment.sh.
# Baseline and candidate builds must use distinct source and target directories.
cargo test --offline --locked --release -j6 -p casita --no-default-features \
  --features native,git,experimental --test git_closure_import --message-format=json
cargo test --offline --locked --release -j6 -p casita --features git,experimental \
  --lib git::repository::closure_import::workers::streaming:: --message-format=json
# For the predecessor, append `-- --skip delta_spill::` to the integration
# test command: it intentionally lacks the three new spilling behaviors.
# Freeze each integration executable immediately after its own fresh build:
# python3 benchmarks/reports/2026-10-01-git-delta-spill/freeze-probe.py CHECKOUT CARGO_JSON_LOG NEW_BINARY
# For the default-feature unit executable use freeze-unit.py instead.
# Set BASELINE and CANDIDATE to the frozen integration executables.
PYTHONPATH=. python3 benchmarks/reports/2026-10-01-git-delta-spill/matrix.py \
  --baseline "$BASELINE" --candidate "$CANDIDATE" --output-dir "$NEW_RESULT_DIRECTORY"
# The original and supplemental exact commands are retained in their JSON records.
python3 -m unittest benchmarks.tests.test_git_delta_spill \
  benchmarks.tests.test_git_source_inflation benchmarks.tests.test_git_source_locator \
  benchmarks.tests.test_revisions
python3 -m benchmarks.cli all --profile smoke --suites git-delta-spill,git-delta-disabled,git-delta-limits \
  --bin-dir "$FROZEN_BINARY_DIRECTORY" --output "$NEW_SMOKE_DIRECTORY"
cargo clippy --offline --locked --release -j6 -p casita --no-default-features \
  --features native,git,experimental --test git_closure_import -- -D warnings

# Final CPU-aware matrix (all three integration fixtures must match):
# python3 benchmarks/reports/2026-10-01-git-delta-spill/cpu-check/matrix.py \
#   --baseline "$BASELINE" --admission "$ADMISSION" --candidate "$CANDIDATE" \
#   --output-dir "$NEW_CPU_RESULT_DIRECTORY"

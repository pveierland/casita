#!/usr/bin/env bash
# Run from the repository root with the recorded compiler/features/flags.
# BASELINE and CANDIDATE identify frozen integration probes; UNIT is the library probe.
set -euo pipefail
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
report=benchmarks/reports/2026-10-01-git-source-inflation
python3 "$report/profile.py" --probe-binary "$CANDIDATE" --baseline-binary "$BASELINE" \
  --no-build --backend local --counts 1 --file-bytes 16777216 \
  --max-buffered-bytes 16777216 --decode-workers 1 --layout both \
  --repetitions 5 --cpu-affinity 0,1,2,3 --output /tmp/replayed-initial-16m.json
python3 "$report/matrix.py" --baseline "$BASELINE" --candidate "$CANDIDATE" \
  --output-dir /tmp/replayed-inflation-matrix
python3 -m benchmarks.suites.git_source_locator --probe-binary "$UNIT" --no-build \
  --repetitions 5 --output /tmp/replayed-locator-limits.json
# Verification commands used after timing (matching Clippy installed separately).
cargo clippy --offline --locked --release -j6 -p casita --no-default-features \
  --features native,git,experimental --test git_closure_import -- -D warnings
cargo test --offline --locked --release -j6 -p casita --no-default-features \
  --features native,git,experimental --test git_closure_import
"$UNIT" git::repository::closure_import::workers:: --nocapture
python3 -m unittest benchmarks.tests.test_git_source_inflation \
  benchmarks.tests.test_git_source_locator benchmarks.tests.test_git_closure_import \
  benchmarks.tests.test_all benchmarks.tests.test_revisions
# BIN_DIR contains git_closure_import and casita-lib-test, with .build.json sidecars.
python3 -m benchmarks.all --suites git-source-inflation,git-source-locator \
  --profile smoke --repetitions 1 --bin-dir "$BIN_DIR" --output /tmp/replayed-inflation-all

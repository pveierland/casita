#!/usr/bin/env bash
set -euo pipefail
source /tmp/git-import-perf-env.sh
cd /home/pveierland/dev/git-import-perf/casita
python3 -m benchmarks.suites.git_closure_import \
  --probe-binary /tmp/git-import-perf-binaries/casita-worker-streaming-candidate \
  --baseline-binary /tmp/git-import-perf-binaries/casita-worker-streaming-baseline \
  --no-build --backend local --layout packed --concurrency 16 \
  --repetitions 5 --cpu-affinity 0,1,2,3 \
  --baseline-decode-workers 1 --decode-workers 1 --counts 16 \
  --file-bytes 4194304 --content random --max-buffered-bytes 67108864 \
  --output /tmp/casita-worker-streaming-serial-repeat.json

#!/usr/bin/env bash
set -euo pipefail
source /tmp/git-import-perf-env.sh
cd /home/pveierland/dev/git-import-perf/casita
streaming_baseline=/tmp/git-import-perf-binaries/casita-worker-streaming-baseline
streaming_candidate=/tmp/git-import-perf-binaries/casita-worker-streaming-candidate
shared=(--probe-binary "$streaming_candidate" --baseline-binary "$streaming_baseline" --no-build --backend both --layout both --concurrency 16 --repetitions 5 --cpu-affinity 0,1,2,3)
python3 -m benchmarks.suites.git_closure_import "${shared[@]}" --baseline-decode-workers 4 --decode-workers 4 --counts 16 --file-bytes 4194304 --content random --max-buffered-bytes 67108864 --output /tmp/casita-worker-streaming-large.json
python3 -m benchmarks.suites.git_closure_import "${shared[@]}" --baseline-decode-workers 4 --decode-workers 4 --counts 64 --file-bytes 1024,65536 --content random --max-buffered-bytes 67108864 --output /tmp/casita-worker-streaming-small.json
python3 -m benchmarks.suites.git_worker_streaming "${shared[@]}" --baseline-decode-workers 4 --decode-workers 4 --counts 64 --file-bytes 4194304 --max-buffered-bytes 67108864 --output /tmp/casita-worker-streaming-mixed.json
python3 -m benchmarks.suites.git_object_workers "${shared[@]}" --baseline-decode-workers 4 --decode-workers 4 --counts 17 --file-bytes 65536 --max-buffered-bytes 131071,131072,131073 --output /tmp/casita-worker-streaming-boundaries.json
python3 -m benchmarks.suites.git_closure_import "${shared[@]}" --baseline-decode-workers 1 --decode-workers 1 --counts 16 --file-bytes 1024,4194304 --content random --max-buffered-bytes 67108864 --output /tmp/casita-worker-streaming-serial.json

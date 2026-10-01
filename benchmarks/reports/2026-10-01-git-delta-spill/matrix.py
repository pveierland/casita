"""Run matched, alternating delta-spill comparisons with host activity records."""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from benchmarks.host_activity import QuietHost
from benchmarks.suites.repository import BenchmarkError

CASES = {
    'tiny-deltas': ['--counts', '16', '--file-bytes', '1024'],
    'disabled-control': ['--counts', '16', '--file-bytes', '1048576', '--no-delta-spilling', '--delta-metrics'],
    'small-deltas': ['--counts', '16', '--file-bytes', '65536'],
    'delta-1m': ['--counts', '16', '--file-bytes', '1048576'],
    'delta-16m': ['--counts', '16', '--file-bytes', '16777216'],
    'dense-deltas': ['--counts', '64', '--file-bytes', '1048576'],
    'compressible-deltas': ['--counts', '16', '--file-bytes', '16777216', '--content', 'repeated'],
    'ordinary-controls': ['--counts', '4', '--file-bytes', '16777216', '--content', 'random', '--pack-window', '0', '--layout', 'both'],
    'byte-budget': ['--counts', '16', '--file-bytes', '1048576', '--max-buffered-bytes', '1048575,1048576,1048577'],
}
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--candidate', type=Path, required=True)
parser.add_argument('--output-dir', type=Path, required=True)
parser.add_argument('--cases', nargs='+', choices=CASES, default=list(CASES))
parser.add_argument('--repetitions', type=int, default=5)
args = parser.parse_args()
root = Path(__file__).resolve().parents[3]
out = args.output_dir.resolve()
out.mkdir(parents=True, exist_ok=True)
if args.repetitions < 1:
    parser.error('repetitions must be positive')
for name in args.cases:
    if (out/(name+'.json')).exists():
        parser.error('refusing to overwrite an existing measurement: '+name)
print('Waiting up to five minutes for a quiet interval.', flush=True)
try:
    with QuietHost(timeout=300, quiet_seconds=5, max_cpu_fraction=0.10) as quiet:
        pass
    host = quiet.report()
except BenchmarkError as error:
    print(str(error)+'; CPU timings will remain exploratory.', flush=True)
    host = dict(error=str(error))
(out/'matrix-start-host.json').write_text(json.dumps(host, indent=2)+'\n')
base = [sys.executable, str(Path(__file__).with_name('profile.py')),
    '--probe-binary', str(args.candidate.resolve()), '--baseline-binary', str(args.baseline.resolve()),
    '--no-build', '--backend', 'local', '--repetitions', str(args.repetitions),
    '--cpu-affinity', '0,1,2,3', '--decode-workers', '1,4', '--match-baseline-decode-workers']
commands = [dict(name=name, command=base+CASES[name]+['--output',str(out/(name+'.json'))]) for name in args.cases]
(out/'matrix-commands.json').write_text(json.dumps(commands, indent=2)+'\n')
for case in commands:
    print('Running '+case['name'], flush=True)
    subprocess.run(case['command'], cwd=root, env={**os.environ, 'PYTHONPATH':str(root)}, check=True)
    print('Completed '+case['name'], flush=True)

"""Replay the CPU-aware comparisons into a new output directory."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--admission', type=Path, required=True)
parser.add_argument('--candidate', type=Path, required=True)
parser.add_argument('--output-dir', type=Path, required=True)
args = parser.parse_args()
report = Path(__file__).resolve().parents[1]
root = report.parents[2]
out = args.output_dir.resolve()
out.mkdir(parents=True, exist_ok=True)
assert not (out / 'matrix-commands.json').exists(), 'use a new output directory'
base = [sys.executable, str(report / 'profile.py'), '--probe-binary', str(args.candidate.resolve()),
        '--no-build', '--backend', 'local', '--cpu-affinity', '0,1,2,3',
        '--decode-workers', '1,4', '--match-baseline-decode-workers', '--counts', '16', '--cpu-metrics']
cases = [
    ('isolated-disabled-4m', args.admission, ['--file-bytes', '4194304', '--no-delta-spilling', '--delta-metrics', '--repetitions', '5']),
    ('final-disabled', args.baseline, ['--file-bytes', '1048576,4194304', '--no-delta-spilling', '--delta-metrics', '--repetitions', '10']),
    ('final-clustered-4m', args.baseline, ['--file-bytes', '4194304', '--repetitions', '5']),
]
commands = [dict(name=name, command=base + ['--baseline-binary', str(baseline.resolve())] + options
                + ['--output', str(out / (name + '.json'))]) for name, baseline, options in cases]
(out / 'matrix-commands.json').write_text(json.dumps(commands, indent=2) + '\n')
for case in commands:
    print('Running ' + case['name'], flush=True)
    subprocess.run(case['command'], cwd=root, env={**os.environ, 'PYTHONPATH': str(root)}, check=True)

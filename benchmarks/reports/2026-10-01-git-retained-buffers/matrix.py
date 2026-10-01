"""Replay the CPU-aware comparisons into a new output directory."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--candidate', type=Path, required=True)
parser.add_argument('--output-dir', type=Path, required=True)
parser.add_argument('--repeat-controls', action='store_true', help='replay the two targeted follow-up comparisons')
args = parser.parse_args()
report = Path(__file__).resolve().parent
root = report.parents[2]
out = args.output_dir.resolve()
out.mkdir(parents=True, exist_ok=True)
ledger = 'supplemental-commands.json' if args.repeat_controls else 'matrix-commands.json'
assert not (out / ledger).exists(), 'use a new output directory or unused ledger'
base = [sys.executable, str(report / 'profile.py'), '--probe-binary', str(args.candidate.resolve()),
        '--no-build', '--backend', 'local', '--cpu-affinity', '0,1,2,3',
        '--decode-workers', '1,4', '--match-baseline-decode-workers', '--counts', '16', '--cpu-metrics']
cases = [
    ('tiny', args.baseline, ['--file-bytes', '1024', '--repetitions', '5']),
    ('stream-threshold', args.baseline, ['--file-bytes', '1048575,1048576,1048577', '--repetitions', '5']),
    ('clustered-4m', args.baseline, ['--file-bytes', '4194304', '--repetitions', '5']),
    ('dense-1m', args.baseline, ['--counts', '64', '--file-bytes', '1048576', '--repetitions', '5']),
    ('ordinary-controls', args.baseline, ['--counts', '4', '--file-bytes', '1048576', '--content', 'random', '--layout', 'both', '--pack-window', '0', '--repetitions', '5']),
    ('incremental-spilling-4m', args.candidate, ['--spill-comparison', '--file-bytes', '4194304', '--repetitions', '5']),
]
if args.repeat_controls:
    cases = [
        ('ordinary-packed-repeat', args.baseline, ['--counts', '4', '--file-bytes', '1048576', '--content', 'random', '--layout', 'packed', '--pack-window', '0', '--repetitions', '5']),
        ('dense-four-repeat', args.baseline, ['--counts', '64', '--file-bytes', '1048576', '--decode-workers', '4', '--repetitions', '5']),
    ]
commands = [dict(name=name, command=base + ['--baseline-binary', str(baseline.resolve())] + options
                + ['--output', str(out / (name + '.json'))]) for name, baseline, options in cases]
(out / ledger).write_text(json.dumps(commands, indent=2) + '\n')
for case in commands:
    print('Running ' + case['name'], flush=True)
    subprocess.run(case['command'], cwd=root, env={**os.environ, 'PYTHONPATH': str(root)}, check=True)

"""Replay isolated B2 comparisons. Every case uses the registered worker suite."""
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
parser.add_argument('--cpu-affinity', default='0,1,2,3')
parser.add_argument('--cases', help='comma-separated subset of named cases')
parser.add_argument('--repeat-controls', action='store_true', help='repeat the tiny memory controls with ten pairs')
args = parser.parse_args()
report = Path(__file__).resolve().parent
root = report.parents[2]
out = args.output_dir.resolve()
out.mkdir(parents=True, exist_ok=True)
ledger = out / 'matrix-commands.json'
if ledger.exists():
    parser.error('use a new output directory')
base = [sys.executable, str(report / 'profile.py'), '--probe-binary', str(args.candidate.resolve()),
        '--no-build', '--cpu-affinity', args.cpu_affinity, '--counts', '16',
        '--file-bytes', '4194304', '--max-buffered-bytes', '67108864',
        '--backend', 'both', '--layout', 'both', '--decode-workers', '1,2,4,8',
        '--repetitions', '5', '--pack-window', '0']
cases = [
    ('tiny', args.baseline, ['--counts', '32', '--file-bytes', '1024', '--pack-window', '16']),
    ('medium', args.baseline, ['--counts', '8', '--file-bytes', '1048576', '--pack-window', '16']),
    ('large', args.baseline, []),
    ('mixed', args.baseline, ['--counts', '32', '--file-bytes', '524288', '--content', 'mixed']),
    ('delta', args.baseline, ['--layout', 'packed', '--content', 'clustered', '--pack-window', '16']),
    ('same-binary-large', args.candidate, ['--baseline-decode-workers', '1', '--decode-workers', '2,4,8']),
    ('boundaries', None, ['--counts', '15,16,17', '--file-bytes', '65536',
                        '--max-buffered-bytes', '131071,131072,131073', '--repetitions', '1']),
    ('local-four-repeat', args.candidate, ['--backend', 'local', '--layout', 'loose',
         '--baseline-decode-workers', '1', '--decode-workers', '4', '--repetitions', '10']),
]
if args.repeat_controls:
    tiny = ['--counts', '32', '--file-bytes', '1024', '--backend', 'memory', '--repetitions', '10', '--pack-window', '16']
    cases = [('tiny-repeat', args.baseline, tiny),
             ('same-binary-tiny', args.candidate, tiny + ['--baseline-decode-workers', '1', '--decode-workers', '2,4,8'])]
if args.cases:
    names = args.cases.split(',')
    if len(set(names)) != len(names) or set(names) - {case[0] for case in cases}:
        parser.error('cases must be unique known names')
    cases = [case for case in cases if case[0] in names]
commands = [dict(name=name, command=base + (['--baseline-binary', str(before.resolve())] if before else [])
                + options + ['--output', str(out / (name + '.json'))])
            for name, before, options in cases]
ledger.write_text(json.dumps(commands, indent=2) + '\n')
for case in commands:
    print('Running ' + case['name'], flush=True)
    subprocess.run(case['command'], cwd=root, env={**os.environ, 'PYTHONPATH': str(root)}, check=True)

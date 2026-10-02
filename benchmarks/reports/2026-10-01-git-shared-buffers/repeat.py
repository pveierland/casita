"""Repeat disabled overhead and cover the two-window/two-writer admission boundary."""
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
parser.add_argument('--cpus', default='0,1,2,3')
args = parser.parse_args()
report = Path(__file__).resolve().parent
root = report.parents[2]
out = args.output_dir.resolve()
out.mkdir(exist_ok=True, parents=True)
ledger = out / 'supplemental-commands.json'
assert not ledger.exists(), 'use a new output directory'
base = [sys.executable, str(report/'profile.py'), '--probe-binary', str(args.candidate.resolve()),
    '--baseline-binary', str(args.baseline.resolve()), '--no-build', '--backend', 'local',
    '--cpu-affinity', args.cpus, '--counts', '16', '--content', 'random', '--decode-workers', '4',
    '--imports', '4', '--shared-cpu-limit', '0', '--file-bytes', '1048577', '--repetitions', '5']
cases = [('disabled-large-repeat', '0:0'),
         ('concurrency-boundary', '140509183:119537663,140509184:119537664,140509185:119537665')]
commands = [dict(name=name, command=base+['--buffer-budget', budgets, '--output', str(out/(name+'.json'))])
            for name, budgets in cases]
ledger.write_text(json.dumps(commands, indent=2)+'\n')
for case in commands:
    print('Running '+case['name'], flush=True)
    subprocess.run(case['command'], cwd=root, env={**os.environ, 'PYTHONPATH': str(root)}, check=True)
result = json.loads((out/'concurrency-boundary.json').read_text())
checks = []
for row in result['samples']:
    if row['variant'] != 'candidate' or row['operation'] != 'cold':
        continue
    windows = 1 if row['requested_source_buffer_bytes'] < 134*2**20 else 2
    writers = 1 if row['requested_destination_buffer_bytes'] < 114*2**20 else 2
    expected = (windows*67*2**20, writers*57*2**20)
    actual = (row['peak_source_buffer_bytes'], row['peak_destination_buffer_bytes'])
    checks.append(dict(repetition=row['repetition'], source_bytes=row['requested_source_buffer_bytes'],
        destination_bytes=row['requested_destination_buffer_bytes'], expected=expected, actual=actual, passed=actual==expected))
gates = dict(complete=len(checks)==15 and all(check['passed'] for check in checks), checks=checks,
    meaning='cold peaks exercise one complete source/writer envelope below the rounded boundary, two at and above it')
(out/'concurrency-boundary-gates.json').write_text(json.dumps(gates, indent=2)+'\n')
assert gates['complete'], gates
print('All 15 cold concurrency-boundary gates passed.', flush=True)

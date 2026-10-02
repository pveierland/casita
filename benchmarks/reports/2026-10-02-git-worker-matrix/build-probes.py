"""Rebuild the exact source revisions and fixture patches retained with this report."""
import argparse
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--directory', type=Path, required=True)
parser.add_argument('--target-dir', type=Path, required=True)
parser.add_argument('--jobs', type=int, default=4)
args = parser.parse_args()
if args.jobs < 1:
    parser.error('jobs must be positive')
report = Path(__file__).resolve().parent
root = report.parents[2]
out = args.directory.resolve()
out.mkdir(parents=True, exist_ok=False)
for variant in ('baseline', 'candidate'):
    manifest = json.loads((report / (variant + '.build.json')).read_text())
    checkout = out / variant
    subprocess.run(['git', 'worktree', 'add', '--detach', str(checkout), manifest['source_head']], cwd=root, check=True)
    patch = gzip.decompress((report / (variant + '.patch.gz')).read_bytes())
    subprocess.run(['git', 'apply', '-'], cwd=checkout, input=patch, check=True)
    (checkout / 'Cargo.lock').write_bytes(gzip.decompress((report / 'Cargo.lock.gz').read_bytes()))
    env = {**os.environ, 'CARGO_TARGET_DIR': str(args.target_dir.resolve() / variant)}
    # Force this workspace package to compile even if a target cache was copied.
    subprocess.run(['cargo', 'clean', '--release', '-p', 'casita'], cwd=checkout, env=env, check=True)
    log = out / (variant + '.cargo.jsonl')
    with log.open('w') as stdout, (out / (variant + '.build.log')).open('w') as stderr:
        subprocess.run(['cargo', 'test', '--locked', '--release', '-j', str(args.jobs), '-p', 'casita',
                        '--no-default-features', '--features', manifest['features'],
                        '--test', 'git_worker_matrix', '--no-run', '--message-format=json'],
                       cwd=checkout, env=env, stdout=stdout, stderr=stderr, check=True)
    subprocess.run([sys.executable, str(report / 'freeze-probe.py'), str(checkout), str(log),
                    str(out / 'bin' / variant)], env=env, check=True)

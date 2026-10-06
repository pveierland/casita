"""Same-executable strategy comparison, pinned to one performance core."""
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time

root = pathlib.Path('/tmp/mnos-ingest/casita')
evidence = pathlib.Path('/tmp/mnos-ingest/evidence')
mode = sys.argv[1] if len(sys.argv) > 1 else 'screen'
assert mode in ('screen', 'matrix')
prefix = evidence / f'collection-mark-snapshot-{mode}-cpu2'
assert not list(evidence.glob(prefix.name + '*')), 'refuse to overwrite evidence'
binary = evidence / 'collection-mark-snapshot'
manifest = json.loads(binary.with_suffix('.build.json').read_text())
assert hashlib.sha256(binary.read_bytes()).hexdigest() == manifest['executable_sha256']
initial_affinity = sorted(os.sched_getaffinity(0))
assert 2 in initial_affinity
os.sched_setaffinity(0, {2})
command = [sys.executable, '-m', 'benchmarks.suites.collection_mark',
           '--parents', '8192' if mode == 'screen' else '127,128,255,256,257,8192',
           '--memory-limits', '256,250000',
           '--mode', 'pins', '--mode', 'snapshot-full', '--mode', 'snapshot-partial', '--mode', 'snapshot-sparse', '--iterations', '3', '--repetitions', '4', '--probe-binary', str(binary),
           '--no-build', '--output', str(prefix.with_suffix('.json'))]
for source, suffix in [(pathlib.Path(__file__), '-launcher.py'),
                       (root / 'benchmarks/suites/collection_mark.py', '-runner.py')]:
    shutil.copy2(source, str(prefix) + suffix)
protocol = dict(command=command, cwd=str(root), build_manifest=manifest,
                initial_affinity=initial_affinity, affinity=sorted(os.sched_getaffinity(0)),
                design='One mode/strategy per process; adjacent matched strategies, AB/BA balanced over four repetitions; warm iterations are subsamples.',
                started=time.time())
protocol_path = pathlib.Path(str(prefix) + '-protocol.json')
protocol_path.write_text(json.dumps(protocol, indent=2) + '\n')
with pathlib.Path(str(prefix) + '.log').open('w') as log:
    result = subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT)
protocol.update(exit_code=result.returncode, ended=time.time())
protocol_path.write_text(json.dumps(protocol, indent=2) + '\n')
print(prefix, 'exit', result.returncode, flush=True)
raise SystemExit(result.returncode)

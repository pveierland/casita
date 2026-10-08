"""Verify archived bytes, identity references and the twelve screen summaries."""
import gzip
import hashlib
import json
import statistics
from pathlib import Path

root = Path(__file__).resolve().parent
artifacts = json.loads((root / 'artifacts.json').read_text())
external = json.loads((root / 'external-inputs.json').read_text())
identities = {r['original_path']: r['sha256'] for r in artifacts + external}
assert len(identities) == len(artifacts) + len(external)
decoded = {}
for row in artifacts:
    packed = (root / row['file']).read_bytes()
    assert hashlib.sha256(packed).hexdigest() == row['compressed_sha256']
    raw = gzip.decompress(packed)
    assert len(raw) == row['bytes'] and hashlib.sha256(raw).hexdigest() == row['sha256']
    decoded[row['original_path']] = raw
references = 0
for name, raw in decoded.items():
    if not name.endswith('.json'):
        continue
    document = json.loads(raw)
    if not isinstance(document, dict):
        continue
    for field in ('input_sha256', 'artifact_sha256'):
        for path, expected in document.get(field, {}).items():
            assert identities[path] == expected, path
            references += 1

def document(suffix):
    matches = [raw for name, raw in decoded.items() if name.endswith(suffix)]
    assert len(matches) == 1, suffix
    return json.loads(matches[0])

guard = document('/retained-verified-screen-guard/result.json')
assert guard['success'] and guard['exit_code'] == 0
assert not any(guard[key] for key in ('errors', 'stop', 'interrupted', 'forced_cleanup', 'unexpected', 'remaining', 'adopted'))
execution = document('/retained-verified-screen-run/execution.json')
assert execution['complete'] and execution['artifacts_unchanged']
assert len(execution['entries']) == 1 and execution['entries'][0]['status'] == 'passed'
qualification = document('/retained-verified-screen-qualification.json')
assert qualification['success'] and not any(qualification[key] for key in ('unexpected', 'forced_cleanup', 'remaining', 'adopted'))
compiler = document('/retained-verified-screen-compiler-check.json')
assert compiler['compiler_success'] and compiler['source_unchanged'] and not compiler['guard_success']
assert compiler['executable_sha256'] == qualification['executable_sha256']
rejected = document('/retained-verified-screen-build/result.json')
assert rejected['exit_code'] == 0 and not rejected['success'] and not rejected['remaining']
assert len(rejected['adopted']) == 1 and rejected['adopted'][0]['exit_code'] == 0
samples = document('/retained-verified-screen-run/retained-verified-paths.json')['samples']
expected = [(size, width, i, mode) for size in (0, 4096, 16383, 16384, 16385, 524289)
            for width in (1, 64) for i, mode in enumerate(('unscoped', 'scoped', 'scoped', 'unscoped', 'unscoped', 'scoped'))]
assert [(r['size'], r['concurrency'], r['sample'], r['path']) for r in samples] == expected
assert all(r['correctness'] == 'passed' and r['reads'] == 64 and r['elapsed_ns'] > 0 for r in samples)
analysis = document('/retained-verified-screen-analysis.json')
assert json.loads((root / 'analysis.json').read_text()) == analysis
assert len(analysis['observations']) == 12
for observation in analysis['observations']:
    rows = [r for r in samples if (r['size'], r['concurrency']) == (observation['size'], observation['concurrency'])]
    a = [r['elapsed_ns'] for r in rows if r['path'] == 'unscoped']
    b = [r['elapsed_ns'] for r in rows if r['path'] == 'scoped']
    assert observation['unscoped_ns'] == a and observation['scoped_ns'] == b
    assert observation['unscoped_median_ns'] == statistics.median(a)
    assert observation['scoped_median_ns'] == statistics.median(b)
    assert observation['median_change_percent'] == 100 * (statistics.median(b) / statistics.median(a) - 1)
    assert observation['paired_change_percent'] == [100 * (rows[j]['elapsed_ns'] / rows[i]['elapsed_ns'] - 1) for i, j in ((0, 1), (3, 2), (4, 5))]
print(json.dumps({'success': True, 'artifacts': len(artifacts), 'external_identities_not_replayed': len(external), 'identity_references': references, 'samples': len(samples), 'comparisons': 12}))

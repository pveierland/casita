"""Verify the saved receipt archive and recompute paired metrics without external stores."""
import hashlib
import json
import statistics
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
manifest = json.loads((ROOT / 'artifacts.json').read_text())
archive = ROOT / manifest['archive']
assert hashlib.sha256(archive.read_bytes()).hexdigest() == manifest['archive_sha256']
expected = {row['path']: row for row in manifest['artifacts']}
assert len(expected) == len(manifest['artifacts'])
data = {}
with tarfile.open(archive, 'r:gz') as tar:
    for member in tar:
        assert member.isfile() and member.name in expected and member.name not in data
        body = tar.extractfile(member).read()
        record = expected[member.name]
        assert len(body) == record['bytes']
        assert hashlib.sha256(body).hexdigest() == record['sha256']
        data[member.name] = body
assert data.keys() == expected.keys()


def read(path):
    return json.loads(data[path])


def rows(path):
    return [json.loads(line) for line in data[path].splitlines()]


server = json.loads((ROOT / 'server-analysis.json').read_text())
micro = json.loads((ROOT / 'micro-analysis.json').read_text())
assert server == read('evidence/collection-membership-server-analysis-v4.json')
assert micro == read('evidence/collection-membership-micro-analysis-v2.json')
protocol = read('evidence/cms3/protocol.json')
matrix = read('evidence/cms3/result.json')
assert matrix['success'] and matrix['input_unchanged'] and len(matrix['runs']) == 24
observed = {}
for index, case in enumerate(protocol['schedule']):
    name = f"{index:02d}-r{case['repetition']}-{case['mode']}-{case['variant']}"
    base = f'evidence/cms3/{name}'
    receipt = read(base + '/result.json')
    assert receipt['success'] and receipt['exit_code'] == 0 and receipt['case'] == case
    aggregate_bytes = data[base + '/benchmark.json']
    assert hashlib.sha256(aggregate_bytes).hexdigest() == receipt['result_sha256']
    aggregate = json.loads(aggregate_bytes)
    nested = base + '/benchmark-artifacts/00-' + case['mode']
    assert hashlib.sha256(data[nested + '/result.json']).hexdigest() == aggregate['cases'][0]['result_sha256']
    result = read(nested + '/result.json')
    assert result['success'] and result['input_unchanged'] and not result['errors']
    assert not any(result['cleanup'][key] for key in ('forced', 'remaining', 'before', 'adopted'))
    assert result['completed'] == [0, 15, 16, 31, 32, 47, 48, 49]
    assert result['read_hash_pairs'] == 28
    events = rows(nested + '/events.jsonl')
    assert not any(event['event'] == 'process-sample-unavailable' for event in events)
    samples = rows(nested + '/resources.jsonl')
    target = result['target']
    pid = next(event['pid'] for event in events if event['event'] == 'spawn' and event['name'] == 'server-first')
    first, last = [next(member for member in target[key]['members'] if member['pid'] == pid)
                   for key in ('start_sample', 'end_sample')]
    assert first['start_ticks'] == last['start_ticks']
    ticks = read(nested + '/protocol.json')['ticks_per_second']
    values = dict(wall_seconds=target['elapsed_seconds'],
                  sampled_cpu_seconds=sum(last[key] - first[key] for key in ('user_ticks', 'system_ticks')) / ticks,
                  write_bytes=last['io']['write_bytes'] - first['io']['write_bytes'],
                  family_rss_bytes=max(sample['family_rss_bytes'] for sample in samples),
                  physical_wal_bytes=max((sample['files']['casita.sqlite-wal'] or {}).get('bytes', 0) for sample in samples))
    observed[case['mode'], case['repetition'], case['variant']] = values
for pair in server['pairs']:
    for variant in ('baseline', 'candidate'):
        actual = observed[pair['mode'], pair['repetition'], variant]
        assert all(value == pair['variants'][variant][key] for key, value in actual.items())
    for key, ratio in pair['ratios'].items():
        assert ratio == observed[pair['mode'], pair['repetition'], 'candidate'][key] / observed[pair['mode'], pair['repetition'], 'baseline'][key]
for gate in server['gates']:
    ratio = statistics.median(pair['ratios'][gate['metric']] for pair in server['pairs'] if pair['mode'] == gate['mode'])
    limit = .9 if gate['mode'] == 'aged' and gate['metric'] == 'wall_seconds' else 1.1 if gate['metric'] == 'physical_wal_bytes' else 1.05
    assert ratio == gate['median_paired_ratio'] and limit == gate['limit']
    assert gate['passed'] == (ratio <= limit)
assert server['server_gates_pass'] == all(gate['passed'] for gate in server['gates'])
assert len(micro['pairs']) == 60
for pair in micro['pairs']:
    for variant, observation in pair['observations'].items():
        base = f"evidence/collection-membership-micro-v2/{pair['index']:03d}-{variant}"
        receipt = read(base + '/result.json')
        assert receipt['success'] and hashlib.sha256(data[base + '/benchmark.json']).hexdigest() == receipt['result_sha256']
        report = read(base + '/benchmark.json')
        assert report['complete'] and len(report['samples']) == 1
        sample = report['samples'][0]
        assert all(sample[key] == observation[key] for key in ('wall_seconds', 'max_rss_bytes', 'metrics'))
    baseline, candidate = pair['observations']['baseline'], pair['observations']['candidate']
    assert pair['wall_ratio'] == candidate['wall_seconds'] / baseline['wall_seconds']
    assert pair['rss_ratio'] == candidate['max_rss_bytes'] / baseline['max_rss_bytes']
for gate in micro['gates']:
    group = [pair for pair in micro['pairs'] if all(pair['case'][key] == gate[key] for key in ('count', 'memory_limit'))]
    assert len(group) == 6
    assert statistics.median(pair['wall_ratio'] for pair in group) == gate['median_paired_wall_ratio']
    assert statistics.median(pair['rss_ratio'] for pair in group) == gate['median_paired_rss_ratio']
    assert gate['wall_pass'] and gate['rss_pass'] and (not gate['primary'] or gate['primary_pass'])
print(json.dumps(dict(archive_files=len(data), server_cases=24, micro_cases=120,
                      server_gates_pass=server['server_gates_pass'], micro_gates_pass=micro['micro_gates_pass'],
                      scope='Saved archive integrity and metric replay; external executables, stores and SDKs are not revalidated.')))

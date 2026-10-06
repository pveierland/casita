"""Independently audit frozen process receipts and summarize paired timings."""
import hashlib
import importlib.util
import json
import pathlib
import statistics
import sys

sys.path.insert(0, '/tmp/mnos-ingest/casita')
path = pathlib.Path(sys.argv[1])
runner = path.with_name(path.stem + '-runner.py')
spec = importlib.util.spec_from_file_location('frozen_runner', runner)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
parse_sample = module.parse_sample
output = path.with_name(path.stem + '-audit.json')
assert not output.exists()
result = json.loads(path.read_text())
assert result['complete'] is True and result['result_schema'] in ('casita.collection-mark.v2', 'casita.collection-mark.v3')
config = result['configuration']
v3 = result['result_schema'] == 'casita.collection-mark.v3'
assert config['strategies'] == ['legacy', 'current'] and config['modes'] == ['named', 'pins']
expected_processes = config['repetitions'] * len(config['parents']) * len(config['memory_limits']) * 4 * len(config.get('shapes', ['shared', 'distinct']))
assert len(result['processes']) == expected_processes
assert len(result['samples']) == expected_processes * 2
groups = {}
audit_count = 0
for index, process in enumerate(result['processes']):
    assert process['exit_code'] == 0
    n, shape_value, limit, strategy, mode, rep = [process[k] for k in
        ('parents', 'shape' if v3 else 'shared', 'memory_limit', 'strategy', 'mode', 'repetition')]
    shape = shape_value if v3 else ('shared' if shape_value else 'distinct')
    case = parse_sample(process['stdout'], n, shape_value, limit, config['iterations'], strategy, mode)
    objects = n * 2 if shape == 'distinct' else n + 1
    reads = objects if shape == 'chain' or (strategy == 'current' and mode == 'named') else n * 2
    for sample in case['samples']:
        assert sample['record_reads'] == reads
        assert (sample['spill_files'] > 0) == (objects >= limit)
        assert (sample['spill_peak_bytes'] > 0) == (objects >= limit)
        audit_count += 1
    if index % 2 == 0:
        mate = result['processes'][index + 1]
        assert all(process[k] == mate[k] for k in ('parents', 'shape' if v3 else 'shared', 'memory_limit', 'mode', 'repetition'))
        assert [strategy, mate['strategy']] == (['legacy', 'current'] if rep % 2 else ['current', 'legacy'])
    for phase in ('first', 'warm'):
        selected = [s for s in case['samples'] if s['warm'] == (phase == 'warm')]
        ms = statistics.mean(s['nanos'] for s in selected) / 1e6
        emitted = [s for s in result['samples'] if s['entries'] == n and s['shape'] == shape
                   and s['spill_memory_objects'] == limit and s['variant'] == strategy and s['repetition'] == rep
                   and s['operation'] == f'mark-{mode}-{phase}']
        assert len(emitted) == 1 and abs(emitted[0]['wall_seconds'] * 1000 - ms) < 1e-9
        key = (n, shape, limit, mode, phase)
        pair = groups.setdefault(key, {}).setdefault(rep, {})
        assert strategy not in pair
        pair[strategy] = ms
rows = []
for key, pairs in sorted(groups.items()):
    assert len(pairs) == config['repetitions']
    legacy = statistics.median(p['legacy'] for p in pairs.values())
    current = statistics.median(p['current'] for p in pairs.values())
    rows.append(dict(zip(('parents', 'shape', 'memory_limit', 'mode', 'phase'), key),
                     legacy_ms=legacy, current_ms=current, median_change_percent=(current/legacy-1)*100,
                     pairs=[dict(repetition=rep, **pair, change_percent=(pair['current']/pair['legacy']-1)*100)
                            for rep, pair in sorted(pairs.items())]))
report = dict(source=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
              processes=expected_processes, exact_mark_audits=audit_count,
              note='Medians of independent process means; warm iterations are subsamples. No end-to-end claim.', rows=rows)
output.write_text(json.dumps(report, indent=2) + '\n')
for row in rows:
    if row['parents'] == 8192 and row['phase'] == 'warm':
        print(row['shape'], row['memory_limit'], row['mode'],
              f"{row['legacy_ms']:.3f} -> {row['current_ms']:.3f} ms ({row['median_change_percent']:+.1f}%)",
              'pairs', [round(p['change_percent'], 1) for p in row['pairs']])
print('audited', expected_processes, 'processes,', audit_count, 'exact mark sets')

"""Audit matched release ingestion identities and summarize independent runs."""
import hashlib
import json
import pathlib
import statistics

evidence = pathlib.Path('/tmp/mnos-ingest/evidence')
path = evidence / 'snapshot-adaptive-ingestion.json'
report = json.loads(path.read_text())
assert report['complete'] is True
expected_order = [('control', 1), ('candidate', 1), ('candidate', 2), ('control', 2),
                  ('control', 3), ('candidate', 3), ('candidate', 4), ('control', 4)]
assert [(s['variant'], s['trial']) for s in report['samples']] == expected_order
revisions = [report['scenario'][phase]['commit'] for phase in ('from', 'to')]
reference_path = pathlib.Path('/tmp/mnos-ingest/mnos-nix-xp/docs/reports/evidence/git-ingestion/2026-10-06-wider-updates.json')
references = json.loads(reference_path.read_text())['identities']
for sample in report['samples']:
    assert sample['process']['exit_code'] == 0
    assert [i['revision'] for i in sample['identities']] == revisions
    assert [(s['revision'], s['stage']) for s in sample['stages']] == [
        (revision, stage) for revision in revisions for stage in ('import_git', 'git_tree', 'nar')]
    for identity in sample['identities']:
        assert all(identity[k] == references[identity['revision']][k]
                   for k in ('revision', 'git_tree', 'root', 'nar_hash', 'nar_bytes'))
    assert sample['pressure_events']
    assert all(s['wall_seconds'] >= 0 for s in sample['stages'])

medians = {}
for variant in ('control', 'candidate'):
    selected = [s for s in report['samples'] if s['variant'] == variant]
    values = dict(wall_seconds=statistics.median(s['process']['wall_seconds'] for s in selected),
                  peak_rss_bytes=statistics.median(s['process']['max_rss_bytes'] for s in selected),
                  observed_pressure_updates=[len(s['pressure_events']) for s in selected],
                  file_counts=[len(s['inventory']) for s in selected])
    for phase, revision in zip(('fresh', 'update'), revisions):
        for stage in ('import_git', 'git_tree', 'nar'):
            values[f'{phase}_{stage}_seconds'] = statistics.median(
                next(x['wall_seconds'] for x in s['stages'] if x['revision'] == revision and x['stage'] == stage)
                for s in selected)
        values[f'{phase}_total_seconds'] = statistics.median(
            sum(x['wall_seconds'] for x in s['stages'] if x['revision'] == revision) for s in selected)
    medians[variant] = values
changes = {key: (medians['candidate'][key] / value - 1) * 100
           for key, value in medians['control'].items() if isinstance(value, (float, int)) and value}
pairs = []
for trial in range(1, 5):
    pair = {s['variant']: s for s in report['samples'] if s['trial'] == trial}
    pairs.append(dict(trial=trial, wall_change_percent=(pair['candidate']['process']['wall_seconds'] /
                     pair['control']['process']['wall_seconds'] - 1) * 100))
output = evidence / 'snapshot-adaptive-ingestion-audit.json'
assert not output.exists()
output.write_text(json.dumps(dict(source=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                      complete_runs=8, identities_checked=16, medians=medians,
                      change_percent=changes, pairs=pairs,
                      limitations=['OS-managed caches and unrestricted recorded CPU affinity.',
                                   'Marker polling observes updates, not exact collection counts or durations.',
                                   'Release pair only; no near/week or browser claim.']), indent=2) + '\n')
print(json.dumps(dict(medians=medians, change_percent=changes, pairs=pairs), indent=2))

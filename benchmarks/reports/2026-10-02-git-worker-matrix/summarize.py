"""Rebuild compact tables from complete, validated paired samples."""
import argparse
import csv
import gzip
import json
from pathlib import Path
from benchmarks.suites.git_worker_matrix import summarize_metrics

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('directory', type=Path)
args = parser.parse_args()
def read_json(path):
    return json.loads(path.read_text() if path.exists()
                      else gzip.decompress(Path(str(path) + '.gz').read_bytes()))


rows, hosts = [], []
for command in json.loads((args.directory / 'matrix-commands.json').read_text()):
    name = command['name']
    result = read_json(args.directory / (name + '.json'))
    if not result['complete']:
        raise ValueError('incomplete run: ' + name)
    if result['configuration']['paired']:
        rows += [dict(case=name, **row) for row in summarize_metrics(result)]
    observations = read_json(args.directory / (name + '.json.host.json'))['samples']
    hosts.append(dict(case=name, samples=len(result['samples']), host_intervals=len(observations),
        competing_build_intervals=sum(bool(row['competing_processes']) for row in observations),
        external_cpu_fraction_max=max((row['external_cpu_fraction'] for row in observations), default=None)))
for name, items in [('summary.csv', rows), ('host-summary.csv', hosts)]:
    with (args.directory / name).open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(items[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(items)
print(f"{sum(row['samples'] for row in hosts)} audited samples; {len(rows)} paired comparison cells")

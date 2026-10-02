"""Summarize verified matched samples, preserving phase and worker settings."""
import csv
import json
import statistics as stats
from pathlib import Path

ROOT = Path(__file__).resolve().parent
KEYS = ['operation', 'files', 'file_bytes', 'packed', 'decode_workers', 'max_buffered_bytes', 'content']
summary, hosts = [], []
commands = json.loads((ROOT/'matrix-commands.json').read_text())
supplemental = ROOT/'supplemental-commands.json'
if supplemental.exists():
    commands += json.loads(supplemental.read_text())
for command in commands:
    name = command['name']
    result = json.loads((ROOT/(name+'.json')).read_text())
    assert result['complete'], name
    groups = {}
    for row in result['samples']:
        assert row['status'] == 'ok' and row['bounded_fixture']
        assert row['payload_correctness'] == 'independent BLAKE3 and exact streaming readback'
        assert row['requested_decode_workers'] == row['decode_workers']
        enabled = result['configuration']['delta_spilling' if row['variant'] == 'candidate' else 'baseline_delta_spilling']
        assert row['delta_spilling'] == enabled
        expected = row['fixture_blob_deltas'] if enabled and row['operation'] == 'cold' else 0
        assert row['spilled_delta_objects'] == expected
        group = groups.setdefault(tuple(row[key] for key in KEYS), {}).setdefault(row['variant'], {})
        assert row['repetition'] not in group
        group[row['repetition']] = row
    for key, variants in sorted(groups.items()):
        baseline, candidate = variants['baseline'], variants['candidate']
        assert baseline.keys() == candidate.keys()
        assert len(baseline) == result['configuration']['repetitions']
        for i in baseline:
            assert baseline[i]['root'] == candidate[i]['root']
            assert baseline[i]['fixture_blob_deltas'] == candidate[i]['fixture_blob_deltas']
        reductions = [100*(1-candidate[i]['wall_seconds']/baseline[i]['wall_seconds']) for i in baseline]
        memory_reductions = [baseline[i]['parent_hwm_after_import_bytes']-candidate[i]['parent_hwm_after_import_bytes'] for i in baseline]
        item = dict(case=name, **dict(zip(KEYS,key)), pairs=len(baseline), enough_samples=len(baseline)>=5,
            blob_deltas=baseline[0]['fixture_blob_deltas'])
        for variant, rows in variants.items():
            item[variant+'_wall_median_seconds'] = stats.median(row['wall_seconds'] for row in rows.values())
            for phase in ['before','after']:
                values = [row['parent_hwm_'+phase+'_import_bytes']/2**20 for row in rows.values()]
                item[variant+'_hwm_'+phase+'_median_mib'] = stats.median(values)
                item[variant+'_hwm_'+phase+'_min_mib'] = min(values)
                item[variant+'_hwm_'+phase+'_max_mib'] = max(values)
            # Both binaries contain the real spill report; the final case compares
            # spilling disabled/enabled in the same normalized-body binary.
            item[variant+'_reserved_spill_median_mib'] = stats.median(row['peak_spill_bytes']/2**20 for row in rows.values())
            # Linux observations are required before making I/O claims. Physical
            # counters reflect kernel accounting and can lag asynchronous writeback.
            assert all(row['import_process_io'] is not None for row in rows.values())
            for counter in ['rchar','wchar','read_bytes','write_bytes','cancelled_write_bytes']:
                item[variant+'_io_'+counter+'_median_bytes'] = stats.median(row['import_process_io'][counter] for row in rows.values())
        rate = result['configuration']['process_cpu_ticks_per_second']
        assert rate > 0 and result['configuration']['cpu_metrics']
        totals = {}
        for variant, rows in variants.items():
            totals[variant] = {}
            for i, row in rows.items():
                cpu = row['import_process_cpu']
                assert set(cpu) == {'user_ticks', 'system_ticks'}
                assert all(type(n) is int and n >= 0 for n in cpu.values())
                totals[variant][i] = sum(cpu.values())
            for counter in ['user_ticks', 'system_ticks']:
                item[variant+'_'+counter+'_median'] = stats.median(row['import_process_cpu'][counter] for row in rows.values())
            item[variant+'_cpu_median_seconds'] = stats.median(totals[variant].values()) / rate
        cpu_differences = [totals['candidate'][i]-totals['baseline'][i] for i in baseline]
        cpu_reductions = [100*(1-totals['candidate'][i]/totals['baseline'][i]) for i in baseline if totals['baseline'][i] > 0]
        item.update(cpu_ticks_per_second=rate, cpu_added_ticks_median=stats.median(cpu_differences),
                    cpu_zero_baseline_pairs=sum(totals['baseline'][i] == 0 for i in baseline),
                    cpu_reduction_median_percent=stats.median(cpu_reductions) if cpu_reductions else None)
        item.update(time_reduction_median_percent=stats.median(reductions),
            time_reduction_min_percent=min(reductions), time_reduction_max_percent=max(reductions),
            faster_pairs=sum(value>0 for value in reductions),
            paired_hwm_reduction_median_mib=stats.median(memory_reductions)/2**20,
            paired_hwm_reduction_min_mib=min(memory_reductions)/2**20,
            paired_hwm_reduction_max_mib=max(memory_reductions)/2**20)
        summary.append(item)
    host = json.loads((ROOT/(name+'.json.host.json')).read_text())['samples']
    hosts.append(dict(case=name, samples=len(result['samples']), host_intervals=len(host),
        competing_build_intervals=sum(bool(row['competing_processes']) for row in host),
        external_cpu_fraction_max=max((row['external_cpu_fraction'] for row in host), default=None)))
for name, rows in [('summary.csv',summary),('host-summary.csv',hosts)]:
    with (ROOT/name).open('w', newline='') as output:
        writer=csv.DictWriter(output,fieldnames=list(rows[0]),lineterminator='\n')
        writer.writeheader(); writer.writerows(rows)
print(f"{sum(row['samples'] for row in hosts)} audited import samples; {len(summary)} comparison cells")

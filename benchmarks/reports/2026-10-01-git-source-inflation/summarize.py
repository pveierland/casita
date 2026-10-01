"""Regenerate cold memory and paired timing summaries from audited samples."""
import csv
import json
import statistics as stats
from pathlib import Path

ROOT = Path(__file__).resolve().parent
NAMES = ["initial-16m", "thresholds", "large-64m", "parallel-16m", "small-control", "delta-fallback", "compressed-64m", "byte-budget"]
KEYS = ["operation", "files", "file_bytes", "packed", "decode_workers", "max_buffered_bytes", "content"]
summary = []
hosts = []
for name in NAMES:
    result = json.loads((ROOT / (name + ".json")).read_text())
    assert result["complete"], name
    groups = {}
    for row in result["samples"]:
        assert row["status"] == "ok" and row["bounded_fixture"]
        assert row["payload_correctness"] == "independent BLAKE3 and exact streaming readback"
        assert row["requested_decode_workers"] == row["decode_workers"]
        groups.setdefault(tuple(row[key] for key in KEYS), {}).setdefault(row["variant"], {})[row["repetition"]] = row
    for key, variants in sorted(groups.items()):
        baseline, candidate = variants["baseline"], variants["candidate"]
        assert baseline.keys() == candidate.keys() and len(baseline) == 5
        reductions = [100 * (1 - candidate[i]["wall_seconds"] / baseline[i]["wall_seconds"]) for i in baseline]
        item = dict(case=name, **dict(zip(KEYS, key)), pairs=len(baseline))
        for variant, rows in variants.items():
            item[variant + "_wall_median_seconds"] = stats.median(row["wall_seconds"] for row in rows.values())
            for phase in ["before", "after"]:
                values = [row["parent_hwm_" + phase + "_import_bytes"] / 2**20 for row in rows.values()]
                item[variant + "_hwm_" + phase + "_median_mib"] = stats.median(values)
                item[variant + "_hwm_" + phase + "_min_mib"] = min(values)
                item[variant + "_hwm_" + phase + "_max_mib"] = max(values)
        item.update(time_reduction_median_percent=stats.median(reductions), time_reduction_min_percent=min(reductions), time_reduction_max_percent=max(reductions), faster_pairs=sum(x > 0 for x in reductions))
        summary.append(item)
    host = json.loads((ROOT / (name + ".json.host.json")).read_text())["samples"]
    hosts.append(dict(case=name, samples=len(result["samples"]), host_intervals=len(host), competing_build_intervals=sum(bool(x["competing_processes"]) for x in host), external_cpu_fraction_max=max(x["external_cpu_fraction"] for x in host)))
for name, rows in [("summary.csv", summary), ("host-summary.csv", hosts)]:
    with (ROOT / name).open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
print(f"{sum(row['samples'] for row in hosts)} audited import samples; {len(summary)} comparison cells")

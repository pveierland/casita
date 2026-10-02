"""Measure Git decoder scaling with bounded fixtures and pre-audit parent memory."""
from __future__ import annotations
import argparse
import sys
import statistics
from benchmarks.suites.git_closure_import import summarize_pairs, validate_worker_metrics
from benchmarks.suites.repository import BenchmarkError
from benchmarks.suites.git_closure_import import main as run


def summarize_metrics(result):
    """Use matched repetitions for time effects; retain absolute memory and CPU."""
    if not result.get("complete"):
        raise BenchmarkError("cannot summarize an incomplete run")
    rate = result["configuration"]["clock_ticks_per_second"]
    if type(rate) is not int or rate <= 0:
        raise BenchmarkError("invalid CPU clock tick rate")
    rows = result["samples"]
    dimensions = ("operation", "backend", "files", "file_bytes", "content", "packed",
                  "concurrency", "max_buffered_bytes", "requested_decode_workers")
    summary = summarize_pairs(rows)
    for item in summary:
        if item["pairs"] != result["configuration"]["repetitions"]:
            raise BenchmarkError("missing repetitions")
        groups = {}
        for variant in ("baseline", "candidate"):
            selected = [row for row in rows if row["variant"] == variant
                        and all(row[key] == item[key] for key in dimensions)]
            for row in selected:
                validate_worker_metrics(row, row["decode_workers"])
            groups[variant] = {row["repetition"]: row for row in selected}
            item[f"{variant}_hwm_decrease_samples"] = sum(
                row["parent_hwm_after_import_bytes"] < row["parent_hwm_before_import_bytes"]
                for row in selected)
            for phase in ("before", "after"):
                values = [row[f"parent_hwm_{phase}_import_bytes"] / 2**20 for row in selected]
                for name, fn in (("median", statistics.median), ("min", min), ("max", max)):
                    item[f"{variant}_hwm_{phase}_{name}_mib"] = fn(values)
            item[f"{variant}_cpu_median_seconds"] = statistics.median(
                sum(row["import_process_cpu"].values()) / rate for row in selected)
            for field in ("peak_decode_workers", "peak_source_bytes"):
                values = [row[field] for row in selected]
                item[f"{variant}_{field}_median"] = (statistics.median(values)
                    if all(value is not None for value in values) else None)
        differences = [(groups["candidate"][i]["parent_hwm_after_import_bytes"]
                        - before["parent_hwm_after_import_bytes"]) / 2**20
                       for i, before in groups["baseline"].items()]
        item["paired_hwm_increase_median_mib"] = statistics.median(differences)
    return summary


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--content", choices=("repeated", "random", "mixed", "clustered"), default="random")
    options, _ = parser.parse_known_args(arguments)
    smoke = options.profile == "smoke"
    return run([
        "--probe-target", "git_worker_matrix", "--bounded-fixture", "--worker-metrics",
        "--pack-window", "16" if options.content == "clustered" else "0",
        "--counts", "16" if smoke else "15,16,17",
        "--file-bytes", "65536" if smoke else "1024,65536,1048576,4194304",
        "--max-buffered-bytes", "131071,131072,131073" if smoke else "131071,131072,131073,67108864",
        "--decode-workers", "1,2,4,8", "--content", "random", *arguments,
    ])


if __name__ == "__main__":
    raise SystemExit(main())

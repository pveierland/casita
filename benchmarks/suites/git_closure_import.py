"""Inventory-free cold, warm, cross-root and wide-delta Git closure imports."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import pathlib
import subprocess
import statistics
import tempfile

from benchmarks import cli
from benchmarks.affinity import cpu_affinity, cpu_list
from benchmarks.suites import repository as common
from benchmarks.suites.metadata_collection import positive_csv

PROBE = "benchmark_git_closure_import"
CORRECTNESS = "exact imported/reused counts and exhaustive closure verification"


def nonnegative_csv(value):
    try:
        values = [int(part) for part in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated nonnegative integers") from error
    if not values or any(value < 0 for value in values):
        raise argparse.ArgumentTypeError("expected comma-separated nonnegative integers")
    return values


def buffer_budgets(value):
    try:
        pairs = [tuple(int(n) for n in part.split(":")) for part in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected SOURCE:DEST byte capacities") from error
    if not pairs or any(len(pair) != 2 or min(pair) < 0 or ((pair[0] == 0) != (pair[1] == 0)) for pair in pairs):
        raise argparse.ArgumentTypeError("expected SOURCE:DEST pairs, both positive or both zero")
    return pairs


def one_buffer_budget(value):
    pairs = buffer_budgets(value)
    if len(pairs) != 1:
        raise argparse.ArgumentTypeError("expected one SOURCE:DEST pair")
    return pairs[0]


def summarize_pairs(samples):
    """Pair identical workloads by repetition; report effects without hiding spread."""
    dimensions = ("operation", "backend", "files", "file_bytes", "content", "packed",
                  "concurrency", "max_buffered_bytes", "requested_decode_workers", "requested_delta_spilling", "imports", "requested_shared_cpu_limit", "requested_source_buffer_bytes", "requested_destination_buffer_bytes", "requested_chunk_upload_concurrency")
    defaults = {"requested_decode_workers": 1, "requested_delta_spilling": False, "imports": 1, "requested_shared_cpu_limit": 0, "requested_source_buffer_bytes": 0, "requested_destination_buffer_bytes": 0, "requested_chunk_upload_concurrency": 32}
    groups = {}
    for sample in samples:
        if not isinstance(sample.get("root"), str) or not sample["root"]:
            raise common.BenchmarkError("missing or invalid benchmark root identity")
        key = tuple(sample.get(name, defaults[name]) if name in defaults else sample[name] for name in dimensions)
        repetitions = groups.setdefault(key, {})
        pair = repetitions.setdefault(sample["repetition"], {})
        if sample["variant"] in pair:
            raise common.BenchmarkError("duplicate variant in paired measurement")
        pair[sample["variant"]] = sample
    summaries = []
    for key, repetitions in groups.items():
        baseline, candidate, reductions = [], [], []
        for pair in repetitions.values():
            if set(pair) != {"baseline", "candidate"}:
                raise common.BenchmarkError("incomplete paired measurement")
            before, after = pair["baseline"], pair["candidate"]
            if before.get("root") != after.get("root"):
                raise common.BenchmarkError("paired imports produced different root identities")
            baseline.append(before["wall_seconds"])
            candidate.append(after["wall_seconds"])
            reductions.append(100 * (1 - after["wall_seconds"] / before["wall_seconds"]))
        summaries.append(dict(zip(dimensions, key), pairs=len(baseline),
            enough_samples=len(baseline) >= 5,
            baseline_median_seconds=statistics.median(baseline),
            candidate_median_seconds=statistics.median(candidate),
            median_paired_reduction_percent=statistics.median(reductions),
            minimum_paired_reduction_percent=min(reductions),
            maximum_paired_reduction_percent=max(reductions)))
    return summaries


def validate_worker_metrics(row, workers):
    """Reject missing observations rather than inventing zero memory/CPU cost."""
    def integer(value):
        return type(value) is int and value >= 0
    before, after = (row.get(f"parent_hwm_{phase}_import_bytes") for phase in ("before", "after"))
    if not integer(before) or not integer(after) or before == 0 or after == 0:
        raise common.BenchmarkError("missing or invalid parent memory observations")
    cpu = row.get("import_process_cpu")
    if (not isinstance(cpu, dict) or set(cpu) != {"user_ticks", "system_ticks"}
            or not all(integer(value) for value in cpu.values())):
        raise common.BenchmarkError("missing or invalid import CPU observations")
    if not {"peak_decode_workers", "peak_source_bytes"} <= row.keys():
        raise common.BenchmarkError("missing source observations")
    peak, source = row.get("peak_decode_workers"), row.get("peak_source_bytes")
    if peak is None and source is None and workers == 1:
        return  # The pre-worker API has neither counter; preserve explicit nulls.
    if not integer(peak) or not integer(source) or peak > workers:
        raise common.BenchmarkError("invalid worker/source observations")
    if row.get("operation") == "warm" and (peak or source):
        raise common.BenchmarkError("warm import unexpectedly performed source work")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--counts", type=positive_csv)
    parser.add_argument("--max-buffered-bytes", type=positive_csv, default=[1023, 1024, 1025, 2047, 2048, 2049])
    parser.add_argument("--layout", choices=("loose", "packed", "both"), default="both")
    parser.add_argument("--backend", choices=("memory", "local", "both"), default="both")
    parser.add_argument("--file-bytes", type=positive_csv, default=[1024])
    parser.add_argument("--concurrency", type=positive_csv, default=[16])
    parser.add_argument("--decode-workers", type=positive_csv, default=[1])
    worker_baseline = parser.add_mutually_exclusive_group()
    worker_baseline.add_argument("--baseline-decode-workers", type=int)
    worker_baseline.add_argument("--match-baseline-decode-workers", action="store_true",
                                 help="use each requested decoder count for both variants")
    parser.add_argument("--imports", type=positive_csv, default=[1], help="concurrent imports into independent destination repositories")
    parser.add_argument("--shared-cpu-limit", type=nonnegative_csv, default=[0], help="shared source/destination CPU limits; zero disables admission")
    parser.add_argument("--baseline-shared-cpu-limit", type=int, default=0)
    parser.add_argument("--buffer-budget", type=buffer_budgets, default=[(0, 0)], help="comma-separated shared SOURCE:DEST buffer capacities; 0:0 disables admission")
    parser.add_argument("--baseline-buffer-budget", type=one_buffer_budget, default=(0, 0))
    parser.add_argument("--chunk-upload-concurrency", type=positive_csv, default=[32])
    parser.add_argument("--baseline-chunk-upload-concurrency", type=int)
    parser.add_argument("--content", choices=("repeated", "random", "mixed", "clustered"), default="repeated")
    parser.add_argument("--delta-spilling", action=argparse.BooleanOptionalAction, default=False, help="enable bounded file-backed reconstruction for located blob deltas")
    parser.add_argument("--baseline-delta-spilling", action="store_true")
    parser.add_argument("--cpu-metrics", action="store_true", help="require import-only Linux process CPU ticks (all threads; excludes children)")
    parser.add_argument("--delta-metrics", action="store_true", help="observe delta counts and import I/O even when spilling is disabled")
    parser.add_argument("--probe-target", choices=("git_closure_import", "git_worker_matrix"), default="git_closure_import")
    parser.add_argument("--worker-metrics", action="store_true", help="require Linux parent memory and import CPU observations")
    parser.add_argument("--bounded-fixture", action="store_true", help="stream fixture generation/readback and capture parent RSS before audits")
    parser.add_argument("--pack-window", type=int, default=16)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--baseline-binary", type=pathlib.Path)
    parser.add_argument("--cpu-affinity", type=cpu_list)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    if args.baseline_chunk_upload_concurrency is not None and args.baseline_chunk_upload_concurrency < 1:
        parser.error("baseline chunk upload concurrency must be positive")
    if args.baseline_shared_cpu_limit < 0:
        parser.error("baseline shared CPU limit must be nonnegative")
    if args.worker_metrics and not pathlib.Path("/proc/self/stat").exists():
        parser.error("worker observations require Linux /proc")
    if args.pack_window < 0:
        parser.error("pack window must be nonnegative")
    if args.content == "clustered" and args.pack_window == 0:
        parser.error("clustered fixture requires delta packing")
    if args.repetitions < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive repetitions and a probe binary with --no-build are required")
    if min(args.file_bytes) < 8:
        parser.error("file sizes must be at least 8 bytes to encode distinct objects")
    if args.baseline_decode_workers is not None and args.baseline_decode_workers < 1:
        parser.error("baseline decode workers must be positive")
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", "test", "--release", "-p", "casita", "--no-default-features", "--features", "native,git,experimental",
                                "--test", args.probe_target, "--no-run", "--message-format=json"],
                               cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        artifacts = [json.loads(line) for line in built.stdout.splitlines() if line.startswith("{")]
        paths = [item["executable"] for item in artifacts if item.get("reason") == "compiler-artifact"
                 and item.get("target", {}).get("name") == args.probe_target and item.get("executable")]
        if len(paths) != 1:
            raise common.BenchmarkError("expected one Git closure benchmark executable")
        binary = pathlib.Path(paths[0])
    binary = binary.resolve()
    variants = [("candidate", binary)]
    if args.baseline_binary:
        variants.insert(0, ("baseline", args.baseline_binary.resolve()))
    artifacts = []
    for variant, executable in variants:
        with executable.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        artifact = dict(variant=variant, path=str(executable), sha256=digest)
        manifest = pathlib.Path(str(executable) + ".build.json")
        if manifest.exists():
            build = json.loads(manifest.read_text())
            if build.get("executable_sha256") != digest:
                raise common.BenchmarkError("build manifest fingerprint does not match executable")
            if not build.get("lockfile_sha256"):
                raise common.BenchmarkError("build manifest is missing its dependency lockfile fingerprint")
            artifact["build"] = build
        artifacts.append(artifact)
    if len(artifacts) == 2 and all("build" in artifact for artifact in artifacts):
        for field in ("lockfile_sha256", "features", "default_features", "rustc_version", "rustflags"):
            if artifacts[0]["build"].get(field) != artifacts[1]["build"].get(field):
                raise common.BenchmarkError(f"paired build manifests differ in {field}")
    if args.bounded_fixture and len(artifacts) == 2:
        hashes = [artifact.get("build", {}).get("fixture_sha256") for artifact in artifacts]
        if any(hashes) and (not all(hashes) or hashes[0] != hashes[1]):
            raise common.BenchmarkError("paired bounded fixture fingerprints differ")
    if (len(artifacts) == 2 and artifacts[0]["sha256"] == artifacts[1]["sha256"]
            and (args.baseline_decode_workers is None or all(n == args.baseline_decode_workers for n in args.decode_workers))
            and all(limit == args.baseline_shared_cpu_limit for limit in args.shared_cpu_limit)
            and all(pair == args.baseline_buffer_budget for pair in args.buffer_budget)
            and (args.baseline_chunk_upload_concurrency is None or all(n == args.baseline_chunk_upload_concurrency for n in args.chunk_upload_concurrency))
            and args.delta_spilling == args.baseline_delta_spilling):
        parser.error("baseline and candidate executables must have distinct hashes")
    cpu_ticks = os.sysconf("SC_CLK_TCK") if args.cpu_metrics else None
    if args.cpu_metrics and (type(cpu_ticks) is not int or cpu_ticks <= 0):
        raise common.BenchmarkError("CPU counters require a positive system clock tick rate")
    counts = args.counts or ([63, 64, 65] if args.profile == "smoke" else [63, 64, 65, 255, 256, 257, 10000])
    layouts = [False, True] if args.layout == "both" else [args.layout == "packed"]
    backends = ["memory", "local"] if args.backend == "both" else [args.backend]
    result = dict(schema_version=1, result_schema="casita.git-closure-import.v1", suite_id="native-git",
                  complete=False, artifacts=artifacts, samples=[], processes=[], configuration=dict(
                      counts=counts, byte_budgets=args.max_buffered_bytes, layouts=layouts,
                      backends=backends, file_bytes=args.file_bytes, concurrency=args.concurrency,
                      decode_workers=args.decode_workers, baseline_decode_workers=None if args.match_baseline_decode_workers else args.baseline_decode_workers or 1,
                      match_baseline_decode_workers=args.match_baseline_decode_workers,
                      chunk_upload_concurrency=args.chunk_upload_concurrency, baseline_chunk_upload_concurrency=args.baseline_chunk_upload_concurrency,
                      buffer_budgets=args.buffer_budget, baseline_buffer_budget=args.baseline_buffer_budget,
                      imports=args.imports, shared_cpu_limits=args.shared_cpu_limit, baseline_shared_cpu_limit=args.baseline_shared_cpu_limit,
                      bounded_fixture=args.bounded_fixture, pack_window=args.pack_window,
                      delta_spilling=args.delta_spilling, baseline_delta_spilling=args.baseline_delta_spilling, delta_metrics=args.delta_metrics,
                      cpu_metrics=args.cpu_metrics, process_cpu_ticks_per_second=cpu_ticks,
                      worker_metrics=args.worker_metrics, clock_ticks_per_second=os.sysconf("SC_CLK_TCK") if args.worker_metrics else None,
                      content=args.content, paired=bool(args.baseline_binary), cpu_affinity=args.cpu_affinity,
                      memory_measurement="whole-process peak RSS includes fixture creation and audits",
                      spill_memory_objects=64, metadata_frontier=256,
                      repetitions=args.repetitions, timing="combined concurrent import makespan; fixture generation and per-repository audits excluded"))
    with cpu_affinity(args.cpu_affinity), tempfile.TemporaryDirectory(prefix="casita-git-closure-benchmark-") as temporary:
        work = pathlib.Path(temporary)
        result["environment"] = common.environment_metadata(work)
        if hasattr(os, "sched_getaffinity"):
            allowed = sorted(os.sched_getaffinity(0))
            result["environment"]["cpu_affinity"] = allowed
            frequencies = {}
            for cpu in allowed:
                frequency = pathlib.Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/cpuinfo_max_freq")
                if frequency.exists():
                    frequencies[str(cpu)] = int(frequency.read_text().strip())
            result["environment"]["cpu_max_frequencies_khz"] = frequencies
        try:
            matrix = itertools.product(counts, args.max_buffered_bytes, layouts, backends,
                                       args.file_bytes, args.concurrency, args.decode_workers, args.imports, args.shared_cpu_limit, args.buffer_budget, args.chunk_upload_concurrency, range(args.repetitions))
            for count, budget, packed, backend, file_bytes, concurrency, decode_workers, imports, shared_cpu_limit, buffer_budget, chunk_concurrency, repetition in matrix:
                ordered = variants if repetition % 2 == 0 else list(reversed(variants))
                for variant, executable in ordered:
                    actual_workers = ((args.baseline_decode_workers or 1)
                                      if variant == "baseline" and not args.match_baseline_decode_workers
                                      else decode_workers)
                    actual_shared_cpu = args.baseline_shared_cpu_limit if variant == "baseline" else shared_cpu_limit
                    actual_chunk_concurrency = args.baseline_chunk_upload_concurrency if variant == "baseline" and args.baseline_chunk_upload_concurrency is not None else chunk_concurrency
                    actual_buffers = args.baseline_buffer_budget if variant == "baseline" else buffer_budget
                    actual_spill = args.baseline_delta_spilling if variant == "baseline" else args.delta_spilling
                    env = {**os.environ, "CASITA_GIT_CLOSURE_CHUNK_CONCURRENCY": str(actual_chunk_concurrency), "CASITA_GIT_CLOSURE_SOURCE_BUFFER_BYTES": str(actual_buffers[0]), "CASITA_GIT_CLOSURE_DESTINATION_BUFFER_BYTES": str(actual_buffers[1]), "CASITA_GIT_CLOSURE_IMPORTS": str(imports), "CASITA_GIT_CLOSURE_SHARED_CPU_LIMIT": str(actual_shared_cpu), "CASITA_GIT_CLOSURE_CPU_METRICS": str(int(args.cpu_metrics)), "CASITA_GIT_CLOSURE_DELTA_METRICS": str(int(args.delta_metrics or args.delta_spilling or args.baseline_delta_spilling)), "CASITA_GIT_CLOSURE_DELTA_SPILL": str(int(actual_spill)), "CASITA_GIT_CLOSURE_BOUNDED_FIXTURE": str(int(args.bounded_fixture)),
                           "CASITA_GIT_CLOSURE_PACK_WINDOW": str(args.pack_window), "CASITA_GIT_CLOSURE_FILES": str(count), "CASITA_GIT_CLOSURE_BYTES": str(budget),
                           "CASITA_GIT_CLOSURE_PACKED": str(int(packed)),
                           "CASITA_GIT_CLOSURE_BACKEND": backend, "CASITA_GIT_CLOSURE_FILE_BYTES": str(file_bytes),
                           "CASITA_GIT_CLOSURE_CONCURRENCY": str(concurrency), "CASITA_GIT_CLOSURE_CONTENT": args.content,
                           "CASITA_GIT_CLOSURE_DECODE_WORKERS": str(actual_workers)}
                    timing = common.measured_command(common.CommandSpec(
                        [[str(executable), PROBE, "--exact", "--ignored", "--nocapture"]], work, env),
                        work / "stdout", work / "stderr", check=False)
                    stdout, stderr = (work / "stdout").read_text(), (work / "stderr").read_text()
                    result["processes"].append(dict(**timing, variant=variant, files=count, budget=budget, packed=packed,
                                                    backend=backend, file_bytes=file_bytes, concurrency=concurrency, content=args.content,
                                                    decode_workers=actual_workers, requested_decode_workers=decode_workers,
                                                    chunk_upload_concurrency=actual_chunk_concurrency, requested_chunk_upload_concurrency=chunk_concurrency,
                                                    source_buffer_bytes=actual_buffers[0], destination_buffer_bytes=actual_buffers[1], requested_source_buffer_bytes=buffer_budget[0], requested_destination_buffer_bytes=buffer_budget[1],
                                                    imports=imports, shared_cpu_limit=actual_shared_cpu, requested_shared_cpu_limit=shared_cpu_limit,
                                                    delta_spilling=actual_spill, requested_delta_spilling=args.delta_spilling,
                                                    repetition=repetition, stdout=stdout, stderr=stderr))
                    if timing["exit_code"] != 0:
                        raise common.BenchmarkError(f"Git closure benchmark failed: {stdout}\n{stderr}")
                    rows = [json.loads(line.removeprefix("git_closure_sample ")) for raw in stdout.splitlines()
                            if (line := raw.removeprefix(f"test {PROBE} ... ")).startswith("git_closure_sample ")]
                    if ([row.get("operation") for row in rows] != ["cold", "warm", "subtree-delta", "wide-delta"]
                            or "test result: ok. 1 passed; 0 failed;" not in stdout):
                        raise common.BenchmarkError("missing benchmark operations or passing correctness gate")
                    for row in rows:
                        if not isinstance(row.get("root"), str) or not row["root"]:
                            raise common.BenchmarkError("missing or invalid benchmark root identity")
                        if row.get("chunk_upload_concurrency", 32) != actual_chunk_concurrency:
                            raise common.BenchmarkError("incorrect destination chunk concurrency")
                        if actual_buffers != (0, 0) or "source_buffer_capacity" in row:
                            for part, requested in zip(("source", "destination"), actual_buffers):
                                cap = requested // 65536 * 65536
                                capacity = row.get(part + "_buffer_capacity")
                                peak = row.get("peak_" + part + "_buffer_bytes")
                                live = row.get("reserved_" + part + "_buffer_bytes")
                                active = cap > 0 and row["operation"] != "warm" and (part == "source" or backend == "local")
                                if (type(capacity) is not int or capacity != cap
                                        or type(peak) is not int or not 0 <= peak <= cap
                                        or (active and peak == 0) or (not active and peak != 0)
                                        or type(live) is not int or live != 0):
                                    raise common.BenchmarkError("incorrect shared buffer admission, peak or release")
                        if imports != 1 or actual_shared_cpu != 0 or "imports" in row:
                            peak_cpu = row.get("peak_cpu_jobs")
                            if (row.get("imports") != imports or row.get("audited_imports") != imports
                                    or row.get("shared_cpu_limit") != actual_shared_cpu
                                    or row.get("timing_scope") != "combined concurrent import makespan"
                                    or type(peak_cpu) is not int or peak_cpu < 0
                                    or peak_cpu > actual_shared_cpu
                                    or (actual_shared_cpu and row["operation"] != "warm" and peak_cpu == 0)
                                    or (row["operation"] == "warm" and peak_cpu != 0)):
                                raise common.BenchmarkError("incorrect shared CPU admission or concurrent import audit")
                        if args.cpu_metrics:
                            counters = row.get("import_process_cpu")
                            if (not isinstance(counters, dict) or set(counters) != {"user_ticks", "system_ticks"}
                                    or any(type(value) is not int or value < 0 for value in counters.values())):
                                raise common.BenchmarkError("missing or invalid import CPU counters")
                        if args.delta_metrics or args.delta_spilling or args.baseline_delta_spilling:
                            deltas = row.get("fixture_blob_deltas")
                            spilled = row.get("spilled_delta_objects")
                            peak = row.get("peak_spill_bytes")
                            expected_spilled = deltas * imports if actual_spill and row["operation"] == "cold" else 0
                            if (row.get("delta_spilling") is not actual_spill
                                    or not isinstance(deltas, int) or deltas < 0
                                    or (packed and args.content == "clustered" and count > 8 and file_bytes >= 65536 and deltas == 0)
                                    or spilled != expected_spilled
                                    or not isinstance(peak, int) or peak < 0 or (spilled and peak == 0)):
                                raise common.BenchmarkError("wrong delta spill selection, counts or reservation gate")
                            counters = row.get("import_process_io")
                            io_fields = {"rchar", "wchar", "read_bytes", "write_bytes", "cancelled_write_bytes"}
                            if ("import_process_io" not in row or (counters is not None and
                                    (not isinstance(counters, dict) or set(counters) != io_fields
                                     or any(type(value) is not int or value < 0 for value in counters.values())))):
                                raise common.BenchmarkError("invalid delta spill import I/O counters")
                        if args.worker_metrics:
                            validate_worker_metrics(row, actual_workers)
                        if args.bounded_fixture:
                            if (row.get("payload_correctness") != "independent BLAKE3 and exact streaming readback"
                                    or row.get("bounded_fixture") is not True
                                    or row.get("pack_window") != args.pack_window
                                    or not isinstance(row.get("parent_hwm_after_import_bytes"), int)
                                    or row["parent_hwm_after_import_bytes"] <= 0):
                                raise common.BenchmarkError("missing bounded fixture or parent RSS correctness gate")
                        if (row.get("correctness") != CORRECTNESS or row.get("files") != count
                                or row.get("packed") != packed or row.get("max_buffered_bytes") != budget
                                or row.get("backend") != backend or row.get("file_bytes") != file_bytes
                                or row.get("concurrency") != concurrency or row.get("content") != args.content
                                or row.get("decode_workers", 1) != actual_workers
                                or not isinstance(row.get("wall_nanos"), int) or row["wall_nanos"] <= 0):
                            raise common.BenchmarkError("wrong benchmark configuration or correctness gate")
                        expected = {"cold": (count + 2, 0), "warm": (0, 1),
                                    "subtree-delta": (2, 1), "wide-delta": (2, count)}[row["operation"]]
                        expected = tuple(value * imports for value in expected)
                        if ((row.get("imported_objects"), row.get("reused_objects")) != expected
                                or (row["operation"] == "warm" and row.get("source_bytes") != 0)):
                            raise common.BenchmarkError("incorrect import/reuse counters")
                        result["samples"].append(dict(status="ok", implementation="casita", variant=variant, entries=count,
                            requested_chunk_upload_concurrency=chunk_concurrency,
                            requested_source_buffer_bytes=buffer_budget[0], requested_destination_buffer_bytes=buffer_budget[1],
                            repetition=repetition, requested_shared_cpu_limit=shared_cpu_limit, requested_decode_workers=decode_workers,
                            requested_delta_spilling=args.delta_spilling,
                            wall_seconds=row["wall_nanos"] / 1e9,
                            max_rss_bytes=timing["max_rss_bytes"], **row))
                    common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
            if args.baseline_binary:
                result["paired_summary"] = summarize_pairs(result["samples"])
            result["complete"] = True
        except Exception as error:
            result["error"] = str(error)
            raise
        finally:
            common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

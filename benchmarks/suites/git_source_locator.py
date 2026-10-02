"""Optional Git pack locator limits with asserted streaming/fallback selection."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
from benchmarks import cli
from benchmarks.suites import repository as common

PROBE = "git::repository::closure_import::workers::streaming::tests::benchmark_git_source_locator"
CORRECTNESS = "asserted stream selection and exact stream/fallback payload"
THRESHOLDS = {"index-files": 32, "index-bytes": 16777216, "directory-entries": 256}


def validate_rows(stdout):
    rows = [json.loads(line.removeprefix("git_source_locator_sample ")) for raw in stdout.splitlines()
            if (line := raw.removeprefix(f"test {PROBE} ... ")).startswith("git_source_locator_sample ")]
    expected = {(dimension, side) for dimension in THRESHOLDS for side in [-1, 0, 1]}
    if len(rows) != 9 or {(r.get("dimension"), r.get("side")) for r in rows} != expected:
        raise common.BenchmarkError("missing locator threshold cases")
    if "test result: ok. 1 passed; 0 failed;" not in stdout:
        raise common.BenchmarkError("locator correctness test did not pass")
    for row in rows:
        threshold = THRESHOLDS[row["dimension"]]
        expected_stream = row["side"] <= 0 if row["dimension"] == "index-bytes" else row["side"] < 0
        if (row.get("correctness") != CORRECTNESS or row.get("threshold") != threshold
                or row.get("value") != threshold + row["side"] or row.get("streamed") is not expected_stream
                or not isinstance(row.get("wall_nanos"), int) or row["wall_nanos"] <= 0):
            raise common.BenchmarkError("wrong locator configuration or fallback gate")
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    if args.repetitions < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive repetitions and a binary with --no-build are required")
    binary = args.probe_binary
    if binary is None:
        build = subprocess.run(["cargo", "test", "--release", "-p", "casita", "--features", "git,experimental",
            "--lib", "--no-run", "--message-format=json"], cwd=cli.ROOT, capture_output=True, text=True)
        if build.returncode:
            raise common.BenchmarkError(build.stderr or build.stdout)
        artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith("{")]
        paths = [item["executable"] for item in artifacts if item.get("reason") == "compiler-artifact"
            and item.get("target", {}).get("kind") == ["lib"] and item.get("executable")]
        if len(paths) != 1:
            raise common.BenchmarkError("expected one Casita unit-test probe")
        binary = pathlib.Path(paths[0])
    binary = binary.resolve()
    with binary.open("rb") as handle:
        fingerprint = hashlib.file_digest(handle, "sha256").hexdigest()
    artifact = dict(path=str(binary), sha256=fingerprint)
    sidecar = pathlib.Path(str(binary)+".build.json")
    if sidecar.exists():
        build = json.loads(sidecar.read_text())
        if build.get("executable_sha256") != fingerprint or not build.get("lockfile_sha256"):
            raise common.BenchmarkError("locator build manifest fingerprint or lockfile is invalid")
        artifact["build"] = build
    result = dict(schema_version=1, result_schema="casita.git-source-locator.v1", suite_id="native-git",
        complete=False, artifacts=[artifact], samples=[], processes=[],
        configuration=dict(repetitions=args.repetitions, thresholds=THRESHOLDS,
            timing="sum of optional locator initialization and selection; intervening pack installation and payload audit excluded",
            memory_measurement="whole-process RSS includes fixture and payload audits"))
    with tempfile.TemporaryDirectory(prefix="casita-source-locator-") as temporary:
        work = pathlib.Path(temporary)
        result["environment"] = common.environment_metadata(work)
        try:
            for repetition in range(args.repetitions):
                timing = common.measured_command(common.CommandSpec(
                    [[str(binary), PROBE, "--exact", "--ignored", "--nocapture"]], work, os.environ.copy()),
                    work/"stdout", work/"stderr", check=False)
                stdout, stderr = (work/"stdout").read_text(), (work/"stderr").read_text()
                result["processes"].append(dict(**timing, stdout=stdout, stderr=stderr, repetition=repetition))
                if timing["exit_code"]:
                    raise common.BenchmarkError(f"locator probe failed: {stdout}\n{stderr}")
                result["samples"].extend(dict(**row, repetition=repetition, status="ok", implementation="casita",
                    wall_seconds=row["wall_nanos"]/1e9, max_rss_bytes=timing["max_rss_bytes"])
                    for row in validate_rows(stdout))
            result["complete"] = True
        except Exception as error:
            result["error"] = str(error)
            raise
        finally:
            common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

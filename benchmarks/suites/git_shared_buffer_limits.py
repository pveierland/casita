"""Reproduce producer-buffer limits and minimum progress envelopes."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import tempfile
from benchmarks import cli
from benchmarks.suites import repository as common

PREFIX = ""
CASES = {
    "rounding": ("reservations_never_round_capacity_up_or_saturate_oversized_requests", [131071, 131072, 131073]),
    "writer-envelope": ("pipeline::writer_capacity_boundary_preserves_one_complete_progress_envelope", ["minimum-1", "minimum", "minimum+1"]),
    "source-envelope": ("pipeline::source_capacity_boundary_and_oversized_buffered_fallback_are_explicit", [589823, 589824, 589825]),
    "window-reuse": ("pipeline::concurrent_imports_release_source_allowances_between_windows", ["two imports", "multiple source windows", "one CPU and one blocking thread"]),
    "queued-cancellation": ("pipeline::cancelled_writer_keeps_admission_with_queued_blocking_bytes", ["queued compression", "cancelled producer", "complete reservation recovery"]),
    "single-thread-progress": ("pipeline::shared_buffers_and_one_cpu_progress_and_release_after_real_streamed_import", ["one CPU permit", "one blocking thread", "oversized eligible stream"]),
}


def validate_case(stdout, name):
    # Serial libtest emits the test prefix before uncaptured boundary diagnostics.
    stdout = re.sub(r"buffer_boundary (?:writer|source)_minimum=[0-9]+\r?\n", "", stdout)
    if (PREFIX + CASES[name][0] + " ... ok" not in stdout
            or "test result: ok. 1 passed; 0 failed;" not in stdout):
        raise common.BenchmarkError("buffer boundary correctness test did not pass: " + name)


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
        build = subprocess.run(["cargo", "test", "--release", "-p", "casita", "--no-default-features", "--features", "native,git,experimental",
            "--test", "git_import_buffers", "--no-run", "--message-format=json"], cwd=cli.ROOT, capture_output=True, text=True)
        if build.returncode:
            raise common.BenchmarkError(build.stderr or build.stdout)
        artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith("{")]
        paths = [item["executable"] for item in artifacts if item.get("reason") == "compiler-artifact"
            and item.get("target", {}).get("name") == "git_import_buffers" and item.get("executable")]
        if len(paths) != 1:
            raise common.BenchmarkError("expected one buffer-admission test probe")
        binary = pathlib.Path(paths[0])
    binary = binary.resolve()
    with binary.open("rb") as handle:
        fingerprint = hashlib.file_digest(handle, "sha256").hexdigest()
    artifact = dict(path=str(binary), sha256=fingerprint)
    sidecar = pathlib.Path(str(binary)+".build.json")
    if sidecar.exists():
        build = json.loads(sidecar.read_text())
        if build.get("executable_sha256") != fingerprint or not build.get("lockfile_sha256"):
            raise common.BenchmarkError("git-shared-buffer-limits build manifest fingerprint or lockfile is invalid")
        artifact["build"] = build
    result = dict(schema_version=1, result_schema="casita.git-shared-buffer-limits.v1", suite_id="native-git",
        complete=False, artifacts=[artifact], samples=[], processes=[],
        configuration=dict(repetitions=args.repetitions, cases={name: dict(test=PREFIX + test, values=values) for name, (test, values) in CASES.items()},
            timing="whole test process including fixture construction, all boundary values, and assertions; diagnostic only",
            memory_measurement="whole-process RSS includes fixture and payload audits"))
    with tempfile.TemporaryDirectory(prefix="casita-git-shared-buffer-limits-") as temporary:
        work = pathlib.Path(temporary)
        result["environment"] = common.environment_metadata(work)
        try:
            for repetition in range(args.repetitions):
                for name, (test, values) in CASES.items():
                    timing = common.measured_command(common.CommandSpec(
                        [[str(binary), PREFIX + test, "--exact", "--nocapture"]], work, os.environ.copy()),
                        work/"stdout", work/"stderr", check=False)
                    stdout, stderr = (work/"stdout").read_text(), (work/"stderr").read_text()
                    result["processes"].append(dict(**timing, stdout=stdout, stderr=stderr, repetition=repetition, case=name))
                    if timing["exit_code"]:
                        raise common.BenchmarkError(f"buffer boundary probe failed: {stdout}\n{stderr}")
                    validate_case(stdout, name)
                    result["samples"].append(dict(repetition=repetition, status="ok", implementation="casita",
                        case=name, values=values, wall_seconds=timing["wall_seconds"], max_rss_bytes=timing["max_rss_bytes"],
                        correctness="exact boundary test passed"))
            result["complete"] = True
        except Exception as error:
            result["error"] = str(error)
            raise
        finally:
            common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

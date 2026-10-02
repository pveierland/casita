"""Reproduce delta limit boundaries, including fixture and correctness costs."""
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

PREFIX = "git::repository::closure_import::workers::streaming::delta::tests::"
CASES = {
    "depth": ("delta_chain_depth_and_reference_cycles_are_bounded", [63, 64, 65]),
    "declared-work": ("declared_work_boundary_is_checked_before_ref_hint_fallback", [68719476735, 68719476736, 68719476737]),
    "spill-capacity": ("delta_zero_copy_length_means_65536_and_spill_boundaries_hold", [131071, 131072, 131073]),
    "tiny-result-spill-capacity": ("tiny_delta_result_still_reserves_its_full_base", [1048576, 1048577, 1048578]),
    "source-handles": ("source_handle_admission_is_bounded_and_recovers_after_a_window", [127, 128, 129]),
    "window-resume": ("full_plan_window_preserves_and_retries_the_pending_key", [128, 129]),
}


def validate_case(stdout, name):
    if (PREFIX + CASES[name][0] + " ... ok" not in stdout
            or "test result: ok. 1 passed; 0 failed;" not in stdout):
        raise common.BenchmarkError("delta boundary correctness test did not pass: " + name)


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
            raise common.BenchmarkError("delta-limits build manifest fingerprint or lockfile is invalid")
        artifact["build"] = build
    result = dict(schema_version=1, result_schema="casita.git-delta-limits.v1", suite_id="native-git",
        complete=False, artifacts=[artifact], samples=[], processes=[],
        configuration=dict(repetitions=args.repetitions, cases={name: dict(test=PREFIX + test, values=values) for name, (test, values) in CASES.items()},
            timing="whole test process including fixture construction, all boundary values, and assertions; diagnostic only",
            memory_measurement="whole-process RSS includes fixture and payload audits"))
    with tempfile.TemporaryDirectory(prefix="casita-delta-limits-") as temporary:
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
                        raise common.BenchmarkError(f"delta boundary probe failed: {stdout}\n{stderr}")
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

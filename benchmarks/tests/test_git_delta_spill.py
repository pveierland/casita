import json
import pathlib
import sys
import tempfile
import unittest
from benchmarks.suites import git_closure_import as suite
from benchmarks.tests.test_git_source_inflation import PROBE as BASE_PROBE

PROBE = BASE_PROBE.replace('wall_nanos=1000,', 'delta_spilling=os.environ["CASITA_GIT_CLOSURE_DELTA_SPILL"] == "1",\n        fixture_blob_deltas=8,\n        spilled_delta_objects=8 if operation == "cold" and os.environ["CASITA_GIT_CLOSURE_DELTA_SPILL"] == "1" else 0,\n        peak_spill_bytes=2097152, import_process_io=None, wall_nanos=1000,')

class DeltaSpillTests(unittest.TestCase):
    def test_runner_requires_spill_selection_and_actual_delta_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "probe"
            binary.write_text("#!" + sys.executable + "\n" + PROBE)
            binary.chmod(0o755)
            args = ["--probe-binary", str(binary), "--no-build", "--bounded-fixture", "--delta-spilling",
                    "--content", "clustered", "--counts", "16", "--file-bytes", "1048576",
                    "--max-buffered-bytes", "67108864", "--decode-workers", "1", "--backend", "local",
                    "--layout", "packed", "--output", str(root/"result.json")]
            self.assertEqual(suite.main(args), 0)
            result = json.loads((root/"result.json").read_text())
            self.assertTrue(result["complete"])
            self.assertEqual(result["samples"][0]["spilled_delta_objects"], 8)
            for before, after in [
                ('delta_spilling=os.environ["CASITA_GIT_CLOSURE_DELTA_SPILL"] == "1"', 'delta_spilling=False'),
                ('spilled_delta_objects=8 if', 'spilled_delta_objects=7 if'),
                ('peak_spill_bytes=2097152', 'peak_spill_bytes=0'),
                ('import_process_io=None', 'import_process_io={"wchar": -1}'),
                ('import_process_io=None, ', ''),
            ]:
                binary.write_text("#!" + sys.executable + "\n" + PROBE.replace(before, after))
                with self.assertRaisesRegex(suite.common.BenchmarkError, "delta spill"):
                    suite.main(args)
                self.assertFalse(json.loads((root/"result.json").read_text())["complete"])

    def test_all_and_revision_runner_use_the_integration_probe(self):
        from benchmarks import all as runner, revisions
        command = runner.build_commands(["git-delta-spill"], pathlib.Path("/build"))[0]
        self.assertEqual(command[command.index("--test") + 1], "git_closure_import")
        arguments = runner.suite_arguments("git-delta-spill", pathlib.Path("/binaries"), "smoke", 1)
        self.assertIn("/binaries/git_closure_import", arguments)
        self.assertIn("--no-build", arguments)
        self.assertEqual(revisions.SUITE_BUILD_SPECS["git-delta-spill"].cargo_json_test, "git_closure_import")

    def test_boundary_fresh_checkout_build_allows_dependency_resolution(self):
        from unittest import mock
        from benchmarks.suites import git_delta_limits as limits
        with mock.patch.object(limits.subprocess, "run", side_effect=RuntimeError("build reached")) as build:
            with self.assertRaisesRegex(RuntimeError, "build reached"):
                limits.main(["--output", "/unused-delta-limits.json"])
        command = build.call_args.args[0]
        self.assertNotIn("--offline", command)
        self.assertNotIn("--locked", command)
        self.assertIn("--lib", command)

    def test_boundary_suite_requires_the_named_test_to_pass(self):
        from benchmarks.suites import git_delta_limits as limits
        for name, (test, values) in limits.CASES.items():
            output = "test " + limits.PREFIX + test + " ... ok\ntest result: ok. 1 passed; 0 failed;"
            limits.validate_case(output, name)
            for bad in [output.replace(test, "wrong_test"), output.replace("1 passed", "0 passed")]:
                with self.assertRaisesRegex(suite.common.BenchmarkError, "correctness"):
                    limits.validate_case(bad, name)
        from benchmarks import all as runner, revisions
        command = runner.build_commands(["git-delta-limits"], pathlib.Path("/build"))[0]
        self.assertIn("--lib", command)
        self.assertEqual(revisions.SUITE_BUILD_SPECS["git-delta-limits"].cargo_json_test, "casita")

    def test_disabled_spilling_can_still_require_delta_and_io_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "probe"
            binary.write_text("#!" + sys.executable + "\n" + PROBE)
            binary.chmod(0o755)
            arguments = ["--probe-binary", str(binary), "--no-build", "--bounded-fixture",
                "--delta-spilling", "--no-delta-spilling", "--delta-metrics", "--content", "clustered",
                "--counts", "16", "--file-bytes", "1048576", "--max-buffered-bytes", "67108864",
                "--decode-workers", "1", "--backend", "local", "--layout", "packed",
                "--output", str(root/"result.json")]
            self.assertEqual(suite.main(arguments), 0)
            report = json.loads((root/"result.json").read_text())
            self.assertTrue(report["configuration"]["delta_metrics"])
            for row in report["samples"]:
                self.assertFalse(row["delta_spilling"])
                self.assertEqual(row["fixture_blob_deltas"], 8)
                self.assertEqual(row["spilled_delta_objects"], 0)
            binary.write_text("#!" + sys.executable + "\n" + PROBE.replace("import_process_io=None, ", ""))
            with self.assertRaisesRegex(suite.common.BenchmarkError, "I/O counters"):
                suite.main(arguments)

    def test_disabled_control_is_registered_and_runs_without_spilling(self):
        from benchmarks.suites.git_delta_disabled import main
        from benchmarks import all as runner, revisions
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "probe"
            binary.write_text("#!" + sys.executable + "\n" + PROBE)
            binary.chmod(0o755)
            self.assertEqual(main(["--probe-binary", str(binary), "--no-build",
                "--decode-workers", "1", "--delta-spilling", "--output", str(root/"result.json")]), 0)
            result = json.loads((root/"result.json").read_text())
            self.assertTrue(result["complete"])
            self.assertTrue(result["configuration"]["delta_metrics"])
            self.assertTrue(all(not row["delta_spilling"] for row in result["samples"]))
        command = runner.build_commands(["git-delta-disabled"], pathlib.Path("/build"))[0]
        self.assertEqual(command[command.index("--test") + 1], "git_closure_import")
        self.assertIn("/binaries/git_closure_import", runner.suite_arguments(
            "git-delta-disabled", pathlib.Path("/binaries"), "smoke", 1))
        self.assertEqual(revisions.SUITE_BUILD_SPECS["git-delta-disabled"].cargo_json_test, "git_closure_import")

    def test_cpu_metrics_require_complete_nonnegative_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "probe"
            probe = PROBE.replace('wall_nanos=1000,', 'import_process_cpu={"user_ticks": 3, "system_ticks": 1}, wall_nanos=1000,')
            binary.write_text("#!" + sys.executable + "\n" + probe)
            binary.chmod(0o755)
            arguments = ["--probe-binary", str(binary), "--no-build", "--cpu-metrics",
                "--counts", "16", "--max-buffered-bytes", "67108864", "--layout", "packed",
                "--backend", "local", "--output", str(root / "result.json")]
            self.assertEqual(suite.main(arguments), 0)
            report = json.loads((root / "result.json").read_text())
            self.assertGreater(report["configuration"]["process_cpu_ticks_per_second"], 0)
            for bad in ['None', '{"user_ticks": 3}', '{"user_ticks": -1, "system_ticks": 1}',
                        '{"user_ticks": True, "system_ticks": 1}', '{"user_ticks": 3, "system_ticks": 1, "extra": 0}']:
                binary.write_text("#!" + sys.executable + "\n" + probe.replace('{"user_ticks": 3, "system_ticks": 1}', bad))
                with self.assertRaisesRegex(suite.common.BenchmarkError, "CPU counters"):
                    suite.main(arguments)
                self.assertFalse(json.loads((root / "result.json").read_text())["complete"])
            binary.write_text("#!" + sys.executable + "\n" + PROBE)
            with self.assertRaisesRegex(suite.common.BenchmarkError, "CPU counters"):
                suite.main(arguments)

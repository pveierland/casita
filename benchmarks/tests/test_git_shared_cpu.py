import json
import pathlib
import sys
import tempfile
import unittest

from benchmarks.suites import git_closure_import as suite

PROBE = r"""
import json, os
for operation, imported, reused in [("cold", 12, 0), ("warm", 0, 2), ("subtree-delta", 4, 2), ("wide-delta", 4, 8)]:
    print("git_closure_sample " + json.dumps(dict(
        operation=operation, files=4, packed=True, max_buffered_bytes=67108864,
        backend="local", file_bytes=1024, concurrency=16, content="random",
        decode_workers=1, imports=2, audited_imports=2,
        shared_cpu_limit=1, peak_cpu_jobs=0 if operation == "warm" else 1,
        timing_scope="combined concurrent import makespan",
        imported_objects=imported, reused_objects=reused,
        source_bytes=0 if operation == "warm" else 4096,
        wall_nanos=1000000, root="fixed-root",
        correctness="exact imported/reused counts and exhaustive closure verification"
    )))
print("test result: ok. 1 passed; 0 failed;")
"""


class SharedCpuTests(unittest.TestCase):
    def test_runner_checks_shared_limits_and_every_concurrent_import(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "probe"
            binary.write_text("#!" + sys.executable + "\n" + PROBE)
            binary.chmod(0o755)
            arguments = ["--probe-binary", str(binary), "--no-build", "--counts", "4",
                "--max-buffered-bytes", "67108864", "--file-bytes", "1024",
                "--decode-workers", "1", "--concurrency", "16", "--backend", "local",
                "--layout", "packed", "--content", "random", "--imports", "2",
                "--shared-cpu-limit", "1", "--output", str(root / "result.json")]
            self.assertEqual(suite.main(arguments), 0)
            result = json.loads((root / "result.json").read_text())
            self.assertTrue(result["complete"])
            self.assertEqual(result["samples"][0]["imported_objects"], 12)
            self.assertEqual(result["samples"][0]["wall_seconds"], 0.001)
            for before, after in [
                ("imports=2,", "imports=1,"),
                ("audited_imports=2,", "audited_imports=1,"),
                ("shared_cpu_limit=1,", "shared_cpu_limit=2,"),
                ('peak_cpu_jobs=0 if operation == "warm" else 1', 'peak_cpu_jobs=2'),
                ('peak_cpu_jobs=0 if operation == "warm" else 1', 'peak_cpu_jobs=0'),
                ('peak_cpu_jobs=0 if operation == "warm" else 1', 'peak_cpu_jobs=True'),
                ('timing_scope="combined concurrent import makespan"', 'timing_scope="divided by imports"'),
                ('("cold", 12, 0)', '("cold", 6, 0)'),
            ]:
                binary.write_text("#!" + sys.executable + "\n" + PROBE.replace(before, after))
                with self.assertRaises(suite.common.BenchmarkError):
                    suite.main(arguments)
                self.assertFalse(json.loads((root / "result.json").read_text())["complete"])

    def test_paired_main_preserves_requested_dimensions_and_repetitions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binaries = [root / "baseline", root / "candidate"]
            for index, binary in enumerate(binaries):
                binary.write_text("#!" + sys.executable + "\n" + PROBE + f"\n# variant {index}\n")
                binary.chmod(0o755)
            self.assertEqual(suite.main([
                "--probe-binary", str(binaries[1]), "--baseline-binary", str(binaries[0]),
                "--no-build", "--counts", "4", "--max-buffered-bytes", "67108864",
                "--file-bytes", "1024", "--decode-workers", "1", "--concurrency", "16",
                "--backend", "local", "--layout", "packed", "--content", "random",
                "--imports", "2", "--shared-cpu-limit", "1", "--baseline-shared-cpu-limit", "1",
                "--repetitions", "2", "--output", str(root / "paired.json")]), 0)
            result = json.loads((root / "paired.json").read_text())
            self.assertTrue(result["complete"])
            self.assertEqual(len(result["samples"]), 16)
            self.assertEqual({row["repetition"] for row in result["samples"]}, {0, 1})
            self.assertTrue(all(row["requested_decode_workers"] == 1
                                and row["requested_shared_cpu_limit"] == 1
                                and row["imports"] == 2 for row in result["samples"]))
            self.assertEqual(len(result["paired_summary"]), 4)
            self.assertTrue(all(row["pairs"] == 2 for row in result["paired_summary"]))

    def test_shared_cpu_corpus_is_registered_and_runs_concurrent_audits(self):
        from benchmarks.suites.git_shared_cpu import main
        from benchmarks import all as runner, revisions
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "probe"
            probe = PROBE.replace("wall_nanos=1000000,", "bounded_fixture=True, pack_window=16, parent_hwm_after_import_bytes=1024, payload_correctness='independent BLAKE3 and exact streaming readback', import_process_cpu={'user_ticks': 3, 'system_ticks': 1}, wall_nanos=1000000,")
            binary.write_text("#!" + sys.executable + "\n" + probe)
            binary.chmod(0o755)
            self.assertEqual(main(["--profile", "smoke", "--probe-binary", str(binary),
                "--no-build", "--counts", "4", "--file-bytes", "1024", "--imports", "2",
                "--shared-cpu-limit", "1", "--decode-workers", "1", "--content", "random",
                "--output", str(root / "result.json")]), 0)
            result = json.loads((root / "result.json").read_text())
            self.assertTrue(result["complete"])
            self.assertTrue(all(row["audited_imports"] == 2 for row in result["samples"]))
        command = runner.build_commands(["git-shared-cpu"], pathlib.Path("/build"))[0]
        self.assertEqual(command[command.index("--test") + 1], "git_closure_import")
        self.assertIn("/binaries/git_closure_import", runner.suite_arguments(
            "git-shared-cpu", pathlib.Path("/binaries"), "smoke", 1))
        self.assertEqual(revisions.SUITE_BUILD_SPECS["git-shared-cpu"].cargo_json_test, "git_closure_import")

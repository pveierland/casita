import json
import os
import subprocess
import pathlib
import sys
import tempfile
import unittest

PROBE = '''import os, json
count = int(os.environ["CASITA_GIT_CLOSURE_FILES"])
for operation in ["cold", "warm", "subtree-delta", "wide-delta"]:
    new, reused = {"cold": (count+2, 0), "warm": (0, 1), "subtree-delta": (2, 1), "wide-delta": (2, count)}[operation]
    print("test benchmark_git_closure_import ... git_closure_sample " + json.dumps(dict(
        operation=operation, files=count, packed=os.environ["CASITA_GIT_CLOSURE_PACKED"] == "1",
        max_buffered_bytes=int(os.environ["CASITA_GIT_CLOSURE_BYTES"]),
        backend=os.environ["CASITA_GIT_CLOSURE_BACKEND"], file_bytes=int(os.environ["CASITA_GIT_CLOSURE_FILE_BYTES"]),
        concurrency=int(os.environ["CASITA_GIT_CLOSURE_CONCURRENCY"]), content=os.environ["CASITA_GIT_CLOSURE_CONTENT"],
        decode_workers=int(os.environ["CASITA_GIT_CLOSURE_DECODE_WORKERS"]),
        root="fixture-root", wall_nanos=1000, imported_objects=new, reused_objects=reused, source_bytes=0 if operation == "warm" else 1,
        correctness="exact imported/reused counts and exhaustive closure verification",
        bounded_fixture=os.environ["CASITA_GIT_CLOSURE_BOUNDED_FIXTURE"] == "1",
        pack_window=int(os.environ["CASITA_GIT_CLOSURE_PACK_WINDOW"]),
        payload_correctness="independent BLAKE3 and exact streaming readback", parent_hwm_after_import_bytes=4096)))
print("test result: ok. 1 passed; 0 failed;")
'''



class RetainedBufferTests(unittest.TestCase):
    def test_corpus_covers_stream_boundary_without_enabling_spilling(self):
        from benchmarks.suites.git_retained_buffers import main
        from benchmarks import all as runner, revisions
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "probe"
            binary.write_text("#!" + sys.executable + "\n" + PROBE)
            binary.chmod(0o755)
            self.assertEqual(main(["--profile", "smoke", "--probe-binary", str(binary),
                "--no-build", "--delta-spilling", "--output", str(root / "result.json")]), 0)
            report = json.loads((root / "result.json").read_text())
            self.assertTrue(report["complete"])
            self.assertFalse(report["configuration"]["delta_spilling"])
            self.assertEqual({r["file_bytes"] for r in report["samples"]}, {1048575, 1048576, 1048577})
            self.assertEqual({r["decode_workers"] for r in report["samples"]}, {1, 4})
            self.assertTrue(all(r["bounded_fixture"] and r["packed"] and r["pack_window"] == 16 for r in report["samples"]))
        command = runner.build_commands(["git-retained-buffers"], pathlib.Path("/build"))[0]
        self.assertEqual(command[command.index("--test") + 1], "git_closure_import")
        self.assertIn("/binaries/git_closure_import", runner.suite_arguments(
            "git-retained-buffers", pathlib.Path("/binaries"), "smoke", 1))
        self.assertEqual(revisions.SUITE_BUILD_SPECS["git-retained-buffers"].cargo_json_test, "git_closure_import")

    def test_profile_help_does_not_create_measurement_evidence(self):
        repository = pathlib.Path(__file__).resolve().parents[2]
        profile = repository / "benchmarks/reports/2026-10-01-git-retained-buffers/profile.py"
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "help.json"
            result = subprocess.run(
                [sys.executable, str(profile), "--help", "--output", str(output)],
                cwd=repository, env={**os.environ, "PYTHONPATH": str(repository)},
                capture_output=True, text=True, timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(output.exists())
            self.assertFalse(pathlib.Path(str(output) + ".host.json").exists())

import json
import pathlib
import sys
import tempfile
import unittest
from benchmarks import all as all_suites
from benchmarks.suites import chunk_upload_completion as suite
from benchmarks.suites.repository import BenchmarkError


class ChunkCompletionTests(unittest.TestCase):
    def test_permanent_suite_builds_its_probe(self):
        commands = all_suites.build_commands(["chunk-upload-completion"], pathlib.Path("/build"))
        self.assertEqual(len(commands), 1)
        self.assertIn("chunk_upload_completion", commands[0])

    def probe(self, path, root="same", correctness=suite.CORRECTNESS, serial_prefix=False):
        prefix = f"test {suite.PROBE} ... " if serial_prefix else ""
        path.write_text("#!" + sys.executable + "\n" + "import os, json\n" +
            "row = dict(file_bytes=int(os.environ['CASITA_CHUNK_COMPLETION_BYTES']), budget=int(os.environ['CASITA_CHUNK_COMPLETION_BUDGET']), delay_ms=int(os.environ['CASITA_CHUNK_COMPLETION_DELAY_MS']), wall_nanos=100, root=" + repr(root) + ", correctness=" + repr(correctness) + ")\n" +
            "print(" + repr(prefix + "chunk_upload_completion_sample ") + " + json.dumps(row))\nprint('test result: ok. 1 passed; 0 failed;')\n")
        path.chmod(0o755)
        return path

    def test_serial_libtest_prefix_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            probe = self.probe(directory / "probe", serial_prefix=True)
            output = directory / "result.json"
            self.assertEqual(suite.main(["--no-build", "--probe-binary", str(probe),
                "--file-bytes", "65536", "--budgets", "65536", "--delays-ms", "0",
                "--output", str(output)]), 0)
            result = json.loads(output.read_text())
            self.assertTrue(result["complete"])
            self.assertEqual(len(result["samples"]), 1)

    def test_different_roots_reject_the_pair_and_preserve_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            before = self.probe(directory/"before")
            after = self.probe(directory/"after", root="different")
            output = directory/"result.json"
            with self.assertRaisesRegex(BenchmarkError, "changed identity"):
                suite.main(["--no-build", "--probe-binary", str(after), "--baseline-binary", str(before),
                    "--file-bytes", "65536", "--budgets", "8192", "--delays-ms", "0", "--output", str(output)])
            result = json.loads(output.read_text())
            self.assertFalse(result["complete"])
            self.assertEqual(len(result["processes"]), 2)

    def test_missing_audit_is_never_an_accepted_measurement(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            probe = self.probe(directory/"probe", correctness="unchecked")
            output = directory/"result.json"
            with self.assertRaisesRegex(BenchmarkError, "missing audit"):
                suite.main(["--no-build", "--probe-binary", str(probe), "--file-bytes", "65536",
                    "--budgets", "8192", "--delays-ms", "0", "--output", str(output)])
            self.assertFalse(json.loads(output.read_text())["complete"])


if __name__ == "__main__":
    unittest.main()

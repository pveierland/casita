import json
import pathlib
import sys
import tempfile
import unittest
from benchmarks.suites import git_source_inflation as suite
from benchmarks.suites import git_closure_import as common

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

class SourceInflationTests(unittest.TestCase):
    def test_threshold_matrix_and_memory_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            probe.write_text("#!" + sys.executable + "\n" + PROBE)
            probe.chmod(0o755)
            args = ["--profile", "smoke", "--probe-binary", str(probe), "--no-build", "--output", str(root/"report.json")]
            self.assertEqual(suite.main(args), 0)
            report = json.loads((root/"report.json").read_text())
            self.assertTrue(report["complete"])
            self.assertEqual(len(report["samples"]), 48)
            self.assertEqual({r["file_bytes"] for r in report["samples"]}, {1048575,1048576,1048577})
            self.assertTrue(all(r["bounded_fixture"] and r["pack_window"] == 0 for r in report["samples"]))
            for before, after in [('parent_hwm_after_import_bytes=4096', 'parent_hwm_after_import_bytes=None'),
                                  ('independent BLAKE3 and exact streaming readback', 'unchecked payload'),
                                  ('pack_window=int(os.environ["CASITA_GIT_CLOSURE_PACK_WINDOW"])', 'pack_window=16')]:
                probe.write_text("#!" + sys.executable + "\n" + PROBE.replace(before, after))
                with self.assertRaisesRegex(common.common.BenchmarkError, "bounded fixture"):
                    suite.main([*args, "--file-bytes", "1048576", "--decode-workers", "1", "--layout", "loose"])
                self.assertFalse(json.loads((root/"report.json").read_text())["complete"])

    def test_paired_fixtures_must_match_when_fingerprinted(self):
        import hashlib
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for name, fixture in [("baseline", "old"), ("candidate", "new")]:
                binary = root/name
                binary.write_text(name)
                pathlib.Path(str(binary)+".build.json").write_text(json.dumps(dict(
                    executable_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                    lockfile_sha256="same-lock", features="native,git,experimental", default_features=False,
                    fixture_sha256=fixture)))
            with self.assertRaisesRegex(common.common.BenchmarkError, "fixture fingerprints"):
                suite.main(["--probe-binary", str(root/"candidate"), "--baseline-binary", str(root/"baseline"),
                            "--no-build", "--output", str(root/"out.json")])

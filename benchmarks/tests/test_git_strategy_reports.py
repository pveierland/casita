"""Exercise real suite output through revision reports and dashboard normalization."""
import itertools
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

from benchmarks import dashboard, revisions
from benchmarks.suites import git_blob_file, git_verified_stream


SUITES = [
    ("git-blob-file", git_blob_file, "ALIAS", {"backend": ["memory", "local"],
     "file_bytes": [8, 16], "files": [1, 2]}, ["reread", "alias"]),
    ("git-verified-stream", git_verified_stream, "STREAM", {"backend": ["memory", "local"],
     "file_bytes": [8, 16]}, ["reread", "stream"]),
]


def write_probe(path, suite, prefix):
    path.write_text("#!" + sys.executable + "\n" + f'''import json, os
prefix = "CASITA_GIT_{prefix}_"
row = dict(strategy=os.environ[prefix + "STRATEGY"], backend=os.environ[prefix + "BACKEND"],
           file_bytes=int(os.environ[prefix + "BYTES"]), wall_nanos=1000, root="same",
           correctness={suite.CORRECTNESS!r})
if prefix + "FILES" in os.environ:
    row["files"] = int(os.environ[prefix + "FILES"])
print({suite.PROBE.removeprefix("benchmark_") + "_sample "!r} + json.dumps(row))
print("test result: ok. 1 passed; 0 failed;")
''')
    path.chmod(0o755)


class GitStrategyReportTests(unittest.TestCase):
    def test_revision_reports_preserve_each_strategy_and_workload(self):
        for name, suite, prefix, dimensions, strategies in SUITES:
            with self.subTest(suite=name), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                series = [revisions.RevisionSpec(label, label, label, digit * 40)
                          for label, digit in [("before", "a"), ("after", "b")]]
                for revision in series:
                    write_probe(root / revision.label, suite, prefix)
                forwarded = ["--file-bytes", "8,16", "--backend", "both"]
                if "files" in dimensions:
                    forwarded += ["--files", "1,2"]
                output = root / "output"
                with (
                    mock.patch.object(revisions, "resolve_revisions", return_value=series),
                    mock.patch.object(revisions, "git_output", side_effect=lambda arguments:
                                      "" if arguments[0] == "status" else "f" * 40),
                ):
                    code = revisions.main([
                        "before", "after", "--suite", name, "--repetitions", "2",
                        "--output-dir", str(output),
                        *[f"--artifact={revision.label}={root / revision.label}" for revision in series],
                        "--", *forwarded])
                self.assertEqual(code, 0)
                report = json.loads((output / "series.json").read_text())
                expected = {(strategy, tuple(sorted(zip(dimensions, values))))
                            for strategy in strategies
                            for values in itertools.product(*dimensions.values())}
                for revision in report["revisions"]:
                    observations = revision["normalized"]["observations"]
                    self.assertEqual(len(observations), len(expected))
                    self.assertEqual({(row["operation"], tuple(sorted(json.loads(
                        row["workload"].split(":", 1)[1]).items()))) for row in observations}, expected)
                    for row in observations:
                        self.assertEqual((row["status"], row["rounds"], row["samples"]), ("ok", 2, 2))
                        self.assertEqual(row["implementation"], "casita")
                        self.assertEqual(row["cache_policy"], "cold")
                walls = [row for row in report["rows"] if row["metric"] == "wall_seconds"]
                self.assertEqual(len(walls), len(expected))
                self.assertTrue((output / "series.md").is_file())

    def test_incomplete_missing_and_failed_samples(self):
        for name, suite, prefix, dimensions, strategies in SUITES:
            with self.subTest(suite=name), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                write_probe(root / "probe", suite, prefix)
                output = root / "output.json"
                args = ["--probe-binary", str(root / "probe"), "--no-build", "--file-bytes", "8",
                        "--backend", "memory", "--output", str(output)]
                if "files" in dimensions:
                    args += ["--files", "1"]
                self.assertEqual(suite.main(args), 0)
                result = json.loads(output.read_text())
                for field in ["strategy", *dimensions]:
                    broken = json.loads(json.dumps(result))
                    del broken["samples"][0][field]
                    output.write_text(json.dumps(broken))
                    with self.assertRaisesRegex(dashboard.DashboardError, "lacks"):
                        dashboard.normalize_result(output)
                for field, value in [("backend", []), ("file_bytes", None), ("strategy", 1)]:
                    broken = json.loads(json.dumps(result))
                    broken["samples"][0][field] = value
                    output.write_text(json.dumps(broken))
                    with self.assertRaisesRegex(dashboard.DashboardError, "invalid workload"):
                        dashboard.normalize_result(output)
                output.write_text(json.dumps({**result, "complete": False}))
                with self.assertRaisesRegex(ValueError, "incomplete Git strategy"):
                    dashboard.normalize_result(output)
                result["samples"][0]["status"] = "failed"
                output.write_text(json.dumps(result))
                failed, = [row for row in dashboard.normalize_result(output)["observations"]
                           if row["status"] == "failed"]
                self.assertEqual((failed["samples"], failed["successful_samples"]), (1, 0))


if __name__ == "__main__":
    unittest.main()

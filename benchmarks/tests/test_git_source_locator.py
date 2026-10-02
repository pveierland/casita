import json
import unittest
from benchmarks.suites import git_source_locator as suite
from benchmarks import all as all_suites
from benchmarks import revisions

class LocatorTests(unittest.TestCase):
    def rows(self):
        return [dict(dimension=dimension, side=side, value=threshold+side, threshold=threshold,
            streamed=side <= 0 if dimension == "index-bytes" else side < 0,
            wall_nanos=1000, correctness=suite.CORRECTNESS)
            for dimension, threshold in suite.THRESHOLDS.items() for side in [-1, 0, 1]]

    def stdout(self, rows):
        return "\n".join((f"test {suite.PROBE} ... " if index == 0 else "") + "git_source_locator_sample " + json.dumps(row) for index, row in enumerate(rows)) + "\ntest result: ok. 1 passed; 0 failed;"

    def test_rejects_missing_boundaries_and_false_selection(self):
        rows = self.rows()
        self.assertEqual(suite.validate_rows(self.stdout(rows)), rows)
        with self.assertRaisesRegex(suite.common.BenchmarkError, "threshold"):
            suite.validate_rows(self.stdout(rows[:-1]))
        rows[0]["streamed"] = False
        with self.assertRaisesRegex(suite.common.BenchmarkError, "fallback"):
            suite.validate_rows(self.stdout(rows))
        rows = self.rows()
        rows[1]["value"] += 1
        with self.assertRaisesRegex(suite.common.BenchmarkError, "configuration"):
            suite.validate_rows(self.stdout(rows))
        with self.assertRaisesRegex(suite.common.BenchmarkError, "did not pass"):
            suite.validate_rows(self.stdout(self.rows()).replace("1 passed", "0 passed"))

    def test_fresh_checkout_build_allows_dependency_resolution(self):
        from unittest import mock
        with mock.patch.object(suite.subprocess, "run", side_effect=RuntimeError("build reached")) as build:
            with self.assertRaisesRegex(RuntimeError, "build reached"):
                suite.main(["--output", "/unused-locator-result.json"])
        command = build.call_args.args[0]
        self.assertEqual(command[:2], ["cargo", "test"])
        self.assertNotIn("--offline", command)
        self.assertNotIn("--locked", command)
        self.assertIn("--lib", command)

    def test_all_and_revision_builds_use_the_library_probe(self):
        import pathlib
        commands = all_suites.build_commands(["git-source-locator"], pathlib.Path("/build"))
        self.assertEqual(len(commands), 1)
        self.assertIn("--lib", commands[0])
        self.assertNotIn("--test", commands[0])
        args = all_suites.suite_arguments("git-source-locator", pathlib.Path("/binaries"), "smoke", 1)
        self.assertIn("/binaries/casita-lib-test", args)
        self.assertIn("--no-build", args)
        self.assertEqual(revisions.SUITE_BUILD_SPECS["git-source-locator"].cargo_json_test, "casita")

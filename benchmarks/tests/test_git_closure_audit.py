import json
import pathlib
import sys
import tempfile
import unittest

from benchmarks.suites import git_closure_audit as suite

FAKE_PROBE = r'''import json, os
commits = int(os.environ["CASITA_GIT_AUDIT_COMMITS"])
registry = os.environ["CASITA_GIT_AUDIT_REGISTRY"]
objects = 3 * commits
audits = 0 if registry == "builtin" else AUDITS
print("test benchmark_git_closure_audit ... git_closure_audit_sample " + json.dumps(dict(
    commits=commits, registry=registry,
    publication_batch_objects=int(os.environ["CASITA_GIT_AUDIT_BATCH_OBJECTS"]),
    objects=objects, imported_objects=objects, reused_objects=0, warm_imported_objects=0,
    warm_source_bytes=0, link_audits=audits, root="fixture-root", wall_nanos=1000,
    correctness="exact import counts, exhaustive closure verification and source-free warm reuse")))
print("test result: ok. 1 passed; 0 failed;")
'''


def write_probe(path, audits="objects"):
    path.write_text("#!" + sys.executable + "\n" + FAKE_PROBE.replace("AUDITS", audits))
    path.chmod(0o755)


class GitClosureAuditBenchmarkTests(unittest.TestCase):
    def test_revision_build_uses_the_custom_format_probe_and_evaluator_features(self):
        from benchmarks import revisions
        arguments = revisions.SUITE_BUILD_SPECS["git-closure-audit"].cargo_arguments
        self.assertIn("--no-default-features", arguments)
        self.assertEqual(arguments[arguments.index("--features") + 1], "native,git,experimental")
        self.assertEqual(arguments[arguments.index("--test") + 1], "git_closure_custom_formats")

    def test_smoke_profile_straddles_one_publication_batch(self):
        from unittest import mock
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            write_probe(probe)
            output = root / "smoke.json"
            with mock.patch.object(suite, "build_probe", side_effect=AssertionError("must not build")):
                self.assertEqual(suite.main(["--profile", "smoke", "--probe-binary", str(probe),
                                             "--no-build", "--output", str(output)]), 0)
            report = json.loads(output.read_text())
            self.assertTrue(report["complete"])
            self.assertEqual(report["configuration"]["commits"], [16, 64])
            self.assertEqual(report["configuration"]["publication_batch_objects"], [64, 4096])
            # 48 objects fit one 64-object witness batch; 192 objects need three.
            self.assertEqual(len(report["samples"]), 2 * 2 * 2)
            custom = [row for row in report["samples"] if row["registry"] == "custom"]
            self.assertTrue(all(row["link_audits_per_object"] == 1 for row in custom))

    def test_rejects_custom_registries_that_skip_audits_and_builtins_that_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            for audits, registry in [("objects - 1", "custom"), ("objects", "custom")]:
                write_probe(probe, audits)
                arguments = ["--probe-binary", str(probe), "--no-build", "--output", str(root / "r.json"),
                             "--commits", "4", "--registry", registry,
                             "--publication-batch-objects", "64"]
                if audits == "objects":
                    self.assertEqual(suite.main(arguments), 0)
                else:
                    with self.assertRaisesRegex(suite.common.BenchmarkError, "audit"):
                        suite.main(arguments)
            row = dict(commits=4, registry="builtin", publication_batch_objects=64,
                       objects=12, imported_objects=12, reused_objects=0, warm_imported_objects=0,
                       warm_source_bytes=0, link_audits=1, root="r", wall_nanos=1,
                       correctness=suite.CORRECTNESS)
            with self.assertRaisesRegex(suite.common.BenchmarkError, "audit"):
                suite.check_row(row, 4, "builtin", 64)
            row["link_audits"] = 0
            suite.check_row(row, 4, "builtin", 64)
            for field, value in [("warm_source_bytes", 1), ("imported_objects", 11), ("root", "")]:
                with self.subTest(field=field):
                    with self.assertRaises(suite.common.BenchmarkError):
                        suite.check_row({**row, field: value}, 4, "builtin", 64)

    def test_paired_runs_alternate_order_and_summarize_deterministic_audit_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe, baseline = root / "probe", root / "baseline"
            write_probe(probe)
            write_probe(baseline, "objects * objects")
            output = root / "paired.json"
            self.assertEqual(suite.main(["--probe-binary", str(probe), "--baseline-binary", str(baseline),
                                         "--no-build", "--output", str(output), "--commits", "4",
                                         "--registry", "custom",
                                         "--publication-batch-objects", "64", "--repetitions", "2"]), 0)
            report = json.loads(output.read_text())
            self.assertEqual([p["variant"] for p in report["processes"]],
                             ["baseline", "candidate", "candidate", "baseline"])
            summary, = report["paired_summary"]
            self.assertEqual((summary["baseline_link_audits"], summary["candidate_link_audits"]), (144, 12))
            self.assertEqual(summary["pairs"], 2)
            self.assertFalse(summary["enough_samples"])
            rows = report["samples"]
            rows[-1]["root"] = "different root"
            with self.assertRaisesRegex(suite.common.BenchmarkError, "root"):
                suite.summarize_pairs(rows)


if __name__ == "__main__":
    unittest.main()

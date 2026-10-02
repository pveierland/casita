import importlib
import unittest


class WorkerMetricTests(unittest.TestCase):
    def test_worker_metrics_reject_missing_or_impossible_observations(self):
        suite = importlib.import_module('benchmarks.suites.git_closure_import')
        validate = getattr(suite, 'validate_worker_metrics', None)
        self.assertTrue(callable(validate), 'worker matrix needs validated import-only observations')
        row = dict(operation='cold', parent_hwm_before_import_bytes=1024,
                   parent_hwm_after_import_bytes=2048,
                   import_process_cpu=dict(user_ticks=2, system_ticks=1),
                   peak_decode_workers=2, peak_source_bytes=4096)
        validate(row, 4)
        # Observed Linux per-CPU RSS accounting can make VmHWM decrease.
        validate(row | dict(parent_hwm_before_import_bytes=8359936,
                            parent_hwm_after_import_bytes=8298496), 4)
        for update in [dict(parent_hwm_before_import_bytes=None),
                       dict(parent_hwm_after_import_bytes=0),
                       dict(import_process_cpu=None),
                       dict(import_process_cpu=dict(user_ticks=True, system_ticks=1)),
                       dict(peak_decode_workers=5), dict(peak_source_bytes=-1),
                       dict(peak_decode_workers=None), dict(operation='warm')]:
            with self.subTest(update=update), self.assertRaises(suite.common.BenchmarkError):
                validate(row | update, 4)
        baseline = row | dict(peak_decode_workers=None, peak_source_bytes=None)
        validate(baseline, 1)
        with self.assertRaises(suite.common.BenchmarkError):
            validate(baseline, 2)
        validate(row | dict(operation='warm', peak_decode_workers=0, peak_source_bytes=0), 4)

    def test_matrix_builds_its_bounded_fixture_probe(self):
        from benchmarks import all as runner, revisions
        self.assertIn('git-worker-matrix', revisions.SUITE_BUILD_SPECS)
        commands = runner.build_commands(['git-worker-matrix'], __import__('pathlib').Path('/build'))
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0][commands[0].index('--test') + 1], 'git_worker_matrix')
        self.assertIn('/bins/git_worker_matrix', runner.suite_arguments(
            'git-worker-matrix', __import__('pathlib').Path('/bins'), 'smoke', 1))

    def test_matrix_runs_and_rejects_missing_metrics_or_failed_audits(self):
        import json
        import pathlib
        import sys
        import tempfile
        from benchmarks.suites import git_worker_matrix as suite
        from benchmarks.suites.repository import BenchmarkError
        from benchmarks.tests.test_git_retained_buffers import PROBE
        valid = PROBE.replace("parent_hwm_after_import_bytes=4096", """parent_hwm_after_import_bytes=4096,
            parent_hwm_before_import_bytes=2048,
            import_process_cpu=dict(user_ticks=0, system_ticks=1),
            peak_decode_workers=0 if operation == 'warm' else 1,
            peak_source_bytes=0 if operation == 'warm' else 16""")
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            probe, output = path / 'probe', path / 'out.json'
            args = ['--profile', 'smoke', '--probe-binary', str(probe), '--no-build',
                    '--backend', 'memory', '--layout', 'loose', '--output', str(output)]
            probe.write_text('#!' + sys.executable + '\n' + valid)
            probe.chmod(0o755)
            self.assertEqual(suite.main(args), 0)
            report = json.loads(output.read_text())
            self.assertTrue(report['complete'])
            self.assertEqual({r['decode_workers'] for r in report['samples']}, {1, 2, 4, 8})
            self.assertEqual({r['max_buffered_bytes'] for r in report['samples']}, {131071, 131072, 131073})
            self.assertTrue(report['configuration']['worker_metrics'])
            for invalid in [valid.replace('parent_hwm_before_import_bytes=2048', 'parent_hwm_before_import_bytes=None'),
                            valid.replace('import_process_cpu=dict(user_ticks=0, system_ticks=1)', 'import_process_cpu=None'),
                            valid.replace('independent BLAKE3 and exact streaming readback', 'unchecked'),
                            valid.replace('1 passed; 0 failed;', '0 passed; 1 failed;')]:
                probe.write_text('#!' + sys.executable + '\n' + invalid)
                with self.subTest(invalid=invalid), self.assertRaises(BenchmarkError):
                    suite.main(args)
                self.assertFalse(json.loads(output.read_text())['complete'])

    def test_matrix_auto_build_selects_its_probe(self):
        from unittest import mock
        from benchmarks.suites import git_worker_matrix, git_closure_import
        with mock.patch.object(git_closure_import.subprocess, 'run', side_effect=RuntimeError('build reached')) as build:
            with self.assertRaisesRegex(RuntimeError, 'build reached'):
                git_worker_matrix.main(['--output', '/tmp/unused-worker-test.json'])
        command = build.call_args.args[0]
        self.assertEqual(command[command.index('--test') + 1], 'git_worker_matrix')
        self.assertNotIn('--locked', command)

    def test_summary_preserves_paired_effects_and_rejects_incomplete_pairs(self):
        from benchmarks.suites import git_worker_matrix as suite
        from benchmarks.suites.repository import BenchmarkError
        summarize = getattr(suite, 'summarize_metrics', None)
        self.assertTrue(callable(summarize))
        rows = []
        for i, (before, after) in enumerate([(1, 2), (10, 1), (100, 101)]):
            for variant, seconds, hwm in [('baseline', before, 1024*1024), ('candidate', after, 2*1024*1024)]:
                rows.append(dict(operation='cold', backend='memory', files=16, file_bytes=1024,
                    content='random', packed=False, concurrency=16, max_buffered_bytes=65536,
                    requested_decode_workers=2, decode_workers=1 if variant == 'baseline' else 2,
                    repetition=i, variant=variant, wall_seconds=seconds, root='identical',
                    parent_hwm_before_import_bytes=1024, parent_hwm_after_import_bytes=hwm,
                    import_process_cpu=dict(user_ticks=2, system_ticks=0),
                    peak_decode_workers=1, peak_source_bytes=16384))
        report = dict(complete=True, samples=rows, configuration=dict(repetitions=3, clock_ticks_per_second=100))
        result, = summarize(report)
        self.assertAlmostEqual(result['median_paired_reduction_percent'], -1)
        self.assertEqual(result['candidate_hwm_after_median_mib'], 2)
        self.assertEqual(result['baseline_cpu_median_seconds'], .02)
        self.assertFalse(result['enough_samples'])
        rows.pop()
        with self.assertRaises(BenchmarkError):
            summarize(report)

    def test_freezer_rejects_a_cached_library_even_with_a_fresh_probe(self):
        import json
        import pathlib
        import subprocess
        import sys
        import tempfile
        report = pathlib.Path(__file__).resolve().parents[1] / 'reports/2026-10-02-git-worker-matrix'
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            log = root / 'cargo.jsonl'
            log.write_text('\n'.join(json.dumps(row) for row in [
                dict(reason='build-finished', success=True),
                dict(reason='compiler-artifact', fresh=True, executable=None,
                     target=dict(name='casita', kind=['lib'], src_path=str(root/'crates/casita/src/lib.rs'))),
                dict(reason='compiler-artifact', fresh=False, executable=str(root/'not-built'),
                     target=dict(name='git_worker_matrix', src_path=str(root/'crates/casita/tests/git_worker_matrix.rs'))),
            ]))
            result = subprocess.run([sys.executable, str(report/'freeze-probe.py'), str(root), str(log), str(root/'frozen')],
                                    capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('fresh library', result.stderr)
            self.assertFalse((root/'frozen').exists())

    def test_all_registers_mixed_and_delta_worker_workloads(self):
        from unittest import mock
        from benchmarks import cli, revisions
        from benchmarks.suites import git_worker_matrix
        entries = {entry['id']: entry for entry in cli.entrypoints()}
        for identifier, content in [('git-worker-matrix-mixed', 'mixed'), ('git-worker-matrix-delta', 'clustered')]:
            self.assertIn(identifier, entries)
            self.assertEqual(revisions.SUITE_BUILD_SPECS[identifier].artifact_name, 'git_worker_matrix')
            with mock.patch.object(git_worker_matrix, 'run', return_value=0) as run:
                self.assertEqual(cli.run_entrypoint(entries[identifier], ['--output', '/tmp/unused-worker-test.json']), 0)
            args = run.call_args.args[0]
            self.assertEqual(args[max(i for i, arg in enumerate(args) if arg == '--content')+1], content)
            self.assertIn('1,2,4,8', args)

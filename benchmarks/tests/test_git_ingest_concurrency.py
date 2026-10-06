import json
import pathlib
import subprocess
import tempfile
import unittest

from benchmarks import all as runner
from benchmarks import cli
from benchmarks.suites import git_ingest_scheduling as suite
from benchmarks.suites import git_ingest_concurrency as native
from benchmarks.suites.git import git_env
from benchmarks.suites import repository as common


class GitIngestTests(unittest.TestCase):
    def test_explicit_blob_size_and_exact_reachable_byte_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            sources = native.fixture(pathlib.Path(directory), 2, 'packed', file_bytes=8193)
            prior = set()
            for source, _, _, _, count, inventory in sources:
                objects = inventory['objects']
                self.assertEqual(len(objects), count)
                blobs = [row for row in objects.values() if row['kind'] == 'blob']
                self.assertTrue(blobs)
                self.assertTrue(all(row['bytes'] == 8193 for row in blobs))
                total = 0
                for oid, row in objects.items():
                    body = subprocess.check_output(['git', f'--git-dir={source}', 'cat-file', row['kind'], oid], env=git_env())
                    self.assertEqual(len(body), row['bytes'])
                    total += len(body)
                self.assertEqual(inventory['reachable_bytes'], total)
                self.assertEqual(inventory['new_bytes'], sum(row['bytes'] for oid, row in objects.items() if oid not in prior))
                prior = set(objects)

    def test_probe_requires_both_matching_imports_and_a_passing_test(self):
        base = dict(files=17, concurrency=16, max_buffered_bytes=65536, delay_ms=5, packed=True,
                    root='git.view.v1:example', wall_nanos=1, peak_active=16, peak_bytes=65536,
                    correctness=suite.CORRECTNESS)
        rows = [{**base, 'operation': operation} for operation in ('initial-import', 'incremental-import')]

        def parse(values, footer='test result: ok. 1 passed; 0 failed;'):
            return suite.parse_samples('\n'.join('git_ingest_sample ' + json.dumps(row) for row in values) + '\n' + footer,
                                       17, 16, 65536, 5, True)

        self.assertEqual(parse(rows), rows)
        for field, value in (('files', 16), ('concurrency', 1), ('max_buffered_bytes', 1),
                             ('delay_ms', 0), ('packed', 1), ('root', ''), ('operation', 'initial-import'),
                             ('wall_nanos', True), ('peak_active', 17), ('peak_bytes', 0), ('correctness', '')):
            with self.subTest(field=field), self.assertRaises(common.BenchmarkError):
                parse([rows[0], {**rows[1], field: value}])
        with self.assertRaises(common.BenchmarkError):
            parse(rows[:1])
        with self.assertRaises(common.BenchmarkError):
            parse(rows, 'test result: ok. 0 passed; 0 failed;')

    def test_all_supplies_binaries_for_both_registered_suites(self):
        for name, binary in (('git-ingest-concurrency', 'casita'), ('git-ingest-scheduling', 'casita-lib-test')):
            entry = next(entry for entry in cli.entrypoints() if entry['id'] == name)
            self.assertEqual(entry['suite_id'], 'native-git')
            args = runner.suite_arguments(name, pathlib.Path('/binaries'), 'smoke', 1)
            self.assertIn('/binaries/' + binary, args)
            self.assertIn('--no-build', args)

    def test_source_window_cases_use_existing_probes_and_explicit_workloads(self):
        for path, binary in (('closure', 'git_closure_import'), ('view', 'casita')):
            for boundary in ('32', '128', 'oversized-32', 'oversized-128'):
                name = f'git-{path}-source-window-{boundary}'
                entry = next(entry for entry in cli.entrypoints() if entry['id'] == name)
                args = runner.suite_arguments(name, pathlib.Path('/binaries'), 'smoke', 1)
                self.assertIn('/binaries/' + binary, args)
                self.assertIn('--no-build', args)
                self.assertIn(name, runner.SMOKE)
                workload = entry['default_arguments']
                self.assertIn('--file-bytes', workload)
                self.assertIn('--max-buffered-bytes', workload)
                counts = workload[workload.index('--counts') + 1]
                if boundary.startswith('oversized'):
                    self.assertEqual(counts, '2')
                if path == 'view' and boundary == '128':
                    self.assertIn('2046', counts.split(','))

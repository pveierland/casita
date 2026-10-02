import json
import pathlib
import sys
import tempfile
import unittest

from benchmarks.suites import git_closure_import as suite
from benchmarks.tests.test_git_shared_cpu import PROBE as CPU_PROBE

PROBE = CPU_PROBE.replace('wall_nanos=1000000,', '''
source_buffer_capacity=1048576, destination_buffer_capacity=5242880,
peak_source_buffer_bytes=0 if operation == "warm" else 1048576,
peak_destination_buffer_bytes=0 if operation == "warm" else 4718592,
reserved_source_buffer_bytes=0, reserved_destination_buffer_bytes=0,
wall_nanos=1000000,''')


class SharedBufferTests(unittest.TestCase):
    def test_partition_counters_and_recovery_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / 'probe'
            binary.write_text('#!' + sys.executable + '\n' + PROBE)
            binary.chmod(0o755)
            args = ['--probe-binary', str(binary), '--no-build', '--counts', '4',
                '--max-buffered-bytes', '67108864', '--file-bytes', '1024',
                '--decode-workers', '1', '--concurrency', '16', '--backend', 'local',
                '--layout', 'packed', '--content', 'random', '--imports', '2',
                '--shared-cpu-limit', '1', '--buffer-budget', '1048576:5242880',
                '--output', str(root / 'result.json')]
            try:
                status = suite.main(args)
            except SystemExit as error:
                status = error.code
            self.assertEqual(status, 0, "runner must accept explicit source/destination partitions")
            for before, after in [
                ('source_buffer_capacity=1048576', 'source_buffer_capacity=1048577'),
                ('destination_buffer_capacity=5242880', 'destination_buffer_capacity=0'),
                ('peak_source_buffer_bytes=0 if operation == "warm" else 1048576', 'peak_source_buffer_bytes=1048577'),
                ('peak_destination_buffer_bytes=0 if operation == "warm" else 4718592', 'peak_destination_buffer_bytes=5242881'),
                ('peak_source_buffer_bytes=0 if operation == "warm" else 1048576', 'peak_source_buffer_bytes=0'),
                ('peak_destination_buffer_bytes=0 if operation == "warm" else 4718592', 'peak_destination_buffer_bytes=True'),
                ('reserved_source_buffer_bytes=0', 'reserved_source_buffer_bytes=65536'),
                ('reserved_destination_buffer_bytes=0', 'reserved_destination_buffer_bytes=65536'),
            ]:
                binary.write_text('#!' + sys.executable + '\n' + PROBE.replace(before, after))
                with self.assertRaises(suite.common.BenchmarkError):
                    suite.main(args)
                self.assertFalse(json.loads((root / 'result.json').read_text())['complete'])

    def test_paired_main_keeps_buffer_and_chunk_dimensions_separate(self):
        probe = CPU_PROBE.replace('wall_nanos=1000000,', """
source_buffer_capacity=int(os.environ['CASITA_GIT_CLOSURE_SOURCE_BUFFER_BYTES']),
destination_buffer_capacity=int(os.environ['CASITA_GIT_CLOSURE_DESTINATION_BUFFER_BYTES']),
peak_source_buffer_bytes=0 if operation == "warm" else int(os.environ['CASITA_GIT_CLOSURE_SOURCE_BUFFER_BYTES']),
peak_destination_buffer_bytes=0 if operation == "warm" else int(os.environ['CASITA_GIT_CLOSURE_DESTINATION_BUFFER_BYTES']),
reserved_source_buffer_bytes=0, reserved_destination_buffer_bytes=0,
chunk_upload_concurrency=int(os.environ['CASITA_GIT_CLOSURE_CHUNK_CONCURRENCY']),
wall_nanos=1000000,""")
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / 'probe'
            binary.write_text('#!' + sys.executable + '\n' + probe)
            binary.chmod(0o755)
            self.assertEqual(suite.main([
                '--probe-binary', str(binary), '--baseline-binary', str(binary),
                '--no-build', '--counts', '4', '--max-buffered-bytes', '67108864',
                '--file-bytes', '1024', '--decode-workers', '1', '--concurrency', '16',
                '--backend', 'local', '--layout', 'packed', '--content', 'random',
                '--imports', '2', '--shared-cpu-limit', '1', '--baseline-shared-cpu-limit', '1',
                '--buffer-budget', '1048576:5242880,2097152:6291456',
                '--chunk-upload-concurrency', '1,4', '--baseline-chunk-upload-concurrency', '2',
                '--repetitions', '2', '--output', str(root / 'paired.json')]), 0)
            result = json.loads((root / 'paired.json').read_text())
            self.assertTrue(result['complete'])
            self.assertEqual(len(result['samples']), 64)
            self.assertEqual({row['repetition'] for row in result['samples']}, {0, 1})
            dimensions = {(row['requested_source_buffer_bytes'],
                           row['requested_destination_buffer_bytes'],
                           row['requested_chunk_upload_concurrency'])
                          for row in result['samples']}
            self.assertEqual(dimensions, {(source, destination, chunks)
                for source, destination in [(1048576, 5242880), (2097152, 6291456)]
                for chunks in [1, 4]})
            for row in result['samples']:
                baseline = row['variant'] == 'baseline'
                self.assertEqual(row['source_buffer_capacity'],
                    0 if baseline else row['requested_source_buffer_bytes'])
                self.assertEqual(row['destination_buffer_capacity'],
                    0 if baseline else row['requested_destination_buffer_bytes'])
                self.assertEqual(row['chunk_upload_concurrency'],
                    2 if baseline else row['requested_chunk_upload_concurrency'])
            self.assertEqual(len(result['paired_summary']), 16)
            self.assertTrue(all(row['pairs'] == 2 for row in result['paired_summary']))

    def test_corpus_registration(self):
        import importlib.util
        self.assertIsNotNone(importlib.util.find_spec('benchmarks.suites.git_shared_buffers'), "shared buffer suite must be permanent")
        from benchmarks.suites.git_shared_buffers import main
        from benchmarks import all as runner, revisions
        self.assertTrue(callable(main))
        command = runner.build_commands(['git-shared-buffers'], pathlib.Path('/build'))[0]
        self.assertEqual(command[command.index('--test') + 1], 'git_closure_import')
        self.assertIn('/binaries/git_closure_import', runner.suite_arguments('git-shared-buffers', pathlib.Path('/binaries'), 'smoke', 1))
        self.assertEqual(revisions.SUITE_BUILD_SPECS['git-shared-buffers'].cargo_json_test, 'git_closure_import')

    def test_boundary_gate_accepts_serial_diagnostic_without_weakening_identity(self):
        from benchmarks.suites.git_shared_buffer_limits import CASES, validate_case
        for name, diagnostic in [('writer-envelope', 'writer_minimum=10944512'),
                                 ('source-envelope', 'source_minimum=589824')]:
            output = ('test ' + CASES[name][0] + ' ... buffer_boundary ' + diagnostic
                      + '\nok\n\ntest result: ok. 1 passed; 0 failed;')
            validate_case(output, name)
            for invalid in [output.replace(CASES[name][0], 'some_other_test'),
                            output.replace('1 passed', '0 passed'),
                            output.replace('\nok\n', '\nFAILED\n'),
                            output.replace('buffer_boundary ', 'unrecognized diagnostic ')]:
                with self.assertRaises(suite.common.BenchmarkError):
                    validate_case(invalid, name)

    def test_boundary_corpus_is_registered_and_checks_passed_tests(self):
        import importlib.util
        self.assertIsNotNone(importlib.util.find_spec('benchmarks.suites.git_shared_buffer_limits'))
        from benchmarks.suites.git_shared_buffer_limits import CASES, validate_case
        from benchmarks import all as runner, revisions
        name = 'writer-envelope'
        with self.assertRaises(suite.common.BenchmarkError):
            validate_case('test result: ok. 0 passed; 0 failed;', name)
        validate_case(CASES[name][0] + ' ... ok\ntest result: ok. 1 passed; 0 failed;', name)
        command = runner.build_commands(['git-shared-buffer-limits'], pathlib.Path('/build'))[0]
        self.assertEqual(command[command.index('--test') + 1], 'git_import_buffers')
        self.assertEqual(revisions.SUITE_BUILD_SPECS['git-shared-buffer-limits'].cargo_json_test, 'git_import_buffers')

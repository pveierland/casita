import contextlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from benchmarks.suites import server_ingest as suite


class ServerPathTests(unittest.TestCase):
    def check_output(self, output, *, reject, repetitions=1, mode='sustained', pressure_case='both'):
        args = ['--configuration', '/unused/config.json', '--output', str(output),
                '--mode', mode, '--repetitions', str(repetitions), '--pressure-case', pressure_case]
        stderr = io.StringIO()
        with mock.patch.object(suite, 'load_configuration',
                               side_effect=RuntimeError('configuration reached')) as load, \
             contextlib.redirect_stderr(stderr):
            if reject:
                with self.assertRaises(SystemExit) as error:
                    suite.main(args)
                self.assertEqual(error.exception.code, 2)
                self.assertIn('shorter --output', stderr.getvalue())
                load.assert_not_called()
            else:
                with self.assertRaisesRegex(RuntimeError, 'configuration reached'):
                    suite.main(args)
                load.assert_called_once()
        self.assertFalse(output.exists())
        self.assertFalse(output.with_name(output.stem + '-artifacts').exists())

    @staticmethod
    def socket_path(output, case='00-sustained', epoch='reopened'):
        return output.with_name(output.stem + '-artifacts') / case / ('server-' + epoch + '.sock')

    def boundary_output(self, root, length):
        output = root / 'r.json'
        padding = length - len(os.fsencode(self.socket_path(output)))
        self.assertGreaterEqual(padding, 0)
        output = root / ('r' + 'x' * padding + '.json')
        self.assertEqual(len(os.fsencode(self.socket_path(output))), length)
        return output

    def test_reopened_socket_boundary_is_checked_before_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.check_output(self.boundary_output(root, 103), reject=False)
            output = self.boundary_output(root, 104)
            self.assertLess(len(os.fsencode(self.socket_path(output, epoch='first'))), 104)
            self.check_output(output, reject=True)

    def test_later_repetition_name_must_also_fit(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = self.boundary_output(Path(temporary), 103)
            self.check_output(output, reject=False, repetitions=100)
            self.check_output(output, reject=True, repetitions=101)

    def test_limit_counts_filesystem_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = self.boundary_output(Path(temporary), 103)
            output = output.with_name(output.name.replace('xx', 'éé', 1))
            self.assertLess(len(str(self.socket_path(output))), 104)
            self.assertGreaterEqual(len(os.fsencode(self.socket_path(output))), 104)
            self.check_output(output, reject=True)

    def test_long_pressure_path_is_rejected_before_any_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            self.check_output(Path(temporary) / ('x' * 90) / 'benchmark.json',
                              reject=True, mode='pressure')

    def test_selected_pressure_case_checks_only_its_actual_socket(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            base = root / 'r.json'
            padding = 103 - len(os.fsencode(self.socket_path(base, case='00-aged', epoch='first')))
            output = root / ('r' + 'x' * padding + '.json')
            self.check_output(output, reject=False, mode='pressure', pressure_case='aged')
            self.check_output(output, reject=True, mode='pressure')

    def test_pressure_selection_requires_pressure_mode(self):
        with mock.patch.object(suite, 'load_configuration') as load, \
             contextlib.redirect_stderr(io.StringIO()) as stderr:
            with self.assertRaises(SystemExit) as error:
                suite.main(['--configuration', '/unused', '--output', '/tmp/unused.json',
                            '--mode', 'sustained', '--pressure-case', 'aged'])
            self.assertEqual(error.exception.code, 2)
            self.assertIn('requires --mode pressure', stderr.getvalue())
            load.assert_not_called()


if __name__ == '__main__':
    unittest.main()

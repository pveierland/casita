import pathlib
import subprocess
import os
import signal
import sys
import time
import json
import base64
import tempfile
import unittest
from unittest import mock

from benchmarks import all as runner, cli
from benchmarks.suites import server_ingest as suite
from benchmarks.suites._server_ingest_process import OwnedFamily


class ServerIngestTests(unittest.TestCase):
    def test_configuration_rejects_changed_binary_and_read_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            for name in ('build', 'corpus', 'reads'):
                (root/name).mkdir()
            binary=root/'build/mnos-eval'; binary.write_bytes(b'qualified binary')
            observer=root/'observer'; observer.write_bytes(b'qualified observer')
            git=root/'git'; git.write_bytes(b'qualified git')
            def save(path, value):
                path.write_text(json.dumps(value))
            build=root/'build/mnos-eval.build.json'
            save(build, dict(features=[], default_features=True, profile='release',
                             executable_sha256=suite.sha(binary), reference_libraries={}))
            save(root/'build/result.json', dict(success=True, source_unchanged=True,
                 interrupted=False, executable_sha256=suite.sha(binary), manifest_sha256=suite.sha(build)))
            value=dict(base64=base64.b64encode(b'content').decode(), text='content', bytes=7,
                       sha256=suite.hashlib.sha256(b'content').hexdigest())
            rows=[dict(revision=str(i), git_tree=str(i), nar_hash=str(i),
                       reads={name:dict(value) for name in suite.PATHS}) for i in range(64)]
            save(root/'corpus/fixtures.json', dict(revisions=rows, repository=str(root),
                 input_sha256={str(git):suite.sha(git)}))
            save(root/'reads/fixtures.json', dict(revisions=rows))
            for name in ('corpus', 'reads'):
                save(root/name/'result.json', dict(success=True, interrupted=False, input_sha256={}))
            config=root/'config.json'
            save(config, dict(server_build=str(root/'build'), observer_binary=str(observer),
                 observer_sha256=suite.sha(observer), corpus_dir=str(root/'corpus'), reads_dir=str(root/'reads')))
            with mock.patch.object(suite.shutil, 'which', return_value=str(git)):
                suite.load_configuration(config)
                binary.write_bytes(b'changed binary')
                with self.assertRaises(AssertionError):
                    suite.load_configuration(config)
                binary.write_bytes(b'qualified binary')
                rows[0]['reads']['.version']['text']='wrong bytes'
                save(root/'reads/fixtures.json', dict(revisions=rows))
                with self.assertRaises(AssertionError):
                    suite.load_configuration(config)

    def test_collection_control_rejects_missing_or_wrong_case_coverage(self):
        before = dict(device=1, inode=2, mtime_ns=10)
        collected = dict(before, mtime_ns=150)
        suite.validate_pressure_transition('recent', before, before, 100, 200)
        suite.validate_pressure_transition('aged', before, collected, 100, 200)
        for mode, after in [('recent', collected), ('aged', before),
                            ('aged', dict(before, inode=3, mtime_ns=150)),
                            ('aged', dict(before, mtime_ns=201))]:
            with self.subTest(mode=mode, after=after), self.assertRaises(AssertionError):
                suite.validate_pressure_transition(mode, before, after, 100, 200)

    def test_external_cases_are_registered_and_need_no_casita_build(self):
        entries = {entry['id']: entry for entry in cli.entrypoints()}
        for mode in ('sustained', 'pressure'):
            identifier = 'server-ingest-' + mode
            self.assertEqual(entries[identifier]['default_arguments'], ['--mode', mode])
            self.assertEqual(runner.build_commands([identifier], pathlib.Path('/unused')), [])
            self.assertEqual(runner.suite_arguments(identifier, pathlib.Path('/unused'), 'smoke', 2),
                             ['--profile', 'smoke', '--repetitions', '2'])

    def test_timeout_reaps_separate_session_child_of_stopped_driver(self):
        self.check_owned_cleanup('timeout')

    def test_interrupt_reaps_separate_session_child(self):
        self.check_owned_cleanup('interrupt')

    def check_owned_cleanup(self, mode):
        family = OwnedFamily()
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            driver = root/'driver.py'
            # Both driver and child ignore termination; driver is then stopped.
            # Process-group-only cleanup cannot pass this regression.
            driver.write_text('''import os, signal, subprocess, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
p = subprocess.Popen([sys.executable, '-c', 'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)'], start_new_session=True)
open(sys.argv[1], 'w').write(str(p.pid))
os.kill(os.getpid(), signal.SIGSTOP)
''')
            wrapper = root/'wrapper.py'
            wrapper.write_text('''import pathlib,sys
from benchmarks import all as runner
runner.execute([sys.executable, sys.argv[1], sys.argv[2]], pathlib.Path(sys.argv[3]), float(sys.argv[4]), cleanup_timeout=.1)
''')
            process = subprocess.Popen([sys.executable, str(wrapper), str(driver),
                str(root/'pid'), str(root/'log'), '.7' if mode=='timeout' else '60'],
                env={**os.environ, 'PYTHONPATH': str(cli.ROOT)},
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            child_pid = None
            try:
                deadline = time.monotonic()+10
                while not (root/'pid').exists():
                    self.assertIsNone(process.poll())
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(.02)
                child_pid = int((root/'pid').read_text())
                if mode=='interrupt':
                    process.send_signal(signal.SIGTERM)
                code = process.wait(timeout=12)
                self.assertEqual(code, 143 if mode=='interrupt' else 0)
                self.assertFalse(pathlib.Path(f'/proc/{child_pid}').exists(), 'orphan child survived')
            finally:
                family.finish(process.poll, lambda: process.poll() is not None, grace=.1)
                if process.poll() is None:
                    process.kill(); process.wait()
                if child_pid is not None:
                    try:
                        os.kill(child_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass


if __name__ == '__main__':
    unittest.main()

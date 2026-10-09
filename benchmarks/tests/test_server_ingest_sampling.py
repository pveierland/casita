"""Procfs sampling races must not hide unreadable live descendants."""
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from benchmarks.suites import _server_ingest_runtime as runtime


class ServerSamplingTests(unittest.TestCase):
    def child(self, *, readable=True):
        setup = '' if readable else 'import ctypes; assert ctypes.CDLL(None).prctl(4, 0, 0, 0, 0) == 0; '
        process = subprocess.Popen(
            [sys.executable, '-c', setup +
             "import sys; print('ready', flush=True); sys.stdin.buffer.read(1)"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        self.addCleanup(self.stop, process)
        self.assertEqual(process.stdout.readline(), b'ready\n')
        return process

    @staticmethod
    def stop(process):
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=5)
        process.stdin.close()
        process.stdout.close()

    def sampler(self, root, *processes):
        seen = {process.pid: runtime.identity(process.pid)[0] for process in processes}
        events = []
        state = SimpleNamespace(
            out=root, family=SimpleNamespace(seen=seen, members=lambda: list(seen)),
            reap_natural=lambda: None, identities={}, observer=None, started=time.monotonic(),
            phase='test', resources=io.StringIO(), latest=None, last_stamp=None, samples=0,
            max_rss=0, max_wal=0, max_backlog=0, logs=[],
            event=lambda name, **fields: events.append(dict(event=name, **fields)))
        return state, events

    def exit_unreaped(self, process):
        process.stdin.write(b'x')
        process.stdin.flush()
        os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOWAIT)
        self.assertEqual(runtime.identity(process.pid)[1], 'Z')

    def test_exit_during_sample_keeps_remaining_measurement_and_records_gap(self):
        process = self.child()
        live = self.child()
        with tempfile.TemporaryDirectory() as temporary:
            state, events = self.sampler(Path(temporary), process, live)
            read_text = Path.read_text
            trigger = Path(f'/proc/{process.pid}/status')

            def exit_before_status(path, *args, **kwargs):
                if path == trigger:
                    self.exit_unreaped(process)
                return read_text(path, *args, **kwargs)

            with mock.patch.object(Path, 'read_text', exit_before_status):
                runtime.Runtime.sample(state)
            self.assertEqual(state.samples, 1)
            self.assertEqual([row['pid'] for row in state.latest['members']], [live.pid])
            self.assertGreater(state.latest['family_rss_bytes'], 0)
            self.assertIn('write_bytes', state.latest['members'][0]['io'])
            gaps = [e for e in events if e['event'] == 'process-sample-unavailable']
            self.assertEqual(len(gaps), 1)
            self.assertEqual(gaps[0]['pid'], process.pid)
            self.assertEqual(gaps[0]['reason'], 'exited')
            self.assertEqual(gaps[0]['after'][1], 'Z')

    def test_unreadable_live_process_still_fails(self):
        process = self.child(readable=False)
        with tempfile.TemporaryDirectory() as temporary:
            state, events = self.sampler(Path(temporary), process)
            with self.assertRaises(PermissionError):
                runtime.Runtime.sample(state)
            self.assertIsNone(process.poll())
            self.assertEqual(state.samples, 0)
            self.assertFalse(any(e['event'] == 'process-sample-unavailable' for e in events))

    def test_permission_race_does_not_attribute_a_missing_or_reused_pid(self):
        process = self.child()
        initial = runtime.identity(process.pid)
        for after in (None, (initial[0] + 1, 'S', initial[2])):
            with self.subTest(after=after), tempfile.TemporaryDirectory() as temporary:
                state, events = self.sampler(Path(temporary), process)
                read_text = Path.read_text
                target = Path(f'/proc/{process.pid}/io')

                def inaccessible(path, *args, **kwargs):
                    if path == target:
                        raise PermissionError(13, 'permission denied', str(path))
                    return read_text(path, *args, **kwargs)

                with mock.patch.object(Path, 'read_text', inaccessible), \
                     mock.patch.object(runtime, 'identity', side_effect=[initial, after]):
                    runtime.Runtime.sample(state)
                self.assertEqual(state.latest['members'], [])
                gaps = [e for e in events if e['event'] == 'process-sample-unavailable']
                self.assertEqual(len(gaps), 1)
                self.assertEqual(gaps[0]['after'], after)
                self.assertEqual(gaps[0]['reason'], 'exited' if after is None else 'reused')


if __name__ == '__main__':
    unittest.main()

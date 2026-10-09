"""Linux ownership tracking for the benchmark's evaluator and sandbox children."""
import ctypes
import os
from pathlib import Path
import signal
import subprocess
import time


def execute_owned(command, log, timeout, environment, cleanup_timeout):
    """Contain a driver and descendants even if they create separate sessions."""
    family = OwnedFamily()
    previous = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    interrupted = []
    for signum in previous:
        signal.signal(signum, lambda number, frame: interrupted.append(number))
    started = time.monotonic()
    process = None
    status, code = 'failed', None
    try:
        with log.open('w') as handle:
            process = subprocess.Popen(command, stdout=handle, stderr=subprocess.STDOUT,
                                       env=environment, start_new_session=True)
            while process.poll() is None and not interrupted and time.monotonic()-started < timeout:
                family.scan()
                time.sleep(.1)
            if interrupted or process.poll() is None:
                status = 'interrupted' if interrupted else 'timeout'
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
                deadline = time.monotonic()+cleanup_timeout
                while process.poll() is None and time.monotonic() < deadline:
                    family.scan()
                    time.sleep(.05)
            else:
                code = process.returncode
                status = 'passed' if code == 0 else 'failed'
    finally:
        try:
            # Adoption plus pidfds covers detached sessions and a driver that
            # died before its own ownership cleanup. A survivor fails the run.
            if process is not None:
                remaining = family.scan()
                if remaining:
                    if status == 'passed':
                        status = 'failed'
                    family.finish(process.poll, lambda: process.poll() is not None, grace=.5)
                process.wait()
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
    if interrupted:
        if interrupted[0] == signal.SIGINT:
            raise KeyboardInterrupt
        raise SystemExit(128+interrupted[0])
    return dict(command=command, exit_code=code, status=status,
                elapsed_seconds=time.monotonic()-started, log=str(log))


def identity(pid):
    try:
        # comm may contain spaces or ')'; fields after its last ')' are stable.
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return int(fields[19]), fields[0], int(fields[1])
    except (FileNotFoundError, ProcessLookupError):
        return None


class OwnedFamily:
    def __init__(self):
        libc = ctypes.CDLL(None, use_errno=True)
        # Adopt orphaned children even when bwrap starts a new process session.
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            raise OSError(ctypes.get_errno(), 'cannot become a child subreaper')
        self.owner = os.getpid()
        self.owner_start = identity(self.owner)[0]
        self.seen = {}
        self.descriptors = {}
        self.reaped = []

    def parent_owned(self, pid):
        current = identity(pid)
        expected = self.owner_start if pid == self.owner else self.seen.get(pid)
        return current is not None and expected is not None and current[0] == expected

    def capture(self, parent, child):
        found = identity(child)
        if found is None:return False
        try:descriptor = os.pidfd_open(child)
        except ProcessLookupError:return False
        current = identity(child)
        owned = current is not None and current[0] == found[0] and (
            current[2] == self.owner or (current[2] == parent and self.parent_owned(parent)))
        if not owned:
            os.close(descriptor);return False
        if child in self.descriptors:os.close(self.descriptors[child])
        self.seen[child] = current[0];self.descriptors[child] = descriptor
        return True

    def scan(self):
        pending = [self.owner];visited = set()
        while pending:
            pid = pending.pop()
            if pid in visited:continue
            visited.add(pid)
            if not self.parent_owned(pid):continue
            try:tasks = list(Path(f'/proc/{pid}/task').iterdir())
            except (FileNotFoundError, ProcessLookupError):continue
            for task in tasks:
                try:children = [int(x) for x in (task/'children').read_text().split()]
                except (FileNotFoundError, ProcessLookupError):continue
                for child in children:
                    if self.capture(pid, child):pending.append(child)
        return self.members()

    def members(self):
        result = []
        for pid, start in list(self.seen.items()):
            found = identity(pid)
            if found is not None and found[0] == start:result.append(pid)
            else:
                del self.seen[pid]
                os.close(self.descriptors.pop(pid))
        return result

    def signal(self, signum):
        self.scan()
        for pid in self.members():
            # pidfds cannot target a different process after PID reuse.
            try:signal.pidfd_send_signal(self.descriptors[pid], signum)
            except ProcessLookupError:pass

    def finish(self, reap_leader, leader_done, grace=5):
        """Terminate/reap every owned child; return whether SIGKILL was needed."""
        self.signal(signal.SIGTERM)
        deadline = time.monotonic()+grace;forced = False
        while True:
            if not leader_done():reap_leader()
            if leader_done():
                while True:
                    try:pid, status, usage = os.wait4(-1, os.WNOHANG)
                    except ChildProcessError:break
                    if not pid:break
                    self.reaped.append(dict(pid=pid, exit_code=os.waitstatus_to_exitcode(status),
                        user_seconds=usage.ru_utime, system_seconds=usage.ru_stime,
                        max_rss_bytes=usage.ru_maxrss*1024, minor_faults=usage.ru_minflt,
                        major_faults=usage.ru_majflt, input_blocks=usage.ru_inblock,
                        output_blocks=usage.ru_oublock))
            if not self.scan() and leader_done():return forced
            now = time.monotonic()
            if now >= deadline:
                forced = True;self.signal(signal.SIGKILL)
            if now >= deadline+2:
                raise RuntimeError(f'owned children survived cleanup: {self.members()}')
            time.sleep(.01)

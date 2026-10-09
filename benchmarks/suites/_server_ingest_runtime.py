"""Resource/ownership and passive WAL observation for the sustained server probe."""
import hashlib
import json
import os
from pathlib import Path
import resource
import select
import signal
import subprocess
import time

from benchmarks.suites._server_ingest_process import OwnedFamily, identity


def sha(path):
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def save(path, value):
    with Path(path).open('x') as f:
        json.dump(value, f, indent=2)
        f.write('\n')


def load(path):
    return json.loads(Path(path).read_text())


class Runtime:
    def __init__(self, out, timeout, observer_binary, interrupt_state):
        self.out = out
        self.interrupt_state = interrupt_state
        self.observer_binary = observer_binary
        self.last_stamp = None
        self.started = time.monotonic()
        self.timeout = timeout
        self.family = OwnedFamily()
        self.children = []
        self.expected = set()
        self.server = None
        self.observer = None
        self.stopped = False
        self.last_sample = -1
        self.phase = 'startup'
        self.sample_busy = False
        self.natural = []
        self.identities = {}
        self.latest = None
        self.logs = [out / 'events.jsonl', out / 'resources.jsonl', out / 'wire.jsonl']
        self.events = self.logs[0].open('x')
        self.resources = self.logs[1].open('x')
        self.samples = 0
        self.max_rss = self.max_wal = self.max_backlog = 0
        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.interrupt)
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

    def interrupt(self, signum, frame):
        self.stopped = True
        self.interrupt_state['stopped'] = True

    def event(self, event, **data):
        self.events.write(json.dumps(dict(event=event, seconds=time.monotonic()-self.started,
                                         unix_ns=time.time_ns(), phase=self.phase, **data))+'\n')
        self.events.flush()

    def spawn(self, name, command, env, pipe=False):
        stdout = subprocess.PIPE if pipe else (self.out / (name+'.stdout')).open('xb')
        stderr = (self.out / (name+'.stderr')).open('xb')
        try:
            child = subprocess.Popen(command, env=env, stdin=subprocess.PIPE if pipe else subprocess.DEVNULL,
                                     stdout=stdout, stderr=stderr, bufsize=0, start_new_session=True)
        finally:
            if not pipe: stdout.close()
            stderr.close()
        self.children.append(child)
        self.logs.extend([self.out / (name+'.stderr')])
        if not pipe: self.logs.append(self.out / (name+'.stdout'))
        self.family.scan()
        self.event('spawn', name=name, command=command, pid=child.pid, identity=identity(child.pid))
        return child

    def reap_natural(self):
        explicit = {p.pid for p in self.children}
        for pid in self.family.scan():
            found = identity(pid)
            if pid in explicit or found is None or found[1:] != ('Z', os.getpid()):
                continue
            assert found[0] == self.family.seen[pid]
            comm = Path(f'/proc/{pid}/comm').read_text().strip()
            waited, status, usage = os.wait4(pid, os.WNOHANG)
            assert waited == pid
            record = dict(pid=pid, start_ticks=found[0], comm=comm, role='unclassified adopted descendant',
                          exit_code=os.waitstatus_to_exitcode(status), user_seconds=usage.ru_utime,
                          system_seconds=usage.ru_stime, max_rss_bytes=usage.ru_maxrss*1024,
                          input_blocks=usage.ru_inblock, output_blocks=usage.ru_oublock)
            self.natural.append(record)
            self.event('natural-reap', **record)
            assert record['exit_code'] == 0, record

    def guard(self):
        assert not self.stopped, 'interrupted'
        assert time.monotonic()-self.started < self.timeout, 'global watchdog'
        for p in self.children:
            if p.pid not in self.expected:
                assert p.poll() is None, ('unexpected child exit', p.pid, p.returncode)
        if self.sample_busy or time.monotonic()-self.last_sample < .5:
            return
        self.sample_busy = True
        try:
            self.last_sample = time.monotonic()
            self.sample()
        finally:
            self.sample_busy = False

    def sample(self):
        self.reap_natural()
        members = []
        for pid in self.family.members():
            before = None
            try:
                before = identity(pid)
                if before is None: continue
                assert before[0] == self.family.seen[pid]
                proc = Path(f'/proc/{pid}')
                status = dict(line.split(':', 1) for line in (proc/'status').read_text().splitlines())
                stat = (proc/'stat').read_text().rsplit(')', 1)[1].split()
                io = {k:int(v) for k,v in (line.split(':', 1) for line in (proc/'io').read_text().splitlines())}
                key = (pid, before[0])
                if key not in self.identities:
                    try: exe = os.readlink(proc/'exe')
                    except FileNotFoundError: exe = None
                    meta = dict(pid=pid, start_ticks=before[0], ppid=before[2], comm=(proc/'comm').read_text().strip(), exe=exe)
                    self.identities[key] = meta
                    self.event('process-observed', **meta)
                after = identity(pid)
                if after is None: continue
                assert after[0] == before[0]
                members.append(dict(pid=pid, start_ticks=before[0], state=after[1], ppid=after[2],
                    rss_bytes=int(status.get('VmRSS', '0').split()[0])*1024,
                    anonymous_bytes=int(status.get('RssAnon', '0').split()[0])*1024,
                    file_bytes=int(status.get('RssFile', '0').split()[0])*1024,
                    user_ticks=int(stat[11]), system_ticks=int(stat[12]),
                    waited_child_user_ticks=int(stat[13]), waited_child_system_ticks=int(stat[14]), io=io))
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError as error:
                # An exiting process can retain readable stat/status files
                # while its io file already rejects access. Recheck ownership
                # and liveness; permission failures for live owners still fail.
                after = identity(pid)
                expected = self.family.seen[pid]
                if after is not None and after[0] == expected and after[1] != 'Z':
                    raise
                self.event('process-sample-unavailable', pid=pid, start_ticks=expected,
                           before=before, after=after, path=error.filename,
                           reason='reused' if after is not None and after[0] != expected else 'exited')
                continue
        fs = os.statvfs(self.out)
        available = fs.f_bavail*fs.f_frsize
        files = {}
        for name in ('casita.sqlite', 'casita.sqlite-wal', 'casita.sqlite-tshm'):
            p = self.out/'cas'/name
            try:
                st = p.stat()
                files[name] = dict(bytes=st.st_size, allocated_bytes=st.st_blocks*512, inode=st.st_ino)
            except FileNotFoundError: files[name] = None
        rss = sum(p['rss_bytes'] for p in members)
        wal = files['casita.sqlite-wal']['bytes'] if files['casita.sqlite-wal'] else 0
        sample = dict(seconds=time.monotonic()-self.started, unix_ns=time.time_ns(), phase=self.phase,
                      members=members, family_rss_bytes=rss, available_bytes=available, files=files)
        if self.observer:
            sample['wal'] = self.observer.sample()
            if sample['wal']['status'] == 'ok':
                self.max_backlog = max(self.max_backlog, sample['wal']['backlog_frames'])
        self.resources.write(json.dumps(sample)+'\n')
        self.resources.flush()
        self.latest = sample
        stamp = self.out/'cas/.casita-pressure-collection'
        try:
            info = stamp.stat()
            state = dict(device=info.st_dev, inode=info.st_ino, mtime_ns=info.st_mtime_ns, bytes=info.st_size)
        except FileNotFoundError:
            state = None
        if state != self.last_stamp:
            self.event('pressure-stamp-observed', state=state, resource_seconds=sample['seconds'])
            self.last_stamp = state
        self.samples += 1
        self.max_rss = max(self.max_rss, rss)
        self.max_wal = max(self.max_wal, wal)
        assert rss < 4*1024**3, 'family sampled RSS limit'
        assert wal <= 1024**3, 'physical WAL limit'
        assert available >= 768*1024**2, 'disk floor'
        assert sum(p.stat().st_size for p in self.logs if p.exists()) <= 256*1024**2, 'log limit'

    def phase_to(self, name):
        self.phase = name
        self.event('phase')
        self.last_sample = -1
        self.guard()
        print(name, round(time.monotonic()-self.started, 2), flush=True)

    def wait(self, seconds, client=None):
        deadline = time.monotonic()+seconds
        while time.monotonic() < deadline:
            self.guard()
            if client: client.pump(.05)
            else: time.sleep(.05)

    def settle_children(self, seconds=30):
        deadline = time.monotonic()+seconds
        while True:
            self.guard()
            explicit = {p.pid for p in self.children}
            other = [p for p in self.family.scan() if p not in explicit]
            if not other: return
            assert time.monotonic() < deadline, ('live descendants failed to settle', other)
            time.sleep(.05)

    def cleanup(self):
        self.expected.update(p.pid for p in self.children)
        inventory = [dict(pid=p, identity=identity(p)) for p in self.family.scan()]
        self.event('cleanup-inventory', processes=inventory)
        forced = self.family.finish(lambda: [p.poll() for p in self.children],
                                    lambda: all(p.poll() is not None for p in self.children), grace=5)
        return dict(forced=forced, remaining=self.family.members(), adopted=self.family.reaped,
                    before=inventory, exits=[dict(pid=p.pid, exit_code=p.returncode) for p in self.children])


def bind_mapping(pid, start, path, permissions):
    assert identity(pid)[0] == start
    stat = path.stat()
    zero, extra = [], []
    for line in Path(f'/proc/{pid}/maps').read_text().splitlines():
        f = line.split(maxsplit=5)
        if len(f) != 6 or f[5] != str(path): continue
        major, minor = (int(v, 16) for v in f[3].split(':'))
        assert os.makedev(major, minor) == stat.st_dev and int(f[4]) == stat.st_ino
        assert f[1] == permissions
        if int(f[2], 16): extra.append(line)
        else:
            a, b = (int(v, 16) for v in f[0].split('-'))
            assert b-a >= 192
            zero.append(line)
    assert len(zero) == 1, zero
    assert identity(pid)[0] == start
    return dict(device=stat.st_dev, inode=stat.st_ino, size=stat.st_size, zero=zero, extra=extra)


class Observer:
    def __init__(self, runtime, epoch, owner, env):
        self.rt = runtime
        self.path = runtime.out/'cas/casita.sqlite-tshm'
        self.owner = owner
        self.owner_start = identity(owner.pid)[0]
        self.binding = bind_mapping(owner.pid, self.owner_start, self.path, 'rw-s')
        helper = runtime.observer_binary
        self.child = runtime.spawn('observer-'+epoch, [str(helper), str(self.path), str(owner.pid), str(self.owner_start)], env, pipe=True)
        self.buffer = bytearray()
        self.index = 0
        self.elapsed = -1
        self.raw = (runtime.out/('observer-'+epoch+'.jsonl')).open('x')
        runtime.logs.append(Path(self.raw.name))
        for stream in (self.child.stdin, self.child.stdout): os.set_blocking(stream.fileno(), False)
        self.common = dict(owner_pid=owner.pid, owner_start_ticks=self.owner_start,
                           device=self.binding['device'], inode=self.binding['inode'])
        assert self.line() == dict(event='ready', **self.common)
        other = bind_mapping(self.child.pid, identity(self.child.pid)[0], self.path, 'r--s')
        fdinfo = {p.name:p.read_text() for p in Path(f'/proc/{self.child.pid}/fdinfo').iterdir()}
        assert all('lock:' not in value for value in fdinfo.values())
        runtime.event('observer-binding', owner=self.binding, helper=other, fdinfo=fdinfo)
        assert self.sample()['status'] == 'ok', 'initial WAL sample invalid'

    def send(self, data):
        deadline = time.monotonic()+5
        view = memoryview(data)
        while view:
            self.rt.guard()
            assert time.monotonic() < deadline, 'observer pipe write timeout'
            try: count = os.write(self.child.stdin.fileno(), view)
            except BlockingIOError:
                select.select([], [self.child.stdin], [], .01)
                continue
            assert count > 0
            view = view[count:]

    def line(self):
        deadline = time.monotonic()+5
        while b'\n' not in self.buffer:
            self.rt.guard()
            assert time.monotonic() < deadline, 'observer pipe read timeout'
            ready, _, _ = select.select([self.child.stdout], [], [], .01)
            if not ready: continue
            data = os.read(self.child.stdout.fileno(), 65536)
            assert data, 'observer EOF'
            self.buffer.extend(data)
            assert len(self.buffer) < 65536, 'observer output cap'
        line, _, rest = self.buffer.partition(b'\n')
        self.buffer = bytearray(rest)
        value = json.loads(line)
        self.raw.write(json.dumps(value)+'\n')
        self.raw.flush()
        return value

    def sample(self):
        assert identity(self.owner.pid)[0] == self.owner_start
        st = self.path.stat()
        assert (st.st_dev, st.st_ino) == (self.binding['device'], self.binding['inode'])
        assert st.st_size >= 192, 'shared header truncated'
        before = time.time_ns()
        self.send(b'sample\n')
        v = self.line()
        assert v['event'] == 'sample' and v['index'] == self.index
        assert all(v[k] == x for k,x in self.common.items())
        assert before <= v['unix_ns'] <= time.time_ns() and v['elapsed_ns'] >= self.elapsed
        self.elapsed = v['elapsed_ns']
        self.index += 1
        if v['status'] == 'ok':
            assert v['sequence'] % 2 == 0 and v['backlog_frames'] == v['max_frame']-v['nbackfills'] >= 0
        else:
            assert v['status'] == 'unavailable' and v['reason'] == 'busy_publication', v
        return v

    def close(self):
        self.rt.observer = None
        self.rt.expected.add(self.child.pid)
        self.send(b'close\n')
        assert self.line() == dict(event='closed', samples=self.index)
        deadline = time.monotonic()+5
        while self.child.poll() is None:
            self.rt.guard()
            assert time.monotonic() < deadline
            time.sleep(.01)
        assert self.child.returncode == 0 and not self.buffer
        self.child.stdin.close()
        self.child.stdout.close()
        self.raw.close()

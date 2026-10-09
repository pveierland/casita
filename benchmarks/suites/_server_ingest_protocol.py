"""Bounded Unix-socket client for sustained-server probes."""
import json
import select
import socket
import time

class Client:
    def __init__(self, path, guard, log, timeout=120):
        self.guard = guard
        self.log = log
        self.timeout = timeout
        self.buffer = bytearray()
        self.inbox = []
        self.sequence = 0
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.settimeout(1)
        try: self.socket.connect(str(path))
        except BaseException:
            self.socket.close()
            raise
        self.socket.setblocking(False)

    def record(self, direction, message):
        self.log.write(json.dumps(dict(unix_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                                       direction=direction, message=message))+'\n')
        self.log.flush()

    def pump(self, delay=.01):
        self.guard()
        ready, _, _ = select.select([self.socket], [], [], delay)
        if not ready: return
        chunk = self.socket.recv(65536)
        assert chunk, 'server closed connection'
        self.buffer.extend(chunk)
        assert len(self.buffer) <= 8*1024**2, 'wire line cap'
        while b'\n' in self.buffer:
            line, _, rest = self.buffer.partition(b'\n')
            self.buffer = bytearray(rest)
            value = json.loads(line)
            self.record('receive', value)
            assert type(value) is dict and len(value) == 1 and next(iter(value)) in ('Response','Notification'), value
            self.inbox.append(value)
        assert len(self.inbox) <= 10000, 'inbox cap'

    def send(self, body):
        self.guard()
        self.sequence += 1
        request = dict(id=self.sequence, body=body)
        data = memoryview((json.dumps(request)+'\n').encode())
        assert len(data) <= 1024**2, 'request cap'
        deadline = time.monotonic()+5
        self.record('send', request)
        while data:
            self.guard()
            assert time.monotonic() < deadline, 'socket write deadline'
            try: count = self.socket.send(data)
            except BlockingIOError:
                self.pump()
                continue
            assert count > 0
            data = data[count:]
        self.record('sent', dict(id=self.sequence))
        return self.sequence

    def take(self, predicate, timeout=None):
        deadline = time.monotonic()+(self.timeout if timeout is None else timeout)
        while True:
            self.guard()
            for index, value in enumerate(self.inbox):
                if predicate(value): return self.inbox.pop(index)
            assert time.monotonic() < deadline, 'response/task deadline'
            self.pump()

    def response(self, request_id, allow_error=False):
        value = self.take(lambda x: x.get('Response',{}).get('id') == request_id)['Response']['body']
        if not allow_error: assert not isinstance(value, dict) or 'Error' not in value, value
        return value

    def request(self, body, allow_error=False): return self.response(self.send(body), allow_error)

    def submit(self, session, command):
        value = self.request({'Submit':dict(session=session, command=command)})
        assert set(value) == {'Submitted'}
        return value['Submitted']['task']

    def finish(self, session, task):
        value = self.take(lambda x: x.get('Notification',{}).get('session') == session and
                          x['Notification'].get('event',{}).get('Finished',{}).get('task') == task)
        outcome = value['Notification']['event']['Finished']['outcome']
        assert set(outcome) == {'Value'}, outcome
        return outcome['Value']['handle']

    def command(self, session, command): return self.finish(session, self.submit(session, command))

    def observe(self, session, handle):
        value = self.request({'Observe':dict(session=session, handle=handle)})
        assert set(value) == {'Observed'}
        return value['Observed']['observation']

    def close(self): self.socket.close()

def string(observed):
    assert observed['type'] == 'string', observed
    value = observed['value']['bytes']
    return bytes(value).decode('utf-8') if isinstance(value, list) else value

def attributes(observed):
    assert observed['type'] == 'attrs', observed
    values = observed['value']
    assert len({x['name'] for x in values}) == len(values)
    return {x['name']:x['value'] for x in values}

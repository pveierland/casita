"""Retained-session workload shared by sustained and pressure benchmark cases."""
import json
import signal
import time
from benchmarks.suites._server_ingest_protocol import Client, attributes, string
from benchmarks.suites._server_ingest_runtime import Observer
PATHS = ('.version', 'default.nix', 'lib/default.nix')

class Workload:
    def __init__(self, rt, env, reads, repository, binary):
        self.rt, self.env, self.reads, self.repository = rt, env, reads, repository
        self.binary = binary
        self.client = None
        self.wire = (rt.out/'wire.jsonl').open('x')
        self.retained = {}
        self.finished = []
        self.reopened = []
        self.cancellations = []
        self.overlaps = []
        self.checks = 0
        self.server = None
        self.import_started = None

    def start(self, epoch):
        sock = self.rt.out/('server-'+epoch+'.sock')
        command = [str(self.binary), 'serve', '--socket', str(sock), '--cas', str(self.rt.out/'cas'),
                   '--build-dir', str(self.rt.out/'build'), '--no-build']
        for k,v in [('io-threads','4'),('blocking-threads','64'),('speculate-literal-fetches','true'),
                    ('speculate-locked-inputs','true'),('speculate-narinfos','true')]:
            command.extend(['--option', k, v])
        self.server = self.rt.spawn('server-'+epoch, command, self.env)
        self.rt.server = self.server
        deadline = time.monotonic()+30
        while not sock.exists():
            self.rt.guard()
            assert time.monotonic() < deadline, 'server startup'
            time.sleep(.02)
        self.client = Client(sock, self.guard, self.wire, timeout=180)
        assert 'Welcome' in self.client.request({'Hello':{'version':1}})
        self.open('responsive')
        # The server's CAS is initialized before its socket is made available.
        self.rt.observer = Observer(self.rt, epoch, self.server, self.env)
        self.rt.phase_to(epoch+'-ready')

    def guard(self):
        self.rt.guard()
        if self.import_started is not None:
            assert time.monotonic()-self.import_started < 180, 'per-import watchdog'
        if self.client:
            # Retain every notification in the wire log, but consume non-finish notices.
            self.client.inbox[:] = [m for m in self.client.inbox if
                'Notification' not in m or 'Finished' in m['Notification'].get('event', {})]

    def open(self, session):
        assert self.client.request({'Open':dict(session=session, config={'experimental_features':'flakes'})}) == {'Opened':dict(session=session)}

    def close(self, session):
        assert self.client.request({'Close':dict(session=session)}) == {'Closed':dict(session=session)}
        self.rt.event('session-closed', session=session)

    def expression(self, index):
        fixture = self.reads[index]
        tree = 'builtins.fetchTree { type = "git"; url = '+json.dumps(self.repository.as_uri())+'; rev = '+json.dumps(fixture['revision'])+'; narHash = '+json.dumps(fixture['nar_hash'])+'; shallow = true; }'
        attrs = ['tree = t;']
        # Unique thunks are selected once each; the parent stays in its original session.
        for slot in range(67):
            path = PATHS[slot % 3] if slot < 64 else PATHS[slot-64]
            target = '(t.outPath + '+json.dumps('/'+path)+')'
            attrs.append(f'r{slot} = builtins.readFile {target}; h{slot} = builtins.hashFile "sha256" {target};')
        return 'let t = '+tree+'; in { '+' '.join(attrs)+' }'

    def begin(self, index, session):
        self.import_started = time.monotonic()
        self.open(session)
        parent = self.client.command(session, {'Evaluate':dict(source=self.expression(index), base=str(self.rt.out))})
        tree = self.client.command(session, {'Select':dict(of=parent, name='tree')})
        task = self.client.submit(session, {'Force':dict(handle=tree, depth='Deep')})
        record = dict(index=index, session=session, parent=parent, tree=tree, task=task)
        self.rt.event('import-submitted', **record, reference={k:self.reads[index][k] for k in ('revision','git_tree','nar_hash')})
        return record

    def running(self, record):
        response = self.client.request({'Status':dict(session=record['session'], task=record['task'])})
        return response['Status']['state'] == 'Running'

    def responsiveness(self, record):
        before = self.running(record)
        started = time.monotonic()
        handle = self.client.command('responsive', {'Evaluate':dict(source='40 + 2', base=str(self.rt.out))})
        value = self.client.observe('responsive', handle)
        assert value['type'] == 'int' and value['value'] == 42, value
        assert self.client.request({'Release':dict(session='responsive', handle=handle)}) == 'Done'
        elapsed = time.monotonic()-started
        after = self.running(record)
        overlap = dict(index=record['index'], session=record['session'], elapsed_seconds=elapsed, running_before=before, running_after=after)
        self.overlaps.append(overlap)
        self.rt.event('responsiveness', **overlap)

    def deferred(self, record, slot):
        path = PATHS[slot % 3] if slot < 64 else PATHS[slot-64]
        expected = self.reads[record['index']]['reads'][path]
        started = time.monotonic()
        for prefix, want in [('r', expected['text']), ('h', expected['sha256'])]:
            handle = self.client.command(record['session'], {'Select':dict(of=record['parent'], name=prefix+str(slot))})
            kind = self.client.request({'Kind':dict(session=record['session'], handle=handle)})
            assert kind == {'Kind':{'kind':'Unforced'}}, (record, slot, kind)
            assert string(self.client.observe(record['session'], handle)) == want
            assert self.client.request({'Release':dict(session=record['session'], handle=handle)}) == 'Done'
        self.checks += 1
        self.rt.event('deferred-read', index=record['index'], session=record['session'], slot=slot,
                      path=path, sha256=expected['sha256'], elapsed_seconds=time.monotonic()-started)

    def finish(self, record):
        forced = self.client.finish(record['session'], record['task'])
        observed = attributes(self.client.observe(record['session'], record['tree']))
        fixture = self.reads[record['index']]
        assert all(string(observed[k]) == fixture[ref] for k,ref in [('narHash','nar_hash'), ('rev','revision')])
        assert self.client.request({'Release':dict(session=record['session'], handle=forced)}) == 'Done'
        self.rt.event('import-verified', **record, elapsed_seconds=time.monotonic()-self.import_started)
        self.import_started = None
        return record

    def cancel_attempt(self, index):
        self.rt.last_sample = -1
        self.rt.guard()
        before = self.rt.latest['wal']
        assert before['status'] == 'ok'
        record = self.begin(index, 'cancel-'+str(index))
        witness = None
        while self.running(record):
            sample = self.rt.latest.get('wal')
            if sample and sample['status'] == 'ok' and sample['unix_ns'] > before['unix_ns'] and (
                sample['transaction_count'] != before['transaction_count'] or sample['max_frame'] != before['max_frame']):
                witness = sample
                break
            self.client.pump(.02)
        if witness is None:
            self.rt.event('cancellation-censored', **record, reason='task finished before WAL activity witness')
            self.finish(record)
            self.close(record['session'])
            self.cancellations.append(dict(index=index, success=False, reason='no active-work witness'))
            return
        self.rt.event('cancellation-witness', **record, before=before, witness=witness)
        assert self.client.request({'Cancel':dict(session=record['session'], task=record['task'])}) == 'Done'
        message = self.client.take(lambda m: m.get('Notification', {}).get('session') == record['session'] and
            m['Notification'].get('event', {}).get('Finished', {}).get('task') == record['task'])
        outcome = message['Notification']['event']['Finished']['outcome']
        cancelled = 'Failed' in outcome and 'E0004' in json.dumps(outcome['Failed'])
        self.cancellations.append(dict(index=index, success=cancelled, outcome=outcome, witness=witness))
        self.rt.event('cancellation-result', **self.cancellations[-1])
        self.import_started = None
        self.close(record['session'])

    def stop(self, epoch):
        self.close('responsive')
        assert self.client.request('Sessions') == {'Sessions':{'sessions':[]}}
        self.rt.settle_children()
        self.rt.observer.close()
        self.client.close()
        self.client = None
        self.rt.expected.add(self.server.pid)
        self.rt.event('intentional-server-stop', epoch=epoch, signal='SIGTERM', pid=self.server.pid)
        signal.pidfd_send_signal(self.rt.family.descriptors[self.server.pid], signal.SIGTERM)
        deadline = time.monotonic()+10
        while self.server.poll() is None:
            self.rt.guard()
            assert time.monotonic() < deadline, 'server stop timeout'
            time.sleep(.02)
        assert self.server.returncode == -15
        self.rt.settle_children()
        assert not self.rt.family.scan(), 'owned children remain after stop'


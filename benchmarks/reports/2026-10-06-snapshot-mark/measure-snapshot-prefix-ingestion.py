"""Matched release ingestion with ordinary pressure collection enabled."""
import json
import os
import pathlib
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, '/tmp/mnos-ingest/casita')
from benchmarks import build_manifest

evidence = pathlib.Path('/tmp/mnos-ingest/evidence')
repo = pathlib.Path('/tmp/mnos-ingest/mnos-nix-xp')
binaries = [('control', evidence / 'import-git-local-range-control'),
            ('candidate', evidence / 'import-git-snapshot-prefix')]
builds = build_manifest.artifacts(binaries, fixture=True)
assert builds[0]['build']['source_sha256'] == builds[1]['build']['source_sha256']
scenarios = json.loads((evidence / 'ingestion-scenarios.json').read_text())
case = scenarios['cases']['release']
reference = json.loads((repo / 'docs/reports/evidence/git-ingestion/2026-10-06-wider-updates.json').read_text())['identities']
output = evidence / 'snapshot-prefix-ingestion.json'
assert not output.exists()
stop = False
def stop_between_runs(*_):
    global stop
    stop = True
    print('Stopping after the current run is retained.', flush=True)
signal.signal(signal.SIGTERM, stop_between_runs)
signal.signal(signal.SIGINT, stop_between_runs)
report = dict(complete=False, builds=builds, scenario=case, repository=scenarios['repository'],
              scope='Same evaluator and fixture; candidate filters only dependencies queued by the snapshot scan and includes the earlier named-root optimization. The fixture publishes no named roots. Ordinary pressure GC remains enabled.',
              protocol=dict(repetitions=4, order='C1,N1,N2,C2,C3,N3,N4,C4',
                            cpu_affinity=sorted(os.sched_getaffinity(0)),
                            retain_previous=True, stores_retained=True, concurrent_builds_or_tests=False,
                            pressure_observation='Read marker mtime every 100ms in parent thread; no modifications.',
                            pressure_limitations='Observed marker updates, not exact GC counts or proof of nonempty named marking; polling may miss updates.',
                            source_cache='OS-managed local mirror; no explicit dropping or warming'), samples=[])
def save():
    output.write_text(json.dumps(report, indent=2) + '\n')
save()
try:
    for trial in range(1, 5):
        for variant, binary in (binaries if trial % 2 else binaries[::-1]):
            if stop:
                raise RuntimeError('interrupted between runs')
            name = f'snapshot-prefix-ingestion-{variant}-{trial}'
            scratch, log_path = evidence / name, evidence / (name + '.log')
            assert not scratch.exists() and not log_path.exists()
            env = dict(os.environ, MNOS_BENCH_RETAIN_PREVIOUS='1', MNOS_BENCH_DIR=str(scratch),
                       MNOS_BENCH_REPO=scenarios['repository'], MNOS_BENCH_REV=case['from']['commit'],
                       MNOS_BENCH_REV2=case['to']['commit'])
            command = [str(binary), '--ignored', '--nocapture', '--exact', 'import_large_git_tree']
            started, started_unix = time.monotonic(), time.time()
            load_start = os.getloadavg()
            filesystem = os.statvfs(evidence)
            print('Starting', name, flush=True)
            events = []
            ended = threading.Event()
            with log_path.open('x') as log:
                child = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                cas = scratch / f'cas-{child.pid}'
                stamp = cas / '.casita-pressure-collection'
                def observe():
                    previous = None
                    while not ended.is_set():
                        try:
                            value = stamp.stat().st_mtime_ns
                        except FileNotFoundError:
                            value = None
                        if value is not None and value != previous:
                            events.append(dict(elapsed_seconds=time.monotonic()-started, mtime_ns=value))
                            previous = value
                        ended.wait(.1)
                observer = threading.Thread(target=observe)
                observer.start()
                try:
                    _, status, usage = os.wait4(child.pid, 0)
                    wall = time.monotonic()-started
                    child.returncode = os.waitstatus_to_exitcode(status)
                finally:
                    ended.set()
                    observer.join()
            process = dict(exit_code=child.returncode, wall_seconds=wall, max_rss_bytes=usage.ru_maxrss*1024,
                           user_seconds=usage.ru_utime, system_seconds=usage.ru_stime,
                           input_blocks=usage.ru_inblock, output_blocks=usage.ru_oublock,
                           started_unix_seconds=started_unix,
                           load_start=load_start, load_end=os.getloadavg(),
                           filesystem_start=dict(total_bytes=filesystem.f_blocks*filesystem.f_frsize,
                                                 available_bytes=filesystem.f_bavail*filesystem.f_frsize))
            lines = [line.removeprefix('test import_large_git_tree ... ')
                     for line in log_path.read_text().splitlines()]
            def rows(prefix):
                return [json.loads(line[len(prefix):]) for line in lines if line.startswith(prefix)]
            identities, stages = rows('git_ingest_identity '), rows('git_ingest_sample ')
            inventory = []
            if cas.exists():
                for path in sorted(cas.rglob('*')):
                    if path.is_file():
                        stat = path.stat()
                        inventory.append(dict(path=str(path.relative_to(cas)), bytes=stat.st_size, allocated_bytes=stat.st_blocks*512))
            sample = dict(variant=variant, trial=trial, command=command, process=process, stages=stages,
                          identities=identities, pressure_events=events, cas=str(cas), inventory=inventory)
            report['samples'].append(sample)
            save()
            assert child.returncode == 0, process
            assert rows('git_ingest_configuration ') == [{'retain_previous': True}]
            assert len(stages) == 6 and len(identities) == 2
            revisions = [case['from']['commit'], case['to']['commit']]
            assert [identity['revision'] for identity in identities] == revisions
            assert [(stage['revision'], stage['stage']) for stage in stages] == [
                (revision, stage) for revision in revisions for stage in ('import_git', 'git_tree', 'nar')]
            for identity in identities:
                expected = reference[identity['revision']]
                for key in ('revision', 'git_tree', 'root', 'nar_hash', 'nar_bytes'):
                    assert identity[key] == expected[key], (key, identity, expected)
            assert events, 'expected ordinary disk-pressure collection to be active'
            print('Passed', name, wall, 'observed pressure-marker updates', len(events), flush=True)
    report['complete'] = True
except Exception as error:
    report['error'] = str(error)
    raise
finally:
    save()

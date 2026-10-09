"""Actual Mnos server ingestion with retained roots and pressure-cooldown controls.

The externally built server, passive WAL observer and CppNix-verified corpus are
explicit inputs. This suite never builds them or downloads a corpus implicitly.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import sys
import time
import traceback

from benchmarks.suites._server_ingest_runtime import Runtime, load, save, sha
from benchmarks.suites._server_ingest_workload import Workload, PATHS


def load_configuration(path):
    """Validate artifact proofs and every frozen input before starting a child."""
    path = path.resolve()
    config = load(path)
    required = {'server_build', 'observer_binary', 'observer_sha256', 'corpus_dir', 'reads_dir'}
    if set(config) != required:
        raise ValueError(f'configuration keys must be {sorted(required)}')
    for key in required - {'observer_sha256'}:
        config[key] = Path(config[key]).resolve(strict=True)
    build_dir = config['server_build']
    binary = build_dir/'mnos-eval'
    build = load(build_dir/'mnos-eval.build.json')
    proof = load(build_dir/'result.json')
    assert proof['success'] and proof['source_unchanged'] and not proof['interrupted']
    assert build['features'] == [] and build['default_features'] is True and build['profile'] == 'release'
    assert sha(binary) == build['executable_sha256'] == proof['executable_sha256']
    assert sha(build_dir/'mnos-eval.build.json') == proof['manifest_sha256']
    assert sha(config['observer_binary']) == config['observer_sha256']
    fixtures = load(config['corpus_dir']/'fixtures.json')
    reads = load(config['reads_dir']/'fixtures.json')['revisions']
    assert len(reads) == len(fixtures['revisions']) == 64
    inputs = dict(fixtures['input_sha256'])
    for directory in (config['corpus_dir'], config['reads_dir']):
        result = load(directory/'result.json')
        assert result['success'] and not result['interrupted']
        inputs.update(result['input_sha256'])
        for name in ('result.json', 'fixtures.json'):
            inputs[str(directory/name)] = sha(directory/name)
    for key in ('revision', 'git_tree', 'nar_hash'):
        assert len({row[key] for row in reads}) == 64
    for fixture, reference in zip(fixtures['revisions'], reads):
        assert all(fixture[k] == reference[k] for k in ('revision', 'git_tree', 'nar_hash'))
        for name in PATHS:
            value = reference['reads'][name]
            contents = base64.b64decode(value['base64'], validate=True)
            assert contents == value['text'].encode() and len(contents) == value['bytes']
            assert hashlib.sha256(contents).hexdigest() == value['sha256']
    inputs.update({v['path']:v['sha256'] for v in build['reference_libraries'].values()})
    sources = [Path(__file__), *Path(__file__).parent.glob('_server_ingest_*.py')]
    sources += [Path(__file__).parent.parent/name for name in ('all.py', 'cli.py', 'manifest.json')]
    for item in [path, binary, build_dir/'mnos-eval.build.json', build_dir/'result.json',
                 config['observer_binary'], *sources]:
        inputs[str(item)] = sha(item)
    assert all(sha(p) == digest for p,digest in inputs.items()), 'input digest mismatch'
    git = Path(shutil.which('git')).resolve()
    assert str(git) in inputs and sha(git) == inputs[str(git)], 'Git executable is not a qualified input'
    repository = Path(fixtures['repository']).resolve(strict=True)
    return dict(config=config, binary=binary, repository=repository, reads=reads,
                inputs=inputs, sources=sources, build=build)


def disk_usage(path):
    value = os.statvfs(path)
    return dict(total_bytes=value.f_blocks*value.f_frsize, free_bytes=value.f_bfree*value.f_frsize,
                available_bytes=value.f_bavail*value.f_frsize)


def above_pressure_threshold(usage):
    return usage['total_bytes'] > 0 and (usage['total_bytes']-usage['free_bytes'])*100 >= usage['total_bytes']*80


def stamp_state(out):
    stamp = out/'cas/.casita-pressure-collection'
    info = stamp.stat()
    assert stamp.is_file() and not stamp.is_symlink()
    assert stamp.read_bytes() == b'pressure-triggered collection completed\n'
    return dict(device=info.st_dev, inode=info.st_ino, mtime_ns=info.st_mtime_ns)


def set_stamp(rt, age):
    """Control only the owned fixture's advisory stamp, outside import timing."""
    stamp = rt.out/'cas/.casita-pressure-collection'
    if stamp.exists():
        assert stamp.is_file() and not stamp.is_symlink()
    else:
        with stamp.open('xb') as f:
            f.write(b'pressure-triggered collection completed\n')
    now = time.time_ns()
    modified = now-int(age*1e9)
    os.utime(stamp, ns=(now, modified), follow_symlinks=False)
    state = stamp_state(rt.out)
    assert state['mtime_ns'] == modified
    rt.event('pressure-stamp-set', controlled_age_seconds=age, state=state)
    return state


def validate_pressure_transition(mode, before, after, started_ns, finished_ns):
    assert before['device'] == after['device'] and before['inode'] == after['inode']
    if mode == 'recent':
        assert after == before, 'recent-stamp case unexpectedly collected'
    elif mode == 'aged':
        assert started_ns < after['mtime_ns'] <= finished_ns, 'aged case did not witness collection completion'
        assert after['mtime_ns'] != before['mtime_ns']
    else:
        raise ValueError(mode)


def run_case(context, out, mode, profile):
    available = disk_usage(out.parent)['available_bytes']
    reserve = 3707665532 if mode == 'sustained' and profile == 'standard' else 2*1024**3
    assert available >= reserve, 'launch capacity floor'
    if mode != 'sustained':
        assert above_pressure_threshold(disk_usage(out.parent)), 'pressure case requires a filesystem at least80% used'
    out.mkdir()
    config_dir = out/'config'
    config_dir.mkdir()
    environment = {k:os.environ[k] for k in ('PATH','LANG','LD_LIBRARY_PATH') if k in os.environ}
    environment.update(HOME=str(config_dir), XDG_CACHE_HOME=str(out/'cache'), NIX_CONF_DIR=str(config_dir),
                       NIX_USER_CONF_FILES='/dev/null', NIX_CONFIG='', GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL='/dev/null')
    rt = Runtime(out, (1200 if profile == 'smoke' else 14400) if mode == 'sustained' else 2400,
                 context['config']['observer_binary'], context['interrupt_state'])
    work = Workload(rt, environment, context['reads'], context['repository'], context['binary'])
    smoke = profile == 'smoke'
    indices = [0,1,16,17] if smoke else list(range(64))
    cancels = [16] if smoke else [8,24,40,56]
    seeds, target = ([0,1],2) if smoke else ([0,15,16,31,32,47,48],49)
    spacing, idle = (5,2) if smoke else (75,120)
    save(out/'protocol.json', dict(mode=mode, profile=profile, environment=environment, input_sha256=context['inputs'],
         indices=indices if mode=='sustained' else seeds+[target], cancellations=cancels if mode=='sustained' else [],
         spacing_seconds=spacing if mode=='sustained' else None, idle_seconds=idle if mode=='sustained' else None,
         pressure_seeds=seeds if mode!='sustained' else [], pressure_target=target if mode!='sustained' else None,
         seed_stamp_age_seconds=-86400 if mode!='sustained' else None,
         target_stamp_age_seconds={'recent':0,'aged':120}.get(mode),
         scope='real server retained roots; controlled stamp cases are attribution, not production policy changes',
         process_io_scope='cumulative process counters can include reaped children; never add wait4 again',
         sampled_rss_scope='contemporaneous process RSS sum, potentially double-counting shared mappings',
         ticks_per_second=os.sysconf('SC_CLK_TCK')))
    errors, cleanup, source_ok, held_duration, target_result = [], None, False, None, None
    try:
        work.start('first')
        if mode == 'sustained':
            schedule = time.monotonic()
            for position,index in enumerate(indices):
                epoch_start = time.monotonic()
                rt.phase_to('epoch-'+str(index))
                rt.event('epoch-start', index=index, nominal_offset=position*spacing,
                         actual_offset=epoch_start-schedule, overrun_seconds=epoch_start-schedule-position*spacing)
                if index in cancels:
                    work.cancel_attempt(index)
                record = work.begin(index, 'root-'+str(index))
                work.responsiveness(record)
                for older in work.retained.values():
                    before = work.running(record)
                    work.deferred(older, index)
                    rt.event('deferred-read-overlap', old_index=older['index'], new_index=index,
                             running_before=before, running_after=work.running(record))
                work.finish(record)
                work.finished.append(index)
                if index == indices[0] or index%16 in (0,15):
                    work.retained[index] = record
                else:
                    work.close(record['session'])
                rt.phase_to('epoch-idle-'+str(index))
                rt.wait(max(0, epoch_start+spacing-time.monotonic()), work.client)
            held_duration = time.monotonic()-schedule
            assert held_duration >= len(indices)*spacing
            rt.phase_to('retained-idle')
            rt.wait(idle, work.client)
        else:
            for index in seeds:
                rt.phase_to('seed-'+str(index))
                before = set_stamp(rt, -86400)
                record = work.begin(index, 'root-'+str(index))
                work.finish(record)
                assert stamp_state(out) == before, 'preparation unexpectedly collected'
                work.finished.append(index)
                work.retained[index] = record
            rt.phase_to('target-'+mode)
            usage = disk_usage(out)
            assert above_pressure_threshold(usage), 'filesystem no longer above pressure threshold'
            before = set_stamp(rt, 0 if mode=='recent' else 120)
            rt.last_sample = -1
            rt.guard()
            start_sample = rt.latest
            started = time.monotonic()
            started_ns = time.time_ns()
            record = work.begin(target, 'target-'+str(target))
            work.responsiveness(record)
            overlaps = []
            for older in work.retained.values():
                running_before = work.running(record)
                work.deferred(older, target)
                running_after = work.running(record)
                overlaps.append(dict(old_index=older['index'], running_before=running_before, running_after=running_after))
                rt.event('deferred-read-overlap', new_index=target, **overlaps[-1])
            work.finish(record)
            finished_ns = time.time_ns()
            elapsed = time.monotonic()-started
            after = stamp_state(out)
            validate_pressure_transition(mode, before, after, started_ns, finished_ns)
            assert all(x['running_before'] and x['running_after'] for x in overlaps), 'old reads missed active target'
            rt.last_sample = -1
            rt.guard()
            target_result = dict(index=target, elapsed_seconds=elapsed, started_unix_ns=started_ns,
                                 finished_unix_ns=finished_ns, before_stamp=before, after_stamp=after,
                                 filesystem=usage, start_sample=start_sample, end_sample=rt.latest,
                                 overlapping_reads=overlaps)
            rt.event('pressure-target-verified', **target_result)
            work.finished.append(target)
            work.close(record['session'])
        for record in work.retained.values():
            for slot in range(64,67):
                work.deferred(record, slot)
        rt.phase_to('release-sessions')
        for record in work.retained.values():
            work.close(record['session'])
        work.retained.clear()
        if mode == 'sustained':
            rt.phase_to('released-idle')
            rt.wait(idle, work.client)
        work.stop('first')
        if mode == 'sustained':
            rt.phase_to('reopening')
            work.start('reopened')
            for index in indices:
                rt.phase_to('reopen-'+str(index))
                record = work.begin(index, 'reopen-'+str(index))
                work.finish(record)
                for slot in range(64,67):
                    work.deferred(record, slot)
                work.close(record['session'])
                work.reopened.append(index)
            work.stop('reopened')
            assert work.finished == work.reopened == indices
            assert len(work.cancellations)==len(cancels) and all(x['success'] for x in work.cancellations)
        else:
            assert work.finished == seeds+[target]
        source_ok = all(sha(p)==h for p,h in context['inputs'].items())
        assert source_ok, 'input changed during case'
        assert all(x['running_before'] for x in work.overlaps), 'response overlap coverage incomplete'
    except BaseException:
        errors.append(traceback.format_exc())
    finally:
        if work.client:
            work.client.close()
        try:
            cleanup = rt.cleanup()
        except BaseException:
            errors.append(traceback.format_exc())
    clean = cleanup is not None and not any(cleanup[k] for k in ('forced','remaining','adopted','before'))
    result = dict(success=not errors and clean and source_ok and not rt.stopped, mode=mode, profile=profile,
        errors=errors, cleanup=cleanup, interrupted=rt.stopped, input_unchanged=source_ok,
        elapsed_seconds=time.monotonic()-rt.started, held_duration_seconds=held_duration,
        completed=work.finished, reopened=work.reopened, cancellations=work.cancellations,
        responsiveness=work.overlaps, read_hash_pairs=work.checks, target=target_result,
        natural_descendants=rt.natural, samples=rt.samples, sampled_family_rss_max_bytes=rt.max_rss,
        sampled_physical_wal_max_bytes=rt.max_wal, sampled_backlog_max_frames=rt.max_backlog,
        input_sha256=context['inputs'])
    save(out/'result.json', result)
    work.wire.close()
    rt.events.close()
    rt.resources.close()
    return result


def main(argv=None):
    if not __debug__:
        raise RuntimeError('correctness gates require Python without -O')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--configuration', required=True, type=Path)
    parser.add_argument('--mode', choices=('sustained','pressure'), default='sustained')
    parser.add_argument('--profile', choices=('smoke','standard'), default='standard')
    parser.add_argument('--repetitions', type=int, default=1)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args(argv)
    if args.repetitions < 1:
        parser.error('repetitions must be positive')
    context = load_configuration(args.configuration)
    context['interrupt_state'] = {'stopped': False}
    output = args.output.resolve()
    assert not output.exists(), 'preserve existing report'
    artifacts = output.with_name(output.stem+'-artifacts')
    artifacts.mkdir(parents=True)
    sources = artifacts/'driver-sources'
    sources.mkdir()
    for path in context['sources']:
        (sources/path.name).write_bytes(path.read_bytes())
    report = dict(schema_version=1, suite_id='native-git', mode=args.mode, profile=args.profile,
                  complete=False, success=False, repetitions=args.repetitions,
                  input_sha256=context['inputs'], cases=[], errors=[])
    # Each terminal case has its own immutable result; the aggregate is emitted once.
    try:
        for repetition in range(args.repetitions):
            modes = ['sustained'] if args.mode=='sustained' else (['recent','aged'] if repetition%2==0 else ['aged','recent'])
            for mode in modes:
                assert not context['interrupt_state']['stopped'], 'interrupted between cases'
                name = f'{repetition:02d}-{mode}'
                print(name, flush=True)
                result = run_case(context, artifacts/name, mode, args.profile)
                report['cases'].append(dict(name=name, result=str(artifacts/name/'result.json'),
                    result_sha256=sha(artifacts/name/'result.json'), success=result['success'], target=result['target']))
                assert result['success'], f'{name} failed: {result["errors"]}'
        report['complete'] = True
        report['success'] = True
    except BaseException:
        report['errors'].append(traceback.format_exc())
    # A pending interrupt cannot turn a completed case into an aggregate success.
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT,signal.SIGTERM})
    if context['interrupt_state']['stopped'] or signal.sigpending() & {signal.SIGINT,signal.SIGTERM}:
        report['complete'] = False
        report['success'] = False
        report['errors'].append('pending interrupt at finalization')
    save(output, report)
    print(json.dumps(dict(success=report['success'], cases=[dict(name=x['name'],success=x['success']) for x in report['cases']],errors=report['errors'])), flush=True)
    return 0 if report['success'] else 1


if __name__ == '__main__':
    sys.exit(main())

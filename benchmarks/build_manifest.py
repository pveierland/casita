"""Build identities for retained probes and fail-closed paired comparisons."""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import subprocess
import tomllib

from benchmarks.suites.repository import BenchmarkError

MATCHED_FIELDS = ('lockfile_sha256', 'features', 'default_features', 'rustc_version',
                  'rustflags', 'target', 'profile', 'build_environment', 'cargo_config_sha256')
REQUIRED_FIELDS = ('schema_version', 'executable_sha256', 'source_revision', 'source_sha256', 'source_dirty', *MATCHED_FIELDS)


def digest(path):
    with pathlib.Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def manifest_path(executable):
    return pathlib.Path(str(executable) + '.build.json')


def read(executable, *, required=False):
    path = manifest_path(executable)
    if not path.exists():
        if required:
            raise BenchmarkError(f'build manifest required for {executable}; rebuild with benchmark all or benchmark revisions')
        return None
    try:
        build = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise BenchmarkError(f'invalid build manifest {path}: {error}') from error
    if not isinstance(build, dict):
        raise BenchmarkError(f'invalid build manifest {path}: expected an object')
    for field in REQUIRED_FIELDS:
        if field not in build or build[field] is None:
            raise BenchmarkError(f'build manifest is missing {field}: {path}')
    if build['schema_version'] != 1:
        raise BenchmarkError(f'unsupported build manifest schema: {path}')
    for field in ('executable_sha256', 'source_sha256', 'lockfile_sha256', 'cargo_config_sha256',
                  *(('fixture_sha256',) if 'fixture_sha256' in build else ())):
        value = build[field]
        if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
            raise BenchmarkError(f'invalid {field} in build manifest: {path}')
    if not isinstance(build['features'], list) or not all(isinstance(f, str) for f in build['features']):
        raise BenchmarkError(f'invalid features in build manifest: {path}')
    if type(build['default_features']) is not bool or type(build['source_dirty']) is not bool or not isinstance(build['build_environment'], dict):
        raise BenchmarkError(f'invalid build settings in manifest: {path}')
    for field in ('source_revision', 'rustc_version', 'target', 'profile'):
        if not isinstance(build[field], str) or not build[field]:
            raise BenchmarkError(f'invalid {field} in build manifest: {path}')
    if not isinstance(build['rustflags'], str):
        raise BenchmarkError(f'invalid rustflags in build manifest: {path}')
    if build['executable_sha256'] != digest(executable):
        raise BenchmarkError(f'build manifest fingerprint does not match executable: {executable}')
    return build


def artifacts(variants, *, fixture=False):
    paired = len(variants) > 1
    result = []
    for variant, executable in variants:
        build = read(executable, required=paired)
        row = dict(variant=variant, path=str(executable), sha256=digest(executable),
                   provenance='verified' if build else 'unverified')
        if build is not None:
            row['build'] = build
        result.append(row)
    if paired:
        if len({r['sha256'] for r in result}) != len(result):
            raise BenchmarkError('paired executables must have distinct fingerprints')
        fields = (*MATCHED_FIELDS, 'fixture_sha256') if fixture else MATCHED_FIELDS
        for field in fields:
            if any(field not in row['build'] or row['build'][field] is None for row in result):
                raise BenchmarkError(f'paired build manifests are missing {field}')
            if any(row['build'][field] != result[0]['build'][field] for row in result[1:]):
                raise BenchmarkError(f'paired build manifests differ in {field}')
    return result


def _run(root, environment, *command):
    return subprocess.check_output(command, cwd=root, env=environment)


def source_identity(root, environment):
    revision = _run(root, environment, 'git', 'rev-parse', 'HEAD').decode().strip()
    paths = sorted(set(_run(root, environment, 'git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z').split(b'\0')) - {b''})
    state = hashlib.sha256()
    for name in paths:
        path = root / os.fsdecode(name)
        if path.is_symlink():
            content = b'link\0' + os.fsencode(os.readlink(path))
        elif path.is_file():
            content = str(path.stat().st_mode & 0o111).encode() + b'\0' + path.read_bytes()
        elif not path.exists():
            content = b'deleted'
        else:
            raise BenchmarkError(f'cannot fingerprint source directory {path}')
        for value in (name, content):
            state.update(len(value).to_bytes(8, 'little'))
            state.update(value)
    return revision, state.hexdigest()


def write(root, executable, command, *, environment=None, fixture=None):
    """Record a successfully Cargo-validated build, after copying its executable."""
    root, executable = pathlib.Path(root), pathlib.Path(executable)
    environment = dict(os.environ if environment is None else environment)
    revision, source_sha = source_identity(root, environment)
    lockfile = root / 'Cargo.lock'
    if not lockfile.is_file():
        raise BenchmarkError(f'build has no dependency lockfile: {lockfile}')
    rustc = _run(root, environment, environment.get('RUSTC', 'rustc'), '-vV').decode().strip()
    cargo_home = pathlib.Path(environment.get('CARGO_HOME', pathlib.Path.home() / '.cargo'))
    config_directories = [directory / '.cargo' for directory in (root, *root.parents)] + [cargo_home]
    configs = []
    for directory in config_directories:
        # Cargo ignores config.toml when the legacy config file exists.
        path = directory / ('config' if (directory / 'config').is_file() else 'config.toml')
        if path.is_file() and path not in configs:
            configs.append(path)
    config_bytes = [path.read_bytes() for path in configs]
    config_hash = hashlib.sha256()
    for content in config_bytes:
        config_hash.update(len(content).to_bytes(8, 'little')); config_hash.update(content)
    def option(name, default=None):
        for index, argument in enumerate(command):
            if argument == name:
                return command[index + 1]
            if argument.startswith(name + '='):
                return argument.split('=', 1)[1]
        return default
    host = next((line.split(': ', 1)[1] for line in rustc.splitlines() if line.startswith('host: ')), None)
    configured_target = next((config['build']['target'] for content in config_bytes
                              if 'target' in (config := tomllib.loads(content.decode())).get('build', {})), host)
    target = option('--target', environment.get('CARGO_BUILD_TARGET', configured_target))
    if not isinstance(target, str) or not target:
        raise BenchmarkError('build manifest requires one explicit target')
    features = sorted(set(option('--features', '').replace(',', ' ').split()))
    if '--all-features' in command:
        features = ['*']
    build = dict(schema_version=1, executable_sha256=digest(executable), source_revision=revision,
                 source_sha256=source_sha, source_dirty=bool(_run(root, environment, 'git', 'status', '--porcelain')),
                 lockfile_sha256=digest(lockfile), features=features,
                 default_features='--no-default-features' not in command, rustc_version=rustc,
                 rustflags=environment.get('CARGO_ENCODED_RUSTFLAGS', environment.get('RUSTFLAGS', '')),
                 target=target, profile=option('--profile', 'release' if '--release' in command or 'bench' in command else 'dev'),
                 cargo_config_sha256=config_hash.hexdigest(),
                 build_environment={key: value for key, value in sorted(environment.items())
                                    if (key.startswith(('CARGO_PROFILE_', 'CARGO_TARGET_', 'CARGO_BUILD_'))
                                        or key in {'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CARGO_INCREMENTAL'})
                                    and key != 'CARGO_TARGET_DIR'},
                 command=list(command))
    if fixture is not None:
        build['fixture_sha256'] = digest(root / fixture)
    manifest_path(executable).write_text(json.dumps(build, indent=2) + '\n')
    return build

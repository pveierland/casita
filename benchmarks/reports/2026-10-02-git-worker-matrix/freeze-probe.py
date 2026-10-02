"""Freeze an already-built Git closure probe and its exact source identity."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

root, cargo_log, destination = map(Path, sys.argv[1:])
rows = [json.loads(line) for line in cargo_log.read_text().splitlines() if line.startswith('{')]
assert any(row.get('reason') == 'build-finished' and row.get('success') for row in rows)
artifacts = [row for row in rows if row.get('reason') == 'compiler-artifact'
             and row.get('target', {}).get('name') == 'git_worker_matrix'
             and row.get('executable')]
assert len(artifacts) == 1
assert not artifacts[0]['fresh'], 'freeze only a freshly compiled probe from its intended source checkout'
libraries = [row for row in rows if row.get('reason') == 'compiler-artifact'
             and row.get('target', {}).get('name') == 'casita'
             and 'lib' in row.get('target', {}).get('kind', [])]
assert len(libraries) == 1 and not libraries[0]['fresh'], 'freeze requires a fresh library from this checkout'
assert Path(libraries[0]['target']['src_path']).resolve() == (root / 'crates/casita/src/lib.rs').resolve()
assert Path(artifacts[0]['target']['src_path']).resolve() == (root / 'crates/casita/tests/git_worker_matrix.rs').resolve()

destination.parent.mkdir(parents=True, exist_ok=True)
assert not destination.exists(), 'refuse to overwrite a frozen executable'
shutil.copy2(artifacts[0]['executable'], destination)
sha = lambda data: hashlib.sha256(data).hexdigest()
tracked = subprocess.check_output(['git', 'ls-files', '-z', 'Cargo.toml', 'crates/casita'], cwd=root).split(b'\0')
untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z', 'crates/casita'], cwd=root).split(b'\0')
source = hashlib.sha256()
for name in sorted(filter(None, tracked + untracked)):
    path = root / os.fsdecode(name)
    if path.is_file():
        source.update(name + b'\0' + path.read_bytes() + b'\0')
patch = subprocess.check_output(['git', 'diff', '--binary', 'HEAD', '--', 'Cargo.toml', 'crates/casita'], cwd=root)
for name in sorted(filter(None, untracked)):
    path = os.fsdecode(name)
    added = subprocess.run(['git', 'diff', '--no-index', '--binary', '--', '/dev/null', path],
                           cwd=root, capture_output=True, check=False)
    assert added.returncode in (0, 1), added.stderr.decode()
    patch += added.stdout
Path(str(destination) + '.patch').write_bytes(patch)
lock = (root / 'Cargo.lock').read_bytes()
Path(str(destination) + '.Cargo.lock').write_bytes(lock)
manifest = dict(executable_sha256=sha(destination.read_bytes()), lockfile_sha256=sha(lock),
                features='native,git,experimental', default_features=False,
                rustc_version=subprocess.check_output(['rustc', '--version'], text=True).strip(),
                rustflags=os.environ.get('RUSTFLAGS', ''),
                source_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
                source_patch_sha256=sha(patch), tracked_source_sha256=source.hexdigest(),
                untracked_source_files=[os.fsdecode(name) for name in sorted(filter(None, untracked))],
                fixture_sha256=sha(b''.join((root / name).read_bytes() for name in ['crates/casita/tests/git_worker_matrix.rs', 'crates/casita/tests/git_closure_import/bounded_fixture.rs', 'crates/casita/tests/git_worker_matrix/observations.rs'])),
                adapter_sha256=sha((root / 'crates/casita/tests/git_worker_matrix/adapter.rs').read_bytes()),
                source_root=str(root), cargo_artifact_fresh=artifacts[0]['fresh'])
Path(str(destination) + '.build.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps(manifest, indent=2))

"""Complete fake build identities for harness tests; never used for real probes."""
import hashlib
import json
import pathlib


def stamp(path, **overrides):
    document = dict(schema_version=1, executable_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    source_revision='a' * 40, source_sha256='b' * 64, source_dirty=False,
                    lockfile_sha256='c' * 64, features=['native', 'git'], default_features=False,
                    rustc_version='rustc fixture', rustflags='', target='fixture-host',
                    profile='release', build_environment={}, cargo_config_sha256='d' * 64,
                    fixture_sha256='e' * 64)
    document.update(overrides)
    pathlib.Path(str(path) + '.build.json').write_text(json.dumps(document))
    return document


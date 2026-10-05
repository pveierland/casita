import hashlib
import json
import pathlib
import tempfile
import unittest

from benchmarks import build_manifest
from benchmarks.suites.repository import BenchmarkError


from benchmarks.tests.build_fixtures import stamp

class BuildManifestTests(unittest.TestCase):
    def test_paired_comparison_requires_both_manifests(self):
        with tempfile.TemporaryDirectory() as directory:
            before, after = [pathlib.Path(directory) / name for name in ('before', 'after')]
            before.write_bytes(b'before'); after.write_bytes(b'after')
            with self.assertRaisesRegex(BenchmarkError, 'manifest'):
                build_manifest.artifacts([('baseline', before), ('candidate', after)])
            stamp(before)
            with self.assertRaisesRegex(BenchmarkError, 'manifest'):
                build_manifest.artifacts([('baseline', before), ('candidate', after)])
            stamp(after)
            self.assertEqual(len(build_manifest.artifacts([('baseline', before), ('candidate', after)])), 2)

    def test_unpaired_probe_may_be_explicitly_unverified(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = pathlib.Path(directory) / 'probe'; probe.write_bytes(b'probe')
            result, = build_manifest.artifacts([('candidate', probe)])
            self.assertEqual(result['provenance'], 'unverified')
            self.assertNotIn('build', result)

    def test_incomplete_tampered_and_incompatible_manifests_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            before, after = [pathlib.Path(directory) / name for name in ('before', 'after')]
            before.write_bytes(b'before'); after.write_bytes(b'after')
            stamp(before)
            for field, value in [('lockfile_sha256', '0' * 64), ('features', ['native']),
                                 ('default_features', True), ('rustc_version', 'different'),
                                 ('rustflags', '-C target-cpu=native'), ('target', 'different'),
                                 ('profile', 'dev'), ('build_environment', {'CARGO_PROFILE_RELEASE_LTO': 'off'}),
                                 ('cargo_config_sha256', '0' * 64)]:
                with self.subTest(field=field):
                    stamp(after, **{field: value})
                    with self.assertRaisesRegex(BenchmarkError, field):
                        build_manifest.artifacts([('baseline', before), ('candidate', after)])
            document = stamp(after)
            del document['rustflags']
            pathlib.Path(str(after) + '.build.json').write_text(json.dumps(document))
            with self.assertRaisesRegex(BenchmarkError, 'rustflags'):
                build_manifest.artifacts([('baseline', before), ('candidate', after)])
            stamp(after)
            after.write_bytes(b'changed after build')
            with self.assertRaisesRegex(BenchmarkError, 'fingerprint'):
                build_manifest.artifacts([('baseline', before), ('candidate', after)])

    def test_fixture_comparison_is_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            before, after = [pathlib.Path(directory) / name for name in ('before', 'after')]
            before.write_bytes(b'before'); after.write_bytes(b'after')
            stamp(before); stamp(after, fixture_sha256='0' * 64)
            with self.assertRaisesRegex(BenchmarkError, 'fixture_sha256'):
                build_manifest.artifacts([('baseline', before), ('candidate', after)], fixture=True)



class ManifestWriterTests(unittest.TestCase):
    def test_writer_fingerprints_built_source_dependencies_flags_and_fixture(self):
        import os
        import subprocess
        from unittest import mock
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory) / 'source'
            root.mkdir()
            (root / 'Cargo.lock').write_text('# resolved dependencies\n')
            (root / 'fixture.rs').write_text('fixture one\n')
            (root / '.cargo').mkdir()
            (root / '.cargo/config.toml').write_text('[build]\ntarget="fixture-target"\n')
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            subprocess.run(['git', '-C', str(root), 'add', '.'], check=True)
            subprocess.run(['git', '-C', str(root), '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
                            'commit', '-qm', 'fixture'], check=True)
            binary = pathlib.Path(directory) / 'probe'
            binary.write_bytes(b'built executable')
            environment = dict(os.environ, RUSTC='fixture-rustc', RUSTFLAGS='-C target-cpu=native',
                               CARGO_HOME=str(pathlib.Path(directory) / 'cargo'),
                               CARGO_PROFILE_RELEASE_LTO='thin')
            command = ['cargo', 'test', '--release', '--features', 'git,native', '--no-default-features']
            real_run = build_manifest._run
            def run(root, environment, *arguments):
                if arguments == ('fixture-rustc', '-vV'):
                    return b'rustc fixture\nhost: fixture-host\n'
                return real_run(root, environment, *arguments)
            with mock.patch.object(build_manifest, '_run', side_effect=run):
                original = build_manifest.write(root, binary, command, environment=environment, fixture='fixture.rs')
                self.assertEqual(build_manifest.read(binary, required=True), original)
                self.assertEqual(original['features'], ['git', 'native'])
                self.assertEqual(original['default_features'], False)
                self.assertEqual(original['target'], 'fixture-target')
                self.assertEqual(original['profile'], 'release')
                self.assertEqual(original['build_environment']['CARGO_PROFILE_RELEASE_LTO'], 'thin')
                self.assertEqual(original['fixture_sha256'], build_manifest.digest(root / 'fixture.rs'))
                self.assertEqual(original['rustflags'], '-C target-cpu=native')
                (root / 'fixture.rs').write_text('fixture changed without a commit\n')
                dirty = build_manifest.write(root, binary, command, environment=environment, fixture='fixture.rs')
                self.assertFalse(original['source_dirty'])
                self.assertTrue(dirty['source_dirty'])
                self.assertEqual(original['source_revision'], dirty['source_revision'])
                self.assertNotEqual(original['source_sha256'], dirty['source_sha256'])
                self.assertNotEqual(original['fixture_sha256'], dirty['fixture_sha256'])
                (root / 'untracked.rs').write_text('new source\n')
                self.assertNotEqual(build_manifest.source_identity(root, environment)[1], dirty['source_sha256'])
                (root / 'Cargo.lock').unlink()
                with self.assertRaisesRegex(BenchmarkError, 'lockfile'):
                    build_manifest.write(root, binary, command, environment=environment)


if __name__ == '__main__':
    unittest.main()

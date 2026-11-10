#!/usr/bin/env python3
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('dev_image', Path(__file__).with_name('dev-image.py'))


def load():
    if SPEC is None or not Path(SPEC.origin).is_file():
        return None
    module = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(module)
    return module


M = load()


def raw(value):
    return json.dumps(value, separators=(',', ':')).encode()


def digest(value):
    return 'sha256:' + hashlib.sha256(value).hexdigest()


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(M, 'manager publisher is absent')
        self.source = dict(repository=M.REPOSITORY, commit='a' * 40, git_tree='b' * 40,
                           payload_inventory_sha256='c' * 64)
        self.labels = M.build_labels(self.source, 'd' * 64)
        self.config = raw(dict(os='linux', architecture='arm64', config=dict(
            User='65532:65532', Entrypoint=['/manager'], Labels=self.labels)))
        self.layer = b'compressed layer fixture'
        self.manifest = raw(dict(schemaVersion=2, mediaType=M.MANIFEST,
                                 config=dict(mediaType=M.CONFIG, digest=digest(self.config), size=len(self.config)),
                                 layers=[dict(mediaType=M.LAYER, digest=digest(self.layer), size=len(self.layer))]))
        self.fetches = []
        self.smoke = dict(schema_version=1, image_id=digest(self.config), os='linux', architecture='arm64',
                          executable_sha256='e' * 64, source_version='m7-' + self.source['payload_inventory_sha256'][:12],
                          help_passed=True, read_only=True, user='65532:65532', network='none', build_info_observed=False)

    def fetch(self, path, **kwargs):
        self.fetches.append(path)
        if '/manifests/' in path:
            return 200, {'Content-Type': M.MANIFEST, 'Docker-Content-Digest': digest(self.manifest)}, self.manifest
        if path.endswith(digest(self.config)):
            return 200, {}, self.config
        if path.endswith(digest(self.layer)):
            return 200, {}, self.layer
        return 404, {}, raw({'errors': [{'code': 'MANIFEST_UNKNOWN'}]})

    def graph(self, **kwargs):
        return M.read_graph(self.fetch, digest(self.manifest), 'arm64', self.labels, **kwargs)

    def test_exact_package_and_commit_tag_only(self):
        self.assertEqual(M.tag_for(self.source, 'arm64'), 'dev-' + 'a' * 40 + '-arm64')
        for arch in ('x86_64', 'ARM64', 'amd64/v3', ''):
            with self.subTest(arch=arch), self.assertRaises(M.Refusal):
                M.tag_for(self.source, arch)
        for field, value in (('repository', 'https://github.com/foreign/project'), ('commit', 'main'),
                             ('git_tree', 'b' * 39), ('payload_inventory_sha256', 'C' * 64)):
            with self.subTest(field=field), self.assertRaises(M.Refusal):
                M.tag_for(dict(self.source, **{field: value}), 'arm64')

    def test_classifies_only_exact_authenticated_manifest_absence(self):
        for status, payload in ((401, {'errors': [{'code': 'UNAUTHORIZED'}]}),
                                (403, {'errors': [{'code': 'DENIED'}]}), (429, {}),
                                (500, {}), (404, {'errors': [{'code': 'NAME_UNKNOWN'}]}),
                                (404, {'message': 'not found'})):
            with self.subTest(status=status, payload=payload), self.assertRaises(M.Refusal):
                M.tag_state(lambda path: (status, {}, raw(payload)), 'dev-' + 'a' * 40 + '-arm64')
        self.assertIsNone(M.tag_state(lambda path: (404, {}, raw({'errors': [{'code': 'MANIFEST_UNKNOWN'}]})),
                                      'dev-' + 'a' * 40 + '-arm64'))
        self.assertEqual(M.tag_state(self.fetch, 'dev-' + 'a' * 40 + '-arm64'), digest(self.manifest))

    def test_graph_binds_raw_manifest_config_platform_labels_and_all_blobs(self):
        public = self.graph(blobs=True)
        graph = public['graph']
        self.assertEqual(graph['manifest_digest'], digest(self.manifest))
        self.assertEqual(graph['config_digest'], digest(self.config))
        self.assertEqual(graph['layers'][0]['digest'], digest(self.layer))
        self.assertEqual(base64.b64decode(graph['manifest_base64']), self.manifest)
        self.assertIn('/blobs/' + digest(self.layer), self.fetches[-1])
        self.assertIsNone(graph['variant'])

    def test_graph_rejects_index_wrong_platform_config_and_source(self):
        for change in ('index', 'architecture', 'source', 'digest', 'size', 'duplicate', 'user'):
            with self.subTest(change=change):
                manifest, config = self.manifest, self.config
                if change == 'index':
                    value = json.loads(self.manifest); value['mediaType'] = M.INDEX; self.manifest = raw(value)
                elif change in ('architecture', 'source', 'user'):
                    value = json.loads(self.config)
                    if change == 'architecture': value['architecture'] = 'amd64'
                    elif change == 'user': value['config']['User'] = '0'
                    else: value['config']['Labels']['org.opencontainers.image.revision'] = 'f' * 40
                    self.config = raw(value)
                    value = json.loads(self.manifest)
                    value['config'].update(digest=digest(self.config), size=len(self.config))
                    self.manifest = raw(value)
                elif change in ('digest', 'size'):
                    value = json.loads(self.manifest)
                    value['config']['digest' if change == 'digest' else 'size'] = 'sha256:' + '0' * 64 if change == 'digest' else 1
                    self.manifest = raw(value)
                else:
                    self.manifest = self.manifest.replace(b'"schemaVersion":2', b'"schemaVersion":2,"schemaVersion":2')
                with self.assertRaises(M.Refusal): self.graph(blobs=True)
                self.manifest, self.config = manifest, config

    def test_graph_rejects_blob_size_or_digest_mismatch(self):
        self.layer += b'foreign'
        with self.assertRaises(M.Refusal): self.graph(blobs=True)

    def test_existing_tag_requires_exact_retained_public_receipt(self):
        retained = M.publication_receipt(self.source, 'arm64', 'd' * 64, 'e' * 64, self.smoke,
                                         self.graph(), self.graph(blobs=True))
        M.require_reusable(retained, self.source, 'arm64', self.graph(blobs=True))
        for key in ('tag', 'source', 'manifest_digest', 'executable_sha256', 'public_receipt', 'status', 'resolved_image'):
            bad = copy.deepcopy(retained); bad[key] = None
            with self.subTest(key=key), self.assertRaises(M.Refusal):
                M.require_reusable(bad, self.source, 'arm64', self.graph(blobs=True))

    def test_push_unknown_outcome_never_retries(self):
        events = []
        def push():
            events.append('push')
            raise M.Refusal('command failed')
        with self.assertRaisesRegex(M.Refusal, 'PUSH_OUTCOME_UNKNOWN'):
            M.push_once(push, lambda: events.append('readback'))
        self.assertEqual(events, ['push', 'readback'])

    def test_public_failure_never_yields_qualifying_image(self):
        receipt = M.publication_receipt(self.source, 'arm64', 'd' * 64, 'e' * 64, self.smoke,
                                        self.graph(), None)
        self.assertEqual(receipt['status'], 'PUBLICITY_PENDING')
        self.assertIsNone(receipt['public_receipt'])
        self.assertIsNone(receipt['resolved_image'])
        receipt = M.publication_receipt(self.source, 'arm64', 'd' * 64, 'e' * 64, self.smoke,
                                        self.graph(), self.graph(blobs=True))
        self.assertEqual(receipt['status'], 'PUBLIC')
        self.assertEqual(receipt['resolved_image']['ref'], M.PACKAGE + '@' + digest(self.manifest))
        self.assertEqual(set(receipt['resolved_image']['source']), set(self.source) |
                         {'build_input_sha256', 'executable_sha256', 'smoke_receipt_sha256'})

    def test_command_diagnostics_do_not_include_captured_secrets(self):
        with patch.object(M.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, b'credential-canary', b'private-path')):
            with self.assertRaises(M.Refusal) as error:
                M.command(['podman', 'push', 'credential-canary'])
        self.assertNotIn('credential-canary', str(error.exception))
        self.assertNotIn('private-path', str(error.exception))

    def test_clean_complete_source_inventory_matches_guard_encoding(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            subprocess.run(['git', '-C', str(root), 'config', 'user.email', 'fixture@example.invalid'], check=True)
            subprocess.run(['git', '-C', str(root), 'config', 'user.name', 'Fixture'], check=True)
            (root / 'a').write_bytes(b'fixture')
            subprocess.run(['git', '-C', str(root), 'add', 'a'], check=True)
            subprocess.run(['git', '-C', str(root), 'commit', '-qm', 'fixture'], check=True)
            with patch.object(M, 'REPOSITORY', M.REPOSITORY):
                source = M.source_identity(root)
            expected = hashlib.sha256(raw([['a', 420, hashlib.sha256(b'fixture').hexdigest()]])).hexdigest()
            self.assertEqual(source['payload_inventory_sha256'], expected)
            (root / 'untracked').write_text('untracked')
            with self.assertRaisesRegex(M.Refusal, 'clean'): M.source_identity(root)


    def test_existing_receipt_rejects_extra_fields_and_pending_needs_verify_only(self):
        public = self.graph(blobs=True)
        receipt = M.publication_receipt(self.source, 'arm64', 'd' * 64, 'e' * 64, self.smoke, self.graph(), public)
        receipt['credential'] = 'do-not-retain'
        with self.assertRaises(M.Refusal): M.require_reusable(receipt, self.source, 'arm64', public)
        pending = M.publication_receipt(self.source, 'arm64', 'd' * 64, 'e' * 64, self.smoke, self.graph(), None)
        with self.assertRaises(M.Refusal): M.require_reusable(pending, self.source, 'arm64', public)
        M.require_reusable(pending, self.source, 'arm64', public, allow_pending=True)

    def test_build_context_contains_only_tracked_payload_not_ignored_private_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / 'source'; root.mkdir()
            output = Path(temporary).resolve() / 'context'; output.mkdir()
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            (root / '.gitignore').write_text('private.env\n')
            (root / 'Dockerfile').write_text('FROM scratch\n')
            (root / 'private.env').write_text('credential-canary')
            subprocess.run(['git', '-C', str(root), 'add', '.gitignore', 'Dockerfile'], check=True)
            M.copy_build_context(root, output)
            self.assertEqual(sorted(path.name for path in output.iterdir()), ['.gitignore', 'Dockerfile'])

    def test_help_smoke_accepts_standard_go_stderr_without_logging_it(self):
        result = subprocess.CompletedProcess([], 0, b'', b'--metrics-bind-address --health-probe-bind-address')
        with patch.object(M.subprocess, 'run', return_value=result):
            self.assertEqual(M.command(['manager', '--help'], combined=True), result.stderr)


    def test_package_linkage_visibility_and_name_are_checked_before_absence(self):
        package = dict(name='pgcopydb-operator', package_type='container', visibility='public',
                       repository=dict(full_name='ydixken/pgcopydb-operator'))
        M.require_package(200, raw(package))
        for key, value in (('repository', dict(full_name='foreign/project')), ('visibility', 'private'),
                           ('name', 'other'), ('package_type', 'npm')):
            with self.subTest(key=key), self.assertRaises(M.Refusal):
                M.require_package(200, raw(dict(package, **{key: value})))
        for status in (401, 403, 404, 429, 500):
            with self.subTest(status=status), self.assertRaises(M.Refusal):
                M.require_package(status, raw(package))

    def test_unknown_push_retains_outcome_even_when_readback_is_uncertain(self):
        events = []
        def fail():
            events.append('write')
            raise M.Refusal('unknown')
        def uncertain():
            events.append('inspect')
            raise M.Refusal('unreachable')
        with self.assertRaisesRegex(M.Refusal, 'PUSH_OUTCOME_UNKNOWN'):
            M.push_once(fail, uncertain)
        self.assertEqual(events, ['write', 'inspect'])

    def test_partial_publication_saves_pending_before_anonymous_failure(self):
        import argparse
        source = self.source
        build_input = M.build_input_sha(source, 'arm64')
        self.labels = M.build_labels(source, build_input)
        config = json.loads(self.config); config['config']['Labels'] = self.labels
        self.config = raw(config)
        manifest = json.loads(self.manifest)
        manifest['config'].update(digest=digest(self.config), size=len(self.config))
        self.manifest = raw(manifest)
        events = []
        class FakeRegistry:
            def __init__(inner, *args):
                inner.authenticated = bool(args)
                if not args: raise M.Refusal('anonymous-denied-canary')
            def namespace(inner): events.append('namespace')
            def __call__(inner, path, **kwargs):
                if '/manifests/dev-' in path and 'push' not in events:
                    return 404, {}, raw({'errors': [{'code': 'MANIFEST_UNKNOWN'}]})
                return self.fetch(path, **kwargs)
        def command(argv, **kwargs):
            if argv[:2] == ['podman', 'build']:
                events.append('build')
                Path(argv[argv.index('--iidfile') + 1]).write_text(digest(self.config))
            elif argv[:2] == ['podman', 'push']:
                events.append('push')
                self.assertIn('--retry=0', argv)
                Path(argv[argv.index('--digestfile') + 1]).write_text(digest(self.manifest))
            return b''
        observed = dict(self.smoke, image_id=digest(self.config))
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve() / 'receipts'
            args = argparse.Namespace(architecture='arm64', output_dir=str(directory), retained_receipt=None, verify_only=False)
            with patch.object(M, 'source_identity', return_value=source), patch.object(M, 'trusted_context'), \
                 patch.object(M, 'Registry', FakeRegistry), patch.object(M, 'command', side_effect=command), \
                 patch.object(M, 'copy_build_context'), patch.object(M, 'smoke', return_value=observed), patch.dict(M.os.environ, {'GITHUB_TOKEN': 'secret', 'GITHUB_ACTOR': 'actor'}):
                with self.assertRaises(M.Refusal): M.publish(args)
            receipt = json.loads((directory / 'publication-receipt.json').read_bytes())
            self.assertEqual(receipt['status'], 'PUBLICITY_PENDING')
            self.assertIsNone(receipt['resolved_image'])
            self.assertEqual(events.count('push'), 1)
            self.assertNotIn('secret', (directory / 'publication-receipt.json').read_text())


    def test_source_rejects_assume_unchanged_payload_that_differs_from_commit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            for argv in (['git', 'init', '-q'], ['git', 'config', 'user.email', 'fixture@example.invalid'],
                         ['git', 'config', 'user.name', 'Fixture']):
                subprocess.run(argv, cwd=root, check=True)
            (root / 'a').write_text('committed')
            subprocess.run(['git', 'add', 'a'], cwd=root, check=True)
            subprocess.run(['git', 'commit', '-qm', 'fixture'], cwd=root, check=True)
            subprocess.run(['git', 'update-index', '--assume-unchanged', 'a'], cwd=root, check=True)
            (root / 'a').write_text('uncommitted payload')
            self.assertFalse(subprocess.check_output(['git', 'status', '--porcelain'], cwd=root))
            with self.assertRaises(M.Refusal): M.source_identity(root)

    def test_native_smoke_binds_binary_metadata_and_does_not_claim_runtime_build_info(self):
        image_id = digest(self.config)
        metadata = ('/manager: go1.27.0\n\tmod\tgithub.com/ydixken/pgcopydb-operator\t(devel)\n'
                    '\tbuild\tGOOS=linux\n\tbuild\tGOARCH=arm64\n\tbuild\tCGO_ENABLED=0\n'
                    '\tbuild\t-ldflags="-X main.version=m7-' + self.source['payload_inventory_sha256'][:12] + '"\n').encode()
        def fake(argv, **kwargs):
            if argv[:3] == ['podman', 'image', 'inspect']:
                return raw([dict(Id=image_id, Os='linux', Architecture='arm64', Config=dict(User='65532:65532', Entrypoint=['/manager']))])
            if argv[:2] == ['podman', 'run']:
                self.assertIn('--pull=never', argv); self.assertIn('--read-only', argv)
                self.assertTrue(kwargs['combined'])
                return b'--metrics-bind-address --health-probe-bind-address'
            if argv[:2] == ['podman', 'create']: return b'a' * 64
            if argv[:2] == ['podman', 'cp']: Path(argv[-1]).write_bytes(b'manager executable')
            if argv[:3] == ['go', 'version', '-m']: return metadata
            return b''
        with tempfile.TemporaryDirectory() as temporary, patch.object(M, 'command', side_effect=fake):
            receipt = M.smoke(image_id, self.source, 'arm64', Path(temporary))
            self.assertEqual(receipt['executable_sha256'], hashlib.sha256(b'manager executable').hexdigest())
            self.assertFalse(receipt['build_info_observed'])
            metadata = metadata.replace(b'GOARCH=arm64', b'GOARCH=amd64')
            with self.assertRaises(M.Refusal): M.smoke(image_id, self.source, 'arm64', Path(temporary))

    def test_redirect_never_forwards_authorization_and_rejects_foreign_origin(self):
        import urllib.request
        request = urllib.request.Request('https://ghcr.io/v2/blob', headers={'Authorization': 'Bearer credential-canary'})
        redirect = M.SafeRedirect()
        redirected = redirect.redirect_request(request, None, 307, 'redirect', {}, 'https://pkg-containers.githubusercontent.com/blob')
        self.assertFalse(redirected.has_header('Authorization'))
        for url in ('http://ghcr.io/blob', 'https://foreign.invalid/blob', 'https://user:password@ghcr.io/blob'):
            with self.subTest(url=url), self.assertRaises(M.Refusal):
                redirect.redirect_request(request, None, 307, 'redirect', {}, url)


    def test_local_verification_requires_pending_source_and_requests_read_only_registry(self):
        pending = M.publication_receipt(self.source, 'arm64', 'd' * 64, 'e' * 64, self.smoke, self.graph(), None)
        registry = unittest.mock.Mock()
        with patch.object(M, 'native_architecture'), patch.object(M, 'command', return_value=(M.REPOSITORY + '.git').encode()), \
             patch.object(M, 'Registry', return_value=registry) as factory, patch.dict(M.os.environ, {'GH_TOKEN': 'credential-canary'}):
            self.assertIs(M.local_verification_registry(self.source, 'arm64', pending, Path('/source')), registry)
            factory.assert_called_once_with('ydixken', 'credential-canary', read_only=True)
            registry.owner.assert_called_once_with()
            for change in ({'status': 'PUBLIC'}, {'source': dict(self.source, commit='f' * 40)}):
                with self.subTest(change=change), self.assertRaises(M.Refusal):
                    M.local_verification_registry(self.source, 'arm64', dict(pending, **change), Path('/source'))

    def test_local_owner_proof_is_exact_and_does_not_accept_push_without_admin(self):
        user = raw({'login': 'ydixken'})
        repo = raw({'full_name': 'ydixken/pgcopydb-operator', 'permissions': {'admin': True}})
        M.require_owner(200, user, 200, repo)
        for status, account, repository in ((401, user, repo), (200, raw({'login': 'other'}), repo),
                                              (200, user, raw({'full_name': 'other/repo', 'permissions': {'admin': True}})),
                                              (200, user, raw({'full_name': 'ydixken/pgcopydb-operator', 'permissions': {'push': True}}))):
            with self.subTest(status=status, account=account), self.assertRaises(M.Refusal):
                M.require_owner(status, account, 200, repository)


    def test_local_pending_continuation_only_reads_registry_and_upgrades_exact_receipt(self):
        import argparse
        source = self.source
        build_input = M.build_input_sha(source, 'arm64')
        self.labels = M.build_labels(source, build_input)
        config = json.loads(self.config); config['config']['Labels'] = self.labels
        self.config = raw(config)
        manifest = json.loads(self.manifest)
        manifest['config'].update(digest=digest(self.config), size=len(self.config))
        self.manifest = raw(manifest)
        smoke = dict(self.smoke, image_id=digest(self.config))
        pending = M.publication_receipt(source, 'arm64', build_input, 'e' * 64, smoke, self.graph(), None)
        events = []
        class FakeRegistry:
            def __init__(inner, *args, **kwargs): events.append(('client', kwargs.get('read_only')))
            def owner(inner): events.append(('owner',))
            def namespace(inner): events.append(('package',))
            def __call__(inner, path, **kwargs): return self.fetch(path, **kwargs)
        def readonly_command(argv, **kwargs):
            self.assertEqual(argv, ['git', 'remote', 'get-url', 'origin'])
            return M.REPOSITORY.encode()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            retained = root / 'pending.json'; M.write_private(retained, pending)
            output = root / 'verified'
            args = argparse.Namespace(architecture='arm64', output_dir=str(output), retained_receipt=str(retained), verify_only=True)
            with patch.object(M, 'source_identity', return_value=source), patch.object(M, 'Registry', FakeRegistry), \
                 patch.object(M, 'command', side_effect=readonly_command), \
                 patch.object(M, 'native_architecture', side_effect=AssertionError('no local runtime work')), \
                 patch.object(M, 'anonymous_smoke', side_effect=AssertionError('no local runtime work')), \
                 patch.dict(M.os.environ, {'GH_TOKEN': 'credential-canary', 'GITHUB_ACTIONS': 'false'}):
                M.publish(args)
            verified = json.loads((output / 'publication-receipt.json').read_bytes())
            self.assertEqual(verified['status'], 'PUBLIC')
            self.assertEqual(verified['smoke_receipt'], pending['smoke_receipt'])
            self.assertEqual(verified['manifest_digest'], pending['manifest_digest'])
            self.assertIn(('client', True), events)
            self.assertEqual(json.loads(retained.read_bytes())['status'], 'PUBLICITY_PENDING')

    def test_workflow_has_exact_trusted_branch_and_serialized_native_jobs(self):
        text = Path(__file__).parents[1].joinpath('.github/workflows/dev-manager-image.yml').read_text()
        self.assertIn('feat/rpgmigrate-native-operator', text)
        self.assertIn('dev-ghcr-ydixken-pgcopydb-operator', text)
        self.assertIn('cancel-in-progress: false', text)
        self.assertIn('max-parallel: 1', text)
        self.assertIn('ubuntu-24.04-arm', text)
        self.assertIn('ubuntu-24.04', text)
        self.assertIn('packages: write', text)
        self.assertNotIn('pull_request', text)
        self.assertNotIn('workflow_dispatch', text)
        self.assertNotIn('setup-qemu', text)
        self.assertNotIn('self-hosted', text)
        import re
        for action in re.findall(r'uses: ([^\s]+)', text): self.assertRegex(action, r'@[0-9a-f]{40}$')
        self.assertIn('persist-credentials: false', text)
        self.assertIn('GITHUB_TOKEN:', text)
        self.assertNotIn('docker/login', text)


if __name__ == '__main__':
    unittest.main()

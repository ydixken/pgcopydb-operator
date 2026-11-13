#!/usr/bin/env python3
import copy
import base64
import importlib.util
import hashlib
import io
import json
import os
import subprocess
from pathlib import Path
import tempfile
import signal
import sys
import tarfile

sys.dont_write_bytecode = True
import unittest
from unittest.mock import patch

# The guard refuses volatile /tmp roots, so fixtures use the reboot-safe temporary root.
tempfile.tempdir = str(Path('/var/tmp').resolve())

SCRIPT = Path(__file__).with_name('e2e-kind.py')
SPEC = importlib.util.spec_from_file_location('e2e_kind', SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC) if SCRIPT.exists() else None
if MODULE:
    SPEC.loader.exec_module(MODULE)


PGCOPYDB_STDOUT = (
    'pgcopydb version 0.18.144.g86bf908\n'
    'compiled with PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit\n'
    'compatible with Postgres 14, 15, 16, 17, and 18\n')


VERSION_STDOUT = {
    'pgcopydb': PGCOPYDB_STDOUT,
    'psql': 'psql (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2)\n',
    'pg_dump': 'pg_dump (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2)\n',
    'pg_restore': 'pg_restore (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2)\n',
    'timeout': 'timeout (GNU coreutils) 9.7\nPackaged by Debian (9.7-3)\nCopyright (C) 2025 Free Software Foundation, Inc.\nLicense GPLv3+: GNU GPL version 3 or later <https://gnu.org/licenses/gpl.html>.\nThis is free software: you are free to change and redistribute it.\nThere is NO WARRANTY, to the extent permitted by law.\n\nWritten by Padraig Brady.\n',
    'sed': 'sed (GNU sed) 4.9\nPackaged by Debian\nCopyright (C) 2022 Free Software Foundation, Inc.\nLicense GPLv3+: GNU GPL version 3 or later <https://gnu.org/licenses/gpl.html>.\nThis is free software: you are free to change and redistribute it.\nThere is NO WARRANTY, to the extent permitted by law.\n\nWritten by Jay Fenlason, Tom Lord, Ken Pizzini,\nPaolo Bonzini, Jim Meyering, and Assaf Gordon.\n\nThis sed program was built with SELinux support.\nSELinux is disabled on this system.\n\nGNU sed home page: <https://www.gnu.org/software/sed/>.\nGeneral help using GNU software: <https://www.gnu.org/gethelp/>.\nE-mail bug reports to: <bug-sed@gnu.org>.\n'}


def runner_packet(binary_sha256):
    values = {'uid': '65532', 'gid': '65532', 'workdir': '/work', 'binary_sha256': binary_sha256,
              'sed_escape': 'a\\:b', 'compare_progress_marker': 'present'}
    for tool in ('pgcopydb', 'psql', 'pg_dump', 'pg_restore'):
        values[tool + '_path'] = '/usr/lib/postgresql/18/bin/' + tool
    for tool, stdout in VERSION_STDOUT.items():
        values[tool + '_version_b64'] = MODULE.base64.b64encode(stdout.encode() + b'\0' + b'0').decode()
    return ''.join(key + '=' + value + '\n' for key, value in values.items())


class RunnerPacketTests(unittest.TestCase):
    def test_full_three_line_version_and_multiline_utility_output_are_framed(self):
        actual = MODULE.Guard.parse_runner_probe(runner_packet('a' * 64))
        self.assertEqual(actual['pgcopydb_version'], 'pgcopydb version 0.18.144.g86bf908')
        self.assertEqual(actual['timeout_version'], 'timeout (GNU coreutils) 9.7')
        self.assertEqual(actual['sed_version'], 'sed (GNU sed) 4.9')
        self.assertEqual(actual['binary_sha256'], 'a' * 64)

    def test_packet_rejects_malformed_frames_without_prefix_qualification(self):
        good = runner_packet('a' * 64)
        records = dict(line.split('=', 1) for line in good.splitlines())
        def version(raw, key='pgcopydb_version_b64'):
            altered = dict(records, **{key: MODULE.base64.b64encode(raw).decode()})
            return ''.join(k + '=' + v + '\n' for k, v in altered.items())
        invalid = [good[:-1], good + 'uid=65532\n', good + 'unknown=value\n',
                   good.replace('uid=65532\n', ''), good + '\n', good.replace('uid=65532', 'uid=65532\ninjected'),
                   good.replace('uid=65532', 'uid=65532\r'), good.replace('uid=65532', 'uid=\0'),
                   good.replace('uid=65532', 'uid=' + 'x' * 65536), good + '\ud800']
        for raw in (b'', b'\0', PGCOPYDB_STDOUT.encode(), PGCOPYDB_STDOUT.encode() + b'\0',
                    PGCOPYDB_STDOUT.encode() + b'\0' + b'7', PGCOPYDB_STDOUT.encode() + b'\0' + b'00',
                    PGCOPYDB_STDOUT.encode() + b'\0-0', PGCOPYDB_STDOUT.encode() + b'\0' + b'0\n',
                    PGCOPYDB_STDOUT.encode() + b'\0\00', b'prefix\0suffix\00',
                    b'pgcopydb version 0.18.144.g86bf908\n\00', PGCOPYDB_STDOUT.replace('18.6', '18.7').encode() + b'\0' + b'0',
                    PGCOPYDB_STDOUT.encode() + b'\n\00', PGCOPYDB_STDOUT.encode().replace(b'\n', b'\r\n') + b'\0' + b'0'):
            invalid.append(version(raw))
        for raw in (b'\xff\00', b'\r\00', b'\n\00', b'x' * 8193 + b'\0' + b'0'):
            invalid.append(version(raw, 'sed_version_b64'))
        for encoded in ('%%%', 'YQ=', 'YR==', 'YQ===', records['pgcopydb_version_b64'][:-1]):
            invalid.append(good.replace(records['pgcopydb_version_b64'], encoded))
        for number, raw in enumerate(invalid):
            with self.subTest(number=number), self.assertRaises(MODULE.Refusal): MODULE.Guard.parse_runner_probe(raw)
        self.assertEqual(MODULE.Guard.parse_runner_probe(version(PGCOPYDB_STDOUT.rstrip('\n').encode() + b'\0' + b'0'))['pgcopydb_version'],
                         'pgcopydb version 0.18.144.g86bf908')



class PreparationEntryTests(unittest.TestCase):
    def test_prepare_is_an_existing_guard_operation(self):
        self.assertTrue(callable(getattr(MODULE.Guard, 'prepare', None)))

    def test_up_requires_explicit_bounded_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            guard = MODULE.Guard(Path(directory).resolve(), environ={})
            for value in (None, True, 29, 7201, '1800', 1800.0, float('inf'), float('nan')):
                with self.subTest(value=value), self.assertRaises(MODULE.Refusal):
                    guard.up(value)

    def test_cli_refuses_missing_up_lease_before_creating_a_run(self):
        with patch.object(MODULE.sys, 'argv', ['e2e-kind.py', 'up', '--run-dir', '/private/tmp/unused-m7-cli-test']), \
                patch.object(MODULE, 'Guard', side_effect=AssertionError('guard must not be constructed')), \
                patch.object(MODULE.sys, 'stderr', io.StringIO()), self.assertRaises(SystemExit) as failure:
            MODULE.main()
        self.assertEqual(failure.exception.code, 2)


BATCHED = 'batched-read'


class FakeCommands:
    def __init__(self, ledger):
        self.calls = []
        self.ledger = ledger
        self.ids = [str(n) * 64 for n in range(1, 4)]
        self.marker = ledger['run_id']
        self.markers = {}
        self.kubernetes_labels = {}
        self.kind_run_labels = True
        self.nodes = {str(n) * 64: {'name': ledger['cluster'] + suffix, 'role': role}
                      for n, suffix, role in zip(range(1, 4), ['-control-plane', '-worker', '-worker2'], ['control-plane', 'worker', 'worker'])}
        self.provider_order = list(self.ids)
        self.unrelated_nodes = {}
        self.namespaces = [{'metadata': {'name': name, 'uid': 'fixture-' + name}}
                           for name in ('default', 'kube-system', 'kube-public', 'kube-node-lease', 'local-path-storage')]
        self.readyz = 'ok\n'
        self.fail_listing = False
        self.foreign = False
        self.volumes = {}
        self.foreign_volume_user = False
        self.fail_volume_lookup = False
        self.fail_volume_remove = False
        self.config = {'apiVersion': 'v1', 'kind': 'Config', 'preferences': {}, 'current-context': ledger['context'],
                       'contexts': [{'name': ledger['context'], 'context': {'cluster': ledger['context'], 'user': ledger['context']}}],
                       'clusters': [{'name': ledger['context'], 'cluster': {'server': 'https://127.0.0.1:36443', 'certificate-authority-data': 'aW5lcnQtZml4dHVyZS1jYQ=='}}],
                       'users': [{'name': ledger['context'], 'user': {'client-certificate-data': 'aW5lcnQtZml4dHVyZS1jZXJ0', 'client-key-data': 'aW5lcnQtZml4dHVyZS1rZXk='}}]}
        self.capacity = {'cpus': 14, 'memTotal': 32 * 1024 ** 3, 'memFree': 1}
        self.meminfo = 'MemTotal: 33554432 kB\nMemFree: 1 kB\nMemAvailable: 33554432 kB\n'
        self.node_image_id = 'a' * 64
        self.node_image_overrides = {}
        self.proc_cgroup_overrides = {}
        self.proc_stat_overrides = {}
        self.node_states = {cid: {'Pid': 1001 + i, 'Running': True, 'StartedAt': '2026-01-01T00:00:00.123456789Z'} for i, cid in enumerate(self.ids)}
        self.node_hosts = {cid: {'CpuQuota': 0, 'CpuPeriod': 0, 'NanoCpus': 0, 'Memory': 0, 'MemorySwap': 0} for cid in self.ids}
        self.node_ticks = {cid: 100001 + i for i, cid in enumerate(self.ids)}
        self.node_cgroups = {cid: '/user.slice/user-1000.slice/user@1000.service/user.slice/libpod-' + cid + '.scope/container' for cid in self.ids}
        self.cgroup_fs = 'cgroup2fs'
        self.cgroup_mountinfo = '42 1 0:42 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw\n'
        self.cgroup_overrides = {}
        self.canonical_overrides = {}
        self.fail_update = None
        self.update_returns = {}
        self.image = {'Id': 'sha256:' + 'a' * 64, 'Digest': 'sha256:' + 'b' * 64,
                      'Architecture': 'arm64', 'Os': 'linux', 'RepoDigests': ['example.invalid/image@sha256:' + 'b' * 64]}
        # Each read of a batched VM call goes through this hook as if it were its own ssh.
        self.batched = self
        self.podman_version = 'podman version 6.2.0\n'

    def kubeconfig_bytes(self):
        # JSON is kubeconfig YAML; all embedded credential bytes are inert placeholders.
        return (json.dumps(self.config, sort_keys=True) + '\n').encode()

    def seed_kubernetes_nodes(self):
        # Kind passes configured labels to kubeadm; these API objects remain synthetic.
        self.kubernetes_labels = {node['name']: {'kubernetes.io/hostname': node['name'],
            'kubernetes.io/os': 'linux', **({MODULE.RUN_LABEL: self.marker} if self.kind_run_labels else {})}
            for node in self.nodes.values()}

    def __call__(self, args, env=None, input=None, timeout=120, umask=0o022):
        self.calls.append((args, env, input))
        batch = MODULE.shlex.split(args[4]) if args[:4] == ['podman', 'machine', 'ssh', '--'] and len(args) == 5 else []
        if batch[:4] == ['sh', '-c', MODULE.VM_BATCH, 'm7-batch']:
            lines = []
            for command in batch[4:]:
                try:
                    output, status = self.batched(['podman', 'machine', 'ssh', '--', command], env=BATCHED), 0
                except MODULE.CommandError as error:
                    output, status = '', error.returncode or 1
                lines.append(str(status) + ' ' + base64.b64encode(output.encode()).decode() + '\n')
            return ''.join(lines)
        if args[:2] == ['kind', 'create']:
            self.ids = list(self.provider_order)
            expected = (MODULE.ROOT / 'test/e2e/kind/cluster.yaml').read_text().replace('RUN_ID', self.marker).replace(
                'STORAGE_HOST_PATH', json.dumps(str(Path(args[args.index('--config') + 1]).parent / 'node-default-storage.yaml')))
            assert Path(args[args.index('--config') + 1]).read_text() == expected
            assert expected.count('  - role: control-plane\n') == 1
            assert expected.count('  - role: worker\n') == 2
            assert expected.count('      ' + MODULE.RUN_LABEL + ': ' + self.marker + '\n') == 3
            assert expected.count('kubeadmConfigPatches:\n  - |\n    kind: KubeletConfiguration\n    podPidsLimit: 4096\n') == 1
            assert expected.count('podPidsLimit') == 1
            self.seed_kubernetes_nodes()
            self.volumes = {letter * 64: {'Name': letter * 64, 'CreatedAt': '2026-11-08T12:00:00Z', 'Driver': 'local',
                'Labels': {self.ledger['cluster'] + suffix: 'true'}} for letter, suffix in zip('abc', ['-control-plane', '-worker', '-worker2'])}
            Path(args[args.index('--kubeconfig') + 1]).write_bytes(self.kubeconfig_bytes())
            return ''
        if args[:2] == ['kind', 'version']:
            return 'kind v0.33.0 go1.27.0 linux/arm64\n'
        if args == ['podman', '--version']:
            return self.podman_version
        if args[:2] == ['podman', 'info']:
            return json.dumps({'host': self.capacity})
        if args == ['podman', 'machine', 'ssh', '--', 'cat', '/proc/meminfo']:
            return self.meminfo
        if args == ['podman', 'machine', 'ssh', '--', 'df -B1 --output=avail /var']:
            return 'Avail\n21474836480\n'
        if args[:2] == ['helm', 'version'] or args == ['task', '--version'] or args[:2] == ['kubectl', 'version']:
            return '{}'
        if args[:3] == ['podman', 'volume', 'ls']:
            if self.fail_volume_lookup:
                raise MODULE.CommandError('CalledProcessError')
            return json.dumps([{'Name': name} for name in self.volumes])
        if args[:3] == ['podman', 'volume', 'inspect']:
            if self.fail_volume_lookup:
                raise MODULE.CommandError('CalledProcessError')
            return json.dumps([self.volumes[args[-1]]])
        if args[:3] == ['podman', 'volume', 'rm']:
            if self.fail_volume_remove:
                raise MODULE.CommandError('CalledProcessError')
            self.volumes.pop(args[-1])
            return ''
        if args[:2] == ['podman', 'ps']:
            if any(arg.startswith('volume=') for arg in args):
                name = next(arg.split('=', 1)[1] for arg in args if arg.startswith('volume='))
                labels = self.volumes[name]['Labels']
                return json.dumps([{'Id': ('f' * 64 if self.foreign_volume_user else cid)} for cid, node in self.nodes.items() if cid in self.ids and node['name'] in labels])
            if self.fail_listing:
                raise MODULE.Refusal('provider lookup: CommandError')
            entries = [{'Id': cid, 'Names': [self.nodes.get(cid, {'name': 'unowned'})['name']],
                        'Labels': {'io.x-k8s.kind.cluster': self.ledger['cluster'],
                                   'io.x-k8s.kind.role': self.nodes.get(cid, {'role': 'worker'})['role']}} for cid in self.ids]
            if '--filter' not in args:
                entries.extend(copy.deepcopy(list(self.unrelated_nodes.values())))
            return json.dumps(entries)
        if args[:2] == ['podman', 'inspect']:
            cid = args[-1]
            node = self.nodes[cid]
            return json.dumps([{'Id': cid, 'Image': self.node_image_overrides.get(cid, self.node_image_id), 'State': self.node_states[cid], 'HostConfig': self.node_hosts[cid], 'Name': node['name'],
                                'Mounts': [{'Type': 'volume', 'Name': name, 'Destination': '/var'} for name, volume in self.volumes.items() if volume['Labels'].get(node['name']) == 'true'],
                                'Config': {'Labels': {'io.x-k8s.kind.cluster': 'foreign' if self.foreign else self.ledger['cluster'],
                                                      'io.x-k8s.kind.role': node['role']}}}])
        if args[:2] == ['podman', 'update']:
            cid = args[-1]
            if cid == self.fail_update: raise MODULE.CommandError('CalledProcessError')
            self.node_hosts[cid] = {'CpuQuota': 200000, 'CpuPeriod': 100000, 'NanoCpus': 2000000000,
                                    'Memory': 4294967296, 'MemorySwap': 4294967296}
            return self.update_returns.get(cid, cid + '\n')
        if args[:4] == ['podman', 'machine', 'ssh', '--'] and len(args) == 5:
            cmd = MODULE.shlex.split(args[4])
            if cmd == ['stat', '-fc', '%T', '/sys/fs/cgroup']: return self.cgroup_fs + '\n'
            if cmd == ['cat', '/proc/self/mountinfo']: return self.cgroup_mountinfo
            if cmd == ['cat', '/proc/meminfo']: return self.meminfo
            if cmd[:3] == ['realpath', '-e', '--']:
                return ''.join(self.canonical_overrides.get(path, path) + '\n' for path in cmd[3:])
            if cmd[:1] == ['cat'] and len(cmd) == 2 and cmd[1].startswith('/proc/'):
                pid, name = cmd[1].split('/')[2:]
                cid = next(cid for cid, state in self.node_states.items() if state.get('Pid') == int(pid))
                if name == 'cgroup': return self.proc_cgroup_overrides.get(cid, '0::' + self.node_cgroups[cid] + '\n')
                if name == 'stat': return self.proc_stat_overrides.get(cid, str(pid) + ' (systemd init) ' + ' '.join(['S'] + ['0'] * 18 + [str(self.node_ticks[cid])] + ['0'] * 10) + '\n')
            if cmd[:1] == ['cat'] and all(path.startswith('/sys/fs/cgroup/') for path in cmd[1:]):
                values = []
                for path in cmd[1:]:
                    if path in ('/sys/fs/cgroup/cpu.max', '/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory.swap.max'):
                        raise MODULE.CommandError('CalledProcessError')
                    key = path.rsplit('/', 1)[1]
                    matches = [cid for cid in self.node_states if '/libpod-' + cid + '.scope' in path]
                    cid = matches[0] if matches else None
                    suffix = path.split('/libpod-' + cid + '.scope', 1)[1].rsplit('/', 1)[0] if cid else ''
                    level = 'scope' if cid and suffix == '' else 'container' if cid and suffix == '/container' else 'descendant' if cid else 'ancestor'
                    defaults = {'cpu.max': 'max 100000', 'memory.max': 'max', 'memory.swap.max': 'max'}
                    if cid and level in ('scope', 'container'):
                        defaults = {'cpu.max': '200000 100000', 'memory.max': '4294967296',
                                    'memory.swap.max': '4294967296' if level == 'scope' else '0'}
                    value = self.cgroup_overrides.get((cid, level, key), defaults[key])
                    if value is None: raise MODULE.CommandError('CalledProcessError')
                    values.append(value)
                return '\n'.join(values) + '\n'
        if (args[:2] == ['podman', 'exec'] and args[3:8] == ['ctr', '--namespace', 'k8s.io', 'images', 'list'] and
                args[8:] in ([], ['--quiet'])):
            return ''
        if args[:2] == ['podman', 'exec']:
            cid = args[args.index('--user') + 2]
            assert args[args.index('--user') + 1] == '0'
            if args[-1] == 'umask 077; set -C; cat > ' + MODULE.MARKER:
                if cid in self.markers:
                    raise MODULE.CommandError('CalledProcessError')
                body = input if '-i' in args or '--interactive' in args else ''
                self.markers[cid] = {'body': body, 'uid': 0, 'mode': 0o600}
                return ''
            assert args[-1] == 'test "$(stat -c %u:%a ' + MODULE.MARKER + ')" = 0:600 && cat ' + MODULE.MARKER
            marker = self.markers.get(cid)
            if marker is None or (marker['uid'], marker['mode']) != (0, 0o600):
                raise MODULE.CommandError('CalledProcessError')
            return marker['body']
        if args[:2] == ['podman', 'rm']:
            assert args[2] == '--force'
            requested = args[3:]
            assert requested and len(requested) == len(set(requested))
            assert set(requested) <= set(self.ids)
            self.ids = [cid for cid in self.ids if cid not in requested]
            return ''
        if args[:3] == ['podman', 'image', 'inspect']:
            return json.dumps([self.image])
        if args[0] == 'kubectl' and 'config' in args:
            try:
                return json.dumps(json.loads(Path(args[args.index('--kubeconfig') + 1]).read_bytes()))
            except (ValueError, UnicodeError):
                raise MODULE.CommandError('CalledProcessError') from None
        if args[0] == 'kubectl' and 'label' in args:
            assert args[args.index('label'):] == ['label', 'nodes', '--all', MODULE.RUN_LABEL + '=' + self.marker, '--overwrite=false']
            if any(labels.get(MODULE.RUN_LABEL, self.marker) != self.marker for labels in self.kubernetes_labels.values()):
                raise MODULE.CommandError('CalledProcessError')
            for labels in self.kubernetes_labels.values():
                labels[MODULE.RUN_LABEL] = self.marker
            return ''
        if args[0] == 'kubectl' and 'get' in args:
            if '--raw=/readyz' in args or args[-2:] == ['--raw', '/readyz']:
                return self.readyz
            if 'nodes' in args:
                return json.dumps({'apiVersion': 'v1', 'kind': 'NodeList', 'items': [
                    {'apiVersion': 'v1', 'kind': 'Node', 'metadata': {'name': name, 'uid': 'fixture-node-' + name, 'labels': labels},
                     'status': {'conditions': [{'type': 'Ready', 'status': 'True'}]}}
                    for name, labels in self.kubernetes_labels.items()]})
            if 'namespaces' in args:
                return json.dumps({'apiVersion': 'v1', 'kind': 'NamespaceList', 'items': self.namespaces})
            return '{"items": []}'
        raise AssertionError('unexpected external command: ' + repr(args))


class GuardFixture(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(MODULE, 'lifecycle wrapper is missing')
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name).resolve()
        self.directory.chmod(0o700)
        self.guard = MODULE.Guard(self.directory, environ={})
        self.ledger = self.guard.initialize()
        self.fake = FakeCommands(self.ledger)
        self.guard.command = self.fake
        self.ledger['nodes'] = [{'id': str(n) * 64, 'name': self.ledger['cluster'] + suffix, 'role': role}
                                for n, suffix, role in zip(range(1, 4), ['-control-plane', '-worker', '-worker2'],
                                                          ['control-plane', 'worker', 'worker'])]
        self.ledger['phase'] = 'ready'
        self.fake.ledger = self.ledger
        self.seed_ready_node_ownership()
        self.guard.save(self.ledger)
        self.guard.write_private('kubeconfig', self.fake.kubeconfig_bytes())
        self.ledger['kubeconfig_sha256'] = hashlib.sha256(self.fake.kubeconfig_bytes()).hexdigest()
        self.guard.save(self.ledger)

    def seed_ready_node_ownership(self):
        self.fake.markers = {node['id']: {'body': self.ledger['run_id'] + '\n', 'uid': 0, 'mode': 0o600} for node in self.ledger['nodes']}
        self.fake.seed_kubernetes_nodes()

    def prepared_fixture(self):
        self.guard.write_private('artifact-preparation-evidence.json', json.dumps({'schema': 1,
            'run_id': self.ledger['run_id'], 'complete': True, 'pending': None, 'source_root': str(MODULE.ROOT),
            'source_tree_sha256': self.guard.source_tree(), 'files': {}}).encode())

    def assert_no_mutations(self):
        for args, _, _ in self.fake.calls:
            self.assertNotEqual(args[:2], ['podman', 'rm'])
            self.assertNotEqual(args[:3], ['podman', 'volume', 'rm'])
            self.assertFalse(args[0] in ('kubectl', 'helm') and any(verb in args for verb in ('apply', 'create', 'delete', 'label', 'install', 'upgrade', 'uninstall')))


class LifecycleTests(GuardFixture):
    def test_hostile_environment_refuses_before_commands(self):
        for key, value in [('KUBECONFIG', '/foreign'), ('E2E_CONTEXT', 'foreign'),
                           ('EXPECT_CONTEXT', 'foreign'), ('KIND_EXPERIMENTAL_PROVIDER', 'docker'),
                           ('TASK_X_REMOTE_TASKFILES', '1'), ('KUBERNETES_MASTER', 'https://foreign'), ('PG_SOURCE', '18'), ('TASKFILE', '/foreign')]:
            with self.subTest(key=key), self.assertRaises(MODULE.Refusal):
                MODULE.Guard(self.directory, environ={key: value})
        self.assertEqual(self.fake.calls, [])


    def test_private_and_symlink_paths_refuse(self):
        self.directory.chmod(0o755)
        with self.assertRaises(MODULE.Refusal):
            MODULE.Guard(self.directory, environ={})
        self.directory.chmod(0o700)
        link = self.directory / 'link'
        link.symlink_to(self.directory, target_is_directory=True)
        with self.assertRaises(MODULE.Refusal):
            MODULE.Guard(link, environ={})
        with self.assertRaises(MODULE.Refusal):
            MODULE.Guard(Path('relative'), environ={})


    def test_run_directory_inside_git_or_worktree_refuses_before_creation(self):
        for marker_type in ('directory', 'file'):
            with self.subTest(marker_type=marker_type):
                repository = self.directory / marker_type
                repository.mkdir(mode=0o700)
                marker = repository / '.git'
                if marker_type == 'directory':
                    marker.mkdir()
                else:
                    marker.write_text('gitdir: /fixture/worktrees/owned\n')
                nested = repository / 'nested'
                nested.mkdir()
                run = nested / 'private-run'
                with self.assertRaisesRegex(MODULE.Refusal, 'outside Git'):
                    MODULE.Guard(run, environ={})
                self.assertFalse(run.exists())
                run.mkdir(mode=0o700)
                with self.assertRaises(MODULE.Refusal):
                    MODULE.Guard(run, environ={})
                self.assertEqual(list(run.iterdir()), [])


    def test_foreign_same_name_refuses_delete(self):
        self.fake.foreign = True
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_replaced_node_refuses_delete(self):
        self.fake.ids[1] = 'f' * 64
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_failed_provider_listing_refuses_delete(self):
        self.fake.fail_listing = True
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_missing_run_marker_refuses_delete(self):
        self.fake.markers['1' * 64]['body'] = ''
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_missing_ledger_never_adopts(self):
        (self.directory / 'ownership.json').unlink()
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_down_checks_exact_ids_then_proves_absence(self):
        self.guard.down()
        self.guard.down()
        deletions = [args for args, _, _ in self.fake.calls if args[:2] == ['podman', 'rm']]
        self.assertEqual(deletions, [['podman', 'rm', '--force', '1' * 64, '2' * 64, '3' * 64]])
        self.assertEqual(self.guard.read()['phase'], 'down')


    def test_empty_lookup_with_owned_id_still_present_refuses(self):
        original = self.fake
        def command(args, **kwargs):
            if args[:2] == ['podman', 'ps'] and '--filter' in args:
                original.calls.append((args, kwargs.get('env'), None))
                return '[]'
            return original(args, **kwargs)
        self.guard.command = command
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_private_kubeconfig_has_explicit_context_flags(self):
        self.guard.kubectl(['get', 'nodes', '-o', 'json'])
        args, env, _ = self.fake.calls[-1]
        self.assertEqual(args[:5], ['kubectl', '--kubeconfig', str(self.directory / 'kubeconfig'), '--context', self.ledger['context']])
        self.assertEqual(env['KUBECONFIG'], str(self.directory / 'kubeconfig'))


    def write_config(self, config):
        raw = config if isinstance(config, bytes) else (json.dumps(config, sort_keys=True) + '\n').encode()
        self.guard.write_private('kubeconfig', raw)
        self.ledger['kubeconfig_sha256'] = hashlib.sha256(raw).hexdigest()
        self.guard.save(self.ledger)


    def test_kind_prefixed_kubeconfig_is_read_from_private_bytes(self):
        self.assertEqual(json.loads((self.directory / 'kubeconfig').read_bytes()), self.fake.config)
        self.guard.kubeconfig(self.guard.read())
        self.assertEqual(len(self.fake.calls), 1)
        self.assertIn('config', self.fake.calls[0][0])


    def test_foreign_context_and_exec_credentials_refuse_requests(self):
        changes = {
            'unprefixed': lambda c: (c['contexts'][0]['context'].update(cluster=self.ledger['cluster'], user=self.ledger['cluster']),
                                     c['clusters'][0].update(name=self.ledger['cluster']), c['users'][0].update(name=self.ledger['cluster'])),
            'cross-run': lambda c: c['contexts'][0].update(name='kind-rpgmigrate-m7-' + 'f' * 32),
            'current-context': lambda c: c.update({'current-context': 'foreign'}),
            'cluster-reference': lambda c: c['contexts'][0]['context'].update(cluster='foreign'),
            'user-reference': lambda c: c['contexts'][0]['context'].update(user='foreign'),
            'cluster-name': lambda c: c['clusters'][0].update(name='foreign'),
            'user-name': lambda c: c['users'][0].update(name='foreign'),
            'extra-context': lambda c: c['contexts'].append(copy.deepcopy(c['contexts'][0])),
            'extra-cluster': lambda c: c['clusters'].append(copy.deepcopy(c['clusters'][0])),
            'extra-user': lambda c: c['users'].append(copy.deepcopy(c['users'][0])),
            'non-loopback': lambda c: c['clusters'][0]['cluster'].update(server='https://192.0.2.1:36443'),
            'exec': lambda c: c['users'][0]['user'].update(exec={'command': 'inert-fixture-command'}),
            'token': lambda c: c['users'][0].update(user={'token': 'inert-fixture-token'}),
            'file-key': lambda c: c['users'][0].update(user={'client-certificate': '/inert/cert', 'client-key': '/inert/key'}),
            'file-ca': lambda c: c['clusters'][0]['cluster'].update({'certificate-authority': '/inert/ca'}),
            'missing-context': lambda c: c.pop('contexts'),
        }
        for name, mutate in changes.items():
            with self.subTest(name=name):
                config = copy.deepcopy(self.fake.config)
                mutate(config)
                self.write_config(config)
                self.fake.calls.clear()
                with self.assertRaisesRegex(MODULE.Refusal, 'single owned loopback context'):
                    self.guard.kubectl(['delete', 'namespace', 'inert-fixture'])
                self.assertEqual(len(self.fake.calls), 1)
                self.assertIn('config', self.fake.calls[0][0])
                self.assert_no_mutations()
        self.write_config(b'{malformed kubeconfig')
        self.fake.calls.clear()
        with self.assertRaises(MODULE.CommandError):
            self.guard.kubectl(['get', 'nodes'])
        self.assertEqual(len(self.fake.calls), 1)
        self.assertIn('config', self.fake.calls[0][0])


    def test_wrong_kubeconfig_mode_refuses_before_request(self):
        (self.directory / 'kubeconfig').chmod(0o644)
        with self.assertRaises(MODULE.Refusal):
            self.guard.kubectl(['delete', 'namespace', 'anything'])
        self.assertEqual(self.fake.calls, [])


    def test_lease_required_before_create(self):
        with self.assertRaises(MODULE.Refusal):
            self.guard.require_lease()
        self.assert_no_mutations()


    def authorize_lease(self, **changes):
        self.ledger['resource_lease'] = {'authorized': True, 'expires_at': MODULE.time.time() + 120, 'monotonic_expires_at': MODULE.time.monotonic() + 120,
            'm7_cpus': 6, 'm7_memory_mib': 12288, 'm3_reserved_cpus': 6, 'm3_reserved_memory_mib': 6144}
        self.ledger['resource_lease'].update(changes)
        self.guard.save(self.ledger)


    def test_reclaimable_cache_uses_available_memory_for_authorized_sum(self):
        self.authorize_lease()
        self.fake.capacity.update(memTotal=32 * 1024 ** 3, memFree=979509248)
        self.fake.meminfo = 'MemTotal: 33554432 kB\nMemFree: 956552 kB\nMemAvailable: 26214400 kB\nCached: 8000000 kB\n'
        self.assertIsNone(self.guard.require_lease())
        self.assert_no_mutations()


    def test_authorized_fixed_budget_needs_only_the_exact_measured_admission_size(self):
        self.authorize_lease()
        self.fake.capacity.update(cpus=14, memTotal=24 * 1024 ** 3, memFree=1)
        self.fake.meminfo = 'MemTotal: 25165824 kB\nMemAvailable: 25165824 kB\n'
        self.assertIsNone(self.guard.require_lease())
        self.assert_no_mutations()


    def test_pressure_and_cpu_or_usable_total_shortfall_refuse(self):
        self.authorize_lease()
        cases = [({'cpus': 13, 'memTotal': 32 * 1024 ** 3}, 'MemTotal: 33554432 kB\nMemAvailable: 33554432 kB\n'),
                 ({'cpus': 14, 'memTotal': 32 * 1024 ** 3}, 'MemTotal: 33554432 kB\nMemAvailable: 24117248 kB\n'),
                 ({'cpus': 14, 'memTotal': 23 * 1024 ** 3}, 'MemTotal: 33554432 kB\nMemAvailable: 33554432 kB\n')]
        for capacity, meminfo in cases:
            with self.subTest(capacity=capacity, meminfo=meminfo):
                self.fake.capacity = capacity
                self.fake.meminfo = meminfo
                with self.assertRaises(MODULE.Refusal): self.guard.require_lease()
        self.assert_no_mutations()


    def test_missing_duplicate_or_malformed_available_memory_never_falls_back_to_free(self):
        self.authorize_lease()
        for meminfo in ['MemTotal: 12582912 kB\nMemFree: 12582912 kB\n',
                        'MemTotal: 12582912 kB\nMemAvailable: 12582912 kB\nMemAvailable: 12582912 kB\n',
                        'MemTotal: 12582912 kB\nMemAvailable: 12582912 MB\n',
                        'MemTotal: 12582912 kB\nMemAvailable: -1 kB\n',
                        'MemTotal: 12582912 kB\nMemAvailable: 12582913 kB\n']:
            with self.subTest(meminfo=meminfo):
                self.fake.meminfo = meminfo
                with self.assertRaises(MODULE.Refusal): self.guard.require_lease()
        self.fake.meminfo = 'MemTotal: 33554432 kB\nMemAvailable: 33554433 kB\n'
        with self.assertRaisesRegex(MODULE.Refusal, 'memory availability is unavailable or invalid'): self.guard.require_lease()
        self.assert_no_mutations()


    def test_resource_lease_limits_and_types_remain_strict(self):
        for changes in [{'authorized': False}, {'expires_at': 0}, {'m7_memory_mib': 12289}, {'m7_memory_mib': 8192}, {'m7_cpus': 3}, {'m3_reserved_cpus': 3}, {'m3_reserved_memory_mib': 4096},
                        {'m7_cpus': 7}, {'m3_reserved_cpus': 2}, {'m3_reserved_memory_mib': 4095},
                        {'m7_cpus': 0}, {'m7_cpus': True}, {'m7_memory_mib': -1}, {'m7_memory_mib': 6144.0},
                        {'m3_reserved_memory_mib': '4096'}, {'expires_at': float('inf')}, {'expires_at': float('nan')},
                        {'expires_at': True}, {'m3_reserved_cpus': 7}, {'m3_reserved_memory_mib': 6145},
                        {'m3_reserved_cpus': True}, {'m3_reserved_memory_mib': True}, {'unknown': 1}]:
            with self.subTest(changes=changes):
                self.authorize_lease(**changes)
                with self.assertRaises(MODULE.Refusal): self.guard.require_lease()
        self.assert_no_mutations()


    def test_nonobject_lease_refuses_and_expired_authority_still_allows_exact_cleanup(self):
        for value in (None, [], 'invalid'):
            self.ledger['resource_lease'] = value;self.guard.save(self.ledger)
            with self.assertRaises(MODULE.Refusal): self.guard.require_lease()
        self.authorize_lease(expires_at=0)
        self.guard.down()
        self.assertEqual(self.guard.read()['phase'], 'down')


    def test_capacity_failure_or_invalid_fields_refuse(self):
        self.authorize_lease()
        original = self.fake
        def command(args, **kwargs):
            if args[:3] == ['podman', 'machine', 'ssh']:
                raise MODULE.CommandError('CalledProcessError')
            return original(args, **kwargs)
        self.guard.command = command
        with self.assertRaises(MODULE.Refusal): self.guard.require_lease()
        self.guard.command = self.fake
        for field, value in [('cpus', True), ('cpus', 0), ('cpus', '9'), ('memTotal', 0), ('memTotal', None)]:
            with self.subTest(field=field, value=value):
                capacity = dict(self.fake.capacity)
                self.fake.capacity[field] = value
                with self.assertRaises(MODULE.Refusal): self.guard.require_lease()
                self.fake.capacity = capacity
        self.assert_no_mutations()


    def test_provider_error_class_redacts_credential_stderr(self):
        with self.assertRaises(MODULE.Refusal) as raised:
            MODULE.execute([sys.executable, '-c', 'import sys; print("registry-password-fixture", file=sys.stderr); sys.exit(9)'])
        self.assertEqual(str(raised.exception), 'CalledProcessError')
        self.assertEqual(raised.exception.failure_class, 'CalledProcessError')
        self.assertEqual(raised.exception.returncode, 9)
        self.assertNotIn('registry-password-fixture', str(raised.exception))


    def test_refused_kubernetes_lookup_never_deletes(self):
        original = self.fake
        def command(args, **kwargs):
            if args[0] == 'kubectl' and 'get' in args:
                raise MODULE.CommandError('CalledProcessError')
            return original(args, **kwargs)
        self.guard.command = command
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_kubernetes_node_labels_are_required_for_delete(self):
        original = self.fake
        def command(args, **kwargs):
            if args[0] == 'kubectl' and 'get' in args:
                original.calls.append((args, kwargs.get('env'), None))
                return '{"items": []}'
            return original(args, **kwargs)
        self.guard.command = command
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_ledger_symlink_and_hardlink_refuse(self):
        path = self.directory / 'ownership.json'
        other = self.directory / 'other.json'
        other.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(other)
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        path.unlink()
        os.link(other, path)
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assertEqual(self.fake.calls, [])



    def test_unlifted_routes_refuse_before_requests(self):
        for args in (('rpgmigrate',), ('rpgmigrate', '16:16'), ('rpgmigrate', None, 'fault-suspend'),
                     ('rpgmigrate', '17:17', 'fault-suspend'), ('rpgmigrate', '18:16', 'fault-suspend'),
                     ('rpgmigrate', '16:17', 'fault-delete'), ('rpgmigrate', '16:16', 'kind-bootstrap'),
                     ('pgcopydb', '16:16'), ('pgcopydb', '17:17', 'fault-suspend'), ('pgcopydb', None, 'fresh-clone'),
                     ('unknown', '16:16', 'fault-suspend')):
            with self.subTest(args=args), self.assertRaisesRegex(MODULE.Refusal, 'kind route admits'):
                self.guard.run(*args)
        self.assertEqual(self.fake.calls, [])


    def test_up_never_adopts_a_foreign_same_name_cluster(self):
        fixture = self.configure_up()
        self.fake.ids = ['1' * 64]
        with patch.object(MODULE, 'CONFIG', fixture), self.assertRaises(MODULE.Refusal):
            self.guard.up(1800)
        self.assertFalse(any(args[:2] == ['kind', 'create'] for args, _, _ in self.fake.calls))
        self.assert_no_mutations()


    def test_successful_up_records_and_marks_exact_nodes(self):
        fixture = self.configure_up()
        self.assertEqual(self.fake.markers, {})
        self.assertEqual(self.fake.kubernetes_labels, {})
        self.assertNotIn('kubeconfig_sha256', self.guard.read())
        self.assertFalse((self.directory / 'kubeconfig').exists())
        original = self.fake
        def command(args, **kwargs):
            if args[:2] == ['podman', 'update']:
                self.assertEqual(self.guard.read()['kubeconfig_sha256'], hashlib.sha256(self.fake.kubeconfig_bytes()).hexdigest())
                self.assertEqual({marker['body'] for marker in original.markers.values()}, {self.ledger['run_id'] + '\n'})
                self.assertEqual(len(original.markers), 3)
            if args[0] == 'kubectl' and 'label' in args:
                self.assertEqual(self.guard.read()['node_limits']['status'], 'verified')
            return original(args, **kwargs)
        self.guard.command = command
        failure = None
        with patch.object(MODULE, 'CONFIG', fixture):
            try:
                self.guard.up(1800)
            except MODULE.Refusal as error:
                failure = error
        if failure is not None:
            self.fail(str(failure))
        ledger = self.guard.read()
        self.assertEqual(ledger['phase'], 'ready')
        self.assertEqual({n['id'] for n in ledger['nodes']}, {'1' * 64, '2' * 64, '3' * 64})
        self.assertEqual({cid: marker['body'] for cid, marker in self.fake.markers.items()},
                         {node['id']: ledger['run_id'] + '\n' for node in ledger['nodes']})
        self.assertTrue(all((marker['uid'], marker['mode']) == (0, 0o600) for marker in self.fake.markers.values()))
        self.assertEqual((self.directory / 'kubeconfig').read_bytes(), self.fake.kubeconfig_bytes())
        self.assertEqual(ledger['kubeconfig_sha256'], hashlib.sha256(self.fake.kubeconfig_bytes()).hexdigest())
        self.assertEqual(ledger['node_limits']['status'], 'verified')
        self.assertEqual(self.fake.kubernetes_labels, {node['name']: {'kubernetes.io/hostname': node['name'], 'kubernetes.io/os': 'linux', MODULE.RUN_LABEL: ledger['run_id']} for node in ledger['nodes']})
        commands = [args for args, _, _ in self.fake.calls]
        creation = next(args for args in commands if args[:2] == ['kind', 'create'])
        self.assertIn('--kubeconfig', creation)
        self.assertIn('--retain', creation)
        self.assertIn('180s', creation)
        first_update = next(i for i, args in enumerate(commands) if args[:2] == ['podman', 'update'])
        reads = [args for args in commands[:first_update] if args[:2] == ['podman', 'exec'] and args[-1].startswith('test ')]
        self.assertEqual({args[args.index('--user') + 2] for args in reads}, set(self.fake.ids))
        updates = [i for i, args in enumerate(commands) if args[:2] == ['podman', 'update']]
        label = next(i for i, args in enumerate(commands) if args[0] == 'kubectl' and 'label' in args)
        self.assertEqual(len(updates), 3)
        self.assertLess(max(updates), label)
        self.assertTrue(any(args[0] == 'kubectl' and 'config' in args for args in commands[max(updates) + 1:label]))


    def test_kind_created_labels_are_identical_noop_and_keep_unrelated_labels(self):
        fixture = self.configure_up()
        original = self.fake
        before = None
        def command(args, **kwargs):
            nonlocal before
            if args[0] == 'kubectl' and 'label' in args:
                before = copy.deepcopy(original.kubernetes_labels)
                self.assertTrue(all(labels[MODULE.RUN_LABEL] == self.ledger['run_id'] for labels in before.values()))
            return original(args, **kwargs)
        self.guard.command = command
        with patch.object(MODULE, 'CONFIG', fixture): self.guard.up(1800)
        self.assertIsNotNone(before)
        self.assertEqual(self.fake.kubernetes_labels, before)


    def test_label_conflict_refuses_ready_and_down_without_relabeling(self):
        fixture = self.configure_up()
        original = self.fake
        def command(args, **kwargs):
            result = original(args, **kwargs)
            if args[:2] == ['kind', 'create']:
                original.kubernetes_labels[self.ledger['cluster'] + '-worker'][MODULE.RUN_LABEL] = 'foreign'
            return result
        self.guard.command = command
        with patch.object(MODULE, 'CONFIG', fixture), self.assertRaises(MODULE.CommandError): self.guard.up(1800)
        self.assertEqual(self.guard.read()['phase'], 'creating')
        self.assertEqual(self.fake.kubernetes_labels[self.ledger['cluster'] + '-worker'][MODULE.RUN_LABEL], 'foreign')
        self.assertEqual(self.guard.read()['node_limits']['status'], 'verified')
        self.fake.calls.clear()
        with self.assertRaisesRegex(MODULE.Refusal, 'Kubernetes node run labels changed'): self.guard.down()
        self.assert_no_mutations()
        self.assertNotIn('cleanup_authorized', self.guard.read())


    def test_realistic_create_ready_down_repeat_preserves_unowned_resources(self):
        fixture = self.configure_up()
        self.fake.provider_order = ['3' * 64, '1' * 64, '2' * 64]
        with patch.object(MODULE, 'CONFIG', fixture): self.guard.up(1800)
        ledger = self.guard.read()
        self.assertEqual(ledger['phase'], 'ready')
        self.assertEqual([node['id'] for node in ledger['nodes']], self.fake.provider_order)
        self.assertEqual(len(ledger['volumes']), 3)
        nodes = json.loads(self.guard.kubectl(['get', 'nodes', '-o', 'json']))
        self.assertEqual(nodes['kind'], 'NodeList')
        self.assertEqual({node['metadata']['name'] for node in nodes['items']}, {node['name'] for node in ledger['nodes']})
        self.assertEqual(len({node['metadata']['uid'] for node in nodes['items']}), 3)
        self.assertTrue(all(node['status']['conditions'] == [{'type': 'Ready', 'status': 'True'}] for node in nodes['items']))
        self.assertEqual(self.guard.kubectl(['get', '--raw=/readyz']).strip(), 'ok')
        self.fake.namespaces.append({'metadata': {'name': 'pgcopydb-e2e', 'uid': 'fixture-owned-namespace',
                                                  'labels': {MODULE.RUN_LABEL: ledger['run_id']}}})
        foreign = {'Id': 'f' * 64, 'Names': ['unrelated-fixture-service'], 'Labels': {'fixture-owner': 'unrelated'}}
        volume = {'Name': 'e' * 64, 'CreatedAt': '2026-11-08T11:00:00Z', 'Driver': 'local', 'Labels': {'fixture-owner': 'unrelated'}}
        self.fake.unrelated_nodes[foreign['Id']] = copy.deepcopy(foreign)
        self.fake.volumes[volume['Name']] = copy.deepcopy(volume)
        self.guard.down()
        self.guard.down()
        self.assertEqual(self.guard.read()['phase'], 'down')
        self.assertEqual(self.fake.ids, [])
        self.assertEqual(self.fake.unrelated_nodes, {foreign['Id']: foreign})
        self.assertEqual(self.fake.volumes, {volume['Name']: volume})
        deleted = [args for args, _, _ in self.fake.calls if args[:2] == ['podman', 'rm']]
        self.assertEqual(deleted, [['podman', 'rm', '--force'] + self.fake.provider_order])
        self.assertEqual({args[-1] for args, _, _ in self.fake.calls if args[:3] == ['podman', 'volume', 'rm']}, {volume['name'] for volume in ledger['volumes']})


    def test_kubernetes_metadata_mismatch_refuses_before_deletion(self):
        for problem in ('node-name', 'node-count', 'node-label', 'namespace-label', 'unavailable-api'):
            with self.subTest(problem=problem):
                self.doCleanups();self.setUp()
                name = self.ledger['nodes'][0]['name']
                if problem == 'node-name': self.fake.kubernetes_labels['foreign-node'] = self.fake.kubernetes_labels.pop(name)
                if problem == 'node-count': self.fake.kubernetes_labels.pop(name)
                if problem == 'node-label': self.fake.kubernetes_labels[name][MODULE.RUN_LABEL] = 'foreign'
                if problem == 'namespace-label': self.fake.namespaces.append({'metadata': {'name': 'pgcopydb-e2e', 'labels': {MODULE.RUN_LABEL: 'foreign'}}})
                if problem == 'unavailable-api':
                    original = self.fake
                    def command(args, **kwargs):
                        if args[0] == 'kubectl' and 'get' in args: raise MODULE.CommandError('CalledProcessError')
                        return original(args, **kwargs)
                    self.guard.command = command
                with self.assertRaises(MODULE.Refusal): self.guard.down()
                self.assertEqual(self.guard.read()['phase'], 'ready')
                self.assertNotIn('cleanup_authorized', self.guard.read())
                self.assert_no_mutations()


    def test_up_cap_failure_with_kind_labels_permits_owned_down(self):
        fixture = self.configure_up()
        self.fake.fail_update = '2' * 64
        with patch.object(MODULE, 'CONFIG', fixture), self.assertRaises(MODULE.Refusal): self.guard.up(1800)
        self.assertEqual(self.guard.read()['phase'], 'creating')
        self.assertEqual(self.guard.read()['node_limits']['status'], 'failed')
        self.assertTrue(all(labels[MODULE.RUN_LABEL] == self.ledger['run_id'] for labels in self.fake.kubernetes_labels.values()))
        self.guard.down()
        self.assertEqual(self.guard.read()['phase'], 'down')
        self.assertEqual(self.fake.ids, [])
        self.assertEqual(self.fake.volumes, {})


    def assert_marker_failure_state(self):
        ledger = self.guard.read()
        self.assertEqual(ledger['phase'], 'creating')
        self.assertEqual([node['id'] for node in ledger['nodes']], [str(n) * 64 for n in range(1, 4)])
        self.assertEqual([(volume['node_id'], volume['name']) for volume in ledger['volumes']],
                         [(str(n) * 64, letter * 64) for n, letter in zip(range(1, 4), 'abc')])
        self.assertEqual(self.fake.markers, {cid: {'body': '', 'uid': 0, 'mode': 0o600} for cid in self.fake.ids})
        self.assertNotIn('kubeconfig_sha256', ledger)
        self.assertNotIn('node_limits', ledger)
        self.assertTrue(all(labels[MODULE.RUN_LABEL] == self.ledger['run_id'] for labels in self.fake.kubernetes_labels.values()))
        self.assertEqual(len(self.fake.kubernetes_labels), 3)
        self.assertFalse(any(args[:2] in (['podman', 'update'], ['kind', 'load']) or args[0] == 'kubectl' and 'version' not in args for args, _, _ in self.fake.calls))
        self.assert_no_mutations()


    def test_up_closed_stdin_retains_empty_markers_without_cleanup_authority(self):
        fixture = self.configure_up()
        original = self.fake
        def command(args, **kwargs):
            if args[:2] == ['podman', 'exec'] and args[-1].startswith('umask '):
                args = [arg for arg in args if arg not in ('-i', '--interactive')]
            return original(args, **kwargs)
        self.guard.command = command
        with patch.object(MODULE, 'CONFIG', fixture), self.assertRaisesRegex(MODULE.Refusal, 'node run marker mismatch'):
            self.guard.up(1800)
        self.assert_marker_failure_state()
        with self.assertRaisesRegex(MODULE.Refusal, 'node run marker mismatch'):
            self.guard.down()
        self.assertNotIn('cleanup_authorized', self.guard.read())
        self.assert_no_mutations()


    def test_up_wrong_marker_body_refuses_before_hash_caps_or_labels(self):
        fixture = self.configure_up()
        original = self.fake
        def command(args, **kwargs):
            if args[:2] == ['podman', 'exec'] and args[-1].startswith('umask ') and '2' * 64 in args:
                kwargs['input'] = 'foreign\n'
            return original(args, **kwargs)
        self.guard.command = command
        with patch.object(MODULE, 'CONFIG', fixture), self.assertRaisesRegex(MODULE.Refusal, 'node run marker mismatch'):
            self.guard.up(1800)
        self.assertEqual(self.fake.markers['2' * 64]['body'], 'foreign\n')
        self.assertNotIn('kubeconfig_sha256', self.guard.read())
        self.assertNotIn('node_limits', self.guard.read())
        self.assertEqual(len(self.fake.kubernetes_labels), 3)
        self.assertFalse(any(args[:2] == ['podman', 'update'] for args, _, _ in self.fake.calls))
        self.assert_no_mutations()


    def test_up_never_clobbers_preexisting_empty_foreign_or_matching_markers(self):
        for value in ('empty', 'foreign', 'matching'):
            with self.subTest(value=value):
                self.doCleanups();self.setUp()
                fixture = self.configure_up()
                body = {'empty': '', 'foreign': 'foreign\n', 'matching': self.ledger['run_id'] + '\n'}[value]
                existing = {'body': body, 'uid': 0, 'mode': 0o600}
                self.fake.markers['2' * 64] = dict(existing)
                with patch.object(MODULE, 'CONFIG', fixture), self.assertRaises(MODULE.CommandError):
                    self.guard.up(1800)
                self.assertEqual(self.fake.markers['2' * 64], existing)
                self.assertNotIn('3' * 64, self.fake.markers)
                self.assertNotIn('kubeconfig_sha256', self.guard.read())
                self.assertNotIn('node_limits', self.guard.read())
                self.assertEqual(len(self.fake.kubernetes_labels), 3)
                self.assertFalse(any(args[:2] == ['podman', 'update'] for args, _, _ in self.fake.calls))
                self.assert_no_mutations()


    def test_marker_owner_and_mode_must_pass_before_cleanup(self):
        for field, value in [('uid', 1000), ('mode', 0o644)]:
            with self.subTest(field=field):
                self.fake.markers['2' * 64][field] = value
                with self.assertRaises(MODULE.CommandError): self.guard.down()
                self.assert_no_mutations()
                self.seed_ready_node_ownership()


    def test_creation_failure_retains_partial_ids_without_readiness(self):
        fixture = self.configure_up()
        original = self.fake
        def command(args, **kwargs):
            if args[:2] == ['kind', 'create']:
                original.calls.append((args, kwargs.get('env'), None))
                original.ids = ['1' * 64]
                raise MODULE.CommandError('CalledProcessError')
            return original(args, **kwargs)
        self.guard.command = command
        with patch.object(MODULE, 'CONFIG', fixture), self.assertRaises(MODULE.Refusal):
            self.guard.up(1800)
        ledger = self.guard.read()
        self.assertEqual(ledger['phase'], 'creating')
        self.assertEqual([n['id'] for n in ledger['nodes']], ['1' * 64])
        self.assertFalse(any(args[:2] == ['podman', 'rm'] for args, _, _ in self.fake.calls))


    def test_up_produces_only_the_fixed_bounded_lease_after_checked_absence(self):
        for seconds in (30, 1800, 7200):
            with self.subTest(seconds=seconds):
                self.setUp()
                fixture = self.configure_up()
                original = self.fake
                with patch.object(MODULE, 'CONFIG', fixture), patch.object(MODULE.time, 'time', return_value=1000000), patch.object(MODULE.time, 'monotonic', return_value=2000000):
                    self.guard.up(seconds)
                self.assertEqual(self.guard.read()['resource_lease'], {'authorized': True, 'm7_cpus': 6, 'm7_memory_mib': 12288,
                    'm3_reserved_cpus': 6, 'm3_reserved_memory_mib': 6144, 'expires_at': 1000000 + seconds, 'monotonic_expires_at': 2000000 + seconds})
                create_at = next(i for i, call in enumerate(original.calls) if call[0][:2] == ['kind', 'create'])
                self.assertTrue(any(call[0][:2] == ['podman', 'ps'] and '--filter' not in call[0] for call in original.calls[:create_at]))


    def test_up_provider_failures_or_contradiction_never_record_authorization(self):
        for problem in ('lookup', 'contradiction', 'missing-names'):
            with self.subTest(problem=problem):
                self.setUp()
                fixture = self.configure_up()
                original = self.fake
                def command(args, **kwargs):
                    if args[:2] == ['podman', 'ps']:
                        if problem == 'lookup': raise MODULE.CommandError('CalledProcessError')
                        if '--filter' in args: return '[]'
                        return json.dumps([{'Id': 'f' * 64, **({} if problem == 'missing-names' else {'Names': [self.ledger['cluster'] + '-worker']})}])
                    return original(args, **kwargs)
                self.guard.command = command
                with patch.object(MODULE, 'CONFIG', fixture), self.assertRaises(MODULE.Refusal): self.guard.up(1800)
                self.assertNotIn('resource_lease', self.guard.read())
                self.assertFalse(any(args[:2] == ['kind', 'create'] for args, _, _ in self.fake.calls))


    def test_up_preserves_kind_postgresql_and_vm_admission_budgets_and_disk_headroom(self):
        for problem in ('cpus', 'available', 'usable-total', 'disk'):
            with self.subTest(problem=problem):
                self.setUp()
                fixture = self.configure_up()
                if problem == 'cpus': self.fake.capacity['cpus'] = 13
                if problem == 'available': self.fake.meminfo = 'MemTotal: 33554432 kB\nMemAvailable: 24117248 kB\n'
                if problem == 'usable-total': self.fake.capacity['memTotal'] = 23 * 1024 ** 3
                if problem == 'disk':
                    self.guard.disk = MODULE.Guard.disk.__get__(self.guard)
                    self.guard.vm = lambda args: 'Avail\n1024\n'
                with patch.object(MODULE, 'CONFIG', fixture), self.assertRaises(MODULE.Refusal): self.guard.up(1800)
                self.assertNotIn('resource_lease', self.guard.read())
                self.assertFalse(any(args[:2] == ['kind', 'create'] for args, _, _ in self.fake.calls))


    def test_up_does_not_renew_expired_or_preexisting_authorization(self):
        fixture = self.configure_up()
        self.authorize_lease(expires_at=0)
        with patch.object(MODULE, 'CONFIG', fixture), self.assertRaises(MODULE.Refusal): self.guard.up(1800)
        self.assertEqual(self.guard.read()['resource_lease']['expires_at'], 0)
        self.assertFalse(any(args[:2] == ['kind', 'create'] for args, _, _ in self.fake.calls))


    def test_up_cap_failure_retains_hash_but_missing_labels_refuse_down(self):
        fixture = self.configure_up()
        self.fake.kind_run_labels = False
        self.fake.fail_update = '2' * 64
        with patch.object(MODULE, 'CONFIG', fixture), self.assertRaises(MODULE.Refusal): self.guard.up(1800)
        ledger = self.guard.read()
        self.assertEqual(ledger['phase'], 'creating')
        self.assertEqual(ledger['node_limits']['status'], 'failed')
        self.assertEqual(ledger['kubeconfig_sha256'], hashlib.sha256((self.directory / 'kubeconfig').read_bytes()).hexdigest())
        self.assertFalse(any(args[0] == 'kubectl' and any(verb in args for verb in ('get', 'apply', 'create', 'label')) for args, _, _ in self.fake.calls))
        self.fake.ledger = ledger
        self.assertEqual(len(self.fake.kubernetes_labels), 3)
        self.assertTrue(all(MODULE.RUN_LABEL not in labels for labels in self.fake.kubernetes_labels.values()))
        with self.assertRaisesRegex(MODULE.Refusal, 'Kubernetes node run labels changed'):
            self.guard.down()
        self.assertEqual(self.guard.read()['phase'], 'creating')
        self.assertNotIn('cleanup_authorized', self.guard.read())
        self.assertEqual(self.fake.ids, [str(n) * 64 for n in range(1, 4)])
        self.assert_no_mutations()


    def test_changed_private_kubeconfig_refuses_before_requests(self):
        self.guard.write_private('kubeconfig', b'substitution')
        with self.assertRaisesRegex(MODULE.Refusal, 'kubeconfig changed'):
            self.guard.kubectl(['get', 'nodes'])
        self.assertEqual(self.fake.calls, [])


    def configure_volume(self):
        name = 'a' * 64
        self.fake.volumes[name] = {'Name': name, 'CreatedAt': '2026-11-08T12:00:00Z', 'Driver': 'local',
                                   'Labels': {self.ledger['nodes'][0]['name']: 'true'}}
        self.ledger['volumes'] = [{'name': name, 'created_at': '2026-11-08T12:00:00Z', 'driver': 'local',
                                   'node_id': '1' * 64, 'node_name': self.ledger['nodes'][0]['name']}]
        self.guard.save(self.ledger)
        return name


    def test_owned_volume_removes_after_nodes_and_proves_absence(self):
        self.configure_volume()
        self.guard.down()
        self.guard.down()
        self.assertEqual(self.fake.volumes, {})
        self.assertTrue(self.guard.read()['teardown']['volumes_absent'])
        commands = [args for args, _, _ in self.fake.calls]
        node_delete = next(i for i, args in enumerate(commands) if args[:2] == ['podman', 'rm'])
        volume_delete = next(i for i, args in enumerate(commands) if args[:3] == ['podman', 'volume', 'rm'])
        self.assertLess(node_delete, volume_delete)
        self.assertNotIn('--force', commands[volume_delete])


    def test_recreated_or_changed_volume_refuses_all_deletion(self):
        name = self.configure_volume()
        for field, bad in [('CreatedAt', 'different'), ('Driver', 'foreign'), ('Labels', {'foreign': 'true'})]:
            original = copy.deepcopy(self.fake.volumes[name])
            self.fake.volumes[name][field] = bad
            with self.subTest(field=field), self.assertRaises(MODULE.Refusal):
                self.guard.down()
            self.fake.volumes[name] = original
        self.assert_no_mutations()


    def test_shared_volume_refuses_all_deletion(self):
        self.configure_volume()
        self.fake.foreign_volume_user = True
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_failed_volume_lookup_refuses_all_deletion(self):
        self.configure_volume()
        self.fake.fail_volume_lookup = True
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_unknown_attached_volume_refuses_all_deletion(self):
        self.configure_volume()
        self.ledger['volumes'] = []
        self.guard.save(self.ledger)
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assert_no_mutations()


    def test_partial_volume_removal_retains_authority_for_retry(self):
        self.configure_volume()
        self.fake.fail_volume_remove = True
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assertEqual(self.fake.ids, [])
        self.assertEqual(self.guard.read()['phase'], 'cleaning')
        self.fake.fail_volume_remove = False
        self.guard.down()
        self.assertEqual(self.guard.read()['phase'], 'down')
        self.assertEqual(self.fake.volumes, {})


    def test_changed_volume_after_partial_cleanup_is_not_absence(self):
        name = self.configure_volume()
        self.fake.fail_volume_remove = True
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.fake.fail_volume_remove = False
        self.fake.volumes[name]['Labels'] = {'foreign': 'true'}
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assertIn(name, self.fake.volumes)
        self.assertEqual(self.guard.read()['phase'], 'cleaning')


    def test_partial_node_removal_retries_only_retained_owned_ids(self):
        self.configure_volume()
        original = self.fake
        once = True
        def command(args, **kwargs):
            nonlocal once
            if args[:2] == ['podman', 'rm'] and once:
                once = False
                original.calls.append((args, kwargs.get('env'), None))
                original.ids.pop(0)
                raise MODULE.CommandError('CalledProcessError')
            return original(args, **kwargs)
        self.guard.command = command
        with self.assertRaises(MODULE.Refusal):
            self.guard.down()
        self.assertEqual(self.guard.read()['phase'], 'cleaning')
        self.guard.down()
        deletions = [args for args, _, _ in self.fake.calls if args[:2] == ['podman', 'rm']]
        self.assertEqual(deletions[-1], ['podman', 'rm', '--force', '2' * 64, '3' * 64])
        self.assertEqual(self.fake.volumes, {})


    def configure_up(self):
        self.ledger['phase'] = 'prepared'
        self.ledger['nodes'] = []
        self.ledger['images'] = [{'role': 'node', 'ref': 'docker.io/kindest/node@sha256:' + 'b' * 64,
            'digest': 'sha256:' + 'b' * 64, 'index_digest': 'sha256:' + 'c' * 64,
            'config_digest': 'sha256:' + 'a' * 64, 'architecture': self.guard.architecture(),
            'provenance': {'origin_ref': 'docker.io/kindest/node:pinned'}}]
        self.ledger.pop('kubeconfig_sha256', None)
        (self.directory / 'kubeconfig').unlink()
        self.fake.markers.clear()
        self.fake.kubernetes_labels.clear()
        self.fake.ids = []
        self.guard.save(self.ledger)
        self.guard.check = lambda: self.guard.read()
        self.guard.require_prepared = lambda ledger: None
        self.guard.disk = lambda vm=None: None
        self.guard.shared_storage = lambda: None
        self.guard.verify_storage_mount = lambda ledger: None
        return MODULE.CONFIG

class NodeLimitFixture(GuardFixture):
    def setUp(self):
        super().setUp()
        self.ledger['phase'] = 'creating'
        self.ledger['images'] = [{'role': 'node', 'config_digest': 'sha256:' + 'a' * 64, 'ref': 'example.invalid/node@sha256:' + 'b' * 64,
                                  'digest': 'sha256:' + 'b' * 64, 'architecture': self.guard.architecture()}]
        self.ledger['resource_lease'] = {'authorized': True, 'expires_at': MODULE.time.time() + 3600, 'monotonic_expires_at': MODULE.time.monotonic() + 3600,
            'm7_cpus': 6, 'm7_memory_mib': 12288, 'm3_reserved_cpus': 6, 'm3_reserved_memory_mib': 6144}
        self.guard.save(self.ledger)

    def assert_no_updates(self):
        self.assertFalse(any(args[:2] == ['podman', 'update'] for args, _, _ in self.fake.calls))

    def assert_no_runtime_effects(self):
        self.assertFalse(any(args[:2] == ['kind', 'load'] or args[:2] == ['task', 'e2e:kind:suite'] or
            args[0] in ('kubectl', 'helm') and any(verb in args for verb in ('apply', 'create', 'install', 'upgrade')) for args, _, _ in self.fake.calls))


class NodeLimitTests(NodeLimitFixture):
    def test_owned_three_nodes_receive_only_exact_cid_updates_and_verified_aggregate(self):
        self.guard.enforce_node_limits(self.ledger)
        updates = [args for args, _, _ in self.fake.calls if args[:2] == ['podman', 'update']]
        self.assertEqual(updates, [['podman', 'update', '--cpus=2', '--memory=4096m', '--memory-swap=4096m', str(i) * 64] for i in range(1, 4)])
        self.assertEqual(self.guard.read()['node_limits']['status'], 'verified')
        self.guard.verify_node_limits(self.ledger)

    def test_missing_cap_proof_refuses_before_any_runtime_effect(self):
        with self.assertRaises(MODULE.Refusal): self.guard.verify_node_limits(self.ledger)
        self.assertFalse(any(args[:2] in (['podman', 'update'], ['kind', 'load']) for args, _, _ in self.fake.calls))


    def test_all_three_identities_and_supported_process_paths_are_checked_before_first_update(self):
        for change in ('missing-node', 'foreign-label', 'missing-marker', 'image', 'pid', 'running', 'started', 'host-type',
                       'host-negative', 'cgroup-fs', 'cgroup-owner', 'cgroup-traversal', 'cgroup-foreign', 'cgroup-extra-line',
                       'linked-cgroup', 'start-ticks', 'stat-pid', 'wrong-role', 'duplicate-ledger', 'extra-node'):
            with self.subTest(change=change):
                self.doCleanups();self.setUp()
                cid = '3' * 64
                if change == 'wrong-role': self.ledger['nodes'][0]['role'] = 'worker';self.guard.save(self.ledger)
                if change == 'duplicate-ledger': self.ledger['nodes'][2]['id'] = '2' * 64;self.guard.save(self.ledger)
                if change == 'extra-node': self.ledger['nodes'].append(dict(self.ledger['nodes'][2]));self.guard.save(self.ledger)
                if change == 'missing-node': self.fake.ids.pop()
                if change == 'foreign-label': self.fake.foreign = True
                if change == 'missing-marker': self.fake.markers[cid]['body'] = 'foreign'
                if change == 'image': self.fake.node_image_overrides[cid] = 'f' * 64
                if change == 'pid': self.fake.node_states[cid]['Pid'] = True
                if change == 'running': self.fake.node_states[cid]['Running'] = False
                if change == 'started': self.fake.node_states[cid]['StartedAt'] = 'unknown'
                if change == 'host-type': self.fake.node_hosts[cid]['Memory'] = None
                if change == 'host-negative': self.fake.node_hosts[cid]['CpuQuota'] = -2
                if change == 'cgroup-fs': self.fake.cgroup_fs = 'cgroupfs'
                if change == 'cgroup-owner': self.fake.node_cgroups[cid] = self.fake.node_cgroups[cid].replace('user@1000', 'user@1001')
                if change == 'cgroup-traversal': self.fake.node_cgroups[cid] += '/../foreign'
                if change == 'cgroup-foreign': self.fake.node_cgroups[cid] = self.fake.node_cgroups[cid].replace('libpod-' + cid, 'libpod-' + 'f' * 64)
                if change == 'cgroup-extra-line': self.fake.proc_cgroup_overrides[cid] = '0::' + self.fake.node_cgroups[cid] + '\n1:name=systemd:/foreign\n'
                if change == 'linked-cgroup': self.fake.canonical_overrides['/sys/fs/cgroup' + self.fake.node_cgroups[cid]] = '/sys/fs/cgroup/foreign'
                if change == 'start-ticks': self.fake.node_ticks[cid] = 0
                if change == 'stat-pid': self.fake.proc_stat_overrides[cid] = '9999 (foreign) S ' + '0 ' * 30
                with self.assertRaises(MODULE.Refusal): self.guard.enforce_node_limits(self.ledger)
                self.assert_no_updates();self.assert_no_runtime_effects()
                if change not in ('wrong-role', 'duplicate-ledger', 'extra-node'):
                    self.assertEqual(self.guard.read()['node_limits']['status'], 'failed')

    def test_duplicate_or_foreign_complete_inventory_and_failed_lookup_refuse_before_update(self):
        for change in ('duplicate', 'foreign-name', 'missing-complete', 'lookup'):
            with self.subTest(change=change):
                self.doCleanups();self.setUp()
                original = self.fake
                def command(args, **kwargs):
                    result = original(args, **kwargs)
                    if args[:2] == ['podman', 'ps'] and '--filter' not in args:
                        if change == 'lookup': raise MODULE.CommandError('CalledProcessError')
                        items = json.loads(result)
                        if change == 'duplicate': items.append(copy.deepcopy(items[0]))
                        if change == 'foreign-name': items.append({'Id': 'f' * 64, 'Names': [self.ledger['cluster'] + '-worker'], 'Labels': {}})
                        if change == 'missing-complete': items.pop()
                        return json.dumps(items)
                    return result
                self.guard.command = command
                with self.assertRaises(MODULE.Refusal): self.guard.enforce_node_limits(self.ledger)
                self.assert_no_updates();self.assert_no_runtime_effects()

    def test_verified_hierarchy_root_has_no_controller_files_but_every_nonroot_control_is_required(self):
        self.guard.enforce_node_limits(self.ledger)
        self.guard.verify_node_limits(self.ledger)
        forbidden = {'/sys/fs/cgroup/cpu.max', '/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory.swap.max'}
        for args, _, _ in self.fake.calls:
            if args[:4] == ['podman', 'machine', 'ssh', '--']:
                self.assertFalse(forbidden & set(MODULE.shlex.split(args[4])))
        self.fake.cgroup_overrides[(None, 'ancestor', 'memory.max')] = None
        with self.assertRaises(MODULE.Refusal): self.guard.verify_node_limits(self.ledger)
        self.assert_no_runtime_effects()

    def test_missing_ambiguous_or_delegated_hierarchy_root_never_authorizes_update(self):
        valid = '42 1 0:42 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n'
        for raw in ('', valid + valid, valid.replace(' / /sys', ' /delegated /sys'),
                    valid.replace(' - cgroup2 ', ' - cgroup '), valid.replace('42 1 ', 'bad 1 ')):
            with self.subTest(raw=raw):
                self.doCleanups();self.setUp();self.fake.cgroup_mountinfo = raw
                with self.assertRaises(MODULE.Refusal): self.guard.enforce_node_limits(self.ledger)
                self.assert_no_updates();self.assert_no_runtime_effects()

    def test_changed_hierarchy_mount_identity_refuses_current_and_saved_cap_authority(self):
        self.guard.enforce_node_limits(self.ledger)
        self.fake.calls.clear()
        self.fake.cgroup_mountinfo = self.fake.cgroup_mountinfo.replace('42 1 ', '43 1 ')
        with self.assertRaises(MODULE.Refusal): self.guard.verify_node_limits(self.ledger)
        self.assert_no_updates();self.assert_no_runtime_effects()

    def test_unrelated_valid_provider_container_is_not_updated(self):
        original = self.fake
        def command(args, **kwargs):
            result = original(args, **kwargs)
            if args[:2] == ['podman', 'ps'] and '--filter' not in args:
                items = json.loads(result);items.append({'Id': 'e' * 64, 'Names': ['unrelated-postgres'], 'Labels': {}})
                return json.dumps(items)
            return result
        self.guard.command = command
        self.guard.enforce_node_limits(self.ledger)
        self.assertFalse(any(args[:2] == ['podman', 'update'] and args[-1] == 'e' * 64 for args, _, _ in self.fake.calls))

    def test_effective_inheritance_accepts_max_only_with_a_finite_matching_owned_scope(self):
        for cid in self.fake.ids:
            self.fake.node_cgroups[cid] += '/init.scope'
            self.fake.cgroup_overrides[(cid, 'container', 'cpu.max')] = 'max 100000'
            self.fake.cgroup_overrides[(cid, 'container', 'memory.max')] = 'max'
            self.fake.cgroup_overrides[(cid, 'scope', 'cpu.max')] = '400000 200000'
        self.guard.enforce_node_limits(self.ledger)
        observed = self.guard.verify_node_limits(self.ledger)
        self.assertEqual(sum(int(value['cpu']) for value in observed), 6)
        self.assertEqual(sum(value['memory'] for value in observed), 12884901888)
        self.assertTrue(all(value['swap'] == 0 for value in observed))

    def test_malformed_unlimited_or_conflicting_effective_limits_never_qualify(self):
        cases = [('scope', 'cpu.max', 'max 100000'), ('scope', 'cpu.max', '200000'),
                 ('scope', 'cpu.max', '200000 0'), ('scope', 'cpu.max', '200000 max'),
                 ('scope', 'memory.max', 'max'), ('scope', 'memory.max', '0'),
                 ('container', 'memory.swap.max', 'max'), ('container', 'memory.swap.max', '-1'),
                 ('container', 'cpu.max', '100000 100000'), ('container', 'memory.max', '2147483648'),
                 ('ancestor', 'cpu.max', '100000 100000'), ('ancestor', 'memory.max', '2147483648'),
                 ('scope', 'memory.max', None), ('ancestor', 'cpu.max', '100000 max')]
        for level, key, value in cases:
            with self.subTest(level=level, key=key, value=value):
                self.doCleanups();self.setUp()
                cid = None if level == 'ancestor' else '1' * 64
                self.fake.cgroup_overrides[(cid, level, key)] = value
                with self.assertRaises(MODULE.Refusal): self.guard.enforce_node_limits(self.ledger)
                proof = self.guard.read()['node_limits']
                self.assertEqual(proof['status'], 'failed')
                self.assertEqual(proof['nodes'][0]['update'], 'returned')
                self.assert_no_runtime_effects()

    def test_provider_limit_disagreement_refuses_after_exact_update_without_workloads(self):
        for key, value in [('CpuQuota', 100000), ('CpuPeriod', 0), ('NanoCpus', 0), ('Memory', 0),
                           ('MemorySwap', -1), ('Memory', True), ('MemorySwap', None)]:
            with self.subTest(key=key, value=value):
                self.doCleanups();self.setUp()
                original = self.fake
                def command(args, **kwargs):
                    result = original(args, **kwargs)
                    if args[:2] == ['podman', 'update']: self.fake.node_hosts[args[-1]][key] = value
                    return result
                self.guard.command = command
                with self.assertRaises(MODULE.Refusal): self.guard.enforce_node_limits(self.ledger)
                self.assertEqual(len([args for args, _, _ in self.fake.calls if args[:2] == ['podman', 'update']]), 1)
                self.assertEqual(self.guard.read()['node_limits']['status'], 'failed')
                self.assert_no_runtime_effects()

    def test_contradictory_update_response_never_qualifies_or_repairs_the_node(self):
        self.fake.update_returns['1' * 64] = 'f' * 64
        with self.assertRaises(MODULE.Refusal): self.guard.enforce_node_limits(self.ledger)
        proof = self.guard.read()['node_limits']
        self.assertEqual(proof['status'], 'failed')
        self.assertEqual(proof['nodes'][0]['update'], 'attempting')
        self.assertEqual(len([args for args, _, _ in self.fake.calls if args[:2] == ['podman', 'update']]), 1)
        self.assert_no_runtime_effects()

    def test_partial_update_failure_with_established_ownership_allows_checked_down(self):
        self.assertEqual(self.fake.kubernetes_labels, {node['name']: {'kubernetes.io/hostname': node['name'], 'kubernetes.io/os': 'linux', MODULE.RUN_LABEL: self.ledger['run_id']} for node in self.ledger['nodes']})
        self.assertEqual(self.ledger['kubeconfig_sha256'], hashlib.sha256((self.directory / 'kubeconfig').read_bytes()).hexdigest())
        self.fake.fail_update = '2' * 64
        with self.assertRaises(MODULE.Refusal): self.guard.enforce_node_limits(self.ledger)
        limits = self.guard.read()['node_limits']
        self.assertEqual([proof['update'] for proof in limits['nodes']], ['verified', 'attempting'])
        self.assertEqual([node['id'] for node in self.guard.read()['nodes']], ['1' * 64, '2' * 64, '3' * 64])
        self.assert_no_runtime_effects()
        self.guard.down()
        self.assertEqual(self.guard.read()['phase'], 'down')
        self.assertEqual(self.fake.ids, [])

    def test_failed_cap_application_is_not_adopted_or_retried(self):
        self.fake.fail_update = '2' * 64
        with self.assertRaises(MODULE.Refusal): self.guard.enforce_node_limits(self.ledger)
        before = len([args for args, _, _ in self.fake.calls if args[:2] == ['podman', 'update']])
        self.fake.fail_update = None
        with self.assertRaises(MODULE.Refusal): self.guard.enforce_node_limits(self.ledger)
        self.assertEqual(len([args for args, _, _ in self.fake.calls if args[:2] == ['podman', 'update']]), before)
        self.assertEqual(self.guard.read()['node_limits']['status'], 'failed')
        self.assert_no_runtime_effects()

    def test_partial_failure_does_not_delete_changed_ownership(self):
        self.fake.fail_update = '2' * 64
        with self.assertRaises(MODULE.Refusal): self.guard.enforce_node_limits(self.ledger)
        self.fake.foreign = True
        with self.assertRaises(MODULE.Refusal): self.guard.down()
        self.assertFalse(any(args[:2] == ['podman', 'rm'] for args, _, _ in self.fake.calls))

    def test_pid_start_changes_during_observation_never_publish_verified_caps(self):
        original, changed = self.fake.batched, False
        def read(args, **kwargs):
            nonlocal changed
            result = original(args, **kwargs)
            if not changed and '/cpu.max' in args[-1]:
                changed = True;self.fake.node_ticks['1' * 64] += 1
            return result
        self.fake.batched = read
        # The change lands between the limit read and the after-identity read of the same batched call.
        with self.assertRaisesRegex(MODULE.Refusal, 'changed while reading cgroups'): self.guard.enforce_node_limits(self.ledger)
        proof = self.guard.read()['node_limits']
        self.assertEqual((proof['status'], [node['update'] for node in proof['nodes']]), ('failed', ['returned']))
        self.assert_no_runtime_effects()

    def test_later_fresh_checks_refuse_lost_limits_process_restart_or_stale_proof_without_updating(self):
        for change in ('host-limit', 'guest-limit', 'pid', 'start-time', 'ticks', 'replacement', 'proof', 'proof-duplicate', 'status'):
            with self.subTest(change=change):
                self.doCleanups();self.setUp();self.guard.enforce_node_limits(self.ledger)
                self.fake.calls.clear()
                cid = '3' * 64
                if change == 'host-limit': self.fake.node_hosts[cid]['Memory'] = 0
                if change == 'guest-limit': self.fake.cgroup_overrides[(cid, 'scope', 'cpu.max')] = 'max 100000'
                if change == 'pid': self.fake.node_states[cid]['Pid'] += 100
                if change == 'start-time': self.fake.node_states[cid]['StartedAt'] = '2026-01-02T00:00:00Z'
                if change == 'ticks': self.fake.node_ticks[cid] += 1
                if change == 'replacement': self.fake.ids[2] = 'f' * 64
                if change == 'proof-duplicate': self.ledger['node_limits']['nodes'][2]['id'] = '2' * 64;self.guard.save(self.ledger)
                if change == 'proof': self.ledger['node_limits']['nodes'][2]['role'] = 'control-plane';self.guard.save(self.ledger)
                if change == 'status': self.ledger['node_limits']['status'] = 'failed';self.guard.save(self.ledger)
                with self.assertRaises(MODULE.Refusal): self.guard.verify_node_limits(self.ledger)
                self.assert_no_updates();self.assert_no_runtime_effects()




class FastAdmissionTests(NodeLimitFixture):
    def vm_calls(self):
        return [args for args, env, _ in self.fake.calls if args[:4] == ['podman', 'machine', 'ssh', '--'] and env != BATCHED]

    def test_each_check_reads_the_vm_once_per_lease_and_once_per_node(self):
        self.guard.enforce_node_limits(self.ledger)
        self.fake.calls.clear()
        observed = self.guard.verify_node_limits(self.ledger)
        self.assertEqual(len(self.vm_calls()), 4)
        self.assertTrue(all('m7-batch' in args[4] for args in self.vm_calls()))
        self.assertEqual([value['host_config']['NanoCpus'] for value in observed], [2000000000] * 3)
        self.assertEqual([value['cgroups'] for value in observed], [proof['limits']['cgroups'] for proof in self.guard.read()['node_limits']['nodes']])

    def test_unplanned_batched_read_or_changed_after_pid_refuses(self):
        self.guard.enforce_node_limits(self.ledger)
        cid = '2' * 64
        self.fake.node_cgroups[cid] += '/moved'
        with self.assertRaisesRegex(MODULE.Refusal, 'cgroup identity changed'): self.guard.verify_node_limits(self.ledger)
        self.doCleanups();self.setUp();self.guard.enforce_node_limits(self.ledger)
        original, inspected = self.fake, []
        def command(args, **kwargs):
            if args[:2] == ['podman', 'inspect'] and args[-1] == cid:
                inspected.append(args)
                if len(inspected) == 3:  # ownership, before, then the after-inspection of the observation
                    self.fake.node_states[cid]['Pid'] += 1
            return original(args, **kwargs)
        self.guard.command = command
        with self.assertRaisesRegex(MODULE.Refusal, 'changed while reading cgroups'): self.guard.verify_node_limits(self.ledger)
        for identity in (None, {}, {'cgroup_path': 'relative'}):
            with self.subTest(identity=identity):
                self.doCleanups();self.setUp();self.guard.enforce_node_limits(self.ledger)
                self.ledger['node_limits']['nodes'][1]['identity'] = identity;self.guard.save(self.ledger);self.fake.calls.clear()
                with self.assertRaisesRegex(MODULE.Refusal, 'cgroup identity changed'): self.guard.verify_node_limits(self.ledger)
                self.assert_no_updates()

    def test_each_refusal_of_a_fresh_observation_names_its_own_fact(self):
        cases = {'ticks': 'start time, or cgroup identity changed', 'ancestor-link': 'path is unavailable, linked, or noncanonical',
                 'long-value': 'inventory is incomplete or malformed', 'after-root': 'changed while reading cgroups',
                 'inspection': 'node inspection unavailable', 'aggregate': 'aggregate owned node limits',
                 'after-host': 'changed while reading cgroups'}
        for change, message in cases.items():
            with self.subTest(change=change):
                self.doCleanups();self.setUp();self.guard.enforce_node_limits(self.ledger);self.fake.calls.clear()
                cid = '3' * 64
                if change == 'ticks': self.fake.node_ticks[cid] += 1
                if change == 'ancestor-link': self.fake.canonical_overrides['/sys/fs/cgroup/user.slice'] = '/sys/fs/cgroup/foreign'
                if change == 'long-value': self.fake.cgroup_overrides[(cid, 'container', 'memory.max')] = '9' * 129
                if change == 'after-root':
                    original, reads = self.fake.batched, []
                    def read(args, **kwargs):
                        if args[-1] == 'cat /proc/self/mountinfo':
                            reads.append(args)
                            if len(reads) == 2:
                                self.fake.cgroup_mountinfo = self.fake.cgroup_mountinfo.replace('42 1 ', '43 1 ')
                        return original(args, **kwargs)
                    self.fake.batched = read
                if change == 'inspection':
                    original = self.fake
                    self.guard.command = lambda args, **kwargs: '[]' if args[:2] == ['podman', 'inspect'] and args[-1] == cid else original(args, **kwargs)
                if change == 'after-host':
                    original, inspected = self.fake, []
                    def command(args, **kwargs):
                        if args[:2] == ['podman', 'inspect'] and args[-1] == cid:
                            inspected.append(args)
                            if len(inspected) == 3:  # ownership, before, then the after-inspection of the observation
                                self.fake.node_hosts[cid] = dict(self.fake.node_hosts[cid], CpuPeriod=50000, CpuQuota=100000)
                        return original(args, **kwargs)
                    self.guard.command = command
                if change == 'aggregate':
                    observation = self.guard.node_limit_observation
                    self.guard.node_limit_observation = lambda ledger, node, expected: dict(observation(ledger, node, expected), cpu='1')
                with self.assertRaisesRegex(MODULE.Refusal, message): self.guard.verify_node_limits(self.ledger)
                self.assert_no_updates()

    def test_batched_vm_framing_and_replay_refuse_anything_unplanned(self):
        commands = [['cat', '/proc/meminfo'], ['df', '-B1', '--output=avail', '/var']]
        for raw in ('', '0 \n', '0 \n0 \n0 \n', '0 \n0 ', 'x \n0 \n', '256 \n0 \n', '0 !!\n0 \n', '0\n0 \n', '-1 \n0 \n'):
            with self.subTest(raw=raw):
                self.guard.command = lambda args, **kwargs: raw
                with self.assertRaisesRegex(MODULE.Refusal, 'framing is invalid'): self.guard.vm_batch(commands, 'unplanned')
        data = MODULE.base64.b64encode(b'\xff').decode()
        self.guard.command = lambda args, **kwargs: '0 ' + MODULE.base64.b64encode(b'ok\n').decode() + '\n3 ' + data + '\n'
        reads = self.guard.vm_batch(commands, 'unplanned')
        with self.assertRaisesRegex(MODULE.Refusal, 'unplanned'): reads(commands[1])
        with self.assertRaisesRegex(MODULE.Refusal, 'unplanned'): reads(commands[0], private_output=True)
        self.assertEqual(reads(commands[0]), 'ok\n')
        with self.assertRaises(MODULE.CommandError) as raised: reads(commands[1])
        self.assertEqual((raised.exception.failure_class, raised.exception.returncode), ('CalledProcessError', 3))
        with self.assertRaisesRegex(MODULE.Refusal, 'unplanned'): reads(commands[1])
        self.guard.command = lambda args, **kwargs: '0 ' + data + '\n0 \n'
        with self.assertRaises(MODULE.CommandError) as raised: self.guard.vm_batch(commands, 'unplanned')(commands[0])
        self.assertEqual(raised.exception.failure_class, 'UnicodeDecodeError')

    def test_batch_script_keeps_exact_bytes_and_status_in_a_posix_shell(self):
        shim = Path(self.tmp.name) / 'bin'
        shim.mkdir()
        # The VM has GNU base64; the shim keeps this test independent of the host flavour.
        (shim / 'base64').write_text('#!/bin/sh\n[ "$1" = -w0 ] || exit 64\nexec ' + sys.executable +
            ' -c "import base64,sys;sys.stdout.write(base64.b64encode(sys.stdin.buffer.read()).decode())"\n')
        (shim / 'base64').chmod(0o700)
        def remote(args, **kwargs):
            self.assertEqual(args[:4], ['podman', 'machine', 'ssh', '--'])
            return subprocess.run(['/bin/sh', '-c', args[4]], env={'PATH': str(shim) + ':/usr/bin:/bin'},
                                  capture_output=True, check=True).stdout.decode()
        self.guard.command = remote
        commands = [['printf', '%s', 'a.b\n\n'], ['true'], ['sh', '-c', "printf 'x y'\\''z.7'; exit 3"], ['printf', '%s', "it's"]]
        reads = self.guard.vm_batch(commands, 'unplanned')
        self.assertEqual(reads(commands[0]), 'a.b\n\n')
        self.assertEqual(reads(commands[1]), '')
        with self.assertRaises(MODULE.CommandError) as raised: reads(commands[2])
        self.assertEqual(raised.exception.returncode, 3)
        self.assertEqual(reads(commands[3]), "it's")


class HostToolTests(GuardFixture):
    def test_only_the_pinned_kind_and_podman_clients_are_admitted(self):
        self.guard.tools()
        original = self.fake
        self.guard.command = lambda args, **kwargs: 'kind v0.32.0 go1.27.0 linux/arm64\n' if args[:2] == ['kind', 'version'] else original(args, **kwargs)
        with self.assertRaisesRegex(MODULE.Refusal, 'kind 0.33.0 is required'): self.guard.tools()
        self.guard.command = original
        for version in ('podman version 6.1.4\n', 'podman version 6.2.1\n', 'podman version 6.2.0-dev\n', ''):
            with self.subTest(version=version):
                self.fake.podman_version = version
                with self.assertRaisesRegex(MODULE.Refusal, 'Podman 6.2.0 is required'): self.guard.tools()


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        directory = Path(self.tmp.name).resolve()
        directory.chmod(0o700)
        self.guard = MODULE.Guard(directory, environ={})

    def run_each(self, items):
        started = MODULE.time.monotonic()
        def item(value):
            return self.guard.each(item, value) if isinstance(value[0], list) else self.guard.call(value)
        with self.assertRaises(BaseException) as raised:
            self.guard.each(item, items)
        self.assertLess(MODULE.time.monotonic() - started, 10)
        return raised.exception

    def test_first_real_failure_kills_running_siblings(self):
        error = self.run_each([['/bin/sleep', '30'], ['/usr/bin/false'], ['/bin/sleep', '30']])
        self.assertEqual((type(error), error.failure_class, error.returncode), (MODULE.CommandError, 'CalledProcessError', 1))
        error = self.run_each([[['/bin/sleep', '30'], ['/usr/bin/false']], ['/bin/sleep', '30']])
        self.assertEqual((type(error), error.failure_class), (MODULE.CommandError, 'CalledProcessError'))
        self.assertEqual(self.guard.call(['/usr/bin/true']), '')

    def test_cancelled_scope_refuses_to_start_new_children(self):
        def item(value):
            if value == 'late':
                MODULE.time.sleep(0.5)
                return self.guard.call(['/bin/sleep', '30'])
            return self.guard.call(['/usr/bin/false'])
        started = MODULE.time.monotonic()
        with self.assertRaises(MODULE.CommandError) as raised:
            self.guard.each(item, ['late', 'fail'])
        self.assertLess(MODULE.time.monotonic() - started, 10)
        self.assertEqual((type(raised.exception), raised.exception.returncode), (MODULE.CommandError, 1))

    def test_stage_alarm_kills_every_child_and_leaves_later_calls_usable(self):
        def expired(_signal, _frame):
            raise MODULE.Refusal('fixed host stage deadline expired')
        previous = signal.signal(signal.SIGALRM, expired)
        self.addCleanup(signal.signal, signal.SIGALRM, previous)
        signal.setitimer(signal.ITIMER_REAL, 0.3)
        error = self.run_each([['/bin/sleep', '30'], [['/bin/sleep', '30'], ['/bin/sleep', '30']]])
        self.assertEqual(str(error), 'fixed host stage deadline expired')
        self.assertEqual(self.guard.call(['/usr/bin/true']), '')

    def test_results_keep_item_order(self):
        self.assertEqual(self.guard.each(lambda value: self.guard.call(['/bin/echo', value]), ['a', 'b', 'c']), ['a\n', 'b\n', 'c\n'])


class RegistryTransportTests(unittest.TestCase):
    def test_registry_pull_api_replaces_archive_transport(self):
        self.assertTrue(callable(getattr(MODULE.Guard, 'pull', None)))
        self.assertTrue(callable(getattr(MODULE.Guard, 'verify_pulled', None)))
        self.assertTrue(callable(getattr(MODULE.Guard, 'pulled_image_binding', None)))

    def test_archive_load_transport_is_deleted(self):
        self.assertFalse(hasattr(MODULE.Guard, 'load'))
        self.assertNotIn('e2e:kind:load', (MODULE.ROOT / 'Taskfile.yml').read_text())

    def test_superseded_load_cli_and_missing_prepare_inputs_create_nothing(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp).resolve() / 'absent-run'
            for args in (['load'], ['prepare']):
                result = subprocess.run([sys.executable, str(SCRIPT), *args, '--run-dir', str(path)], capture_output=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(path.exists())
            self.assertIn(b'requires --image-lock and --receipt-dir', result.stderr)

    def test_registry_proxy_credential_and_mirror_overrides_refuse_before_run_creation(self):
        names = ('HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'ALL_PROXY', 'REGISTRY_AUTH_FILE', 'DOCKER_CONFIG',
            'DOCKER_AUTH_CONFIG', 'CONTAINERS_REGISTRIES_CONF', 'CONTAINERS_REGISTRIES_CONF_DIR',
            'CONTAINERS_CONF', 'CONTAINERS_CONF_OVERRIDE', 'REGISTRY_USERNAME', 'REGISTRY_PASSWORD', 'REGISTRY_TOKEN')
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            for name in [*names, *(name.lower() for name in names)]:
                path = root / name
                with self.subTest(name=name):
                    with self.assertRaises(MODULE.Refusal) as raised:
                        MODULE.Guard(path, environ={name: 'private-override-canary'})
                    self.assertNotIn('private-override-canary', str(raised.exception))
                    self.assertFalse(path.exists())
            clear = root / 'clear'
            MODULE.Guard(clear, environ={})
            self.assertTrue(clear.is_dir())

    def test_duplicate_json_fields_are_rejected_at_every_depth(self):
        for value in ('{"x":1,"x":2}', '{"nested":{"x":1,"x":2}}'):
            with self.subTest(value=value), self.assertRaises(MODULE.Refusal):
                MODULE.decode(value, 'registry proof')


LEGACY_REGISTRY_INDEX_RAW = b'{\n  "schemaVersion": 2,\n  "mediaType": "application/vnd.oci.image.index.v1+json",\n  "manifests": [\n    {\n      "mediaType": "application/vnd.oci.image.manifest.v1+json",\n      "digest": "sha256:4010429893c46f87c903d64f913d9d0b76b237d14a9685274b27a264f2d729b5",\n      "size": 1428,\n      "platform": {\n        "architecture": "amd64",\n        "os": "linux"\n      }\n    },\n    {\n      "mediaType": "application/vnd.oci.image.manifest.v1+json",\n      "digest": "sha256:5e5b148d63f4b3327c20d22daeef0e62fc85a530418671cc0b4a88ed2960be2a",\n      "size": 1428,\n      "platform": {\n        "architecture": "arm64",\n        "os": "linux"\n      }\n    },\n    {\n      "mediaType": "application/vnd.oci.image.manifest.v1+json",\n      "digest": "sha256:65b18ebfdc76a6eb18d2b00542cdfd18ddcfc2497cb0319a9a2fc5315efdf35c",\n      "size": 838,\n      "annotations": {\n        "vnd.docker.reference.digest": "sha256:4010429893c46f87c903d64f913d9d0b76b237d14a9685274b27a264f2d729b5",\n        "vnd.docker.reference.type": "attestation-manifest"\n      },\n      "platform": {\n        "architecture": "unknown",\n        "os": "unknown"\n      }\n    },\n    {\n      "mediaType": "application/vnd.oci.image.manifest.v1+json",\n      "digest": "sha256:2c2e63881b104f60ab57afc9939575f6c0a556f8495ffbd8075b6bfbf3040ae2",\n      "size": 838,\n      "annotations": {\n        "vnd.docker.reference.digest": "sha256:5e5b148d63f4b3327c20d22daeef0e62fc85a530418671cc0b4a88ed2960be2a",\n        "vnd.docker.reference.type": "attestation-manifest"\n      },\n      "platform": {\n        "architecture": "unknown",\n        "os": "unknown"\n      }\n    }\n  ]\n}'


class RegistrySchemaTests(unittest.TestCase):
    def test_single_manifest_graph_accepts_null_index_and_optional_variant(self):
        image, graph = registry_fixture('manager')
        MODULE.registry.validate_graph(image, graph, development=True)

    def test_graph_refuses_wrong_platform_size_digest_and_ambiguous_index(self):
        image, graph = registry_fixture('manager')
        for change in ('config', 'platform', 'extra', 'index'):
            bad = copy.deepcopy(graph)
            if change == 'config':
                bad['config_digest'] = 'sha256:' + 'f' * 64
            elif change == 'platform':
                bad['architecture'] = 'amd64'
            elif change == 'extra':
                bad['archive'] = 'image.tar'
            else:
                bad['index_digest'] = image['digest']
            with self.subTest(change=change), self.assertRaises(MODULE.Refusal):
                MODULE.registry.validate_graph(image, bad, development=True)

    def test_lock_requires_exact_roles_closed_shapes_and_native_platform(self):
        lock = registry_lock()
        MODULE.registry.validate_lock(lock, 'arm64')
        for change in ('schema1', 'missing-role', 'extra-role', 'null', 'mixed', 'archive', 'missing-null', 'null-pin'):
            bad = copy.deepcopy(lock)
            if change == 'schema1': bad['schema_version'] = 1
            elif change == 'missing-role': del bad['images']['storage-helper']
            elif change == 'extra-role': bad['images']['extra'] = bad['images']['node']
            elif change == 'null': bad['images']['postgresql-16'] = None
            elif change == 'mixed': bad['images']['postgresql-18']['architecture'] = 'amd64'
            elif change == 'archive': bad['images']['node']['archive'] = 'node.tar'
            elif change == 'null-pin': bad['images']['manager']['digest'] = None
            else: del bad['images']['node']['index_digest']
            with self.subTest(change=change), self.assertRaises(MODULE.Refusal):
                MODULE.registry.validate_lock(bad, 'arm64')

    def test_actual_pinned_legacy_index_selects_runnable_and_excludes_marked_attestations(self):
        self.assertEqual(MODULE.registry.sha(LEGACY_REGISTRY_INDEX_RAW), 'ec9e552b833afcf08034018d98f329486c324488c61e0c4b392edc83ad5c6440')
        selected = MODULE.registry.selected_platform(LEGACY_REGISTRY_INDEX_RAW, 'arm64', None)
        self.assertEqual(selected['digest'], 'sha256:5e5b148d63f4b3327c20d22daeef0e62fc85a530418671cc0b4a88ed2960be2a')
        self.assertEqual(selected['size'], 1428)
        self.assertNotIn('variant', selected['platform'])
        with self.assertRaises(MODULE.Refusal): MODULE.registry.selected_platform(LEGACY_REGISTRY_INDEX_RAW, 'unknown', None)

    def test_attestation_exception_rejects_unmarked_extra_dangling_duplicate_and_cyclic_descriptors(self):
        original = json.loads(LEGACY_REGISTRY_INDEX_RAW)
        for case in ('unmarked', 'extra-marker', 'extra-platform', 'wrong-media', 'dangling', 'nonrunnable-target',
                'duplicate-target', 'duplicate-descriptor', 'duplicate-runnable-platform', 'cyclic'):
            value = copy.deepcopy(original)
            items = value['manifests']
            if case == 'unmarked': del items[2]['annotations']
            elif case == 'extra-marker': items[2]['annotations']['unreviewed'] = 'value'
            elif case == 'extra-platform': items[2]['platform']['variant'] = None
            elif case == 'wrong-media': items[2]['mediaType'] = 'application/vnd.docker.distribution.manifest.v2+json'
            elif case == 'dangling': items[2]['annotations']['vnd.docker.reference.digest'] = 'sha256:' + 'f' * 64
            elif case == 'nonrunnable-target': items[2]['annotations']['vnd.docker.reference.digest'] = items[3]['digest']
            elif case == 'duplicate-target': items[2]['annotations']['vnd.docker.reference.digest'] = items[3]['annotations']['vnd.docker.reference.digest']
            elif case == 'duplicate-descriptor': items.append(copy.deepcopy(items[2]))
            elif case == 'duplicate-runnable-platform': items[0]['platform'] = copy.deepcopy(items[1]['platform'])
            else:
                items[2]['annotations']['vnd.docker.reference.digest'] = items[3]['digest']
                items[3]['annotations']['vnd.docker.reference.digest'] = items[2]['digest']
            with self.subTest(case=case), self.assertRaises(MODULE.Refusal):
                MODULE.registry.selected_platform(MODULE.registry.canonical(value), 'arm64', None)

    def test_index_requires_one_exact_typed_runnable_platform(self):
        image, graph = registry_fixture('node', indexed=True)
        MODULE.registry.validate_graph(image, graph, development=False)
        raw = base64.b64decode(graph['index_base64'])
        value = json.loads(raw)
        value['manifests'].append(copy.deepcopy(value['manifests'][0]))
        raw = json.dumps(value, separators=(',', ':')).encode()
        image['index_digest'] = 'sha256:' + hashlib.sha256(raw).hexdigest()
        graph['index_base64'] = base64.b64encode(raw).decode()
        with self.assertRaises(MODULE.Refusal):
            MODULE.registry.validate_graph(image, graph, development=False)


def registry_fixture(role, indexed=False):
    config = json.dumps({'os': 'linux', 'architecture': 'arm64', 'config': {'Labels': {
        'org.opencontainers.image.source': 'https://github.com/ydixken/' + ('rpgmigrate' if role == 'native-runner' else 'pgcopydb-operator'),
        'org.opencontainers.image.revision': 'a' * 40, 'org.opencontainers.image.licenses': 'Apache-2.0'}} if role in ('manager', 'native-runner') else {}}, separators=(',', ':')).encode()
    cfg = 'sha256:' + hashlib.sha256(config).hexdigest()
    layer = {'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip', 'digest': 'sha256:' + 'c' * 64, 'size': 37}
    manifest = json.dumps({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json',
        'config': {'mediaType': 'application/vnd.oci.image.config.v1+json', 'digest': cfg, 'size': len(config)},
        'layers': [layer]}, separators=(',', ':')).encode()
    digest = 'sha256:' + hashlib.sha256(manifest).hexdigest()
    source = {'repository': 'https://github.com/ydixken/' + ('rpgmigrate' if role == 'native-runner' else 'pgcopydb-operator'),
        'commit': 'a' * 40, 'git_tree': 'b' * 40, 'payload_inventory_sha256': 'd' * 64}
    repos = {'node': 'docker.io/kindest/node', 'cnpg-controller': 'ghcr.io/cloudnative-pg/cloudnative-pg',
        'manager': 'ghcr.io/ydixken/pgcopydb-operator', 'native-runner': 'ghcr.io/ydixken/rpgmigrate',
        'legacy-runner': 'ghcr.io/ydixken/pgcopydb-operator/runner', 'seed-tool-runner': 'ghcr.io/ydixken/pgcopydb-operator/runner',
        'otel-collector': 'ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector',
        'storage-helper': 'docker.io/kindest/local-path-helper', 'storage-provisioner': 'docker.io/kindest/local-path-provisioner'}
    repo = 'ghcr.io/cloudnative-pg/postgresql' if role.startswith('postgresql-') else repos[role]
    origin = repo + (':' + role.rsplit('-', 1)[-1] if role.startswith('postgresql-') else ':pinned')
    image = {'role': role, 'ref': (origin if role.startswith('postgresql-') else repo) + '@' + digest,
        'os': 'linux', 'architecture': 'arm64', 'variant': None, 'digest': digest, 'index_digest': None, 'config_digest': cfg,
        'provenance': {'origin_ref': origin, 'registry_receipt_sha256': 'd' * 64, 'public_receipt_sha256': 'e' * 64}}
    graph = {'manifest_digest': digest, 'config_digest': cfg, 'index_digest': None,
        'os': 'linux', 'architecture': 'arm64', 'variant': None, 'manifest_base64': base64.b64encode(manifest).decode(),
        'config_base64': base64.b64encode(config).decode(), 'layers': [layer]}
    if role in ('manager', 'native-runner'):
        image['source'] = dict(source, build_input_sha256='e' * 64, executable_sha256='f' * 64, smoke_receipt_sha256='a' * 64)
        image['provenance']['origin_ref'] = repo + ':dev-' + source['commit'] + '-arm64'
    if indexed:
        raw = json.dumps({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.index.v1+json', 'manifests': [{
            'mediaType': 'application/vnd.oci.image.manifest.v1+json', 'digest': digest, 'size': len(manifest),
            'platform': {'os': 'linux', 'architecture': 'arm64'}}]}, separators=(',', ':')).encode()
        image['index_digest'] = 'sha256:' + hashlib.sha256(raw).hexdigest()
        graph = {'schema_version': 1, 'index_base64': base64.b64encode(raw).decode(),
            'manifest_base64': graph['manifest_base64'], 'config_base64': graph['config_base64']}
    return image, graph


def registry_lock():
    roles = ('node', 'cnpg-controller', 'postgresql-16', 'postgresql-17', 'postgresql-18', 'manager',
        'legacy-runner', 'native-runner', 'seed-tool-runner', 'otel-collector', 'storage-helper', 'storage-provisioner')
    images = {role: registry_fixture(role)[0] for role in roles}
    return {'schema_version': 2, 'transport': 'public-registry',
        'operator_source': {key: images['manager']['source'][key] for key in ('repository', 'commit', 'git_tree', 'payload_inventory_sha256')},
        'engine_source': {key: images['native-runner']['source'][key] for key in ('repository', 'commit', 'git_tree', 'payload_inventory_sha256')},
        'images': images, 'publication_receipts_sha256': {'manager': 'a' * 64, 'native-runner': 'b' * 64}}


class RegistryPullTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name).resolve()
        self.directory.chmod(0o700)
        self.guard = MODULE.Guard(self.directory, environ={})
        self.ledger = self.guard.initialize()
        self.images = registry_lock()['images']
        self.ledger.update(phase='ready', images=list(self.images.values()), nodes=[{
            'id': str(n) * 64, 'name': self.ledger['cluster'] + suffix, 'role': role}
            for n, suffix, role in ((1, '-control-plane', 'control-plane'), (2, '-worker', 'worker'), (3, '-worker2', 'worker'))],
            resource_lease={'expires_at': MODULE.time.time() + 3600, 'monotonic_expires_at': MODULE.time.monotonic() + 3600, 'monotonic_expires_at': MODULE.time.monotonic() + 3600})
        self.guard.save(self.ledger)
        self.guard.write_private('image-lock.json', json.dumps(registry_lock()).encode())
        self.guard.source_tree = lambda: 'd' * 64
        self.guard.inventory = lambda: list(self.images.values())
        self.guard.registry_admit = lambda ledger: ledger['nodes']
        self.guard.storage_metadata = lambda ledger, images: None
        self.calls = []
        self.present = set()
        self.corrupt = None
        self.guard.command = self.command
        for role in self.images:
            image, graph = registry_fixture(role)
            if role not in ('manager', 'native-runner'):
                graph = {'schema_version': 1, 'index_base64': None,
                    'manifest_base64': graph['manifest_base64'], 'config_base64': graph['config_base64']}
            self.guard.write_private(role + '-registry-receipt.json', json.dumps(graph).encode())

    def command(self, args, **kwargs):
        self.calls.append(args)
        if 'crictl' in args:
            action = args[-2]
            image = next(image for image in self.images.values() if image['ref'] == args[-1])
            if action == 'pull':
                self.present.add((args[2], image['ref']))
                return image['config_digest'] + '\n'
            if action == 'inspecti':
                if (args[2], image['ref']) not in self.present:
                    raise MODULE.CommandError('CalledProcessError')
                return json.dumps({'status': {'id': image['config_digest'], 'repoTags': [],
                    'repoDigests': [MODULE.registry.repository(image['ref']) + '@' + image['digest']]}})
        if 'ctr' in args:
            if 'list' in args:
                refs = {MODULE.registry.repository(image['ref']) + '@' + image['digest']: image
                    for image in self.images.values() if (args[2], image['ref']) in self.present}
                if args[-1] == '--quiet': return '\n'.join(refs) + ('\n' if refs else '')
                return 'REF TYPE DIGEST SIZE PLATFORMS LABELS\n' + ''.join(
                    ref + ' application/vnd.oci.image.manifest.v1+json ' + image['digest'] + ' 100 linux/arm64 -\n'
                    for ref, image in refs.items())
            if 'content' in args:
                for role in self.images:
                    image, graph = registry_fixture(role)
                    if args[-1] == image['digest']:
                        return base64.b64decode(graph['manifest_base64']).decode() + (' ' if self.corrupt == 'manifest' else '')
                    if args[-1] == image['config_digest']:
                        return base64.b64decode(graph['config_base64']).decode()
        raise AssertionError(args)

    def test_cri_prefix_uses_duration_for_pull_and_inspecti(self):
        for node in self.ledger['nodes']:
            self.assertEqual(self.guard.cri_prefix(node['id']), [
                'podman', 'exec', node['id'], 'env', '-i',
                'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
                'crictl', '--config=/dev/null', '--runtime-endpoint=unix:///run/containerd/containerd.sock',
                '--image-endpoint=unix:///run/containerd/containerd.sock', '--timeout=10s'])
        for node_id in (None, 1, [], '', '1' * 63, 'A' * 64, 'g' * 64, '1' * 64 + '\n',
                        'containerd://' + '1' * 64, 'sha256:' + '1' * 64):
            with self.subTest(node_id=node_id), self.assertRaises(MODULE.Refusal):
                self.guard.cri_prefix(node_id)
        self.assertEqual(self.calls, [])
        self.guard.pull()
        for action in ('pull', 'inspecti'):
            calls = [args for args in self.calls if 'crictl' in args and args[-2] == action]
            self.assertTrue(calls, action)
            for args in calls:
                self.assertIn('--timeout=10s', args)
                self.assertNotIn('--timeout=10', args)

    def test_cri_pull_failure_retains_safe_operation_class_and_returncode(self):
        sentinel = 'registry-password-fixture@foreign.invalid/repo@sha256:' + 'f' * 64
        cases = [('CalledProcessError', 1, 1), ('TimeoutExpired', None, None),
                 ('OSError', -255, -255), ('FileNotFoundError', 255, 255),
                 ('PermissionError', 0, 0), ('UnicodeDecodeError', None, None),
                 (sentinel, 1, 1), ('CalledProcessError', True, None),
                 ('CalledProcessError', 999, None), ('CalledProcessError', '1', None),
                 ('CalledProcessError', -256, None)]
        for failure_class, code, expected_code in cases:
            with self.subTest(failure_class=failure_class, returncode=code):
                path = self.directory / 'pulled-image-resolution-evidence.json'
                path.unlink(missing_ok=True)
                self.calls.clear()
                before = self.guard.private_bytes('ownership.json')
                def fail(args, **kwargs):
                    if 'crictl' in args and args[-2] == 'pull':
                        self.calls.append(args)
                        raise MODULE.CommandError(failure_class, code)
                    return self.command(args, **kwargs)
                self.guard.command = fail
                with self.assertRaises(MODULE.Refusal) as raised:
                    self.guard.pull()
                expected_class = 'CommandError' if failure_class == sentinel else failure_class
                expected = {'operation': 'cri-pull', 'error_class': expected_class, 'returncode': expected_code}
                evidence = self.guard.private_json(path.name)
                # The three nodes of the first role pull concurrently, so each records its own failure.
                self.assertEqual({tuple(args[-2:]) for args in self.calls}, {('pull', self.calls[0][-1])})
                self.assertEqual(sorted(evidence['pulls'], key=lambda pull: pull['node_id']), [dict(
                    node_id=node['id'], requested_ref=self.calls[0][-1], status='failed', **expected) for node in self.ledger['nodes']])
                self.assertIs(evidence['complete'], False)
                self.assertTrue(evidence['node_bindings'])
                self.assertTrue(all(nodes == {} for nodes in evidence['node_bindings'].values()))
                self.assertEqual(self.guard.read()['phase'], 'ready')
                self.assertEqual(self.guard.private_bytes('ownership.json'), before)
                self.assertEqual(str(raised.exception),
                    f'cri-pull failed (class={expected_class}, returncode={expected_code})')
                self.assertNotIn(sentinel, str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)
                self.assertTrue(raised.exception.__suppress_context__)
                self.assertEqual(len(self.calls), 3)
                with self.assertRaises(MODULE.Refusal):
                    self.guard.pull_evidence(self.ledger, MODULE.registry.PULL_ROLES, self.directory, path.name)
                with self.assertRaises(MODULE.Refusal):
                    self.guard.pull()
                self.assertEqual(len(self.calls), 3)

    def test_full_pull_deduplicates_physical_images_preserving_logical_matrix(self):
        self.guard.pull()
        ledger = self.guard.read()
        self.assertEqual(ledger['phase'], 'pulled')
        self.assertEqual(ledger['images'], self.ledger['images'])
        evidence = self.guard.private_json('pulled-image-resolution-evidence.json')
        self.assertTrue(evidence['complete'])
        self.assertEqual(set(evidence['node_bindings']), set(MODULE.registry.PULL_ROLES))
        self.assertEqual(sum(len(nodes) for nodes in evidence['node_bindings'].values()), 33)
        self.assertTrue({'native-runner', 'postgresql-16', 'postgresql-17', 'postgresql-18'} <= set(MODULE.registry.PULL_ROLES))
        expected = {(n['id'], self.images[r]['ref']) for r in MODULE.registry.PULL_ROLES for n in self.ledger['nodes']}
        self.assertEqual(len([a for a in self.calls if 'crictl' in a and 'pull' in a]), len(expected))
        before = len(self.calls)
        binding = self.guard.pulled_image_binding(ledger, 'manager')
        self.assertEqual(set(binding['node_bindings']), {n['name'] for n in ledger['nodes']})
        self.assertFalse(any('pull' in a for a in self.calls[before:]))

    def test_partial_pull_is_retained_and_cannot_resume(self):
        original = self.command
        def fail(args, **kwargs):
            if 'crictl' in args and 'pull' in args and args[2] == '2' * 64:
                raise MODULE.CommandError('TimeoutExpired')
            return original(args, **kwargs)
        self.guard.command = fail
        with self.assertRaises(MODULE.Refusal): self.guard.pull()
        self.assertEqual(self.guard.read()['phase'], 'ready')
        self.assertFalse(self.guard.private_json('pulled-image-resolution-evidence.json')['complete'])
        self.guard.require_prepared = lambda ledger: None
        with self.assertRaises(MODULE.Refusal): self.guard.check()
        count = len(self.calls)
        with self.assertRaises((MODULE.Refusal, FileExistsError)): self.guard.pull()
        self.assertEqual(len(self.calls), count)

    def test_cancelled_sibling_pull_never_masks_the_real_failure(self):
        nodes = self.ledger['nodes']
        def fail(args, **kwargs):
            if 'crictl' in args and args[-2] == 'pull' and args[2] == nodes[0]['id']:
                raise MODULE.Cancelled()
            if 'crictl' in args and args[-2] == 'pull' and args[2] == nodes[1]['id']:
                raise MODULE.CommandError('TimeoutExpired')
            return self.command(args, **kwargs)
        self.guard.command = fail
        with self.assertRaisesRegex(MODULE.Refusal, r'^cri-pull failed \(class=TimeoutExpired, returncode=None\)$'): self.guard.pull()
        pulls = {pull['node_id']: pull for pull in self.guard.private_json('pulled-image-resolution-evidence.json')['pulls']}
        self.assertEqual(pulls[nodes[0]['id']]['status'], 'failed')
        self.assertNotIn('error_class', pulls[nodes[0]['id']])

    def test_pull_deadline_is_checked_before_and_after_the_matrix(self):
        images = {image['role']: image for image in self.images.values()}
        with self.assertRaisesRegex(MODULE.Refusal, 'original deadline expired'):
            self.guard.perform_pulls(self.ledger, images, ('manager',), self.directory, 'early.json', MODULE.time.time() - 1,
                                     {'pulls': [], 'node_bindings': {}}, lambda: None)
        self.assertFalse(any('pull' in args for args in self.calls))
        now = MODULE.time.time()
        clock, deadline, proof = [now], now + 100, {'pulls': [], 'node_bindings': {}}
        def admit():
            if len(proof['node_bindings'].get('manager', {})) == 3:
                clock[0] = deadline + 1
        with patch.object(MODULE.time, 'time', side_effect=lambda: clock[0]), self.assertRaisesRegex(MODULE.Refusal, 'original deadline expired'):
            self.guard.perform_pulls(self.ledger, images, ('manager',), self.directory, 'late.json', deadline, proof, admit)
        self.assertEqual(len(proof['node_bindings']['manager']), 3)

    def test_completed_pull_evidence_refuses_partial_foreign_or_changed_matrices(self):
        self.guard.pull()
        ledger = self.guard.read()
        name = 'pulled-image-resolution-evidence.json'
        evidence = self.guard.private_json(name)
        for change, message in (('missing', 'matrix is incomplete'), ('duplicate', 'partial, duplicate or foreign'),
                                ('failed', 'partial, duplicate or foreign')):
            with self.subTest(change=change):
                value = copy.deepcopy(evidence)
                if change == 'missing': value['pulls'].pop()
                if change == 'duplicate': value['pulls'][-1] = value['pulls'][0]
                if change == 'failed': value['pulls'][0]['status'] = 'failed'
                self.guard.write_private(name, MODULE.registry.canonical(value) + b'\n')
                with self.assertRaisesRegex(MODULE.Refusal, message):
                    self.guard.pull_evidence(ledger, MODULE.registry.PULL_ROLES, self.directory, name)
        self.guard.write_private(name, MODULE.registry.canonical(evidence) + b'\n')
        self.guard.pull_evidence(ledger, MODULE.registry.PULL_ROLES, self.directory, name)
        verify = self.guard._verify_pulled
        def changed(image, node_id, *args):
            binding = verify(image, node_id, *args)
            return dict(binding, accepted_image_ids=binding['accepted_image_ids'][:-1]) if node_id == '3' * 64 else binding
        self.guard._verify_pulled = changed
        with self.assertRaisesRegex(MODULE.Refusal, 'differs from completed pull evidence'):
            self.guard.pull_evidence(ledger, MODULE.registry.PULL_ROLES, self.directory, name)

    def test_foreign_node_and_unavailable_content_refuse_without_pull(self):
        image = self.images['manager']
        self.present.add(('1' * 64, image['ref']))
        with self.assertRaises(MODULE.Refusal): self.guard.verify_pulled(image, 'f' * 64)
        self.corrupt = 'manifest'
        with self.assertRaises(MODULE.Refusal): self.guard.verify_pulled(image, '1' * 64)
        self.assertFalse(any('pull' in a for a in self.calls))

    def component_packet(self):
        self.guard.pull()
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        qual = Path(temp.name).resolve(); qual.chmod(0o700)
        self.guard.require_prepared = lambda ledger: None
        ledger = self.guard.read()
        packet = {'schema_version': 2, 'qualification_id': 'a' * 32, 'run_id': ledger['run_id'],
            'legacy_source_root': str(MODULE.ROOT), 'legacy_source_tree_sha256': 'd' * 64,
            'legacy_guard_sha256': MODULE.registry.sha(SCRIPT.read_bytes()),
            'legacy_ownership_sha256': MODULE.registry.sha(self.guard.private_bytes('ownership.json')),
            'qualification_source_sha256': 'b' * 64, 'evidence_program_sha256': 'c' * 64,
            'image_lock_sha256': MODULE.registry.sha(self.guard.private_bytes('image-lock.json')),
            'images': {role: {'image': self.images[role]} for role in ('native-runner', 'postgresql-16', 'postgresql-18')}}
        raw = MODULE.registry.canonical(packet) + b'\n'
        path = qual / 'native-assets.json'; path.write_bytes(raw); path.chmod(0o600)
        return qual, packet

    def test_component_direct_pull_and_separate_verify_budget_preserve_parent_and_original_proof(self):
        qual, packet = self.component_packet()
        before = self.guard.private_bytes('ownership.json')
        original_deadline = MODULE.time.time() + 900
        proof = self.guard.qualify_component_images(qual, packet, original_deadline)
        self.assertEqual(set(proof['node_bindings']), {'native-runner', 'postgresql-16', 'postgresql-18'})
        self.assertEqual(sum(len(v) for v in proof['node_bindings'].values()), 9)
        self.assertEqual(len(proof['pulls']), 9)
        self.assertEqual(proof['deadline'], original_deadline)
        self.assertEqual(self.guard.private_bytes('ownership.json'), before)
        count = len(self.calls)
        read_only = self.guard.verify_component_images(qual, packet, MODULE.time.time() + 600)
        self.assertEqual(read_only, proof)
        self.assertEqual(self.guard.private_bytes('ownership.json'), before)
        self.assertFalse(any('pull' in a for a in self.calls[count:]))

    def test_component_missing_partial_changed_packet_and_expired_stage_refuse_without_pull(self):
        qual, packet = self.component_packet()
        for case in ('missing', 'schema1', 'ownership', 'expired'):
            bad = copy.deepcopy(packet)
            if case == 'schema1': bad['schema_version'] = 1
            if case == 'ownership': bad['legacy_ownership_sha256'] = 'f' * 64
            path = qual / 'native-assets.json'; path.write_bytes(MODULE.registry.canonical(bad) + b'\n')
            count = len(self.calls)
            with self.subTest(case=case), self.assertRaises(MODULE.Refusal):
                self.guard.verify_component_images(qual, bad, 1 if case == 'expired' else MODULE.time.time() + 600)
            self.assertFalse(any('pull' in a for a in self.calls[count:]))
        path.write_bytes(MODULE.registry.canonical(packet) + b'\n')
        proof = self.guard.qualify_component_images(qual, packet, MODULE.time.time() + 900)
        proof['complete'] = False
        path = qual / 'component-image-resolution-evidence.json'; path.write_bytes(MODULE.registry.canonical(proof) + b'\n')
        count = len(self.calls)
        with self.assertRaises(MODULE.Refusal): self.guard.verify_component_images(qual, packet, MODULE.time.time() + 600)
        self.assertFalse(any('pull' in a for a in self.calls[count:]))

    def test_stage_and_parent_clocks_clip_calls_without_renewal(self):
        ledger = copy.deepcopy(self.ledger)
        ledger['resource_lease'].update(expires_at=5000, monotonic_expires_at=6000)
        self.guard.save(ledger)
        calls = []
        self.guard.command = lambda args, **kwargs: calls.append(kwargs['timeout']) or ''
        with patch.object(MODULE.time, 'time', return_value=1000), patch.object(MODULE.time, 'monotonic', return_value=2000):
            self.guard.registry_call(ledger, ['inert'], deadline=1000.5, monotonic_deadline=2000.25, timeout=300)
        self.assertEqual(calls, [0.25])
        changed = copy.deepcopy(ledger); changed['resource_lease']['expires_at'] += 1; self.guard.save(changed)
        with patch.object(MODULE.time, 'time', return_value=1000), patch.object(MODULE.time, 'monotonic', return_value=2000), self.assertRaisesRegex(MODULE.Refusal, 'changed'):
            self.guard.registry_call(ledger, ['inert'])
        self.assertEqual(calls, [0.25])

    def test_original_clock_expiration_refuses_before_command(self):
        for key in ('expires_at', 'monotonic_expires_at'):
            ledger = copy.deepcopy(self.ledger)
            ledger['resource_lease'][key] = 1
            self.guard.save(ledger)
            with self.subTest(clock=key), self.assertRaises(MODULE.Refusal):
                self.guard.registry_call(ledger, ['inert'])
        self.assertEqual(self.calls, [])

    def test_raw_id_validation_preserves_exact_forms(self):
        image = self.images['manager']
        node = self.ledger['nodes'][0]
        binding = {'node_id': node['id'], 'requested_ref': image['ref'], 'manifest_digest': image['digest'],
            'config_digest': image['config_digest'], 'os': 'linux', 'architecture': 'arm64', 'variant': None,
            'accepted_image_ids': sorted([image['digest'], image['config_digest'], image['ref']])}
        MODULE.registry.validate_binding(binding, image, node['id'])
        for value in ('containerd://' + image['config_digest'], 'foreign.invalid/repo@' + image['digest'], image['config_digest'][7:]):
            bad = copy.deepcopy(binding)
            bad['accepted_image_ids'] = sorted(bad['accepted_image_ids'] + [value])
            with self.subTest(value=value), self.assertRaises(MODULE.Refusal):
                MODULE.registry.validate_binding(bad, image, node['id'])




class RegistryStorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name).resolve(); self.directory.chmod(0o700)
        self.guard = MODULE.Guard(self.directory, environ={})
        self.lock = registry_lock()
        catalog = json.loads((MODULE.CONFIG / 'images.json').read_bytes())
        self.lock['images']['node']['digest'] = catalog['storage_manifest']['node_manifest_digest']
        self.lock['images']['node']['index_digest'] = catalog['storage_manifest']['node_index_digest']
        for entry in catalog['images']:
            if entry['role'] in ('storage-helper', 'storage-provisioner') and entry['architecture'] == 'arm64':
                self.lock['images'][entry['role']]['provenance']['origin_ref'] = entry['origin_ref']

    def test_only_admitted_storage_refs_are_rendered_before_kind_creation(self):
        outputs = self.guard.storage_outputs(self.lock)
        source = outputs['node-default-storage-source.yaml']
        rendered = outputs['node-default-storage.yaml']
        expected = source
        for role in ('storage-helper', 'storage-provisioner'):
            image = self.lock['images'][role]
            expected = expected.replace(image['provenance']['origin_ref'].encode(), image['ref'].encode())
        self.assertEqual(rendered, expected)
        self.assertEqual(MODULE.registry.sha(source), 'dc16a44e6a49a1f26fc39f341c361e7939993bd68431efc3a49d216e5914e903')
        self.assertNotIn(b'image: docker.io/kindest/local-path-helper:v', rendered)
        cluster = (MODULE.CONFIG / 'cluster.yaml').read_text()
        self.assertEqual(cluster.count('containerPath: /kind/manifests/default-storage.yaml'), 1)
        self.assertIn('readOnly: true', cluster)

    def test_shared_storage_path_and_hash_refuse_before_any_provider_creation(self):
        raw = b'sealed digest manifest'
        self.guard.write_private('node-default-storage.yaml', raw)
        path = self.directory / 'node-default-storage.yaml'
        good = {'realpath': str(path) + '\n', 'sha256sum': MODULE.registry.sha(raw) + '  ' + str(path) + '\n'}
        self.guard.vm = lambda args: good[args[0]]
        self.guard.shared_storage()
        for command in ('realpath', 'sha256sum'):
            saved = good[command]; good[command] = 'wrong\n'
            with self.subTest(command=command), self.assertRaises(MODULE.Refusal): self.guard.shared_storage()
            good[command] = saved

    def test_exact_control_plane_readonly_mount_and_node_bytes_are_required(self):
        raw = b'sealed digest manifest'
        self.guard.write_private('node-default-storage.yaml', raw)
        node = {'id': 'a' * 64, 'name': 'owned-control-plane', 'role': 'control-plane'}
        ledger = {'nodes': [node]}
        mount = {'Type': 'bind', 'Source': str(self.directory / 'node-default-storage.yaml'),
            'Destination': '/kind/manifests/default-storage.yaml', 'RW': False}
        content = [raw.decode()]
        self.guard.command = lambda args, **kwargs: json.dumps([{'Id': node['id'], 'Mounts': [mount]}]) if args[1] == 'inspect' else content[0]
        self.guard.verify_storage_mount(ledger)
        for key, value in (('RW', True), ('Type', 'volume'), ('Source', '/unowned/path'), ('Destination', '/other')):
            saved = mount[key]; mount[key] = value
            with self.subTest(key=key), self.assertRaises(MODULE.Refusal): self.guard.verify_storage_mount(ledger)
            mount[key] = saved
        content[0] += 'changed'
        with self.assertRaises(MODULE.Refusal): self.guard.verify_storage_mount(ledger)


class RegistrySourceTests(unittest.TestCase):
    def test_source_only_accepts_exact_known_github_origins(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp).resolve(); directory.chmod(0o700)
            guard = MODULE.Guard(directory, environ={})
            expected = {'repository': 'https://github.com/ydixken/pgcopydb-operator', 'commit': 'a' * 40,
                'git_tree': 'b' * 40, 'payload_inventory_sha256': 'd' * 64}
            def command(args, **kwargs):
                if args[1] == 'status': return ''
                if args[1] == 'remote': return origin + '\n'
                return ('a' if args[-1] == 'HEAD' else 'b') * 40 + '\n'
            guard.command = command
            guard.source_tree = lambda: 'd' * 64
            for origin in ('https://github.com/ydixken/pgcopydb-operator', 'https://github.com/ydixken/pgcopydb-operator.git',
                    'git@github.com:ydixken/pgcopydb-operator.git'):
                with self.subTest(origin=origin): self.assertEqual(guard.source_identity(), expected)
            for origin in ('https://user:secret@github.com/ydixken/pgcopydb-operator.git',
                    'https://github.com/unknown/pgcopydb-operator.git', 'https://github.com/ydixken/pgcopydb-operator.git.git',
                    'git@github.com:unknown/pgcopydb-operator.git', 'git@unknown.invalid:ydixken/pgcopydb-operator.git',
                    'other@github.com:ydixken/pgcopydb-operator.git', 'git@github.com.evil.invalid:ydixken/pgcopydb-operator.git',
                    'ssh://git@github.com/ydixken/pgcopydb-operator.git', 'git@github.com:ydixken/pgcopydb-operator',
                    'git@github.com:ydixken/rpgmigrate.git', 'git@github.com:ydixken/pgcopydb-operator.git '):
                with self.subTest(origin=origin), self.assertRaises(MODULE.Refusal): guard.source_identity()
            guard.command = lambda *args, **kwargs: '?? untracked-input\n'
            with self.assertRaises(MODULE.Refusal): guard.source_identity()

    def test_external_file_rejects_private_mode_links_and_unbounded_values(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp).resolve(); directory.chmod(0o700)
            guard = MODULE.Guard(directory, environ={})
            path = directory / 'receipt.json'; path.write_text('{}'); path.chmod(0o600)
            self.assertEqual(guard.external_bytes(path), b'{}')
            path.chmod(0o644)
            with self.assertRaises(MODULE.Refusal): guard.external_bytes(path)
            path.chmod(0o600)
            linked = directory / 'linked'; linked.symlink_to(path)
            with self.assertRaises(MODULE.Refusal): guard.external_bytes(linked)
            with self.assertRaises(MODULE.Refusal): guard.external_bytes(path, maximum=1)
            path.write_bytes(b'')
            with self.assertRaises(MODULE.Refusal): guard.external_bytes(path)

class RegistryPreparationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.root.chmod(0o700)
        self.receipts, self.catalog, self.run = [self.root / name for name in ('receipts', 'catalog', 'run')]
        for path in (self.receipts, self.catalog, self.run): path.mkdir(mode=0o700)
        self.guard = MODULE.Guard(self.run, environ={})
        self.lock = registry_lock()
        self.graphs = {}
        for role, image in self.lock['images'].items():
            _, graph = registry_fixture(role)
            if role not in MODULE.registry.DEV_ROLES:
                graph = {'schema_version': 1, 'index_base64': None,
                    'manifest_base64': graph['manifest_base64'], 'config_base64': graph['config_base64']}
            self.graphs[role] = graph
            layers = MODULE.registry.validate_graph(image, graph, role in MODULE.registry.DEV_ROLES)['layers']
            public = {'graph': graph, 'blobs': [{'digest': p['digest'], 'size': p['size']} for p in layers]}
            image['provenance'].update(registry_receipt_sha256=MODULE.registry.sha(MODULE.registry.canonical(graph)),
                public_receipt_sha256=MODULE.registry.sha(MODULE.registry.canonical(public)))
            self.write(role + '-registry-receipt.json', graph)
            self.write(role + '-public-receipt.json', public)
            if role in MODULE.registry.DEV_ROLES:
                receipt = development_receipt(image, graph, public)
                self.write(role + '-publication-receipt.json', receipt)
                self.lock['publication_receipts_sha256'][role] = MODULE.registry.sha((self.receipts / (role + '-publication-receipt.json')).read_bytes())
        entries = []
        for role, image in self.lock['images'].items():
            entries.append({'role': role, 'os': image['os'], 'architecture': image['architecture'], 'variant': image['variant'],
                'repository': MODULE.registry.repository(image['ref']),
                'origin_ref': None if role in MODULE.registry.DEV_ROLES else image['provenance']['origin_ref'],
                **{k: None if role in MODULE.registry.DEV_ROLES else image[k] for k in ('digest', 'index_digest', 'config_digest')}})
        actual = self.guard.parse_runner_probe(runner_packet('a' * 64))
        image = self.lock['images']['legacy-runner']
        q = {'actual': actual, 'manifest_digest': image['digest'], 'image_id': image['config_digest'],
            'architecture': 'arm64', 'os': 'linux', 'builder_ref': MODULE.BASELINE_BUILDER, 'ref': image['ref'],
            'trusted_builder_binary_sha256': 'a' * 64, 'container_id': 'f' * 64, 'network': 'none',
            'nonroot': True, 'read_only': True, 'host_mounts': False, 'source_pin_changed': False,
            'signature_verified': False, 'runtime_migration_qualified': False, 'cleanup_removed_and_absence_proven': True}
        qraw = MODULE.registry.canonical(q)
        eraw = MODULE.registry.canonical({'builder_ref': MODULE.BASELINE_BUILDER, 'binary_sha256': 'a' * 64,
            'qualification_sha256': MODULE.registry.sha(qraw), 'runner_binary_member': {
                'member': 'usr/lib/postgresql/18/bin/pgcopydb', 'layer_digest': 'sha256:' + 'c' * 64, 'sha256': 'a' * 64, 'size': 123}})
        praw = runner_packet('a' * 64).encode()
        for name, raw in (('evidence.json', eraw), ('qualification.json', qraw), ('probe.stdout', praw)):
            self.write_raw('legacy-runner-original-' + name, raw)
        pin = {'architecture': 'arm64', 'os': 'linux', 'variant': None, 'runner_repository': MODULE.registry.repository(image['ref']),
            'runner_manifest_digest': image['digest'], 'runner_config_digest': image['config_digest'],
            'baseline_commit': '86bf908adca02d87063849dae775a02c1659c829', 'builder_ref': MODULE.BASELINE_BUILDER,
            'binary_sha256': 'a' * 64, 'binary_size': 123, 'binary_layer_digest': 'sha256:' + 'c' * 64,
            'original_evidence_sha256': MODULE.registry.sha(eraw), 'original_qualification_sha256': MODULE.registry.sha(qraw),
            'original_probe_sha256': MODULE.registry.sha(praw)}
        proof = {'schema_version': 1, 'role': 'legacy-runner', 'ref': image['ref'], 'manifest_digest': image['digest'],
            'config_digest': image['config_digest'], 'os': 'linux', 'architecture': 'arm64', 'variant': None,
            **{k: pin[k] for k in ('baseline_commit', 'builder_ref', 'binary_sha256', 'binary_size', 'binary_layer_digest',
                'original_evidence_sha256', 'original_qualification_sha256', 'original_probe_sha256')}, 'actual': actual,
            'execution': {k: q[k] for k in ('nonroot', 'read_only', 'network', 'host_mounts', 'source_pin_changed',
                'runtime_migration_qualified', 'cleanup_removed_and_absence_proven')}}
        self.write('legacy-runner-executable-receipt.json', proof)
        self.catalog_value = {'schema': 2, 'cnpg': {'version': '1.30.1', 'manifest_sha256': MODULE.CNPG_SOURCE_SHA256},
            'legacy_baseline': pin['baseline_commit'], 'legacy_qualification': pin, 'storage_manifest': {}, 'images': entries}
        (self.catalog / 'images.json').write_text(json.dumps(self.catalog_value))
        self.lock_path = self.receipts / 'image-lock.json'
        self.write('image-lock.json', self.lock)
        self.guard.architecture = lambda: 'arm64'
        self.guard.source_identity = lambda: self.lock['operator_source']
        self.guard.source_tree = lambda: 'd' * 64
        self.guard.source_inventory = lambda: [['fixture', 420, 'e' * 64]]
        self.guard.tools = lambda: None
        self.guard.preparation_tools = lambda: None
        self.guard.preparation_capacity = lambda: None
        self.guard.storage_outputs = lambda lock: {}
        self.anonymous = []
        self.guard.anonymous_metadata = lambda image, metadata: self.anonymous.append(image['role'])
        self.config_patch = patch.object(MODULE, 'CONFIG', self.catalog)
        self.config_patch.start()
        self.addCleanup(self.config_patch.stop)

    def write_raw(self, name, raw):
        path = self.receipts / name
        path.write_bytes(raw); path.chmod(0o600)

    def write(self, name, value):
        self.write_raw(name, MODULE.registry.canonical(value) + b'\n')

    def test_prepare_retains_only_complete_metadata_and_check_is_read_only(self):
        ledger = self.guard.prepare(self.lock_path, self.receipts)
        self.assertEqual(ledger['phase'], 'prepared')
        self.assertEqual(set(self.anonymous), set(MODULE.registry.ROLES))
        self.assertEqual(self.guard.private_json('artifact-preparation-evidence.json')['files'].keys(),
            {p.name for p in self.run.iterdir()} - {'ownership.json', 'artifact-preparation-evidence.json'})
        self.assertFalse(any('original' in p.name for p in self.run.iterdir()))
        before = {p.name: p.read_bytes() for p in self.run.iterdir()}
        self.assertEqual(self.guard.check(), ledger)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.run.iterdir()})
        self.assertEqual(len(self.anonymous), 12)

    def test_private_pending_source_schema_and_receipt_failures_precede_any_acquisition(self):
        for case in ('pending', 'source', 'schema1', 'private', 'duplicate', 'baseline'):
            with self.subTest(case=case):
                raw = self.lock_path.read_bytes()
                publication = self.receipts / 'manager-publication-receipt.json'
                pubraw = publication.read_bytes()
                proof = self.receipts / 'legacy-runner-original-probe.stdout'
                proofraw = proof.read_bytes()
                try:
                    if case == 'pending':
                        p = json.loads(pubraw); p['status'] = 'PUBLICITY_PENDING'; self.write(publication.name, p)
                        self.lock['publication_receipts_sha256']['manager'] = MODULE.registry.sha(publication.read_bytes()); self.write('image-lock.json', self.lock)
                    elif case == 'source': self.guard.source_identity = lambda: dict(self.lock['operator_source'], commit='f' * 40)
                    elif case == 'schema1':
                        bad = copy.deepcopy(self.lock); bad['schema_version'] = 1; self.write('image-lock.json', bad)
                    elif case == 'private': self.lock_path.chmod(0o644)
                    elif case == 'duplicate': self.lock_path.write_bytes(b'{"schema_version":2,"schema_version":2}')
                    else: proof.write_bytes(proofraw + b' ')
                    with self.assertRaises(MODULE.Refusal): self.guard.prepare(self.lock_path, self.receipts)
                    self.assertEqual(self.anonymous, [])
                    self.assertEqual(list(self.run.iterdir()), [])
                finally:
                    self.lock_path.write_bytes(raw); self.lock_path.chmod(0o600)
                    publication.write_bytes(pubraw); proof.write_bytes(proofraw)
                    self.lock = json.loads(raw)
                    self.guard.source_identity = lambda: self.lock['operator_source']

    def test_changed_external_lock_or_retained_receipt_refuses_read_only_check(self):
        self.guard.prepare(self.lock_path, self.receipts)
        before = (self.run / 'ownership.json').read_bytes()
        raw = self.lock_path.read_bytes(); self.lock_path.write_bytes(raw + b' ')
        with self.assertRaises(MODULE.Refusal): self.guard.check()
        self.assertEqual((self.run / 'ownership.json').read_bytes(), before)
        self.lock_path.write_bytes(raw)
        path = self.run / 'manager-public-receipt.json'; path.write_bytes(path.read_bytes() + b' ')
        with self.assertRaises(MODULE.Refusal): self.guard.check()
        self.assertEqual((self.run / 'ownership.json').read_bytes(), before)

    def test_native_smoke_cannot_qualify_missing_runtime_or_license_facts_even_when_hashes_agree(self):
        original = json.loads((self.receipts / 'native-runner-publication-receipt.json').read_bytes())
        for case in ('ctypes', 'license', 'libpq', 'client', 'inputs', 'mount'):
            proof = copy.deepcopy(original)
            smoke = proof['smoke_receipt']
            if case == 'ctypes': smoke['python']['ctypes'] = False
            elif case == 'license': smoke['python']['license_sha256'] = 'f' * 64
            elif case == 'libpq': smoke['libpq']['runtime_version'] = 170000
            elif case == 'client': smoke['clients']['18']['psql'] = 'psql (PostgreSQL) 16.1'
            elif case == 'inputs': smoke['source_inputs'] = []
            else: smoke['mounts']['modes']['/'] = 'rw'
            proof['smoke_receipt_sha256'] = MODULE.registry.sha(MODULE.registry.canonical(smoke))
            proof['resolved_image']['source']['smoke_receipt_sha256'] = proof['smoke_receipt_sha256']
            with self.subTest(case=case), self.assertRaises(MODULE.Refusal):
                MODULE.registry.validate_publication(proof['resolved_image'], proof, proof['registry_receipt'], proof['public_receipt'])

    def test_final_preparation_validation_failure_never_leaves_complete_authority(self):
        self.guard.check = lambda: (_ for _ in ()).throw(MODULE.Refusal('final source changed'))
        with self.assertRaises(MODULE.Refusal): self.guard.prepare(self.lock_path, self.receipts)
        self.assertFalse(self.guard.private_json('artifact-preparation-evidence.json')['complete'])

    def test_failed_anonymous_fetch_keeps_preparation_incomplete_and_never_retries(self):
        self.guard.anonymous_metadata = lambda *args: (_ for _ in ()).throw(MODULE.CommandError('CalledProcessError'))
        with self.assertRaises(MODULE.Refusal): self.guard.prepare(self.lock_path, self.receipts)
        self.assertFalse(self.guard.private_json('artifact-preparation-evidence.json')['complete'])
        with self.assertRaises(MODULE.Refusal): self.guard.prepare(self.lock_path, self.receipts)
        self.assertEqual(self.guard.read()['phase'], 'new')


def development_receipt(image, graph, public):
    source = {k: image['source'][k] for k in MODULE.registry.SOURCE_KEYS}
    if image['role'] == 'manager':
        smoke = {'schema_version': 1, 'image_id': image['config_digest'], 'os': 'linux', 'architecture': 'arm64',
            'executable_sha256': image['source']['executable_sha256'], 'source_version': 'm7-' + source['payload_inventory_sha256'][:12],
            'help_passed': True, 'read_only': True, 'user': '65532:65532', 'network': 'none', 'build_info_observed': False}
    else:
        inputs = ['a' * 64 + '  ' + name for name in ('.dockerignore', 'Cargo.lock', 'Cargo.toml', 'Dockerfile', 'LICENSE', 'NOTICE', 'build.rs', 'src/main.rs')]
        image['source']['build_input_sha256'] = MODULE.registry.sha(('\n'.join(inputs) + '\n').encode())
        metadata = {'version': 1, 'architecture': 'arm64', 'library_path': '/usr/lib/aarch64-linux-gnu/libpq.so.5.18',
            'library_sha256': 'c' * 64, 'libpq_version': 180001, 'package_name': 'libpq5', 'package_version': '18.1-1.pgdg13+2'}
        packages = {name: {'version': '1', 'architecture': 'arm64', 'source': name, 'source_version': '1',
            'copyright_path': '/usr/share/doc/' + name + '/copyright', 'copyright_sha256': 'd' * 64}
            for name in ('python3', 'python3-minimal', 'python3.13', 'python3.13-minimal', 'libpython3.13-stdlib',
                'libpython3.13-minimal', 'libffi8', 'libc6', 'libpq5', 'libssl3t64', 'postgresql-client-16', 'postgresql-client-18')}
        packages['libpq5']['version'] = metadata['package_version']
        python = {'version': '3.13.5', 'isolated': True, 'ctypes': True, 'private_input': True,
            'modules': ['ctypes', '_ctypes', 'os', 'stat', 'fcntl', 'selectors', 'select', 'time', 'json', 'hashlib', 'struct', 'errno', 'sys', 'binascii', 'resource'],
            'symbols': ['PQlibVersion', 'PQconnectStart', 'PQconnectPoll', 'PQsetnonblocking', 'PQsocket', 'PQstatus',
                'PQtransactionStatus', 'PQbackendPID', 'PQsetNoticeReceiver', 'PQresultErrorField', 'PQsendQueryParams',
                'PQflush', 'PQconsumeInput', 'PQisBusy', 'PQgetResult', 'PQresultStatus', 'PQcmdStatus', 'PQcmdTuples',
                'PQntuples', 'PQnfields', 'PQgetisnull', 'PQgetlength', 'PQgetvalue', 'PQclear', 'PQfinish', 'PQescapeIdentifier', 'PQfreemem'],
            'metadata': metadata, 'metadata_sha256': 'e' * 64, 'mapped_library': metadata['library_path'], 'packages': packages,
            'python_license': {'path': '/usr/share/doc/libpython3.13-stdlib/copyright', 'sha256': 'd' * 64},
            'dependency_notices': {'unicode-ident-1.0.27/LICENSE-UNICODE': 'f' * 64}, 'notice_sha256': 'a' * 64, 'license_sha256': 'a' * 64}
        smoke = {'runtime_binary_sha256': image['source']['executable_sha256'], 'python': python, 'source_inputs': inputs,
            'image_id': image['config_digest'], 'architecture': 'arm64', 'effective_uid': 65532,
            'version': {'protocol_version': 1, 'engine_version': '0.1.0', 'capabilities': ['fixture.capability']},
            'libpq': {'runtime_version': 180001, 'ldd': 'libpq fixture', 'resolved_path': metadata['library_path'],
                'package_version': metadata['package_version'], 'package_owner': 'libpq5'},
            'clients': {str(n): {tool: tool + ' (PostgreSQL) ' + str(n) + '.1' for tool in ('psql', 'pg_dump', 'pg_restore')} for n in (16, 18)},
            'default_psql': 'psql (PostgreSQL) 18.1', 'build_inputs_sha256': image['source']['build_input_sha256'],
            'mounts': {'modes': {'/': 'ro', '/work': 'rw', '/tmp': 'rw'}, 'work_owner': '65532:65532:700', 'tmp_owner': '65532:65532:700'},
            'checks': {'read_only_rootfs': True, 'work_write': True, 'tmp_write': True, 'root_write_denied': True, 'notices': True, 'gnu_utilities': True}}
    image['source']['smoke_receipt_sha256'] = MODULE.registry.sha(MODULE.registry.canonical(smoke))
    return {'schema_version': 1, 'status': 'PUBLIC', 'role': image['role'], 'source': source,
        'tag': 'dev-' + source['commit'] + '-arm64', 'architecture': 'arm64', 'smoke_receipt': smoke,
        'smoke_receipt_sha256': image['source']['smoke_receipt_sha256'], 'authority': {'visibility': 'public'},
        'os': 'linux', 'variant': None, 'index_digest': None, 'manifest_digest': image['digest'], 'config_digest': image['config_digest'],
        'registry_receipt': graph, 'build_input_sha256': image['source']['build_input_sha256'],
        'executable_sha256': image['source']['executable_sha256'], 'public_receipt': public, 'resolved_image': image}

class EvidenceRootTests(unittest.TestCase):
    def test_volatile_tmp_run_directories_refuse_before_creation(self):
        for raw in ('/tmp/rpgmigrate-m7-volatile-control', '/private/tmp/rpgmigrate-m7-volatile-control'):
            path = Path(raw)
            with self.subTest(path=raw), self.assertRaises(MODULE.Refusal):
                MODULE.Guard(path, environ={})
            self.assertFalse(path.exists())

    def test_prepare_defaults_to_a_fresh_reboot_safe_run_directory(self):
        seen = []
        def capture(directory, environ=None):
            seen.append(directory)
            raise MODULE.Refusal('captured')
        with patch.object(MODULE.sys, 'argv', ['e2e-kind.py', 'prepare', '--image-lock', '/x', '--receipt-dir', '/y']), \
                patch.object(MODULE, 'Guard', side_effect=capture), patch.object(MODULE.sys, 'stderr', io.StringIO()):
            self.assertEqual(MODULE.main(), 1)
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].parent, Path('/var/tmp').resolve())
        self.assertRegex(seen[0].name, r'\Arpgmigrate-m7-run-[0-9a-f]{32}\Z')

    def test_other_commands_still_require_an_explicit_run_directory(self):
        with patch.object(MODULE.sys, 'argv', ['e2e-kind.py', 'check']), \
                patch.object(MODULE, 'Guard', side_effect=AssertionError('guard must not be constructed')), \
                patch.object(MODULE.sys, 'stderr', io.StringIO()), self.assertRaises(SystemExit) as failure:
            MODULE.main()
        self.assertEqual(failure.exception.code, 2)


class HostSleepGuardTests(GuardFixture):
    ASSERTIONS = ('   pid {pid}(caffeinate): [0x1] 00:00:00 PreventUserIdleSystemSleep named: "caffeinate command-line tool"\n'
                  '   pid {pid}(caffeinate): [0x2] 00:00:00 PreventSystemSleep named: "caffeinate command-line tool"\n')

    class Child:
        pid = 4242
        def __init__(self):
            self.killed = False
        def poll(self):
            return None
        def kill(self):
            self.killed = True
        def wait(self, timeout=None):
            return 0

    def hold(self, battery, assertions):
        child = self.Child()
        spawned = []
        def command(args, **kwargs):
            if args == ['pmset', '-g', 'batt']:
                return battery
            if args == ['pmset', '-g', 'assertions']:
                return assertions.format(pid=child.pid)
            raise AssertionError(args)
        self.guard.command = command
        def popen(args, **kwargs):
            spawned.append(args)
            return child
        with patch.object(MODULE.platform, 'system', return_value='Darwin'), patch.object(MODULE.subprocess, 'Popen', side_effect=popen), \
                patch.object(MODULE, 'SLEEP_ASSERTION_SECONDS', 0.05):
            result = self.guard.hold_awake()
        return result, child, spawned

    def test_ac_power_and_live_caffeinate_assertions_admit(self):
        result, child, spawned = self.hold("Now drawing from 'AC Power'\n", self.ASSERTIONS)
        self.assertIs(result, child)
        self.assertEqual(spawned, [['caffeinate', '-dims', '-w', str(os.getpid())]])

    def test_battery_power_refuses_before_spawning(self):
        with self.assertRaisesRegex(MODULE.Refusal, 'AC power'):
            self.hold("Now drawing from 'Battery Power'\n", self.ASSERTIONS)

    def test_missing_system_sleep_assertion_kills_child_and_refuses(self):
        partial = self.ASSERTIONS.splitlines()[0] + '\n'
        for assertions in ('', partial, self.ASSERTIONS.replace('{pid}', '1')):
            with self.subTest(assertions=assertions):
                child = self.Child()
                self.guard.command = lambda args, **kwargs: ("Now drawing from 'AC Power'\n" if args[-1] == 'batt' else assertions.format(pid=child.pid))
                with patch.object(MODULE.platform, 'system', return_value='Darwin'), \
                        patch.object(MODULE.subprocess, 'Popen', return_value=child), \
                        patch.object(MODULE, 'SLEEP_ASSERTION_SECONDS', 0.05), self.assertRaisesRegex(MODULE.Refusal, 'sleep assertion'):
                    self.guard.hold_awake()
                self.assertTrue(child.killed)


class RetainedFailureReplayTests(GuardFixture):
    def lease(self, wall_start, mono_start):
        self.ledger['resource_lease'] = {'authorized': True, 'm7_cpus': 6, 'm7_memory_mib': 12288, 'm3_reserved_cpus': 6,
            'm3_reserved_memory_mib': 6144, 'expires_at': wall_start + 7200, 'monotonic_expires_at': mono_start + 7200}
        self.guard.save(self.ledger)

    def test_29ed_wall_only_expiry_classifies_as_host_sleep(self):
        # 29ed S2-failed-admission-proof.json, SHA-256 b658467f...e3b: original_policy and observed elapsed clocks.
        wall_start, mono_start = 1794319491.737077, 605583.336303916
        self.lease(wall_start, mono_start)
        with patch.object(MODULE.time, 'time', return_value=wall_start + 15869.021349906921), \
                patch.object(MODULE.time, 'monotonic', return_value=mono_start + 775.4935572090326), \
                self.assertRaisesRegex(MODULE.Refusal, 'host sleep or clock step'):
            self.guard.require_lease()
        with patch.object(MODULE.time, 'time', return_value=wall_start + 7300), \
                patch.object(MODULE.time, 'monotonic', return_value=mono_start + 7300), self.assertRaises(MODULE.Refusal) as both:
            self.guard.require_lease()
        self.assertNotIn('host sleep', str(both.exception))

    def test_f59_tagged_image_id_cannot_bind_on_the_registry_path(self):
        # f59 final-failed-gate-proof.json, SHA-256 269d0780...cc92: probe_runtime_outcome.raw_imageID.
        repo = 'ghcr.io/ydixken/pgcopydb-operator/runner'
        digest = 'sha256:5e5b148d63f4b3327c20d22daeef0e62fc85a530418671cc0b4a88ed2960be2a'
        accepted = sorted([digest, 'sha256:28d72892ba637fd609b77ed36b975adb4603b60b7499287c981e800b05518ee8', repo + '@' + digest,
            repo + '@sha256:ec9e552b833afcf08034018d98f329486c324488c61e0c4b392edc83ad5c6440'])
        names = {node['name'] for node in self.ledger['nodes']}
        image = {'node_bindings': {name: {'accepted_image_ids': accepted} for name in names}}
        node = sorted(names)[0]
        self.assertFalse(MODULE.Guard.valid_loaded_image_id(repo + ':v0.25.0@' + digest, node, image, names))
        self.assertTrue(MODULE.Guard.valid_loaded_image_id(repo + '@' + digest, node, image, names))

    def test_b5_go_gate_failure_retains_the_failing_assertion(self):
        # b5 final-failed-Go-proof.json, SHA-256 281aaeae...0990: CalledProcessError with zero executed specs.
        self.ledger['phase'] = 'pulled'
        self.guard.save(self.ledger)
        roles = ('manager', 'legacy-runner', 'seed-tool-runner', 'otel-collector', 'postgresql-17')
        inputs = {'source_tree_sha256': 'c' * 64, 'images': {role: {'ref': 'example.invalid/' + role} for role in roles}}
        for name in ('owned_nodes',):
            setattr(self.guard, name, lambda ledger: ledger['nodes'])
        for name in ('require_lease', 'verify_node_limits'):
            setattr(self.guard, name, lambda *args: None)
        self.guard.kubeconfig = lambda ledger: None
        self.guard.suite_input = lambda ledger, selection: inputs
        self.guard.bootstrap = lambda ledger, source: None
        assertion = json.dumps({'Action': 'output', 'Output': '  [FAILED] Expected <int>: 0 to equal <int>: 1\n'}) + '\n'
        def suite(ledger, args, **kwargs):
            self.assertEqual(args[:2], ['task', 'e2e:kind:suite'])
            fd = os.open(self.directory / 'go-test.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w') as stream:
                stream.write(assertion)
            raise MODULE.CommandError('CalledProcessError', 1)
        self.guard.registry_call = suite
        with self.assertRaisesRegex(MODULE.Refusal, 'go-test.json') as refused:
            self.guard.run('pgcopydb')
        execution = self.guard.read()['suite_execution']
        self.assertEqual((execution['error_class'], execution['returncode'], execution['receipt_status'], execution['go_test_json']),
                         ('CalledProcessError', 1, 'absent', 'go-test.json'))
        self.assertEqual((self.directory / 'go-test.json').read_text(), assertion)
        self.assertNotIn('[FAILED]', json.dumps(execution) + str(refused.exception))
        with self.assertRaisesRegex(MODULE.Refusal, 'go-test.json'):
            self.guard.run('pgcopydb')

    def test_suite_task_streams_go_test_json_into_the_run(self):
        taskfile = (MODULE.ROOT / 'Taskfile.yml').read_text()
        suite = taskfile[taskfile.index('  e2e:kind:suite:'):]
        self.assertIn('go test ./test/e2e -json ', suite)
        self.assertIn('> {{.RUN_DIR | shellQuote}}/go-test.json', suite)


class SuiteRouteTests(unittest.TestCase):
    setUp, command = RegistryPullTests.setUp, RegistryPullTests.command
    NATIVE = ('16:16', '18:18', '16:18')

    def pulled(self):
        self.guard.require_prepared = lambda ledger: None
        self.guard.pull()
        return self.guard.read()

    def test_selection_admits_exactly_the_lifted_pairs_and_named_specs(self):
        self.assertEqual(MODULE.Guard.selection('pgcopydb'), MODULE.Guard.selection('pgcopydb', '17:17'))
        self.assertEqual(MODULE.Guard.selection('pgcopydb')['spec'], MODULE.LEGACY_SPEC)
        for pair in self.NATIVE:
            for case, (label, spec) in MODULE.NATIVE_SPECS.items():
                self.assertEqual(MODULE.Guard.selection('rpgmigrate', pair, case),
                                 {'engine': 'rpgmigrate', 'pair': pair, 'label': label, 'spec': spec})
        self.assertEqual({label for label, _ in MODULE.NATIVE_SPECS.values()}, {'native', 'native-fault'})
        for args in (('rpgmigrate', '17:17', 'fault-suspend'), ('rpgmigrate', '18:16', 'fault-suspend'),
                     ('rpgmigrate', '16:16', None), ('rpgmigrate', '16:16', 'fault-unknown'), ('pgcopydb', '16:16', None),
                     ('pgcopydb', '17:17', 'fresh-clone'), ('rpgmigrate', '16:16 ', 'fault-suspend')):
            with self.subTest(args=args), self.assertRaisesRegex(MODULE.Refusal, 'kind route admits'):
                MODULE.Guard.selection(*args)

    def test_projection_binds_engine_pair_spec_and_only_the_pulled_images_it_runs(self):
        ledger = self.pulled()
        common = {'manager', 'seed-tool-runner', 'otel-collector'}
        expected = {'17:17': common | {'legacy-runner', 'postgresql-17'}, '16:16': common | {'native-runner', 'postgresql-16'},
                    '18:18': common | {'native-runner', 'postgresql-18'},
                    '16:18': common | {'native-runner', 'postgresql-16', 'postgresql-18'}}
        evidence = self.guard.private_json('pulled-image-resolution-evidence.json')
        for pair, roles in expected.items():
            selection = MODULE.Guard.selection('pgcopydb') if pair == '17:17' else MODULE.Guard.selection('rpgmigrate', pair, 'fault-suspend')
            projection = self.guard.suite_projection(ledger, selection)
            with self.subTest(pair=pair):
                self.assertEqual(set(projection['images']), roles)
                self.assertEqual((projection['engine'], projection['pair'], projection['spec']),
                                 (selection['engine'], pair, selection['spec']))
                for role, image in projection['images'].items():
                    self.assertEqual(image['node_bindings'], evidence['node_bindings'][role])
                    self.assertEqual(image['loaded_digest'], self.images[role]['digest'])
                self.assertEqual({role for _, role in MODULE.Guard.suite_environment(selection)}, roles)

    def bound(self, selection, change=None):
        ledger = self.pulled()
        ledger['bootstrap'] = {'cnpg_manifest_sha256': MODULE.CNPG_SHA256, 'migration_crd_sha256': 'a' * 64,
                               'source_tree_sha256': 'd' * 64, 'admission_verified': True, 'storage_mounted': True, 'probe_cleanup': True}
        inputs = self.guard.suite_input(ledger, selection)
        manager, node = inputs['images']['manager'], ledger['nodes'][0]['name']
        receipt = {'reporter': 'ginkgo-report-after-suite', 'suite_succeeded': True, 'failures': [], 'schema_version': 2,
                   'run_id': ledger['run_id'], 'engine': selection['engine'], 'pair': selection['pair'],
                   'input_sha256': MODULE.registry.sha(self.guard.private_bytes('suite-input.json')),
                   'selected_spec': selection['spec'], 'executed_specs': 1, 'passed_specs': 1, 'skipped_specs': 0, 'cleanup': True,
                   'manager_runtime': {'namespace_uid': 'u1', 'deployment_uid': 'u2', 'replicaset_uid': 'u3', 'pod_uid': 'u4',
                       'expected_version': 'm7-' + 'd' * 12, 'observed_version': 'm7-' + 'd' * 12, 'node_name': node,
                       'image_ref': manager['ref'], 'image_config_id': manager['image_id'],
                       'image_manifest_digest': manager['loaded_digest'],
                       'container_image_id': manager['node_bindings'][node]['accepted_image_ids'][0],
                       'container_id': 'containerd://' + '9' * 64, 'authenticated': True, 'restart_count': 0,
                       'observed_at': MODULE.time.time(), 'cleanup': True}}
        if selection['engine'] == 'pgcopydb':
            receipt.update(phase='Completed', reason='DrainVerified', complete=True, complete_reason='MigrationSucceeded',
                           cutover_completed=True, nonempty_data_verified=True, sequence_verified=True)
        if change:
            change(receipt)
        self.guard.write_private('suite-receipt.json', json.dumps(receipt).encode())
        return ledger, receipt

    def test_positive_receipts_validate_against_their_own_selection(self):
        for selection in (MODULE.Guard.selection('pgcopydb'), *[MODULE.Guard.selection('rpgmigrate', pair, 'fault-suspend')
                                                                  for pair in self.NATIVE]):
            with self.subTest(selection=selection['pair']):
                self.setUp()
                ledger, receipt = self.bound(selection)
                self.assertEqual(self.guard.receipt(ledger, selection), receipt)

    def test_native_receipt_refuses_each_misreport(self):
        selection = MODULE.Guard.selection('rpgmigrate', '16:16', 'fault-suspend')
        cases = {
            'legacy claim': lambda r: r.update(phase='Completed', complete=True),
            'legacy spec': lambda r: r.update(selected_spec=MODULE.LEGACY_SPEC),
            'other native spec': lambda r: r.update(selected_spec=MODULE.NATIVE_SPECS['fault-delete'][1]),
            'legacy engine': lambda r: r.update(engine='pgcopydb'), 'other pair': lambda r: r.update(pair='16:18'),
            'schema 1': lambda r: r.update(schema_version=1), 'skipped': lambda r: r.update(skipped_specs=1),
            'two executed': lambda r: r.update(executed_specs=2, passed_specs=2), 'not passed': lambda r: r.update(passed_specs=0),
            'suite failed': lambda r: r.update(suite_succeeded=False), 'failure': lambda r: r.update(failures=[{'stage': 'x'}]),
            'not cleaned': lambda r: r.update(cleanup=False), 'missing spec': lambda r: r.pop('selected_spec'),
            'unknown field': lambda r: r.update(unexpected=True), 'other input': lambda r: r.update(input_sha256='0' * 64),
            'unauthenticated manager': lambda r: r['manager_runtime'].update(authenticated=False),
        }
        for name, change in cases.items():
            with self.subTest(case=name):
                self.setUp()
                ledger, _ = self.bound(selection, change)
                with self.assertRaises(MODULE.Refusal):
                    self.guard.receipt(ledger, selection)

    def test_a_receipt_never_validates_under_another_selection(self):
        legacy = MODULE.Guard.selection('pgcopydb')
        native = {pair: MODULE.Guard.selection('rpgmigrate', pair, 'fault-suspend') for pair in self.NATIVE}
        for written, checked in ((legacy, native['16:16']), (native['16:16'], legacy), (native['16:16'], native['16:18']),
                                 (native['18:18'], native['16:16']),
                                 (native['16:16'], MODULE.Guard.selection('rpgmigrate', '16:16', 'fault-delete'))):
            with self.subTest(written=written['pair'], checked=checked['pair']):
                self.setUp()
                ledger, _ = self.bound(written)
                with self.assertRaises(MODULE.Refusal):
                    self.guard.receipt(ledger, checked)

    def test_legacy_receipt_still_requires_its_verified_cutover_claims(self):
        legacy = MODULE.Guard.selection('pgcopydb')
        for name, change in {'not completed': lambda r: r.update(phase='Failed'), 'no sequences': lambda r: r.update(sequence_verified=False),
                             'unverified data': lambda r: r.pop('nonempty_data_verified')}.items():
            with self.subTest(case=name):
                self.setUp()
                ledger, _ = self.bound(legacy, change)
                with self.assertRaisesRegex(MODULE.Refusal, 'positive legacy'):
                    self.guard.receipt(ledger, legacy)

    def run_route(self, *args):
        ledger = self.pulled()
        guard, calls, checked = self.guard, [], []
        guard.owned_nodes = lambda ledger: ledger['nodes']
        guard.require_lease = guard.verify_node_limits = guard.kubeconfig = lambda *a: None
        guard.bootstrap = lambda ledger, source: None
        original = guard.registry_call
        guard.registry_call = lambda ledger, argv, **kwargs: ((calls.append(argv), guard.write_private('suite-receipt.json', b'{}'))
                                                              if argv[:1] == ['task'] else original(ledger, argv, **kwargs))
        guard.receipt = lambda ledger, selection: checked.append(selection)
        guard.run(*args)
        return calls, checked, guard.environment, guard.read()

    def test_native_run_binds_its_task_label_images_and_pair(self):
        for pair in self.NATIVE:
            with self.subTest(pair=pair):
                self.setUp()
                calls, checked, env, ledger = self.run_route('rpgmigrate', pair, 'fault-suspend')
                source, target = pair.split(':')
                self.assertEqual(calls, [['task', 'e2e:kind:suite', 'RUN_DIR=' + str(self.directory), 'EXPECT_CONTEXT=' + ledger['context'],
                                          'E2E_ENGINE=rpgmigrate', 'LABEL=native-fault']])
                self.assertEqual(checked, [MODULE.Guard.selection('rpgmigrate', pair, 'fault-suspend')])
                self.assertEqual((env['E2E_ENGINE'], env['E2E_PG_SOURCE'], env['E2E_PG_TARGET']), ('rpgmigrate', source, target))
                self.assertEqual(env['E2E_RUNNER_IMAGE'], self.images['native-runner']['ref'])
                images = {key for key in env if key.startswith('E2E_POSTGRES_')}
                self.assertEqual(images, {'E2E_POSTGRES_' + major + '_IMAGE' for major in (source, target)})
                for major in (source, target):
                    self.assertEqual(env['E2E_POSTGRES_' + major + '_IMAGE'], self.images['postgresql-' + major]['ref'])
                inputs = self.guard.private_json('suite-input.json')
                self.assertEqual((inputs['engine'], inputs['pair'], inputs['spec']),
                                 ('rpgmigrate', pair, MODULE.NATIVE_SPECS['fault-suspend'][1]))
                self.assertEqual((ledger['phase'], ledger['suite_execution']['selection']),
                                 ('verified', {'engine': 'rpgmigrate', 'pair': pair, 'case': 'fault-suspend'}))

    def test_run_records_the_digest_of_the_receipt_it_validated(self):
        _, _, _, ledger = self.run_route('rpgmigrate', '16:16', 'fault-suspend')
        self.assertEqual(ledger['suite_execution']['receipt_sha256'], MODULE.registry.sha(b'{}'))

    def torn_down(self, change=None, result='passed'):
        selection = MODULE.Guard.selection('rpgmigrate', '16:16', 'fault-suspend')
        ledger, receipt = self.bound(selection)
        raw = self.guard.private_bytes('suite-receipt.json')
        ledger['suite_execution'] = {'result': result, 'receipt_sha256': MODULE.registry.sha(raw),
                                     'selection': {'engine': 'rpgmigrate', 'pair': '16:16', 'case': 'fault-suspend'}}
        ledger['phase'] = 'verified' if result == 'passed' else 'pulled'
        self.guard.save(ledger)
        if change:
            change()
        # Node-dependent checks must not run once the nodes are gone.
        self.guard.receipt = self.guard.kubernetes_ownership = lambda *a: self.fail('live-node check after teardown')
        self.guard.owned_nodes = self.guard.owned_volumes = lambda *a: []
        return receipt, raw

    def test_down_writes_the_final_receipt_from_the_validated_bytes_without_live_nodes(self):
        receipt, raw = self.torn_down()
        self.guard.down()
        final = self.guard.private_json('final-receipt.json')
        self.assertEqual(final, dict(receipt, cluster_teardown=True, suite_receipt_sha256=MODULE.registry.sha(raw),
                                     teardown={'nodes_absent': True, 'volumes_absent': True}))
        self.assertEqual(self.guard.read()['phase'], 'down')

    def test_down_refuses_a_receipt_changed_after_run_validated_it(self):
        self.torn_down(lambda: self.guard.write_private('suite-receipt.json', b'{"suite_succeeded": true}'))
        with self.assertRaisesRegex(MODULE.Refusal, 'differs from the bytes run'):
            self.guard.down()
        self.assertFalse((self.directory / 'final-receipt.json').exists())
        self.assertEqual(self.guard.read()['teardown'], {'nodes_absent': True, 'volumes_absent': True})

    def test_down_refuses_a_passed_run_without_a_recorded_digest(self):
        def drop():
            ledger = self.guard.read()
            del ledger['suite_execution']['receipt_sha256']
            self.guard.save(ledger)
        self.torn_down(drop)
        with self.assertRaisesRegex(MODULE.Refusal, 'differs from the bytes run'):
            self.guard.down()
        self.assertFalse((self.directory / 'final-receipt.json').exists())

    def test_down_after_a_failed_run_tears_down_without_a_final_receipt(self):
        self.torn_down(result='failed')
        self.guard.down()
        self.assertFalse((self.directory / 'final-receipt.json').exists())
        self.assertEqual(self.guard.read()['phase'], 'down')

    def test_legacy_run_keeps_its_environment(self):
        calls, checked, env, ledger = self.run_route('pgcopydb')
        self.assertEqual(calls[0][-2:], ['E2E_ENGINE=pgcopydb', 'LABEL=kind-bootstrap'])
        self.assertEqual(checked, [MODULE.Guard.selection('pgcopydb')])
        self.assertEqual(env['E2E_RUNNER_IMAGE'], self.images['legacy-runner']['ref'])
        self.assertEqual({key for key in env if key.startswith('E2E_POSTGRES_')}, {'E2E_POSTGRES_17_IMAGE'})
        self.assertNotIn('E2E_PG_SOURCE', env)
        self.assertNotIn('E2E_PG_TARGET', env)

    def test_native_task_targets_pass_pair_case_engine_and_label(self):
        taskfile = (MODULE.ROOT / 'Taskfile.yml').read_text()
        native = taskfile[taskfile.index('  e2e:kind:native:'):taskfile.index('  e2e:kind:down:')]
        self.assertIn('--engine rpgmigrate --pair {{.PAIR | shellQuote}} --case {{.CASE | shellQuote}}', native)
        suite = taskfile[taskfile.index('  e2e:kind:suite:'):]
        self.assertIn('-ginkgo.label-filter={{.LABEL | shellQuote}}', suite)
        self.assertIn('E2E_ENGINE={{.E2E_ENGINE | shellQuote}}', suite)


LOCK_SCRIPT = Path(__file__).with_name('e2e-kind-lock.py')
LOCK_SPEC = importlib.util.spec_from_file_location('e2e_kind_lock', LOCK_SCRIPT)
LOCK = importlib.util.module_from_spec(LOCK_SPEC)
LOCK_SPEC.loader.exec_module(LOCK)


class LockGeneratorTests(unittest.TestCase):
    def test_lock_generator_leaves_no_bytecode_in_the_checkout(self):
        with tempfile.TemporaryDirectory() as directory:
            for name in ('e2e-kind-lock.py', 'e2e-kind.py', 'e2e-kind-registry.py'):
                (Path(directory) / name).write_bytes((LOCK_SCRIPT.parent / name).read_bytes())
            environment = {key: value for key, value in os.environ.items() if key not in ('PYTHONDONTWRITEBYTECODE', 'PYTHONPYCACHEPREFIX')}
            result = subprocess.run([sys.executable, str(Path(directory) / 'e2e-kind-lock.py'), '--help'],
                                    capture_output=True, env=environment, timeout=60, check=False)
            self.assertEqual(result.returncode, 0)
            self.assertFalse((Path(directory) / '__pycache__').exists())

    def setUp(self):
        self.fixture = RegistryPreparationTests('test_prepare_retains_only_complete_metadata_and_check_is_read_only')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        for name, value in (('kind', MODULE), ('registry', MODULE.registry), ('Refusal', MODULE.Refusal)):
            module_patch = patch.object(LOCK, name, value)
            module_patch.start()
            self.addCleanup(module_patch.stop)
        self.output = self.fixture.root / 'lock'
        self.guard = MODULE.Guard(self.output, environ={})
        self.guard.architecture = lambda: 'arm64'
        self.anchors = [self.fixture.receipts / ('legacy-runner-original-' + name + suffix) for name, suffix in MODULE.LEGACY_ORIGINALS]
        self.blobs = {}
        for role, graph in self.fixture.graphs.items():
            image = self.fixture.lock['images'][role]
            for digest, key in ((image['digest'], 'manifest_base64'), (image['config_digest'], 'config_base64')):
                self.blobs[(MODULE.registry.repository(image['ref']), digest)] = base64.b64decode(graph[key])
        self.layer = 200

    def fetch(self, repository, path, method='GET'):
        digest = path.rsplit('/', 1)[1]
        if method == 'HEAD':
            return self.layer, {'content-length': '37'}, b''
        body = self.blobs.get((repository, digest))
        return (200, {}, body) if body is not None else (404, {}, b'{}')

    def download(self, role):
        return (self.fixture.receipts / (role + '-publication-receipt.json')).read_bytes()

    def generate(self, **overrides):
        return LOCK.generate(self.guard, self.fixture.lock_path, overrides.get('anchors', self.anchors),
                             overrides.get('fetch', self.fetch), overrides.get('download', self.download))

    def test_regenerated_lock_matches_pins_and_passes_the_guard_receipt_inputs(self):
        lock = self.generate()
        self.assertEqual(lock, self.fixture.lock)
        written, outputs, _ = self.fixture.guard.receipt_inputs(self.output / 'image-lock.json', self.output)
        self.assertEqual(written, lock)
        manifest = json.loads((self.output / 'lock-manifest.json').read_bytes())
        self.assertEqual(manifest['image-lock.json'], MODULE.registry.sha((self.output / 'image-lock.json').read_bytes()))
        self.assertTrue(set(outputs) - {'node-default-storage.yaml'} <= set(manifest) | {'storage-registry-receipt.json'})

    def test_changed_registry_bytes_missing_blob_or_receipt_refuse_before_any_output(self):
        image = self.fixture.lock['images']['postgresql-17']
        def tampered(repository, path, method='GET'):
            status, headers, body = self.fetch(repository, path, method)
            return (status, headers, body + b' ') if path.endswith(image['digest']) else (status, headers, body)
        def foreign(role):
            receipt = json.loads(self.download(role))
            receipt['resolved_image']['digest'] = 'sha256:' + 'f' * 64
            return json.dumps(receipt).encode()
        cases = {'bytes': {'fetch': tampered}, 'receipt': {'download': foreign}, 'anchor': {'anchors': self.anchors[:1] * 3}}
        for name, overrides in cases.items():
            with self.subTest(case=name), self.assertRaises(MODULE.Refusal):
                self.generate(**overrides)
            self.assertEqual(list(self.output.iterdir()), [])
        self.layer = 404
        with self.assertRaisesRegex(MODULE.Refusal, 'not anonymously public'):
            self.generate()
        self.assertEqual(list(self.output.iterdir()), [])

    def test_task_target_runs_the_generator(self):
        taskfile = (MODULE.ROOT / 'Taskfile.yml').read_text()
        self.assertTrue('python3 hack/e2e-kind-lock.py --pins {{.PINS | shellQuote}}' in taskfile)

if __name__ == '__main__':
    unittest.main()

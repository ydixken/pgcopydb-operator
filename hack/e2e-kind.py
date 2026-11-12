#!/usr/bin/env python3
"""Guard the disposable Podman-backed M7 cluster and its private artifacts."""
import argparse
import base64
import fcntl
import hashlib
import importlib.util
import ipaddress
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import stat
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from datetime import datetime
from fractions import Fraction

sys.dont_write_bytecode = True

_registry_spec = importlib.util.spec_from_file_location('e2e_kind_registry', Path(__file__).with_name('e2e-kind-registry.py'))
registry = importlib.util.module_from_spec(_registry_spec)
_registry_spec.loader.exec_module(registry)

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'test/e2e/kind'
CLUSTER_LABEL = 'io.x-k8s.kind.cluster'
ROLE_LABEL = 'io.x-k8s.kind.role'
RUN_LABEL = 'pgcopydb-operator.io/m7-run'
MARKER = '/etc/rpgmigrate-m7-run'
CNPG_SOURCE_SHA256 = '37237f145d8138256ea25ae830f87759255665ff08f8d552fdd8224a5ec032fb'
CNPG_SHA256 = CNPG_SOURCE_SHA256
CNPG_URL = 'https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/refs/tags/v1.30.1/releases/cnpg-1.30.1.yaml'
# Generated from the pinned CNPG v1.30.1 client-rendered resource identities.
CNPG_RESOURCE_IDENTITIES = (
    ('v1', 'Namespace', '', 'cnpg-system'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'backups.postgresql.cnpg.io'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'clusterimagecatalogs.postgresql.cnpg.io'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'clusters.postgresql.cnpg.io'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'databaseroles.postgresql.cnpg.io'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'databases.postgresql.cnpg.io'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'failoverquorums.postgresql.cnpg.io'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'imagecatalogs.postgresql.cnpg.io'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'poolers.postgresql.cnpg.io'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'publications.postgresql.cnpg.io'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'scheduledbackups.postgresql.cnpg.io'),
    ('apiextensions.k8s.io/v1', 'CustomResourceDefinition', '', 'subscriptions.postgresql.cnpg.io'),
    ('v1', 'ServiceAccount', 'cnpg-system', 'cnpg-manager'),
    ('rbac.authorization.k8s.io/v1', 'ClusterRole', '', 'cnpg-database-editor-role'),
    ('rbac.authorization.k8s.io/v1', 'ClusterRole', '', 'cnpg-database-viewer-role'),
    ('rbac.authorization.k8s.io/v1', 'ClusterRole', '', 'cnpg-manager'),
    ('rbac.authorization.k8s.io/v1', 'ClusterRole', '', 'cnpg-publication-editor-role'),
    ('rbac.authorization.k8s.io/v1', 'ClusterRole', '', 'cnpg-publication-viewer-role'),
    ('rbac.authorization.k8s.io/v1', 'ClusterRole', '', 'cnpg-subscription-editor-role'),
    ('rbac.authorization.k8s.io/v1', 'ClusterRole', '', 'cnpg-subscription-viewer-role'),
    ('rbac.authorization.k8s.io/v1', 'ClusterRoleBinding', '', 'cnpg-manager-rolebinding'),
    ('v1', 'ConfigMap', 'cnpg-system', 'cnpg-default-monitoring'),
    ('v1', 'Service', 'cnpg-system', 'cnpg-webhook-service'),
    ('apps/v1', 'Deployment', 'cnpg-system', 'cnpg-controller-manager'),
    ('admissionregistration.k8s.io/v1', 'MutatingWebhookConfiguration', '', 'cnpg-mutating-webhook-configuration'),
    ('admissionregistration.k8s.io/v1', 'ValidatingWebhookConfiguration', '', 'cnpg-validating-webhook-configuration'),
)
DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')
IDENTITY = re.compile(r'[0-9a-f]{64}\Z')
# A reboot wipes /tmp, which lost earlier proofs; evidence defaults to /var/tmp instead.
VOLATILE_ROOTS = (Path('/tmp'), Path('/private/tmp'))
EVIDENCE_ROOT = Path('/var/tmp').resolve()
SLEEP_ASSERTION_SECONDS = 5
LEGACY_ORIGINALS = (('evidence', '.json'), ('qualification', '.json'), ('probe', '.stdout'))
LEGACY_SPEC = 'Kind bootstrap migrates nonempty PostgreSQL 17 fixtures with committed writer load and verified cutover'
NATIVE_PAIRS = ('16:16', '18:18', '16:18')
# One native RUN executes exactly one of these specs: case -> (Ginkgo label, full spec text).
NATIVE_SPECS = {
    'fresh-clone': ('native', 'Migration completes a fresh clone with matching rows and sequences'),
    'early-manual': ('native', 'Migration holds early Manual approval until a paused backlog catches up'),
    'live-writer': ('native', 'Migration loses no committed transaction when the source is written throughout'),
    'automatic-cutover': ('native', 'Migration runs an Automatic cutover to completion unattended'),
    'fault-copy': ('native-fault', 'Native faults resumes or fails closed when the runner is killed mid-copy'),
    'fault-stream': ('native-fault', 'Native faults resumes or fails closed when the runner is killed mid-stream'),
    'fault-drain': ('native-fault', 'Native faults completes or fails closed when the runner is killed mid-drain'),
    'fault-failover': ('native-fault', 'Native faults loses no committed row when the target primary fails over under synchronous replication'),
    'fault-suspend': ('native-fault', 'Native faults keeps its state while suspended and loses nothing after resume'),
    'fault-delete': ('native-fault', 'Native faults removes only its own source resources when deleted while streaming'),
}
NATIVE_RECEIPT_KEYS = {'reporter', 'suite_succeeded', 'failures', 'manager_runtime', 'schema_version', 'run_id', 'engine', 'pair',
                       'input_sha256', 'selected_spec', 'executed_specs', 'passed_specs', 'skipped_specs', 'cleanup'}
BASELINE_BUILDER = 'ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:86bf908adca02d87063849dae775a02c1659c829@sha256:ef596ea105604ef128244057dd34411f433d4c40fdf897bcaa8b33793df9fec8'


Refusal = registry.Refusal


class CommandError(Refusal):
    def __init__(self, failure_class, returncode=None):
        self.failure_class = failure_class
        self.returncode = returncode
        super().__init__(failure_class)


class Cancelled(CommandError):
    def __init__(self):
        super().__init__('Cancelled')


class Cancellation:
    """Children of one concurrent region; a failure or stage alarm kills them all at once."""
    def __init__(self):
        self.lock, self.cancelled, self.children = threading.Lock(), False, set()

    def cancel(self):
        with self.lock:
            self.cancelled = True
            for child in self.children:
                child.kill()


class Replay:
    """Serve reads from one batched VM call, refusing any read the batch did not plan."""
    def __init__(self, records, mismatch):
        self.records, self.mismatch = list(records), mismatch

    def __call__(self, args, timeout=120, *, private_output=False):
        if private_output or not self.records or self.records[0][0] != args:
            raise Refusal(self.mismatch)
        _, status, data = self.records.pop(0)
        if status:
            raise CommandError('CalledProcessError', status)
        try:
            return data.decode('utf-8')
        except UnicodeError:
            raise CommandError('UnicodeDecodeError') from None


_UNSCOPED = Cancellation()
_LOCAL = threading.local()
# Command substitution drops NUL bytes; none of the batched kernel files contain them.
VM_BATCH = 'for c do o=$(eval "$c" 2>/dev/null; printf .%s "$?"); printf \'%s %s\\n\' "${o##*.}" "$(printf %s "${o%.*}" | base64 -w0)"; done'


def execute(args, env=None, input=None, timeout=120, umask=0o077):
    scope = getattr(_LOCAL, 'scope', None) or _UNSCOPED
    try:
        with scope.lock:
            if scope.cancelled:
                raise Cancelled()
            child = subprocess.Popen(args, env=env, stdin=None if input is None else subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, cwd=ROOT, umask=umask)
            scope.children.add(child)
        try:
            with child:
                try:
                    stdout, _ = child.communicate(None if input is None else input.encode('utf-8'), timeout=timeout)
                except BaseException:
                    child.kill()
                    raise
        finally:
            with scope.lock:
                scope.children.discard(child)
        if scope.cancelled:
            raise Cancelled()
        if child.returncode:
            raise subprocess.CalledProcessError(child.returncode, args)
        return stdout.decode('utf-8')
    except (subprocess.SubprocessError, OSError, UnicodeError) as error:
        # Child stderr can contain registry credentials or Kubernetes Secret data.
        raise CommandError(type(error).__name__, getattr(error, 'returncode', None)) from None


def decode(raw, description):
    return registry.decode(raw, description)

def decode_manifest_stream(text: str, what: str) -> list[dict]:
    if not isinstance(text, str):
        raise Refusal(f'{what}: invalid manifest stream (framing=type, line=1, column=1, bytes=0, resources=0)')
    try:
        byte_count = len(text.encode('utf-8'))
    except UnicodeError:
        raise Refusal(f'{what}: invalid manifest stream (framing=utf8, line=1, column=1, bytes=invalid, resources=0)') from None
    resources, identities, offset = [], set(), 0

    def refuse(framing, position):
        line = text.count('\n', 0, position) + 1
        column = position - text.rfind('\n', 0, position)
        raise Refusal(f'{what}: invalid manifest stream (framing={framing}, line={line}, column={column}, '
                      f'bytes={byte_count}, resources={len(resources)})')

    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                refuse('duplicate-key', offset)
            result[key] = value
        return result

    def add(item):
        if not isinstance(item, dict) or not isinstance(item.get('metadata'), dict):
            refuse('resource', offset)
        meta = item['metadata']
        values = (item.get('apiVersion'), item.get('kind'), meta.get('name'), meta.get('namespace', ''))
        if (any(not isinstance(value, str) or any(c.isspace() or ord(c) < 32 for c in value) for value in values) or
                not all(values[:3]) or item['kind'] == 'List' or
                len(item['apiVersion'].split('/')) > 2 or not all(item['apiVersion'].split('/'))):
            refuse('resource', offset)
        identity = (item['apiVersion'].rpartition('/')[0], item['kind'], values[3], meta['name'])
        if identity in identities:
            refuse('duplicate-identity', offset)
        identities.add(identity)
        resources.append(item)

    if byte_count > 8 * 1024 ** 2:
        refuse('size', 0)
    decoder = json.JSONDecoder(object_pairs_hook=pairs, parse_constant=lambda value: refuse('non-finite', offset))
    while offset < len(text):
        while offset < len(text) and text[offset] in ' \t\r\n':
            offset += 1
        if offset == len(text):
            break
        try:
            value, end = decoder.raw_decode(text, offset)
        except json.JSONDecodeError as error:
            refuse('json', error.pos)
        except (ValueError, RecursionError):
            refuse('json', offset)
        if isinstance(value, dict) and value.get('kind') == 'List':
            if value.get('apiVersion') != 'v1' or not isinstance(value.get('items'), list) or not value['items']:
                refuse('list', offset)
            for item in value['items']:
                add(item)
        else:
            add(value)
        offset = end
    if not resources:
        refuse('empty', offset)
    return resources


def require_cnpg_inventory(resources, source_sha256) -> None:
    if source_sha256 != CNPG_SOURCE_SHA256 or not isinstance(resources, list) or len(resources) != len(CNPG_RESOURCE_IDENTITIES):
        raise Refusal('pinned CNPG source or complete resource inventory mismatch')
    identities = []
    for item in resources:
        if not isinstance(item, dict) or not isinstance(item.get('metadata'), dict):
            raise Refusal('pinned CNPG resource identity is invalid')
        meta = item['metadata']
        identity = (item.get('apiVersion'), item.get('kind'), meta.get('namespace', ''), meta.get('name'))
        if any(not isinstance(value, str) for value in identity):
            raise Refusal('pinned CNPG resource identity is invalid')
        identities.append(identity)
    if len(set(identities)) != len(identities) or set(identities) != set(CNPG_RESOURCE_IDENTITIES):
        raise Refusal('pinned CNPG source or complete resource inventory mismatch')


class Guard:
    def __init__(self, directory, environ=None):
        self.directory = Path(directory)
        self.environ = dict(os.environ if environ is None else environ)
        registry_overrides = {'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'ALL_PROXY', 'REGISTRY_AUTH_FILE', 'DOCKER_CONFIG',
            'DOCKER_AUTH_CONFIG', 'CONTAINERS_REGISTRIES_CONF', 'CONTAINERS_REGISTRIES_CONF_DIR', 'CONTAINERS_CONF',
            'CONTAINERS_CONF_OVERRIDE', 'REGISTRY_USERNAME', 'REGISTRY_PASSWORD', 'REGISTRY_TOKEN'}
        if any(key.upper() in registry_overrides for key in self.environ):
            raise Refusal('inherited registry credentials, proxy, or mirror override')
        forbidden = {'KUBECONFIG', 'E2E_CONTEXT', 'EXPECT_CONTEXT', 'KUBERNETES_MASTER',
                     'KIND_EXPERIMENTAL_PODMAN_NETWORK', 'CONTAINER_HOST', 'CONTAINER_CONNECTION',
                     'DOCKER_HOST', 'DOCKER_CONTEXT', 'TASKFILE', 'PG_SOURCE', 'PG_TARGET', 'PG_MAJORS',
                     'IMG', 'CONTAINER_TOOL', 'FOCUS', 'GOFLAGS', 'HELM_KUBECONTEXT', 'HELM_KUBEAPISERVER',
                     'HELM_KUBETOKEN', 'HELM_KUBECAFILE', 'HELM_KUBEINSECURE_SKIP_TLS_VERIFY'}
        if any(key in self.environ for key in forbidden) or any(key.startswith(('TASK_', 'E2E_', 'HELM_KUBE', 'KUBECTL_')) for key in self.environ):
            raise Refusal('inherited Kubernetes, runtime, or Task override')
        if self.environ.get('KIND_EXPERIMENTAL_PROVIDER', 'podman') != 'podman':
            raise Refusal('only the Podman provider is allowed')
        if not self.directory.is_absolute() or self.directory.resolve() != self.directory:
            raise Refusal('RUN_DIR must be absolute without symlinks or traversal')
        if any(root == self.directory or root in self.directory.parents for root in VOLATILE_ROOTS):
            raise Refusal('RUN_DIR must not be under /tmp, which a reboot wipes')
        if any((ancestor / '.git').exists() or (ancestor / '.git').is_symlink()
               for ancestor in (self.directory, *self.directory.parents)):
            raise Refusal('RUN_DIR must be outside Git checkouts and worktrees')
        if not self.directory.exists():
            self.directory.mkdir(mode=0o700)
        self.private(self.directory, directory=True)
        self.command = execute
        self.environment = dict(self.environ, KIND_EXPERIMENTAL_PROVIDER='podman',
                                KUBECONFIG=str(self.directory / 'kubeconfig'))

    def private(self, path, directory=False):
        try:
            info = path.lstat()
        except OSError:
            raise Refusal('private artifact is unavailable') from None
        expected = stat.S_ISDIR if directory else stat.S_ISREG
        if not expected(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != (0o700 if directory else 0o600):
            raise Refusal('private artifact ownership, type, or mode mismatch')
        if path.resolve() != path or (not directory and info.st_nlink != 1):
            raise Refusal('private artifact is linked')

    def write_private(self, name, content, *, exclusive=False):
        return self._write_private_at(self.directory, name, content, exclusive=exclusive)

    def _write_private_at(self, directory, name, content, *, exclusive=False):
        self.private(directory, directory=True)
        path = directory / name
        if path.parent != directory:
            raise Refusal('artifact must be directly inside RUN_DIR')
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        temporary = directory / ('.write-' + uuid.uuid4().hex)
        fd = os.open(temporary, flags, 0o600)
        primary = None
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            if exclusive:
                os.link(temporary, path, follow_symlinks=False)
                temporary.unlink()
            else:
                if path.exists() or path.is_symlink():
                    self.private(path)
                os.replace(temporary, path)
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                if temporary.exists():
                    temporary.unlink()
            except BaseException:
                if primary is None:
                    raise

    def save(self, ledger):
        self.write_private('ownership.json', (json.dumps(ledger, indent=2) + '\n').encode())

    def initialize(self):
        if (self.directory / 'ownership.json').exists():
            return self.read()
        if any(p.name != 'lifecycle.lock' for p in self.directory.iterdir()):
            raise Refusal('RUN_DIR is not empty and has no ownership ledger')
        run = uuid.uuid4().hex
        cluster = 'rpgmigrate-m7-' + run
        ledger = {'schema': 2, 'run_id': run, 'cluster': cluster, 'context': 'kind-' + cluster,
                  'kubeconfig': str(self.directory / 'kubeconfig'), 'nodes': [], 'images': [], 'phase': 'new'}
        self.save(ledger)
        return ledger

    def read(self):
        path = self.directory / 'ownership.json'
        self.private(path)
        ledger = decode(path.read_text(), 'ownership ledger')
        if not isinstance(ledger, dict) or ledger.get('schema') != 2:
            raise Refusal('unsupported ownership ledger')
        run = ledger.get('run_id', '')
        if not re.fullmatch('[0-9a-f]{32}', run) or ledger.get('cluster') != 'rpgmigrate-m7-' + run:
            raise Refusal('invalid run identity')
        if ledger.get('context') != 'kind-' + ledger['cluster'] or ledger.get('kubeconfig') != str(self.directory / 'kubeconfig'):
            raise Refusal('private context identity mismatch')
        nodes = ledger.get('nodes')
        if not isinstance(nodes, list) or len(nodes) > 3:
            raise Refusal('invalid node inventory')
        expected = {ledger['cluster'] + suffix: role for suffix, role in
                    [('-control-plane', 'control-plane'), ('-worker', 'worker'), ('-worker2', 'worker')]}
        if len({n.get('id') for n in nodes}) != len(nodes) or len({n.get('name') for n in nodes}) != len(nodes):
            raise Refusal('duplicate node identity')
        for node in nodes:
            if not IDENTITY.fullmatch(node.get('id', '')) or expected.get(node.get('name')) != node.get('role'):
                raise Refusal('invalid node identity')
        return ledger

    def call(self, args, **kwargs):
        kwargs.setdefault('umask', 0o077)
        environment = kwargs.pop('env_override', self.environment)
        return self.command(args, env=environment, **kwargs)

    def listing(self, cluster=None):
        args = ['podman', 'ps', '--all', '--no-trunc', '--format', 'json']
        if cluster:
            args += ['--filter', 'label=' + CLUSTER_LABEL + '=' + cluster]
        result = decode(self.call(args), 'provider listing')
        if not isinstance(result, list) or any(not isinstance(item, dict) or not IDENTITY.fullmatch(item.get('Id', '')) for item in result):
            raise Refusal('invalid provider inventory')
        return result

    def owned_nodes(self, ledger, marker=True):
        listed = self.listing(ledger['cluster'])
        actual = {item['Id'] for item in listed}
        expected = {node['id'] for node in ledger['nodes']}
        if not listed:
            if any(item['Id'] in expected or bool(set(item.get('Names', [])) & {n['name'] for n in ledger['nodes']}) or
                   item.get('Labels', {}).get(CLUSTER_LABEL) == ledger['cluster'] for item in self.listing()):
                raise Refusal('cluster lookup and complete provider inventory disagree')
            return []
        cleaning = ledger.get('phase') == 'cleaning' and ledger.get('cleanup_authorized', {}).get('node_ids') == sorted(expected)
        if actual != expected and not (cleaning and actual < expected):
            raise Refusal('node identities changed or were never recorded')
        remaining = [node for node in ledger['nodes'] if node['id'] in actual]
        def owned(node):
            result = decode(self.call(['podman', 'inspect', '--type', 'container', node['id']]), 'node inspection')
            if not isinstance(result, list) or len(result) != 1:
                raise Refusal('node inspection unavailable')
            item = result[0]
            labels = item.get('Config', {}).get('Labels', {})
            if (item.get('Id') != node['id'] or item.get('Name', '').lstrip('/') != node['name'] or
                    labels.get(CLUSTER_LABEL) != ledger['cluster'] or labels.get(ROLE_LABEL) != node['role']):
                raise Refusal('node ownership changed')
            if marker and self.call(['podman', 'exec', '--user', '0', node['id'], 'sh', '-c',
                                            'test "$(stat -c %u:%a ' + MARKER + ')" = 0:600 && cat ' + MARKER]).strip() != ledger['run_id']:
                raise Refusal('node run marker mismatch')
        self.each(owned, remaining)
        return remaining

    def volume_inventory(self, ledger):
        listed = decode(self.call(['podman', 'volume', 'ls', '--format', 'json']), 'volume listing')
        if not isinstance(listed, list):
            raise Refusal('invalid volume inventory')
        observed = {}
        node_names = {node['name'] for node in ledger['nodes']}
        node_names.update(ledger['cluster'] + suffix for suffix in ('-control-plane', '-worker', '-worker2'))
        for record in listed:
            name = record.get('Name')
            if not isinstance(name, str):
                raise Refusal('invalid volume listing identity')
            results = decode(self.call(['podman', 'volume', 'inspect', name]), 'volume inspection')
            item, = results
            labels = item.get('Labels', {}) or {}
            if node_names & set(labels) or name in {v['name'] for v in ledger.get('volumes', [])}:
                if not IDENTITY.fullmatch(name) or item.get('Name') != name:
                    raise Refusal('invalid owned volume name')
                observed[name] = item
        return observed

    def capture_volumes(self, ledger):
        volumes = []
        for node in ledger['nodes']:
            item, = decode(self.call(['podman', 'inspect', '--type', 'container', node['id']]), 'node mounts')
            for mount in item.get('Mounts', []):
                if mount.get('Type') != 'volume':
                    continue
                if mount.get('Destination') != '/var' or not IDENTITY.fullmatch(mount.get('Name', '')):
                    raise Refusal('unexpected node volume mount')
                volume, = decode(self.call(['podman', 'volume', 'inspect', mount['Name']]), 'owned volume')
                if (volume.get('Name') != mount['Name'] or volume.get('Driver') != 'local' or
                        not volume.get('CreatedAt') or volume.get('Labels', {}).get(node['name']) != 'true'):
                    raise Refusal('new node volume ownership is unknown')
                volumes.append({'name': volume['Name'], 'created_at': volume['CreatedAt'], 'driver': volume['Driver'],
                                'node_id': node['id'], 'node_name': node['name']})
        ledger['volumes'] = volumes
        self.save(ledger)
        self.owned_volumes(ledger, {node['id'] for node in ledger['nodes']})

    def owned_volumes(self, ledger, live_ids):
        expected = ledger.get('volumes', [])
        if not isinstance(expected, list) or len({v.get('name') for v in expected}) != len(expected):
            raise Refusal('invalid recorded volume inventory')
        by_name = {volume['name']: volume for volume in expected}
        observed = self.volume_inventory(ledger)
        cleaning = (ledger.get('phase') in ('cleaning', 'down') and
                    ledger.get('cleanup_authorized', {}).get('volume_names') == sorted(by_name))
        if set(observed) != set(by_name) and not (cleaning and set(observed) < set(by_name)):
            raise Refusal('owned volume inventory changed or was never recorded')
        node_pairs = {(node['id'], node['name']) for node in ledger['nodes']}
        for name, item in observed.items():
            volume = by_name[name]
            if (not IDENTITY.fullmatch(name) or (volume.get('node_id'), volume.get('node_name')) not in node_pairs or
                    item.get('Name') != name or item.get('Driver') != volume.get('driver') or
                    item.get('CreatedAt') != volume.get('created_at') or not volume.get('created_at') or
                    item.get('Labels', {}).get(volume['node_name']) != 'true'):
                raise Refusal('volume creation identity or ownership changed')
            users = decode(self.call(['podman', 'ps', '--all', '--no-trunc', '--filter', 'volume=' + name, '--format', 'json']), 'volume attachments')
            if not isinstance(users, list) or any(not IDENTITY.fullmatch(user.get('Id', '')) for user in users):
                raise Refusal('invalid volume attachment inventory')
            expected_users = {volume['node_id']} if volume['node_id'] in live_ids else set()
            if {user['Id'] for user in users} != expected_users:
                raise Refusal('volume has unknown or shared attachments')
        return sorted(observed)

    def kubeconfig(self, ledger, timeout=120):
        path = Path(ledger['kubeconfig'])
        self.private(path)
        if hashlib.sha256(path.read_bytes()).hexdigest() != ledger.get('kubeconfig_sha256'):
            raise Refusal('private kubeconfig changed after cluster creation')
        args = ['kubectl', '--kubeconfig', str(path), '--context', ledger['context'],
                'config', 'view', '--raw', '-o', 'json']
        config = decode(self.call(args, timeout=timeout), 'private kubeconfig')
        try:
            context, = config['contexts']
            cluster, = config['clusters']
            user, = config['users']
            address = cluster['cluster']['server']
            from urllib.parse import urlparse
            url = urlparse(address)
            safe = (config['current-context'] == context['name'] == ledger['context'] and
                    context['context']['cluster'] == cluster['name'] == ledger['context'] and
                    context['context']['user'] == user['name'] == ledger['context'] and
                    url.scheme == 'https' and ipaddress.ip_address(url.hostname).is_loopback and
                    url.port is not None and not url.username and not url.password and
                    not url.query and not url.fragment and url.path in ('', '/') and
                    set(cluster['cluster']) <= {'server', 'certificate-authority-data'} and
                    set(user['user']) == {'client-certificate-data', 'client-key-data'})
        except (KeyError, ValueError, TypeError):
            safe = False
        if not safe:
            raise Refusal('kubeconfig is not the single owned loopback context')

    def kubectl(self, args, **kwargs):
        ledger = self.read()
        deadline = kwargs.pop('deadline', None)
        self.kubeconfig(ledger, timeout=kwargs.get('timeout', 120))
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise Refusal('Kubernetes request deadline exceeded during context validation')
            kwargs['timeout'] = min(kwargs.get('timeout', 120), remaining)
        return self.call(['kubectl', '--kubeconfig', ledger['kubeconfig'], '--context', ledger['context']] + args, **kwargs)

    def architecture(self):
        architecture = {'aarch64': 'arm64', 'arm64': 'arm64', 'x86_64': 'amd64'}.get(platform.machine())
        if not architecture:
            raise Refusal('unsupported host architecture')
        return architecture


    def private_bytes(self, name):
        path = self.directory / name
        if path.parent != self.directory:
            raise Refusal('artifact must be directly inside RUN_DIR')
        self.private(path)
        if not 0 < path.stat().st_size <= 8 * 1024 ** 2:
            raise Refusal('private metadata artifact size is invalid')
        return path.read_bytes()

    def private_json(self, name):
        return decode(self.private_bytes(name), name)


    def tools(self):
        if not self.call(['kind', 'version']).startswith('kind v0.33.0 '):
            raise Refusal('kind 0.33.0 is required')
        if self.call(['podman', '--version']).strip() != 'podman version 6.2.0':
            raise Refusal('Podman 6.2.0 is required')
        for tool in [['kubectl', 'version', '--client', '-o', 'json'], ['helm', 'version', '--short'], ['task', '--version']]:
            self.call(tool)


    def preparation_capacity(self):
        self.capacity({'m7_cpus': 3, 'm7_memory_mib': 5120, 'm3_reserved_cpus': 8, 'm3_reserved_memory_mib': 12288})
        self.disk()
        if shutil.disk_usage(self.directory).free < 2 * 1024 ** 3:
            raise Refusal('host private output disk headroom is below 2 GiB')

    def vm(self, args, timeout=120, *, private_output=False):
        if private_output:
            args = ['sh', '-c', 'umask 077; exec \"$@\"', 'm7-private', *args]
        return self.call(['podman', 'machine', 'ssh', '--', shlex.join(args)], timeout=timeout)

    def vm_batch(self, commands, mismatch):
        raw = self.vm(['sh', '-c', VM_BATCH, 'm7-batch', *(shlex.join(command) for command in commands)])
        lines = raw.split('\n')
        records = []
        try:
            if len(lines) != len(commands) + 1 or lines[-1]:
                raise ValueError
            for command, line in zip(commands, lines):
                status, separator, data = line.partition(' ')
                if not separator or not re.fullmatch('[0-9]{1,3}', status) or int(status) > 255:
                    raise ValueError
                records.append((command, int(status), base64.b64decode(data, validate=True)))
        except ValueError:
            raise Refusal('batched VM read framing is invalid') from None
        return Replay(records, mismatch)

    def each(self, function, items):
        """Run independent per-node work concurrently; the first real failure cancels the rest."""
        items = list(items)
        if len(items) < 2:
            return [function(item) for item in items]
        scope = getattr(_LOCAL, 'scope', None) or Cancellation()
        def work(item):
            _LOCAL.scope = scope
            return function(item)
        pool = ThreadPoolExecutor(len(items))
        try:
            futures = [pool.submit(work, item) for item in items]
            done, _ = wait(futures, return_when=FIRST_EXCEPTION)
            if any(future.exception() for future in done):
                scope.cancel()
        except BaseException:
            scope.cancel()
            raise
        finally:
            pool.shutdown(wait=True)
        errors = [future.exception() for future in futures if future.exception() is not None]
        if errors:
            raise next((error for error in errors if not isinstance(error, Cancelled)), errors[0])
        return [future.result() for future in futures]

    def disk(self, vm=None):
        raw = (vm or self.vm)(['df', '-B1', '--output=avail', '/var'])
        lines = raw.splitlines()
        if len(lines) != 2 or not lines[1].strip().isdigit() or int(lines[1]) < 10 * 1024 ** 3:
            raise Refusal('VM disk availability is unavailable or below 10 GiB')

    @staticmethod
    def repository(pin):
        repository = pin.split('@')[0]
        return repository.rsplit(':', 1)[0] if ':' in repository.rsplit('/', 1)[-1] else repository


    @staticmethod
    def parse_runner_probe(raw: str) -> dict[str, str]:
        tools = ('pgcopydb', 'psql', 'pg_dump', 'pg_restore', 'timeout', 'sed')
        scalar_keys = {'uid', 'gid', 'workdir', 'binary_sha256', 'sed_escape', 'compare_progress_marker',
                       'pgcopydb_path', 'psql_path', 'pg_dump_path', 'pg_restore_path'}
        expected = scalar_keys | {tool + '_version_b64' for tool in tools}
        try:
            if not isinstance(raw, str) or len(raw.encode('utf-8')) > 65536 or not raw.endswith('\n') or '\r' in raw or '\0' in raw:
                raise ValueError
            records = {}
            for line in raw.split('\n')[:-1]:
                key, separator, value = line.partition('=')
                if not separator or key not in expected or key in records:
                    raise ValueError
                records[key] = value
            if set(records) != expected:
                raise ValueError
            actual = {key: records[key] for key in scalar_keys}
            full_pgcopydb = ('pgcopydb version 0.18.144.g86bf908\n'
                'compiled with PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit\n'
                'compatible with Postgres 14, 15, 16, 17, and 18')
            for tool in tools:
                encoded = records[tool + '_version_b64']
                packet = base64.b64decode(encoded.encode('ascii'), validate=True)
                if base64.b64encode(packet).decode('ascii') != encoded or packet.count(b'\0') != 1:
                    raise ValueError
                stdout, status = packet.split(b'\0')
                if status != b'0' or len(stdout) > 8192:
                    raise ValueError
                text = stdout.decode('utf-8')
                if not text or '\r' in text or '\0' in text or not text.partition('\n')[0]:
                    raise ValueError
                if tool == 'pgcopydb' and text not in (full_pgcopydb, full_pgcopydb + '\n'):
                    raise ValueError
                actual[tool + '_version'] = text.partition('\n')[0]
            return actual
        except (ValueError, UnicodeError):
            raise Refusal('runner probe framing, command status, or full version output is invalid') from None


    def require_lease(self):
        ledger = self.read()
        lease = ledger.get('resource_lease', {})
        keys = ('m7_cpus', 'm7_memory_mib', 'm3_reserved_cpus', 'm3_reserved_memory_mib')
        if not isinstance(lease, dict) or set(lease) != {*keys, 'authorized', 'expires_at', 'monotonic_expires_at'}:
            raise Refusal('live resource lease fields do not match the fixed profile')
        expiry, monotonic_expiry = lease.get('expires_at'), lease.get('monotonic_expires_at')
        if (all(type(value) in (int, float) and math.isfinite(value) for value in (expiry, monotonic_expiry)) and
                expiry <= time.time() and monotonic_expiry > time.monotonic()):
            # macOS pauses the monotonic clock during sleep, so only the wall lease runs out.
            raise Refusal('wall lease expired while monotonic time remains: host sleep or clock step')
        if (lease.get('authorized') is not True or type(expiry) not in (int, float) or not math.isfinite(expiry) or expiry <= time.time() or type(lease.get('monotonic_expires_at')) not in (int, float) or
                not math.isfinite(lease['monotonic_expires_at']) or lease['monotonic_expires_at'] <= time.monotonic() or
                any(type(lease.get(key)) is not int for key in keys) or
                lease['m7_cpus'] != 6 or lease['m7_memory_mib'] != 12288 or
                lease['m3_reserved_cpus'] != 6 or lease['m3_reserved_memory_mib'] != 6144):
            raise Refusal('leader-authorized live resource lease is missing, invalid, or expired')
        reads = self.vm_batch([['cat', '/proc/meminfo'], ['df', '-B1', '--output=avail', '/var']], 'VM capacity reads changed')
        self.capacity(dict(lease, m3_reserved_cpus=8, m3_reserved_memory_mib=12288), reads)
        self.disk(reads)

    def hold_awake(self):
        # shortcut: macOS only; the Linux-native-host transport (M7-HOSTED) needs its own sleep policy.
        if platform.system() != 'Darwin':
            return None
        if "'AC Power'" not in self.call(['pmset', '-g', 'batt']).split('\n', 1)[0]:
            raise Refusal('host sleep guard requires AC power, because caffeinate -s is ignored on battery')
        child = subprocess.Popen(['caffeinate', '-dims', '-w', str(os.getpid())], stdin=subprocess.DEVNULL,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        pattern = re.compile(r'^\s*pid ' + str(child.pid) + r'\(caffeinate\): .* (PreventSystemSleep|PreventUserIdleSystemSleep) named:', re.M)
        deadline = time.monotonic() + SLEEP_ASSERTION_SECONDS
        while child.poll() is None and time.monotonic() < deadline:
            if {match[1] for match in pattern.finditer(self.call(['pmset', '-g', 'assertions']))} == {'PreventSystemSleep', 'PreventUserIdleSystemSleep'}:
                return child
            time.sleep(0.1)
        child.kill()
        raise Refusal('host sleep assertion is not active')

    def capacity(self, lease, vm=None):
        info = decode(self.call(['podman', 'info', '--format', 'json']), 'Podman capacity')
        host = info.get('host', {})
        if any(type(host.get(key)) is not int or host[key] <= 0 for key in ('cpus', 'memTotal')):
            raise Refusal('VM usable capacity is unavailable')
        memory = {}
        raw = vm(['cat', '/proc/meminfo']) if vm else self.call(['podman', 'machine', 'ssh', '--', 'cat', '/proc/meminfo'])
        for line in raw.splitlines():
            if not line.startswith(('MemTotal:', 'MemAvailable:')):
                continue
            match = re.fullmatch(r'(MemTotal|MemAvailable):\s+([0-9]+)\s+kB', line)
            if not match or match[1] in memory:
                raise Refusal('VM memory availability is malformed or duplicated')
            memory[match[1]] = int(match[2]) * 1024
        if (set(memory) != {'MemTotal', 'MemAvailable'} or memory['MemTotal'] <= 0 or
                memory['MemAvailable'] > memory['MemTotal']):
            raise Refusal('VM memory availability is unavailable or invalid')
        requested_cpus = lease['m7_cpus'] + lease['m3_reserved_cpus']
        requested_memory = (lease['m7_memory_mib'] + lease['m3_reserved_memory_mib']) * 1024 ** 2
        if (requested_cpus > host['cpus'] or requested_memory > min(host['memTotal'], memory['MemTotal']) or
                requested_memory > memory['MemAvailable']):
            raise Refusal('VM capacity cannot preserve the authorized M3 reservation and M7 budget')

    def node_inventory(self, ledger):
        current = self.read()
        if any(current.get(key) != ledger.get(key) for key in ('run_id', 'cluster', 'nodes', 'node_limits')):
            raise Refusal('node ownership ledger changed')
        pin_fields = ('ref', 'digest', 'config_digest', 'architecture')
        current_node = [tuple(image.get(key) for key in pin_fields) for image in current.get('images', []) if image.get('role') == 'node']
        expected_node = [tuple(image.get(key) for key in pin_fields) for image in ledger.get('images', []) if image.get('role') == 'node']
        if len(current_node) != 1 or current_node != expected_node:
            raise Refusal('owned node image binding changed')
        nodes = self.owned_nodes(ledger)
        if len(nodes) != 3 or [node['role'] for node in nodes].count('control-plane') != 1 or [node['role'] for node in nodes].count('worker') != 2:
            raise Refusal('exactly one owned control-plane and two workers are required')
        expected = {node['id']: node for node in nodes}
        names = {node['name'] for node in nodes}
        related = []
        for item in self.listing():
            labels = item.get('Labels') or {}
            observed_names = item.get('Names')
            if (not isinstance(labels, dict) or not isinstance(observed_names, list) or not observed_names or
                    any(not isinstance(name, str) for name in observed_names)):
                raise Refusal('complete provider identity inventory is malformed')
            if item['Id'] in expected or names & set(observed_names) or labels.get(CLUSTER_LABEL) == ledger['cluster']:
                node = expected.get(item['Id'])
                if (node is None or observed_names != [node['name']] or labels.get(CLUSTER_LABEL) != ledger['cluster'] or
                        labels.get(ROLE_LABEL) != node['role']):
                    raise Refusal('filtered and complete owned-node inventories disagree')
                related.append(item['Id'])
        if len(related) != 3 or set(related) != set(expected):
            raise Refusal('complete provider inventory does not prove all three owned nodes')
        return nodes

    def node_state(self, ledger, node):
        inspected = decode(self.call(['podman', 'inspect', '--type', 'container', node['id']]), 'node limit inspection')
        if not isinstance(inspected, list) or len(inspected) != 1 or not isinstance(inspected[0], dict):
            raise Refusal('exact node provider inspection is missing or malformed')
        item = inspected[0]
        config = item.get('Config')
        labels = config.get('Labels') if isinstance(config, dict) else None
        if not isinstance(labels, dict):
            raise Refusal('node provider ownership labels are missing or malformed')
        image = item.get('Image', '')
        image = 'sha256:' + image.removeprefix('sha256:') if isinstance(image, str) else ''
        node_images = [entry for entry in ledger['images'] if entry.get('role') == 'node']
        state, host = item.get('State', {}), item.get('HostConfig', {})
        if (len(node_images) != 1 or not DIGEST.fullmatch(image) or image != node_images[0].get('config_digest') or
                item.get('Id') != node['id'] or item.get('Name', '').lstrip('/') != node['name'] or
                labels.get(CLUSTER_LABEL) != ledger['cluster'] or labels.get(ROLE_LABEL) != node['role'] or
                not isinstance(state, dict) or state.get('Running') is not True or type(state.get('Pid')) is not int or state['Pid'] <= 0 or
                not isinstance(host, dict) or any(type(host.get(key)) is not int for key in ('CpuQuota', 'CpuPeriod', 'NanoCpus', 'Memory', 'MemorySwap'))):
            raise Refusal('node image, process, ownership, or provider limit fields are invalid')
        if (host['CpuQuota'] < -1 or host['CpuPeriod'] < 0 or host['NanoCpus'] < 0 or host['Memory'] < 0 or host['MemorySwap'] < -1):
            raise Refusal('provider limit fields are malformed')
        started = state.get('StartedAt')
        try:
            if not isinstance(started, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})', started):
                raise ValueError
            parsed = datetime.fromisoformat(started.replace('Z', '+00:00'))
            if not parsed.tzinfo or parsed.timestamp() <= 0:
                raise ValueError
        except (AttributeError, TypeError, ValueError, OverflowError):
            raise Refusal('node process start identity is unavailable') from None
        identity = {'id': node['id'], 'role': node['role'], 'image_id': image, 'pid': state['Pid'], 'started_at': started}
        return identity, {key: host[key] for key in ('CpuQuota', 'CpuPeriod', 'NanoCpus', 'Memory', 'MemorySwap')}

    def process_identity(self, identity, vm=None):
        vm = vm or self.vm
        pid, cid = identity['pid'], identity['id']
        raw = vm(['cat', '/proc/' + str(pid) + '/cgroup'])
        lines = raw.splitlines()
        if len(lines) != 1 or not lines[0].startswith('0::'):
            raise Refusal('node init process has no unambiguous cgroup-v2 binding')
        path = lines[0][3:]
        scope = 'libpod-' + cid + '.scope'
        shape = r'/user\.slice/user-([0-9]+)\.slice/user@\1\.service/user\.slice/' + re.escape(scope) + r'/container(?:/[A-Za-z0-9_.@:-]+)*'
        if (not re.fullmatch(shape, path) or len(Path(path).parts) > 32 or
                any(part in ('.', '..') or len(part) > 128 for part in path.split('/')[1:]) or
                path.split('/').count(scope) != 1 or any(part.startswith('libpod-') and part != scope for part in path.split('/'))):
            raise Refusal('node init cgroup does not match the supported exact-CID ownership shape')
        full = '/sys/fs/cgroup' + path
        if vm(['realpath', '-e', '--', full]).splitlines() != [full]:
            raise Refusal('node init cgroup path is linked or unavailable')
        raw_stat = vm(['cat', '/proc/' + str(pid) + '/stat'])
        head, separator, tail = raw_stat.rpartition(') ')
        fields = tail.split()
        if (not separator or not head.startswith(str(pid) + ' (') or len(fields) < 20 or
                not re.fullmatch('[RSDTtWKPI]', fields[0]) or not re.fullmatch('[0-9]+', fields[19]) or int(fields[19]) <= 0):
            raise Refusal('node init process start ticks are malformed')
        return dict(identity, cgroup_path=path, start_ticks=int(fields[19]))

    @staticmethod
    def cgroup_value(raw, *, zero=False):
        if raw == 'max':
            return None
        if not re.fullmatch('[0-9]+', raw) or not (0 if zero else 1) <= int(raw) <= 2 ** 63 - 1:
            raise Refusal('cgroup limit is malformed')
        return int(raw)

    def cgroup_root(self, vm=None):
        vm = vm or self.vm
        root = '/sys/fs/cgroup'
        if (vm(['stat', '-fc', '%T', root]).strip() != 'cgroup2fs' or
                vm(['realpath', '-e', '--', root]).splitlines() != [root]):
            raise Refusal('canonical cgroup-v2 hierarchy root is unavailable')
        matches = []
        for line in vm(['cat', '/proc/self/mountinfo']).splitlines():
            before, separator, after = line.partition(' - ')
            fields, filesystem = before.split(), after.split()
            if len(fields) < 5 or fields[4] != root:
                continue
            if (not separator or len(fields) < 6 or len(filesystem) < 3 or fields[3] != '/' or filesystem[0] != 'cgroup2' or
                    any(not re.fullmatch('[0-9]+', fields[index]) or int(fields[index]) <= 0 for index in (0, 1)) or
                    not re.fullmatch('[0-9]+:[0-9]+', fields[2])):
                raise Refusal('cgroup-v2 hierarchy root mount identity is malformed or delegated')
            matches.append({'mount_id': int(fields[0]), 'parent_id': int(fields[1]), 'device': fields[2],
                            'root': fields[3], 'mountpoint': fields[4], 'filesystem': filesystem[0]})
        if len(matches) != 1:
            raise Refusal('cgroup-v2 hierarchy root mount is missing or ambiguous')
        return matches[0]

    @staticmethod
    def cgroup_paths(cgroup_path):
        relative = Path(cgroup_path)
        return ['/sys/fs/cgroup' + (str(path) if str(path) != '/' else '') for path in reversed([relative, *relative.parents])]

    def node_reads(self, pid, expected):
        """Plan every read of one observation from the recorded cgroup path and fetch them in one VM call."""
        changed = 'node image, PID, start time, or cgroup identity changed'
        path = expected.get('cgroup_path') if isinstance(expected, dict) else None
        if not isinstance(path, str) or not path.startswith('/'):
            raise Refusal(changed)
        paths = self.cgroup_paths(path)
        identity = [['cat', '/proc/' + str(pid) + '/cgroup'], ['realpath', '-e', '--', '/sys/fs/cgroup' + path],
                    ['cat', '/proc/' + str(pid) + '/stat']]
        root = [['stat', '-fc', '%T', '/sys/fs/cgroup'], ['realpath', '-e', '--', '/sys/fs/cgroup'], ['cat', '/proc/self/mountinfo']]
        limits = [['realpath', '-e', '--', *paths],
                  ['cat', *(path + '/' + key for path in paths[1:] for key in ('cpu.max', 'memory.max', 'memory.swap.max'))]]
        return self.vm_batch(identity + root + limits + identity + root, changed)

    def node_limit_observation(self, ledger, node, expected):
        identity, host = self.node_state(ledger, node)
        reads = self.node_reads(identity['pid'], expected)
        identity = self.process_identity(identity, reads)
        if identity != expected:
            raise Refusal('node image, PID, start time, or cgroup identity changed')
        if (host['CpuQuota'] <= 0 or host['CpuPeriod'] <= 0 or Fraction(host['CpuQuota'], host['CpuPeriod']) != 2 or
                host['NanoCpus'] != 2000000000 or host['Memory'] != 4294967296 or host['MemorySwap'] != 4294967296):
            raise Refusal('node provider limits do not match two CPUs, four GiB, and zero additional swap')
        root = self.cgroup_root(reads)
        if root != ledger['node_limits'].get('cgroup_root'):
            raise Refusal('cgroup-v2 hierarchy root mount changed')
        paths = self.cgroup_paths(identity['cgroup_path'])
        canonical = reads(['realpath', '-e', '--', *paths]).splitlines()
        if canonical != paths:
            raise Refusal('cgroup limit path is unavailable, linked, or noncanonical')
        # The kernel exposes controller limit files on non-root cgroups only.
        paths = paths[1:]
        files = [path + '/' + key for path in paths for key in ('cpu.max', 'memory.max', 'memory.swap.max')]
        values = reads(['cat', *files]).splitlines()
        if len(values) != len(files) or any(len(value) > 128 for value in values):
            raise Refusal('cgroup limit inventory is incomplete or malformed')
        cpus, memories, swaps, observations = [], [], [], []
        scope_path = '/sys/fs/cgroup' + identity['cgroup_path'].split('/' + 'libpod-' + node['id'] + '.scope')[0] + '/libpod-' + node['id'] + '.scope'
        for index, path in enumerate(paths):
            cpu, memory, swap = values[index * 3:index * 3 + 3]
            parts = cpu.split()
            if len(parts) != 2:
                raise Refusal('cgroup CPU quota is malformed')
            quota = self.cgroup_value(parts[0])
            period = self.cgroup_value(parts[1])
            if period is None:
                raise Refusal('cgroup CPU period is not finite')
            ratio = Fraction(quota, period) if quota is not None else None
            memory_value, swap_value = self.cgroup_value(memory), self.cgroup_value(swap, zero=True)
            if path == scope_path and (ratio != 2 or memory_value != 4294967296):
                raise Refusal('exact owned cgroup scope has no matching finite CPU and memory cap')
            if ratio is not None: cpus.append(ratio)
            if memory_value is not None: memories.append(memory_value)
            if swap_value is not None: swaps.append(swap_value)
            observations.append({'path': path, 'cpu.max': cpu, 'memory.max': memory, 'memory.swap.max': swap})
        if (not cpus or not memories or not swaps or min(cpus) != 2 or min(memories) != 4294967296 or min(swaps) != 0):
            raise Refusal('effective node CPU, memory, or swap limit differs from the fixed allocation')
        # The batch read the after-identity last, so this inspection still brackets every cgroup read.
        # A new PID asks for reads the batch never planned, which the replay refuses with the same message.
        after, after_host = self.node_state(ledger, node)
        reads.mismatch = 'node process or provider limits changed while reading cgroups'
        if self.process_identity(after, reads) != expected or after_host != host or self.cgroup_root(reads) != root:
            raise Refusal('node process or provider limits changed while reading cgroups')
        return {'cpu': '2', 'memory': 4294967296, 'swap': 0, 'host_config': host, 'cgroups': observations}

    def live_node_limits(self, ledger):
        nodes = self.node_inventory(ledger)
        proofs = ledger.get('node_limits', {}).get('nodes', [])
        if (not isinstance(proofs, list) or len(proofs) != 3 or any(not isinstance(proof, dict) for proof in proofs) or
                {proof.get('id') for proof in proofs} != {node['id'] for node in nodes}):
            raise Refusal('three matching node-cap proofs are required')
        expected = {}
        for node in nodes:
            proof = next(proof for proof in proofs if proof['id'] == node['id'])
            if proof.get('role') != node['role'] or proof.get('update') != 'verified':
                raise Refusal('node cap application is incomplete')
            expected[node['id']] = proof.get('identity')
        observed = self.each(lambda node: self.node_limit_observation(ledger, node, expected[node['id']]), nodes)
        if sum(Fraction(value['cpu']) for value in observed) != 6 or sum(value['memory'] for value in observed) != 12884901888:
            raise Refusal('aggregate owned node limits differ from six CPUs and twelve GiB')
        return observed

    def enforce_node_limits(self, ledger):
        if ledger.get('phase') != 'creating' or 'node_limits' in ledger:
            raise Refusal('node caps can be applied only once to newly created owned nodes')
        limits = {'status': 'applying', 'nodes': []}
        try:
            self.require_lease()
            nodes = self.node_inventory(ledger)
            limits['cgroup_root'] = self.cgroup_root()
            identities = {node['id']: self.process_identity(self.node_state(ledger, node)[0]) for node in nodes}
            ledger['node_limits'] = limits
            self.save(ledger)
            for node in nodes:
                self.require_lease()
                self.node_inventory(ledger)
                identity = self.process_identity(self.node_state(ledger, node)[0])
                if identity != identities[node['id']]:
                    raise Refusal('node process changed before exact-CID update')
                proof = {'id': node['id'], 'role': node['role'], 'identity': identity, 'update': 'attempting'}
                limits['nodes'].append(proof)
                self.save(ledger)
                returned = self.call(['podman', 'update', '--cpus=2', '--memory=4096m', '--memory-swap=4096m', node['id']])
                if returned.strip() != node['id']:
                    raise Refusal('node update returned an unexpected identity')
                proof['update'] = 'returned'
                self.save(ledger)
                proof['limits'] = self.node_limit_observation(ledger, node, identity)
                proof['update'] = 'verified'
                self.save(ledger)
            observed = self.live_node_limits(ledger)
            for proof, value in zip(limits['nodes'], observed):
                proof['limits'] = value
            limits['status'], limits['verified_at'] = 'verified', time.time()
            self.save(ledger)
        except BaseException as error:
            limits.update(status='failed', error_class=getattr(error, 'failure_class', type(error).__name__))
            ledger['node_limits'] = limits
            try:
                self.save(ledger)
            except BaseException:
                pass
            raise

    def verify_node_limits(self, ledger):
        self.require_lease()
        if not isinstance(ledger.get('node_limits'), dict) or ledger['node_limits'].get('status') != 'verified':
            raise Refusal('verified owned-node caps are required before runtime effects')
        return self.live_node_limits(ledger)

    def up(self, lease_seconds: int):
        if type(lease_seconds) is not int or not 30 <= lease_seconds <= 7200:
            raise Refusal('up requires explicit lease seconds from 30 through 7200')
        ledger = self.check()
        if ledger['phase'] != 'prepared':
            raise Refusal('cluster creation is allowed only for a fresh prepared owned run')
        if 'resource_lease' in ledger:
            self.require_lease()
            raise Refusal('live authorization is already recorded; implicit lease renewal is refused')
        filtered = self.listing(ledger['cluster'])
        complete = self.listing()
        names = {ledger['cluster'] + suffix for suffix in ('-control-plane', '-worker', '-worker2')}
        if any(not isinstance(item.get('Names'), list) or not item['Names'] or
               any(not isinstance(name, str) for name in item['Names']) or not isinstance(item.get('Labels', {}), dict) for item in complete):
            raise Refusal('complete provider identity inventory is unavailable')
        if filtered or any(names & set(item['Names']) or item.get('Labels', {}).get(CLUSTER_LABEL) == ledger['cluster'] for item in complete):
            raise Refusal('same-name cluster already exists or provider inventories disagree')
        lease = {'authorized': True, 'm7_cpus': 6, 'm7_memory_mib': 12288,
                 'm3_reserved_cpus': 6, 'm3_reserved_memory_mib': 6144,
                 'expires_at': time.time() + lease_seconds, 'monotonic_expires_at': time.monotonic() + lease_seconds}
        # Steady kind and PostgreSQL retain the VM reserve; image preparation uses a separate window.
        self.capacity(dict(lease, m3_reserved_cpus=8, m3_reserved_memory_mib=12288))
        self.disk()
        ledger['resource_lease'] = lease
        self.save(ledger)
        self.require_lease()
        node_image = next(image for image in ledger['images'] if image['role'] == 'node')
        self.shared_storage()
        config = (CONFIG / 'cluster.yaml').read_text().replace('RUN_ID', ledger['run_id']).replace(
            'STORAGE_HOST_PATH', json.dumps(str(self.directory / 'node-default-storage.yaml')))
        self.write_private('cluster.yaml', config.encode())
        self.write_private('kubeconfig', b'')
        ledger['phase'] = 'creating'
        self.save(ledger)
        try:
            self.registry_call(ledger, ['kind', 'create', 'cluster', '--name', ledger['cluster'], '--kubeconfig', ledger['kubeconfig'],
                       '--config', str(self.directory / 'cluster.yaml'), '--image', registry.repository(node_image['ref']) + '@' + (node_image['index_digest'] or node_image['digest']), '--wait', '180s', '--retain'], timeout=300)
        finally:
            # Retain exact partial creation evidence even when kind never became ready.
            nodes = self.listing(ledger['cluster'])
            ledger['nodes'] = [{'id': item['Id'], 'name': item['Names'][0],
                                'role': item.get('Labels', {}).get(ROLE_LABEL)} for item in nodes]
            self.save(ledger)
            self.read()
            self.owned_nodes(ledger, marker=False)
            self.capture_volumes(ledger)
            for node in ledger['nodes']:
                self.call(['podman', 'exec', '-i', '--user', '0', node['id'], 'sh', '-c',
                           'umask 077; set -C; cat > ' + MARKER], input=ledger['run_id'] + '\n')
        if len(ledger['nodes']) != 3:
            raise Refusal('cluster creation did not produce all expected nodes')
        self.owned_nodes(ledger)
        self.private(Path(ledger['kubeconfig']))
        ledger['kubeconfig_sha256'] = hashlib.sha256(Path(ledger['kubeconfig']).read_bytes()).hexdigest()
        self.save(ledger)
        self.enforce_node_limits(ledger)
        self.kubeconfig(ledger)
        self.verify_node_limits(ledger)
        self.kubectl(['label', 'nodes', '--all', RUN_LABEL + '=' + ledger['run_id'], '--overwrite=false'])
        self.verify_storage_mount(ledger)
        self.require_lease()
        ledger['phase'] = 'ready'
        self.save(ledger)

    def download_manifest(self):
        path = self.directory / 'cnpg-source.yaml'
        if path.exists():
            self.private(path)
            return path.read_bytes()
        from urllib.request import urlopen
        try:
            with urlopen(CNPG_URL, timeout=30) as response:
                if response.geturl() != CNPG_URL:
                    raise Refusal('CNPG manifest redirect refused')
                content = response.read(4 * 1024 * 1024 + 1)
                if len(content) > 4 * 1024 * 1024:
                    raise Refusal('CNPG manifest exceeds the bounded input size')
                return content
        except (OSError, ValueError) as error:
            raise Refusal('CNPG manifest unavailable: ' + type(error).__name__) from None

    def bootstrap(self, ledger, source_hash, timeout=300):
        self.verify_node_limits(ledger)
        content = self.download_manifest()
        cnpg_source_sha256 = hashlib.sha256(content).hexdigest()
        if cnpg_source_sha256 != CNPG_SHA256:
            raise Refusal('CNPG manifest digest mismatch')
        if self.source_tree() != source_hash:
            raise Refusal('assigned source tree changed before bootstrap')
        crd_path = ROOT / 'config/crd/bases/pgcopydb-operator.io_migrations.yaml'
        if not crd_path.is_file() or crd_path.is_symlink():
            raise Refusal('assigned Migration CRD is missing or linked')
        crd_bytes = crd_path.read_bytes()
        proof = self.pull_evidence(ledger, registry.PULL_ROLES, self.directory, 'pulled-image-resolution-evidence.json')
        images = {image['role']: dict(image) for image in ledger['images']}
        for role in registry.PULL_ROLES:
            images[role]['node_bindings'] = proof['node_bindings'][role]
        postgres_image_name = self.cnpg_image_name(images.get('postgresql-17'))
        bundle = self.private_json('storage-registry-receipt.json')
        registry.closed(bundle, {'schema_version', 'node_manifest_digest', 'source_sha256', 'rendered_sha256', 'images'}, 'storage registry receipt')
        if (bundle['schema_version'] != 2 or bundle['node_manifest_digest'] != images['node']['digest'] or
                bundle['source_sha256'] != registry.sha(self.private_bytes('node-default-storage-source.yaml')) or
                bundle['rendered_sha256'] != registry.sha(self.private_bytes('node-default-storage.yaml')) or
                bundle['images'] != {r: self.private_json('image-lock.json')['images'][r] for r in ('storage-helper', 'storage-provisioner')}):
            raise Refusal('pinned public storage metadata changed')
        bundled = {role: images[role] for role in ('storage-helper', 'storage-provisioner')}
        self.write_private('cnpg-source.yaml', content)
        self.write_private('migration-crd-source.yaml', crd_bytes)
        deadline = time.monotonic() + timeout

        def request(args, **kwargs):
            if args[0] in ('apply', 'create') and '--dry-run=client' not in args:
                self.verify_node_limits(ledger)
            remaining = min(deadline - time.monotonic(), ledger['resource_lease']['monotonic_expires_at'] - time.monotonic(),
                ledger['resource_lease']['expires_at'] - time.time())
            if remaining <= 0:
                raise Refusal('bootstrap readiness or original parent deadline exceeded')
            return self.kubectl(args, timeout=min(10, remaining), deadline=deadline, **kwargs)

        def wait(args, ready, description):
            while time.monotonic() < deadline:
                raw = request(['get'] + args + ['-o', 'json'])
                value = decode(raw, description) if raw.strip() else None
                if ready(value):
                    return value
                time.sleep(min(1, max(0, deadline - time.monotonic())))
            raise Refusal(description + ': readiness deadline exceeded')

        def parse(raw, description):
            rendered = request(['create', '--dry-run=client', '--validate=false', '-f', '-', '-o', 'json'], input=raw)
            return decode_manifest_stream(rendered, description)

        def apply(objects):
            for item in objects:
                item.setdefault('metadata', {}).setdefault('labels', {})[RUN_LABEL] = ledger['run_id']
            body = json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': objects})
            raw = request(['get', '-f', '-', '--ignore-not-found', '-o', 'json'], input=body)
            current = decode(raw, 'existing bootstrap objects') if raw.strip() else {'kind': 'List', 'items': []}
            existing = current.get('items') if current.get('kind') == 'List' else [current]
            if not isinstance(existing, list):
                raise Refusal('existing bootstrap ownership inventory is invalid')
            expected = {(item['apiVersion'], item['kind'], item['metadata'].get('namespace', ''), item['metadata']['name']) for item in objects}
            for item in existing:
                meta = item.get('metadata', {})
                identity = (item.get('apiVersion'), item.get('kind'), meta.get('namespace', ''), meta.get('name'))
                if identity not in expected or not meta.get('uid') or meta.get('labels', {}).get(RUN_LABEL) != ledger['run_id']:
                    raise Refusal('bootstrap refuses an existing foreign object')
            request(['apply', '--server-side', '--field-manager=rpgmigrate-m7', '-f', '-'], input=body)
            return body

        def established(item, name, version):
            return (isinstance(item, dict) and item.get('metadata', {}).get('name') == name and
                    any(c.get('type') == 'Established' and c.get('status') == 'True' for c in item.get('status', {}).get('conditions', [])) and
                    any(v.get('name') == version and v.get('served') is True for v in item.get('spec', {}).get('versions', [])))

        def deployment_ready(item):
            if not isinstance(item, dict) or not item.get('metadata', {}).get('uid'):
                return False
            desired = item.get('spec', {}).get('replicas', 1)
            state = item.get('status', {})
            return (type(desired) is int and desired > 0 and state.get('observedGeneration', 0) >= item.get('metadata', {}).get('generation', 1) and
                    all(state.get(key, 0) >= desired for key in ('readyReplicas', 'updatedReplicas', 'availableReplicas')))

        objects = parse(content.decode('utf-8'), 'CNPG manifest rendering')
        require_cnpg_inventory(objects, cnpg_source_sha256)
        cnpg_crds = [item['metadata']['name'] for item in objects if item.get('kind') == 'CustomResourceDefinition']
        if 'clusters.postgresql.cnpg.io' not in cnpg_crds:
            raise Refusal('pinned CNPG manifest has no Cluster CRD')
        namespaces = [item for item in objects if item.get('kind') == 'Namespace']
        deployments = [item for item in objects if item.get('kind') == 'Deployment']
        services = [item for item in objects if item.get('kind') == 'Service']
        webhooks = [item for item in objects if item.get('kind') in ('MutatingWebhookConfiguration', 'ValidatingWebhookConfiguration')]
        if len(namespaces) != 1 or len(deployments) != 1 or len(services) != 1 or len(webhooks) != 2:
            raise Refusal('pinned CNPG manifest shape is unsupported')
        namespace = namespaces[0]['metadata']['name']
        deployment = deployments[0]
        service = services[0]
        containers = deployment['spec']['template']['spec']['containers']
        if namespace != 'cnpg-system' or len(containers) != 1 or containers[0].get('name') != 'manager':
            raise Refusal('pinned CNPG controller identity is unsupported')
        containers[0].update(image=images['cnpg-controller']['ref'], imagePullPolicy='IfNotPresent')
        for entry in containers[0].get('env', []):
            if entry.get('name') == 'OPERATOR_IMAGE_NAME':
                entry['value'] = images['cnpg-controller']['ref']
        deployment['spec']['template']['metadata'].setdefault('labels', {})[RUN_LABEL] = ledger['run_id']
        service['spec']['selector'][RUN_LABEL] = ledger['run_id']
        self.write_private('cnpg-applied.json', apply(objects).encode())
        for name in cnpg_crds:
            wait(['crd', name], lambda value, name=name: established(value, name, 'v1'), 'CNPG CRD')
        wait(['deployment', deployment['metadata']['name'], '-n', namespace], deployment_ready, 'CNPG deployment')
        controller_pods = wait(['pods', '-n', namespace, '-l', RUN_LABEL + '=' + ledger['run_id']],
                               lambda value: bool(value and value.get('items')) and all(
                                   pod.get('metadata', {}).get('uid') and
                                   any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in pod.get('status', {}).get('conditions', [])) and
                                   any(c.get('ready') is True and self.valid_loaded_image_id(c.get('imageID'), pod.get('spec', {}).get('nodeName'),
                                       images['cnpg-controller'], {node['name'] for node in ledger['nodes']})
                                       for c in pod.get('status', {}).get('containerStatuses', [])) for pod in value['items']), 'CNPG pods')
        pod_uids = {pod['metadata']['uid'] for pod in controller_pods['items']}
        wait(['endpointslices', '-n', namespace, '-l', 'kubernetes.io/service-name=' + service['metadata']['name']],
             lambda value: bool(value) and any(endpoint.get('conditions', {}).get('ready') is True and endpoint.get('addresses') and
                 endpoint.get('targetRef', {}).get('uid') in pod_uids for item in value.get('items', []) for endpoint in item.get('endpoints', [])),
             'CNPG webhook endpoints')
        for hook in webhooks:
            def hooked(value):
                entries = value.get('webhooks', []) if isinstance(value, dict) else []
                if not entries:
                    return False
                expected_entries = {entry['name']: entry for entry in hook['webhooks']}
                if {entry.get('name') for entry in entries} != set(expected_entries):
                    return False
                for entry in entries:
                    original = expected_entries[entry['name']]
                    if (entry.get('sideEffects') != original.get('sideEffects') or
                            entry.get('matchPolicy', 'Equivalent') != original.get('matchPolicy', 'Equivalent') or
                            any((entry.get(key) or {}) != (original.get(key) or {}) for key in ('namespaceSelector', 'objectSelector')) or
                            (entry.get('matchConditions') or []) != (original.get('matchConditions') or []) or
                            [dict(rule, scope=rule.get('scope', '*')) for rule in entry.get('rules', [])] !=
                            [dict(rule, scope=rule.get('scope', '*')) for rule in original.get('rules', [])]):
                        return False
                    config = entry.get('clientConfig', {})
                    actual_service = dict(config.get('service', {}))
                    expected_service = dict(original.get('clientConfig', {}).get('service', {}))
                    actual_service.setdefault('port', 443)
                    expected_service.setdefault('port', 443)
                    try:
                        certificate = base64.b64decode(config.get('caBundle', ''), validate=True)
                    except (ValueError, TypeError):
                        return False
                    if (not certificate or actual_service != expected_service or config.get('service', {}).get('namespace') != namespace or
                            config.get('service', {}).get('name') != service['metadata']['name'] or entry.get('failurePolicy') != 'Fail'):
                        return False
                return True
            wait([hook['kind'].lower(), hook['metadata']['name']], hooked, 'CNPG admission configuration')
        migrations = parse(crd_bytes.decode('utf-8'), 'Migration CRD rendering')
        if len(migrations) != 1:
            raise Refusal('assigned Migration CRD rendering must contain one resource')
        migration = migrations[0]
        if migration.get('kind') != 'CustomResourceDefinition' or migration['metadata']['name'] != 'migrations.pgcopydb-operator.io':
            raise Refusal('assigned Migration CRD identity mismatch')
        if self.source_tree() != source_hash:
            raise Refusal('assigned source tree changed before CRD apply')
        self.write_private('migration-crd-applied.json', apply([migration]).encode())
        wait(['crd', 'migrations.pgcopydb-operator.io'], lambda value: established(value, 'migrations.pgcopydb-operator.io', 'v1beta1'), 'Migration CRD')
        storage = wait(['storageclasses'], lambda value: isinstance(value, dict) and sum(
            item.get('metadata', {}).get('annotations', {}).get('storageclass.kubernetes.io/is-default-class') == 'true'
            for item in value.get('items', [])) == 1, 'default StorageClass')
        storage = next(item for item in storage['items'] if item.get('metadata', {}).get('annotations', {}).get('storageclass.kubernetes.io/is-default-class') == 'true')
        if storage.get('provisioner') != 'rancher.io/local-path' or storage.get('reclaimPolicy') != 'Delete' or not storage.get('metadata', {}).get('uid'):
            raise Refusal('owned kind storage provisioner is unsupported')
        provisioner = wait(['deployment', 'local-path-provisioner', '-n', 'local-path-storage'], deployment_ready, 'storage provisioner')
        provisioner_container, = provisioner['spec']['template']['spec']['containers']
        command = provisioner_container.get('command', []) + provisioner_container.get('args', [])
        if (provisioner_container.get('image') != bundled['storage-provisioner']['ref'] or
                provisioner_container.get('imagePullPolicy') != 'IfNotPresent' or '--helper-image' not in command or
                command[command.index('--helper-image') + 1] != bundled['storage-helper']['ref']):
            raise Refusal('storage provisioner does not use the verified bundled images')
        provisioner_pods = wait(['pods', '-n', 'local-path-storage', '-l', 'app=local-path-provisioner'],
             lambda value: bool(value and value.get('items')) and all(
                 pod.get('metadata', {}).get('uid') and pod.get('spec', {}).get('nodeName') in {node['name'] for node in ledger['nodes']} and
                 any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in pod.get('status', {}).get('conditions', [])) and
                 any(c.get('ready') is True and self.valid_loaded_image_id(c.get('imageID'), pod.get('spec', {}).get('nodeName'),
                     bundled['storage-provisioner'], {node['name'] for node in ledger['nodes']})
                     for c in pod.get('status', {}).get('containerStatuses', [])) for pod in value['items']), 'storage provisioner pods')
        helper_config = wait(['configmap', 'local-path-config', '-n', 'local-path-storage'], lambda value: bool(value and value.get('data', {}).get('helperPod.yaml')), 'storage helper template')
        helpers = parse(helper_config['data']['helperPod.yaml'], 'storage helper rendering')
        if len(helpers) != 1:
            raise Refusal('storage helper rendering must contain one resource')
        helper = helpers[0]
        helper_container, = helper['spec']['containers']
        if helper_container.get('image') != bundled['storage-helper']['ref'] or helper_container.get('imagePullPolicy') != 'IfNotPresent':
            raise Refusal('storage helper can pull an unverified substitute')
        probe_namespace = 'rpgmigrate-m7-bootstrap'
        existing_probe = request(['get', 'namespace', probe_namespace, '--ignore-not-found', '-o', 'json'])
        if existing_probe.strip():
            raise Refusal('storage probe namespace already exists; refusing a stale probe witness')
        apply([{'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': probe_namespace}}])
        probe = {'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster', 'metadata': {'name': 'admission-probe', 'namespace': probe_namespace},
                 'spec': {'instances': 1, 'imageName': postgres_image_name, 'storage': {'size': '1Gi', 'storageClass': storage['metadata']['name']}}}
        try:
            admitted = decode(request(['create', '--dry-run=server', '-f', '-', '-o', 'json'], input=json.dumps(probe)), 'CNPG admission probe')
        except CommandError as error:
            failure = error.failure_class if error.failure_class in ('CalledProcessError', 'TimeoutExpired',
                'FileNotFoundError', 'PermissionError', 'OSError', 'UnicodeDecodeError') else 'CommandError'
            returncode = error.returncode if type(error.returncode) is int else 'none'
            raise Refusal(f'CNPG admission probe request failed (class={failure}, returncode={returncode})') from None
        if (admitted.get('kind') != 'Cluster' or admitted.get('metadata', {}).get('name') != 'admission-probe' or admitted.get('spec', {}).get('instances') != 1 or admitted.get('metadata', {}).get('namespace') != probe_namespace or
                admitted.get('spec', {}).get('imageName') != postgres_image_name):
            raise Refusal('CNPG admission probe did not execute')
        claim = {'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim', 'metadata': {'name': 'storage-probe', 'namespace': probe_namespace},
                 'spec': {'storageClassName': storage['metadata']['name'], 'accessModes': ['ReadWriteOnce'], 'resources': {'requests': {'storage': '1Gi'}}}}
        pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'storage-probe', 'namespace': probe_namespace},
               'spec': {'restartPolicy': 'Never', 'securityContext': {'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532, 'fsGroup': 65532},
                 'containers': [{'name': 'probe', 'image': images['seed-tool-runner']['ref'], 'imagePullPolicy': 'IfNotPresent',
                   'command': ['bash', '-ceu', 'test -d /probe && printf m7-probe > /probe/witness && test "$(cat /probe/witness)" = m7-probe'],
                   'resources': {'requests': {'cpu': '100m', 'memory': '128Mi'}, 'limits': {'cpu': '100m', 'memory': '128Mi'}},
                   'securityContext': {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}},
                   'volumeMounts': [{'name': 'probe', 'mountPath': '/probe'}]}],
                 'volumes': [{'name': 'probe', 'persistentVolumeClaim': {'claimName': 'storage-probe'}}]}}
        apply([claim, pod])
        pvc = wait(['pvc', 'storage-probe', '-n', probe_namespace], lambda value: bool(value and value.get('metadata', {}).get('uid') and
                   value.get('status', {}).get('phase') == 'Bound' and value.get('spec', {}).get('volumeName') and value.get('status', {}).get('capacity', {}).get('storage') == '1Gi'), 'storage probe claim')
        volume_name = pvc['spec']['volumeName']
        wait(['pv', volume_name], lambda value: bool(value and value.get('status', {}).get('phase') == 'Bound' and
             value.get('spec', {}).get('claimRef', {}).get('uid') == pvc['metadata']['uid'] and value.get('spec', {}).get('storageClassName') == storage['metadata']['name']), 'storage probe volume')
        finished = wait(['pod', 'storage-probe', '-n', probe_namespace], lambda value: bool(value and value.get('status', {}).get('phase') in ('Succeeded', 'Failed')), 'storage mount probe')
        statuses = finished.get('status', {}).get('containerStatuses', [])
        if (finished['status']['phase'] != 'Succeeded' or len(statuses) != 1 or statuses[0].get('state', {}).get('terminated', {}).get('exitCode') != 0 or
                not self.valid_loaded_image_id(statuses[0].get('imageID'), finished.get('spec', {}).get('nodeName'),
                    images['seed-tool-runner'], {node['name'] for node in ledger['nodes']})):
            raise Refusal('storage mount probe failed')
        namespace_object = wait(['namespace', probe_namespace], lambda value: bool(value and value.get('metadata', {}).get('uid') and
                                value.get('metadata', {}).get('labels', {}).get(RUN_LABEL) == ledger['run_id']), 'probe namespace ownership')
        request(['delete', '--raw=/api/v1/namespaces/' + probe_namespace, '-f', '-'], input=json.dumps(
            {'apiVersion': 'v1', 'kind': 'DeleteOptions', 'preconditions': {'uid': namespace_object['metadata']['uid']}, 'propagationPolicy': 'Foreground'}))
        wait(['namespace', probe_namespace, '--ignore-not-found'], lambda value: value is None, 'probe namespace deletion')
        wait(['pv', volume_name, '--ignore-not-found'], lambda value: value is None, 'probe volume deletion')
        receipt = {'cnpg_manifest_sha256': CNPG_SHA256, 'cnpg_applied_sha256': hashlib.sha256((self.directory / 'cnpg-applied.json').read_bytes()).hexdigest(),
                   'migration_crd_sha256': hashlib.sha256(crd_bytes).hexdigest(), 'source_tree_sha256': source_hash,
                   'controller_pod_uids': sorted(pod_uids), 'storage_registry': bundle, 'storageclass_uid': storage['metadata']['uid'],
                   'provisioner_uid': provisioner['metadata']['uid'], 'provisioner_pod_uids': sorted(pod['metadata']['uid'] for pod in provisioner_pods['items']), 'probe_pvc_uid': pvc['metadata']['uid'],
                   'admission_verified': True, 'storage_mounted': True, 'probe_cleanup': True}
        ledger['bootstrap'] = receipt
        self.save(ledger)
        self.write_private('bootstrap-receipt.json', (json.dumps(receipt, sort_keys=True) + '\n').encode())
        return receipt

    def source_inventory(self):
        files = self.call(['git', 'ls-files', '-z', '--cached']).split('\0')
        inventory = []
        for name in sorted(set(filter(None, files))):
            path = ROOT / name
            if not path.is_file() or path.resolve() != path or not path.resolve().is_relative_to(ROOT) or path.stat().st_nlink != 1:
                raise Refusal('source tree contains a missing or linked input')
            inventory.append([name, stat.S_IMODE(path.stat().st_mode), hashlib.sha256(path.read_bytes()).hexdigest()])
        if not inventory:
            raise Refusal('source tree inventory is empty')
        return inventory

    def source_tree(self):
        return hashlib.sha256(json.dumps(self.source_inventory(), separators=(',', ':')).encode()).hexdigest()


    @staticmethod
    def selection(engine, pair=None, case=None):
        if engine == 'pgcopydb' and pair in (None, '17:17') and case is None:
            return {'engine': engine, 'pair': '17:17', 'label': 'kind-bootstrap', 'spec': LEGACY_SPEC}
        if engine == 'rpgmigrate' and pair in NATIVE_PAIRS and case in NATIVE_SPECS:
            label, spec = NATIVE_SPECS[case]
            return {'engine': engine, 'pair': pair, 'label': label, 'spec': spec}
        raise Refusal('kind route admits legacy pgcopydb 17:17, or native rpgmigrate 16:16, 18:18 or 16:18 with one named spec')

    def receipt(self, ledger, selection):
        path = self.directory / 'suite-receipt.json'
        self.private(path)
        input_path = self.directory / 'suite-input.json'
        self.private(input_path)
        receipt = decode(path.read_text(), 'suite receipt')
        inputs = decode(input_path.read_text(), 'suite input')
        if not isinstance(receipt, dict) or not isinstance(inputs, dict) or inputs != self.suite_projection(ledger, selection):
            raise Refusal('private suite input or receipt differs from current source, lock or node bindings')
        bootstrap = ledger.get('bootstrap') or {}
        if (not isinstance(bootstrap, dict) or bootstrap.get('cnpg_manifest_sha256') != CNPG_SHA256 or
                not re.fullmatch('[0-9a-f]{64}', bootstrap.get('migration_crd_sha256', '')) or
                bootstrap.get('source_tree_sha256') != inputs.get('source_tree_sha256') or
                not all(bootstrap.get(key) is True for key in ('admission_verified', 'storage_mounted', 'probe_cleanup'))):
            raise Refusal('bound bootstrap readiness proof is missing')
        executed = receipt.get('executed_specs')
        if (receipt.get('schema_version') != 2 or receipt.get('run_id') != ledger['run_id'] or
                receipt.get('engine') != selection['engine'] or receipt.get('pair') != selection['pair'] or
                receipt.get('reporter') != 'ginkgo-report-after-suite' or receipt.get('suite_succeeded') is not True or
                receipt.get('failures') != [] or
                receipt.get('input_sha256') != hashlib.sha256(input_path.read_bytes()).hexdigest() or
                type(executed) is not int or executed != 1 or type(receipt.get('passed_specs')) is not int or
                receipt['passed_specs'] != executed or type(receipt.get('skipped_specs')) is not int or
                receipt['skipped_specs'] != 0 or receipt.get('selected_spec') != selection['spec'] or
                receipt.get('cleanup') is not True):
            raise Refusal('positive ' + ('native execution' if selection['engine'] == 'rpgmigrate' else
                                         'legacy execution and verification') + ' receipt is required')
        # A native receipt carries no legacy bootstrap claims; the spec it names asserted its own outcome.
        if selection['engine'] == 'rpgmigrate' and set(receipt) != NATIVE_RECEIPT_KEYS:
            raise Refusal('positive native execution receipt is required')
        if selection['engine'] == 'pgcopydb' and (receipt.get('phase') != 'Completed' or
                receipt.get('reason') != 'DrainVerified' or receipt.get('complete') is not True or
                receipt.get('cutover_completed') is not True or receipt.get('complete_reason') != 'MigrationSucceeded' or
                receipt.get('selected_spec') != LEGACY_SPEC or
                receipt.get('nonempty_data_verified') is not True or
                receipt.get('sequence_verified') is not True or receipt.get('cleanup') is not True):
            raise Refusal('positive legacy execution and verification receipt is required')
        source, expiry = inputs.get('source_tree_sha256'), inputs.get('lease_expires_at')
        images = inputs.get('images')
        manager = images.get('manager') if isinstance(images, dict) else None
        if (not isinstance(source, str) or not re.fullmatch('[0-9a-f]{64}', source) or
                type(expiry) not in (int, float) or not 0 < expiry <= sys.float_info.max or
                not isinstance(manager, dict) or source != self.private_json('image-lock.json')['operator_source']['payload_inventory_sha256'] or
                not isinstance(manager.get('ref'), str) or not manager['ref'] or
                any(not isinstance(manager.get(key), str) or not DIGEST.fullmatch(manager[key])
                    for key in ('image_id', 'loaded_digest'))):
            raise Refusal('bound manager runtime input is invalid')

        def identity(value, maximum):
            return (isinstance(value, str) and 0 < len(value) <= maximum and
                    all(not char.isspace() and char.isprintable() for char in value))

        runtime = receipt.get('manager_runtime')
        uid_fields = ('namespace_uid', 'deployment_uid', 'replicaset_uid', 'pod_uid')
        string_fields = {*uid_fields, 'expected_version', 'observed_version', 'node_name', 'image_ref',
                         'image_config_id', 'image_manifest_digest', 'container_image_id', 'container_id'}
        fields = {*string_fields, 'authenticated', 'restart_count', 'observed_at', 'cleanup'}
        version = 'm7-' + source[:12]
        if (not isinstance(runtime, dict) or set(runtime) != fields or
                any(not isinstance(runtime[key], str) for key in string_fields) or
                runtime.get('expected_version') != version or runtime.get('observed_version') != version or
                runtime.get('authenticated') is not True or runtime.get('cleanup') is not True or
                any(not identity(runtime.get(key), 256) for key in uid_fields) or
                not identity(runtime.get('container_id'), 512) or
                runtime.get('node_name') not in {node['name'] for node in ledger['nodes']} or
                runtime.get('image_ref') != manager['ref'] or runtime.get('image_config_id') != manager['image_id'] or
                runtime.get('image_manifest_digest') != manager['loaded_digest'] or
                not self.valid_loaded_image_id(runtime.get('container_image_id'), runtime.get('node_name'), manager,
                    {node['name'] for node in ledger['nodes']}) or
                type(runtime.get('restart_count')) is not int or runtime['restart_count'] < 0 or
                type(runtime.get('observed_at')) not in (int, float) or
                not 0 < runtime['observed_at'] <= expiry):
            raise Refusal('authenticated manager runtime proof is required')
        return receipt

    def run(self, engine, pair=None, case=None):
        selection = self.selection(engine, pair, case)
        ledger = self.read()
        if ledger['phase'] != 'pulled' or len(self.owned_nodes(ledger)) != 3:
            raise Refusal('owned pulled cluster is required')
        self.require_lease()
        self.verify_node_limits(ledger)
        self.kubeconfig(ledger)
        if any((self.directory / name).exists() for name in ('suite-receipt.json', 'go-test.json')):
            raise Refusal('suite receipt or go-test.json already exists; refusing a stale execution witness')
        suite_input = self.suite_input(ledger, selection)
        self.bootstrap(ledger, suite_input['source_tree_sha256'])
        suite_input = self.suite_input(ledger, selection)
        self.require_lease()
        roles = {role: image['ref'] for role, image in suite_input['images'].items()}
        self.environment.update(E2E_KIND_RUN_DIR=str(self.directory), E2E_KIND_CONTEXT=ledger['context'],
                                E2E_RUN_LABEL_VALUE=ledger['run_id'], E2E_ENGINE=engine, E2E_SMALL='true', E2E_SCALE='0.01',
                                E2E_CNPG_INSTANCES='1', E2E_KEEP_FIXTURES='false', E2E_FORCE='false',
                                E2E_STRESS='false', E2E_MANAGE_NAMESPACES='true')
        for env_key, role in self.suite_environment(selection):
            self.environment['E2E_' + env_key + '_IMAGE'] = roles[role]
        if engine == 'rpgmigrate':
            self.environment.update(zip(('E2E_PG_SOURCE', 'E2E_PG_TARGET'), selection['pair'].split(':')))
        ledger['suite_execution'] = {'budget_seconds': 1800, 'result': 'running',
                                     'selection': {'engine': engine, 'pair': selection['pair'], 'case': case}}
        self.save(ledger)
        try:
            self.verify_node_limits(ledger)
            self.registry_call(ledger, ['task', 'e2e:kind:suite', 'RUN_DIR=' + str(self.directory), 'EXPECT_CONTEXT=' + ledger['context'],
                       'E2E_ENGINE=' + engine, 'LABEL=' + selection['label']], timeout=1800)
            self.receipt(ledger, selection)
        except Refusal as error:
            failure = getattr(error, 'failure_class', type(error).__name__)
            if failure not in ('CalledProcessError', 'TimeoutExpired', 'OSError', 'FileNotFoundError',
                               'PermissionError', 'UnicodeDecodeError', 'Refusal'):
                failure = 'CommandError'
            returncode = getattr(error, 'returncode', None)
            if type(returncode) is not int or not -255 <= returncode <= 255:
                returncode = None
            execution = ledger['suite_execution']
            execution.update(result='failed', error_class=failure, returncode=returncode, receipt_status='absent',
                             go_test_json=self.go_test_json())
            path = self.directory / 'suite-receipt.json'
            if path.exists() or path.is_symlink():
                execution.update(receipt_file='suite-receipt.json', receipt_status='unvalidated')
                try:
                    self.private(path)
                    if path.stat().st_size <= 131072:
                        report = self.private_json('suite-receipt.json')
                        if (isinstance(report, dict) and type(report.get('schema_version')) is int and
                                report.get('schema_version') == 2 and
                                report.get('reporter') == 'ginkgo-report-after-suite' and
                                type(report.get('suite_succeeded')) is bool and
                                report.get('run_id') == ledger['run_id'] and report.get('engine') == engine and
                                report.get('pair') == selection['pair'] and report.get('input_sha256') ==
                                hashlib.sha256(self.private_bytes('suite-input.json')).hexdigest()):
                            execution['receipt_status'] = 'ginkgo-report-after-suite'
                except (Refusal, OSError):
                    pass
            self.save(ledger)
            raise Refusal(f'suite execution failed within 1800s budget: {failure} '
                          f'(returncode={returncode}, receipt={execution["receipt_status"]}, go_test_json={execution["go_test_json"]})') from None
        ledger['suite_execution'].update(result='passed', go_test_json=self.go_test_json())
        ledger['phase'] = 'verified'
        self.save(ledger)

    def go_test_json(self):
        # The private stream keeps a failing assertion; only its presence enters the ledger.
        try:
            self.private(self.directory / 'go-test.json')
            return 'go-test.json'
        except Refusal:
            return 'absent'

    def kubernetes_ownership(self, ledger):
        nodes = decode(self.kubectl(['get', 'nodes', '-o', 'json']), 'Kubernetes nodes')
        observed = nodes.get('items', [])
        if ({item.get('metadata', {}).get('name') for item in observed} != {node['name'] for node in ledger['nodes']} or
                any(item.get('metadata', {}).get('labels', {}).get(RUN_LABEL) != ledger['run_id'] for item in observed)):
            raise Refusal('Kubernetes node run labels changed')
        namespaces = decode(self.kubectl(['get', 'namespaces', '-o', 'json']), 'Kubernetes namespaces')
        owned_names = {'pgcopydb-e2e', 'pgcopydb-e2e-x', 'pgcopydb-e2e-system'}
        for item in namespaces.get('items', []):
            meta = item.get('metadata', {})
            if meta.get('name') in owned_names and meta.get('labels', {}).get(RUN_LABEL) != ledger['run_id']:
                raise Refusal('fixture namespace run label mismatch')

    def down(self):
        ledger = self.read()
        nodes = self.owned_nodes(ledger)
        volumes = self.owned_volumes(ledger, {node['id'] for node in nodes})
        if nodes:
            if ledger.get('phase') != 'cleaning':
                self.kubernetes_ownership(ledger)
                ledger['cleanup_authorized'] = {'node_ids': sorted(node['id'] for node in ledger['nodes']),
                                                'volume_names': sorted(volume['name'] for volume in ledger.get('volumes', []))}
                ledger['phase'] = 'cleaning'
                self.save(ledger)
            # Delete by immutable IDs so a name replacement cannot expand cleanup authority.
            self.call(['podman', 'rm', '--force'] + [node['id'] for node in nodes])
            if self.owned_nodes(ledger):
                raise Refusal('owned nodes remain after teardown')
        elif volumes and ledger.get('phase') != 'cleaning':
            raise Refusal('volume cleanup has no retained node deletion authorization')
        for name in self.owned_volumes(ledger, set()):
            self.call(['podman', 'volume', 'rm', name])
        if self.owned_nodes(ledger) or self.owned_volumes(ledger, set()):
            raise Refusal('owned resources remain after teardown')
        ledger['teardown'] = {'nodes_absent': True, 'volumes_absent': True}
        ledger['phase'] = 'down'
        self.save(ledger)
        if (self.directory / 'suite-receipt.json').exists():
            selection = (ledger.get('suite_execution') or {}).get('selection') or {}
            receipt = dict(self.receipt(ledger, self.selection(selection.get('engine'), selection.get('pair'), selection.get('case'))),
                           cluster_teardown=True)
            self.write_private('final-receipt.json', (json.dumps(receipt, sort_keys=True) + '\n').encode())


    def inventory(self):
        lock = self.private_json('image-lock.json')
        registry.validate_lock(lock, self.architecture())
        return [dict(lock['images'][role]) for role in registry.ROLES]

    def cnpg_image_name(self, image):
        if not isinstance(image, dict) or image.get('role') not in ('postgresql-16', 'postgresql-17', 'postgresql-18'):
            raise Refusal('CNPG PostgreSQL role is invalid')
        expected = self.private_json('image-lock.json')['images'].get(image['role'])
        if any(image.get(key) != expected.get(key) for key in registry.IMAGE_KEYS):
            raise Refusal('CNPG PostgreSQL image differs from the admitted lock')
        return expected['ref']

    def external_bytes(self, path, *, maximum=8 * 1024 ** 2):
        path = Path(path)
        if not path.is_absolute() or path.resolve() != path:
            raise Refusal('external receipt path is not canonical')
        self.private(path.parent, directory=True)
        self.private(path)
        if not 0 < path.stat().st_size <= maximum:
            raise Refusal('external receipt size is invalid')
        return path.read_bytes()

    def source_identity(self):
        if self.call(['git', 'status', '--porcelain=v1', '--untracked-files=all']).strip():
            raise Refusal('qualification requires a clean tracked source checkout')
        origin = self.call(['git', 'remote', 'get-url', 'origin']).removesuffix('\n')
        repository = 'https://github.com/ydixken/pgcopydb-operator'
        if origin not in {repository, repository + '.git', 'git@github.com:ydixken/pgcopydb-operator.git'}:
            raise Refusal('source origin is not an approved operator repository')
        identity = {'repository': repository,
            'commit': self.call(['git', 'rev-parse', 'HEAD']).strip(),
            'git_tree': self.call(['git', 'rev-parse', 'HEAD^{tree}']).strip(),
            'payload_inventory_sha256': self.source_tree()}
        registry.source(identity, 'https://github.com/ydixken/pgcopydb-operator')
        return identity

    def receipt_inputs(self, image_lock, receipt_dir):
        lock_path, receipts = Path(image_lock), Path(receipt_dir)
        if not receipts.is_absolute() or receipts.resolve() != receipts:
            raise Refusal('receipt directory must be canonical')
        self.private(receipts, directory=True)
        for directory in (lock_path.parent, receipts):
            if any((p / '.git').exists() or (p / '.git').is_symlink() for p in (directory, *directory.parents)):
                raise Refusal('image lock and receipts must be outside Git')
        raw = self.external_bytes(lock_path, maximum=1024 ** 2)
        lock = registry.validate_lock(decode(raw, 'image lock'), self.architecture())
        catalog = decode((CONFIG / 'images.json').read_bytes(), 'static catalog')
        registry.validate_catalog(catalog, lock)
        if catalog['cnpg'] != {'version': '1.30.1', 'manifest_sha256': CNPG_SOURCE_SHA256}:
            raise Refusal('static CNPG source pin changed')
        if self.source_identity() != lock['operator_source']:
            raise Refusal('publication source differs from current clean source')
        outputs = {'image-lock.json': raw}
        graphs = {}
        for role, image in lock['images'].items():
            graph_name, public_name = role + '-registry-receipt.json', role + '-public-receipt.json'
            graph_raw, public_raw = self.external_bytes(receipts / graph_name), self.external_bytes(receipts / public_name)
            graph, public = decode(graph_raw, 'registry receipt'), decode(public_raw, 'public receipt')
            if (registry.sha(registry.canonical(graph)) != image['provenance']['registry_receipt_sha256'] or
                    registry.sha(registry.canonical(public)) != image['provenance']['public_receipt_sha256']):
                raise Refusal('registry or public receipt hash differs from lock')
            graphs[role] = registry.validate_public(image, graph, public)
            outputs.update({graph_name: graph_raw, public_name: public_raw})
            if role in registry.DEV_ROLES:
                name = role + '-publication-receipt.json'
                publication_raw = self.external_bytes(receipts / name)
                if registry.sha(publication_raw) != lock['publication_receipts_sha256'][role]:
                    raise Refusal('full publication receipt hash differs from lock')
                registry.validate_publication(image, decode(publication_raw, 'publication receipt'), graph, public)
                outputs[name] = publication_raw
        name = 'legacy-runner-executable-receipt.json'
        outputs[name] = self.external_bytes(receipts / name)
        self.baseline_provenance(lock, decode(outputs[name], 'legacy executable qualification'), receipts)
        outputs.update(self.storage_outputs(lock))
        return lock, outputs, graphs

    def anonymous_metadata(self, image, metadata):
        repository = registry.repository(image['ref'])
        prefix = ['skopeo', '--registries-conf', '/dev/null', 'inspect', '--no-creds', '--tls-verify=true', '--raw']
        for digest, expected, config in ((image['index_digest'], metadata['index'], False),
                (image['digest'], metadata['manifest'], False), (image['digest'], metadata['config'], True)):
            if expected is None:
                continue
            args = prefix + (['--config'] if config else []) + ['docker://' + repository + '@' + digest]
            observed = self.vm(['env', '-i', 'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin', *args], timeout=120).encode()
            if observed != expected:
                raise Refusal('anonymous raw registry metadata differs from receipt')


    def prepare(self, image_lock, receipt_dir):
        lock, outputs, graphs = self.receipt_inputs(image_lock, receipt_dir)
        if self.directory.exists() and any(p.name != 'lifecycle.lock' for p in self.directory.iterdir()):
            raise Refusal('registry preparation requires an empty fresh RUN')
        ledger = self.initialize()
        self.tools()
        self.preparation_tools()
        self.preparation_capacity()
        source = lock['operator_source']['payload_inventory_sha256']
        preparation = {'schema': 2, 'transport': 'public-registry', 'run_id': ledger['run_id'], 'complete': False,
            'pending': 'anonymous-registry-verification', 'source_root': str(ROOT), 'source_tree_sha256': source,
            'image_lock_sha256': registry.sha(outputs['image-lock.json']), 'image_lock_path': str(Path(image_lock)),
            'receipt_dir': str(Path(receipt_dir)), 'files': {}}
        self.write_private('artifact-preparation-evidence.json', registry.canonical(preparation) + b'\n', exclusive=True)
        for role, image in lock['images'].items():
            self.anonymous_metadata(image, graphs[role])
            if self.source_identity() != lock['operator_source']:
                raise Refusal('clean source changed during registry preparation')
        outputs['source-inventory.json'] = registry.canonical(self.source_inventory()) + b'\n'
        for name, raw in outputs.items():
            self.write_private(name, raw, exclusive=True)
            preparation['files'][name] = registry.sha(raw)
        ledger['images'] = [lock['images'][role] for role in registry.ROLES]
        ledger['phase'] = 'prepared'
        self.save(ledger)
        preparation.update(complete=True, pending=None)
        try:
            self.write_private('artifact-preparation-evidence.json', registry.canonical(preparation) + b'\n')
            return self.check()
        except BaseException:
            preparation.update(complete=False, pending='final-verification-failed')
            try:
                self.write_private('artifact-preparation-evidence.json', registry.canonical(preparation) + b'\n')
            except BaseException:
                pass
            raise

    def preparation_tools(self):
        for args, flags in [(['podman', 'update', '--help'], ('--cpus', '--memory', '--memory-swap')),
                (['kind', 'create', 'cluster', '--help'], ('--retain', '--image')),
                (['podman', 'machine', 'ssh', '--help'], ('COMMAND',))]:
            if any(flag not in self.call(args) for flag in flags):
                raise Refusal('required registry transport tool flag is unavailable')
        if not re.fullmatch(r'skopeo version 1\.22\.2(?: commit: [0-9a-f]{40})?\n?', self.vm(['skopeo', '--registries-conf', '/dev/null', '--version'])):
            raise Refusal('skopeo 1.22.2 is required')
        architecture = {'aarch64': 'arm64', 'arm64': 'arm64', 'x86_64': 'amd64'}.get(self.vm(['uname', '-m']).strip())
        if architecture != self.architecture():
            raise Refusal('Podman VM architecture differs from the host qualification')
        self.cgroup_root()
        return {'transport': 'public-registry'}

    def require_prepared(self, ledger):
        preparation = self.private_json('artifact-preparation-evidence.json')
        fields = {'schema', 'transport', 'run_id', 'complete', 'pending', 'source_root', 'source_tree_sha256',
            'image_lock_sha256', 'image_lock_path', 'receipt_dir', 'files'}
        registry.closed(preparation, fields, 'preparation evidence')
        if (preparation['schema'] != 2 or preparation['transport'] != 'public-registry' or
                preparation['run_id'] != ledger['run_id'] or preparation['complete'] is not True or
                preparation['pending'] is not None or preparation['source_root'] != str(ROOT) or
                not isinstance(preparation['files'], dict)):
            raise Refusal('registry preparation is incomplete or belongs to other source inputs')
        lock, outputs, _ = self.receipt_inputs(preparation['image_lock_path'], preparation['receipt_dir'])
        outputs['source-inventory.json'] = registry.canonical(self.source_inventory()) + b'\n'
        expected = {name: registry.sha(raw) for name, raw in outputs.items()}
        if (preparation['files'] != expected or preparation['image_lock_sha256'] != registry.sha(outputs['image-lock.json']) or
                preparation['source_tree_sha256'] != lock['operator_source']['payload_inventory_sha256'] or
                ledger['images'] != [lock['images'][role] for role in registry.ROLES]):
            raise Refusal('prepared lock, image or complete artifact inventory changed')
        for name, raw in outputs.items():
            if self.private_bytes(name) != raw:
                raise Refusal('prepared private metadata bytes changed')
        return preparation

    def check(self):
        ledger = self.read()
        self.require_prepared(ledger)
        if ledger['phase'] not in ('prepared', 'ready', 'pulled', 'verified'):
            raise Refusal('RUN phase does not have complete registry authority')
        if ledger['phase'] == 'ready' and (self.directory / 'pulled-image-resolution-evidence.json').exists():
            raise Refusal('partial or failed pull evidence is retained; a fresh RUN is required')
        if ledger['phase'] in ('ready', 'pulled', 'verified'):
            self.registry_admit(ledger)
        if ledger['phase'] in ('pulled', 'verified'):
            self.pull_evidence(ledger, registry.PULL_ROLES, self.directory, 'pulled-image-resolution-evidence.json')
        return ledger

    def _check(self, ledger):
        return self.check()

    def registry_admit(self, ledger):
        if self.read().get('resource_lease') != ledger.get('resource_lease'):
            raise Refusal('original resource lease changed during registry admission')
        self.require_prepared(ledger)
        self.require_lease()
        self.verify_node_limits(ledger)
        nodes = self.node_inventory(ledger)
        if nodes != ledger['nodes'] or len(nodes) != 3:
            raise Refusal('registry operation requires all exact owned nodes')
        self.verify_storage_mount(ledger)
        self.require_lease()
        return nodes

    def registry_call(self, ledger, args, *, deadline=None, monotonic_deadline=None, **kwargs):
        lease = self.read().get('resource_lease', {})
        if lease != ledger.get('resource_lease'):
            raise Refusal('original resource lease changed during registry operation')
        wall = lease.get('expires_at', 0) if deadline is None else min(lease.get('expires_at', 0), deadline)
        mono = lease.get('monotonic_expires_at', 0) if monotonic_deadline is None else min(lease.get('monotonic_expires_at', 0), monotonic_deadline)
        remaining = min(wall - time.time(), mono - time.monotonic())
        if remaining <= 0:
            raise Refusal('original registry clocks expired')
        kwargs['timeout'] = min(kwargs.get('timeout', 30), remaining)
        result = self.call(args, **kwargs)
        if time.time() >= wall or time.monotonic() >= mono:
            raise Refusal('original registry clocks expired')
        return result

    @staticmethod
    def cri_prefix(node_id):
        if not isinstance(node_id, str) or not IDENTITY.fullmatch(node_id):
            raise Refusal('CRI operation requires a canonical exact node CID')
        return ['podman', 'exec', node_id, 'env', '-i', 'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
            'crictl', '--config=/dev/null', '--runtime-endpoint=unix:///run/containerd/containerd.sock',
            '--image-endpoint=unix:///run/containerd/containerd.sock', '--timeout=10s']

    def pulled_references(self, node_id, call):
        prefix = ['podman', 'exec', node_id, 'env', '-i', 'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
            'ctr', '--address', '/run/containerd/containerd.sock', '--namespace', 'k8s.io', 'images', 'list']
        names = call(prefix + ['--quiet']).splitlines()
        rows = [line.split() for line in call(prefix).splitlines() if line.split()]
        if len(names) != len(set(names)) or any(not name or any(c.isspace() for c in name) for name in names):
            raise Refusal('node image reference inventory is ambiguous')
        result = {}
        for name in names:
            matches = [row for row in rows if row[0] == name]
            if len(matches) != 1 or len(matches[0]) < 3 or not DIGEST.fullmatch(matches[0][2]):
                raise Refusal('node image typed descriptor is unavailable')
            result[name] = matches[0][1:3]
        if any(row[0] not in result and row[0] != 'REF' for row in rows):
            raise Refusal('node image reference inventories disagree')
        return result

    def verify_pulled(self, image, node_id):
        return self._verify_pulled(image, node_id)

    def _verify_pulled(self, image, node_id, deadline=None, monotonic_deadline=None):
        ledger = self.read()
        nodes = self.registry_admit(ledger)
        matching = [node for node in nodes if node['id'] == node_id]
        if len(matching) != 1:
            raise Refusal('pulled image node is foreign or replaced')
        current = {item['role']: item for item in self.inventory()}
        if image != current.get(image.get('role')):
            raise Refusal('pulled image differs from the complete immutable lock')
        def call(args, **kwargs):
            return self.registry_call(ledger, args, deadline=deadline, monotonic_deadline=monotonic_deadline, **kwargs)
        observed = decode(call(self.cri_prefix(node_id) + ['inspecti', image['ref']]), 'CRI ImageStatus')
        status = observed.get('status') if isinstance(observed, dict) else None
        if (not isinstance(status, dict) or status.get('id') != image['config_digest'] or
                any(not isinstance(status.get(k), list) or any(not isinstance(v, str) for v in status[k]) or
                    len(status[k]) != len(set(status[k])) for k in ('repoTags', 'repoDigests'))):
            raise Refusal('current CRI config or advertised references are unavailable')
        refs = self.pulled_references(node_id, call)
        repo = registry.repository(image['ref'])
        requested = image['ref'] if image['ref'] in refs else repo + '@' + image['digest']
        advertised = status['repoDigests']
        if not advertised or requested not in refs or any(registry.repository(ref) != repo or '@' not in ref for ref in advertised):
            raise Refusal('current CRI public requested reference is unavailable')
        graph = self.private_json(image['role'] + '-registry-receipt.json')
        expected = registry.validate_graph(image, graph, image['role'] in registry.DEV_ROLES)
        prefix = ['podman', 'exec', node_id, 'env', '-i', 'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
            'ctr', '--address', '/run/containerd/containerd.sock', '--namespace', 'k8s.io', 'content', 'get']
        def content(digest, raw):
            actual = call(prefix + [digest]).encode()
            if actual != raw or 'sha256:' + registry.sha(actual) != digest:
                raise Refusal('node-local content digest differs from the public typed graph')
        content(image['digest'], expected['manifest'])
        content(image['config_digest'], expected['config'])
        accepted = {image['digest'], image['config_digest']}
        for ref in sorted(set([requested, *advertised])):
            if registry.repository(ref) != repo or ref not in refs:
                raise Refusal('CRI reference has no exact node-local repository binding')
            media_type, target = refs[ref]
            if target == image['digest'] and media_type in registry.MANIFEST_TYPES and ref in (repo + '@' + target, image['ref']):
                accepted.add(ref)
            elif target == image['index_digest'] and media_type in registry.INDEX_TYPES and ref == repo + '@' + target and ref in advertised and expected['index'] is not None:
                content(target, expected['index'])
                accepted.add(ref)
            else:
                raise Refusal('CRI reference targets an unsupported or different typed image')
        binding = {'node_id': node_id, 'requested_ref': image['ref'], 'manifest_digest': image['digest'],
            'config_digest': image['config_digest'], 'os': image['os'], 'architecture': image['architecture'],
            'variant': image['variant'], 'accepted_image_ids': sorted(accepted)}
        self.registry_admit(ledger)
        return registry.validate_binding(binding, image, node_id)

    def pull(self):
        ledger = self.read()
        self.registry_admit(ledger)
        if ledger['phase'] != 'ready' or (self.directory / 'pulled-image-resolution-evidence.json').exists():
            raise Refusal('pull requires a fresh Ready registry run without a previous pull attempt')
        images = {image['role']: image for image in self.inventory()}
        evidence = self.pull_matrix(ledger, images, registry.PULL_ROLES, self.directory,
            'pulled-image-resolution-evidence.json', ledger['resource_lease']['expires_at'])
        self.registry_admit(ledger)
        evidence['complete'] = True
        self.write_private('pulled-image-resolution-evidence.json', registry.canonical(evidence) + b'\n')
        ledger['phase'] = 'pulled'
        self.save(ledger)

    def pull_matrix(self, ledger, images, roles, directory, name, deadline):
        proof = {'schema_version': 2, 'transport': 'public-registry', 'run_id': ledger['run_id'],
            'source_tree_sha256': self.source_tree(), 'image_lock_sha256': registry.sha(self.private_bytes('image-lock.json')),
            'nodes': ledger['nodes'], 'roles': list(roles), 'deadline': deadline,
            'monotonic_deadline': ledger['resource_lease']['monotonic_expires_at'], 'complete': False, 'pulls': [], 'node_bindings': {}}
        return self.perform_pulls(ledger, images, roles, directory, name, deadline, proof)

    def perform_pulls(self, ledger, images, roles, directory, name, deadline, proof, admit=None, monotonic_deadline=None):
        admit = (lambda: self.registry_admit(ledger)) if admit is None else admit
        lock = threading.Lock()
        def retain(exclusive=False):
            with lock:
                self._write_private_at(directory, name, registry.canonical(proof) + b'\n', exclusive=exclusive)
        retain(exclusive=True)
        pulled = set()
        def node_pull(role, image, node):
            admit()
            if time.time() >= deadline:
                raise Refusal('component original deadline expired')
            key = (node['id'], image['ref'])
            if key not in pulled:
                attempt = {'node_id': node['id'], 'requested_ref': image['ref'], 'status': 'pending'}
                with lock:
                    proof['pulls'].append(attempt)
                retain()
                try:
                    admit()
                    try:
                        self.registry_call(ledger, self.cri_prefix(node['id']) + ['pull', image['ref']], deadline=deadline, monotonic_deadline=monotonic_deadline, timeout=min(300, deadline - time.time()))
                    except Cancelled:
                        raise
                    except CommandError as error:
                        failure = error.failure_class if error.failure_class in ('CalledProcessError', 'TimeoutExpired',
                            'OSError', 'FileNotFoundError', 'PermissionError', 'UnicodeDecodeError') else 'CommandError'
                        returncode = error.returncode
                        if type(returncode) is not int or not -255 <= returncode <= 255:
                            returncode = None
                        with lock:
                            attempt.update(operation='cri-pull', error_class=failure, returncode=returncode)
                        raise Refusal(f'cri-pull failed (class={failure}, returncode={returncode})') from None
                    admit()
                    with lock:
                        attempt['status'] = 'verified'
                        pulled.add(key)
                except BaseException:
                    with lock:
                        attempt['status'] = 'failed'
                    retain()
                    raise
            binding = self._verify_pulled(image, node['id'], deadline, monotonic_deadline)
            with lock:
                proof['node_bindings'][role][node['name']] = binding
            retain()
        for role in roles:
            proof['node_bindings'][role] = {}
            self.each(lambda node, role=role: node_pull(role, images[role], node), ledger['nodes'])
        admit()
        if time.time() >= deadline:
            raise Refusal('component original deadline expired')
        return proof

    def pull_evidence(self, ledger, roles, directory, name, deadline=None):
        self.registry_admit(ledger)
        path = directory / name
        self.private(path)
        if not 0 < path.stat().st_size <= 1024 ** 2:
            raise Refusal('pull evidence size is invalid')
        raw = path.read_bytes()
        evidence = decode(raw, 'pull evidence')
        registry.closed(evidence, {'schema_version', 'transport', 'run_id', 'source_tree_sha256', 'image_lock_sha256',
            'nodes', 'roles', 'deadline', 'monotonic_deadline', 'complete', 'pulls', 'node_bindings'}, 'pull evidence')
        lease = ledger['resource_lease']
        if (evidence['schema_version'] != 2 or evidence['transport'] != 'public-registry' or evidence['run_id'] != ledger['run_id'] or
                evidence['source_tree_sha256'] != self.source_tree() or evidence['image_lock_sha256'] != registry.sha(self.private_bytes('image-lock.json')) or
                evidence['nodes'] != ledger['nodes'] or evidence['roles'] != list(roles) or evidence['complete'] is not True or
                type(evidence['deadline']) not in (float, int) or not time.time() < evidence['deadline'] <= lease['expires_at'] or
                evidence['monotonic_deadline'] != lease['monotonic_expires_at'] or
                deadline is not None and evidence['deadline'] != deadline):
            raise Refusal('complete current-source original-clock pull evidence is required')
        self.validate_matrix(ledger, roles, evidence['pulls'], evidence['node_bindings'])
        if path.read_bytes() != raw:
            raise Refusal('pull evidence changed during read-only verification')
        return evidence

    def validate_matrix(self, ledger, roles, pulls, node_bindings, deadline=None, monotonic_deadline=None):
        images = {image['role']: image for image in self.inventory()}
        expected = {(node['id'], images[role]['ref']) for role in roles for node in ledger['nodes']}
        if not isinstance(pulls, list) or len(pulls) != len(expected):
            raise Refusal('physical pull matrix is incomplete')
        seen = set()
        for pull in pulls:
            registry.closed(pull, {'node_id', 'requested_ref', 'status'}, 'pull attempt')
            pair = (pull['node_id'], pull['requested_ref'])
            if pair not in expected or pair in seen or pull['status'] != 'verified':
                raise Refusal('physical pull matrix is partial, duplicate or foreign')
            seen.add(pair)
        registry.closed(node_bindings, set(roles), 'logical role binding matrix')
        for role in roles:
            registry.closed(node_bindings[role], {node['name'] for node in ledger['nodes']}, 'node binding matrix')
        def verify_node(node):
            for role in roles:
                binding = node_bindings[role][node['name']]
                registry.validate_binding(binding, images[role], node['id'])
                if self._verify_pulled(images[role], node['id'], deadline, monotonic_deadline) != binding:
                    raise Refusal('current node-local binding differs from completed pull evidence')
        self.each(verify_node, ledger['nodes'])

    def pulled_image_binding(self, ledger, role):
        if ledger['phase'] not in ('pulled', 'verified') or role not in registry.PULL_ROLES:
            raise Refusal('complete pulled phase and selected legacy role are required')
        evidence = self.pull_evidence(ledger, registry.PULL_ROLES, self.directory, 'pulled-image-resolution-evidence.json')
        image = self.private_json('image-lock.json')['images'][role]
        return {'registry_index_digest': image['index_digest'], 'node_bindings': evidence['node_bindings'][role]}

    @staticmethod
    def valid_loaded_image_id(raw, node_name, image, node_names):
        bindings = image.get('node_bindings')
        if not isinstance(raw, str) or node_name not in node_names or not isinstance(bindings, dict) or set(bindings) != set(node_names):
            return False
        binding = bindings.get(node_name)
        return isinstance(binding, dict) and isinstance(binding.get('accepted_image_ids'), list) and raw in binding['accepted_image_ids']


    def baseline_provenance(self, lock, proof, receipts):
        originals = {kind: self.external_bytes(receipts / ('legacy-runner-original-' + kind + suffix), maximum=65536)
                     for kind, suffix in LEGACY_ORIGINALS}
        graph = decode(self.external_bytes(receipts / 'legacy-runner-registry-receipt.json'), 'legacy public graph')
        expected = self.legacy_receipt(lock, originals, graph)
        if proof != expected:
            raise Refusal('sanitized legacy executable receipt was not derived from the admitted original bytes')
        return expected

    def legacy_receipt(self, lock, originals, graph):
        catalog = decode((CONFIG / 'images.json').read_bytes(), 'static catalog')
        pin = catalog['legacy_qualification']
        fields = {'architecture', 'os', 'variant', 'runner_repository', 'runner_manifest_digest', 'runner_config_digest',
            'baseline_commit', 'builder_ref', 'binary_sha256', 'binary_size', 'binary_layer_digest',
            'original_evidence_sha256', 'original_qualification_sha256', 'original_probe_sha256'}
        registry.closed(pin, fields, 'pinned legacy qualification')
        image, seed = lock['images']['legacy-runner'], lock['images']['seed-tool-runner']
        if (pin['baseline_commit'] != '86bf908adca02d87063849dae775a02c1659c829' or pin['builder_ref'] != BASELINE_BUILDER or
                pin['architecture'] != 'arm64' or image['architecture'] != pin['architecture'] or
                any(image[k] != pin[p] for k, p in (('digest', 'runner_manifest_digest'), ('config_digest', 'runner_config_digest'),
                    ('os', 'os'), ('variant', 'variant'))) or registry.repository(image['ref']) != pin['runner_repository'] or
                any(seed[k] != image[k] for k in ('ref', 'digest', 'config_digest', 'architecture', 'os', 'variant'))):
            raise Refusal('legacy runner has no admitted immutable executable qualification for this platform')
        if set(originals) != {kind for kind, _ in LEGACY_ORIGINALS} or any(
                registry.sha(raw) != pin['original_' + kind + '_sha256'] for kind, raw in originals.items()):
            raise Refusal('original legacy qualification bytes differ from the admitted immutable evidence')
        evidence = decode(originals['evidence'], 'original legacy evidence')
        qualification = decode(originals['qualification'], 'original legacy qualification')
        actual = self.parse_runner_probe(originals['probe'].decode())
        member = evidence.get('runner_binary_member', {})
        if (qualification.get('actual') != actual or qualification.get('manifest_digest') != image['digest'] or
                qualification.get('image_id') != image['config_digest'] or qualification.get('architecture') != image['architecture'] or
                qualification.get('os') != 'linux' or qualification.get('builder_ref') != pin['builder_ref'] or
                registry.repository(qualification.get('ref')) != pin['runner_repository'] or
                qualification.get('trusted_builder_binary_sha256') != pin['binary_sha256'] or
                any(qualification.get(k) is not v for k, v in {'nonroot': True, 'read_only': True, 'host_mounts': False,
                    'source_pin_changed': False, 'signature_verified': False, 'runtime_migration_qualified': False,
                    'cleanup_removed_and_absence_proven': True}.items()) or qualification.get('network') != 'none' or
                not IDENTITY.fullmatch(qualification.get('container_id', '')) or
                evidence.get('builder_ref') != pin['builder_ref'] or evidence.get('binary_sha256') != pin['binary_sha256'] or
                evidence.get('qualification_sha256') != pin['original_qualification_sha256'] or
                member != {'member': 'usr/lib/postgresql/18/bin/pgcopydb', 'layer_digest': pin['binary_layer_digest'],
                    'sha256': pin['binary_sha256'], 'size': pin['binary_size']} or
                actual.get('uid') != '65532' or actual.get('gid') != '65532' or actual.get('workdir') != '/work' or
                actual.get('pgcopydb_version') != 'pgcopydb version 0.18.144.g86bf908' or actual.get('binary_sha256') != pin['binary_sha256'] or
                actual.get('sed_escape') != 'a\\:b' or actual.get('compare_progress_marker') != 'present' or
                any(not actual.get(tool + '_version') for tool in ('psql', 'pg_dump', 'pg_restore', 'timeout', 'sed')) or
                any(actual.get(tool + '_path') != '/usr/lib/postgresql/18/bin/' + tool for tool in ('pgcopydb', 'psql', 'pg_dump', 'pg_restore'))):
            raise Refusal('legacy executable, utility or cleanup invariants differ from the admitted evidence')
        metadata = registry.validate_graph(image, graph, False)
        if pin['binary_layer_digest'] not in {layer['digest'] for layer in metadata['layers']}:
            raise Refusal('fresh public runner graph does not contain the admitted executable layer')
        expected = {'schema_version': 1, 'role': 'legacy-runner', 'ref': image['ref'], 'manifest_digest': image['digest'],
            'config_digest': image['config_digest'], 'os': image['os'], 'architecture': image['architecture'], 'variant': image['variant'],
            **{k: pin[k] for k in ('baseline_commit', 'builder_ref', 'binary_sha256', 'binary_size', 'binary_layer_digest',
                'original_evidence_sha256', 'original_qualification_sha256', 'original_probe_sha256')},
            'actual': actual, 'execution': {'nonroot': True, 'read_only': True, 'network': 'none', 'host_mounts': False,
                'source_pin_changed': False, 'runtime_migration_qualified': False, 'cleanup_removed_and_absence_proven': True}}
        return expected

    @staticmethod
    def suite_environment(selection):
        if selection['engine'] == 'pgcopydb':
            return [('MANAGER', 'manager'), ('RUNNER', 'legacy-runner'), ('SEED', 'seed-tool-runner'),
                    ('TOOL', 'seed-tool-runner'), ('COLLECTOR', 'otel-collector'), ('POSTGRES_17', 'postgresql-17')]
        majors = sorted(set(selection['pair'].split(':')))
        return [('MANAGER', 'manager'), ('RUNNER', 'native-runner'), ('SEED', 'seed-tool-runner'), ('TOOL', 'seed-tool-runner'),
                ('COLLECTOR', 'otel-collector'), *[('POSTGRES_' + major, 'postgresql-' + major) for major in majors]]

    def suite_projection(self, ledger, selection):
        self.require_prepared(ledger)
        if ledger['phase'] not in ('pulled', 'verified'):
            raise Refusal('suite projection requires complete existing pulled evidence')
        evidence = self.pull_evidence(ledger, registry.PULL_ROLES, self.directory, 'pulled-image-resolution-evidence.json')
        locked, images = self.private_json('image-lock.json')['images'], {}
        for role in sorted({role for _, role in self.suite_environment(selection)}):
            image = locked[role]
            images[role] = {key: image[key] for key in ('ref', 'digest', 'architecture')}
            images[role].update(image_id=image['config_digest'], loaded_digest=image['digest'],
                registry_index_digest=image['index_digest'], node_bindings=evidence['node_bindings'][role])
        return {'schema_version': 2, 'run_id': ledger['run_id'], 'context': ledger['context'],
            'source_tree_sha256': self.source_tree(), 'image_lock_sha256': registry.sha(self.private_bytes('image-lock.json')),
            'images': images, 'engine': selection['engine'], 'pair': selection['pair'], 'spec': selection['spec'],
            'lease_expires_at': ledger['resource_lease']['expires_at']}

    def suite_input(self, ledger, selection):
        data = self.suite_projection(ledger, selection)
        self.write_private('suite-input.json', registry.canonical(data) + b'\n')
        return data

    def _component_context(self, qual_dir, packet, deadline):
        qual = Path(qual_dir)
        if (not qual.is_absolute() or qual.resolve() != qual or qual == self.directory or qual.is_relative_to(self.directory) or
                self.directory.is_relative_to(qual) or any((p / '.git').exists() or (p / '.git').is_symlink() for p in (qual, *qual.parents))):
            raise Refusal('component qualification directory must be separate, canonical and outside Git')
        self.private(qual, directory=True)
        raw = self.external_bytes(qual / 'native-assets.json', maximum=1024 ** 2)
        fields = set('schema_version qualification_id run_id legacy_source_root legacy_source_tree_sha256 legacy_guard_sha256 '
            'legacy_ownership_sha256 qualification_source_sha256 evidence_program_sha256 image_lock_sha256 images'.split())
        registry.closed(packet, fields, 'component packet')
        if (decode(raw, 'component packet') != packet or type(packet['schema_version']) is not int or packet['schema_version'] != 2 or
                not isinstance(packet['qualification_id'], str) or not re.fullmatch('[0-9a-f]{32}', packet['qualification_id']) or
                any(not isinstance(packet[k], str) or not IDENTITY.fullmatch(packet[k]) for k in fields -
                    {'schema_version', 'qualification_id', 'run_id', 'legacy_source_root', 'images'})):
            raise Refusal('component packet fields or exact private bytes are invalid')
        ledger = self.read()
        self.registry_admit(ledger)
        ownership = self.private_bytes('ownership.json')
        if (packet['run_id'] != ledger['run_id'] or packet['legacy_source_root'] != str(ROOT) or
                packet['legacy_source_tree_sha256'] != self.source_tree() or
                packet['legacy_guard_sha256'] != registry.sha(Path(__file__).read_bytes()) or
                packet['legacy_ownership_sha256'] != registry.sha(ownership) or
                packet['image_lock_sha256'] != registry.sha(self.private_bytes('image-lock.json')) or ledger['phase'] not in ('pulled', 'verified')):
            raise Refusal('component packet belongs to a different RUN, source or image lock')
        lock = self.private_json('image-lock.json')
        registry.closed(packet['images'], {'native-runner', 'postgresql-16', 'postgresql-18'}, 'component image roles')
        for role, entry in packet['images'].items():
            registry.closed(entry, {'image'}, 'component resolved image')
            if entry['image'] != lock['images'][role]:
                raise Refusal('component image differs from the admitted complete lock')
        self.pulled_image_binding(ledger, 'manager')
        if type(deadline) not in (int, float) or not math.isfinite(deadline) or not time.time() < deadline <= ledger['resource_lease']['expires_at']:
            raise Refusal('component stage deadline is invalid or expired')
        context = {'qual': qual, 'packet': packet, 'raw': raw, 'ledger': ledger, 'ownership': ownership,
            'deadline': deadline, 'stage_monotonic_deadline': min(ledger['resource_lease']['monotonic_expires_at'], time.monotonic() + deadline - time.time()), 'images': {role: entry['image'] for role, entry in packet['images'].items()}}
        self._component_admit(context)
        return context

    def _component_admit(self, context):
        ledger, deadline = self.read(), context['deadline']
        if (type(deadline) not in (int, float) or not math.isfinite(deadline) or not time.time() < deadline <= ledger['resource_lease']['expires_at'] or
                time.monotonic() >= context['stage_monotonic_deadline'] or self.private_bytes('ownership.json') != context['ownership'] or
                self.external_bytes(context['qual'] / 'native-assets.json', maximum=1024 ** 2) != context['raw']):
            raise Refusal('component original deadline, packet or ownership changed')
        self.registry_admit(ledger)
        return ledger

    def component_evidence(self, context):
        packet, ledger = context['packet'], context['ledger']
        return {'schema_version': 2, 'transport': 'public-registry', 'qualification_id': packet['qualification_id'],
            'run_id': ledger['run_id'], 'native_assets_sha256': registry.sha(context['raw']), 'image_lock_sha256': packet['image_lock_sha256'],
            'legacy_source_tree_sha256': packet['legacy_source_tree_sha256'], 'legacy_guard_sha256': packet['legacy_guard_sha256'],
            'legacy_ownership_sha256': registry.sha(context['ownership']), 'qualification_source_sha256': packet['qualification_source_sha256'],
            'deadline': context['deadline'], 'monotonic_deadline': ledger['resource_lease']['monotonic_expires_at'],
            'complete': False, 'pulls': [], 'node_bindings': {}}

    def qualify_component_images(self, qual_dir, packet, deadline):
        context = self._component_context(qual_dir, packet, deadline)
        proof = self.component_evidence(context)
        self.perform_pulls(context['ledger'], context['images'], ('native-runner', 'postgresql-16', 'postgresql-18'),
            context['qual'], 'component-image-resolution-evidence.json', deadline, proof, lambda: self._component_admit(context), context['stage_monotonic_deadline'])
        self.pulled_image_binding(context['ledger'], 'manager')
        self._component_admit(context)
        proof['complete'] = True
        self._write_private_at(context['qual'], 'component-image-resolution-evidence.json', registry.canonical(proof) + b'\n')
        return proof

    def verify_component_images(self, qual_dir, packet, deadline):
        context = self._component_context(qual_dir, packet, deadline)
        raw = self.external_bytes(context['qual'] / 'component-image-resolution-evidence.json', maximum=1024 ** 2)
        proof = decode(raw, 'component image resolution')
        expected = self.component_evidence(context)
        registry.closed(proof, set(expected), 'component resolution')
        for key, value in expected.items():
            if key not in ('complete', 'pulls', 'node_bindings', 'deadline') and proof[key] != value:
                raise Refusal('component resolution source, original clock or packet binding differs')
        if (type(proof['deadline']) not in (int, float) or not math.isfinite(proof['deadline']) or
                not 0 < proof['deadline'] <= context['ledger']['resource_lease']['expires_at'] or proof['complete'] is not True):
            raise Refusal('component pull matrix is incomplete')
        self.validate_matrix(context['ledger'], ('native-runner', 'postgresql-16', 'postgresql-18'), proof['pulls'], proof['node_bindings'], deadline, context['stage_monotonic_deadline'])
        self.pulled_image_binding(context['ledger'], 'manager')
        self._component_admit(context)
        if self.external_bytes(context['qual'] / 'component-image-resolution-evidence.json') != raw:
            raise Refusal('component evidence changed during read-only verification')
        return proof

    def storage_outputs(self, lock):
        catalog = decode((CONFIG / 'images.json').read_bytes(), 'static catalog')
        pin = catalog['storage_manifest']
        registry.closed(pin, {'file', 'sha256', 'node_index_digest', 'node_manifest_digest', 'node_layer_digest'}, 'storage source pin')
        if (pin['file'] != 'default-storage.yaml' or pin['sha256'] != 'dc16a44e6a49a1f26fc39f341c361e7939993bd68431efc3a49d216e5914e903' or
                pin['node_index_digest'] != lock['images']['node']['index_digest'] or pin['node_manifest_digest'] != lock['images']['node']['digest'] or
                not isinstance(pin['node_layer_digest'], str) or not DIGEST.fullmatch(pin['node_layer_digest'])):
            raise Refusal('storage source is not bound to the approved node image')
        path = CONFIG / pin['file']
        if not path.is_file() or path.is_symlink() or path.resolve() != path or path.stat().st_nlink != 1:
            raise Refusal('sealed storage source is unavailable or linked')
        raw = path.read_bytes()
        if registry.sha(raw) != pin['sha256'] or len(raw) != 4567:
            raise Refusal('sealed storage source bytes changed')
        expected = {('v1', 'Namespace', '', 'local-path-storage'),
            ('v1', 'ServiceAccount', 'local-path-storage', 'local-path-provisioner-service-account'),
            ('rbac.authorization.k8s.io/v1', 'Role', 'local-path-storage', 'local-path-provisioner-role'),
            ('rbac.authorization.k8s.io/v1', 'ClusterRole', '', 'local-path-provisioner-role'),
            ('rbac.authorization.k8s.io/v1', 'RoleBinding', 'local-path-storage', 'local-path-provisioner-bind'),
            ('rbac.authorization.k8s.io/v1', 'ClusterRoleBinding', '', 'local-path-provisioner-bind'),
            ('apps/v1', 'Deployment', 'local-path-storage', 'local-path-provisioner'),
            ('storage.k8s.io/v1', 'StorageClass', 'kube-system', 'standard'),
            ('v1', 'ConfigMap', 'local-path-storage', 'local-path-config')}
        identities = []
        text = raw.decode()
        for document in text.split('\n---\n'):
            version = re.findall(r'^apiVersion: ([^\n]+)$', document, re.M)
            kind = re.findall(r'^kind: ([^\n]+)$', document, re.M)
            metadata = re.findall(r'^metadata:\n((?:  [^\n]*\n)+)', document, re.M)
            if len(version) != 1 or len(kind) != 1 or len(metadata) != 1:
                raise Refusal('sealed storage source document framing differs')
            name = re.findall(r'^  name: ([^\n]+)$', metadata[0], re.M)
            namespace = re.findall(r'^  namespace: ([^\n]+)$', metadata[0], re.M)
            if len(name) != 1 or len(namespace) > 1:
                raise Refusal('sealed storage metadata identity differs')
            identities.append((version[0], kind[0], namespace[0] if namespace else '', name[0]))
        if len(identities) != 9 or set(identities) != expected:
            raise Refusal('sealed storage source does not contain the exact nine resources')
        for role, count in (('storage-provisioner', 1), ('storage-helper', 2)):
            origin = lock['images'][role]['provenance']['origin_ref']
            if text.count(origin) != count:
                raise Refusal('sealed storage public source image references differ')
            text = text.replace(origin, lock['images'][role]['ref'])
        rendered = text.encode()
        receipt = {'schema_version': 2, 'node_manifest_digest': lock['images']['node']['digest'],
            'source_sha256': registry.sha(raw), 'rendered_sha256': registry.sha(rendered),
            'images': {r: lock['images'][r] for r in ('storage-helper', 'storage-provisioner')}}
        return {'node-default-storage-source.yaml': raw, 'node-default-storage.yaml': rendered,
            'storage-registry-receipt.json': registry.canonical(receipt) + b'\n'}

    def shared_storage(self):
        path = self.directory / 'node-default-storage.yaml'
        self.private(path)
        digest = registry.sha(self.private_bytes('node-default-storage.yaml'))
        if (self.vm(['realpath', '-e', '--', str(path)]).strip() != str(path) or
                self.vm(['sha256sum', '--', str(path)]).strip() != digest + '  ' + str(path)):
            raise Refusal('private rendered storage path is not shared with the VM at its exact identity')

    def verify_storage_mount(self, ledger):
        nodes = [node for node in ledger['nodes'] if node['role'] == 'control-plane']
        if len(nodes) != 1:
            raise Refusal('storage requires the single exact owned control plane')
        node = nodes[0]
        values = decode(self.call(['podman', 'inspect', '--type', 'container', node['id']]), 'storage mount inspection')
        if not isinstance(values, list) or len(values) != 1 or not isinstance(values[0], dict) or values[0].get('Id') != node['id']:
            raise Refusal('storage mount node identity is unavailable')
        mounts = values[0].get('Mounts')
        if not isinstance(mounts, list) or any(not isinstance(mount, dict) for mount in mounts):
            raise Refusal('storage mount inventory is unavailable')
        selected = [mount for mount in mounts if mount.get('Destination') == '/kind/manifests/default-storage.yaml']
        path = self.directory / 'node-default-storage.yaml'
        self.private(path)
        if (len(selected) != 1 or selected[0].get('Type') != 'bind' or selected[0].get('Source') != str(path) or selected[0].get('RW') is not False or
                self.call(['podman', 'exec', node['id'], 'cat', '/kind/manifests/default-storage.yaml']).encode() != self.private_bytes('node-default-storage.yaml')):
            raise Refusal('storage bootstrap must use the sealed read-only digest-rendered manifest mount')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'check', 'up', 'run', 'down', 'pull', 'self-check'])
    parser.add_argument('--run-dir', type=Path)
    parser.add_argument('--lease-seconds', type=int)
    parser.add_argument('--image-lock', type=Path)
    parser.add_argument('--receipt-dir', type=Path)
    parser.add_argument('--engine', choices=['pgcopydb', 'rpgmigrate'], default='pgcopydb')
    parser.add_argument('--pair', choices=['17:17', *NATIVE_PAIRS])
    parser.add_argument('--case', choices=sorted(NATIVE_SPECS))
    args = parser.parse_args()
    if args.command == 'self-check':
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('test_e2e_kind.py'))], check=False)
        return result.returncode
    if args.command == 'prepare' and (args.image_lock is None or args.receipt_dir is None):
        parser.error('prepare requires --image-lock and --receipt-dir')
    if args.command != 'prepare' and (args.image_lock is not None or args.receipt_dir is not None):
        parser.error('--image-lock and --receipt-dir are only valid with prepare')
    if args.command == 'up' and args.lease_seconds is None:
        parser.error('up requires --lease-seconds')
    if args.command != 'up' and args.lease_seconds is not None:
        parser.error('--lease-seconds is only valid with up')
    if args.run_dir is None and args.command == 'prepare':
        args.run_dir = EVIDENCE_ROOT / ('rpgmigrate-m7-run-' + uuid.uuid4().hex)
    if args.run_dir is None:
        parser.error('--run-dir is required')
    try:
        if args.command != 'prepare' and not args.run_dir.exists():
            raise Refusal('existing owned RUN_DIR is required')
        guard = Guard(args.run_dir)
        lock = guard.directory / 'lifecycle.lock'
        fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w') as stream:
            guard.private(lock)
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise Refusal('another lifecycle command owns RUN_DIR') from None
            if args.command in ('up', 'pull', 'run'):
                guard.hold_awake()
            if args.command == 'prepare':
                guard.prepare(args.image_lock, args.receipt_dir)
            elif args.command == 'up':
                guard.up(args.lease_seconds)
            elif args.command == 'run':
                guard.run(args.engine, args.pair, args.case)
            else:
                getattr(guard, args.command)()
        print(json.dumps({'command': args.command, 'result': 'passed', 'run_dir': str(guard.directory)}))
        return 0
    except Exception as error:
        message = str(error) if isinstance(error, Refusal) else type(error).__name__
        print(json.dumps({'command': args.command, 'result': 'refused', 'error': message}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())

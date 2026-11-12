#!/usr/bin/env python3
"""Test-only packaged runtime observations, separate from legacy and production authority."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time
import uuid

sys.dont_write_bytecode = True

ROOT = Path(__file__).resolve().parents[1]
FILE_LIMIT = 1048576
ROLES = ('native-runner', 'postgresql-16', 'postgresql-18')
TEST = 'TestNativePackagedRuntimeQualification'
PASSWORD_TEST = 'TestNativePackagedPasswordTLSProbe'
PASSWORD_SELECTOR = 'RPGMIGRATE_NATIVE_PASSWORD_TLS_INPUT'
PASSWORD_FILE = 'internal/controller/native_runtime_password_tls_probe_test.go'
DELIVERY_TEST = 'TestNativePackagedDeliveryQualification'
DELIVERY_SELECTOR = 'RPGMIGRATE_NATIVE_DELIVERY_INPUT'
DELIVERY_FILE = 'internal/controller/native_runtime_delivery_test.go'
DELIVERY_CASES = ('quiet_success', 'concurrent_relay_refused', 'foreign_lock_refused', 'truncated_result_refused',
                  'stderr_refused', 'startup_log_refused', 'main_failure_refused', 'disconnect_no_replay')
SELECTOR = 'RPGMIGRATE_NATIVE_RUNTIME_INPUT'
OWNED = ('hack/native-runtime-qualification.py', 'hack/test_native_runtime_qualification.py',
         'internal/controller/native_runtime_qualification_test.go')
DIGEST = re.compile(r'sha256:[0-9a-f]{64}')
HEX = re.compile(r'[0-9a-f]{64}')
METADATA_START = 'const nativeRuntimeMetadataProgram = `'
REQUEST_KEYS = set('schema_version qualification_id phase pair run_id run_dir legacy_source_root '
                   'legacy_source_tree_sha256 legacy_guard_sha256 legacy_ownership_sha256 '
                   'qualification_source_sha256 evidence_program_sha256 native_assets_sha256 '
                   'kubeconfig context namespace namespace_uid nodes lease_expires_at stage_deadline '
                   'required_cases receipt_path metadata_program_sha256'.split())
RECEIPT_KEYS = set('schema_version qualification_id phase pair run_id input_sha256 '
                   'qualification_source_sha256 evidence_program_sha256 native_assets_sha256 '
                   'metadata_program_sha256 metadata_sha256 namespace_uid started_at finished_at cases cleanup'.split())
PREPARATION_KEYS = set('schema_version qualification_id run_id legacy_source_root '
                       'legacy_source_tree_sha256 legacy_guard_sha256 legacy_ownership_sha256 '
                       'qualification_source_sha256 evidence_program_sha256 image_lock_sha256 images'.split())
STAGE_BUDGETS = {'qualify': (900, 2400), 'verify': (600, 1800), 'go': (900, 900),
                 'final_verify': (600, 300), 'readback': (300, 0)}


class Refusal(Exception):
    pass


def closed(value, keys, what):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise Refusal(what + ' fields differ from the closed schema')
    return value


def decode(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError('duplicate')
            value[key] = item
        return value
    try:
        return json.loads(raw, object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError('constant')))
    except (UnicodeError, ValueError, TypeError):
        raise Refusal('invalid closed JSON') from None


def read_file(path, *, private=False):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or
                    info.st_size > FILE_LIMIT or path.resolve() != path or
                    private and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600)):
                raise Refusal('private or source file identity, mode, or size differs')
            raw = stream.read(FILE_LIMIT + 1)
            identity = lambda value: (value.st_dev, value.st_ino, value.st_mode, value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
            if len(raw) > FILE_LIMIT or identity(os.fstat(stream.fileno())) != identity(info) or identity(path.stat()) != identity(info):
                raise Refusal('private or source file changed during read')
            return raw
    except OSError:
        raise Refusal('private or source file unavailable') from None


def read_private(path):
    return read_file(path, private=True)


def private_directory(path, *, create=False):
    if not path.is_absolute() or path.resolve() != path:
        raise Refusal('private directory must be absolute without links')
    if any((ancestor / '.git').exists() or (ancestor / '.git').is_symlink() for ancestor in (path, *path.parents)):
        raise Refusal('private output must be outside Git')
    if create and not path.exists():
        path.mkdir(mode=0o700)
    try:
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise Refusal('private directory mode or ownership differs')
    except OSError:
        raise Refusal('private directory unavailable') from None


def write_private(path, value):
    raw = (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()
    if len(raw) > FILE_LIMIT:
        raise Refusal('private JSON size limit')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return raw


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def required_cases(phase, pair):
    if phase not in ('prepare', 'metadata', 'password_tls_probe', 'delivery', 'connections', 'evidence'):
        raise Refusal('unknown phase')
    if (phase in ('connections', 'evidence')) != (pair is not None):
        raise Refusal('pair is required only for connection and evidence phases')
    if pair is not None and pair not in ((16, 16), (18, 18), (16, 18)):
        raise Refusal('unselected pair')
    if phase == 'metadata':
        return [f'node_{node}/{case}' for node in range(3)
                for case in ('runtime', 'resolver', 'home', 'provider')]
    if phase == 'prepare':
        return ['direct-pull/' + role for role in ROLES]
    if phase == 'password_tls_probe':
        return [f'pg{major}/{case}' for major in (16, 18) for case in (
            'good_scram_verify_full', 'wrong_password_refused', 'hostname_mismatch_refused',
            'sealed_ca_identity', 'private_closure', 'no_sensitive_persistence')]
    if phase == 'delivery':
        return ['delivery/' + case for case in DELIVERY_CASES] + ['pvc/fsgroup_readonly_remount']
    raise Refusal('missing production policy and actual producer fixtures')


def readonly_guard_type(base):
    class ReadOnlyLegacyGuard(base):
        def save(self, ledger):
            expected = (json.dumps(ledger, indent=2) + '\n').encode()
            if read_private(self.directory / 'ownership.json') != expected:
                raise Refusal('legacy validation would write; required normal M7 CHECK')
    return ReadOnlyLegacyGuard


def load_guard(source_root, run_dir):
    if not source_root.is_absolute() or source_root.resolve() != source_root:
        raise Refusal('legacy source must be absolute without links')
    path = source_root / 'hack/e2e-kind.py'
    raw_source = read_file(path)
    spec = importlib.util.spec_from_file_location('native_qualification_legacy_guard', path)
    module = importlib.util.module_from_spec(spec)
    exec(compile(raw_source, str(path), 'exec'), module.__dict__)
    module._qualification_source_sha256 = sha(raw_source)
    if sha(read_file(path)) != module._qualification_source_sha256:
        raise Refusal('legacy Guard changed during import')
    if module.ROOT != source_root:
        raise Refusal('legacy Guard source root differs')
    return module, readonly_guard_type(module.Guard)(run_dir)


@contextmanager
def legacy_lock(guard):
    path = guard.directory / 'lifecycle.lock'
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        guard.private(path)
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Refusal('legacy preparation or lifecycle owner is active') from None
        yield


def source_identity():
    names = set(OWNED) | {'go.mod', 'go.sum', 'Taskfile.yml', 'internal/controller/native_worker_test.go',
                          'internal/controller/native_preflight_test.go', 'internal/controller/native_lifecycle_test.go'}
    names.update((PASSWORD_FILE, DELIVERY_FILE))
    for subtree in ('internal', 'api', 'cmd'):
        names.update(str(path.relative_to(ROOT)) for path in (ROOT / subtree).rglob('*.go')
                     if not path.name.endswith('_test.go'))
    names.update(str(path.relative_to(ROOT)) for path in (ROOT / 'internal/native').glob('*.py')
                 if not path.name.endswith('_test.py'))
    if not names or len(names) > 128:
        raise Refusal('qualification source path cap')
    inventory = [[name, sha(read_file(ROOT / name))] for name in sorted(names)]
    fixture = read_file(ROOT / OWNED[2]).decode()
    if fixture.count(METADATA_START) != 1:
        raise Refusal('fixed metadata program source unavailable')
    program, separator, _ = fixture.split(METADATA_START, 1)[1].partition('`')
    if not separator or not program or len(program.encode()) > 122880:
        raise Refusal('metadata program source size')
    marker = 'const nativeRuntimeNodeProgram = `'
    if fixture.count(marker) != 1:
        raise Refusal('fixed active profile program unavailable')
    node, separator, _ = fixture.split(marker, 1)[1].partition('`')
    if not separator or not node or len(node.encode()) > 122880:
        raise Refusal('active profile program source size')
    return (sha(json.dumps(inventory, separators=(',', ':')).encode()),
            sha(read_file(ROOT / 'internal/native/evidence_program.py')), sha(program.encode() + b'\0' + node.encode()))


def validate_inventory(inventory):
    closed(inventory, set('schema_version transport operator_source engine_source images publication_receipts_sha256'.split()),
           'public image lock')
    if (type(inventory['schema_version']) is not int or inventory['schema_version'] != 2 or
            inventory['transport'] != 'public-registry'):
        raise Refusal('native public image lock version or transport')
    all_roles = set('node cnpg-controller postgresql-16 postgresql-17 postgresql-18 manager legacy-runner '
                    'native-runner seed-tool-runner otel-collector storage-helper storage-provisioner'.split())
    locked = closed(inventory['images'], all_roles, 'public image roles')
    for name in ('operator_source', 'engine_source'):
        source = closed(inventory[name], set('repository commit git_tree payload_inventory_sha256'.split()), 'lock source')
        if (not isinstance(source['repository'], str) or not source['repository'] or
                any(not isinstance(source[key], str) or not re.fullmatch('[0-9a-f]{40}', source[key])
                    for key in ('commit', 'git_tree')) or
                not isinstance(source['payload_inventory_sha256'], str) or not HEX.fullmatch(source['payload_inventory_sha256'])):
            raise Refusal('public image source binding missing')
    publication = closed(inventory['publication_receipts_sha256'], {'manager', 'native-runner'}, 'publication receipts')
    if any(not isinstance(value, str) or not HEX.fullmatch(value) for value in publication.values()):
        raise Refusal('public development publication receipt missing')
    images = {role: locked[role] for role in ROLES}
    basic = set('role ref os architecture variant digest index_digest config_digest provenance'.split())
    for role, image in images.items():
        closed(image, basic | ({'source'} if role == 'native-runner' else set()), 'native public image')
        repository = ('ghcr.io/ydixken/rpgmigrate' if role == 'native-runner' else
                      'ghcr.io/cloudnative-pg/postgresql:' + role.rsplit('-', 1)[1])
        if (image['role'] != role or image['os'] != 'linux' or image['architecture'] != 'arm64' or
                image['variant'] not in (None, 'v8') or
                any(not isinstance(image[key], str) or not DIGEST.fullmatch(image[key])
                    for key in ('digest', 'config_digest')) or image['digest'] == image['config_digest'] or
                image['ref'] != repository + '@' + image['digest'] or
                image['index_digest'] is not None and
                (not isinstance(image['index_digest'], str) or not DIGEST.fullmatch(image['index_digest']) or
                 image['index_digest'] in (image['digest'], image['config_digest'])) or
                role == 'native-runner' and image['index_digest'] is not None):
            raise Refusal('native public selected manifest, config or platform differs')
        provenance = closed(image['provenance'], set('origin_ref registry_receipt_sha256 public_receipt_sha256'.split()),
                            'public image provenance')
        if (not isinstance(provenance['origin_ref'], str) or not provenance['origin_ref'] or
                any(not isinstance(provenance[key], str) or not HEX.fullmatch(provenance[key])
                    for key in ('registry_receipt_sha256', 'public_receipt_sha256'))):
            raise Refusal('independent public registry provenance missing')
        if role == 'native-runner':
            source = closed(image['source'], set('repository commit git_tree payload_inventory_sha256 '
                            'build_input_sha256 executable_sha256 smoke_receipt_sha256'.split()), 'native publication source')
            if (any(source[key] != value for key, value in inventory['engine_source'].items()) or
                    any(not isinstance(source[key], str) or not HEX.fullmatch(source[key])
                        for key in ('build_input_sha256', 'executable_sha256', 'smoke_receipt_sha256'))):
                raise Refusal('native public engine source/build/smoke binding missing')
    return images


def prepare_assets(guard):
    raw = read_private(guard.directory / 'image-lock.json')
    lock = decode(raw)
    images = validate_inventory(lock)
    entries = guard.inventory()
    admitted = {image['role']: image for image in entries}
    if (len(entries) != len(admitted) or admitted != lock['images'] or
            lock['operator_source']['payload_inventory_sha256'] != guard.source_tree()):
        raise Refusal('prepared public image lock or clean source changed')
    return sha(raw), {role: {'image': image} for role, image in images.items()}


def stage_deadline(expiry, now=None, *, parent_deadline=None,
                   parent_monotonic_deadline=None, monotonic_now=None, stage='go'):
    now = time.time() if now is None else now
    monotonic_now = time.monotonic() if monotonic_now is None else monotonic_now
    if (any(type(value) not in (int, float) or not math.isfinite(value)
            for value in (expiry, parent_deadline, parent_monotonic_deadline, now, monotonic_now)) or
            stage not in STAGE_BUDGETS):
        raise Refusal('invalid original clocks or stage cleanup budget')
    budget, reserve = STAGE_BUDGETS[stage]
    remaining = min(expiry - now, parent_deadline - now, parent_monotonic_deadline - monotonic_now)
    if remaining < budget + reserve:
        raise Refusal('insufficient original lease and cleanup budget')
    return now + min(budget, remaining)


@contextmanager
def stage_limit(clocks, stage):
    now, mono = time.time(), time.monotonic()
    deadline = stage_deadline(clocks[0], now, parent_deadline=clocks[1],
                              parent_monotonic_deadline=clocks[2], monotonic_now=mono, stage=stage)
    mono_deadline = mono + deadline - now
    if signal.getitimer(signal.ITIMER_REAL) != (0., 0.):
        raise Refusal('existing host watchdog cannot be replaced')
    def expired(_signal, _frame):
        raise Refusal('fixed host stage deadline expired')
    handler = signal.signal(signal.SIGALRM, expired)
    try:
        remaining = min(deadline - time.time(), mono_deadline - time.monotonic())
        if remaining <= 0:
            raise Refusal('fixed host stage deadline expired')
        signal.setitimer(signal.ITIMER_REAL, remaining)
        yield deadline, mono_deadline
        if time.time() >= deadline or time.monotonic() >= mono_deadline:
            raise Refusal('fixed host stage deadline expired')
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, handler)


def go_limits(clocks, wall_stop, mono_stop, *, now=None, monotonic_now=None):
    now = time.time() if now is None else now
    mono = time.monotonic() if monotonic_now is None else monotonic_now
    timeout = min(900, wall_stop - now, mono_stop - mono,
                  clocks[0] - now, clocks[1] - now, clocks[2] - mono)
    if timeout <= 601:
        raise Refusal('insufficient Go work and cleanup budget')
    return now + min(299, timeout - 601), timeout


def cleanup_valid(cleanup, namespace_uid, namespace=None):
    closed(cleanup, {'complete', 'objects'}, 'cleanup')
    objects = cleanup['objects']
    if cleanup['complete'] is not True or not isinstance(objects, list) or len(objects) != 7:
        raise Refusal('all three Jobs, Pods and namespace require verified cleanup')
    seen = set()
    for item in objects:
        closed(item, {'api_version', 'kind', 'namespace', 'name', 'uid', 'action', 'verified_absent'}, 'cleanup object')
        if any(not isinstance(item[key], str) for key in ('kind', 'namespace', 'name', 'uid')):
            raise Refusal('cleanup identity type differs')
        identity = (item['kind'], item['namespace'], item['name'], item['uid'])
        if (identity in seen or item['api_version'] != ('batch/v1' if item['kind'] == 'Job' else 'v1') or item['kind'] not in ('Pod', 'Job', 'Namespace') or
                not all(isinstance(item[key], str) and item[key] for key in ('name', 'uid')) or
                item['action'] != 'deleted' or item['verified_absent'] is not True or
                item['kind'] == 'Namespace' and (item['uid'] != namespace_uid or item['namespace'] != '') or
                namespace is not None and (item['name'] if item['kind'] == 'Namespace' else item['namespace']) != namespace):
            raise Refusal('cleanup identity or verified absence missing')
        seen.add(identity)
    if {kind: sum(item['kind'] == kind for item in objects) for kind in ('Namespace', 'Job', 'Pod')} != {'Namespace': 1, 'Job': 3, 'Pod': 3}:
        raise Refusal('cleanup namespace binding missing')


def validate_receipt(request, receipt, events):
    closed(request, REQUEST_KEYS, 'metadata request')
    closed(receipt, RECEIPT_KEYS, 'metadata receipt')
    expected = required_cases(request['phase'], request['pair'])
    if type(receipt['schema_version']) is not int or request['required_cases'] != expected or request['phase'] != 'metadata':
        raise Refusal('required cases differ from fixed phase inventory')
    for key in ('schema_version', 'qualification_id', 'phase', 'pair', 'run_id',
                'qualification_source_sha256', 'evidence_program_sha256', 'native_assets_sha256',
                'metadata_program_sha256'):
        if receipt[key] != request[key]:
            raise Refusal('stale receipt source or invocation binding')
    if (receipt['input_sha256'] != sha((json.dumps(request, sort_keys=True, separators=(',', ':')) + '\n').encode()) or
            not isinstance(receipt['metadata_sha256'], str) or not HEX.fullmatch(receipt['metadata_sha256']) or
            not isinstance(receipt['namespace_uid'], str) or not receipt['namespace_uid'] or
            request['namespace_uid'] is not None and request['namespace_uid'] != receipt['namespace_uid']):
        raise Refusal('metadata receipt input or namespace binding')
    if (any(type(receipt[key]) not in (int, float) or not math.isfinite(receipt[key])
            for key in ('started_at', 'finished_at')) or
            not request['stage_deadline'] - 600 <= receipt['started_at'] <= receipt['finished_at'] <= min(request['stage_deadline'] + 600, request['lease_expires_at'])):
        raise Refusal('stale receipt outside original stage deadline')
    cases = receipt['cases']
    if not isinstance(cases, list) or [item.get('name') if isinstance(item, dict) else None for item in cases] != expected:
        raise Refusal('missing or empty required cases')
    for item in cases:
        closed(item, {'name', 'status', 'exit_code', 'cleanup'}, 'case receipt')
        if item['status'] != 'passed' or type(item['exit_code']) is not int or item['exit_code'] != 0 or item['cleanup'] is not True:
            raise Refusal('required case did not pass and close')
    terminal = []
    parent = []
    for event in events:
        action, test = event.get('Action'), event.get('Test')
        if action == 'skip' or action == 'fail':
            raise Refusal('required Go qualification failed or skipped')
        if action == 'pass' and test == TEST:
            parent.append(test)
        if action == 'pass' and isinstance(test, str) and test.startswith(TEST + '/'):
            terminal.append(test[len(TEST) + 1:])
    if len(parent) != 1 or sorted(terminal) != sorted(expected):
        raise Refusal('actual exact required Go subtests did not execute')
    cleanup_valid(receipt['cleanup'], receipt['namespace_uid'], request['namespace'])


def validate_profile(observation):
    runtime, provider = observation.get('runtime', {}), observation.get('provider', {})
    if runtime.get('observation_only') is not True or provider.get('authentication_qualified') is not False:
        raise Refusal('metadata supplies observations only, never authentication qualification')
    expected = {
        '/etc/passwd': '21352194cc533bc5878721507450d867d28ccb1c2f5cd773c792251fa1e63185',
        '/etc/nsswitch.conf': 'eec30745bade42a3f3f792e4d4192e57d2bcfe8e472433b1de426fe39a39cddb',
        '/usr/share/rpgmigrate/python-libpq-runtime.json': '122ec42c730898d3f9680b394b0a521ef6f808e29c6db013c0c495caa34dab44',
        '/usr/lib/aarch64-linux-gnu/libpq.so.5.18': '094cbe08c8c6134b91b1946e42012318728003cf2dd2fcc0ab1ee9821ba38f58',
        '/usr/lib/aarch64-linux-gnu/libc.so.6': None,
        '/usr/bin/python3.13': None,
        # read off the abd1a36 arm64 runner layers, matching its publication executable_sha256
        '/usr/local/bin/rpgmigrate': '9fdadd2a376e750565cb94f2e978465a9b006566ba16874cef1852fdbb406759',
    }
    files = closed(runtime.get('image_files'), expected, 'metadata image files')
    fields = set('sha256 device inode uid gid mode links size mount_id root_readonly shadow_mounts'.split())
    for path, digest in expected.items():
        value = closed(files[path], fields, 'metadata image file')
        if (not isinstance(value['sha256'], str) or not HEX.fullmatch(value['sha256']) or
                digest is not None and value['sha256'] != digest or
                any(type(value[key]) is not int or value[key] < 0
                    for key in ('device', 'inode', 'uid', 'gid', 'mode', 'links', 'size')) or
                value['uid'] != 0 or value['gid'] != 0 or value['mode'] & 0o022 or
                value['links'] != 1 or not 0 < value['size'] <= 16777216 or
                not isinstance(value['mount_id'], str) or not value['mount_id'].isdigit() or
                value['root_readonly'] is not True or value['shadow_mounts'] != []):
            raise Refusal('metadata image file hash, ownership or mount visibility differs')


def validate_observations(request, receipt, observations, image, node_bindings):
    fields = set('node_name node_uid node_container_id job_name job_uid pod_name pod_uid '
                 'container_id image_ref image_id node_info node_runtime cri runtime resolver home '
                 'provider logs_eof logs_closed terminal_exit_code'.split())
    if not isinstance(observations, list) or len(observations) != 3:
        raise Refusal('actual three-node metadata observations missing')
    closed(node_bindings, {node['name'] for node in request['nodes']}, 'metadata exact node bindings')
    cleanup = {(item['kind'], item['namespace'], item['name'], item['uid'])
               for item in receipt['cleanup']['objects']}
    for number, (node, observation) in enumerate(zip(request['nodes'], observations)):
        closed(observation, fields, 'metadata observation')
        binding = closed(node_bindings[node['name']], set('node_id requested_ref manifest_digest config_digest '
                         'os architecture variant accepted_image_ids'.split()), 'metadata node image binding')
        ids = binding['accepted_image_ids']
        if (binding['node_id'] != node['container_id'] or binding['requested_ref'] != image['ref'] or
                binding['manifest_digest'] != image['digest'] or binding['config_digest'] != image['config_digest'] or
                binding['os'] != 'linux' or binding['architecture'] != 'arm64' or binding['variant'] != image['variant'] or
                not isinstance(ids, list) or not ids or any(not isinstance(value, str) for value in ids) or
                ids != sorted(set(ids)) or image['digest'] not in ids or image['config_digest'] not in ids):
            raise Refusal('metadata direct-pull node binding differs')
        if (observation['node_name'] != node['name'] or observation['node_uid'] != node['uid'] or
                observation['node_container_id'] != node['container_id'] or
                observation['image_ref'] != image['ref'] or
                observation['image_id'] not in ids or
                observation['job_name'] != f'native-metadata-{number}' or
                observation['logs_eof'] is not True or observation['logs_closed'] is not True or
                type(observation['terminal_exit_code']) is not int or observation['terminal_exit_code'] != 0):
            raise Refusal('actual metadata runtime binding changed')
        for kind, prefix in (('Job', 'job'), ('Pod', 'pod')):
            if (kind, request['namespace'], observation[prefix + '_name'],
                    observation[prefix + '_uid']) not in cleanup:
                raise Refusal('metadata observation lacks exact owned cleanup binding')
        for name in ('node_info', 'node_runtime', 'cri', 'runtime', 'resolver', 'home', 'provider'):
            if not isinstance(observation[name], dict) or not observation[name]:
                raise Refusal('required metadata observation is empty')
        validate_active_profile(observation)
        validate_profile(observation)


def validate_active_profile(observation):
    node = closed(observation['node_runtime'], set('default_runtime runtime_type sandboxer runtime_ready '
        'network_ready active_runtime_verified stock_runtime_options containerd_no_deprecation_warnings '
        'endpoint_verified immutable_node_files_verified'.split()), 'active node profile')
    if (node['default_runtime'] != 'runc' or node['runtime_type'] != 'io.containerd.runc.v2' or
            node['sandboxer'] != 'podsandbox' or type(node['containerd_no_deprecation_warnings']) is not bool or
            any(node[key] is not True for key in ('runtime_ready', 'network_ready', 'active_runtime_verified',
                'stock_runtime_options', 'endpoint_verified', 'immutable_node_files_verified'))):
        raise Refusal('active owned node profile refused')
    keys = set('container_id pod_uid sandbox_id pid start_ticks boot_id mount_namespace admitted_mount_count '
        'tmp_device tmp_inode key_stat_verified termination_log_id image_volume_id sandbox_pid state '
        'live_process_verified terminal_pid_absent final_spec_verified actual_mounts_verified '
        'actual_environment_verified resource_bounds_verified stock_hook_verified release_created '
        'last_admitted_process_verified endpoint_verified immutable_node_files_verified resolver_source '
        'node_bytes_match node_inode_match metadata_release_created'.split())
    cri = closed(observation['cri'], keys, 'active terminal profile')
    if (not isinstance(cri['container_id'], str) or
            observation['container_id'] != 'containerd://' + cri['container_id'] or
            cri['pod_uid'] != observation['pod_uid'] or not isinstance(cri['sandbox_id'], str) or
            not HEX.fullmatch(cri['sandbox_id']) or not HEX.fullmatch(cri['container_id']) or
            any(type(cri[key]) is not int or cri[key] <= 0 for key in ('pid', 'start_ticks', 'sandbox_pid', 'admitted_mount_count')) or
            not 1 < cri['pid'] < 4194304 or not 1 < cri['sandbox_pid'] < 4194304 or cri['admitted_mount_count'] > 128 or
            any(not isinstance(cri[key], str) or not re.fullmatch('[0-9]{1,20}', cri[key]) for key in ('tmp_device', 'tmp_inode')) or
            not isinstance(cri['boot_id'], str) or not re.fullmatch('[0-9a-f-]{36}', cri['boot_id']) or
            not isinstance(cri['mount_namespace'], str) or not re.fullmatch(r'mnt:\[[0-9]+\]', cri['mount_namespace']) or
            not isinstance(cri['termination_log_id'], str) or not re.fullmatch('[0-9a-f]{8}', cri['termination_log_id']) or
            cri['image_volume_id'] is not None or cri['state'] != 'terminal' or
            any(cri[key] is not False for key in ('key_stat_verified', 'live_process_verified',
                'actual_mounts_verified', 'actual_environment_verified', 'resource_bounds_verified', 'release_created')) or
            any(cri[key] is not True for key in ('terminal_pid_absent', 'final_spec_verified', 'stock_hook_verified',
                'last_admitted_process_verified', 'endpoint_verified', 'immutable_node_files_verified',
                'node_bytes_match', 'node_inode_match', 'metadata_release_created')) or
            cri['resolver_source'] != '/var/lib/containerd/io.containerd.grpc.v1.cri/sandboxes/' + cri['sandbox_id'] + '/resolv.conf'):
        raise Refusal('actual terminal and last-admitted process closure refused')


def run_go(request_path, timeout):
    environment = dict(os.environ)
    if SELECTOR in environment or any(key in environment for key in ('GOFLAGS', 'GOWORK', 'GOTOOLCHAIN', 'GOENV')):
        raise Refusal('inherited Go qualification override')
    environment[SELECTOR] = str(request_path)
    result = subprocess.run(['go', 'test', './internal/controller', '-run', '^' + TEST + '$',
                             '-count=1', '-json', '-timeout', '15m'], cwd=ROOT,
                            env=environment, capture_output=True, timeout=timeout, check=False)
    if len(result.stdout) > FILE_LIMIT or len(result.stderr) > 65536:
        raise Refusal('metadata Go output cap')
    events = [decode(line) for line in result.stdout.splitlines() if line]
    write_private(request_path.parent / 'metadata-go-events.json', events)
    if result.returncode != 0 or result.stderr:
        raise Refusal('metadata Go qualification failed; retain private request and partial cleanup evidence')
    return events


def fixed_program(path, marker):
    fixture = read_file(ROOT / path).decode('utf-8')
    if fixture.count(marker) != 1:
        raise Refusal('fixed program missing')
    program, separator, _ = fixture.split(marker, 1)[1].partition('`')
    if not separator or not program:
        raise Refusal('fixed program missing')
    return program


def password_program():
    # The canonical helper is a separate argument, so each argument keeps its own cap.
    probe = fixed_program(PASSWORD_FILE, 'const nativePasswordTLSProgram = `')
    source = read_file(ROOT / 'internal/native/evidence_program.py')
    program = ("import hashlib,sys,types\nSOURCE=sys.argv[1]\nif hashlib.sha256(SOURCE.encode()).hexdigest()!='" + sha(source) +
               "':raise SystemExit(2)\ncanonical=types.ModuleType('native_runtime_probe_canonical')\n"
               "exec(compile(SOURCE,'<canonical-native-evidence>','exec'),canonical.__dict__)\n" + probe).encode()
    if len(program) > 122880 or len(program) + 1 > 126976 or len(source) + 1 > 126976:
        raise Refusal('password TLS fixed source cap')
    return program


def delivery_program():
    program = fixed_program(DELIVERY_FILE, 'const nativeDeliveryProgram = `').encode()
    if len(program) > 122880 or len(read_file(ROOT / 'internal/native/evidence_program.py')) + 1 > 126976:
        raise Refusal('delivery fixed source cap')
    return program


def validate_password_receipt(request, receipt, events):
    closed(request, {'schema_version', 'runtime', 'probe_program_sha256', 'original_parent_deadline'},
           'password TLS request')
    runtime = closed(request['runtime'], REQUEST_KEYS, 'password TLS runtime')
    fields = set('schema_version kind qualification_id run_id input_sha256 qualification_source_sha256 '
                 'evidence_program_sha256 native_assets_sha256 probe_program_sha256 namespace_uid '
                 'started_at finished_at cases cleanup scanned_buffers canary_hits'.split())
    closed(receipt, fields, 'password TLS receipt')
    expected = required_cases('password_tls_probe', None)
    if (type(request['schema_version']) is not int or request['schema_version'] != 1 or
            type(runtime['schema_version']) is not int or runtime['schema_version'] != 1 or
            runtime['phase'] != 'password_tls_probe' or runtime['pair'] is not None or
            runtime['required_cases'] != expected or type(receipt['schema_version']) is not int or receipt['schema_version'] != 1 or
            receipt['kind'] != 'password_tls_probe' or receipt['probe_program_sha256'] != request['probe_program_sha256']):
        raise Refusal('fixed primitive phase selection differs')
    for key in ('qualification_id', 'run_id', 'qualification_source_sha256',
                'evidence_program_sha256', 'native_assets_sha256'):
        if receipt[key] != runtime[key]:
            raise Refusal('primitive receipt source binding differs')
    if (receipt['input_sha256'] != sha((json.dumps(request, sort_keys=True, separators=(',', ':')) + '\n').encode()) or
            not isinstance(receipt['namespace_uid'], str) or not receipt['namespace_uid'] or
            type(receipt['scanned_buffers']) is not int or receipt['scanned_buffers'] < 1 or
            type(receipt['canary_hits']) is not int or receipt['canary_hits'] != 0 or
            any(type(receipt[key]) not in (int, float) or not math.isfinite(receipt[key])
                for key in ('started_at', 'finished_at')) or
            not runtime['stage_deadline'] - 600 <= receipt['started_at'] <= receipt['finished_at'] <= min(
                runtime['stage_deadline'] + 600, runtime['lease_expires_at'], request['original_parent_deadline'])):
        raise Refusal('primitive receipt output or deadline binding differs')
    if not isinstance(receipt['cases'], list) or [item.get('name') for item in receipt['cases']
                                                if isinstance(item, dict)] != expected:
        raise Refusal('actual primitive inventory missing')
    for item in receipt['cases']:
        closed(item, {'name', 'status', 'exit_code', 'cleanup'}, 'primitive case')
        if item['status'] != 'passed' or type(item['exit_code']) is not int or item['exit_code'] != 0 or item['cleanup'] is not True:
            raise Refusal('primitive case failed or did not close')
    passed = [item.get('Test') for item in events if item.get('Action') == 'pass']
    if (any(item.get('Action') in ('fail', 'skip') for item in events) or
            passed.count(PASSWORD_TEST) != 1 or
            sorted(item.removeprefix(PASSWORD_TEST + '/') for item in passed
                   if isinstance(item, str) and item.startswith(PASSWORD_TEST + '/')) != sorted(expected)):
        raise Refusal('actual exact primitive Go cases did not execute')
    cleanup = closed(receipt['cleanup'], {'complete', 'objects'}, 'primitive cleanup')
    objects = cleanup['objects']
    if cleanup['complete'] is not True or not isinstance(objects, list) or len(objects) != 13:
        raise Refusal('exact thirteen-object cleanup missing')
    seen = set()
    uids = set()
    pod_roles = set()
    names = {'Job': {'client-16', 'client-18'}, 'Secret': {'server-16', 'server-18', 'client-16', 'client-18'},
             'Service': {'pg16', 'pg18'}, 'Namespace': {runtime['namespace']}}
    counts = {kind: 0 for kind in ('Pod', 'Job', 'Secret', 'Service', 'Namespace')}
    for item in objects:
        closed(item, {'api_version', 'kind', 'namespace', 'name', 'uid', 'resource_version',
                      'action', 'verified_absent'}, 'primitive cleanup object')
        if (item['kind'] not in counts or any(not isinstance(item[key], str) or not item[key]
                                            for key in ('name', 'uid', 'resource_version')) or
                item['api_version'] != ('batch/v1' if item['kind'] == 'Job' else 'v1') or
                item['action'] != 'deleted' or item['verified_absent'] is not True or
                item['namespace'] != ('' if item['kind'] == 'Namespace' else runtime['namespace'])):
            raise Refusal('primitive cleanup identity or verified absence missing')
        identity = (item['kind'], item['namespace'], item['name'])
        if identity in seen or item['uid'] in uids:
            raise Refusal('duplicate primitive cleanup identity')
        seen.add(identity)
        uids.add(item['uid'])
        if item['kind'] == 'Pod':
            role = item['name'] if item['name'] in ('pg16', 'pg18') else next((
                prefix for prefix in ('client-16-', 'client-18-')
                if item['name'].startswith(prefix) and len(item['name']) > len(prefix)), None)
            if role is None or role in pod_roles:
                raise Refusal('unselected or replaced primitive Pod')
            pod_roles.add(role)
        elif item['name'] not in names[item['kind']]:
            raise Refusal('unselected primitive cleanup name')
        counts[item['kind']] += 1
        if item['kind'] == 'Namespace' and (item['name'] != runtime['namespace'] or
                                           item['uid'] != receipt['namespace_uid']):
            raise Refusal('primitive namespace identity differs')
    if counts != {'Pod': 4, 'Job': 2, 'Secret': 4, 'Service': 2, 'Namespace': 1}:
        raise Refusal('primitive cleanup kinds differ')


def run_selected_go(request_path, timeout, selector, test, package, name):
    environment = dict(os.environ)
    if any(key in environment for key in (SELECTOR, PASSWORD_SELECTOR, DELIVERY_SELECTOR, 'GOFLAGS', 'GOWORK', 'GOTOOLCHAIN', 'GOENV')):
        raise Refusal('inherited primitive Go override')
    environment[selector] = str(request_path)
    result = subprocess.run(['go', 'test', package, '-run', '^' + test + '$',
                             '-count=1', '-json', '-timeout', '15m'], cwd=ROOT,
                            env=environment, capture_output=True, timeout=timeout, check=False)
    if len(result.stdout) > FILE_LIMIT or len(result.stderr) > 65536:
        raise Refusal('primitive Go output cap')
    events = [decode(line) for line in result.stdout.splitlines() if line]
    write_private(request_path.parent / (name + '-go-events.json'), events)
    if result.returncode != 0 or result.stderr:
        raise Refusal('actual ' + name + ' Go qualification failed; retain private cleanup evidence')
    return events


def run_password_go(request_path, timeout):
    return run_selected_go(request_path, timeout, PASSWORD_SELECTOR, PASSWORD_TEST, './internal/controller', 'password-tls')


def delivery_expected(name, observed):
    closed(observed, set('relay_successful second_relay_successful job_completed_native composite_authority '
                         'payload_bytes main_exit_code'.split()), 'delivery observation')
    relay, second, completed = observed['relay_successful'], observed['second_relay_successful'], observed['job_completed_native']
    composite, payload, exit_code = observed['composite_authority'], observed['payload_bytes'], observed['main_exit_code']
    if (type(relay) is not bool or type(composite) is not bool or second not in (None, True, False) or
            completed not in (None, True, False) or type(payload) is not int or
            exit_code is not None and type(exit_code) is not int):
        raise Refusal('delivery observation types differ')
    # Authority requires both the checked relay result and the terminal main proof.
    if composite != (relay and completed is True):
        raise Refusal('delivery composite authority differs from its two checked results')
    expected = {
        'quiet_success': relay and completed is True and payload >= 60000 and exit_code == 0 and second is None,
        'concurrent_relay_refused': relay and second is False and completed is True and exit_code == 0,
        'foreign_lock_refused': not relay and completed is False and exit_code not in (None, 0),
        'truncated_result_refused': not relay and completed is True and exit_code == 0,
        'stderr_refused': not relay and completed is not None and exit_code is not None,
        'startup_log_refused': relay and completed is False and exit_code == 0,
        'main_failure_refused': relay and completed is False and exit_code == 3,
        'disconnect_no_replay': not relay and second is False and completed is not None and exit_code is not None,
    }
    if not expected.get(name):
        raise Refusal('delivery case observation differs from its fixed expectation')
    return composite


def validate_delivery_receipt(request, receipt, events):
    closed(request, {'schema_version', 'runtime', 'delivery_program_sha256', 'original_parent_deadline'}, 'delivery request')
    runtime = closed(request['runtime'], REQUEST_KEYS, 'delivery runtime')
    fields = set('schema_version kind qualification_id run_id input_sha256 qualification_source_sha256 '
                 'evidence_program_sha256 native_assets_sha256 delivery_program_sha256 relay_program_sha256 '
                 'namespace_uid started_at finished_at cases cleanup'.split())
    closed(receipt, fields, 'delivery receipt')
    expected = required_cases('delivery', None)
    if (type(request['schema_version']) is not int or request['schema_version'] != 1 or runtime['phase'] != 'delivery' or
            runtime['pair'] is not None or runtime['required_cases'] != expected or
            type(receipt['schema_version']) is not int or receipt['schema_version'] != 1 or receipt['kind'] != 'delivery' or
            receipt['delivery_program_sha256'] != request['delivery_program_sha256'] or
            receipt['relay_program_sha256'] != sha(read_file(ROOT / 'internal/native/evidence_request_writer.py'))):
        raise Refusal('fixed delivery phase selection differs')
    for key in ('qualification_id', 'run_id', 'qualification_source_sha256', 'evidence_program_sha256', 'native_assets_sha256'):
        if receipt[key] != runtime[key]:
            raise Refusal('delivery receipt source binding differs')
    if (receipt['input_sha256'] != sha((json.dumps(request, sort_keys=True, separators=(',', ':')) + '\n').encode()) or
            not isinstance(receipt['namespace_uid'], str) or not receipt['namespace_uid'] or
            any(type(receipt[key]) not in (int, float) or not math.isfinite(receipt[key]) for key in ('started_at', 'finished_at')) or
            not runtime['stage_deadline'] - 600 <= receipt['started_at'] <= receipt['finished_at'] <= min(
                runtime['stage_deadline'] + 600, runtime['lease_expires_at'], request['original_parent_deadline'])):
        raise Refusal('delivery receipt output or deadline binding differs')
    cases = receipt['cases']
    if not isinstance(cases, list) or [item.get('name') if isinstance(item, dict) else None for item in cases] != expected:
        raise Refusal('actual delivery inventory missing')
    for item in cases:
        closed(item, {'name', 'status', 'exit_code', 'cleanup', 'observed'}, 'delivery case')
        if item['status'] != 'passed' or type(item['exit_code']) is not int or item['exit_code'] != 0 or item['cleanup'] is not True:
            raise Refusal('delivery case failed or did not close')
        if item['name'].startswith('delivery/'):
            delivery_expected(item['name'].split('/', 1)[1], item['observed'])
    passed = [item.get('Test') for item in events if item.get('Action') == 'pass']
    if (any(item.get('Action') in ('fail', 'skip') for item in events) or passed.count(DELIVERY_TEST) != 1 or
            sorted(item.removeprefix(DELIVERY_TEST + '/') for item in passed
                   if isinstance(item, str) and item.startswith(DELIVERY_TEST + '/')) != sorted(expected)):
        raise Refusal('actual exact delivery Go cases did not execute')
    cleanup = closed(receipt['cleanup'], {'complete', 'objects'}, 'delivery cleanup')
    objects = cleanup['objects']
    if cleanup['complete'] is not True or not isinstance(objects, list) or len(objects) != 22:
        raise Refusal('exact twenty-two-object delivery cleanup missing')
    counts, uids = {}, set()
    for item in objects:
        closed(item, {'api_version', 'kind', 'namespace', 'name', 'uid', 'resource_version', 'action', 'verified_absent'},
               'delivery cleanup object')
        if (any(not isinstance(item[key], str) or not item[key] for key in ('name', 'uid')) or item['uid'] in uids or
                item['action'] != 'deleted' or item['verified_absent'] is not True or
                item['api_version'] != ('batch/v1' if item['kind'] == 'Job' else 'v1') or
                item['namespace'] != ('' if item['kind'] == 'Namespace' else runtime['namespace']) or
                item['kind'] == 'Namespace' and (item['name'] != runtime['namespace'] or item['uid'] != receipt['namespace_uid'])):
            raise Refusal('delivery cleanup identity or verified absence missing')
        uids.add(item['uid'])
        counts[item['kind']] = counts.get(item['kind'], 0) + 1
    if counts != {'Pod': 10, 'Job': 10, 'PersistentVolumeClaim': 1, 'Namespace': 1}:
        raise Refusal('delivery cleanup kinds differ')


def component_images(guard, output_dir, packet, clocks, *, verify_only=False, before_first=None):
    if not all(callable(getattr(guard, method, None)) for method in
               ('qualify_component_images', 'verify_component_images')):
        raise Refusal('missing independent component image qualification API')
    path = output_dir / 'component-image-resolution-evidence.json'
    # A partial pull receipt remains failed rather than starting a second pull.
    if not verify_only and not (path.exists() or path.is_symlink()):
        with stage_limit(clocks, 'qualify') as (deadline, _):
            if before_first is not None:
                before_first()
                before_first = None
            guard.qualify_component_images(output_dir, packet, deadline)
    with stage_limit(clocks, 'final_verify' if verify_only else 'verify') as (deadline, _):
        if before_first is not None:
            before_first()
        return guard.verify_component_images(output_dir, packet, deadline)


def qualify(run_dir: Path, legacy_source_root: Path, output_dir: Path,
            phase: str, pair: tuple[int, int] | None = None, *,
            parent_deadline: float | None = None,
            parent_monotonic_deadline: float | None = None) -> dict:
    cases = required_cases(phase, pair)
    private_directory(run_dir)
    private_directory(output_dir, create=True)
    if output_dir == run_dir or output_dir.is_relative_to(run_dir) or run_dir.is_relative_to(output_dir):
        raise Refusal('native QUAL_DIR must be separate from legacy RUN_DIR')
    module, guard = load_guard(legacy_source_root, run_dir)
    with legacy_lock(guard):
        raw_ownership = read_private(run_dir / 'ownership.json')
        ledger = guard.read()
        guard.require_prepared(ledger)
        if ledger['phase'] not in ('pulled', 'verified'):
            raise Refusal('legacy public image pull/bootstrap is incomplete')
        if phase == 'prepare':
            guard._check(ledger)
        source, evidence, program = source_identity()
        binding = {'schema_version': 2, 'run_id': ledger['run_id'],
                   'legacy_source_root': str(legacy_source_root),
                   'legacy_source_tree_sha256': guard.source_tree(),
                   'legacy_guard_sha256': module._qualification_source_sha256,
                   'legacy_ownership_sha256': sha(raw_ownership),
                   'qualification_source_sha256': source, 'evidence_program_sha256': evidence}
        if phase == 'prepare':
            inventory, images = prepare_assets(guard)
            packet = dict(binding, qualification_id=uuid.uuid4().hex,
                          image_lock_sha256=inventory, images=images)
            closed(packet, PREPARATION_KEYS, 'preparation packet')
            write_private(output_dir / 'native-assets.json', packet)
            return packet
        packet_raw = read_private(output_dir / 'native-assets.json')
        packet = closed(decode(packet_raw), PREPARATION_KEYS, 'preparation packet')
        if any(packet[key] != value for key, value in binding.items()):
            raise Refusal('prepared native packet source or legacy RUN changed')
        inventory, images = prepare_assets(guard)
        if packet['image_lock_sha256'] != inventory or packet['images'] != images:
            raise Refusal('prepared public image lock identity changed')
        clocks = (ledger.get('resource_lease', {}).get('expires_at'), parent_deadline, parent_monotonic_deadline)
        nodes = []
        def observe_nodes():
            guard._check(ledger)
            guard.require_lease()
            if len(guard.owned_nodes(ledger)) != 3:
                raise Refusal('exact three owned nodes required')
            guard.verify_node_limits(ledger)
            guard.kubeconfig(ledger, timeout=30)
            guard.kubernetes_ownership(ledger)
            nodes_raw = decode(guard.kubectl(['get', 'nodes', '-o', 'json'], timeout=30))
            observed = {node['metadata']['name']: node for node in nodes_raw.get('items', [])}
            if set(observed) != {node['name'] for node in ledger['nodes']}:
                raise Refusal('Kubernetes node identities differ')
            for node in sorted(ledger['nodes'], key=lambda item: item['name']):
                uid = observed[node['name']].get('metadata', {}).get('uid')
                if not isinstance(uid, str) or not uid:
                    raise Refusal('Kubernetes node UID missing')
                nodes.append({'name': node['name'], 'container_id': node['id'], 'uid': uid})
        component = component_images(guard, output_dir, packet, clocks, before_first=observe_nodes)
        password = phase == 'password_tls_probe'
        delivery = phase == 'delivery'
        prefix = {'password_tls_probe': 'native-password-tls-', 'delivery': 'native-delivery-'}.get(phase, 'native-metadata-')
        receipt_name = {'password_tls_probe': 'password-tls', 'delivery': 'delivery'}.get(phase, 'metadata')
        with stage_limit(clocks, 'go') as (wall_stop, mono_stop):
            deadline, _ = go_limits(clocks, wall_stop, mono_stop)
            request = dict(binding, schema_version=1, qualification_id=packet['qualification_id'], phase=phase, pair=None,
                       run_dir=str(run_dir), native_assets_sha256=sha(packet_raw),
                       kubeconfig=ledger['kubeconfig'], context=ledger['context'],
                       namespace=prefix + packet['qualification_id'][:16], namespace_uid=None,
                       nodes=nodes, lease_expires_at=ledger['resource_lease']['expires_at'],
                       stage_deadline=deadline, required_cases=cases,
                       receipt_path=str(output_dir / (receipt_name + '-receipt.json')), metadata_program_sha256=program)
            closed(request, REQUEST_KEYS, 'metadata request')
            if password:
                request = dict(schema_version=1, runtime=request,
                           probe_program_sha256=sha(password_program()), original_parent_deadline=parent_deadline)
            if delivery:
                request = dict(schema_version=1, runtime=request,
                           delivery_program_sha256=sha(delivery_program()), original_parent_deadline=parent_deadline)
            request_path = output_dir / (receipt_name + '-input.json')
            write_private(request_path, request)
            _, timeout = go_limits(clocks, wall_stop, mono_stop)
            if time.time() >= deadline:
                raise Refusal('Go input work deadline expired')
            if password:
                events = run_password_go(request_path, timeout)
                receipt = decode(read_private(Path(request['runtime']['receipt_path'])))
                validate_password_receipt(request, receipt, events)
            elif delivery:
                events = run_selected_go(request_path, timeout, DELIVERY_SELECTOR, DELIVERY_TEST, './internal/controller', 'delivery')
                receipt = decode(read_private(Path(request['runtime']['receipt_path'])))
                validate_delivery_receipt(request, receipt, events)
            else:
                events = run_go(request_path, timeout)
                receipt = decode(read_private(Path(request['receipt_path'])))
                validate_receipt(request, receipt, events)
                observations_raw = read_private(output_dir / 'metadata-observations.json')
                if sha(observations_raw) != receipt['metadata_sha256']:
                    raise Refusal('metadata observation bytes changed')
                validate_observations(request, receipt, decode(observations_raw), images['native-runner']['image'],
                                      component['node_bindings']['native-runner'])
        if source_identity() != (source, evidence, program) or read_private(run_dir / 'ownership.json') != raw_ownership:
            raise Refusal('source or legacy RUN changed during metadata')
        component_images(guard, output_dir, packet, clocks, verify_only=True)
        with stage_limit(clocks, 'readback'):
            guard.require_lease()
            guard.owned_nodes(ledger)
            guard.verify_node_limits(ledger)
            guard.kubeconfig(ledger, timeout=30)
            guard.kubernetes_ownership(ledger)
            if source_identity() != (source, evidence, program) or read_private(run_dir / 'ownership.json') != raw_ownership:
                raise Refusal('source or legacy RUN changed during final image verification')
        return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('run-dir', 'legacy-source-root', 'qual-dir'):
        parser.add_argument('--' + key, type=Path, required=True)
    parser.add_argument('--phase', choices=('prepare', 'metadata', 'password_tls_probe', 'delivery', 'connections', 'evidence'), required=True)
    parser.add_argument('--pair', choices=('16:16', '18:18', '16:18'))
    parser.add_argument('--parent-deadline', type=float)
    parser.add_argument('--parent-monotonic-deadline', type=float)
    args = parser.parse_args()
    pair = tuple(map(int, args.pair.split(':'))) if args.pair else None
    try:
        qualify(args.run_dir, args.legacy_source_root, args.qual_dir, args.phase, pair,
                parent_deadline=args.parent_deadline, parent_monotonic_deadline=args.parent_monotonic_deadline)
    except Exception as error:
        message = str(error) if isinstance(error, Refusal) else type(error).__name__
        print(json.dumps({'phase': args.phase, 'result': 'refused', 'class': message}), file=sys.stderr)
        return 1
    print(json.dumps({'phase': args.phase, 'result': 'passed', 'scope': 'test-only observations'}))
    return 0


if __name__ == '__main__':
    sys.exit(main())

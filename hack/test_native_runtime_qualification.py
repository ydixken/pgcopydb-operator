#!/usr/bin/env python3
"""Host-only boundary checks; no test may launch an external process."""
import ast
import hashlib
import importlib.util
import inspect
import json
import os
import stat
from pathlib import Path
import tempfile
import sys
from types import SimpleNamespace
sys.dont_write_bytecode = True
import unittest
from unittest import mock

PATH = Path(__file__).with_name('native-runtime-qualification.py')
SPEC = importlib.util.spec_from_file_location('qualification', PATH)
Q = importlib.util.module_from_spec(SPEC)
if PATH.exists():
    SPEC.loader.exec_module(Q)


def fake_request():
    request = {key: '1' * 64 for key in Q.REQUEST_KEYS}
    request.update(schema_version=1, qualification_id='1' * 32, phase='metadata', pair=None, namespace='native-test',
                   run_id='2' * 32, namespace_uid=None, stage_deadline=1600, lease_expires_at=2200,
                   required_cases=Q.required_cases('metadata', None))
    return request


def fake_receipt(request):
    objects = [{'api_version': 'v1', 'kind': 'Pod', 'namespace': request['namespace'],
                'name': f'node-{node}', 'uid': f'pod-{node}', 'action': 'deleted',
                'verified_absent': True} for node in range(3)]
    objects.extend([{'api_version': 'batch/v1', 'kind': 'Job', 'namespace': request['namespace'],
                     'name': f'native-metadata-{node}', 'uid': f'job-{node}', 'action': 'deleted',
                     'verified_absent': True} for node in range(3)])
    objects.append({'api_version': 'v1', 'kind': 'Namespace', 'namespace': '',
                    'name': request['namespace'], 'uid': 'namespace-uid', 'action': 'deleted',
                    'verified_absent': True})
    receipt = {key: request[key] for key in Q.RECEIPT_KEYS if key in request}
    receipt.update(input_sha256=Q.sha((json.dumps(request, sort_keys=True, separators=(',', ':')) + '\n').encode()),
                   metadata_sha256='3' * 64, namespace_uid='namespace-uid',
                   started_at=1000, finished_at=1200,
                   cleanup={'complete': True, 'objects': objects},
                   cases=[{'name': case, 'status': 'passed', 'exit_code': 0, 'cleanup': True}
                          for case in request['required_cases']])
    return receipt


def fake_events(request):
    return ([{'Action': 'pass', 'Test': Q.TEST + '/' + case}
             for case in request['required_cases']] + [{'Action': 'pass', 'Test': Q.TEST}])


def fake_registry_lock():
    source = dict(repository='https://github.com/ydixken/rpgmigrate', commit='1' * 40,
                  git_tree='2' * 40, payload_inventory_sha256='3' * 64)
    images = {}
    for role in ('node', 'cnpg-controller', 'postgresql-16', 'postgresql-17', 'postgresql-18',
                 'manager', 'legacy-runner', 'native-runner', 'seed-tool-runner',
                 'otel-collector', 'storage-helper', 'storage-provisioner'):
        repository = 'ghcr.io/ydixken/rpgmigrate' if role == 'native-runner' else 'ghcr.io/cloudnative-pg/postgresql:' + role.rsplit('-', 1)[-1]
        images[role] = dict(role=role, ref=repository + '@sha256:' + '4' * 64,
                            os='linux', architecture='arm64', variant=None,
                            digest='sha256:' + '4' * 64, config_digest='sha256:' + '5' * 64,
                            index_digest=None if role == 'native-runner' else 'sha256:' + '6' * 64,
                            provenance=dict(origin_ref=repository + '@sha256:' + '4' * 64,
                                            registry_receipt_sha256='7' * 64, public_receipt_sha256='8' * 64))
    images['native-runner']['source'] = dict(source, build_input_sha256='9' * 64,
        executable_sha256='a' * 64, smoke_receipt_sha256='b' * 64)
    return dict(schema_version=2, transport='public-registry', operator_source=source,
                engine_source=source, images=images,
                publication_receipts_sha256={'manager': 'c' * 64, 'native-runner': 'd' * 64})


def image_files():
    hashes = {
        '/etc/passwd': '21352194cc533bc5878721507450d867d28ccb1c2f5cd773c792251fa1e63185',
        '/etc/nsswitch.conf': 'eec30745bade42a3f3f792e4d4192e57d2bcfe8e472433b1de426fe39a39cddb',
        '/usr/share/rpgmigrate/python-libpq-runtime.json': '122ec42c730898d3f9680b394b0a521ef6f808e29c6db013c0c495caa34dab44',
        '/usr/lib/aarch64-linux-gnu/libpq.so.5.18': '094cbe08c8c6134b91b1946e42012318728003cf2dd2fcc0ab1ee9821ba38f58',
        '/usr/lib/aarch64-linux-gnu/libc.so.6': '5' * 64,
        '/usr/bin/python3.13': '6' * 64,
        '/usr/local/bin/rpgmigrate': '46dcc819e2ff401501aa5f907324c6612ee1331074a61454edc5eb4fc64a1a4c',
    }
    return {path: dict(sha256=digest, uid=0, gid=0, mode=0o644, links=1,
                       root_readonly=True, shadow_mounts=[], mount_id='1', size=100,
                       device=1, inode=number + 1)
            for number, (path, digest) in enumerate(hashes.items())}


def metadata_functions():
    fixture = (Q.ROOT / Q.OWNED[2]).read_text()
    program = fixture.split(Q.METADATA_START, 1)[1].partition('`')[0]
    tree = ast.parse(program)
    definitions = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef)],
                             type_ignores=[])
    scope = dict(hashlib=hashlib, os=os, stat=stat)
    exec(compile(definitions, '<metadata-functions>', 'exec'), scope)
    return scope


def node_functions():
    fixture = (Q.ROOT / Q.OWNED[2]).read_text()
    program = fixture.split('const nativeRuntimeNodeProgram = `', 1)[1].partition('`')[0]
    tree = ast.parse(program)
    selected = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef))]
    scope = dict(__name__='active_cri_host_test')
    exec(compile(ast.Module(body=selected, type_ignores=[]), '<active-cri-functions>', 'exec'), scope)
    return scope


def active_profile_fixture():
    node=dict(default_runtime='runc',runtime_type='io.containerd.runc.v2',sandboxer='podsandbox',
              runtime_ready=True,network_ready=True,active_runtime_verified=True,stock_runtime_options=True,
              containerd_no_deprecation_warnings=False,endpoint_verified=True,immutable_node_files_verified=True)
    cri=dict(container_id='1'*64,pod_uid='owned-pod',sandbox_id='2'*64,pid=100,start_ticks=200,
             boot_id='00000000-0000-0000-0000-000000000001',mount_namespace='mnt:[3]',admitted_mount_count=20,
             tmp_device='1',tmp_inode='2',key_stat_verified=False,termination_log_id='0123abcd',image_volume_id=None,
             sandbox_pid=50,state='terminal',live_process_verified=False,terminal_pid_absent=True,
             final_spec_verified=True,actual_mounts_verified=False,actual_environment_verified=False,
             resource_bounds_verified=False,stock_hook_verified=True,release_created=False,
             last_admitted_process_verified=True,endpoint_verified=True,immutable_node_files_verified=True,
             resolver_source='/var/lib/containerd/io.containerd.grpc.v1.cri/sandboxes/'+'2'*64+'/resolv.conf',
             node_bytes_match=True,node_inode_match=True,metadata_release_created=True)
    value=dict(node_runtime=node,cri=cri,pod_uid='owned-pod',container_id='containerd://'+'1'*64)
    return value


class QualificationBoundaries(unittest.TestCase):
    def test_sandbox_uses_its_own_pinned_default_spec(self):
        functions = node_functions()
        expected = dict(pod_uid='owned-pod', pod_name='owned-name', namespace='owned-namespace')
        sandbox = '2' * 64
        value = dict(status=dict(id=sandbox, state='SANDBOX_READY', labels={'io.kubernetes.pod.uid':'owned-pod'}),
                     info=dict(runtimeType='io.containerd.runc.v2', runtimeOptions=dict(systemd_cgroup=True),
                               processStatus='running', netNamespaceClosed=False, pid=100,
                               config=dict(metadata=dict(uid='owned-pod', name='owned-name', namespace='owned-namespace')),
                               sandboxMetadata=dict(RuntimeHandler=''),
                               runtimeSpec=dict(linux=dict(namespaces=[dict(type=kind) for kind in ('pid','ipc','uts','mount')]+
                                                          [dict(type='network', path='/var/run/netns/cni-owned')]))))
        self.assertEqual(functions['sandbox_value'](json.dumps(value).encode(), expected, sandbox),
                         dict(sandbox_id=sandbox, sandbox_pid=100))
        for mutate in (lambda spec:spec.update(hooks=dict(createContainer=[dict(path='/custom')])),
                       lambda spec:spec['linux']['namespaces'].append(dict(type='cgroup')),
                       lambda spec:spec['linux']['namespaces'].append(dict(type='pid'))):
            candidate=json.loads(json.dumps(value));mutate(candidate['info']['runtimeSpec'])
            with self.assertRaises(RuntimeError):
                functions['sandbox_value'](json.dumps(candidate).encode(), expected, sandbox)

    def test_persistent_active_profile_requires_honest_terminal_evidence(self):
        self.assertTrue(callable(getattr(Q,'validate_active_profile',None)), 'persistent active profile gate missing')
        value = active_profile_fixture()
        Q.validate_active_profile(value)
        for key,changed in [('actual_environment_verified',True),('last_admitted_process_verified',False),
                            ('terminal_pid_absent',False),('metadata_release_created',False),('pid',0),
                            ('raw_config','canary-private')]:
            candidate=json.loads(json.dumps(value));candidate['cri'][key]=changed
            with self.subTest(key=key),self.assertRaises(Q.Refusal) as refused:Q.validate_active_profile(candidate)
            self.assertNotIn('canary-private',str(refused.exception))

    def test_primitive_binds_process_before_opening_credential_payload(self):
        program = Q.password_program().decode()
        body = program[program.index('P=canonical'):]
        payload = body.index(' payload=[]')
        for key in ('process_start_ticks','process_mount_namespace','process_pid'):
            self.assertIn(key,body[:payload])
        self.assertIn("need(local_closure()==process)",body[:body.index('session=P.Session')])
        self.assertIn("process=local_closure()",body[:body.index("kind='ready'")])
        self.assertIn('process=process',body[body.index("kind='result'"):])

    def test_metadata_has_a_bounded_observed_release_barrier(self):
        source = (Q.ROOT / Q.OWNED[2]).read_text()
        program = source.split(Q.METADATA_START, 1)[1].partition('`')[0]
        self.assertIn("kind='metadata_ready'", program)
        self.assertIn("kind='metadata_complete'", program)
        self.assertIn("'/tmp/.native-metadata-release'", program)
        self.assertIn('time.monotonic()+30', program)
        self.assertIn('st_size==0', program)
        self.assertIn('0o400', program)
        self.assertNotIn('time.sleep(30)', program)

    def test_process_environment_identity_and_pid_reuse_are_closed(self):
        functions = node_functions()
        self.assertTrue(callable(functions.get('proc_values')), 'actual process comparison missing')
        self.assertTrue(callable(functions.get('same_incarnation')), 'PID incarnation binding missing')
        expected = json.loads((Q.ROOT / 'hack/testdata/native-runtime-active-profile.json').read_bytes())['expected']
        args = ('\0'.join(expected['running_args']) + '\0').encode()
        env = ('\0'.join(key+'='+value for key,value in expected['process_environment'].items())+'\0').encode()
        status = dict(Uid='65532 65532 65532 65532', Gid='65532 65532 65532 65532', Groups='65532',
                      CapInh='00000000', CapPrm='00000000', CapEff='00000000', CapBnd='00000000',
                      CapAmb='00000000', NoNewPrivs='1', Seccomp='2')
        functions['proc_values'](expected, args, env, status)
        for key,value in [('Uid','0 0 0 0'),('Groups','65532 0'),('CapEff','1'),('NoNewPrivs','0'),('Seccomp','0')]:
            changed = dict(status, **{key:value})
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                functions['proc_values'](expected, args, env, changed)
        with self.assertRaises(RuntimeError) as refused:
            functions['proc_values'](expected, args, env+b'LD_PRELOAD=canary-private\0', status)
        self.assertNotIn('canary-private',str(refused.exception))
        binding = dict(container_id='1'*64,pod_uid='owned-pod',sandbox_id='2'*64,pid=100,
                       start_ticks=200,boot_id='boot',mount_namespace='mnt:[7]')
        functions['same_incarnation'](binding,dict(binding))
        for key,value in [('container_id','3'*64),('pod_uid','foreign'),('pid',101),('start_ticks',201),
                          ('boot_id','replacement'),('mount_namespace','mnt:[8]')]:
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                functions['same_incarnation'](binding,dict(binding,**{key:value}))

    def test_server_process_identity_is_the_locked_cnpg_postgres_user(self):
        functions = node_functions()
        expected = json.loads((Q.ROOT / 'hack/testdata/native-runtime-active-profile.json').read_bytes())['expected']
        expected = dict(expected, profile='pg16')
        args = ('\0'.join(expected['running_args']) + '\0').encode()
        env = ('\0'.join(key+'='+value for key,value in expected['process_environment'].items())+'\0').encode()
        status = dict(Uid='26 26 26 26', Gid='26 26 26 26', Groups='26 101',
                      CapInh='0', CapPrm='0', CapEff='0', CapBnd='0', CapAmb='0', NoNewPrivs='1', Seccomp='2')
        functions['proc_values'](expected, args, env, status)
        for key, value in [('Uid', '999 999 999 999'), ('Gid', '999 999 999 999'), ('Groups', '101 999'), ('Groups', '26')]:
            with self.subTest(key=key, value=value), self.assertRaises(RuntimeError):
                functions['proc_values'](expected, args, env, dict(status, **{key: value}))

    def test_mountinfo_admits_only_runc_bound_default_devices(self):
        functions = node_functions()
        spec = dict(mounts=[dict(destination='/tmp', type='bind', options=['rbind', 'rprivate', 'rw'])],
                    linux=dict(maskedPaths=[], readonlyPaths=[]))
        dmi = dict.fromkeys(('/sys/class/dmi/id/product_name', '/sys/class/dmi/id/product_uuid',
                             '/sys/devices/virtual/dmi/id/product_name', '/sys/devices/virtual/dmi/id/product_uuid'), '1')
        meta = dict(dmi_pairs=dmi, volume_pairs=dict(tmp='1', client=None, server=None, pgdata=None), tmp_bytes='67108864')
        rows = ['1 0 0:1 / / ro,relatime - overlay overlay ro', '2 1 0:2 / /tmp rw,nosuid - tmpfs tmpfs rw']
        devices = ['%d 1 0:5 /%s /dev/%s rw,nosuid,noexec - devtmpfs devtmpfs rw' % (3 + number, name, name)
                   for number, name in enumerate(('null', 'zero', 'full', 'random', 'urandom', 'tty'))]
        raw = lambda lines: ('\n'.join(lines) + '\n').encode()
        self.assertEqual(functions['proc_mounts'](raw(rows + devices), spec, {}, meta), 8)
        for changed in (devices[:1] + [devices[0]], [devices[0].replace(',noexec', '')],
                        [devices[0].replace('devtmpfs devtmpfs', 'tmpfs tmpfs')], [devices[0].replace('/dev/null', '/dev/kmsg')]):
            with self.subTest(changed=changed), self.assertRaises(RuntimeError):
                functions['proc_mounts'](raw(rows + changed), spec, {}, meta)

    def test_cgroup_values_follow_runc_weight_and_kubelet_slices(self):
        functions = node_functions()
        uid, container = 'owned-pod', '1' * 64
        stat = b'100 (python3) ' + b' '.join([b'S'] + [b'0'] * 18 + [b'200'] + [b'0'] * 5)
        cgroup = ('0::/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-burstable.slice/kubelet-kubepods-burstable-pod'
                  + uid.replace('-', '_') + '.slice/cri-containerd-' + container + '.scope\n').encode()
        values = {'cpu.max': '100000 100000', 'cpu.weight': '35', 'memory.max': '536870912', 'memory.swap.max': '0',
                  'pids.max': 'max', 'parent.pids.max': '4096', 'ancestry.pids.max': ['max', '256076', 'max', 'max']}
        def frames(cg, path):
            meta = dict(boot_id='00000000-0000-0000-0000-000000000001', mount_namespace='mnt:[3]',
                        namespace_matches=dict.fromkeys(('mnt', 'pid', 'ipc', 'uts', 'net', 'cgroup', 'user'), '1'),
                        binary_kind='python313', tmp_device='1', tmp_inode='2', tmp_bytes='67108864', volume_pairs={},
                        dmi_pairs={}, cgroup_values=cg, key_stat=None)
            return dict(meta=meta, stat_before=stat, stat_after=stat, status=b'Name:\tpython3\n', cmdline=b'', environ=b'',
                        mountinfo=b'', cgroup=path)
        functions.update(proc_values=lambda *args: None, proc_mounts=lambda *args: 20)
        def snapshot(cg, path):
            functions['primitive'] = lambda *args: frames(cg, path)
            return functions['process_snapshot'](dict(profile='metadata', pod_uid=uid, container_id=container),
                                                 dict(info=dict(pid=100, runtimeSpec={})), dict(sandbox_pid=50, sandbox_id='2' * 64))
        self.assertEqual(snapshot(values, cgroup)['start_ticks'], 200)
        # systemd's TasksMax on the scope is admitted only beneath the pod slice's 4096 bound.
        for scope in ('38411', '4096', '1'):
            self.assertEqual(snapshot(dict(values, **{'pids.max': scope}), cgroup)['start_ticks'], 200)
        self.assertEqual(snapshot(dict(values, **{'ancestry.pids.max': []}), cgroup)['start_ticks'], 200)
        self.assertEqual(functions['pids_bound']('38411', '4096', ['max', '256076']), 4096)
        self.assertEqual(functions['pids_bound']('100', '4096', ['max']), 100)
        refused = [{'parent.pids.max': value} for value in ('max', '4095', '8192', '04096', ' 4096')]
        refused += [{'pids.max': value} for value in ('0', '-1', '', '1 2', 38411)]
        refused += [{'ancestry.pids.max': value} for value in ('max', ['0'], ['many'], [None], ['max'] * 15)]
        for changed in refused:
            with self.subTest(changed=changed), self.assertRaises(RuntimeError):
                snapshot(dict(values, **changed), cgroup)
        without = dict(values); del without['ancestry.pids.max']
        for cg, path in ((dict(values, **{'cpu.weight': '10'}), cgroup), (values, cgroup.replace(b'/kubelet-kubepods-burstable-pod', b'/kubepods-burstable-pod')),
                         (without, cgroup)):
            with self.subTest(cg=cg, path=path), self.assertRaises(RuntimeError):
                snapshot(cg, path)

    def test_release_path_is_only_the_matching_owned_tmp_volume(self):
        functions = node_functions()
        self.assertTrue(callable(functions.get('release_root')), 'fixed release derivation missing')
        expected = json.loads((Q.ROOT / 'hack/testdata/native-runtime-active-profile.json').read_bytes())['expected']
        root='/var/lib/kubelet/pods/owned-pod/volumes/kubernetes.io~empty-dir/tmp'
        mounts=[dict(destination='/tmp',source=root,type='bind',options=['rbind','rprivate','rw'])]
        self.assertEqual(functions['release_root'](mounts,expected),root)
        for changed in ([dict(mounts[0],source=root.replace('owned-pod','foreign'))],
                        [dict(mounts[0],source=root+'/../other')], mounts+mounts,
                        [dict(mounts[0],options=['rbind','rprivate','ro'])]):
            with self.subTest(mounts=changed), self.assertRaises(RuntimeError):
                functions['release_root'](changed,expected)

    def test_final_oci_rejects_nri_execution_adjustments(self):
        functions = node_functions()
        self.assertTrue(callable(functions.get('checked_spec')), 'final OCI projection missing')
        fixture = json.loads((Q.ROOT / 'hack/testdata/native-runtime-active-profile.json').read_bytes())
        self.assertEqual(functions['checked_spec'](fixture['spec'], fixture['expected'], fixture['sandbox']),
                         {'termination_log_id': '0123abcd', 'image_volume_id': None})
        for change in ('env', 'args', 'hook', 'namespace', 'mount', 'capability', 'seccomp', 'resources', 'unknown'):
            candidate = json.loads(json.dumps(fixture['spec']))
            if change == 'env': candidate['process']['env'].append('LD_PRELOAD=canary-sensitive-value')
            elif change == 'args': candidate['process']['args'].append('canary-sensitive-value')
            elif change == 'hook': candidate['hooks']['poststart'] = [{'path': '/custom'}]
            elif change == 'namespace': candidate['linux']['namespaces'][0]['path'] = '/proc/1/ns/pid'
            elif change == 'mount': candidate['mounts'].append(dict(destination='/etc/nsswitch.conf', source='/custom', type='bind', options=['ro']))
            elif change == 'capability': candidate['process']['capabilities']['effective'] = ['CAP_SYS_ADMIN']
            elif change == 'seccomp': candidate['linux']['seccomp']['syscalls'][0]['action'] = 'SCMP_ACT_ALLOW_ALWAYS'
            elif change == 'resources': candidate['linux']['resources']['cpu']['quota'] += 1
            else: candidate['process']['unknown_execution_setting'] = True
            with self.subTest(change=change), self.assertRaises(RuntimeError) as refusal:
                functions['checked_spec'](candidate, fixture['expected'], fixture['sandbox'])
            self.assertNotIn('canary-sensitive-value', str(refusal.exception))

    def test_active_cri_accepts_only_pinned_live_runtime_fields(self):
        functions = node_functions()
        self.assertTrue(callable(functions.get('active')), 'active CRI adapter missing')
        selected = dict(runtimeType='io.containerd.runc.v2', runtimePath='', sandboxer='podsandbox',
            baseRuntimeSpec='/etc/containerd/cri-base.json', PodAnnotations=None, ContainerAnnotations=None,
            cgroupWritable=False, privileged_without_host_devices=False,
            privileged_without_host_devices_all_devices_allowed=False, cniConfDir='', cniMaxConfNum=0,
            snapshotter='', io_type='', options=dict(NoNewKeyring=False, ShimCgroup='', IoUid=0, IoGid=0,
                BinaryName='', Root='', SystemdCgroup=True, CriuImagePath='', CriuWorkPath=''))
        value = dict(status=dict(conditions=[dict(type=kind, status=True, reason='', message='')
            for kind in ('RuntimeReady', 'NetworkReady', 'ContainerdHasNoDeprecationWarnings')]),
            config=dict(containerd=dict(defaultRuntimeName='runc', runtimes=dict(runc=selected)),
                        containerdEndpoint='/run/containerd/containerd.sock'))
        value['status']['conditions'][0]['message']='canary-never-persist'
        with mock.patch('hashlib.sha256',side_effect=AssertionError('raw CRI hash')):
            actual = functions['active'](json.dumps(value).encode())
        self.assertNotIn('canary-never-persist',json.dumps(actual))
        self.assertEqual(actual['default_runtime'], 'runc')
        self.assertIs(actual['active_runtime_verified'], True)
        self.assertNotIn('nri_disabled', actual)
        for key, changed in [('runtimeType', 'unknown'), ('runtimePath', '/custom'), ('sandboxer', 'shim'),
                             ('baseRuntimeSpec', '/foreign'), ('PodAnnotations', ['*']), ('cgroupWritable', True),
                             ('options', dict(SystemdCgroup=True, BinaryName='/custom')),
                             ('unrecognized_execution_setting', True)]:
            candidate = json.loads(json.dumps(value))
            candidate['config']['containerd']['runtimes']['runc'][key] = changed
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                functions['active'](json.dumps(candidate).encode())
        for key, changed in [('defaultRuntimeName', 'test-handler'), ('defaultRuntimeName', True)]:
            candidate = json.loads(json.dumps(value)); candidate['config']['containerd'][key] = changed
            with self.subTest(key=key, changed=changed), self.assertRaises(RuntimeError):
                functions['active'](json.dumps(candidate).encode())
        for raw in (b'{"status":{},"status":{}}', b'[]', b'{} trailing', b' ' * 1048577):
            with self.subTest(raw_size=len(raw)), self.assertRaises(RuntimeError):
                functions['active'](raw)
        candidate = json.loads(json.dumps(value)); candidate['config']['containerdEndpoint'] = '/foreign.sock'
        with self.assertRaises(RuntimeError):
            functions['active'](json.dumps(candidate).encode())
        candidate = json.loads(json.dumps(value)); candidate['config']['unrecognized_execution_setting'] = True
        with self.assertRaises(RuntimeError):
            functions['active'](json.dumps(candidate).encode())

    def test_active_cri_redacts_known_opaque_messages_and_refuses_extra_actions(self):
        functions = node_functions()
        with mock.patch('subprocess.Popen',side_effect=AssertionError('unadmitted external process')) as command:
            for action,identifier in [('exec','1'*64),('inspect','foreign'),('inspectp','../foreign'),('info','1'*64)]:
                with self.subTest(action=action), self.assertRaises(RuntimeError):
                    functions['run'](action,identifier)
            command.assert_not_called()
        with mock.patch.object(sys,'stdin',SimpleNamespace(buffer=__import__('io').BytesIO(b'{"mode":"node","argv":["canary-private"]}'))), \
                self.assertRaises(RuntimeError) as refused:
            functions['main']()
        self.assertNotIn('canary-private',str(refused.exception))

    def test_owned_release_directory_refuses_symlink_components(self):
        functions=node_functions()
        self.assertTrue(callable(functions.get('perl_source')), 'pinned node primitive missing')
        source=functions['perl_source']()
        self.assertIn("@INC = ('/usr/lib/aarch64-linux-gnu/perl-base')",source)
        self.assertIn('O_RDONLY|O_DIRECTORY|O_NOFOLLOW',source)
        self.assertIn('O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW,0400',source)
        self.assertIn(".native-metadata-release",source)
        self.assertNotIn('JSON::PP',source)
        with mock.patch('subprocess.Popen',side_effect=AssertionError('foreign primitive launch')) as command:
            for action,args in [('probe',(100,'../foreign',101,0)),('release',(100,'owned-pod',1,'/foreign',1,2)),('absence',('/foreign',1))]:
                with self.subTest(action=action),self.assertRaises(RuntimeError):functions['primitive'](action,*args)
            command.assert_not_called()

    def test_proc_wire_has_fixed_fields_caps_and_no_trailing_bytes(self):
        functions=node_functions()
        self.assertTrue(callable(functions.get('decode_primitive')), 'bounded node proc wire missing')
        import struct
        fields=('stat_before','status','cmdline','environ','mountinfo','cgroup','stat_after','meta')
        raw=b'M6FD01'+struct.pack('!I',8)
        for key in fields:
            value=b'{}' if key=='meta' else b'fixture'
            raw+=struct.pack('!I',len(key))+key.encode()+struct.pack('!I',len(value))+value
        self.assertEqual(functions['decode_primitive'](raw,'probe')['meta'],{})
        for bad in (raw+b'canary-private',raw[:-1],raw.replace(b'stat_before',b'foreign_key',1),
                    raw[:6]+struct.pack('!I',9)+raw[10:],b'x'*1048577):
            with self.subTest(size=len(bad)),self.assertRaises(RuntimeError) as refused:
                functions['decode_primitive'](bad,'probe')
            self.assertNotIn('canary-private',str(refused.exception))

    def test_active_cri_authority_uses_fixed_endpoint_and_never_disk_dump(self):
        fixture = (Q.ROOT / Q.OWNED[2]).read_text()
        program = fixture.split('const nativeRuntimeNodeProgram = `', 1)[1].partition('`')[0]
        self.assertNotIn("['containerd','config','dump']", program,
                         'disk configuration remains authoritative')
        self.assertIn('--config=/dev/null', program)
        self.assertIn('--runtime-endpoint=unix:///run/containerd/containerd.sock', program)
        self.assertIn('--image-endpoint=unix:///run/containerd/containerd.sock', program)
        self.assertIn('--timeout=2s', program)
        self.assertIn('--debug=false', program)
        self.assertLessEqual(len(program.encode()), 122880)
        self.assertLessEqual(len(program.encode()) + 1, 126976)

    def test_resolver_producer_utilities_have_fixed_paths_and_cleared_environment(self):
        fixture = (Q.ROOT / Q.OWNED[2]).read_text()
        body = fixture[fixture.index('func nativeRuntimeCRIFacts('):fixture.index('func nativeRuntimeChildrenAbsent(')]
        self.assertNotIn('node.ContainerID, "stat"', body)
        self.assertNotIn('node.ContainerID, "cat"', body)
        for binary in ('/usr/bin/stat', '/usr/bin/cat'):
            self.assertIn('"LC_ALL=C", "LANG=C", "' + binary + '"', body)
        self.assertIn("['/usr/bin/cat',", node_functions()['perl_source']())

    def test_original_clock_stage_budgets_are_explicit_and_reserved(self):
        self.assertIn('parent_monotonic_deadline', inspect.signature(Q.stage_deadline).parameters,
                      'the original monotonic hard stop is missing')
        for stage, budget, required in [('qualify', 900, 3300), ('verify', 600, 2400),
                                        ('go', 900, 1800), ('final_verify', 600, 900), ('readback', 300, 300)]:
            with self.subTest(stage=stage):
                self.assertEqual(Q.stage_deadline(1000 + required, 1000, parent_deadline=10000,
                    parent_monotonic_deadline=10000, monotonic_now=1000, stage=stage), 1000 + budget)
                with self.assertRaises(Q.Refusal):
                    Q.stage_deadline(1000 + required - .001, 1000, parent_deadline=10000,
                        parent_monotonic_deadline=10000, monotonic_now=1000, stage=stage)
                with self.assertRaises(Q.Refusal):
                    Q.stage_deadline(10000, 1000, parent_deadline=10000,
                        parent_monotonic_deadline=1000 + required - .001, monotonic_now=1000, stage=stage)

    def test_cold_image_stages_do_not_share_the_qualification_deadline(self):
        clock = dict(wall=1000., mono=1000.)
        calls = []
        def qualify(directory, packet, deadline):
            self.assertEqual(deadline, 1900, 'qualification did not receive its fixed900-second budget')
            calls.append('qualify')
            clock.update(wall=1899., mono=1899.)
        def verify(directory, packet, deadline):
            self.assertEqual(deadline, 2499, 'fresh verification inherited an almost-expired deadline')
            calls.append('verify')
            return 'verified'
        guard = SimpleNamespace(qualify_component_images=qualify, verify_component_images=verify)
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(Q.time, 'time', side_effect=lambda: clock['wall']), \
                mock.patch.object(Q.time, 'monotonic', side_effect=lambda: clock['mono']):
            self.assertEqual(Q.component_images(guard, Path(temporary).resolve(), {}, (10000, 10000, 10000)), 'verified')
        self.assertEqual(calls, ['qualify', 'verify'])

    def test_go_limits_leave_six_hundred_seconds_before_the_earliest_kill(self):
        self.assertTrue(callable(getattr(Q, 'go_limits', None)), 'Go clock composition is missing')
        for wall_stop, mono_stop, expected_timeout in [(1900, 5900, 900), (1900, 5790, 790), (1750, 5900, 750)]:
            work, timeout = Q.go_limits((10000, 10000, 10000), wall_stop, mono_stop, now=1000, monotonic_now=5000)
            self.assertEqual(timeout, expected_timeout)
            self.assertEqual(work + 600, 1000 + timeout - 1)
        with self.assertRaises(Q.Refusal):
            Q.go_limits((10000, 10000, 5600), 1900, 5900, now=1000, monotonic_now=5000)

    def test_insufficient_parent_refuses_before_any_image_api(self):
        with tempfile.TemporaryDirectory() as temporary, \
                mock.patch.object(Q.time, 'time', return_value=1000), \
                mock.patch.object(Q.time, 'monotonic', return_value=5000):
            output = Path(temporary).resolve()
            for cached in (False, True):
                if cached:
                    (output / 'component-image-resolution-evidence.json').write_text('{}')
                required = 2400 if cached else 3300
                for clocks in ((1000 + required - .001, 10000, 10000),
                               (10000, 1000 + required - .001, 10000),
                               (10000, 10000, 5000 + required - .001)):
                    guard = mock.Mock()
                    with self.subTest(cached=cached, clocks=clocks), self.assertRaises(Q.Refusal):
                        Q.component_images(guard, output, {}, clocks)
                    guard.qualify_component_images.assert_not_called()
                    guard.verify_component_images.assert_not_called()

    def test_complete_cache_still_requires_fresh_bounded_verification(self):
        clock = dict(wall=1000., mono=5000.)
        with tempfile.TemporaryDirectory() as temporary, \
                mock.patch.object(Q.time, 'time', side_effect=lambda: clock['wall']), \
                mock.patch.object(Q.time, 'monotonic', side_effect=lambda: clock['mono']):
            output = Path(temporary).resolve()
            receipt = output / 'component-image-resolution-evidence.json'
            receipt.write_text('exact opaque source-bound receipt fixture')
            raw = receipt.read_bytes()
            guard = mock.Mock()
            guard.verify_component_images.return_value = mock.sentinel.verified
            self.assertIs(Q.component_images(guard, output, {}, (3400, 10000, 7400)), mock.sentinel.verified)
            guard.qualify_component_images.assert_not_called()
            guard.verify_component_images.assert_called_once_with(output, {}, 1600)
            self.assertEqual(receipt.read_bytes(), raw)
            guard.verify_component_images.side_effect = Q.Refusal('incomplete or changed exact receipt')
            with self.assertRaisesRegex(Q.Refusal, 'incomplete or changed'):
                Q.component_images(guard, output, {}, (3400, 10000, 7400))
            self.assertEqual(receipt.read_bytes(), raw)

    def test_stage_watchdog_keeps_original_clocks_and_refuses_existing_alarm(self):
        clock = dict(wall=1000., mono=5000.)
        with mock.patch.object(Q.time, 'time', side_effect=lambda: clock['wall']), \
                mock.patch.object(Q.time, 'monotonic', side_effect=lambda: clock['mono']):
            for changed in ('wall', 'mono'):
                with self.subTest(clock=changed), self.assertRaisesRegex(Q.Refusal, 'stage deadline expired'):
                    with Q.stage_limit((10000, 10000, 10000), 'verify') as (wall, mono):
                        self.assertEqual((wall, mono), (1600, 5600))
                        clock[changed] += 600
                clock.update(wall=1000., mono=5000.)
            with mock.patch.object(Q.signal, 'getitimer', return_value=(10., 0.)), \
                    mock.patch.object(Q.signal, 'setitimer') as timer, \
                    self.assertRaisesRegex(Q.Refusal, 'cannot be replaced'):
                with Q.stage_limit((10000, 10000, 10000), 'go'):
                    self.fail('existing watchdog accepted')
            timer.assert_not_called()

    def test_failed_fresh_verification_cannot_reach_the_namespace_stage(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary).resolve(); run = base / 'run'; qual = base / 'qual'
            run.mkdir(mode=0o700); qual.mkdir(mode=0o700)
            ownership = Q.write_private(run / 'ownership.json', {'unit_fixture': True})
            (run / 'lifecycle.lock').write_bytes(b''); (run / 'lifecycle.lock').chmod(0o600)
            nodes = [{'name': f'node-{i}', 'id': str(i + 1) * 64} for i in range(3)]
            ledger = dict(run_id='2' * 32, phase='pulled', nodes=nodes, resource_lease=dict(expires_at=10000),
                          kubeconfig=str(run / 'kubeconfig'), context='kind-unit-fixture')
            guard = mock.Mock(directory=run); guard.read.return_value=ledger
            guard.source_tree.return_value='3' * 64; guard.owned_nodes.return_value=nodes
            guard.kubectl.return_value=json.dumps(dict(items=[dict(metadata=dict(name=n['name'], uid=f'uid-{i}'))
                for i, n in enumerate(nodes)])).encode()
            guard.verify_component_images.side_effect=Q.Refusal('fresh verification refused')
            source, evidence, _ = Q.source_identity(); images={'unit_fixture': True}
            packet=dict(schema_version=2, qualification_id='1' * 32, run_id=ledger['run_id'],
                legacy_source_root='/private/unit-source', legacy_source_tree_sha256='3' * 64,
                legacy_guard_sha256='4' * 64, legacy_ownership_sha256=Q.sha(ownership),
                qualification_source_sha256=source, evidence_program_sha256=evidence,
                image_lock_sha256='5' * 64, images=images)
            Q.write_private(qual / 'native-assets.json', packet)
            for parent, reason in ((4299.999, 'insufficient original lease'), (10000, 'fresh verification refused')):
                guard.reset_mock()
                with self.subTest(parent=parent), \
                        mock.patch.object(Q, 'load_guard', return_value=(SimpleNamespace(_qualification_source_sha256='4' * 64), guard)), \
                        mock.patch.object(Q, 'prepare_assets', return_value=('5' * 64, images)), \
                        mock.patch.object(Q.time, 'time', return_value=1000), mock.patch.object(Q.time, 'monotonic', return_value=1000), \
                        mock.patch.object(Q, 'run_go', side_effect=AssertionError('namespace stage reached before fresh verification')), \
                        self.assertRaisesRegex(Q.Refusal, reason):
                    Q.qualify(run, Path('/private/unit-source'), qual, 'metadata', parent_deadline=parent,
                              parent_monotonic_deadline=10000)
                if parent < 4300:
                    guard._check.assert_not_called()
                    guard.require_lease.assert_not_called()
                    guard.qualify_component_images.assert_not_called()
                    guard.verify_component_images.assert_not_called()
            self.assertFalse((qual / 'metadata-input.json').exists())

    def test_node_key_stat_uses_fixed_perl_metadata_only_path(self):
        functions=node_functions()
        program=functions['perl_source']()
        self.assertIn("'server.key'",program)
        self.assertIn("$part ne '.' && $part ne '..'",program)
        self.assertIn('O_RDONLY|O_DIRECTORY|O_NOFOLLOW',program)
        self.assertIn('($st[2]&07777)==0440',program)
        self.assertNotIn("raw_file($root",program)
        source=(Q.ROOT / Q.PASSWORD_FILE).read_text()
        self.assertNotIn('nativePasswordKeyStatProgram',source)


    def test_password_program_keeps_the_actual_canonical_guards_closed(self):
        program = Q.password_program()
        self.assertLessEqual(len(program), 122880)
        self.assertLessEqual(len(program) + 1, 126976)
        tree = ast.parse(program)
        source = (Q.ROOT / 'internal/native/evidence_program.py').read_bytes()
        self.assertNotIn(source, program)
        loader = compile(ast.Module(body=tree.body[:5], type_ignores=[]), '<private-module-loader>', 'exec')
        with mock.patch.object(sys, 'argv', ['-c', source.decode() + '#']), self.assertRaises(SystemExit):
            exec(loader, {})
        scope = {}
        with mock.patch.object(sys, 'argv', ['-c', source.decode()]):
            exec(loader, scope)
        canonical = scope['canonical']
        self.assertEqual(scope['SOURCE'].encode(), source)
        self.assertEqual(canonical.__name__, 'native_runtime_probe_canonical')
        with self.assertRaises(canonical.Refusal):
            canonical.qualified_resolver_policy(None)
        with self.assertRaises(canonical.Refusal):
            canonical.connection_home({})
        definitions = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef)],
                                 type_ignores=[])
        functions = dict(stat=stat)
        exec(compile(definitions, '<probe-functions>', 'exec'), functions)
        valid = type('Info', (), dict(st_mode=stat.S_IFREG | 0o440, st_uid=0, st_gid=65532, st_nlink=1))
        functions['private_info'](valid)
        for field, value in [('st_mode', stat.S_IFREG | 0o444), ('st_uid', 65532), ('st_gid', 0), ('st_nlink', 2)]:
            bad = type('Bad', (), dict(st_mode=valid.st_mode, st_uid=valid.st_uid,
                                       st_gid=valid.st_gid, st_nlink=valid.st_nlink))
            setattr(bad, field, value)
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                functions['private_info'](bad)

    def test_password_receipt_requires_thirteen_exact_cleanups_and_real_cases(self):
        runtime = fake_request()
        runtime.update(phase='password_tls_probe', required_cases=Q.required_cases('password_tls_probe', None))
        request = dict(schema_version=1, runtime=runtime, probe_program_sha256='4' * 64, original_parent_deadline=2200)
        objects = []
        for kind, names in [('Pod', ['pg16', 'pg18', 'client-16-pod', 'client-18-pod']),
                            ('Job', ['client-16', 'client-18']), ('Secret', ['server-16', 'server-18', 'client-16', 'client-18']),
                            ('Service', ['pg16', 'pg18']), ('Namespace', [runtime['namespace']])]:
            for name in names:
                objects.append(dict(api_version='batch/v1' if kind == 'Job' else 'v1', kind=kind,
                                    namespace='' if kind == 'Namespace' else runtime['namespace'], name=name,
                                    uid='namespace-uid' if kind == 'Namespace' else kind + '-' + name,
                                    resource_version='7', action='deleted', verified_absent=True))
        receipt = {key: runtime[key] for key in ('qualification_id', 'run_id', 'qualification_source_sha256',
                   'evidence_program_sha256', 'native_assets_sha256')}
        receipt.update(schema_version=1, kind='password_tls_probe', input_sha256=Q.sha((
            json.dumps(request, sort_keys=True, separators=(',', ':')) + '\n').encode()),
            probe_program_sha256='4' * 64, namespace_uid='namespace-uid', started_at=1000, finished_at=1200,
            cases=[dict(name=name, status='passed', exit_code=0, cleanup=True) for name in runtime['required_cases']],
            cleanup=dict(complete=True, objects=objects), scanned_buffers=20, canary_hits=0)
        events = [dict(Action='pass', Test=Q.PASSWORD_TEST + '/' + name) for name in runtime['required_cases']]
        events.append(dict(Action='pass', Test=Q.PASSWORD_TEST))
        Q.validate_password_receipt(request, receipt, events)
        for change in ('missing_object', 'wrong_kind', 'missing_rv', 'unknown_create', 'secret_read',
                       'skip', 'duplicate_case', 'canary', 'stale_parent', 'boolean_version',
                       'foreign_name', 'replacement_name', 'reused_uid', 'private_field'):
            bad = json.loads(json.dumps(receipt)); observed = json.loads(json.dumps(events))
            if change == 'missing_object': bad['cleanup']['objects'].pop()
            elif change == 'wrong_kind': bad['cleanup']['objects'][0]['kind'] = 'Secret'
            elif change == 'missing_rv': bad['cleanup']['objects'][0]['resource_version'] = ''
            elif change == 'unknown_create': bad['cleanup']['complete'] = False
            elif change == 'secret_read': bad['cleanup']['objects'][0]['action'] = 'observed'
            elif change == 'skip': observed[0]['Action'] = 'skip'
            elif change == 'duplicate_case': observed.append(observed[0])
            elif change == 'canary': bad['canary_hits'] = 1
            elif change == 'stale_parent': bad['finished_at'] = 2201
            elif change == 'boolean_version': bad['schema_version'] = True
            elif change == 'foreign_name': bad['cleanup']['objects'][0]['name'] = 'unselected-pod'
            elif change == 'replacement_name': bad['cleanup']['objects'][1]['name'] = 'pg16'
            elif change == 'reused_uid': bad['cleanup']['objects'][1]['uid'] = bad['cleanup']['objects'][0]['uid']
            else: bad['raw_dsn'] = 'forbidden'
            with self.subTest(change=change), self.assertRaises(Q.Refusal):
                Q.validate_password_receipt(request, bad, observed)

    def test_password_tls_probe_has_a_fixed_primitive_inventory(self):
        expected = [f'pg{major}/{case}' for major in (16, 18) for case in (
            'good_scram_verify_full', 'wrong_password_refused', 'hostname_mismatch_refused',
            'sealed_ca_identity', 'private_closure', 'no_sensitive_persistence')]
        try:
            actual = Q.required_cases('password_tls_probe', None)
        except Q.Refusal:
            self.fail('the fixed password/TLS primitive probe inventory is missing')
        self.assertEqual(actual, expected)
        with self.assertRaises(Q.Refusal):
            Q.required_cases('password_tls_probe', (16, 16))

    def test_nss_lookup_requires_exact_unshadowed_readonly_image_files(self):
        functions = metadata_functions()
        self.assertIn('admit_nss', functions, 'metadata NSS admission is missing')
        files = image_files()
        functions['admit_nss'](files)
        for path in ('/etc/passwd', '/etc/nsswitch.conf'):
            for field, value in [('sha256', '0' * 64), ('uid', 65532), ('mode', 0o666),
                                 ('links', 2), ('root_readonly', False),
                                 ('shadow_mounts', ['/etc'])]:
                changed = json.loads(json.dumps(files))
                changed[path][field] = value
                with self.subTest(path=path, field=field), self.assertRaises(RuntimeError):
                    functions['admit_nss'](changed)

    def test_image_file_mount_facts_include_ancestor_and_exact_shadows(self):
        functions = metadata_functions()
        self.assertIn('image_mount', functions, 'metadata image mount observation is missing')
        mounts = [dict(id='1', path='/', options='ro'), dict(id='2', path='/tmp', options='rw')]
        self.assertEqual(functions['image_mount']('/etc/passwd', mounts),
                         dict(mount_id='1', root_readonly=True, shadow_mounts=[]))
        for path in ('/etc', '/etc/passwd'):
            with self.subTest(path=path):
                self.assertEqual(functions['image_mount']('/etc/passwd', mounts + [
                    dict(id='3', path=path, options='ro')])['shadow_mounts'], [path])
        mounts[0]['options'] = 'rw'
        self.assertIs(functions['image_mount']('/etc/passwd', mounts)['root_readonly'], False)

    def test_shadowed_provider_configuration_is_refused_before_read_or_hash(self):
        functions = metadata_functions()
        functions['safe'] = lambda *args: self.fail('read before immutable-image admission')
        for mounts in ([dict(id='1', path='/', options='rw')],
                       [dict(id='1', path='/', options='ro'),
                        dict(id='2', path='/etc/krb5.conf', options='ro')]):
            with self.subTest(mounts=mounts), \
                    mock.patch('os.path.realpath', side_effect=lambda path: path), \
                    self.assertRaises(RuntimeError):
                functions['image_file']('/etc/krb5.conf', mounts)

    def test_profile_receipt_cannot_qualify_authentication_or_omit_image_facts(self):
        self.assertTrue(callable(getattr(Q, 'validate_profile', None)),
                        'metadata image profile validator is missing')
        observation = dict(runtime=dict(image_files=image_files(), observation_only=True),
                           provider=dict(authentication_qualified=False))
        try:
            Q.validate_profile(observation)
        except Q.Refusal:
            self.fail('complete selected image profile, including engine bytes, was refused')
        for field, value in [('authentication_qualified', True), ('authentication_qualified', None)]:
            changed = json.loads(json.dumps(observation))
            changed['provider'][field] = value
            with self.subTest(value=value), self.assertRaises(Q.Refusal):
                Q.validate_profile(changed)
        for change in ('missing', 'empty', 'shadow', 'unknown_nss', 'writable', 'claim'):
            changed = json.loads(json.dumps(observation))
            if change == 'missing':
                del changed['runtime']['image_files']['/usr/bin/python3.13']
            elif change == 'empty':
                changed['runtime']['image_files']['/usr/lib/aarch64-linux-gnu/libc.so.6']['sha256'] = ''
            elif change == 'shadow':
                changed['runtime']['image_files']['/etc/passwd']['shadow_mounts'] = ['/etc/passwd']
            elif change == 'unknown_nss':
                changed['runtime']['image_files']['/etc/nsswitch.conf']['sha256'] = '0' * 64
            elif change == 'writable':
                changed['runtime']['image_files']['/etc/passwd']['root_readonly'] = False
            else:
                changed['runtime']['observation_only'] = False
            with self.subTest(change=change), self.assertRaises(Q.Refusal):
                Q.validate_profile(changed)

    def test_fixed_metadata_programs_are_syntax_valid_and_bound(self):
        fixture = (Q.ROOT / Q.OWNED[2]).read_text()
        programs = [fixture.split(prefix, 1)[1].partition('`')[0]
                    for prefix in (Q.METADATA_START, 'const nativeRuntimeNodeProgram = `')]
        for program in programs:
            compile(program, '<fixed-metadata-program>', 'exec')
        source, evidence, program_sha = Q.source_identity()
        self.assertEqual(program_sha, Q.sha(programs[0].encode() + b'\0' + programs[1].encode()))
        self.assertEqual(len(source), 64)
        self.assertEqual(len(evidence), 64)

    def test_phase_inventory_is_nonempty_and_driver_owned(self):
        self.assertTrue(hasattr(Q, 'required_cases'), 'required case inventory is missing')
        self.assertEqual(Q.required_cases('metadata', None), [
            f'node_{node}/{case}' for node in range(3)
            for case in ('runtime', 'resolver', 'home', 'provider')])
        for phase, pair in [('metadata', (16, 16)), ('connections', None),
                            ('evidence', (18, 16)), ('prepare', (16, 16))]:
            with self.subTest(phase=phase, pair=pair), self.assertRaises(Q.Refusal):
                Q.required_cases(phase, pair)

    def test_connection_phases_are_explicitly_unavailable(self):
        with mock.patch('subprocess.run', side_effect=AssertionError('external process')), \
            mock.patch('subprocess.Popen', side_effect=AssertionError('external process')):
            for phase in ('connections', 'evidence'):
                with self.subTest(phase=phase), self.assertRaisesRegex(
                        Q.Refusal, 'missing production policy and actual producer fixtures'):
                    Q.qualify(Path('/missing-run'), Path('/missing-source'),
                              Path('/missing-qual'), phase, (16, 16))

    def test_private_file_requires_mode_identity_and_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve() / 'input.json'
            path.write_text('{}\n')
            path.chmod(0o600)
            self.assertEqual(Q.read_private(path), b'{}\n')
            path.chmod(0o644)
            with self.assertRaisesRegex(Q.Refusal, 'private'):
                Q.read_private(path)
            path.chmod(0o600)
            linked = path.with_name('linked')
            os.link(path, linked)
            with self.assertRaises(Q.Refusal):
                Q.read_private(path)
            linked.unlink()
            path.write_bytes(b'x' * (Q.FILE_LIMIT + 1))
            with self.assertRaises(Q.Refusal):
                Q.read_private(path)

    def test_closed_json_refuses_duplicates_and_unknown_fields(self):
        for raw in (b'{"a":1,"a":1}', b'{"a":NaN}', b'{}{}'):
            with self.subTest(raw=raw), self.assertRaises(Q.Refusal):
                Q.decode(raw)
        with self.assertRaises(Q.Refusal):
            Q.closed({'a': 1, 'extra': 2}, {'a'}, 'test')

    def test_receipt_requires_actual_exact_subtests_and_cleanup(self):
        request = fake_request()
        receipt = fake_receipt(request)
        events = fake_events(request)
        Q.validate_receipt(request, receipt, events)
        for change in ('missing', 'skip', 'wrong_source', 'stale', 'cleanup', 'empty'):
            bad_receipt = json.loads(json.dumps(receipt))
            bad_events = json.loads(json.dumps(events))
            if change == 'missing':
                bad_events.pop(0)
            elif change == 'skip':
                bad_events[0]['Action'] = 'skip'
            elif change == 'wrong_source':
                bad_receipt['qualification_source_sha256'] = '0' * 64
            elif change == 'stale':
                bad_receipt['finished_at'] = request['lease_expires_at'] + 1
            elif change == 'cleanup':
                bad_receipt['cleanup']['objects'][0]['verified_absent'] = False
            else:
                bad_receipt['cases'] = []
            with self.subTest(change=change), self.assertRaises(Q.Refusal):
                Q.validate_receipt(request, bad_receipt, bad_events)

    def test_component_api_calls_use_production_signatures_and_real_packet(self):
        path = Q.ROOT / 'hack/e2e-kind.py'
        spec = importlib.util.spec_from_file_location('component_api_boundary', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for name in ('qualify_component_images', 'verify_component_images'):
            self.assertEqual(list(inspect.signature(getattr(module.Guard, name)).parameters),
                             ['self', 'qual_dir', 'packet', 'deadline'])
        lock = fake_registry_lock()
        images = Q.validate_inventory(lock)
        packet = {key: '1' * 64 for key in Q.PREPARATION_KEYS}
        packet.update(schema_version=2, qualification_id='2' * 32, run_id='3' * 32,
                      legacy_source_root=str(Q.ROOT), images={role: {'image': image} for role, image in images.items()})
        self.assertEqual(set(packet), Q.PREPARATION_KEYS)
        self.assertIsNone(packet['images']['native-runner']['image']['index_digest'])
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch('subprocess.run', side_effect=AssertionError('external process')), \
                mock.patch.object(Q.time, 'time', return_value=1000), \
                mock.patch.object(Q.time, 'monotonic', return_value=1000):
            output = Path(directory).resolve()
            guard = mock.create_autospec(module.Guard, instance=True, spec_set=True)
            guard.verify_component_images.return_value = mock.sentinel.verified_api_return
            self.assertIs(Q.component_images(guard, output, packet, (10000, 10000, 10000)),
                          mock.sentinel.verified_api_return)
            self.assertEqual(guard.method_calls, [
                mock.call.qualify_component_images(output, packet, 1900),
                mock.call.verify_component_images(output, packet, 1600)])
            guard.reset_mock()
            Q.component_images(guard, output, packet, (10000, 10000, 10000), verify_only=True)
            guard.qualify_component_images.assert_not_called()
            guard.verify_component_images.assert_called_once_with(output, packet, 1600)
            path = output / 'component-image-resolution-evidence.json'
            path.write_text('{}')
            for refusal in (None, Q.Refusal('partial receipt')):
                guard.reset_mock()
                guard.verify_component_images.side_effect = refusal
                if refusal:
                    with self.assertRaisesRegex(Q.Refusal, 'partial receipt'):
                        Q.component_images(guard, output, packet, (10000, 10000, 10000))
                else:
                    Q.component_images(guard, output, packet, (10000, 10000, 10000))
                guard.qualify_component_images.assert_not_called()
                guard.verify_component_images.assert_called_once_with(output, packet, 1600)

    def test_observations_bind_current_nodes_images_and_cleanup(self):
        request = fake_request()
        request['nodes'] = [{'name': f'node-{n}', 'uid': f'uid-{n}', 'container_id': str(n) * 64}
                            for n in range(3)]
        receipt = fake_receipt(request)
        image = {'ref': 'example.test/native@sha256:' + '3' * 64,
                 'digest': 'sha256:' + '3' * 64, 'config_digest': 'sha256:' + '4' * 64, 'variant': None}
        bindings = {node['name']: dict(node_id=node['container_id'], requested_ref=image['ref'],
            manifest_digest=image['digest'], config_digest=image['config_digest'], os='linux', architecture='arm64',
            variant=None, accepted_image_ids=sorted([image['ref'], image['digest'], image['config_digest']]))
            for node in request['nodes']}
        observations = []
        for number, node in enumerate(request['nodes']):
            observation = {field: {'observed': True} for field in
                           ('node_info', 'node_runtime', 'cri', 'runtime', 'resolver', 'home', 'provider')}
            observation.update(node_name=node['name'], node_uid=node['uid'],
                               node_container_id=node['container_id'],
                               job_name=f'native-metadata-{number}', job_uid=f'job-{number}',
                               pod_name=f'node-{number}', pod_uid=f'pod-{number}',
                               container_id='containerd://' + str(number) * 64,
                               image_ref=image['ref'], image_id=image['config_digest'],
                               logs_eof=True, logs_closed=True, terminal_exit_code=0)
            active = active_profile_fixture()
            active['cri'].update(container_id=str(number) * 64, pod_uid=f'pod-{number}')
            observation.update(node_runtime=active['node_runtime'], cri=active['cri'])
            observation['runtime'].update(image_files=image_files(), observation_only=True)
            observation['provider']['authentication_qualified'] = False
            observations.append(observation)
        Q.validate_observations(request, receipt, observations, image, bindings)
        for field, value in [('node_uid', 'changed'), ('image_id', 'changed'),
                             ('pod_uid', 'changed'), ('logs_eof', False),
                             ('runtime', {})]:
            changed = json.loads(json.dumps(observations))
            changed[0][field] = value
            with self.subTest(field=field), self.assertRaises(Q.Refusal):
                Q.validate_observations(request, receipt, changed, image, bindings)

    def test_existing_legacy_alias_method_is_not_component_authority(self):
        guard = type('LegacyOnly', (), {
            'ensure_loaded_index_alias': lambda *args: self.fail('legacy alias invoked')})()
        with self.assertRaisesRegex(Q.Refusal, 'independent component image qualification API'):
            Q.component_images(guard, Path('/unused'), {}, 1600)

    def test_readonly_legacy_save_requires_byte_equality(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve() / 'ownership.json'
            data = {'schema': 1}
            raw = (json.dumps(data, indent=2) + '\n').encode()
            path.write_bytes(raw)
            path.chmod(0o600)
            guard = Q.readonly_guard_type(type('Base', (), {}))()
            guard.directory = Path(directory).resolve()
            guard.save(data)
            self.assertEqual(path.read_bytes(), raw)
            with self.assertRaisesRegex(Q.Refusal, 'normal M7 CHECK'):
                guard.save({'schema': 2})
            path.write_text('{"schema":1}\n')
            with self.assertRaises(Q.Refusal):
                guard.save(data)

    def test_stage_budget_reserves_cleanup(self):
        for key in ('expiry', 'parent_deadline', 'parent_monotonic_deadline'):
            for invalid in (None, True, float('nan'), float('inf'), 2799):
                arguments = dict(expiry=10000, now=1000, parent_deadline=10000,
                                 parent_monotonic_deadline=10000, monotonic_now=1000)
                arguments[key] = invalid
                with self.subTest(key=key, invalid=invalid), self.assertRaisesRegex(Q.Refusal, 'cleanup budget'):
                    Q.stage_deadline(**arguments)

    def test_old_archive_inventory_is_not_execution_authority(self):
        inventory = Q.decode(Q.read_file(Q.ROOT / 'hack/testdata/native-runtime-images-schema1.json'))
        with self.assertRaises(Q.Refusal):
            Q.validate_inventory(inventory)


class RegistryEnvelopeBoundaries(unittest.TestCase):
    def test_prepare_refuses_parent_before_stable_pulled_ownership(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary).resolve(); run = base / 'run'; qual = base / 'qual'
            run.mkdir(mode=0o700); qual.mkdir(mode=0o700)
            Q.write_private(run / 'ownership.json', {'unit_fixture': True})
            Q.write_private(run / 'lifecycle.lock', {})
            guard = mock.Mock(directory=run)
            guard.read.return_value = dict(run_id='2' * 32, phase='ready')
            guard.source_tree.return_value = '3' * 64
            with mock.patch.object(Q, 'load_guard', return_value=(SimpleNamespace(_qualification_source_sha256='4' * 64), guard)), \
                    mock.patch.object(Q, 'prepare_assets', return_value=('5' * 64, {})) as prepare, \
                    self.assertRaisesRegex(Q.Refusal, 'public image pull/bootstrap'):
                Q.qualify(run, Path('/private/unit-source'), qual, 'prepare')
            prepare.assert_not_called()
            guard._check.assert_not_called()
            self.assertFalse((qual / 'native-assets.json').exists())

    def test_real_component_api_accepts_schema2_packet_and_distinct_stage_deadlines(self):
        path = Q.ROOT / 'hack/e2e-kind.py'
        spec = importlib.util.spec_from_file_location('registry_component_api_boundary', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary).resolve(); run = base / 'run'; qual = base / 'qual'
            run.mkdir(mode=0o700); qual.mkdir(mode=0o700)
            ownership = Q.write_private(run / 'ownership.json', {'unit_fixture': True})
            lock = fake_registry_lock(); lock_raw = Q.write_private(run / 'image-lock.json', lock)
            ledger = dict(run_id='2' * 32, phase='pulled', resource_lease=dict(expires_at=10000, monotonic_expires_at=11000))
            packet = dict(schema_version=2, qualification_id='1' * 32, run_id=ledger['run_id'],
                legacy_source_root=str(module.ROOT), legacy_source_tree_sha256='3' * 64,
                legacy_guard_sha256=Q.sha(path.read_bytes()), legacy_ownership_sha256=Q.sha(ownership),
                qualification_source_sha256='4' * 64, evidence_program_sha256='5' * 64,
                image_lock_sha256=Q.sha(lock_raw), images={role: {'image': lock['images'][role]} for role in Q.ROLES})
            Q.write_private(qual / 'native-assets.json', packet)
            guard = SimpleNamespace(directory=run, private=lambda *args, **kwargs: None,
                external_bytes=lambda path, **kwargs: Q.read_private(path), private_bytes=lambda name: Q.read_private(run / name),
                private_json=lambda name: Q.decode(Q.read_private(run / name)), read=lambda: ledger,
                source_tree=lambda: '3' * 64, registry_admit=lambda ledger: None,
                pulled_image_binding=lambda ledger, role: None, _component_admit=lambda context: None,
                validate_matrix=lambda *args: None)
            guard._component_context = lambda *args: module.Guard._component_context(guard, *args)
            guard.component_evidence = lambda context: module.Guard.component_evidence(guard, context)
            with mock.patch.object(module.time, 'time', return_value=1000), \
                    mock.patch.object(module.time, 'monotonic', return_value=1000):
                context = guard._component_context(qual, packet, 1900)
                proof = guard.component_evidence(context); proof['complete'] = True
                Q.write_private(qual / 'component-image-resolution-evidence.json', proof)
                self.assertEqual(module.Guard.verify_component_images(guard, qual, packet, 1600), proof)
            self.assertEqual(proof['deadline'], 1900)

    def test_schema2_lock_selects_exact_native_public_roles(self):
        lock = fake_registry_lock()
        self.assertEqual(Q.validate_inventory(lock), {role: lock['images'][role] for role in Q.ROLES})
        self.assertEqual(Q.required_cases('prepare', None), ['direct-pull/' + role for role in Q.ROLES])

    def test_schema2_lock_refuses_old_private_or_unbound_inputs(self):
        for mutation in ('schema', 'archive', 'missing', 'private', 'pg-major', 'source', 'manifest', 'index'):
            lock = fake_registry_lock()
            image = lock['images']['native-runner']
            if mutation == 'schema': lock['schema_version'] = 1
            elif mutation == 'archive': image['archive'] = 'native-runner.tar'
            elif mutation == 'missing': del lock['images']['postgresql-18']
            elif mutation == 'private': image['provenance']['public_receipt_sha256'] = None
            elif mutation == 'pg-major': lock['images']['postgresql-18']['ref'] = lock['images']['postgresql-16']['ref']
            elif mutation == 'source': image['source']['payload_inventory_sha256'] = 'f' * 64
            elif mutation == 'manifest': image['digest'] = image['config_digest']
            elif mutation == 'index': image['index_digest'] = image['digest']
            with self.subTest(mutation=mutation), self.assertRaises(Q.Refusal):
                Q.validate_inventory(lock)

    def test_prepare_binds_exact_retained_lock_without_archive_access(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            raw = Q.write_private(directory / 'image-lock.json', fake_registry_lock())
            guard = SimpleNamespace(directory=directory, source_tree=lambda: '3' * 64,
                inventory=lambda: list(fake_registry_lock()['images'].values()))
            digest, images = Q.prepare_assets(guard)
            self.assertEqual(digest, Q.sha(raw))
            self.assertEqual(images, {role: {'image': fake_registry_lock()['images'][role]} for role in Q.ROLES})
            guard.source_tree = lambda: 'e' * 64
            with self.assertRaises(Q.Refusal):
                Q.prepare_assets(guard)


class DeliveryBoundaries(unittest.TestCase):
    GOOD = {
        'quiet_success': (True, None, True, 60000, 0),
        'concurrent_relay_refused': (True, False, True, 64000, 0),
        'foreign_lock_refused': (False, None, False, 0, 1),
        'truncated_result_refused': (False, None, True, 0, 0),
        'stderr_refused': (False, None, False, 0, 1),
        'startup_log_refused': (True, None, False, 64000, 0),
        'main_failure_refused': (True, None, False, 64000, 3),
        'disconnect_no_replay': (False, False, True, 0, 0),
    }

    @staticmethod
    def observed(relay, second, completed, payload, code):
        return dict(relay_successful=relay, second_relay_successful=second, job_completed_native=completed,
                    composite_authority=relay and completed is True, payload_bytes=payload, main_exit_code=code)

    def fixture(self):
        runtime = fake_request()
        runtime.update(phase='delivery', required_cases=Q.required_cases('delivery', None), namespace='native-delivery-test')
        request = dict(schema_version=1, runtime=runtime, delivery_program_sha256='6' * 64, original_parent_deadline=2200)
        cases = [dict(name='delivery/' + name, status='passed', exit_code=0, cleanup=True, observed=self.observed(*values))
                 for name, values in self.GOOD.items()]
        cases.append(dict(name='pvc/fsgroup_readonly_remount', status='passed', exit_code=0, cleanup=True,
                          observed=self.observed(False, None, None, 0, None)))
        objects = [dict(api_version='v1', kind='Namespace', namespace='', name='native-delivery-test', uid='namespace-uid',
                        resource_version='1', action='deleted', verified_absent=True),
                   dict(api_version='v1', kind='PersistentVolumeClaim', namespace='native-delivery-test', name='delivery-work',
                        uid='claim-uid', resource_version='1', action='deleted', verified_absent=True)]
        for number in range(10):
            for kind in ('Job', 'Pod'):
                objects.append(dict(api_version='batch/v1' if kind == 'Job' else 'v1', kind=kind, namespace='native-delivery-test',
                                    name=f'{kind.lower()}-{number}', uid=f'{kind}-{number}', resource_version='1',
                                    action='deleted', verified_absent=True))
        receipt = dict(schema_version=1, kind='delivery', qualification_id=runtime['qualification_id'], run_id=runtime['run_id'],
                       input_sha256=Q.sha((json.dumps(request, sort_keys=True, separators=(',', ':')) + '\n').encode()),
                       qualification_source_sha256=runtime['qualification_source_sha256'],
                       evidence_program_sha256=runtime['evidence_program_sha256'], native_assets_sha256=runtime['native_assets_sha256'],
                       delivery_program_sha256='6' * 64,
                       relay_program_sha256=Q.sha((Q.ROOT / 'internal/native/evidence_request_writer.py').read_bytes()),
                       namespace_uid='namespace-uid', started_at=1500, finished_at=1700, cases=cases,
                       cleanup=dict(complete=True, objects=objects))
        events = [dict(Action='pass', Test=Q.DELIVERY_TEST + '/' + name) for name in runtime['required_cases']]
        events.append(dict(Action='pass', Test=Q.DELIVERY_TEST))
        return request, receipt, events

    def test_delivery_inventory_is_fixed_and_driver_owned(self):
        self.assertEqual(Q.required_cases('delivery', None)[-1], 'pvc/fsgroup_readonly_remount')
        self.assertEqual(len(Q.required_cases('delivery', None)), 9)
        with self.assertRaises(Q.Refusal):
            Q.required_cases('delivery', (16, 16))
        source = (Q.ROOT / Q.DELIVERY_FILE).read_text()
        # The Go inventory names each case once as a constant, in the same order as the driver.
        order = source.split('var nativeDeliveryOrder = []string{', 1)[1].partition('}')[0].split(',')
        constants = {key.strip(): value.strip() for key, _, value in (line.partition('=') for line in
                     source.split('const (', 1)[1].partition(')')[0].strip().splitlines())}
        self.assertEqual([constants[item.strip()].strip('"') for item in order if item.strip()], list(Q.DELIVERY_CASES))

    def test_delivery_program_is_the_go_fixture_and_fits_its_argument(self):
        program = Q.delivery_program()
        self.assertLessEqual(len(program), 122880)
        self.assertIn(b"P.InputChannel(op,start+30)", program)
        self.assertNotIn((Q.ROOT / 'internal/native/evidence_program.py').read_bytes(), program)

    def test_valid_delivery_receipt_has_exactly_two_authorities(self):
        request, receipt, events = self.fixture()
        Q.validate_delivery_receipt(request, receipt, events)
        self.assertEqual([item['observed']['composite_authority'] for item in receipt['cases'][:-1]].count(True), 2)

    def test_composite_must_equal_both_checked_results(self):
        for name, values in self.GOOD.items():
            observed = self.observed(*values)
            observed['composite_authority'] = not observed['composite_authority']
            with self.subTest(case=name), self.assertRaisesRegex(Q.Refusal, 'composite'):
                Q.delivery_expected(name, observed)

    def test_false_authority_and_partial_receipts_refuse(self):
        mutations = {
            'truncated_claims_authority': lambda r, e: r['cases'][3]['observed'].update(relay_successful=True, composite_authority=True),
            'composite_without_main': lambda r, e: r['cases'][0]['observed'].update(job_completed_native=False),
            'forged_composite': lambda r, e: r['cases'][5]['observed'].update(composite_authority=True),
            'replay_succeeded': lambda r, e: r['cases'][7]['observed'].update(second_relay_successful=True),
            'second_concurrent_relay': lambda r, e: r['cases'][1]['observed'].update(second_relay_successful=True),
            'small_payload': lambda r, e: r['cases'][0]['observed'].update(payload_bytes=256),
            'failed_case': lambda r, e: r['cases'][2].update(status='refused', exit_code=1),
            'missing_case': lambda r, e: r['cases'].pop(),
            'skipped_go_case': lambda r, e: e.append(dict(Action='skip', Test=Q.DELIVERY_TEST + '/pvc/fsgroup_readonly_remount')),
            'cleanup_retained': lambda r, e: r['cleanup']['objects'][3].update(action='retained', verified_absent=False),
            'cleanup_missing': lambda r, e: r['cleanup']['objects'].pop(),
            'stale_relay': lambda r, e: r.update(relay_program_sha256='7' * 64),
            'late_receipt': lambda r, e: r.update(finished_at=2201),
        }
        for name, mutate in mutations.items():
            request, receipt, events = self.fixture()
            mutate(receipt, events)
            with self.subTest(mutation=name), self.assertRaises(Q.Refusal):
                Q.validate_delivery_receipt(request, receipt, events)


if __name__ == '__main__':
    with mock.patch('subprocess.run', side_effect=AssertionError('external process forbidden')):
        unittest.main()

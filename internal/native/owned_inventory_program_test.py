import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

if sys.flags.optimize:
    raise SystemExit('optimized tests refused')

HERE = Path(__file__).resolve().parent
CORE = (HERE / 'evidence_program.py').read_bytes()
WRITER = (HERE / 'evidence_request_writer.py').read_bytes()
SCOPE = {'__name__': '_owned_inventory_test'}
exec(compile(CORE, '<canonical>', 'exec'), SCOPE)
HELPER = HERE / 'owned_inventory_program.py'
if HELPER.is_file():
    exec(compile(HELPER.read_bytes(), '<inventory>', 'exec'), SCOPE)
SPEC = importlib.util.spec_from_file_location('_inventory_existing_fixtures', HERE / 'evidence_program_test.py')
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)
PHASES = 'fresh pre_data_pending copying post_data_pending post_data_complete sequences_pending sequences_complete analyzing complete'.split()
SEPARATOR = b'\n# RPGMIGRATE_NATIVE_OWNED_INVENTORY_V1\n'


def request():
    req = FIXTURE.source_request('owned_inventory')
    req['execution']['role'] = 'cleanup'
    req['execution']['job_uid'] = '44445555-6666-7777-aaaa-bbbbccccdddd'
    req['program_sha256'] = '6' * 64
    return req


def manifest(info, phase='analyzing', table=False):
    _, value, _, _ = FIXTURE.private_fixture(info)
    req = request()
    value['operator']['workdir_id'] = req['execution']['work']['workdir_id']
    clone = value['clone_state']
    clone['phase'] = phase
    if table:
        item = dict(source_oid=101, name=dict(schema='public', table='records'), writable_columns=['id'], all_columns=['id'], identity_modes=[0], partition_identity_modes=None)
        clone['plan']['tables'] = [item]
        if phase not in ('fresh', 'pre_data_pending'):
            clone['owned_objects'] = [dict(class_oid=1259, object_oid=102, object_sub_id=0, namespace_oid=103, namespace_name='public', name='records', kind='r', owner_oid=33)]
            clone['owned_tables'] = [dict(table=copy.deepcopy(item), target_oid=102, target_namespace_oid=103, target_owner_oid=33, generation=1)]
    if phase == 'complete':
        clone['completion'] = dict(tables=len(clone['owned_tables']), rows=4, bytes=16)
    return req, value


class PrivateTree:
    def __init__(self, phase='analyzing', table=False, control=None):
        self.phase, self.table, self.control = phase, table, control

    def __enter__(self):
        self.temp = tempfile.TemporaryDirectory()
        self.parent = Path(self.temp.name) / 'work'
        self.parent.mkdir(mode=0o700)
        self.root = self.parent / 'pgcopydb'
        self.root.mkdir(mode=0o700)
        self.req, self.value = manifest(self.root.stat(), self.phase, self.table)
        self.write()
        if self.control is not None:
            control = self.root / 'control'
            control.mkdir(mode=0o700)
            for name, raw in self.control.items():
                path = control / name
                path.write_bytes(raw)
                path.chmod(0o600)
        original_os, original_open = os, SCOPE['fdopen']
        parent = self.parent

        class FileAPI:
            def __getattr__(self, name):
                return getattr(original_os, name)

            def stat(self, path, *args, **kwargs):
                return original_os.stat(parent if path == '/work' else path, *args, **kwargs)

        def open_file(path, directory=False, parent=None):
            return original_open(self.parent if path == '/work' else path, directory, parent)

        work = self.req['execution']['work']
        env = {name: str(work[field]) for name, field in [('RPGMIGRATE_MIGRATION_UID', 'migration_uid'), ('RPGMIGRATE_RUN_ID', 'run_id'), ('RPGMIGRATE_WORKDIR_ID', 'workdir_id'), ('RPGMIGRATE_CHECKPOINT_GENERATION', 'checkpoint_generation')]}
        self.scope = mock.patch.dict(SCOPE, os=FileAPI(), fdopen=open_file)
        self.env = mock.patch.dict(os.environ, env)
        self.scope.__enter__()
        self.env.__enter__()
        return self

    def write(self):
        path = self.root / 'manifest.json'
        path.write_bytes(json.dumps(self.value).encode())
        path.chmod(0o600)

    def __exit__(self, *args):
        self.env.__exit__(*args)
        self.scope.__exit__(*args)
        self.temp.cleanup()


class InventoryPQ:
    # The real Session owns parsing, transaction checks, notices and client closure.
    def __init__(self, fault=None):
        self.fault = fault
        self.next = 0
        self.roles = {}
        self.closed = []
        self.queries = []
        self.results = {}
        self.buffers = []
        self.callback = {}

    def PQconnectStartParams(self, keywords, values, expand):
        self.next += 1
        self.roles[self.next] = keywords
        return self.next

    def PQsetNoticeReceiver(self, conn, callback, _): self.callback[conn] = callback
    def PQconnectPoll(self, conn): return 3
    def PQsetnonblocking(self, conn, value): return 0
    def PQstatus(self, conn): return 0
    def PQtransactionStatus(self, conn): return 0
    def PQbackendPID(self, conn): return 23 + conn
    def PQhost(self, conn): return b'fixture'
    def PQhostaddr(self, conn): return b'127.0.0.1'
    def PQport(self, conn): return b'5432'
    def PQdb(self, conn): return b'fixture'
    def PQuser(self, conn): return b'fixture'
    def PQparameterStatus(self, conn, key): return b'UTF8'
    def PQsslInUse(self, conn): return 0
    def PQconninfo(self, conn): return (SCOPE['ConnOption'] * 1)()
    def PQconninfoFree(self, options): pass
    def PQflush(self, conn): return 0
    def PQconsumeInput(self, conn): return 1
    def PQisBusy(self, conn): return 0

    def PQsendQueryParams(self, conn, raw, count, _, params, *args):
        sql = raw.decode()
        role = self.roles[conn]
        self.queries.append((role, sql, [params[i] for i in range(count)]))
        if sql.startswith('SET '): result = dict(status=1, tag=b'SET', count=b'', rows=[], width=0)
        else:
            if 'CURRENT_USER, SESSION_USER' in sql: rows = [[b'fixture', b'fixture', b'fixture', b'f']]
            elif 'pg_control_system' in sql:
                if role == 'source': rows = [[b'123', b'11', b'1', b'160006', str(23 + conn).encode(), b'33', b'33', b't', b'off']]
                else: rows = [[b'456', b'22', b'33', b'33', str(23 + conn).encode(), b'180006', b't']]
                if self.fault == role + '-identity': rows[0][0] = b'999'
            else:
                rows = [[b'f' if self.fault and self.fault in sql else b't']]
                if self.fault == 'target-catalog' and role == 'target' and 'pg_class' in sql: rows = [[b'f']]
            result = dict(status=2, tag=b'SELECT 1', count=b'1', rows=rows, width=len(rows[0]))
            if self.fault == 'unknown-commit': result['tag'] = b'COMMIT'
            if self.fault == 'notice': self.callback[conn](None, None)
        self.results[conn] = [result]
        return 1

    def PQgetResult(self, conn): return self.results[conn].pop(0) if self.results[conn] else None
    def PQresultStatus(self, result): return result['status']
    def PQresultErrorField(self, result, field): return None
    def PQcmdStatus(self, result): return result['tag']
    def PQcmdTuples(self, result): return result['count']
    def PQntuples(self, result): return len(result['rows'])
    def PQnfields(self, result): return result['width']
    def PQgetisnull(self, result, row, col): return int(result['rows'][row][col] is None)
    def PQgetlength(self, result, row, col): return len(result['rows'][row][col])
    def PQgetvalue(self, result, row, col):
        buffer = SCOPE['C'].create_string_buffer(result['rows'][row][col])
        self.buffers.append(buffer)
        return SCOPE['C'].addressof(buffer)
    def PQclear(self, result): self.buffers.clear()
    def PQfinish(self, conn):
        self.closed.append(self.roles[conn])
        if self.fault == self.roles[conn] + '-close': raise OSError('private-close-canary')


class Materials:
    def __init__(self): self.closes = []; self.checks = 0
    def check(self): self.checks += 1
    def connection(self, role):
        return types.SimpleNamespace(claim=lambda: (role, None), check=self.check, close=lambda: self.closes.append(role), overrides={})


def result(req, body, origin=False):
    digest, execution = SCOPE['request_digests'](body)
    resources = [dict(kind='replication_slot', name='native_slot', owned=True, retain=False), dict(kind='publication', name='native_publication', owned=True, retain=False), dict(kind='workdir', name='/work/pgcopydb', owned=True, retain=True)]
    if origin: resources.append(dict(kind='replication_origin', name='external_origin', owned=False, retain=False))
    return dict(version=1, kind='owned_inventory', operation_id=req['operation_id'], program_sha256=req['program_sha256'], request_sha256=digest, execution_sha256=execution, work=req['execution']['work'], resources=resources, source_closed=True, target_closed=True, materialization_closed=True, private_closed=True, diagnostics_clean=True)


def relay_scope():
    scope = {'__name__': '_owned_inventory_writer_test'}
    exec(compile(WRITER, '<writer>', 'exec'), scope)
    exec(compile((HERE / 'owned_inventory_request_writer.py').read_bytes(), '<policy>', 'exec'), scope)
    return scope


class InventoryTests(unittest.TestCase):
    def required(self, name):
        self.assertTrue(name in SCOPE, 'owned inventory implementation is missing: ' + name)
        return SCOPE[name]

    def test_InventoryConfirmedPhases(self):
        validate = self.required('validate_inventory_manifest')
        with tempfile.TemporaryDirectory() as directory:
            info = os.stat(directory)
            for phase in PHASES:
                for table in (False, True):
                    req, value = manifest(info, phase, table)
                    for progress in (None, dict(slot_removed=False, publication_removed=False), dict(slot_removed=True, publication_removed=False), dict(slot_removed=False, publication_removed=True), dict(slot_removed=True, publication_removed=True)):
                        value['follow_state']['cleanup'] = progress
                        with self.subTest(phase=phase, table=table, cleanup=progress):
                            self.assertEqual(validate(value, req, info), value['target_provenance'][0]['record'])

    def test_InventoryPendingRefusals(self):
        validate = self.required('validate_inventory_manifest')
        with tempfile.TemporaryDirectory() as directory:
            req, original = manifest(os.stat(directory), table=True)
            mutations = [
                lambda m: m.update(follow_state=None),
                lambda m: m['follow_state'].update(binding=None),
                lambda m: m['follow_state'].update(restart={}),
                lambda m: m['follow_state'].update(preliminary={}),
                lambda m: m['source_slot'].update(phase='provisioning'),
                lambda m: m.update(target_provenance=[]),
                lambda m: m['target_provenance'][0].update(phase='provisioning'),
                lambda m: m['follow_state']['publication'].update(phase='created'),
                lambda m: m['follow_state']['publication'].update(receipt=dict(name='native_publication', oid=89, owner_oid=33)),
                lambda m: m['clone_state']['plan'].update(generation=2),
                lambda m: m['clone_state'].update(phase='fresh'),
                lambda m: m['clone_state'].update(phase='complete'),
                lambda m: m['clone_state'].update(completion=dict(tables=1, rows=1, bytes=1)),
                lambda m: m['clone_state']['owned_tables'][0].update(generation=2),
                lambda m: m['clone_state']['owned_objects'][0].update(namespace_oid=44),
                lambda m: m['clone_state']['owned_objects'].append(copy.deepcopy(m['clone_state']['owned_objects'][0])),
                lambda m: m['clone_state']['owned_tables'].clear(),
                lambda m: m['follow_state'].update(cleanup=dict(slot_removed=1, publication_removed=False)),
                lambda m: m['operator'].update(workdir_id='foreign'),
                lambda m: m.update(format_version=7),
                lambda m: m.update(extra=True),
            ]
            for index, mutate in enumerate(mutations):
                value = copy.deepcopy(original); mutate(value)
                with self.subTest(case=index), self.assertRaises(SCOPE['Refusal']): validate(value, req, os.stat(directory))

    def test_InventoryNoEndposAuthority(self):
        state_type = self.required('InventoryState')
        for control in (None, {}, {'lock': b''}, {'lock': b'', 'endpos.json': b'unsealed or absent E is not inventory authority'}):
            with PrivateTree(phase='copying', table=True, control=control) as fixture:
                before = (fixture.root / 'manifest.json').read_bytes()
                state = state_type(fixture.req)
                self.assertEqual(len(state.resources()), 3)
                state.revalidate(); state.close()
                self.assertEqual((fixture.root / 'manifest.json').read_bytes(), before)
                self.assertEqual(set(os.listdir(fixture.root)), {'manifest.json'} | ({'control'} if control is not None else set()))
        for name in ('.manifest.tmp', 'foreign'):
            with PrivateTree() as fixture:
                (fixture.root / name).write_bytes(b'private')
                with self.assertRaises(SCOPE['Refusal']): state_type(fixture.req)
        with PrivateTree(control={'lock': b'', '.endpos.tmp': b''}) as fixture:
            with self.assertRaises(SCOPE['Refusal']): state_type(fixture.req)
        with PrivateTree() as fixture:
            state = state_type(fixture.req)
            fixture.write()
            with self.assertRaises(SCOPE['Refusal']): state.revalidate()
            state.close(False)
        with PrivateTree() as fixture:
            fd = os.open(fixture.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                SCOPE['fcntl'].flock(fd, SCOPE['fcntl'].LOCK_EX | SCOPE['fcntl'].LOCK_NB)
                with self.assertRaises(BlockingIOError): state_type(fixture.req)
            finally: os.close(fd)
        with PrivateTree() as fixture:
            fixture.value['follow_state']['origin_name'] = 'external_origin'; fixture.write()
            state = state_type(fixture.req)
            self.assertEqual(state.resources()[-1], dict(kind='replication_origin', name='external_origin', owned=False, retain=False))
            state.close()

    def test_InventorySourceReplacement(self):
        observe = self.required('observe_inventory')
        for fault in (None, 'source-identity', 'target-identity', 'pg_replication_slots', 'pg_publication', 'pg_publication_tables', 'target-catalog'):
            with PrivateTree() as fixture:
                state = SCOPE['InventoryState'](fixture.req)
                library, materials = InventoryPQ(fault), Materials()
                try:
                    if fault is None: observe(state, library, materials, time.monotonic() + 5, types.SimpleNamespace(check=lambda: None))
                    else:
                        with self.assertRaises(SCOPE['Refusal']): observe(state, library, materials, time.monotonic() + 5, types.SimpleNamespace(check=lambda: None))
                    self.assertNotEqual(library.closed, [])
                    self.assertEqual(sorted(library.closed), sorted(materials.closes))
                    for _, sql, _ in library.queries:
                        self.assertTrue(sql.startswith(('SET ', 'SELECT ')))
                        self.assertFalse(any(word in sql.upper() for word in ('FOR UPDATE', 'ADVISORY', 'COMMIT;', 'UPDATE ONLY', 'DROP ')))
                finally: state.close()

    def test_InventoryClosedSessions(self):
        observe = self.required('observe_inventory')
        for fault in (None, 'source-close', 'target-close', 'notice', 'unknown-commit'):
            with PrivateTree() as fixture:
                state = SCOPE['InventoryState'](fixture.req)
                library, materials = InventoryPQ(fault), Materials()
                try:
                    if fault is None:
                        observe(state, library, materials, time.monotonic() + 5, types.SimpleNamespace(check=lambda: None))
                        self.assertEqual(sorted(library.closed), ['source', 'target'])
                    else:
                        with self.assertRaises((SCOPE['Refusal'], OSError)): observe(state, library, materials, time.monotonic() + 5, types.SimpleNamespace(check=lambda: None))
                    self.assertEqual(sorted(library.closed), sorted(materials.closes))
                finally: state.close()
        with self.assertRaises(SCOPE['Refusal']) as error: SCOPE['qualified_resolver_policy']()
        self.assertEqual(error.exception.error_class, 'capability')
        with PrivateTree() as fixture:
            state = SCOPE['InventoryState'](fixture.req); library, materials = InventoryPQ(), Materials()
            try:
                with self.assertRaises(SCOPE['Refusal']) as error: observe(state, library, materials, time.monotonic() - 1, types.SimpleNamespace(check=lambda: None))
                self.assertEqual(error.exception.error_class, 'timeout'); self.assertEqual(library.queries, [])
                self.assertEqual(library.closed, ['source']); self.assertEqual(materials.closes, ['source'])
            finally: state.close()

    def test_InventoryCompositeDelivery(self):
        self.required('owned_inventory_main')
        self.assertEqual(hashlib.sha256(CORE).hexdigest(), 'd1b311f2930955dd6ed6d8085786ab8f0f7db52581d30b0a7e1c292c98ebdbe8')
        self.assertEqual(hashlib.sha256(WRITER).hexdigest(), '4412adaaa55dbd69aff1ad754c7252562da32440e0f2d2a07e60205d4fb6b882')
        helper = HELPER.read_bytes(); adapter = (HERE / 'owned_inventory_request_writer.py').read_bytes()
        self.assertLessEqual(len(helper), 16384); self.assertLessEqual(len(adapter), 3072)
        self.assertTrue(helper.endswith(b'\n') and adapter.endswith(b'\n'))
        self.assertNotIn(SEPARATOR, helper); self.assertNotIn(SEPARATOR, CORE)
        req = request(); op = os.urandom(16).hex(); req['operation_id'] = op
        body = json.dumps(req).encode(); payload = json.dumps(result(req, body)).encode() + b'\n'
        channel = SCOPE['InputChannel'](op, time.monotonic() + 5)
        script = "import sys,types\nm=types.ModuleType('_inventory_relay')\nexec(" + repr(WRITER) + ",m.__dict__)\nexec(" + repr(adapter) + ",m.__dict__)\nraise SystemExit(m.main(sys.argv))\n"
        env = dict(os.environ, LANG='C', LC_ALL='C')
        for key in list(env):
            if key.startswith('LD_') or key in SCOPE['BANNED']: del env[key]
        process = subprocess.Popen([sys.executable, '-I', '-B', '-c', script, op], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        failures = []
        def receive():
            try:
                self.assertEqual(channel.consume(), body); channel.finish(payload)
            except BaseException as error: failures.append(repr(error))
        thread = threading.Thread(target=receive); thread.start()
        stdout, stderr = process.communicate(body, timeout=7); thread.join(7)
        self.assertFalse(thread.is_alive()); self.assertEqual(failures, [])
        self.assertEqual((process.returncode, stdout, stderr), (0, payload, b''))
        op = os.urandom(16).hex(); req['operation_id'] = op; body = json.dumps(req).encode()
        env['PGCONNECT_TIMEOUT'] = '10'
        for name, field in [('RPGMIGRATE_MIGRATION_UID', 'migration_uid'), ('RPGMIGRATE_RUN_ID', 'run_id'), ('RPGMIGRATE_WORKDIR_ID', 'workdir_id'), ('RPGMIGRATE_CHECKPOINT_GENERATION', 'checkpoint_generation')]: env[name] = str(req['execution']['work'][field])
        producer = "import os,sys\ns={'__name__':'_inventory_main_test'}\nexec(" + repr(CORE) + ",s)\nexec(" + repr(helper) + ",s)\nos._exit(s['owned_inventory_main'](sys.argv))\n"
        main = subprocess.Popen([sys.executable, '-I', '-B', '-c', producer, 'owned_inventory', op, req['program_sha256']], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        relay = subprocess.run([sys.executable, '-I', '-B', '-c', script, op], input=body, capture_output=True, env=env, timeout=7)
        main_stdout, main_stderr = main.communicate(timeout=7)
        self.assertEqual((main.returncode, main_stdout, main_stderr), (1, b'', b''))
        self.assertEqual((relay.returncode, relay.stdout, relay.stderr), (1, b'', b''))

    def test_InventoryRelayPolicy(self):
        self.required('validate_inventory_request')
        relay = relay_scope(); req = request(); body = json.dumps(req).encode(); valid = result(req, body)
        validate = SCOPE['validate_inventory_request']
        self.assertEqual(validate(body, req['operation_id'], req['program_sha256']), req)
        for origin in (False, True):
            self.assertTrue(relay['result_envelope'](json.dumps(result(req, body, origin)).encode() + b'\n', body, req['operation_id']))
        for key in ('source_closed', 'target_closed', 'materialization_closed', 'private_closed', 'diagnostics_clean', 'program_sha256', 'request_sha256', 'execution_sha256', 'operation_id', 'work', 'resources'):
            value = copy.deepcopy(valid); value[key] = False
            with self.subTest(key=key), self.assertRaises(Exception): relay['result_envelope'](json.dumps(value).encode() + b'\n', body, req['operation_id'])
        for mutate in (lambda r: r.update(saved_e='0/20'), lambda r: r.update(kind='completed_evidence'), lambda r: r['execution'].update(role='worker'), lambda r: r['execution'].update(job_uid=r['execution']['worker_job_uid']), lambda r: r['execution'].update(attempt=0), lambda r: r['execution']['work'].update(checkpoint_generation=2), lambda r: r['container'].update(restart_count=1), lambda r: r['execution'].update(extra='private')):
            value = copy.deepcopy(req); mutate(value); raw = json.dumps(value).encode()
            with self.assertRaises(Exception): validate(raw, req['operation_id'], req['program_sha256'])
            with self.assertRaises(Exception): relay['strict_body'](raw, req['operation_id'])
        for mutate in (lambda r: r.update(checkpoint={}), lambda r: r.update(kind='completed_evidence'), lambda r: r['resources'].clear(), lambda r: r['resources'].append(copy.deepcopy(r['resources'][0])), lambda r: r['resources'][0].update(retain=True), lambda r: r['resources'][0].update(owned=False), lambda r: r['resources'][2].update(name='/foreign')):
            value = copy.deepcopy(valid); mutate(value)
            with self.assertRaises(Exception): relay['result_envelope'](json.dumps(value).encode() + b'\n', body, req['operation_id'])
        payload = json.dumps(valid).encode() + b'\n'
        value = copy.deepcopy(valid); value['work']['checkpoint_generation'] = True
        with self.assertRaises(Exception): relay['result_envelope'](json.dumps(value).encode() + b'\n', body, req['operation_id'])
        for raw in (payload[:-1], payload + b'\n', payload + b'{}', b'null\n', payload.replace(b'"version": 1', b'"version": 1.0'), payload[:-2] + b',"version":1}\n'):
            with self.assertRaises(Exception): relay['result_envelope'](raw, body, req['operation_id'])
        for raw in (body.replace(b'"restart_count": 0', b'"restart_count": -0'), body[:-1] + b',"version":1}', body + b'{}'):
            with self.assertRaises(Exception): validate(raw, req['operation_id'], req['program_sha256'])
            with self.assertRaises(Exception): relay['strict_body'](raw, req['operation_id'])


if __name__ == '__main__':
    unittest.main(verbosity=2)

import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import types
import unittest
from unittest import mock

if sys.flags.optimize:
    raise SystemExit('optimized tests refused')

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('_cleaned_inventory_fixture', HERE / 'owned_inventory_program_test.py')
I = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(I)
S = I.SCOPE
HELPER = HERE / 'cleaned_completion_program.py'
if HELPER.is_file():
    exec(compile(HELPER.read_bytes(), '<cleaned>', 'exec'), S)


def request():
    req = I.request()
    req['kind'] = 'cleaned_completed_evidence'
    req['execution']['role'] = 'evidence'
    req['saved_e'] = '0/20'
    return req


def tree():
    return I.PrivateTree(table=True)


def prepare(t):
    t.req = request()
    t.value['follow_state']['cleanup'] = dict(slot_removed=True, publication_removed=True)
    t.write()
    _, _, control, row = I.FIXTURE.private_fixture(t.root.stat())
    control_dir = t.root / 'control'
    control_dir.mkdir(mode=0o700)
    for name, raw in [('endpos.json', json.dumps(control).encode()), ('lock', b'')]:
        path = control_dir / name
        path.write_bytes(raw)
        path.chmod(0o600)
    return control, row


def candidate(t):
    _, model, _, _ = I.FIXTURE.candidate_fixture(t.root.stat())
    t.value['format_version'] = 9
    t.value['target_provenance'] = model['target_provenance']
    plan = t.value['clone_state']['plan']
    plan['copy_policy'] = model['clone_state']['plan']['copy_policy']
    plan['copy_tables'] = [dict(source_oid=item['source_oid'], format='text', parts=[dict(number=1, count=1, start_block=0, end_block=None)]) for item in sorted(plan['tables'], key=lambda item:item['source_oid'])]
    t.write()


class Sessions:
    def __init__(self, state, fault=None):
        self.state, self.fault, self.events = state, fault, []
        fixture = I.FIXTURE.candidate_fixture if state.manifest['format_version'] == 9 else I.FIXTURE.private_fixture
        _, _, _, self.row = fixture(os.stat(HERE))
        self.row[24] = json.dumps(dict(binding=state.manifest['follow_state']['binding'], operator=state.manifest['operator'], origin_name=None))
        clone = json.loads(self.row[22]); clone['table_count'] = len(state.manifest['clone_state']['plan']['tables'])
        self.row[22] = json.dumps(clone)
        self.sources = 0
        self.next_backend = 100

    def session(self, library, connection, deadline, diag):
        owner = self
        role = connection
        owner.next_backend += 1; backend = owner.next_backend
        owner.events.append(('open', role))
        if role == b'source': owner.sources += 1
        source_number = owner.sources

        class Session:
            props = {'PQuser': b'fixture', 'PQdb': b'database', 'ssl': 0}

            def __init__(self): self.backend = backend

            def quote(self, value): return '"' + value + '"'

            def query(self, sql, params=(), tag='SELECT 1', state=0, rows=1, width=1, commit=False):
                owner.events.append(('query', role, sql, params))
                if commit and owner.fault == 'unknown_commit': raise S['Refusal']('unknown_commit')
                if not rows: return []
                if width == 9:
                    row = [b'123', b'11', b'1', b'160006', str(backend).encode(), b'33', b'33', b't', b'off']
                    if owner.fault == 'own_pid_changed': row[4] = str(backend+1).encode()
                    changes = {'source_system': (0,b'124'), 'source_database': (1,b'12'), 'source_timeline': (2,b'2'), 'source_version': (3,b'160007'), 'source_role': (5,b'34'), 'source_session_role': (6,b'34'), 'source_recovery': (7,b'f'), 'source_readonly': (8,b'on')}
                    if source_number>=3 and owner.fault in changes:
                        index,value=changes[owner.fault];row[index]=value
                    return [row]
                if width == 6: return [[b'123', b'11', b'1', b't', str(backend).encode(), b'160006']]
                if width == 7: return [[b'456', b'22', b'33', b'33', str(backend).encode(), b'180006', b't']]
                if width == 4: return [[b'fixture', b'fixture', b'database', b'f']]
                if width == 29:
                    row = list(owner.row)
                    if owner.fault == 'row_mutation' and sum(e[0] == 'query' and 'LIMIT 2' in e[2] for e in owner.events) > 1: row[18] = '8'
                    return [[None if cell is None else cell.encode() for cell in row]]
                if 'set_config' in sql: return [[b'on']]
                if role == b'source' and ('pg_replication_slots' in sql or 'pg_publication p WHERE' in sql):
                    if owner.fault in ('source_reappeared', 'source_present') and (source_number >= 3 or owner.fault == 'source_present'): return [[b'f']]
                    if len(params) >= 2 and isinstance(params[1], bool): assert params[1] is True
                if 'pg_advisory_unlock' in sql and owner.fault == 'unlock': return [[b'f']]
                if role == b'target' and 'pg_class' in sql and owner.fault == 'catalog': return [[b'f']]
                return [[b't']]

            def close(self, healthy=True):
                owner.events.append(('close', role, healthy))
                if owner.fault == 'close' and role == b'target': raise S['Refusal']('closure')

        return Session()


class CleanedTests(unittest.TestCase):
    def required(self, name):
        self.assertTrue(name in S, 'separate cleaned-completion implementation absent: ' + name)
        return S[name]

    def test_Request(self):
        validate = self.required('validate_cleaned_request')
        req = request(); raw = json.dumps(req).encode()
        self.assertEqual(validate(raw, req['operation_id'], req['program_sha256']), req)
        for path, value in [(('kind',), 'completed_evidence'), (('saved_e',), '0/0'), (('execution', 'role'), 'cleanup'), (('execution', 'job_uid'), req['execution']['worker_job_uid']), (('container', 'restart_count'), 1)]:
            bad = copy.deepcopy(req); node = bad
            for key in path[:-1]: node = node[key]
            node[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(S['Refusal']): validate(json.dumps(bad).encode(), req['operation_id'], req['program_sha256'])
        for key in req:
            bad = copy.deepcopy(req); del bad[key]
            with self.subTest(missing=key), self.assertRaises(S['Refusal']): validate(json.dumps(bad).encode(), req['operation_id'], req['program_sha256'])
        for bad in [raw.replace(b'"version": 1', b'"version": -0'), raw.replace(b'"version": 1', b'"version": 1,"version": 1')]:
            with self.assertRaises(S['Refusal']): validate(bad, req['operation_id'], req['program_sha256'])

    def test_StateAndActivePolicy(self):
        cls = self.required('CleanedState')
        with tree() as t:
            control, _ = prepare(t)
            before = {p: p.read_bytes() for p in t.root.rglob('*') if p.is_file()}
            state = cls(t.req); state.revalidate(); state.close()
            self.assertEqual(before, {p: p.read_bytes() for p in before})
            with self.assertRaises(S['Refusal']): S['validate_manifest'](t.value, t.req, t.root.stat())
            for path, value in [(('follow_state', 'cleanup'), None), (('follow_state', 'cleanup'), dict(slot_removed=True, publication_removed=False)), (('follow_state', 'cleanup'), dict(slot_removed=False, publication_removed=True)), (('clone_state', 'phase'), 'copying'), (('clone_state', 'completion'), dict(tables=1, rows=0, bytes=0)), (('clone_state', 'owned_tables'), []), (('follow_state', 'restart'), {}), (('operator', 'workdir_id'), 'foreign')]:
                bad = copy.deepcopy(t.value); node = bad
                for key in path[:-1]: node = node[key]
                node[path[-1]] = value
                with self.subTest(path=path), self.assertRaises(S['Refusal']): S['validate_cleaned_manifest'](bad, t.req, t.root.stat())
            for key, value in [('sealed', False), ('end_lsn', 33), ('slot_nonce', [9]*16)]:
                path = t.root / 'control' / 'endpos.json'
                path.write_bytes(json.dumps(dict(control, **{key:value})).encode())
                with self.subTest(control=key), self.assertRaises(S['Refusal']): cls(t.req)

    def test_RowWitness(self):
        cls = self.required('CleanedState')
        with tree() as t:
            prepare(t); state = cls(t.req)
            try:
                sessions = Sessions(state)
                later = S['validate_control_row'](sessions.row, state)
                self.assertEqual(later['durable_replay_lsn'], '0/10')
                self.assertEqual(later['drain_end_lsn'], '0/20')
                row = list(sessions.row); row[23] = '0/30'; row[27] = 'keepalive'
                self.assertEqual(S['validate_control_row'](row, state)['drain_kind'], 'keepalive')
                for index, value in [(18, '18446744073709551616'), (19, 'foreign'), (25, '0/21'), (26, '0/20'), (27, 'keepalive'), (28, 'f')]:
                    row = list(sessions.row); row[index] = value
                    with self.subTest(index=index), self.assertRaises(S['Refusal']): S['validate_control_row'](row, state)
            finally: state.close()

    def test_Observe(self):
        observe = self.required('observe_cleaned')
        for candidate_format in (False, True):
            with tree() as t:
                prepare(t)
                if candidate_format: candidate(t)
                state = S['CleanedState'](t.req)
                material = types.SimpleNamespace(connection=lambda role: role.encode(), check=lambda: None)
                try:
                    sessions = Sessions(state)
                    with mock.patch.dict(S, Session=sessions.session): checkpoint, guards = observe(state, None, material, time.monotonic()+25, None)
                    self.assertEqual(checkpoint['checkpoint_id'], '1:0:7'); self.assertEqual(guards, S['GUARDS'])
                    self.assertEqual(sessions.events[-1], ('close', b'source', True))
                    self.assertEqual(sessions.sources, 3)
                    target = next(i for i,e in enumerate(sessions.events) if e == ('open', b'target'))
                    self.assertIn(('close', b'source', True), sessions.events[:target])
                    queries = [e for e in sessions.events if e[0] == 'query']
                    self.assertFalse(any(word in e[2].lower() for e in queries for word in ['pg_current_wal', 'pg_create', 'pg_drop', 'drop publication', 'create publication']))
                    updates = [e for e in queries if e[2].startswith('UPDATE ONLY ')]
                    self.assertEqual(len(updates), 1); self.assertEqual(len(updates[0][3]), 29)
                    self.assertEqual(updates[0][3], sessions.row)
                    self.assertEqual(sessions.next_backend, 104)
                    for mode in ['source_present', 'source_reappeared', 'unknown_commit', 'unlock', 'catalog', 'close', 'row_mutation', 'own_pid_changed', 'source_system', 'source_database', 'source_timeline', 'source_version', 'source_role', 'source_session_role', 'source_recovery', 'source_readonly']:
                        sessions = Sessions(state, mode)
                        with self.subTest(mode=mode), mock.patch.dict(S, Session=sessions.session), self.assertRaises(S['Refusal']): observe(state, None, material, time.monotonic()+25, None)
                finally: state.close()

    def test_StateReplacement(self):
        cls = self.required('CleanedState')
        with tree() as t:
            prepare(t); state = cls(t.req)
            path = t.root / 'manifest.json'; raw = path.read_bytes()
            changed = copy.deepcopy(t.value); changed['follow_state']['cleanup']['slot_removed'] = False
            path.write_bytes(json.dumps(changed).encode())
            with self.assertRaises(S['Refusal']): state.revalidate()
            state.close(False)
            self.assertNotEqual(path.read_bytes(), raw)

    def test_MainRefusesBeforeSQL(self):
        main = self.required('cleaned_completion_main')
        req = request(); req['kind'] = 'owned_inventory'
        with mock.patch.dict(S, InputChannel=mock.Mock(), Session=mock.Mock()), mock.patch.dict(os.environ, LANG='C', LC_ALL='C', PGCONNECT_TIMEOUT='10'), mock.patch.object(S['resource'], 'setrlimit'):
            S['InputChannel'].return_value.consume.return_value = json.dumps(req).encode()
            self.assertEqual(main(['-', 'cleaned_completed_evidence', req['operation_id'], req['program_sha256']]), 1)
            S['Session'].assert_not_called()

    def test_WriterPolicy(self):
        writer = {}
        exec(compile((HERE / 'evidence_request_writer.py').read_bytes(), '<writer>', 'exec'), writer)
        adapter = HERE / 'cleaned_completion_request_writer.py'
        self.assertTrue(adapter.is_file(), 'cleaned relay policy absent')
        exec(compile(adapter.read_bytes(), '<cleaned-writer>', 'exec'), writer)
        req = request(); raw = json.dumps(req).encode()
        result = I.FIXTURE.response(req, raw)
        result['kind'] = req['kind']
        result.update(materialization_closed=True, cleanup_complete=True, source_resources_absent=True)
        result['resources'] = [dict(kind='replication_slot', name='native_slot', owned=True, retain=False), dict(kind='publication', name='native_publication', owned=True, retain=False), dict(kind='workdir', name='/work/pgcopydb', owned=True, retain=True)]
        def check(value, body=raw): return writer['result_envelope'](json.dumps(value).encode()+b'\n', body, req['operation_id'])
        self.assertTrue(check(result))
        for key in result:
            bad = copy.deepcopy(result); del bad[key]
            with self.subTest(missing=key), self.assertRaises((ValueError, KeyError)): check(bad)
        for key in ['source_closed', 'target_closed', 'materialization_closed', 'private_closed', 'diagnostics_clean', 'cleanup_complete', 'source_resources_absent']:
            bad = copy.deepcopy(result); bad[key] = False
            with self.subTest(flag=key), self.assertRaises(ValueError): check(bad)
        for key, value in [('barrier_confirmed', False), ('drain_kind', 'keepalive'), ('drain_end_lsn', '0/21'), ('checkpoint_id', '2:0:7')]:
            bad = copy.deepcopy(result); bad['checkpoint'][key] = value
            with self.subTest(checkpoint=key), self.assertRaises(ValueError): check(bad)
        bad = copy.deepcopy(result); bad['checkpoint']['certified_lsn'] = '0/30'; bad['checkpoint']['durable_replay_lsn'] = '0/30'; bad['checkpoint']['drain_kind'] = 'keepalive'
        self.assertTrue(check(bad))
        for key in req:
            bad = copy.deepcopy(req); del bad[key]
            with self.subTest(request_missing=key), self.assertRaises((ValueError, KeyError)): writer['strict_body'](json.dumps(bad).encode(), req['operation_id'])

    def test_MainClosure(self):
        main = self.required('cleaned_completion_main')
        req = request(); raw = json.dumps(req).encode()
        work_env = {key: str(req['execution']['work'][field]) for key, field in [('RPGMIGRATE_MIGRATION_UID', 'migration_uid'), ('RPGMIGRATE_RUN_ID', 'run_id'), ('RPGMIGRATE_WORKDIR_ID', 'workdir_id'), ('RPGMIGRATE_CHECKPOINT_GENERATION', 'checkpoint_generation')]}
        checkpoint = I.FIXTURE.response(req, raw)['checkpoint']
        original_policy = S['qualified_resolver_policy']
        for fault in [None, 'source', 'barrier', 'materials', 'state', 'diagnostics', 'admission', 'closure', 'closed_policy']:
            events = []
            def closed(name):
                def close(*args):
                    events.append(name)
                    if fault == name: raise S['Refusal']('closure')
                return close
            channel = mock.Mock(); channel.consume.return_value = raw
            state = types.SimpleNamespace(revalidate=lambda: None, close=closed('state'), resources=lambda: [])
            materials = types.SimpleNamespace(files=[], prepare=lambda *a:None, check=lambda:None, close=closed('materials'))
            resolver = types.SimpleNamespace(raw=b'fixture', check=lambda:None)
            diag = types.SimpleNamespace(close=closed('diagnostics'))
            def policy(retained):
                self.assertIs(retained, resolver); self.assertEqual(materials.files, [resolver])
                phase = 'admission' if not events else 'closure'; events.append(phase)
                if fault == 'closed_policy': original_policy(retained)
                if fault == phase: raise S['Refusal']('capability')
            def observe(*args):
                self.assertEqual(events, ['admission'])
                if fault in ('source', 'barrier'): raise S['Refusal']('unknown_commit' if fault=='barrier' else 'connection')
                events.append('barrier'); return checkpoint, S['GUARDS']
            with self.subTest(fault=fault), mock.patch.dict(S, InputChannel=lambda *a:channel, CleanedState=lambda *a:state, Materialization=lambda *a:materials, StableFile=lambda *a:resolver, Diagnostics=lambda:diag, load_runtime=lambda:None, qualified_resolver_policy=policy, observe_cleaned=observe), mock.patch.dict(os.environ, dict(work_env, LANG='C', LC_ALL='C', PGCONNECT_TIMEOUT='10'), clear=True), mock.patch.object(S['resource'], 'setrlimit'), mock.patch.object(S['os'], 'geteuid', return_value=65532):
                code = main(['-', req['kind'], req['operation_id'], req['program_sha256']])
            if fault is None:
                self.assertEqual(code, 0); self.assertEqual(events, ['admission', 'barrier', 'closure', 'materials', 'state', 'diagnostics'])
                result = json.loads(channel.finish.call_args.args[0]); self.assertTrue(result['cleanup_complete'] and result['source_resources_absent'])
            else:
                self.assertEqual(code, 1); self.assertEqual(channel.finish.call_args.args, ())
                if fault in ('admission', 'closed_policy'): self.assertNotIn('barrier', events)
                if fault == 'closure': self.assertIn('barrier', events)


if __name__ == '__main__': unittest.main()

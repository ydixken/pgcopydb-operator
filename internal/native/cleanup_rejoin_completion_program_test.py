import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import types
import unittest
from unittest import mock

if sys.flags.optimize: raise SystemExit('optimized tests refused')

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('_rejoin_completed_fixtures', HERE / 'cleaned_completion_program_test.py')
C = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(C)
S = C.S
HELPER = HERE / 'cleanup_rejoin_completion_program.py'
if HELPER.is_file(): exec(compile(HELPER.read_bytes(), '<cleanup-rejoin>', 'exec'), S)
INTENTS = [(False, False), (True, False), (False, True)]


def request():
    req = C.request(); req['kind'] = 'cleanup_rejoin_completed_evidence'
    return req


def prepare(t, flags=(False, False)):
    control, row = C.prepare(t)
    t.req = request()
    t.value['follow_state']['cleanup'] = dict(zip(('slot_removed', 'publication_removed'), flags))
    t.write()
    return control, row


class Sessions:
    def __init__(self, state, fault=None, absent=False):
        self.state, self.fault, self.events = state, fault, []
        self.row = C.Sessions(state).row
        self.next = self.sources = 0
        flags = state.manifest['follow_state']['cleanup']
        self.present = [not absent and not flags[key] for key in ('slot_removed', 'publication_removed')]

    def session(self, library, connection, deadline, diag):
        owner = self; role = connection
        owner.next += 1; backend = 100 + owner.next
        if role == b'source': owner.sources += 1
        source_number = owner.sources
        present = list(owner.present)
        if owner.fault == 'reappearance' and source_number >= 3: present = [True, True]
        if owner.fault == 'disappearance' and source_number >= 3: present = [False, False]
        owner.events.append(('open', role, backend))

        class Session:
            props = {'PQuser': b'fixture', 'PQdb': b'database', 'ssl': 0}

            def __init__(self): self.backend = backend; self.identity_reads = 0
            def quote(self, value): return '"' + value + '"'

            def query(self, sql, params=(), tag='SELECT 1', state=0, rows=1, width=1, commit=False):
                owner.events.append(('query', role, sql, params))
                if 'pg_try_advisory_lock' in sql: assert not owner.state.locked
                if commit and owner.fault == 'unknown_commit': raise S['Refusal']('unknown_commit')
                if not rows: return []
                if width == 9:
                    self.identity_reads += 1
                    writer = b'34' if owner.fault == 'source_identity' and source_number >= 3 else b'33'
                    pid = backend+1 if owner.fault=='own_pid_changed' and self.identity_reads>1 else backend
                    return [[b'123', b'11', b'1', b'160006', str(pid).encode(), writer, b'33', b't', b'off']]
                if width == 6: return [[b'123', b'11', b'1', b't', str(backend).encode(), b'160006']]
                if width == 7: return [[b'456', b'22', b'33', b'33', str(backend).encode(), b'180006', b't']]
                if width == 4: return [[b'fixture', b'fixture', b'database', b'f']]
                if width == 29:
                    row = list(owner.row)
                    if owner.fault == 'row_mutation' and sum(e[0] == 'query' and 'LIMIT 2' in e[2] for e in owner.events) > 1: row[18] = '8'
                    return [[None if cell is None else cell.encode() for cell in row]]
                if width == 2 and 'EXISTS' in sql:
                    return [[b't' if value else b'f' for value in present]]
                if 'set_config' in sql: return [[b'on']]
                if role == b'source' and 'pg_replication_slots s WHERE' in sql:
                    if params[1] and present[0] or owner.fault in ('foreign_slot', 'active_slot'): return [[b'f']]
                if role == b'source' and 'pg_publication p WHERE' in sql:
                    if params[1] and present[1] or owner.fault == 'foreign_publication': return [[b'f']]
                if 'pg_advisory_unlock' in sql and owner.fault == 'unlock': return [[b'f']]
                if role == b'target' and 'pg_class' in sql and owner.fault == 'catalog': return [[b'f']]
                return [[b't']]

            def close(self, healthy=True):
                owner.events.append(('close', role, backend, healthy))
                if owner.fault == 'source_close' and role == b'source' or owner.fault == 'source_close_after_barrier' and role == b'source' and source_number>=3 or owner.fault == 'target_close' and role == b'target': raise S['Refusal']('closure')

        return Session()


class CleanupRejoinTests(unittest.TestCase):
    def required(self, name):
        self.assertTrue(name in S, 'partial-only completion implementation absent: ' + name)
        return S[name]

    def test_Request(self):
        validate = self.required('validate_cleanup_rejoin_request')
        req = request(); raw = json.dumps(req).encode()
        self.assertEqual(validate(raw, req['operation_id'], req['program_sha256']), req)
        for path, value in [(('kind',), 'cleaned_completed_evidence'), (('saved_e',), '0/0'), (('execution', 'role'), 'cleanup'), (('execution', 'job_uid'), req['execution']['worker_job_uid']), (('container', 'restart_count'), 1)]:
            bad = copy.deepcopy(req); node = bad
            for key in path[:-1]: node = node[key]
            node[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(S['Refusal']): validate(json.dumps(bad).encode(), req['operation_id'], req['program_sha256'])
        for key in req:
            bad = copy.deepcopy(req); del bad[key]
            with self.subTest(missing=key), self.assertRaises(S['Refusal']): validate(json.dumps(bad).encode(), req['operation_id'], req['program_sha256'])
        for bad in [raw.replace(b'"version": 1', b'"version": -0'), raw.replace(b'"version": 1', b'"version": 1,"version": 1')]:
            with self.assertRaises(S['Refusal']): validate(bad, req['operation_id'], req['program_sha256'])

    def test_PartialState(self):
        cls = self.required('CleanupRejoinState')
        for flags in INTENTS:
            with self.subTest(flags=flags), C.tree() as t:
                prepare(t, flags)
                before = {p: p.read_bytes() for p in t.root.rglob('*') if p.is_file()}
                state = cls(t.req); state.revalidate(); state.close()
                self.assertEqual(before, {p: p.read_bytes() for p in before})
        with C.tree() as t:
            control, _ = prepare(t)
            for path, value in [(('follow_state', 'cleanup'), None), (('follow_state', 'cleanup'), dict(slot_removed=True, publication_removed=True)), (('clone_state', 'phase'), 'copying'), (('clone_state', 'completion'), dict(tables=1, rows=0, bytes=0)), (('clone_state', 'owned_tables'), []), (('follow_state', 'restart'), {}), (('follow_state', 'preliminary'), {}), (('operator', 'workdir_id'), 'foreign')]:
                bad = copy.deepcopy(t.value); node = bad
                for key in path[:-1]: node = node[key]
                node[path[-1]] = value
                with self.subTest(path=path), self.assertRaises(S['Refusal']): S['validate_cleanup_rejoin_manifest'](bad, t.req, t.root.stat())
            for key, value in [('sealed', False), ('end_lsn', 33), ('slot_nonce', [9]*16)]:
                path = t.root / 'control' / 'endpos.json'; path.write_bytes(json.dumps(dict(control, **{key:value})).encode())
                with self.subTest(control=key), self.assertRaises(S['Refusal']): cls(t.req)
        with C.tree() as t:
            prepare(t)
            for path in (t.root/'control').iterdir(): path.unlink()
            (t.root/'control').rmdir()
            with self.assertRaises(S['Refusal']): cls(t.req)
        for missing in ('lock', 'endpos.json'):
            with C.tree() as t:
                prepare(t); (t.root / 'control' / missing).unlink()
                with self.subTest(missing=missing), self.assertRaises(S['Refusal']): cls(t.req)

    def test_ControlLock(self):
        cls = self.required('CleanupRejoinState'); flock = S['fcntl'].flock
        with C.tree() as t:
            prepare(t); events = []
            def checked(fd, op): events.append((fd, op)); return flock(fd, op)
            with mock.patch.object(S['fcntl'], 'flock', checked):
                state = cls(t.req)
                self.assertIn((state.lock, S['fcntl'].LOCK_EX | S['fcntl'].LOCK_NB), events)
                self.assertIn((state.lock, S['fcntl'].LOCK_UN), events)
                self.assertFalse(state.locked)
                events.clear(); state.revalidate()
                self.assertEqual(events, [(state.lock, S['fcntl'].LOCK_EX | S['fcntl'].LOCK_NB), (state.lock, S['fcntl'].LOCK_UN)])
                other = os.open(t.root / 'control' / 'lock', os.O_RDONLY)
                try:
                    flock(other, S['fcntl'].LOCK_EX | S['fcntl'].LOCK_NB)
                    with self.assertRaises(BlockingIOError): state.revalidate()
                finally: os.close(other)
                changed = copy.deepcopy(t.value); changed['follow_state']['cleanup']['slot_removed'] = True
                (t.root / 'manifest.json').write_bytes(json.dumps(changed).encode())
                with self.assertRaises(S['Refusal']): state.revalidate()
                self.assertFalse(state.locked); state.close(False)
        with C.tree() as t:
            control,_=prepare(t);state=cls(t.req)
            (t.root/'control'/'endpos.json').write_bytes(json.dumps(dict(control,end_lsn=33)).encode())
            with self.assertRaises(S['Refusal']):state.revalidate()
            self.assertFalse(state.locked);state.close(False)
        with C.tree() as t:
            prepare(t); other = os.open(t.root / 'control' / 'lock', os.O_RDONLY)
            try:
                flock(other, S['fcntl'].LOCK_EX | S['fcntl'].LOCK_NB)
                with self.assertRaises(BlockingIOError): cls(t.req)
            finally: os.close(other)

    def test_SourceObservations(self):
        observe = self.required('observe_cleanup_rejoin')
        for flags in INTENTS:
            for absent in (False, True):
                with self.subTest(flags=flags, absent=absent), C.tree() as t:
                    prepare(t, flags); state = S['CleanupRejoinState'](t.req)
                    material = types.SimpleNamespace(connection=lambda role:role.encode(), check=lambda:None)
                    try:
                        sessions = Sessions(state, absent=absent)
                        with mock.patch.dict(S, Session=sessions.session): checkpoint, guards = observe(state, None, material, time.monotonic()+25, None)
                        self.assertEqual(checkpoint['checkpoint_id'], '1:0:7'); self.assertEqual(guards, S['GUARDS'])
                        self.assertEqual(sessions.sources, 3)
                        backends = [e[2] for e in sessions.events if e[0]=='open' and e[1]==b'source']
                        self.assertEqual(len(set(backends)), 3)
                        self.assertEqual(sessions.events[-1], ('close', b'source', backends[-1], True))
                        queries = [e for e in sessions.events if e[0]=='query']
                        self.assertFalse(any(word in e[2].lower() for e in queries for word in ['pg_current_wal', 'pg_create', 'pg_drop', 'drop publication', 'create publication']))
                        updates = [e for e in queries if e[2].startswith('UPDATE ONLY ')]
                        self.assertEqual(len(updates), 1); self.assertEqual(updates[0][3], sessions.row)
                        for fault in ('foreign_slot', 'foreign_publication', 'active_slot', 'source_identity', 'own_pid_changed', 'source_close', 'source_close_after_barrier'):
                            bad = Sessions(state, fault, absent)
                            with self.subTest(fault=fault), mock.patch.dict(S, Session=bad.session), self.assertRaises(S['Refusal']): observe(state, None, material, time.monotonic()+25, None)
                        for fault, absent_before in [('reappearance', True), ('disappearance', False)]:
                            if fault=='disappearance' and all(flags): continue
                            bad = Sessions(state, fault, absent_before)
                            with self.subTest(fault=fault), mock.patch.dict(S, Session=bad.session), self.assertRaises(S['Refusal']): observe(state, None, material, time.monotonic()+25, None)
                    finally: state.close()

    def test_TargetAuthority(self):
        observe = self.required('observe_cleanup_rejoin')
        for replay_safe_mode in (False, True):
            with C.tree() as t:
                prepare(t)
                if replay_safe_mode: C.replay_safe(t)
                state = S['CleanupRejoinState'](t.req)
                material = types.SimpleNamespace(connection=lambda role:role.encode(), check=lambda:None)
                try:
                    sessions = Sessions(state)
                    proof = S['validate_control_row'](sessions.row, state)
                    self.assertEqual(proof['certified_lsn'], '0/10'); self.assertEqual(proof['durable_replay_lsn'], '0/10')
                    self.assertEqual(proof['drain_end_lsn'], '0/20'); self.assertEqual(proof['drain_replay_lsn'], '0/30')
                    row = list(sessions.row); row[23] = '0/30'; row[27] = 'keepalive'
                    self.assertEqual(S['validate_control_row'](row, state)['drain_kind'], 'keepalive')
                    for index, value in [(18, '18446744073709551616'), (19, 'foreign'), (25, '0/21'), (26, '0/20'), (27, 'keepalive'), (28, 'f')]:
                        row = list(sessions.row); row[index] = value
                        with self.subTest(index=index), self.assertRaises(S['Refusal']): S['validate_control_row'](row, state)
                    for fault in ('unknown_commit', 'unlock', 'catalog', 'target_close', 'row_mutation'):
                        bad = Sessions(state, fault)
                        with self.subTest(fault=fault), mock.patch.dict(S, Session=bad.session), self.assertRaises(S['Refusal']): observe(state, None, material, time.monotonic()+25, None)
                finally: state.close()

    def test_RoutesStayNarrow(self):
        self.required('validate_cleanup_rejoin_manifest')
        with C.tree() as t:
            prepare(t)
            for flags, expected in [(None, 'active'), ((False,False), 'partial'), ((True,False), 'partial'), ((False,True), 'partial'), ((True,True), 'cleaned')]:
                value = copy.deepcopy(t.value)
                value['follow_state']['cleanup'] = None if flags is None else dict(zip(('slot_removed','publication_removed'),flags))
                for label, validator in [('active', 'validate_manifest'), ('partial', 'validate_cleanup_rejoin_manifest'), ('cleaned', 'validate_cleaned_manifest')]:
                    with self.subTest(flags=flags, route=label):
                        if label==expected: S[validator](value,t.req,t.root.stat())
                        else:
                            with self.assertRaises(S['Refusal']): S[validator](value,t.req,t.root.stat())

    def test_MainClosure(self):
        main = self.required('cleanup_rejoin_completion_main')
        with C.tree() as t:
            prepare(t)
            C.I.FIXTURE.check_preconnection_admission(self,S,t.req,t.root/'manifest.json',t.value,'CleanupRejoinState',lambda:main(['-',t.req['kind'],t.req['operation_id'],t.req['program_sha256']]))
        req = request(); raw = json.dumps(req).encode(); checkpoint = C.I.FIXTURE.response(req,raw)['checkpoint']
        env = {key:str(req['execution']['work'][field]) for key,field in [('RPGMIGRATE_MIGRATION_UID','migration_uid'),('RPGMIGRATE_RUN_ID','run_id'),('RPGMIGRATE_WORKDIR_ID','workdir_id'),('RPGMIGRATE_CHECKPOINT_GENERATION','checkpoint_generation')]}
        original_policy = S['qualified_resolver_policy']
        for fault in (None, 'source', 'barrier', 'materials', 'state', 'diagnostics', 'resolver_check', 'material_check', 'private_check', 'admission', 'closure', 'closed_policy'):
            events=[]
            def closed(name):
                def close(*args):
                    events.append(name)
                    if fault==name: raise S['Refusal']('closure')
                return close
            channel=mock.Mock();channel.consume.return_value=raw
            def checked(name):
                def check():
                    if fault==name:raise S['Refusal']('closure')
                return check
            resources=[dict(kind='replication_slot',name='native_slot',owned=True,retain=False),dict(kind='publication',name='native_publication',owned=True,retain=False),dict(kind='workdir',name='/work/pgcopydb',owned=True,retain=True)]
            state=types.SimpleNamespace(revalidate=checked('private_check'),close=closed('state'),resources=lambda:resources)
            materials=types.SimpleNamespace(files=[],prepare=lambda *a:None,check=checked('material_check'),close=closed('materials'))
            resolver=types.SimpleNamespace(raw=b'fixture',check=checked('resolver_check'));diag=types.SimpleNamespace(close=closed('diagnostics'))
            def policy(retained):
                self.assertIs(retained, resolver); self.assertEqual(materials.files, [resolver])
                phase = 'admission' if not events else 'closure'; events.append(phase)
                if fault == 'closed_policy': original_policy(retained)
                if fault == phase: raise S['Refusal']('capability')
            def observe(*args):
                self.assertEqual(events, ['admission'])
                if fault in ('source','barrier'):raise S['Refusal']('unknown_commit' if fault=='barrier' else 'connection')
                events.append('barrier');return checkpoint,S['GUARDS']
            with self.subTest(fault=fault),mock.patch.dict(S,InputChannel=lambda *a:channel,CleanupRejoinState=lambda *a:state,Materialization=lambda *a:materials,StableFile=lambda *a:resolver,Diagnostics=lambda:diag,load_runtime=lambda:None,qualified_resolver_policy=policy,observe_cleanup_rejoin=observe),mock.patch.dict(os.environ,dict(env,LANG='C',LC_ALL='C',PGCONNECT_TIMEOUT='10'),clear=True),mock.patch.object(S['resource'],'setrlimit'),mock.patch.object(S['os'],'geteuid',return_value=65532):
                code=main(['-',req['kind'],req['operation_id'],req['program_sha256']])
            if fault is None:
                self.assertEqual(code,0);self.assertEqual(events,['admission','barrier','closure','materials','state','diagnostics'])
                result=json.loads(channel.finish.call_args.args[0]);self.assertTrue(result['cleanup_pending'] and result['source_resources_checked'])
                self.assertNotIn('cleanup_complete',result);self.assertNotIn('source_resources_absent',result)
            else:
                self.assertEqual(code,1);self.assertEqual(channel.finish.call_args.args,())
                if fault in ('admission', 'closed_policy'): self.assertNotIn('barrier', events)
                if fault == 'closure': self.assertIn('barrier', events)
        bad=dict(req,kind='completed_evidence');channel=mock.Mock();channel.consume.return_value=json.dumps(bad).encode()
        sql=mock.Mock()
        with mock.patch.dict(S,InputChannel=lambda *a:channel,Session=sql),mock.patch.dict(os.environ,dict(env,LANG='C',LC_ALL='C',PGCONNECT_TIMEOUT='10'),clear=True),mock.patch.object(S['resource'],'setrlimit'):
            self.assertEqual(main(['-',req['kind'],req['operation_id'],req['program_sha256']]),1)
            sql.assert_not_called()

    def test_WriterPolicy(self):
        writer={};exec(compile((HERE/'evidence_request_writer.py').read_bytes(),'<writer>','exec'),writer)
        adapter=HERE/'cleanup_rejoin_completion_request_writer.py';self.assertTrue(adapter.is_file(),'partial relay policy absent')
        exec(compile(adapter.read_bytes(),'<rejoin-writer>','exec'),writer)
        req=request();raw=json.dumps(req).encode();result=C.I.FIXTURE.response(req,raw)
        result['kind']=req['kind'];result.update(materialization_closed=True,cleanup_pending=True,source_resources_checked=True)
        result['resources']=[dict(kind='replication_slot',name='native_slot',owned=True,retain=False),dict(kind='publication',name='native_publication',owned=True,retain=False),dict(kind='workdir',name='/work/pgcopydb',owned=True,retain=True)]
        def check(value):return writer['result_envelope'](json.dumps(value).encode()+b'\n',raw,req['operation_id'])
        self.assertTrue(check(result))
        for key in result:
            bad=copy.deepcopy(result);del bad[key]
            with self.subTest(missing=key),self.assertRaises((ValueError,KeyError)):check(bad)
        for key in ['source_closed','target_closed','materialization_closed','private_closed','diagnostics_clean','cleanup_pending','source_resources_checked']:
            bad=copy.deepcopy(result);bad[key]=False
            with self.subTest(flag=key),self.assertRaises(ValueError):check(bad)
        for key,value in [('barrier_confirmed',False),('drain_kind','keepalive'),('drain_end_lsn','0/21'),('checkpoint_id','2:0:7')]:
            bad=copy.deepcopy(result);bad['checkpoint'][key]=value
            with self.subTest(checkpoint=key),self.assertRaises(ValueError):check(bad)


if __name__=='__main__':unittest.main()

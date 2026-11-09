import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import struct
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

if sys.flags.optimize:
    raise SystemExit('optimized tests refused')

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('native_evidence_program', HERE / 'evidence_program.py')
P = None
if (HERE / 'evidence_program.py').is_file():
    P = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(P)


def request():
    work = dict(migration_uid='00112233-4455-6677-8899-aabbccddeeff', run_id='00112233445566778899aabbccddeeff', workdir_id='pvc-fixture', checkpoint_generation=1)
    execution = dict(namespace='fixture', migration_name='migration', pvc_name='work', job_name='evidence', migration_uid=work['migration_uid'], pvc_uid='pvc-fixture', job_uid='job-fixture', worker_job_uid='worker-fixture', migration_generation=1, attempt=0, role='evidence', work=work, image_ref='example.invalid/native@sha256:' + '1'*64, platform_image_id='containerd://sha256:' + '2'*64, template_sha256='3'*64, connection_reference_sha256='4'*64)
    container = dict(pod_uid='pod-fixture', container_name='pgcopydb', container_id='containerd://fixture', restart_count=0, image_id=execution['platform_image_id'], template_sha256=execution['template_sha256'])
    return dict(version=1, kind='completed_evidence', operation_id='5'*32, program_sha256='6'*64, execution=execution, pod_name='evidence-pod', container=container, saved_e='0/20')


def private_fixture(directory_info):
    req=request(); work=req['execution']['work']; run=list(bytes.fromhex(work['run_id']))
    stream=dict(run=run,source=dict(system_identifier=123,database_oid=11),generation=1)
    source=dict(identity=stream['source'],timeline=1,major=16)
    slot=dict(intent=dict(binding=dict(stream=stream,slot_name='native_slot',plugin='pgoutput',catalog_epoch=1),nonce=[1]*16),consistent_point=8,snapshot_token='snapshot')
    pub=dict(name='native_publication',oid=88,owner_oid=33)
    binding=dict(stream=stream,target_system_identifier=456,target_database_oid=22,schema_name='_rpgmigrate_'+work['run_id'],table_name='checkpoint',owner_oid=33,metadata_version=6)
    target=dict(intent=dict(binding=binding,nonce=[2]*16),schema_oid=44,table_oid=55,marker_table_oid=66,marker_index_oid=77)
    operator=dict(migration_uid=work['migration_uid'],run=run,workdir_id=work['workdir_id'],checkpoint_generation=1)
    plan=dict(source=source,target=dict(identity=dict(system_identifier=456,database_oid=22),major=18),generation=1,tables=[],sequences=[],policy=dict(no_owner=False,no_acl=False,no_comments=False,no_tablespaces=False))
    follow_binding=dict(slot=slot,publication=pub,source_timeline=1,copy_generation=1,source_types=[dict(oid=25,schema='pg_catalog',name='text')])
    clone=dict(plan=plan,ticket=dict(token='snapshot',source=source,generation=1),phase='analyzing',owned_objects=[],owned_tables=[],completion=None)
    follow=dict(cleanup=None,origin_name=None,publication=dict(phase='confirmed',intent=dict(run=run,name=pub['name'],nonce=[3]*16),receipt=pub),binding=follow_binding,restart=None,preliminary=None)
    manifest=dict(magic='RPGMIGRATE',format_version=8,run=run,canonical_path='/work/pgcopydb',directory_device=directory_info.st_dev,directory_inode=directory_info.st_ino,copy_complete=None,target_provenance=[dict(phase='confirmed',record=target)],source_slot=dict(phase='confirmed',record=slot),clone_state=clone,follow_state=follow,operator=operator)
    endpos=dict(version=1,stream=stream,copy_generation=1,slot_nonce=[1]*16,target_nonce=[2]*16,end_lsn=32,sealed=True)
    row=['rpgmigrate','6',bytes(run).hex(),bytes([2]*16).hex(),'123','11','1','456','22','33','44','55','1','7','9','0/9','0/10','0','7','snapshot','0/8',bytes([1]*16).hex(),json.dumps(dict(generation=1,phase='analyzing',snapshot_token='snapshot',source_timeline=1,table_count=0)),'0/10',json.dumps(dict(binding=follow_binding,operator=operator,origin_name=None)),'0/20','0/30','later_begin','t']
    return req,manifest,endpos,row


def response(value,raw):
    digest,execution=P.request_digests(raw)
    return dict(version=1,kind='completed_evidence',operation_id=value['operation_id'],request_sha256=digest,program_sha256=value['program_sha256'],execution_sha256=execution,work=value['execution']['work'],checkpoint=dict(checkpoint_id='1:0:7',certified_lsn='0/10',durable_replay_lsn='0/10',drain_end_lsn='0/20',drain_replay_lsn='0/30',drain_kind='later_begin',barrier_confirmed=True),resources=[dict(kind='workdir',name='/work/pgcopydb',owned=True,retain=True)],guards=P.GUARDS,source_closed=True,target_closed=True,private_closed=True,diagnostics_clean=True)


class FileOptionsLibrary:
    defaults={b'service':(None,b'PGSERVICE',None),b'password':(None,b'PGPASSWORD',None),b'passfile':(None,b'PGPASSFILE',None),b'sslmode':(None,b'PGSSLMODE',b'prefer'),b'sslrootcert':(None,b'PGSSLROOTCERT',None),b'sslcert':(None,b'PGSSLCERT',None),b'sslkey':(None,b'PGSSLKEY',None),b'sslcrl':(None,b'PGSSLCRL',None),b'sslcrldir':(None,b'PGSSLCRLDIR',None),b'dbname':(None,b'PGDATABASE',None),b'sslpassword':(None,None,None),b'host':(None,b'PGHOST',None)}
    def __init__(self,cases):self.cases=cases;self.parsed=[];self.options_freed=0;self.errors_freed=0;self.keep=[]
    def PQconninfoParse(self,raw,error):
        self.parsed.append(raw);values=self.cases[raw]
        if values is None:
            message=P.C.create_string_buffer(b'diagnostic-canary');self.keep.append(message)
            P.C.cast(error,P.C.POINTER(P.PTR))[0]=P.C.addressof(message)
            return P.C.POINTER(P.ConnOption)()
        if isinstance(values,dict):
            rows=[(key,values.get(key,value),env,compiled) for key,(value,env,compiled) in self.defaults.items()]
            rows.extend((key,value,None,None) for key,value in values.items() if key not in self.defaults)
        else:rows=[(key,value,None,None) for key,value in values]
        table=(P.ConnOption*(len(rows)+1))()
        for index,(key,value,env,compiled) in enumerate(rows):table[index].keyword=key;table[index].val=value;table[index].envvar=env;table[index].compiled=compiled
        self.keep.append(table);return table
    def PQconninfoFree(self,table):
        self.options_freed+=1
        for i in range(256):
            if table[i].keyword is None:break
            table[i].val=b'freed'
    def PQfreemem(self,error):self.errors_freed+=1


class FakeMemfd:
    def __init__(self,failure=None):self.failure=failure;self.handles={};self.next=50000;self.stack=None
    def live(self):return list(self.handles)
    def __enter__(self):
        from contextlib import ExitStack
        from types import SimpleNamespace
        self.stack=ExitStack();self.uid=os.geteuid();self.ns=SimpleNamespace
        self.original={name:getattr(os,name,None) for name in ('write','read','lseek','fstat','stat','fchmod','close','dup','ftruncate','get_inheritable')}
        for name,value in [('MFD_CLOEXEC',1),('MFD_ALLOW_SEALING',2),('MFD_NOEXEC_SEAL',8)]:self.stack.enter_context(mock.patch.object(P.os,name,value,create=True))
        import fcntl
        for name,value in [('F_ADD_SEALS',1033),('F_GET_SEALS',1034),('F_SEAL_SEAL',1),('F_SEAL_SHRINK',2),('F_SEAL_GROW',4),('F_SEAL_WRITE',8)]:self.stack.enter_context(mock.patch.object(P.fcntl,name,value,create=True))
        self.original_fcntl=fcntl.fcntl
        for name,method in [('memfd_create',self.create),('write',self.write),('read',self.read),('lseek',self.seek),('fstat',self.fstat),('stat',self.stat),('fchmod',self.chmod),('close',self.close),('dup',self.dup),('ftruncate',self.truncate),('get_inheritable',self.inheritable)]:self.stack.enter_context(mock.patch.object(P.os,name,method,create=True))
        self.stack.enter_context(mock.patch.object(P.fcntl,'fcntl',side_effect=self.fcntl));return self
    def __exit__(self,*args):return self.stack.__exit__(*args)
    def create(self,name,flags):
        if self.failure=='create':raise PermissionError(1,'private')
        assert flags==11
        fd=self.next;self.next+=1;self.handles[fd]={'raw':bytearray(),'offset':0,'mode':0o100600,'seals':0,'inode':fd};return fd
    def write(self,fd,raw):
        if fd not in self.handles:return self.original['write'](fd,raw)
        item=self.handles[fd]
        if item['seals']&8:raise PermissionError(1,'sealed')
        count=min(3,len(raw));at=item['offset'];item['raw'][at:at+count]=raw[:count];item['offset']+=count;return count
    def read(self,fd,size):
        if fd not in self.handles:return self.original['read'](fd,size)
        item=self.handles[fd];at=item['offset'];raw=bytes(item['raw'][at:at+size]);item['offset']+=len(raw);return raw
    def seek(self,fd,offset,whence):
        if fd not in self.handles:return self.original['lseek'](fd,offset,whence)
        assert whence==0;self.handles[fd]['offset']=offset;return offset
    def fstat(self,fd):
        if fd not in self.handles:return self.original['fstat'](fd)
        item=self.handles[fd];return self.ns(st_dev=7,st_ino=item['inode'],st_mode=item['mode'],st_uid=self.uid,st_gid=0,st_nlink=0,st_size=len(item['raw']),st_mtime_ns=1,st_ctime_ns=1)
    def stat(self,path,*args,**kwargs):
        raw=os.fsencode(path)
        if raw.startswith(b'/proc/self/fd/'):
            if self.failure=='proc':raise FileNotFoundError(2,'private')
            return self.fstat(int(raw.rsplit(b'/',1)[1]))
        return self.original['stat'](path,*args,**kwargs)
    def chmod(self,fd,mode):
        if fd not in self.handles:return self.original['fchmod'](fd,mode)
        self.handles[fd]['mode']=0o100000|mode
    def close(self,fd):
        if fd not in self.handles:return self.original['close'](fd)
        del self.handles[fd]
    def dup(self,fd):
        if fd not in self.handles:return self.original['dup'](fd)
        result=self.next;self.next+=1;self.handles[result]=self.handles[fd];return result
    def truncate(self,fd,size):
        if fd not in self.handles:return self.original['ftruncate'](fd,size)
        if self.handles[fd]['seals']&6:raise PermissionError(1,'sealed')
    def inheritable(self,fd):return False if fd in self.handles else self.original['get_inheritable'](fd)
    def fcntl(self,fd,command,*args):
        if fd not in self.handles:return self.original_fcntl(fd,command,*args)
        if command==1033:
            if self.failure=='seal':raise PermissionError(1,'private')
            self.handles[fd]['seals']|=args[0];return 0
        if command==1034:return self.handles[fd]['seals']
        raise AssertionError('unexpected fake seal operation')


class NativePythonTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(P, 'reviewed evidence program is absent')

    def test_RequestWire(self):
        value = request()
        raw = json.dumps(value).encode()
        self.assertEqual(P.validate_request(raw, '5'*32, '6'*64), value)
        variants = []
        for key in value:
            v = copy.deepcopy(value); del v[key]; variants.append(json.dumps(v).encode())
        for key, bad in [('version', True), ('version', 1.0), ('saved_e', '0/0'), ('operation_id', 'A'*32), ('container', None)]:
            v = copy.deepcopy(value); v[key] = bad; variants.append(json.dumps(v).encode())
        variants += [raw[:-1]+b',"Version":1}', raw[:-1]+b',"version":1}', b'\xef\xbb\xbf'+raw, raw+b' {}', raw.replace(b'"version": 1', b'"version": 1e0'), raw.replace(b'"version": 1', b'"version": 18446744073709551616')]
        for bad in variants:
            with self.subTest(bad=variants.index(bad)):
                with self.assertRaises(P.Refusal): P.validate_request(bad, '5'*32, '6'*64)
        for key in value['execution']:
            v = copy.deepcopy(value); del v['execution'][key]
            with self.assertRaises(P.Refusal): P.validate_request(json.dumps(v).encode(), '5'*32, '6'*64)
        for bad in [b'{"x":1,"\\u0078":2}', b'[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[0]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]', b'"\\ud800"', b'{"x":NaN}', b'{"x":-2147483649}', b'\xff']:
            with self.assertRaises(P.Refusal): P.strict_json(bad, 65536)

    def test_RequestDigests(self):
        raw = json.dumps(request(), separators=(',', ':')).encode()
        sha, execution = P.request_digests(raw)
        self.assertEqual(sha, hashlib.sha256(raw).hexdigest())
        self.assertEqual(execution, hashlib.sha256(b'rpgmigrate.native.execution.v1\0'+bytes.fromhex(sha)).hexdigest())
        self.assertNotEqual(P.request_digests(raw+b' '), (sha, execution))

    def test_EnvironmentDNS(self):
        clean = {'LANG':'C','LC_ALL':'C'}
        P.validate_environment(clean)
        for name in ['LD_PRELOAD','LD_LIBRARY_PATH','LD_AUDIT','LOCALDOMAIN','RES_OPTIONS','LANGUAGE','LOCPATH','GCONV_PATH','NLSPATH']:
            with self.subTest(name=name), self.assertRaises(P.Refusal): P.validate_environment(dict(clean, **{name:''}))
        for locale in ['en_US.UTF-8', '', 'C.UTF-8']:
            with self.assertRaises(P.Refusal): P.validate_environment(dict(clean, LANG=locale))
        with self.assertRaises(P.Refusal) as failure: P.qualified_resolver_policy()
        self.assertEqual(failure.exception.error_class, 'capability')

    def test_ABI(self):
        self.assertEqual(P.main.__annotations__,{'argv':list[str],'return':int})
        self.assertEqual(P.run_completed_evidence.__annotations__,{'operation_id':str,'program_sha256':str,'return':int})
        self.assertGreaterEqual(len(P.ABI), 35)
        self.assertNotIn('PQexec', P.ABI)
        class Function:
            def __call__(self, *args): return 180006
        class Library: pass
        library = Library()
        for name in P.ABI: setattr(library, name, Function())
        bound = P.bind_abi(library, 180006)
        self.assertIs(bound, library)
        for name, (restype, argtypes) in P.ABI.items():
            self.assertEqual(getattr(library, name).argtypes, argtypes)
            self.assertIs(getattr(library, name).restype, restype)
        with self.assertRaises(P.Refusal): P.bind_abi(Library(), 180006)
        with self.assertRaises(P.Refusal): P.bind_abi(library, 180007)

    def test_OrderedGuards(self):
        expected = 'target_settings target_identity configure_durability target_fence idle_guard begin lock_checkpoint lock_marker catalog_checkpoint catalog_marker row_read same_value_update deferred_effects row_comparison precommit_guard commit idle_guard unlock closure_guard'.split()
        self.assertEqual(P.GUARDS, expected)
        P.validate_guards(expected)
        for i in range(len(expected)):
            for bad in [expected[:i]+expected[i+1:], expected[:i]+[expected[i]]+expected[i:], list(reversed(expected))]:
                with self.assertRaises(P.Refusal): P.validate_guards(bad)

    def test_ResultDiscipline(self):
        self.assertEqual(P.exact_true([[b't']]), True)
        for rows in [[], [[b'f']], [[b't'],[b't']], [[b't',b't']], [[None]], [[b't\n']]]:
            with self.assertRaises(P.Refusal): P.exact_true(rows)
        failure = P.failure_envelope('5'*32, P.Refusal('unknown_commit', '57014'))
        self.assertEqual(set(failure), {'version','kind','operation_id','error_class','sqlstate'})
        self.assertFalse(any(k in failure for k in ['checkpoint','resources','message','request_sha256']))

    def test_PrivateInput(self):
        operation = os.urandom(16).hex()
        raw = json.dumps(request()).encode()
        channel = P.InputChannel(operation, time.monotonic()+3)
        errors = []
        def write(frame):
            try:
                fd = os.open(channel.path+'/request.fifo', os.O_WRONLY)
                for at in range(0, len(frame), 17): os.write(fd, frame[at:at+17])
                os.close(fd)
            except BaseException as exc: errors.append(type(exc).__name__)
        import fcntl
        lease=os.open(channel.path,os.O_RDONLY|os.O_DIRECTORY);fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
        thread = threading.Thread(target=write, args=(struct.pack('!I',len(raw))+raw,))
        thread.start()
        self.assertEqual(channel.consume(), raw)
        thread.join(3); self.assertFalse(thread.is_alive()); self.assertEqual(errors, [])
        self.assertFalse(os.path.exists(channel.path+'/request.fifo'))
        with self.assertRaises(P.Refusal): channel.consume()
        os.close(lease);channel.close()
        self.assertFalse(os.path.exists(channel.path))
        channel=P.InputChannel(os.urandom(16).hex(),time.monotonic()+3)
        thread=threading.Thread(target=write,args=(struct.pack('!I',len(raw))+raw,));thread.start()
        try:
            with self.assertRaises(P.Refusal) as refusal:channel.consume()
            self.assertEqual(refusal.exception.error_class,'input')
        finally:
            thread.join(3);channel.close()

    def test_PrivateInputTrailing(self):
        operation = os.urandom(16).hex()
        channel = P.InputChannel(operation, time.monotonic()+2)
        def write():
            fd = os.open(channel.path+'/request.fifo', os.O_WRONLY)
            os.write(fd, b'\0\0\0\2{}x'); os.close(fd)
        thread = threading.Thread(target=write); thread.start()
        with self.assertRaises(P.Refusal): channel.consume()
        thread.join(3); channel.close()
        self.assertFalse(os.path.exists(channel.path))

    def test_CredentialsPrivate(self):
        secret = 'canary-password-secret'
        failure = P.failure_envelope('5'*32, P.Refusal('connection'))
        self.assertNotIn(secret, json.dumps(failure))
        source = (HERE/'evidence_program.py').read_text()
        for forbidden in ['PQerrorMessage','PQresultErrorMessage','find_library','PQreset','traceback','/probe/']:
            self.assertNotIn(forbidden, source)
        self.assertLessEqual(len(source.encode()),122880)
        self.assertTrue(source.endswith('\n'))
        self.assertNotIn('\nRPGMIGRATE_NATIVE_EVIDENCE_PY\n',source)


    def test_CatalogFixtures(self):
        base=HERE/'testdata'/'evidence'
        inventory=json.loads((base/'native_source_manifest.json').read_bytes())
        for major in (16,18):
            for kind in ('catalog','marker_catalog'):
                name=f'native_{kind}_{major}.sql'
                raw=(base/name).read_bytes()
                self.assertEqual(hashlib.sha256(raw).hexdigest(),inventory['files'][name])
                expected=' '.join(line.strip() for line in raw.decode().splitlines() if not line.startswith('--'))
                self.assertEqual(P.catalog_sql(kind=='marker_catalog',major),expected)
        columns=json.loads((base/'native_columns.json').read_bytes())['checkpoint']
        self.assertEqual(P.COLUMNS,[(c['name'],c['type']) for c in columns])

    def test_RuntimeMetadata(self):
        value=dict(version=1,architecture='arm64',library_path='/usr/lib/aarch64-linux-gnu/libpq.so.5.18',library_sha256='a'*64,libpq_version=180006,package_name='libpq5',package_version='18.6-1.pgdg13+1')
        self.assertEqual(P.runtime_metadata(value,'aarch64'),value['library_path'])
        for key,bad in [('version',True),('architecture','amd64'),('library_path','libpq.so.5'),('library_path','/usr/lib/aarch64-linux-gnu/libpq.so.5.x'),('libpq_version',True),('libpq_version',170000),('package_name','untrusted'),('package_version','18.6 pass')]:
            with self.subTest(key=key,bad=bad), self.assertRaises(P.Refusal): P.runtime_metadata(dict(value,**{key:bad}),'aarch64')

    def test_UnknownCommit(self):
        class Diag:
            def check(self): pass
        class PQ:
            def __init__(self,results,finish_state=0):
                self.results=list(results); self.finish_state=finish_state; self.state=2
                self.cleared=[]; self.buffers=[]; self.finished=0
            def PQstatus(self,_): return 0
            def PQtransactionStatus(self,_): return self.state
            def PQbackendPID(self,_): return 23
            def PQsendQueryParams(self,*args): return 1
            def PQflush(self,_): return 0
            def PQconsumeInput(self,_): return 1
            def PQisBusy(self,_): return 0
            def PQgetResult(self,_):
                if self.results: return self.results.pop(0)
                self.state=self.finish_state; return None
            def PQresultStatus(self,r): return r['status']
            def PQresultErrorField(self,r,_): return r.get('sqlstate')
            def PQcmdStatus(self,r): return r['tag']
            def PQcmdTuples(self,r): return r.get('count',b'')
            def PQntuples(self,r): return len(r.get('rows',[]))
            def PQnfields(self,r): return len(r.get('rows',[[None]])[0]) if r.get('rows') else 0
            def PQgetisnull(self,r,i,j): return int(r['rows'][i][j] is None)
            def PQgetlength(self,r,i,j): return len(r['rows'][i][j])
            def PQgetvalue(self,r,i,j):
                buffer=P.C.create_string_buffer(r['rows'][i][j]); self.buffers.append(buffer); return P.C.addressof(buffer)
            def PQclear(self,r):
                self.cleared.append(r)
                for buffer in self.buffers: P.C.memset(P.C.addressof(buffer),ord('!'),len(buffer))
                self.buffers=[]
            def PQfinish(self,_): self.finished+=1
        def session(pq):
            s=P.Session.__new__(P.Session)
            s.lib=pq; s.conn=1; s.deadline=time.monotonic()+3; s.diag=Diag(); s.notice=False; s.sqlstate=''
            from types import SimpleNamespace
            s.connection=SimpleNamespace(check=lambda:None,close=lambda:None);s.claimed=True
            s.commit_dispatched=False; s.commit_ack=False; s.backend=23; s.props={}; s.connection_facts=lambda:{}
            return s
        good=dict(status=1,tag=b'COMMIT')
        for results,after in [([],0),([dict(status=7,sqlstate=b'57014')],0),([good,good],0),([good,good,good],0),([good],2)]:
            pq=PQ(results,after); s=session(pq)
            with self.assertRaises(P.Refusal) as failure: s.query('COMMIT;',tag='COMMIT',rows=0,width=0,commit=True)
            self.assertEqual(failure.exception.error_class,'unknown_commit')
            self.assertFalse(s.commit_ack)
            self.assertEqual(len(pq.cleared),len(results))
            s.close(False); s.close(False); self.assertEqual(pq.finished,1)
        pq=PQ([good]); s=session(pq)
        self.assertEqual(s.query('COMMIT;',tag='COMMIT',rows=0,width=0,commit=True),[])
        self.assertTrue(s.commit_ack); s.close(); self.assertEqual(pq.finished,1)
        result=dict(status=2,tag=b'SELECT 1',count=b'1',rows=[[b't']])
        pq=PQ([result]); pq.state=0; s=session(pq)
        copied=s.query('SELECT true;')
        self.assertEqual(copied,[[b't']]); self.assertEqual(len(pq.cleared),1); s.close()
        result=dict(status=2,tag=b'SELECT 1',count=b'1',rows=[[b'f']])
        pq=PQ([result]); pq.state=0; s=session(pq)
        with self.assertRaises(P.Refusal): P.exact_true(s.query('SELECT false;'))
        s.close(False)

    def test_Writer(self):
        import subprocess
        op=os.urandom(16).hex(); value=request(); value['operation_id']=op
        raw=json.dumps(value).encode(); channel=P.InputChannel(op,time.monotonic()+5)
        payload=json.dumps(response(value,raw),separators=(',',':')).encode()+b'\n'
        env=dict(os.environ,LANG='C',LC_ALL='C')
        for key in list(env):
            if key.startswith('LD_') or key in P.BANNED:del env[key]
        writer=(HERE/'evidence_request_writer.py').read_text()
        proc=subprocess.Popen([sys.executable,'-I','-B','-W','error','-c',writer,op],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
        errors=[]
        def receive():
            try:self.assertEqual(channel.consume(),raw);channel.finish(payload)
            except BaseException as exc:errors.append(type(exc).__name__)
        thread=threading.Thread(target=receive);thread.start()
        stdout,stderr=proc.communicate(raw,timeout=7);thread.join(7)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        self.assertEqual(proc.returncode,0);self.assertEqual(stderr,b'');self.assertEqual(stdout,payload)
        self.assertFalse(os.path.exists(channel.path))
        for bad in [raw+b' {}',raw[:-1]+b',"version":1}',b'{}',raw.replace(b'"version": 1',b'"version": true')]:
            other=os.urandom(16).hex();channel=P.InputChannel(other,time.monotonic()+5)
            try:
                result=subprocess.run([sys.executable,'-I','-B','-W','error','-c',writer,other],input=bad.replace(op.encode(),other.encode()),capture_output=True,env=env,timeout=7)
                self.assertNotEqual(result.returncode,0);self.assertEqual(result.stdout,b'');self.assertEqual(result.stderr,b'')
            finally:channel.close()

    def test_PrivateAdmission(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'work'; root.mkdir(mode=0o700)
            owned=root/'pgcopydb'; owned.mkdir(mode=0o700)
            control=owned/'control'; control.mkdir(mode=0o700)
            req,manifest,endpos,row=private_fixture(owned.stat())
            for path,value in [(owned/'manifest.json',json.dumps(manifest).encode()),(control/'endpos.json',json.dumps(endpos).encode()),(control/'lock',b'')]:
                path.write_bytes(value); path.chmod(0o600)
            actual_open=P.fdopen; actual_stat=os.stat
            def mapped_open(path,directory=False,parent=None):
                return actual_open(str(root) if path=='/work' else path,directory,parent)
            def mapped_stat(path,*args,**kwargs):
                return actual_stat(str(root) if path=='/work' else path,*args,**kwargs)
            with mock.patch.object(P,'fdopen',side_effect=mapped_open), mock.patch.object(P.os,'stat',side_effect=mapped_stat):
                state=P.PrivateState(req)
                checkpoint=P.validate_control_row(row,state)
                self.assertEqual(checkpoint['checkpoint_id'],'1:0:7')
                self.assertEqual(checkpoint['durable_replay_lsn'],'0/10')
                self.assertEqual(state.resources(),[dict(kind='replication_slot',name='native_slot',owned=True,retain=False),dict(kind='publication',name='native_publication',owned=True,retain=False),dict(kind='workdir',name='/work/pgcopydb',owned=True,retain=True)])
                with self.assertRaises((P.Refusal,BlockingIOError)): P.PrivateState(req)
                for index,bad in [(3,'ff'*16),(6,'2'),(18,'18446744073709551616'),(25,'0/21'),(26,'0/10'),(27,'keepalive'),(28,'f')]:
                    changed=list(row); changed[index]=bad
                    with self.subTest(index=index), self.assertRaises(P.Refusal): P.validate_control_row(changed,state)
                changed=list(row); follow=json.loads(changed[24]); follow['operator']['checkpoint_generation']=True; changed[24]=json.dumps(follow)
                with self.assertRaises(P.Refusal): P.validate_control_row(changed,state)
                state.revalidate()
                replacement=owned/'replacement'; replacement.write_bytes((owned/'manifest.json').read_bytes()); replacement.chmod(0o600)
                os.replace(replacement,owned/'manifest.json')
                with self.assertRaises(P.Refusal): state.revalidate()
                state.close(False)
            for key in manifest:
                changed=copy.deepcopy(manifest); del changed[key]
                with self.subTest(key=key), self.assertRaises(P.Refusal): P.validate_manifest(changed,req,owned.stat())
            for path,bad in [(('format_version',),7),(('operator','checkpoint_generation'),True),(('operator','workdir_id'),'other'),(('follow_state','cleanup'),dict(slot_removed=True,publication_removed=True)),(('clone_state','phase'),'finished')]:
                changed=copy.deepcopy(manifest); location=changed
                for key in path[:-1]: location=location[key]
                location[path[-1]]=bad
                with self.subTest(path=path), self.assertRaises(P.Refusal): P.validate_manifest(changed,req,owned.stat())
            for key,bad in [('sealed',False),('sealed',1),('end_lsn',33),('version',True)]:
                with self.subTest(key=key), self.assertRaises(P.Refusal): P.validate_control(dict(endpos,**{key:bad}),manifest,req)

    def test_DescriptorMutation(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'private'; path.write_bytes(b'original'); path.chmod(0o600)
            pinned=P.StableFile(str(path),100,True)
            path.write_bytes(b'replaced')
            with self.assertRaises(P.Refusal): pinned.check()
            pinned.close()
            alias=Path(directory)/'alias'; alias.symlink_to(path)
            with self.assertRaises(OSError): P.StableFile(str(alias),100,True)
            os.link(path,Path(directory)/'hardlink')
            with self.assertRaises(P.Refusal): P.StableFile(str(path),100,True)

    def test_PrivateInputTimeout(self):
        op=os.urandom(16).hex(); channel=P.InputChannel(op,time.monotonic()-1)
        try:
            with self.assertRaises(P.Refusal) as failure: channel.consume()
            self.assertEqual(failure.exception.error_class,'timeout')
        finally: channel.close()
        self.assertFalse(os.path.exists(channel.path))


    def test_ObservationOrder(self):
        from types import SimpleNamespace
        req,manifest,endpos,row=private_fixture(os.stat(HERE))
        state=SimpleNamespace(req=req,manifest=manifest,target=manifest['target_provenance'][0]['record'],revalidate=lambda:None)
        materials=SimpleNamespace(connection=lambda role:role.encode(),check=lambda:None)
        events=[]; fault=[None]
        class Session:
            def __init__(self,library,conninfo,deadline,diag):
                self.role=conninfo; self.backend=23; self.props={'PQuser':b'fixture','PQdb':b'database','ssl':0}
                if conninfo==b'target':
                    if events[-1] != ('close',b'source',True): raise AssertionError('target opened before positive source closure')
                events.append(('open',conninfo))
            def quote(self,value): return '"'+value+'"'
            def query(self,sql,params=(),tag='SELECT 1',state=0,rows=1,width=1,commit=False):
                events.append(('query',self.role,tag))
                if commit and fault[0]=='unknown_commit': raise P.Refusal('unknown_commit')
                if not rows: return []
                if width==6: return [[b'123',b'11',b'1',b't',b'23',b'160006']]
                if width==7: return [[b'456',b'22',b'33',b'33',b'23',b'180006',b't']]
                if width==4: return [[b'fixture',b'fixture',b'database',b'f']]
                if width==29: return [[None if v is None else v.encode() for v in row]]
                if 'set_config' in sql: return [[b'remote_apply']]
                if 'pg_advisory_unlock' in sql and fault[0]=='false_unlock': return [[b'f']]
                return [[b't']]
            def close(self,healthy=True):
                events.append(('close',self.role,healthy))
                if self.role==b'target' and fault[0]=='unknown_commit': raise P.Refusal('diagnostic')
        with mock.patch.object(P,'Session',Session):
            checkpoint,guards=P.observe(state,None,materials,time.monotonic()+25,None)
            self.assertEqual(guards,P.GUARDS); self.assertEqual(checkpoint['checkpoint_id'],'1:0:7')
            self.assertEqual(events[-1],('close',b'target',True))
            for mode,want in [('false_unlock','result'),('unknown_commit','unknown_commit')]:
                events.clear(); fault[0]=mode
                with self.assertRaises(P.Refusal) as error: P.observe(state,None,materials,time.monotonic()+25,None)
                self.assertEqual(error.exception.error_class,want)
                self.assertEqual(events[-1],('close',b'target',False))

    def test_NoSQLBeforeAdmission(self):
        raw=json.dumps(dict(request(),version=True)).encode()
        writes=[]
        with mock.patch.dict(os.environ,{'LANG':'C','LC_ALL':'C'},clear=True), mock.patch.object(P.resource,'setrlimit'), mock.patch.object(P,'InputChannel') as channel, mock.patch.object(P,'Session') as session, mock.patch.object(P.os,'write',side_effect=lambda fd,data:(writes.append((fd,data)) or len(data))):
            channel.return_value.consume.return_value=raw
            self.assertEqual(P.run_completed_evidence('5'*32,'6'*64),1)
            session.assert_not_called()
        self.assertEqual(writes,[])
        channel.return_value.finish.assert_called_once()
        failure=json.loads(channel.return_value.finish.call_args.args[0])
        self.assertEqual(failure['kind'],'completed_evidence_failure')
        self.assertEqual(set(failure),{'version','kind','operation_id','error_class','sqlstate'})


    def test_ConnectPolling(self):
        events=[]
        class Diag:
            def check(self): pass
        class Library:
            def __init__(self): self.polls=0; self.finished=0
            def PQconnectStartParams(self,*args): events.append('start'); return 1
            def PQsetNoticeReceiver(self,*args): events.append('receiver')
            def PQconnectPoll(self,_):
                events.append('poll'); self.polls+=1; return 1 if self.polls==1 else 3
            def PQstatus(self,_): return 2 if self.polls<2 else 0
            def PQsocket(self,_): return 23
            def PQsetnonblocking(self,*args): return 0
            def PQtransactionStatus(self,_): return 0
            def PQbackendPID(self,_): return 23
            def PQfinish(self,_): self.finished+=1
        lib=Library()
        with mock.patch.object(P,'wait_fd',side_effect=[False,True]), mock.patch.object(P.Session,'connection_facts',return_value={}):
            from types import SimpleNamespace
            connection=SimpleNamespace(claim=lambda:(None,None),check=lambda:None,close=lambda:None,overrides={})
            session=P.Session(lib,connection,time.monotonic()+2,Diag())
            session.close()
        self.assertEqual(events,['start','receiver','poll','poll']); self.assertEqual(lib.finished,1)


    def test_RelayProtocol(self):
        import subprocess
        op=os.urandom(16).hex(); value=request(); value['operation_id']=op
        raw=json.dumps(value).encode(); path=Path('/tmp')/('rpgmigrate-evidence-'+op)
        path.mkdir(mode=0o700)
        readers={}; writers={}
        try:
            for name in ('request.fifo','stdout.fifo','stderr.fifo'):
                os.mkfifo(path/name,0o600)
                readers[name]=os.open(path/name,os.O_RDONLY|os.O_NONBLOCK)
                writers[name]=os.open(path/name,os.O_WRONLY|os.O_NONBLOCK)
            env=dict(os.environ,LANG='C',LC_ALL='C')
            for key in list(env):
                if key.startswith('LD_') or key in P.BANNED: del env[key]
            source=(HERE/'evidence_request_writer.py').read_text()
            proc=subprocess.Popen([sys.executable,'-I','-B','-W','error','-c',source,op],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
            proc.stdin.write(raw);proc.stdin.close();proc.stdin=None
            framed=bytearray();deadline=time.monotonic()+2
            while len(framed)<len(raw)+4 and time.monotonic()<deadline:
                try: framed.extend(os.read(readers['request.fifo'],8192))
                except BlockingIOError: time.sleep(.005)
                if proc.poll() is not None: break
            self.assertEqual(bytes(framed),struct.pack('!I',len(raw))+raw)
            import fcntl
            directory=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
            try:
                with self.assertRaises(BlockingIOError): fcntl.flock(directory,fcntl.LOCK_EX|fcntl.LOCK_NB)
                os.close(writers.pop('request.fifo'));os.close(readers.pop('request.fifo'));os.unlink(path/'request.fifo')
                request_sha,execution_sha=P.request_digests(raw)
                response=dict(version=1,kind='completed_evidence',operation_id=op,request_sha256=request_sha,program_sha256=value['program_sha256'],execution_sha256=execution_sha,work=value['execution']['work'],checkpoint=dict(checkpoint_id='1:0:7',certified_lsn='0/10',durable_replay_lsn='0/10',drain_end_lsn='0/20',drain_replay_lsn='0/30',drain_kind='later_begin',barrier_confirmed=True),resources=[dict(kind='workdir',name='/work/pgcopydb',owned=True,retain=True)],guards=P.GUARDS,source_closed=True,target_closed=True,private_closed=True,diagnostics_clean=True)
                payload=json.dumps(response,separators=(',',':')).encode()+b'\n'
                for name in ('stdout.fifo','stderr.fifo'):
                    os.unlink(path/name);os.close(readers.pop(name))
                path.rmdir()
                os.write(writers['stdout.fifo'],payload)
                self.assertIsNone(proc.poll(),'relay accepted EOF before all output writers closed')
                for fd in writers.values():os.close(fd)
                writers.clear()
                stdout,stderr=proc.communicate(timeout=5)
                self.assertEqual(proc.returncode,0,stderr);self.assertEqual(stdout,payload);self.assertEqual(stderr,b'')
                fcntl.flock(directory,fcntl.LOCK_EX|fcntl.LOCK_NB)
            finally:os.close(directory)
        finally:
            for fd in list(readers.values())+list(writers.values()):os.close(fd)
            if 'proc' in locals():
                if proc.poll() is None:proc.kill()
                proc.communicate(timeout=3)
            if path.exists():
                for child in path.iterdir():child.unlink()
                path.rmdir()


    def relay_environment(self):
        env=dict(os.environ,LANG='C',LC_ALL='C')
        for key in list(env):
            if key.startswith('LD_') or key in P.BANNED:del env[key]
        return env

    def test_RelayFaults(self):
        import subprocess
        for mode in ('early_eof','duplicate','bad_digest','stderr','failure','malformed','extra_line'):
            with self.subTest(mode=mode):
                op=os.urandom(16).hex();value=request();value['operation_id']=op;raw=json.dumps(value).encode()
                channel=P.InputChannel(op,time.monotonic()+4)
                obj=response(value,raw)
                if mode=='bad_digest':obj['request_sha256']='0'*64
                if mode=='failure':obj=P.failure_envelope(op,P.Refusal('unknown_commit'))
                payload=json.dumps(obj,separators=(',',':')).encode()+b'\n'
                if mode=='duplicate':payload*=2
                if mode=='malformed':payload=b'{"kind":null}\n'
                if mode=='extra_line':payload+=b' \n'
                errors=[]
                def produce():
                    try:
                        self.assertEqual(channel.consume(),raw)
                        if mode=='stderr':os.write(channel.writers['stderr.fifo'],b'fixture diagnostic')
                        channel.finish(None if mode=='early_eof' else payload)
                    except BaseException as error:errors.append(type(error).__name__)
                thread=threading.Thread(target=produce);thread.start()
                proc=subprocess.Popen([sys.executable,'-I','-B','-W','error','-c',(HERE/'evidence_request_writer.py').read_text(),op],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=self.relay_environment())
                stdout,stderr=proc.communicate(raw,timeout=7);thread.join(7)
                self.assertFalse(thread.is_alive());self.assertNotEqual(proc.returncode,0)
                self.assertEqual(stderr,b'fixture diagnostic' if mode=='stderr' else b'')
                self.assertEqual(stdout,payload if mode=='failure' else b'')
                self.assertFalse(os.path.exists(channel.path))
        # The outer pipe cap also rejects a fully received oversized envelope.
        spec=importlib.util.spec_from_file_location('relay',HERE/'evidence_request_writer.py');relay=importlib.util.module_from_spec(spec);spec.loader.exec_module(relay)
        with self.assertRaises(ValueError):relay.result_envelope(b'x'*65536+b'\n',raw,op)

    def test_RelayLifetime(self):
        import subprocess,fcntl,select
        op=os.urandom(16).hex();value=request();value['operation_id']=op;raw=json.dumps(value).encode()
        channel=P.InputChannel(op,time.monotonic()+5);probe=os.open(channel.path,os.O_RDONLY|os.O_DIRECTORY)
        announce_r,announce_w=os.pipe();gate_r,gate_w=os.pipe()
        source=(HERE/'evidence_request_writer.py').read_text().rsplit("if __name__=='__main__':",1)[0]
        source+=f"rc=main(sys.argv)\nos.write({announce_w},b'R')\nos.read({gate_r},1)\nraise SystemExit(rc)\n"
        errors=[];payload=json.dumps(response(value,raw),separators=(',',':')).encode()+b'\n'
        def produce():
            try:self.assertEqual(channel.consume(),raw);channel.finish(payload)
            except BaseException as error:errors.append(type(error).__name__)
        thread=threading.Thread(target=produce);thread.start()
        proc=subprocess.Popen([sys.executable,'-I','-B','-W','error','-c',source,op],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=self.relay_environment(),pass_fds=(announce_w,gate_r))
        os.close(announce_w);os.close(gate_r)
        try:
            proc.stdin.write(raw);proc.stdin.close();proc.stdin=None
            self.assertTrue(select.select([announce_r],[],[],3)[0]);self.assertEqual(os.read(announce_r,1),b'R')
            with self.assertRaises(BlockingIOError):fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
            self.assertTrue(thread.is_alive(),'main completed before relay OS exit')
            os.write(gate_w,b'X')
            stdout,stderr=proc.communicate(timeout=4);thread.join(4)
            self.assertEqual(proc.returncode,0);self.assertEqual(stdout,payload);self.assertEqual(stderr,b'')
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
            fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finally:
            os.close(probe);os.close(announce_r);os.close(gate_w)
            if proc.poll() is None:proc.kill();proc.communicate(timeout=3)
            thread.join(6)
            channel.close()

    def test_RelayDuplicate(self):
        import subprocess,fcntl,select
        op=os.urandom(16).hex();value=request();value['operation_id']=op;raw=json.dumps(value).encode()
        channel=P.InputChannel(op,time.monotonic()+6);probe=os.open(channel.path,os.O_RDONLY|os.O_DIRECTORY)
        source=(HERE/'evidence_request_writer.py').read_text()
        first=subprocess.Popen([sys.executable,'-I','-B','-W','error','-c',source,op],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=self.relay_environment())
        try:
            deadline=time.monotonic()+2;held=False
            while time.monotonic()<deadline and not held:
                try:fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(probe,fcntl.LOCK_UN);select.select([],[],[],.005)
                except BlockingIOError:held=True
            self.assertTrue(held)
            duplicate=subprocess.run([sys.executable,'-I','-B','-W','error','-c',source,op],input=raw,capture_output=True,env=self.relay_environment(),timeout=3)
            self.assertNotEqual(duplicate.returncode,0);self.assertEqual(duplicate.stdout,b'');self.assertEqual(duplicate.stderr,b'')
            first.stdin.write(raw);first.stdin.close();first.stdin=None
            self.assertEqual(channel.consume(),raw)
            # The consumed request path cannot be recreated by another collector.
            first.kill();first.communicate(timeout=3)
            repeated=subprocess.run([sys.executable,'-I','-B','-W','error','-c',source,op],input=raw,capture_output=True,env=self.relay_environment(),timeout=3)
            self.assertNotEqual(repeated.returncode,0);self.assertEqual(repeated.stdout,b'');self.assertEqual(repeated.stderr,b'')
            self.assertFalse(os.path.exists(channel.path+'/request.fifo'))
            with self.assertRaises(P.Refusal):channel.consume()
        finally:
            if first.poll() is None:first.kill();first.communicate(timeout=3)
            os.close(probe);channel.close()

    def test_RelayDeadline(self):
        import subprocess
        op=os.urandom(16).hex();channel=P.InputChannel(op,time.monotonic()+7)
        proc=subprocess.Popen([sys.executable,'-I','-B','-W','error','-c',(HERE/'evidence_request_writer.py').read_text(),op],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=self.relay_environment())
        try:
            # Keep stdin open: a complete request needs actual checked-input EOF.
            value=request();value['operation_id']=op;proc.stdin.write(json.dumps(value).encode());proc.stdin.flush()
            proc.wait(timeout=7);proc.stdin.close();proc.stdin=None
            stdout,stderr=proc.communicate(timeout=1)
            self.assertNotEqual(proc.returncode,0);self.assertEqual(stdout,b'');self.assertEqual(stderr,b'')
            self.assertTrue(os.path.exists(channel.path+'/request.fifo'))
        finally:
            if proc.poll() is None:proc.kill();proc.communicate(timeout=3)
            channel.close()

    def test_RelayMainQuiet(self):
        import subprocess
        op=os.urandom(16).hex();sha=hashlib.sha256((HERE/'evidence_program.py').read_bytes()).hexdigest();value=request();value['operation_id']=op;value['program_sha256']=sha
        main=subprocess.Popen([sys.executable,'-I','-B','-W','error','-','completed_evidence',op,sha],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=self.relay_environment())
        main.stdin.write((HERE/'evidence_program.py').read_bytes());main.stdin.close();main.stdin=None
        try:
            relay=subprocess.run([sys.executable,'-I','-B','-W','error','-c',(HERE/'evidence_request_writer.py').read_text(),op],input=json.dumps(value).encode(),capture_output=True,env=self.relay_environment(),timeout=9)
            stdout,stderr=main.communicate(timeout=7)
            self.assertEqual(stdout,b'');self.assertEqual(stderr,b'')
            self.assertNotEqual(main.returncode,0);self.assertNotEqual(relay.returncode,0)
            self.assertEqual(relay.stderr,b'')
            failure=json.loads(relay.stdout);self.assertEqual(failure['kind'],'completed_evidence_failure')
            self.assertEqual(set(failure),{'version','kind','operation_id','error_class','sqlstate'})
            self.assertFalse(os.path.exists('/tmp/rpgmigrate-evidence-'+op))
        finally:
            if main.poll() is None:main.kill();main.communicate(timeout=3)


    def test_RelayClosureOrder(self):
        from types import SimpleNamespace
        value=request();raw=json.dumps(value).encode()
        for unknown in (False,True):
            events=[];payloads=[]
            class Channel:
                def __init__(self,*args):pass
                def route(self):events.append('route')
                def consume(self):events.append('consume');return raw
                def finish(self,payload,deadline=None):
                    self_payload=json.loads(payload);payloads.append(self_payload)
                    for name in ('source','target','private','materials','diagnostics','resolver'):
                        if not unknown:self.assert_before(name)
                    events.append('publish')
                def assert_before(self,name):
                    if name not in events:raise AssertionError('publication before '+name)
            def observe(*args):
                events.extend(['source','target'])
                if unknown:raise P.Refusal('unknown_commit')
                return response(value,raw)['checkpoint'],P.GUARDS
            state=SimpleNamespace(close=lambda:events.append('private'),resources=lambda:response(value,raw)['resources'])
            materials=SimpleNamespace(files=[],close=lambda:events.append('materials'),prepare=lambda *args:events.append('prepare'))
            resolver=SimpleNamespace(raw=b'nameserver 127.0.0.1\n',check=lambda:None,close=lambda:events.append('resolver'))
            diagnostics=SimpleNamespace(close=lambda:events.append('diagnostics'))
            with mock.patch.dict(os.environ,{'LANG':'C','LC_ALL':'C'},clear=True),mock.patch.object(P.os,'geteuid',return_value=65532),mock.patch.object(P.resource,'setrlimit'),mock.patch.object(P,'InputChannel',Channel),mock.patch.object(P,'PrivateState',return_value=state),mock.patch.object(P,'Materialization',return_value=materials),mock.patch.object(P,'StableFile',return_value=resolver),mock.patch.object(P,'Diagnostics',return_value=diagnostics),mock.patch.object(P,'load_runtime'),mock.patch.object(P,'qualified_resolver_policy'),mock.patch.object(P,'observe',side_effect=observe) as operation,mock.patch.object(P.os,'write') as ordinary:
                self.assertEqual(P.run_completed_evidence('5'*32,'6'*64),1 if unknown else 0)
                operation.assert_called_once();ordinary.assert_not_called()
            self.assertEqual(events.count('consume'),1);self.assertEqual(events.count('publish'),1)
            if unknown:self.assertEqual(payloads[0]['error_class'],'unknown_commit');self.assertNotIn('checkpoint',payloads[0])
            else:self.assertIs(payloads[0]['private_closed'],True)

    def test_RelayCloseFault(self):
        channel=P.InputChannel(os.urandom(16).hex(),time.monotonic()+3)
        target=channel.writers['stdout.fifo'];owned=list(channel.writers.values())+list(channel.readers.values())
        real=P.checked_close
        def close(fd):
            real(fd)
            if fd==target:raise P.Refusal('closure')
        with mock.patch.object(P,'checked_close',side_effect=close):
            with self.assertRaises(P.Refusal) as error:channel.finish()
        self.assertEqual(error.exception.error_class,'closure');self.assertFalse(os.path.exists(channel.path))
        for fd in owned:
            with self.assertRaises(OSError):os.fstat(fd)

    def test_RelayReplacement(self):
        import subprocess
        op=os.urandom(16).hex();value=request();value['operation_id']=op;raw=json.dumps(value).encode();channel=P.InputChannel(op,time.monotonic()+4)
        proc=subprocess.Popen([sys.executable,'-I','-B','-W','error','-c',(HERE/'evidence_request_writer.py').read_text(),op],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=self.relay_environment())
        try:
            proc.stdin.write(raw);proc.stdin.close();proc.stdin=None;self.assertEqual(channel.consume(),raw)
            os.rename(channel.path+'/stdout.fifo',channel.path+'/replaced.fifo')
            with self.assertRaises(P.Refusal):channel.finish()
            stdout,stderr=proc.communicate(timeout=4)
            self.assertNotEqual(proc.returncode,0);self.assertEqual(stdout,b'');self.assertEqual(stderr,b'')
        finally:
            if proc.poll() is None:proc.kill();proc.communicate(timeout=3)
            channel.close()
            if os.path.isdir(channel.path):
                for child in Path(channel.path).iterdir():child.unlink()
                os.rmdir(channel.path)

    def test_RelayOverflow(self):
        import subprocess,select
        op=os.urandom(16).hex();value=request();value['operation_id']=op;raw=json.dumps(value).encode();channel=P.InputChannel(op,time.monotonic()+4)
        proc=subprocess.Popen([sys.executable,'-I','-B','-W','error','-c',(HERE/'evidence_request_writer.py').read_text(),op],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=self.relay_environment())
        try:
            proc.stdin.write(raw);proc.stdin.close();proc.stdin=None;self.assertEqual(channel.consume(),raw)
            channel.check()
            for name in tuple(channel.remaining):os.unlink(name,dir_fd=channel.directory)
            channel.remaining.clear();os.rmdir(channel.path)
            payload=b'x'*65537;at=0;deadline=time.monotonic()+3
            while at<len(payload) and time.monotonic()<deadline:
                try:at+=os.write(channel.writers['stdout.fifo'],payload[at:])
                except BlockingIOError:select.select([],[channel.writers['stdout.fifo']],[],.02)
                except BrokenPipeError:break
            # Both producer writers remain open: refusal must precede EOF/parser checks.
            proc.wait(timeout=3);stdout,stderr=proc.communicate(timeout=1)
            self.assertNotEqual(proc.returncode,0);self.assertEqual(stdout,b'');self.assertEqual(stderr,b'')
        finally:
            if proc.poll() is None:proc.kill();proc.communicate(timeout=3)
            with self.assertRaises(P.Refusal):channel.close()

    def require_file_api(self):
        for name in ('parse_connection_options','selected_option','connection_home','SealedFile','ConnectionInput'):
            self.assertTrue(hasattr(P,name),'missing pre-consumption file API '+name)

    def test_FileOptions(self):
        self.require_file_api()
        cases=[(b"host=one,two dbname='a=b' password='' sslpassword='q\\'x'",{b'dbname':b'a=b',b'password':b'',b'sslpassword':b"q'x"}),(b'postgresql://u:p%2Bss@first,second/db%3Dname?sslpassword=x+y',{b'password':b'p+ss',b'dbname':b'db=name',b'sslpassword':b'x+y'}),(b"host=ignored host=last password='a\\\\b'",{b'host':b'last',b'password':b'a\\b'})]
        for raw,values in cases:
            lib=FileOptionsLibrary({raw:values});actual=P.parse_connection_options(lib,raw)
            self.assertEqual(lib.parsed,[raw]);self.assertEqual(lib.options_freed,1)
            for key,value in values.items():self.assertEqual(actual[key][0],value)
        lib=FileOptionsLibrary({b'bad':None})
        with self.assertRaises(P.Refusal) as error:P.parse_connection_options(lib,b'bad')
        self.assertEqual(error.exception.error_class,'connection');self.assertEqual(lib.errors_freed,1)
        self.assertNotIn('diagnostic-canary',str(error.exception))
        lib=FileOptionsLibrary({b'duplicate':[(b'host',b'one'),(b'host',b'two')]})
        with self.assertRaises(P.Refusal):P.parse_connection_options(lib,b'duplicate')
        self.assertEqual(lib.options_freed,1)

    def test_FileOptionPrecedence(self):
        self.require_file_api();env={b'PGPASSWORD':b'env',b'EMPTY':b''}
        for explicit,environment,compiled,want in [(b'',b'PGPASSWORD',b'default',b''),(b'raw',b'PGPASSWORD',b'default',b'raw'),(None,b'PGPASSWORD',b'default',b'env'),(None,b'EMPTY',b'default',b''),(None,b'ABSENT',b'default',b'default'),(None,None,None,None)]:
            self.assertEqual(P.selected_option({b'password':(explicit,environment,compiled)},b'password',env),want)
        self.assertEqual(P.connection_home({b'HOME':b'/private/home'}),b'/private/home')
        for env in ({},{b'HOME':b''}):
            with self.assertRaises(P.Refusal) as error:P.connection_home(env)
            self.assertEqual(error.exception.error_class,'capability')

    def test_FileSources(self):
        self.require_file_api()
        from types import SimpleNamespace
        selected=[]
        class Files:
            def pin(self,path):
                selected.append(path)
                return SimpleNamespace(raw=b'private-file',before=SimpleNamespace(st_mode=0o100600,st_uid=os.geteuid()),check=lambda:None)
            def check(self):pass
        class Snapshot:
            def __init__(self,raw,deadline):self.path=b'/proc/self/fd/987';self.closed=False
            def check(self):
                if self.closed:raise P.Refusal('capability')
            def close(self):self.closed=True
        raw=b"dbname='a=b' passfile='' sslmode=disable password=''"
        values={b'dbname':b'a=b',b'passfile':b'',b'sslmode':b'disable',b'password':b''}
        with mock.patch.object(P,'SealedFile',Snapshot):
            inp=P.ConnectionInput(FileOptionsLibrary({raw:values}),raw,{b'HOME':b'/home/fixture',b'PGPASSFILE':b'/ignored',b'PGPASSWORD':b'ignored'},Files(),time.monotonic()+3)
            self.assertEqual(selected,[b'/home/fixture/.pgpass']);self.assertEqual(inp.values[0],raw);self.assertEqual(inp.keywords[0],b'dbname');inp.close()
            for path in (b'local/pass',b'~/literal',b'/absolute/pass'):
                selected.clear();raw=b'fixture'
                inp=P.ConnectionInput(FileOptionsLibrary({raw:{b'passfile':path,b'sslmode':b'disable'}}),raw,{},Files(),time.monotonic()+3)
                self.assertEqual(selected,[path]);inp.close()
            selected.clear()
            inp=P.ConnectionInput(FileOptionsLibrary({b'fixture':{b'password':b'explicit',b'sslmode':b'disable'}}),b'fixture',{b'PGPASSFILE':b'/ignored',b'PGSSLKEY':b'/ignored'},Files(),time.monotonic()+3)
            self.assertEqual(selected,[]);inp.close()
            for service in (b'',b'name'):
                with self.assertRaises(P.Refusal) as error:P.ConnectionInput(FileOptionsLibrary({b'fixture':{}}),b'fixture',{b'PGSERVICE':service},Files(),time.monotonic()+3)
                self.assertEqual(error.exception.error_class,'capability')
            selected.clear()
            inp=P.ConnectionInput(FileOptionsLibrary({b'fixture':{b'password':b'explicit',b'sslmode':b'disable'}}),b'fixture',{b'PGSERVICEFILE':b'/ignored',b'PGSYSCONFDIR':b'/ignored'},Files(),time.monotonic()+3)
            self.assertEqual(selected,[]);inp.close()
            with self.assertRaises(P.Refusal):P.ConnectionInput(FileOptionsLibrary({b'fixture':{b'password':b'explicit',b'sslrootcert':b'x'*1024}}),b'fixture',{b'HOME':b'/home/fixture'},Files(),time.monotonic()+3)
            class InsecureFiles(Files):
                def pin(self,path):return SimpleNamespace(raw=b'insecure',before=SimpleNamespace(st_mode=0o100644,st_uid=os.geteuid()),check=lambda:None)
            with mock.patch.object(P,'SealedFile',side_effect=Snapshot) as copying:
                with self.assertRaises(P.Refusal) as error:P.ConnectionInput(FileOptionsLibrary({b'fixture':{b'passfile':b'/insecure',b'sslmode':b'disable'}}),b'fixture',{},InsecureFiles(),time.monotonic()+3)
                self.assertEqual(error.exception.error_class,'connection');copying.assert_not_called()
            for option in ({b'service':b''},{b'service':b'name'},{b'sslrootcert':b'system'},{b'sslkey':b'engine:key'},{b'sslcrldir':b'/directory'}):
                with self.subTest(option=option),self.assertRaises(P.Refusal) as error:P.ConnectionInput(FileOptionsLibrary({b'fixture':option}),b'fixture',{b'HOME':b'/home/fixture'},Files(),time.monotonic()+3)
                self.assertEqual(error.exception.error_class,'capability')
        for mode,uid,key,allowed in [(0o600,os.geteuid(),b'passfile',True),(0o640,os.geteuid(),b'passfile',False),(0o640,0,b'sslkey',True),(0o644,0,b'sslkey',False),(0o640,12345,b'sslkey',False)]:
            info=SimpleNamespace(st_mode=0o100000|mode,st_uid=uid)
            if allowed:P.original_file_permissions(info,key)
            else:
                with self.assertRaises(P.Refusal) as error:P.original_file_permissions(info,key)
                self.assertEqual(error.exception.error_class,'connection')

    def test_FileAbsence(self):
        self.require_file_api()
        from types import SimpleNamespace
        missing=FileNotFoundError(2,'private')
        class Files:
            def pin(self,path):raise missing
            def check(self):pass
        absent=b'/usr/share/rpgmigrate/.native-file-absent'
        with mock.patch.object(P,'stable_absence',return_value=absent) as proof:
            inp=P.ConnectionInput(FileOptionsLibrary({b'fixture':{b'passfile':b'/missing',b'sslmode':b'disable'}}),b'fixture',{},Files(),time.monotonic()+3)
            self.assertEqual(inp.overrides,{b'passfile':absent});self.assertEqual(inp.snapshots,[]);proof.assert_called();inp.close()
        empty=SimpleNamespace(raw=b'',before=SimpleNamespace(st_mode=0o100600,st_uid=os.geteuid()),check=lambda:None)
        with FakeMemfd(),mock.patch.object(P,'stable_absence') as proof:
            inp=P.ConnectionInput(FileOptionsLibrary({b'empty':{b'passfile':b'/empty',b'sslmode':b'disable'}}),b'empty',{},SimpleNamespace(pin=lambda path:empty,check=lambda:None),time.monotonic()+3)
            self.assertEqual(len(inp.snapshots),1);self.assertNotEqual(inp.overrides[b'passfile'],absent);proof.assert_not_called();inp.close()
        with mock.patch.object(P,'root_ancestors'),mock.patch.object(P.os,'statvfs',return_value=SimpleNamespace(f_flag=1)),mock.patch.object(P.os,'lstat',side_effect=missing),mock.patch.object(P.os,'ST_RDONLY',1,create=True):
            self.assertEqual(P.stable_absence(),absent)
        with mock.patch.object(P,'root_ancestors'),mock.patch.object(P.os,'statvfs',return_value=SimpleNamespace(f_flag=0)),mock.patch.object(P.os,'ST_RDONLY',1,create=True):
            with self.assertRaises(P.Refusal):P.stable_absence()
        with mock.patch.object(P,'root_ancestors'),mock.patch.object(P.os,'statvfs',return_value=SimpleNamespace(f_flag=1)),mock.patch.object(P.os,'lstat',return_value=object()),mock.patch.object(P.os,'ST_RDONLY',1,create=True):
            with self.assertRaises(P.Refusal):P.stable_absence()

    def test_FileSnapshots(self):
        self.require_file_api()
        with FakeMemfd() as kernel:
            snapshot=P.SealedFile(b'synthetic-private-bytes',time.monotonic()+3)
            self.assertEqual(snapshot.path,b'/proc/self/fd/'+str(snapshot.fd).encode());snapshot.check()
            second=P.os.dup(snapshot.fd)
            try:
                for fd in (snapshot.fd,second):
                    with self.assertRaises(PermissionError):P.os.write(fd,b'x')
                    with self.assertRaises(PermissionError):P.os.ftruncate(fd,1)
                    with self.assertRaises(PermissionError):P.os.ftruncate(fd,999)
            finally:P.os.close(second)
            snapshot.close();snapshot.close();self.assertEqual(kernel.live(),[])
        with FakeMemfd() as kernel:
            snapshot=P.SealedFile(b'private',time.monotonic()+3);fd=snapshot.fd
            kernel.handles[fd]['inode']+=1
            with self.assertRaises(P.Refusal):snapshot.check()
            with self.assertRaises(P.Refusal):snapshot.close()
            self.assertIn(fd,kernel.live(),'cleanup closed a foreign reused descriptor')
            kernel.close(fd)
        for failure in ('create','seal','proc'):
            with self.subTest(failure=failure),FakeMemfd(failure) as kernel:
                with self.assertRaises(P.Refusal) as error:P.SealedFile(b'private',time.monotonic()+3)
                self.assertEqual(error.exception.error_class,'capability');self.assertEqual(kernel.live(),[])
        with FakeMemfd() as kernel:
            with self.assertRaises(P.Refusal) as error:P.SealedFile(b'private',time.monotonic()-1)
            self.assertEqual(error.exception.error_class,'timeout');self.assertEqual(kernel.live(),[])

    def test_FileConnectionStart(self):
        self.require_file_api()
        from types import SimpleNamespace
        material=SimpleNamespace(pin=lambda path:None,check=lambda:None)
        lib=FileOptionsLibrary({b'fixture':{b'password':b'password',b'sslmode':b'disable'}})
        inp=P.ConnectionInput(lib,b'fixture',{},material,time.monotonic()+3)
        keys,values=inp.claim();self.assertEqual(list(keys),[b'dbname',None]);self.assertEqual(list(values),[b'fixture',None])
        with self.assertRaises(P.Refusal):inp.claim()
        inp.close()
        with self.assertRaises(P.Refusal):inp.claim()
        inp=P.ConnectionInput(lib,b'fixture',{},material,time.monotonic()+3)
        calls=[];lib.PQconnectStartParams=lambda keywords,values,expand:(calls.append((list(keywords),list(values),expand)) or None)
        with self.assertRaises(P.Refusal):P.Session(lib,inp,time.monotonic()+3,SimpleNamespace(check=lambda:None))
        self.assertTrue(inp.closed);self.assertEqual(calls,[([b'dbname',None],[b'fixture',None],1)])
        inp=P.ConnectionInput(lib,b'fixture',{},material,time.monotonic()+3);lib.PQconnectStartParams=lambda *args:0
        with self.assertRaises(P.Refusal) as error:P.Session(lib,inp,time.monotonic()+3,SimpleNamespace(check=lambda:None))
        self.assertEqual(error.exception.error_class,'connection');self.assertTrue(inp.closed)
        inp=P.ConnectionInput(lib,b'fixture',{},material,time.monotonic()+3)
        lib.PQconnectStartParams=lambda *args:1;lib.PQsetNoticeReceiver=lambda *args:None;lib.PQconnectPoll=lambda *args:3
        lib.PQstatus=lambda *args:0;lib.PQsetnonblocking=lambda *args:0;lib.PQtransactionStatus=lambda *args:0;lib.PQbackendPID=lambda *args:23;lib.PQfinish=lambda *args:None
        with mock.patch.object(P.Session,'connection_facts',return_value={}):
            first=P.Session(lib,inp,time.monotonic()+3,SimpleNamespace(check=lambda:None))
            try:
                with self.assertRaises(P.Refusal):P.Session(lib,inp,time.monotonic()+3,SimpleNamespace(check=lambda:None))
                self.assertFalse(inp.closed,'rejected second session closed the first session input')
            finally:first.close()

    def test_FileCleanupOrder(self):
        self.require_file_api()
        from types import SimpleNamespace
        events=[]
        class Input:
            overrides={}
            def check(self):pass
            def claim(self):events.append('claim');return None,None
            def close(self):events.append('snapshot-close')
        class Library:
            def PQconnectStartParams(self,*args):events.append('start');return 1
            def PQsetNoticeReceiver(self,*args):pass
            def PQconnectPoll(self,*args):return 3
            def PQstatus(self,*args):return 0
            def PQsetnonblocking(self,*args):return 0
            def PQtransactionStatus(self,*args):return 0
            def PQbackendPID(self,*args):return 23
            def PQfinish(self,*args):events.append('finish')
        with mock.patch.object(P.Session,'connection_facts',return_value={}):
            session=P.Session(Library(),Input(),time.monotonic()+3,SimpleNamespace(check=lambda:None));session.close()
        self.assertLess(events.index('finish'),events.index('snapshot-close'));self.assertEqual(events[:2],['claim','start'])


    def test_FileKernelOpen(self):
        self.require_file_api()
        from types import SimpleNamespace
        import errno,stat
        class Function:
            def __init__(self,call):self.call=call
            def __call__(self,*args):return self.call(*args)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);(path/'data').write_bytes(b'owned');(path/'link').symlink_to('data');(path/'sub').mkdir();os.mkfifo(path/'pipe',0o600)
            cwd=os.open(path,os.O_RDONLY|os.O_DIRECTORY);calls=[];state={'errno':None,'type':0x01021994}
            def syscall(number,fd,raw,how,size):
                actual=P.C.cast(how,P.C.POINTER(P.OpenHow)).contents
                calls.append((number,fd,raw,actual.flags,actual.resolve,size))
                if state['errno'] is not None:P.C.set_errno(state['errno']);return -1
                return os.open(raw,actual.flags,dir_fd=fd)
            def fstatfs(fd,output):P.C.cast(output,P.C.POINTER(P.LinuxStatFS)).contents.f_type=state['type'];return 0
            library=SimpleNamespace(syscall=Function(syscall),fstatfs=Function(fstatfs))
            try:
                with mock.patch.object(P,'_FILE_API',None),mock.patch.object(P.sys,'platform','linux'),mock.patch.object(P.os,'uname',return_value=SimpleNamespace(machine='aarch64')),mock.patch.object(P.C,'CDLL',return_value=library) as load:
                    for raw in (b'data',b'link',b'sub/../data'):
                        fd=P.open_connection_file(raw,cwd)
                        try:self.assertEqual(os.read(fd,5),b'owned')
                        finally:os.close(fd)
                    load.assert_called_once_with(None,use_errno=True)
                    self.assertEqual(P.C.sizeof(P.OpenHow),24);self.assertEqual(P.C.sizeof(P.LinuxStatFS),120)
                    for number,fd,raw,flags,resolve,size in calls:
                        self.assertEqual((number,fd,resolve,size),(437,cwd,2,24));self.assertFalse(flags&os.O_NOFOLLOW)
                    for code,want in [(errno.ENOSYS,'capability'),(errno.EPERM,'capability'),(errno.EINVAL,'capability'),(errno.ELOOP,'capability'),(errno.EACCES,'connection')]:
                        state['errno']=code
                        with self.assertRaises(P.Refusal) as error:P.open_connection_file(b'data',cwd)
                        self.assertEqual(error.exception.error_class,want)
                    for code in (errno.ENOENT,errno.ENOTDIR):
                        state['errno']=code
                        with self.assertRaises(OSError) as error:P.open_connection_file(b'data',cwd)
                        self.assertEqual(error.exception.errno,code)
                    state['errno']=None;state['type']=0x9fa0
                    with self.assertRaises(P.Refusal):P.open_connection_file(b'data',cwd)
                    state['type']=0x01021994
                    with self.assertRaises(P.Refusal):P.open_connection_file(b'pipe',cwd)
            finally:os.close(cwd)
        with mock.patch.object(P.sys,'platform','darwin'),mock.patch.object(P.C,'CDLL') as load:
            with self.assertRaises(P.Refusal):P.file_api()
            load.assert_not_called()

    def test_FileMaterialization(self):
        self.require_file_api()
        from types import SimpleNamespace
        raw_source=b'source';raw_target=b'target';events=[];created=[]
        material=P.Materialization.__new__(P.Materialization)
        material.inputs={};material.prepared=False;material.roles={'source':raw_source,'target':raw_target};material.envb={};material.check=lambda:None
        def prepare(library,raw,envb,owner,deadline):
            events.append(raw)
            if raw==raw_target:raise P.Refusal('capability')
            value=SimpleNamespace(closed=False)
            value.close=lambda:setattr(value,'closed',True);created.append(value);return value
        with mock.patch.object(P,'ConnectionInput',side_effect=prepare):
            with self.assertRaises(P.Refusal):material.prepare(None,time.monotonic()+3)
        self.assertEqual(events,[raw_source,raw_target]);self.assertTrue(created[0].closed);self.assertFalse(material.prepared)
        with self.assertRaises(P.Refusal):material.prepare(None,time.monotonic()+3)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);file=path/'file';file.write_bytes(b'original');file.chmod(0o600);(path/'projection').symlink_to('file')
            cwd=os.open('.',os.O_RDONLY|os.O_DIRECTORY)
            material=P.Materialization.__new__(P.Materialization);material.env=dict(os.environ);material.cwd=cwd;material.cwd_id=P.identity(os.fstat(cwd));material.paths={};material.absences=set();material.files=[];material.inputs={}
            def open_file(raw,fd):return os.open(raw,os.O_RDONLY|os.O_NONBLOCK,dir_fd=fd)
            try:
                with mock.patch.object(P,'open_connection_file',side_effect=open_file):
                    pinned=material.pin(os.fsencode(path/'projection'));self.assertEqual(pinned.raw,b'original');material.check()
                    file.write_bytes(b'changed')
                    with self.assertRaises(P.Refusal):material.check()
            finally:material.close()
        cwd=os.open('.',os.O_RDONLY|os.O_DIRECTORY)
        material=P.Materialization.__new__(P.Materialization);material.env=dict(os.environ);material.cwd=cwd;material.cwd_id=P.identity(os.fstat(cwd));material.files=[];material.absences=set()
        dead=SimpleNamespace(check=lambda:(_ for _ in ()).throw(AssertionError('closed source snapshot reopened')))
        live=mock.Mock()
        material.inputs={'source':SimpleNamespace(closed=True,absent=False,snapshots=[dead]),'target':SimpleNamespace(closed=False,absent=False,snapshots=[live])}
        try:material.check();live.check.assert_called_once()
        finally:os.close(cwd)


if __name__ == '__main__':
    unittest.main(verbosity=2)

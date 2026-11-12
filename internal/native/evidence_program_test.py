import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
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


def source_request(kind='source_head'):
    value=request();del value['saved_e'];value['kind']=kind
    value['program_sha256']=hashlib.sha256((HERE/'evidence_program.py').read_bytes()).hexdigest()
    execution=value['execution'];execution['role']='worker';execution['attempt']=1
    execution['pvc_uid']='11112222-3333-4444-aaaa-bbbbccccdddd';execution['work']['workdir_id']=execution['pvc_uid']
    execution['job_uid']=execution['worker_job_uid']='22223333-4444-5555-aaaa-bbbbccccdddd'
    value['container']['pod_uid']='33334444-5555-6666-aaaa-bbbbccccdddd'
    value['container']['container_id']='containerd://sha256:'+'7'*64
    value['pod_name']='worker-pod';return value


def retry_request(commit=True, local=False):
    value=source_request('target_retry');value['inputs']={'commit_test':commit,'expect_local':local}
    value['execution']['role']='retry_gate';value['execution']['job_uid']='44445555-6666-7777-aaaa-bbbbccccdddd'
    value['container']['pod_uid']='55556666-7777-8888-aaaa-bbbbccccdddd';value['pod_name']='retry-pod'
    return value


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



def candidate_fixture(info):
    req, manifest, endpos, row = private_fixture(info)
    manifest['format_version'] = 9
    target = manifest['target_provenance'][0]['record']
    target['intent']['binding']['metadata_version'] = 7
    target.update(large_object_table_oid=78, large_object_index_oid=79)
    plan = manifest['clone_state']['plan']
    plan.update(copy_policy=dict(use_copy_binary=True, split_tables_larger_than=536870912, split_max_parts=8, skip_large_objects=False), copy_tables=[])
    row[1] = '7'
    control = json.loads(row[22])
    control.update(prepared_generation=None, large_objects=dict(generation=1, initial=dict(objects=0, bytes=0), final_sync=dict(objects=0, bytes=0)))
    row[22] = json.dumps(control)
    return req, manifest, endpos, row


def response(value,raw):
    digest,execution=P.request_digests(raw)
    return dict(version=1,kind='completed_evidence',operation_id=value['operation_id'],request_sha256=digest,program_sha256=value['program_sha256'],execution_sha256=execution,work=value['execution']['work'],checkpoint=dict(checkpoint_id='1:0:7',certified_lsn='0/10',durable_replay_lsn='0/10',drain_end_lsn='0/20',drain_replay_lsn='0/30',drain_kind='later_begin',barrier_confirmed=True),resources=[dict(kind='workdir',name='/work/pgcopydb',owned=True,retain=True)],guards=P.GUARDS,source_closed=True,target_closed=True,private_closed=True,diagnostics_clean=True)


class FileOptionsLibrary:
    defaults={b'service':(None,b'PGSERVICE',None),b'password':(None,b'PGPASSWORD',None),b'passfile':(None,b'PGPASSFILE',None),b'sslmode':(None,b'PGSSLMODE',b'prefer'),b'sslrootcert':(None,b'PGSSLROOTCERT',None),b'sslcert':(None,b'PGSSLCERT',None),b'sslkey':(None,b'PGSSLKEY',None),b'sslcrl':(None,b'PGSSLCRL',None),b'sslcrldir':(None,b'PGSSLCRLDIR',None),b'dbname':(None,b'PGDATABASE',None),b'sslpassword':(None,None,None),b'host':(None,b'PGHOST',None)}
    defaults[b'sslkeylogfile']=(None,None,None)
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


class PreflightFixture:
    # These doubles exercise orchestration, not packaged runtime or database qualification.
    def __init__(self, case, follow='0', source=16, target=18, fault=None, rows=None):
        self.case=case;self.follow=follow;self.source=source;self.target=target
        self.fault=fault;self.rows=rows or {};self.events=[];self.queries=[];self.output=[];self.sessions=[]
        self.deadlines=[];self.clock=100.;self.closes=[]
    def event(self, name):
        self.events.append(name)
        if self.fault=='late-'+name:self.clock=161.
        if self.fault=='cleanup-last-'+name:self.clock=189.
        if self.fault=='cleanup-expire-'+name:self.clock=191.
        if self.fault==name:raise P.Refusal('closure' if name.endswith('close') else 'result','42501')
    def __enter__(self):
        from contextlib import ExitStack
        from types import SimpleNamespace
        self.stack=ExitStack();outer=self
        class Material:
            def __init__(self, env):self.files=[];outer.event('material-open')
            def prepare(self, library, deadline):outer.deadlines.append(deadline);outer.event('prepare')
            def connection(self, role):outer.event(role+'-input');return role
            def check(self):outer.event('material-check')
            def close(self):
                failure=None
                for file in self.files:
                    try:file.close()
                    except P.Refusal as error:failure=error
                outer.closes.append('material');outer.event('material-close')
                if failure:raise failure
        class Resolver:
            raw=b'nameserver 127.0.0.1\n'
            def __init__(self,*args):outer.event('resolver-open')
            def check(self):outer.event('resolver-check')
            def close(self):outer.closes.append('resolver');outer.event('resolver-close')
        class Diagnostics:
            def __init__(self):outer.event('diagnostics-open')
            def check(self):outer.event('diagnostics-check')
            def close(self):outer.closes.append('diagnostics');outer.event('diagnostics-close')
        class Session:
            def __init__(self, library, role, deadline, diag):
                outer.event(role+'-open');outer.deadlines.append(deadline);self.role=role;self.deadline=deadline
                self.backend=23 if role=='source' else 24;self.props={'PQuser':b'private-role','PQdb':b'private-db','ssl':1}
                self.state=0;outer.sessions.append(self)
            def check(self):outer.event(self.role+'-check');P.need(outer.clock<self.deadline,'timeout')
            def query(self,sql,params=(),tag='SELECT 1',state=0,rows=1,width=1,commit=False):
                self.check();outer.queries.append((self.role,sql,params,tag,state,rows,width,commit))
                outer.event('query-'+str(len(outer.queries)));outer.event(self.role+'-query')
                if outer.fault=='expire':outer.clock=161.
                outer.case.assertFalse(commit)
                if tag=='BEGIN':outer.case.assertEqual(self.state,0)
                else:outer.case.assertEqual(self.state,2 if tag=='ROLLBACK' else state)
                self.state=state
                if rows==0:return []
                if 'CURRENT_USER, SESSION_USER' in sql:return [[b'private-role',b'private-role',b'private-db',b't']]
                if "'server_version_num'" in sql:
                    key=self.role+'-identity';major=outer.source if self.role=='source' else outer.target
                    row=[str(self.backend).encode(),b'11',b'33',b'33',str(major*10000+6).encode(),b't',b'off']
                    count=sum(1 for role,query,*_ in outer.queries if role==self.role and "'server_version_num'" in query)
                    return copy.deepcopy(outer.rows.get(key+'-'+str(count),outer.rows.get(key,[row])))
                if 'jsonb_build_array' in sql:key='extensions-source';value=[[b'[["plpgsql",false],["example",true]]']]
                elif 'jsonb_agg(n.nspname' in sql:key='schemas';value=[[b'["public"]']]
                elif 'pg_available_extensions' in sql:key='extensions';value=[[b't']]
                elif "'TEMP'" in sql:key='temporary_privilege';value=[[b't']]
                elif 'has_schema_privilege' in sql:key='clone_rights';value=[[b't']]
                elif 'relrowsecurity' in sql:key='rls';value=[[b't']]
                elif "'wal_level'" in sql:key='wal';value=[[b't']]
                elif 'max_replication_slots' in sql:key='slots';value=[[b't']]
                elif 'rolreplication' in sql:key='replication';value=[[b't']]
                elif 'relpersistence' in sql and "'u'" in sql:key='unlogged';value=[[b't']]
                elif 'relreplident' in sql:key='replica_identity';value=[[b't']]
                elif 'relowner' in sql and 'has_database_privilege' in sql:key='publication';value=[[b't']]
                elif sql=='SELECT 1;':key='connections';value=[[b'1']]
                else:raise AssertionError('unexpected fixed query')
                return copy.deepcopy(outer.rows.get(key,value))
            def close(self,healthy=True):
                if healthy:outer.case.assertEqual(self.state,0)
                outer.closes.append(self.role);outer.event(self.role+'-close')
        env={'LANG':'C','LC_ALL':'C','PGCONNECT_TIMEOUT':'10','RPGMIGRATE_NATIVE_PREFLIGHT_FOLLOW':self.follow,
             'PGCOPYDB_SOURCE_PGURI':'postgresql://private-role:secret-sentinel@private-host/private-db',
             'PGCOPYDB_TARGET_PGURI':"host=private-host dbname=private-db password='secret-sentinel'"}
        self.stack.enter_context(mock.patch.dict(os.environ,env,clear=True))
        for name,value in [('Materialization',Material),('StableFile',Resolver),('Diagnostics',Diagnostics),('Session',Session)]:self.stack.enter_context(mock.patch.object(P,name,value))
        self.stack.enter_context(mock.patch.object(P,'load_runtime',side_effect=lambda:self.event('runtime')))
        self.stack.enter_context(mock.patch.object(P,'qualified_resolver_policy',side_effect=lambda resolver:self.event('resolver-policy')))
        self.stack.enter_context(mock.patch.object(P,'preflight_tools',side_effect=lambda major,deadline,diag:(self.event('tools') or (major,major))))
        self.stack.enter_context(mock.patch.object(P.os,'geteuid',return_value=65532))
        self.stack.enter_context(mock.patch.object(P.resource,'setrlimit'))
        self.stack.enter_context(mock.patch.object(P.os,'set_blocking'))
        self.stack.enter_context(mock.patch.object(P.time,'monotonic',side_effect=lambda:self.clock))
        def write(fd,raw):
            outer.case.assertEqual(fd,1)
            if b'"native_preflight_failure"' not in raw:
                for name in ('source','target','material','diagnostics'):outer.case.assertIn(name,outer.closes)
            outer.event('write');outer.output.append(bytes(raw));return len(raw)
        self.stack.enter_context(mock.patch.object(P.os,'write',side_effect=write))
        for name in ('InputChannel','PrivateState','observe','validate_request'):
            self.stack.enter_context(mock.patch.object(P,name,side_effect=AssertionError('evidence boundary entered')))
        return self
    def __exit__(self,*args):return self.stack.__exit__(*args)
    def run(self):return P.run_native_preflight('5'*32,hashlib.sha256((HERE/'evidence_program.py').read_bytes()).hexdigest())


class SourceFixture:
    # SQL and runtime doubles establish private-role shape, not live continuity or durability.
    def __init__(self,case,kind='source_head',rows=None,fault=None,major=16):
        self.case=case;self.kind=kind;self.rows=rows or {};self.fault=fault;self.events=[];self.queries=[];self.closes=[];self.output=[];self.clock=100.
        self.major=major;self.req=source_request(kind);self.raw=json.dumps(self.req,indent=1).encode();self.parts=[self.raw,b''];self.deadlines=[]
    def event(self,name):
        self.events.append(name)
        if self.fault=='late-'+name:self.clock=126.
        if self.fault=='overrun-'+name:self.clock=131.
        if self.fault==name:raise P.Refusal('result','57014')
    def __enter__(self):
        from contextlib import ExitStack
        self.stack=ExitStack();outer=self
        class State:
            def __init__(self,req):
                outer.event('private-open');outer.case.assertEqual(req,outer.req)
                self.binding=({'identity':{'system_identifier':123,'database_oid':11},'timeline':1,'major':outer.major},
                    {'intent':{'binding':{'slot_name':'native_slot'}},'consistent_point':8},
                    {'name':'native_publication','oid':88,'owner_oid':33},'fe1781120b07a00ef1128150a37c78e97584c53d585ee972367ff67a745f9b59' if outer.major==16 else 'd0ef0c78791e7cc83b77e0c95e7c4415a356ed61b404fa20fb90a2544ae9ae20')
            def revalidate(self):outer.event('private-check')
            def close(self,recheck=True):outer.closes.append('private');outer.event('private-close')
        class Material:
            def __init__(self,env,source_only=False):outer.case.assertIs(source_only,True);self.files=[];outer.event('material-open')
            def prepare(self,library,deadline):outer.deadlines.append(deadline);outer.event('prepare')
            def connection(self,role):outer.case.assertEqual(role,'source');outer.event('source-input');return role
            def check(self):outer.event('material-check')
            def close(self):
                failure=None
                for file in self.files:
                    try:file.close()
                    except P.Refusal as error:failure=error
                outer.closes.append('material');outer.event('material-close')
                if failure:raise failure
        class Resolver:
            raw=b'nameserver 127.0.0.1\n'
            def __init__(self,*args):outer.event('resolver-open')
            def check(self):outer.event('resolver-check')
            def close(self):outer.closes.append('resolver');outer.event('resolver-close')
        class Diagnostics:
            def __init__(self):outer.event('diagnostics-open')
            def check(self):outer.event('diagnostics-check')
            def close(self):outer.closes.append('diagnostics');outer.event('diagnostics-close')
        class Session:
            def __init__(self,library,role,deadline,diag):
                outer.case.assertEqual(role,'source');outer.event('source-open');outer.deadlines.append(deadline);self.deadline=deadline
                self.backend=23;self.props={'PQuser':b'private-user','PQdb':b'private-db','ssl':1}
            def check(self):outer.event('source-check');P.need(outer.clock<self.deadline,'timeout')
            def query(self,sql,params=(),tag='SELECT 1',state=0,rows=1,width=1,commit=False):
                self.check();outer.case.assertFalse(commit);outer.case.assertEqual(state,0)
                outer.queries.append((sql,params,tag,rows,width));outer.event('query-'+str(len(outer.queries)))
                if rows==0:outer.case.assertEqual(tag,'SET');return []
                if 'CURRENT_USER, SESSION_USER' in sql:key='role';value=[[b'private-user',b'private-user',b'private-db',b't']]
                elif 'pg_control_system' in sql:
                    count=sum(1 for q,*_ in outer.queries if 'pg_control_system' in q);key='identity-'+str(count)
                    value=[[b'123',b'11',b'1',str(outer.major*10000+6).encode(),b'23',b'33',b'33',b't',b'off']]
                elif 'pg_publication' in sql:
                    outer.case.assertEqual(params,('native_publication',));key='publication-'+str(sum(1 for q,*_ in outer.queries if 'pg_publication' in q));value=outer.rows.get('publication',[[b'88',b'33']])
                elif 'pg_replication_slots' in sql:
                    outer.case.assertEqual(params,('native_slot',));key='slot';value=[[b'pgoutput',b'logical',b'11',b'f',b't',b'1234',b'reserved',b'0/8',b'0/20',b'f',b'',b'false',b'false',b'0/30']]
                elif 'max_slot_wal_keep_size' in sql:key='retention';value=[[b'logical',b'-1',b'-1',None]] if outer.major==16 else [[b'logical',b'-1',None,b'0']]
                elif sql=='SELECT @pg_current_wal_insert_lsn();':key='head';value=[[b'0/30']]
                else:raise AssertionError('unexpected source-only SQL')
                return copy.deepcopy(outer.rows.get(key,value))
            def close(self,healthy=True):outer.closes.append('source');outer.event('source-close')
        work=self.req['execution']['work']
        env={'LANG':'C','LC_ALL':'C','PGCONNECT_TIMEOUT':'10','PGCOPYDB_SOURCE_PGURI':'opaque-source','PGCOPYDB_TARGET_PGURI':'target-secret-sentinel',
             'RPGMIGRATE_MIGRATION_UID':work['migration_uid'],'RPGMIGRATE_RUN_ID':work['run_id'],'RPGMIGRATE_WORKDIR_ID':work['workdir_id'],'RPGMIGRATE_CHECKPOINT_GENERATION':'1'}
        self.stack.enter_context(mock.patch.dict(os.environ,env,clear=True))
        for name,value in (('SourceState',State),('Materialization',Material),('StableFile',Resolver),('Diagnostics',Diagnostics),('Session',Session)):self.stack.enter_context(mock.patch.object(P,name,value))
        self.stack.enter_context(mock.patch.object(P,'load_runtime',side_effect=lambda:self.event('runtime')))
        self.stack.enter_context(mock.patch.object(P,'qualified_resolver_policy',side_effect=lambda resolver:self.event('resolver-policy')))
        self.stack.enter_context(mock.patch.object(P.os,'geteuid',return_value=65532));self.stack.enter_context(mock.patch.object(P.resource,'setrlimit'))
        self.stack.enter_context(mock.patch.object(P.time,'monotonic',side_effect=lambda:self.clock));self.stack.enter_context(mock.patch.object(P.os,'set_blocking'))
        def read(fd,cap):outer.case.assertEqual(fd,0);part=outer.parts.pop(0);outer.case.assertLessEqual(len(part),cap);return part
        def write(fd,raw):
            outer.case.assertEqual(fd,1)
            for name in ('source','material','private','diagnostics'):outer.case.assertIn(name,outer.closes)
            outer.output.append(bytes(raw));return len(raw)
        self.stack.enter_context(mock.patch.object(P.os,'read',side_effect=read));self.stack.enter_context(mock.patch.object(P.os,'write',side_effect=write))
        for name in ('InputChannel','PrivateState','observe','run_completed_evidence','run_native_preflight'):self.stack.enter_context(mock.patch.object(P,name,side_effect=AssertionError('non-source boundary entered')))
        return self
    def __exit__(self,*args):return self.stack.__exit__(*args)
    def run(self):return P.run_source_observation(self.kind,self.req['operation_id'],self.req['program_sha256'])


class RetryFixture:
    # Doubles verify fixed probe control flow and failure handling, not target durability.
    def __init__(self,case,commit=True,local=False,census=Ellipsis,fault=None,major=16):
        self.case=case;self.req=retry_request(commit,local);self.raw=json.dumps(self.req,indent=1).encode();self.fault=fault
        self.census=census if census is not Ellipsis else [b'0',b'0',b't',b'0',b'0',b'on',b'1760000200',b'pgm-retry-gate']
        self.major=major;self.events=[];self.queries=[];self.closes=[];self.docs=[];self.clock=100.;self.deadlines=[]
    def event(self,name):
        self.events.append(name)
        if self.fault=='late-'+name:self.clock=161.
        if self.fault=='overrun-'+name:self.clock=191.
        if self.fault==name:raise P.Refusal('closure' if name.endswith('close') else 'result','57014')
    def __enter__(self):
        from contextlib import ExitStack
        self.stack=ExitStack();outer=self
        class Channel:
            def __init__(self,op,deadline):outer.case.assertEqual(op,'5'*32);outer.deadlines.append(deadline);outer.event('channel-open')
            def route(self):outer.event('route')
            def consume(self):outer.event('consume');return outer.raw
            def finish(self,payload=None,deadline=None):
                if payload is not None:
                    for item in ('target','material','diagnostics'):outer.case.assertIn(item,outer.closes)
                    outer.case.assertEqual(deadline,190.);outer.docs.append(bytes(payload));outer.event('channel-close')
                else:outer.event('channel-empty')
        class Material:
            def __init__(self,env,source_only=False,target_only=False):
                outer.case.assertIs(source_only,False);outer.case.assertIs(target_only,True);self.files=[];outer.event('material-open')
            def prepare(self,library,deadline):outer.deadlines.append(deadline);outer.event('prepare')
            def connection(self,role):outer.case.assertEqual(role,'target');outer.event('target-input');return role
            def check(self):outer.event('material-check')
            def close(self):
                failed=False
                for file in self.files:
                    try:file.close()
                    except P.Refusal:failed=True
                outer.closes.append('material');outer.event('material-close');P.need(not failed,'closure')
        class Resolver:
            raw=b'nameserver 127.0.0.1\n'
            def __init__(self,*args):outer.event('resolver-open')
            def check(self):outer.event('resolver-check')
            def close(self):outer.closes.append('resolver');outer.event('resolver-close')
        class Diagnostics:
            def __init__(self):outer.event('diagnostics-open')
            def check(self):outer.event('diagnostics-check')
            def close(self):outer.closes.append('diagnostics');outer.event('diagnostics-close')
        class Session:
            def __init__(self,library,role,deadline,diag):
                outer.case.assertEqual(role,'target');outer.deadlines.append(deadline);outer.event('target-open');self.deadline=deadline
                self.backend=24;self.props={'PQuser':b'private-role','PQdb':b'private-db','ssl':1};self.state=0;self.commit_ack=False
            def check(self):outer.event('target-check');P.need(outer.clock<self.deadline,'timeout')
            def query(self,sql,params=(),tag='SELECT 1',state=0,rows=1,width=1,commit=False):
                self.check();outer.case.assertEqual(self.state,0 if tag=='BEGIN' else 2 if commit else state)
                outer.queries.append((sql,params,tag,state,rows,width,commit,self.deadline));outer.event('query-'+str(len(outer.queries)))
                if commit:
                    outer.case.assertEqual(sql,'COMMIT;');outer.case.assertEqual(self.deadline,105.)
                    if outer.fault=='unknown-commit':raise P.Refusal('unknown_commit','57014')
                    if outer.fault=='late-commit':outer.clock=106.
                    self.commit_ack=outer.fault!='missing-ack'
                self.state=state
                if rows==0:return []
                if 'CURRENT_USER, SESSION_USER' in sql:return [[b'private-role',b'private-role',b'private-db',b't']]
                if "'server_version_num'" in sql:
                    count=sum(1 for query,*_ in outer.queries if "'server_version_num'" in query)
                    return [[b'24',b'22',b'33',b'33',str((17 if outer.fault=='major17' else outer.major)*10000+6).encode(),b'f' if outer.fault=='standby' else b't',b'off' if not (outer.fault=='identity-change' and count==2) else b'on']]
                if '@pg_stat_activity' in sql:return copy.deepcopy(outer.census)
                if sql=="SELECT @current_setting('synchronous_commit'),@current_setting('application_name');":
                    return [[b'local' if outer.fault=='setting-change' else outer.census[0][5],b'foreign' if outer.fault=='app-change' else outer.census[0][7]]]
                raise AssertionError('unexpected retry SQL')
            def close(self,healthy=True):
                if healthy:outer.case.assertEqual(self.state,0)
                outer.closes.append('target');outer.event('target-close')
        if type(self.census) is list and self.census and type(self.census[0]) is bytes:self.census=[self.census]
        work=self.req['execution']['work']
        env={'LANG':'C','LC_ALL':'C','PGCONNECT_TIMEOUT':'10','PGAPPNAME':'pgm-retry-gate','PGCOPYDB_TARGET_PGURI':'target-opaque','PGCOPYDB_SOURCE_PGURI':'source-secret-sentinel',
             'RPGMIGRATE_MIGRATION_UID':work['migration_uid'],'RPGMIGRATE_RUN_ID':work['run_id'],'RPGMIGRATE_WORKDIR_ID':work['workdir_id'],'RPGMIGRATE_CHECKPOINT_GENERATION':'1'}
        self.stack.enter_context(mock.patch.dict(os.environ,env,clear=True))
        for name,value in (('InputChannel',Channel),('Materialization',Material),('StableFile',Resolver),('Diagnostics',Diagnostics),('Session',Session)):self.stack.enter_context(mock.patch.object(P,name,value))
        self.stack.enter_context(mock.patch.object(P,'load_runtime',side_effect=lambda:self.event('runtime')));self.stack.enter_context(mock.patch.object(P,'qualified_resolver_policy',side_effect=lambda resolver:self.event('resolver-policy')))
        self.stack.enter_context(mock.patch.object(P.os,'geteuid',return_value=65532));self.stack.enter_context(mock.patch.object(P.resource,'setrlimit'));self.stack.enter_context(mock.patch.object(P.time,'monotonic',side_effect=lambda:self.clock))
        for name in ('SourceState','PrivateState','observe','run_completed_evidence','run_source_observation','run_native_preflight'):self.stack.enter_context(mock.patch.object(P,name,side_effect=AssertionError('foreign role or authority path entered')))
        return self
    def __exit__(self,*args):return self.stack.__exit__(*args)
    def run(self):return P.run_target_retry(self.req['operation_id'],self.req['program_sha256'])


class NativePythonTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(P, 'reviewed evidence program is absent')

    def test_TargetRetryWire(self):
        self.assertIsNotNone(getattr(P,'run_target_retry',None),'fixed target retry role is absent')
        self.assertEqual(P.run_target_retry.__annotations__,{'operation_id':str,'program_sha256':str,'return':int})
        value=retry_request();raw=json.dumps(value).encode();sha=value['program_sha256']
        self.assertEqual(P.validate_target_retry_request(raw,'5'*32,sha),value)
        variants=[b'',b'\xff',raw+b' {}',raw[:-1]+b',"version":1}',b'['*33+b'0'+b']'*33,b' '*65537]
        for path,bad in ((('kind',),'completed_evidence'),(('version',),True),(('operation_id',),'A'*32),(('program_sha256',),'0'*64),(('execution','role'),'evidence'),(('execution','role'),'worker'),(('execution','job_uid'),value['execution']['worker_job_uid']),(('execution','attempt'),0),(('inputs','commit_test'),1),(('inputs','expect_local'),'false')):
            changed=copy.deepcopy(value);at=changed
            for key in path[:-1]:at=at[key]
            at[path[-1]]=bad;variants.append(json.dumps(changed).encode())
        for section in (None,'execution','container','inputs'):
            for field in (value if section is None else value[section]):
                changed=copy.deepcopy(value);at=changed if section is None else changed[section];del at[field];variants.append(json.dumps(changed).encode())
                changed=copy.deepcopy(value);at=changed if section is None else changed[section];at[field]=None;variants.append(json.dumps(changed).encode())
            changed=copy.deepcopy(value);at=changed if section is None else changed[section];at['extra']=True;variants.append(json.dumps(changed).encode())
        for bad in variants:
            with self.subTest(bad=variants.index(bad)),self.assertRaises(P.Refusal):P.validate_target_retry_request(bad,'5'*32,sha)
        with self.assertRaises(P.Refusal):P.validate_request(raw,'5'*32,sha)
        with self.assertRaises(P.Refusal):P.validate_source_request(raw,'source_head','5'*32,sha)
        with mock.patch.object(P,'run_target_retry',return_value=7) as retry:
            self.assertEqual(P.main(['program','target_retry','5'*32,sha]),7);retry.assert_called_once_with('5'*32,sha)

    def test_TargetRetryData(self):
        cases=[(True,False,[b'0',b'0',b't',b'0',b'0',b'on',b'1760000200',b'pgm-retry-gate'],True,True),
               (True,False,[b'2',b'1',b't',b'1760000000',b'0',b'on',b'1760000200',b'pgm-retry-gate'],False,True),
               (True,False,[b'0',b'0',b't',b'0',b'1',b'on',b'1760000200',b'pgm-retry-gate'],False,True),
               (True,False,[b'1',b'0',b'f',b'0',b'0',b'off',b'1760000200',b'pinned-app'],True,True),
               (True,True,[b'0',b'0',b't',b'0',b'0',b'local',b'1760000200',b'pgm-retry-gate'],True,True),
               (False,True,[b'0',b'0',b't',b'0',b'0',b'local',b'1760000200',b'pgm-retry-gate'],False,True),
               (False,True,[b'0',b'0',b't',b'0',b'0',b'on',b'1760000200',b'pgm-retry-gate'],False,False)]
        for commit,local,census,attempted,admitted in cases:
            with self.subTest(commit=commit,local=local,census=census),RetryFixture(self,commit,local,census) as fixture:
                self.assertEqual(fixture.run(),0);self.assertEqual(len(fixture.docs),1);raw=fixture.docs[0];value=json.loads(raw)
                self.assertEqual(set(value),set('version kind operation_id program_sha256 request_sha256 execution_sha256 work observed_at_unix census_known total syncrep gate_waiting oldest_syncrep_wait_unix application_attribution_known target_synchronous_commit synchronous_commit_admitted probe_attempted probe_committed target_closed materialization_closed diagnostics_clean'.split()))
                self.assertEqual(value['version'],1);self.assertEqual(value['kind'],'target_retry');self.assertEqual(value['operation_id'],'5'*32);self.assertEqual(value['program_sha256'],fixture.req['program_sha256']);self.assertEqual(value['work'],fixture.req['execution']['work'])
                digest=hashlib.sha256(fixture.raw).digest();self.assertEqual(value['request_sha256'],digest.hex());self.assertEqual(value['execution_sha256'],hashlib.sha256(b'rpgmigrate.native.execution.v1\0'+digest).hexdigest())
                self.assertIs(value['probe_attempted'],attempted);self.assertIs(value['probe_committed'],attempted);self.assertIs(value['synchronous_commit_admitted'],admitted)
                self.assertEqual([value[key] for key in ('total','syncrep','oldest_syncrep_wait_unix','gate_waiting','observed_at_unix')],[int(census[index]) for index in (0,1,3,4,6)])
                self.assertIs(value['application_attribution_known'],census[2]==b't');self.assertEqual(value['target_synchronous_commit'],census[5].decode())
                for key in ('census_known','target_closed','materialization_closed','diagnostics_clean'):self.assertIs(value[key],True)
                self.assertEqual(fixture.deadlines,[130.,160.,110.]);self.assertEqual(fixture.events.count('target-open'),1)
                mutations=[(sql,tag,state,commit_flag) for sql,_,tag,state,_,_,commit_flag,_ in fixture.queries if tag in ('BEGIN','CREATE TABLE','COMMIT') or sql.startswith('SET LOCAL')]
                self.assertEqual(mutations,[('BEGIN READ WRITE;','BEGIN',2,False),("SET LOCAL synchronous_commit TO 'on';",'SET',2,False),('CREATE TEMP TABLE pgm_commit_probe () ON COMMIT DROP;','CREATE TABLE',2,False),('COMMIT;','COMMIT',0,True)] if attempted else [])
                sql=next(sql for sql,*_ in fixture.queries if '@pg_stat_activity' in sql)
                for predicate in ("a.usename=SESSION_USER","a.pid<>@pg_backend_pid()","a.datname=@current_database()","a.application_name LIKE 'pgcopydb%' OR a.application_name LIKE 'rpgmigrate%'","@current_setting('application_name')<>'pgm-retry-gate'","g.application_name='pgm-retry-gate'","g.wait_event_type='IPC' AND g.wait_event='SyncRep'"):self.assertIn(predicate,sql)
                self.assertNotIn('client_addr',sql);self.assertNotIn('backend_type',sql)
                for forbidden in ('ready','checkpoint','saved_e','end_lsn','token','resources','owned','acknowledged','durable'):self.assertNotIn(forbidden,value)
                for sentinel in (b'source-secret-sentinel',b'private-role',b'private-db',b'pinned-app'):self.assertNotIn(sentinel,raw)
                self.assertTrue(raw.endswith(b'\n'));self.assertLessEqual(len(raw),65536)
        for setting in (b'on',b'off',b'local',b'remote_write',b'remote_apply'):
            census=[b'0',b'0',b't',b'0',b'0',setting,b'1760000200',b'pgm-retry-gate']
            with RetryFixture(self,census=census,major=18) as fixture:
                self.assertEqual(fixture.run(),0);value=json.loads(fixture.docs[0]);self.assertIs(value['probe_committed'],True);self.assertEqual(value['target_synchronous_commit'],setting.decode())

    def test_TargetRetryRefusals(self):
        with RetryFixture(self) as fixture:self.assertEqual(fixture.run(),0);queries=len(fixture.queries)
        faults=['query-'+str(i) for i in range(1,queries+1)]+['channel-open','route','consume','diagnostics-open','material-open','prepare','runtime','resolver-open','resolver-policy','target-open','target-check','resolver-check','material-check','diagnostics-check','target-close','resolver-close','material-close','diagnostics-close','unknown-commit','missing-ack','late-commit','major17','standby','identity-change','setting-change','app-change','late-query-'+str(queries),'overrun-target-close']
        for fault in faults:
            with self.subTest(fault=fault),RetryFixture(self,fault=fault) as fixture:self.assertEqual(fixture.run(),1);self.assertEqual(fixture.docs,[])
        with RetryFixture(self,fault='channel-close') as fixture:
            self.assertEqual(fixture.run(),1);self.assertEqual(len(fixture.docs),1)
            self.assertNotIn('ready',json.loads(fixture.docs[0]))
        with RetryFixture(self) as fixture,mock.patch.object(P,'qualified_resolver_policy',side_effect=P.Refusal('capability')):
            self.assertEqual(fixture.run(),1);self.assertEqual(fixture.docs,[]);self.assertNotIn('target-open',fixture.events)
        good=[b'0',b'0',b't',b'0',b'0',b'on',b'1760000200',b'pgm-retry-gate']
        bad_rows=[[],None,[[None]*8],[good,good],[good[:-1]],[good+[b'extra']]]
        for index,bad in ((0,b'-1'),(0,b'9223372036854775808'),(1,b'1'),(2,b'f'),(3,b'1'),(4,b'-1'),(5,b'unknown'),(6,b'0'),(7,None)):
            changed=good.copy();changed[index]=bad;bad_rows.append([changed])
        bad_rows.append([[b'1',b'1',b't',b'1760000300',b'0',b'on',b'1760000200',b'pgm-retry-gate']])
        for rows in bad_rows:
            with self.subTest(rows=rows),RetryFixture(self,census=rows) as fixture:
                self.assertEqual(fixture.run(),1);self.assertEqual(fixture.docs,[]);self.assertFalse(any(sql.startswith('BEGIN') for sql,*_ in fixture.queries))

    def test_TargetRetryMaterialization(self):
        with tempfile.TemporaryDirectory() as directory:
            uri=Path(directory)/'uri';password=Path(directory)/'pass';raw=b"host=target dbname='opaque' password='' passfile='/tmp/mixed' sslmode=disable\n"
            uri.write_bytes(raw);uri.chmod(0o600);mixed=b'target:5432:db:app:target-secret\nsource:5432:db:app:unrelated-source-secret\n';password.write_bytes(mixed);password.chmod(0o600)
            before={p:(p.read_bytes(),p.stat().st_mode,p.stat().st_ino) for p in (uri,password)};opened=[]
            def open_file(path,fd):
                opened.append(path)
                if path==b'/tmp/pgm-target-uri':return os.open(uri,os.O_RDONLY|os.O_NONBLOCK)
                if path==b'/tmp/mixed':return os.open(password,os.O_RDONLY|os.O_NONBLOCK)
                raise AssertionError('unselected source or credential file read')
            env={'PGCOPYDB_TARGET_PGURI':'env-target','PGCOPYDB_SOURCE_PGURI':'poisoned-source'}
            poisoned=dict(env,PGCOPYDB_SOURCE_PGURI=object(),PGCOPYDB_SOURCE_SUPER_PGURI=object())
            library=FileOptionsLibrary({raw:{b'password':b'',b'passfile':b'/tmp/mixed',b'sslmode':b'disable'}})
            with mock.patch.dict(os.environ,env,clear=True),mock.patch.object(P,'open_connection_file',side_effect=open_file),mock.patch.object(P.os,'walk',side_effect=AssertionError('credential tree scan')),FakeMemfd() as kernel:
                material=P.Materialization(poisoned,target_only=True)
                try:
                    self.assertEqual(material.roles,{'target':raw});material.prepare(library,time.monotonic()+3)
                    self.assertEqual(set(material.inputs),{'target'});self.assertEqual(material.connection('target').raw,raw);self.assertEqual(material.connection('target').snapshots[0].raw,mixed)
                    with self.assertRaises(P.Refusal):material.connection('source')
                    self.assertFalse(any(key.startswith(b'PGCOPYDB_SOURCE_') for key in material.envb));os.environ['PGCOPYDB_SOURCE_PGURI']='different-poison';material.check()
                finally:material.close()
                self.assertEqual(kernel.live(),[])
            self.assertEqual({p:(p.read_bytes(),p.stat().st_mode,p.stat().st_ino) for p in (uri,password)},before)
            self.assertEqual(set(opened),{b'/tmp/pgm-target-uri',b'/tmp/mixed'})
            with self.assertRaises(P.Refusal):P.Materialization(env,source_only=True,target_only=True)
            def missing(path,fd):self.assertEqual(path,b'/tmp/pgm-target-uri');raise FileNotFoundError(2,'not provisioned')
            with mock.patch.dict(os.environ,env,clear=True),mock.patch.object(P,'open_connection_file',side_effect=missing):
                material=P.Materialization(env,target_only=True)
                try:self.assertEqual(material.roles,{'target':b'env-target'});material.check()
                finally:material.close()

    def test_SourceObservationWire(self):
        self.assertIsNotNone(getattr(P,'run_source_observation',None),'fixed source observation role is absent')
        self.assertEqual(P.run_source_observation.__annotations__,{'kind':str,'operation_id':str,'program_sha256':str,'return':int})
        for kind in ('source_head','active_lag'):
            value=source_request(kind);raw=json.dumps(value).encode();sha=value['program_sha256']
            self.assertEqual(P.validate_source_request(raw,kind,'5'*32,sha),value)
            variants=[b'',b'\xff',raw+b'{}',b'\xef\xbb\xbf'+raw,raw[:-1]+b',"version":1}',b'['*33+b'0'+b']'*33,b' '*65537]
            for path,bad in ((('version',),True),(('kind',),'completed_evidence'),(('kind',),'SQL'),(('operation_id',),'A'*32),(('program_sha256',),'0'*64),(('execution','attempt'),0),(('execution','attempt'),True),(('execution','job_uid'),'other'),(('execution','role'),'evidence'),(('execution','template_sha256'),'0'*64),(('execution','connection_reference_sha256'),'0'*64),(('container','pod_uid'),'0'*36),(('container','restart_count'),1),(('container','restart_count'),False),(('container','template_sha256'),'4'*64),(('container','image_id'),'other')):
                v=copy.deepcopy(value);target=v
                for key in path[:-1]:target=target[key]
                target[path[-1]]=bad;variants.append(json.dumps(v).encode())
            for field in value:
                v=copy.deepcopy(value);del v[field];variants.append(json.dumps(v).encode())
                v=copy.deepcopy(value);v[field]=None;variants.append(json.dumps(v).encode())
            for section in ('execution','container'):
                for field in value[section]:
                    v=copy.deepcopy(value);del v[section][field];variants.append(json.dumps(v).encode())
                    v=copy.deepcopy(value);v[section][field]=None;variants.append(json.dumps(v).encode())
            for bad in variants:
                with self.subTest(kind=kind,bad=variants.index(bad)),self.assertRaises(P.Refusal):P.validate_source_request(bad,kind,'5'*32,sha)
        with mock.patch.object(P,'run_source_observation') as source:
            self.assertEqual(P.main(['program','source_head','5'*32,'6'*64]),1);source.assert_not_called()

    def test_SourceObservationState(self):
        from contextlib import ExitStack
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'work';root.mkdir(mode=0o700);owned=root/'pgcopydb';owned.mkdir(mode=0o700)
            req=source_request();_,manifest,_,_=private_fixture(owned.stat());manifest['operator']['workdir_id']=req['execution']['work']['workdir_id'];manifest['clone_state']['phase']='copying'
            file=owned/'manifest.json';file.write_text(json.dumps(manifest));file.chmod(0o600)
            control=owned/'control';control.mkdir(mode=0o700);(control/'endpos.json').write_bytes(b'not E');(owned/'data').mkdir(mode=0o700)
            actual_open=P.fdopen;actual_stat=os.stat;events=[]
            def opened(path,directory=False,parent=None):events.append(path);return actual_open(str(root) if path=='/work' else path,directory,parent)
            def stated(path,*args,**kwargs):return actual_stat(str(root) if path=='/work' else path,*args,**kwargs)
            env={key:str(req['execution']['work'][field]) for key,field in (('RPGMIGRATE_MIGRATION_UID','migration_uid'),('RPGMIGRATE_RUN_ID','run_id'),('RPGMIGRATE_WORKDIR_ID','workdir_id'),('RPGMIGRATE_CHECKPOINT_GENERATION','checkpoint_generation'))}
            with ExitStack() as stack:
                stack.enter_context(mock.patch.dict(os.environ,env,clear=True));stack.enter_context(mock.patch.object(P,'fdopen',side_effect=opened));stack.enter_context(mock.patch.object(P.os,'stat',side_effect=stated))
                stack.enter_context(mock.patch.object(P.fcntl,'flock',side_effect=AssertionError('lifetime or control lock acquired')))
                state=P.SourceState(req);self.assertEqual(state.binding[3],'fe1781120b07a00ef1128150a37c78e97584c53d585ee972367ff67a745f9b59');state.revalidate()
                self.assertNotIn('control',events);self.assertNotIn('endpos.json',events);self.assertNotIn('lock',events)
                self.assertEqual((control/'endpos.json').read_bytes(),b'not E');self.assertTrue((owned/'data').is_dir())
                replacement=owned/'new';replacement.write_bytes(file.read_bytes());replacement.chmod(0o600);os.replace(replacement,file)
                with self.assertRaises(P.Refusal):state.revalidate()
                state.close(False)
                state=P.SourceState(req);os.environ['RPGMIGRATE_WORKDIR_ID']='foreign'
                with self.assertRaises(P.Refusal):state.revalidate()
                os.environ['RPGMIGRATE_WORKDIR_ID']=req['execution']['work']['workdir_id'];state.close(False)
                state=P.SourceState(req);previous=root/'previous';owned.rename(previous);owned.mkdir(mode=0o700)
                with self.assertRaises(P.Refusal):state.revalidate()
                state.close(False);owned.rmdir();previous.rename(owned)
            same=copy.deepcopy(manifest);same['target_provenance'][0]['record']['intent']['binding']['target_system_identifier']=999
            self.assertEqual(P.validate_source_manifest(same,req,owned.stat())[3],'fe1781120b07a00ef1128150a37c78e97584c53d585ee972367ff67a745f9b59')
            newer=json.loads(json.dumps(manifest));newer['clone_state']['plan']['source']['major']=18;newer['clone_state']['ticket']['source']['major']=18
            self.assertEqual(P.validate_source_manifest(newer,req,owned.stat())[3],'d0ef0c78791e7cc83b77e0c95e7c4415a356ed61b404fa20fb90a2544ae9ae20')
            for path,bad in ((('format_version',),7),(('operator','workdir_id'),'other'),(('source_slot','phase'),'provisioning'),(('source_slot','record','intent','nonce'),[0]*16),(('source_slot','record','intent','binding','catalog_epoch'),2),(('clone_state','ticket','token'),'other'),(('clone_state','plan','generation'),2),(('follow_state','restart'),{}),(('follow_state','cleanup'),{}),(('follow_state','binding','publication','oid'),999)):
                changed=json.loads(json.dumps(manifest));at=changed
                for key in path[:-1]:at=at[key]
                at[path[-1]]=bad
                with self.subTest(path=path),self.assertRaises(P.Refusal):P.validate_source_manifest(changed,req,owned.stat())

    def test_SourceObservationMaterialization(self):
        with tempfile.TemporaryDirectory() as directory:
            uri=Path(directory)/'uri';mixed=Path(directory)/'pass';raw=b"host=source dbname='opaque' password='' passfile='/tmp/mixed' sslmode=disable\n"
            uri.write_bytes(raw);uri.chmod(0o600);mixed_bytes=b'source:5432:db:app:source-secret\ntarget:5432:db:app:unrelated-secret\n';mixed.write_bytes(mixed_bytes);mixed.chmod(0o600)
            before={path:(path.read_bytes(),path.stat().st_mode,path.stat().st_ino) for path in (uri,mixed)};opened=[]
            def open_file(path,fd):
                opened.append(path)
                if path==b'/tmp/pgm-source-uri':return os.open(uri,os.O_RDONLY|os.O_NONBLOCK)
                if path==b'/tmp/mixed':return os.open(mixed,os.O_RDONLY|os.O_NONBLOCK)
                raise AssertionError('unselected credential consumed')
            env={'PGCOPYDB_SOURCE_PGURI':'env-source','PGCOPYDB_TARGET_PGURI':'poisoned-target'}
            poisoned=dict(env,PGCOPYDB_TARGET_PGURI=object(),PGCOPYDB_TARGET_SUPER_PGURI=object())
            library=FileOptionsLibrary({raw:{b'password':b'',b'passfile':b'/tmp/mixed',b'sslmode':b'disable'}})
            with mock.patch.dict(os.environ,env,clear=True),mock.patch.object(P,'open_connection_file',side_effect=open_file),mock.patch.object(P.os,'walk',side_effect=AssertionError('credential tree scanned')),FakeMemfd() as kernel:
                material=P.Materialization(poisoned,source_only=True)
                try:
                    self.assertEqual(material.roles,{'source':raw});material.prepare(library,time.monotonic()+3)
                    self.assertEqual(set(material.inputs),{'source'});self.assertEqual(material.connection('source').raw,raw)
                    self.assertEqual(material.connection('source').snapshots[0].raw,mixed_bytes)
                    with self.assertRaises(P.Refusal):material.connection('target')
                    self.assertFalse(any(key.startswith(b'PGCOPYDB_TARGET_') for key in material.envb));material.check()
                    os.environ['PGCOPYDB_TARGET_PGURI']='different-poisoned-target';material.check()
                finally:material.close()
                self.assertEqual(kernel.live(),[])
            self.assertEqual({path:(path.read_bytes(),path.stat().st_mode,path.stat().st_ino) for path in (uri,mixed)},before)
            self.assertEqual(set(opened),{b'/tmp/pgm-source-uri',b'/tmp/mixed'})
            def missing(path,fd):self.assertEqual(path,b'/tmp/pgm-source-uri');raise FileNotFoundError(2,'not provisioned')
            with mock.patch.dict(os.environ,env,clear=True),mock.patch.object(P,'open_connection_file',side_effect=missing):
                material=P.Materialization(env,source_only=True)
                try:self.assertEqual(material.roles,{'source':b'env-source'});material.check()
                finally:material.close()
            service=Path(directory)/'service';service.write_bytes(b'[source]\npassword=source-secret\n[target]\npassword=unrelated-secret\n');service.chmod(0o600)
            service_before=(service.read_bytes(),service.stat().st_ino,service.stat().st_mode);env['PGSERVICEFILE']=str(service)
            library=FileOptionsLibrary({b'env-source':{b'service':b'source',b'sslmode':b'disable'}})
            with mock.patch.dict(os.environ,env,clear=True),mock.patch.object(P,'open_connection_file',side_effect=missing):
                material=P.Materialization(env,source_only=True)
                try:
                    with self.assertRaises(P.Refusal):material.prepare(library,time.monotonic()+3)
                finally:material.close()
            self.assertEqual((service.read_bytes(),service.stat().st_ino,service.stat().st_mode),service_before)

    def test_SourceObservationClosedResult(self):
        for kind in ('source_head','active_lag'):
            with self.subTest(kind=kind),SourceFixture(self,kind) as fixture:
                self.assertEqual(fixture.run(),0);raw=b''.join(fixture.output);result=json.loads(raw)
                self.assertEqual(set(result),set('version kind operation_id program_sha256 request_sha256 execution_sha256 work source_binding_sha256 source_closed private_closed diagnostics_clean'.split())|{'head_lsn' if kind=='source_head' else 'lag_bytes'})
                self.assertEqual(result['kind'],kind);self.assertEqual(result['version'],1);self.assertEqual(result['operation_id'],'5'*32)
                self.assertEqual(result['program_sha256'],fixture.req['program_sha256']);self.assertEqual(result['work'],fixture.req['execution']['work'])
                digest=hashlib.sha256(fixture.raw).digest();self.assertEqual(result['request_sha256'],digest.hex());self.assertEqual(result['execution_sha256'],hashlib.sha256(b'rpgmigrate.native.execution.v1\0'+digest).hexdigest())
                self.assertEqual(result['source_binding_sha256'],'fe1781120b07a00ef1128150a37c78e97584c53d585ee972367ff67a745f9b59')
                for field in ('source_closed','private_closed','diagnostics_clean'):self.assertIs(result[field],True)
                self.assertEqual(fixture.events.count('source-open'),1);self.assertEqual(fixture.deadlines,[125.,110.]);self.assertEqual(fixture.parts,[])
                self.assertEqual(sum(1 for sql,*_ in fixture.queries if 'pg_control_system' in sql),2)
                self.assertLessEqual(len(raw),65536);self.assertTrue(raw.endswith(b'\n'))
                for field in ('checkpoint','end_lsn','saved_e','certified_lsn','durable_replay_lsn','drained','resources','owned','target_closed'):self.assertNotIn(field,result)
                for sentinel in (b'target-secret-sentinel',b'private-user',b'private-db',b'native_slot',b'native_publication'):self.assertNotIn(sentinel,raw)
                if kind=='source_head':self.assertEqual(result['head_lsn'],'0/30')
                else:self.assertEqual(result['lag_bytes'],16)
        zero=[[b'pgoutput',b'logical',b'11',b'f',b't',b'1234',b'extended',b'0/8',b'0/30',b'f',b'',b'false',b'false',b'0/30']]
        with SourceFixture(self,'active_lag',{'slot':zero}) as fixture:self.assertEqual(fixture.run(),0);self.assertEqual(json.loads(b''.join(fixture.output))['lag_bytes'],0)
        for kind in ('source_head','active_lag'):
            with SourceFixture(self,kind,major=18) as fixture:
                self.assertEqual(fixture.run(),0);result=json.loads(b''.join(fixture.output))
                self.assertEqual(result['source_binding_sha256'],'d0ef0c78791e7cc83b77e0c95e7c4415a356ed61b404fa20fb90a2544ae9ae20')
                self.assertEqual(result['head_lsn' if kind=='source_head' else 'lag_bytes'],'0/30' if kind=='source_head' else 16)

    def test_SourceObservationRefusals(self):
        for kind in ('source_head','active_lag'):
            with SourceFixture(self,kind) as fixture:self.assertEqual(fixture.run(),0);count=len(fixture.queries)
            faults=['query-'+str(i) for i in range(1,count+1)]+['private-open','private-check','material-open','prepare','source-open','source-check','resolver-open','runtime','resolver-check','material-check','diagnostics-check','resolver-policy','source-close','material-close','private-close','resolver-close','diagnostics-close','late-query-'+str(count),'overrun-source-close']
            for fault in faults:
                with self.subTest(kind=kind,fault=fault),SourceFixture(self,kind,fault=fault) as fixture:
                    self.assertEqual(fixture.run(),1);self.assertEqual(fixture.output,[])
        for key in ('head','identity-1','identity-2','publication','role','retention','slot'):
            for bad in ([],[[None]],None,[[b'0/0']],[[b'0/30'],[b'0/30']]):
                with self.subTest(key=key,bad=bad),SourceFixture(self,'active_lag' if key in ('slot','retention') else 'source_head',{key:bad}) as fixture:self.assertEqual(fixture.run(),1);self.assertEqual(fixture.output,[])
        slot=[b'pgoutput',b'logical',b'11',b'f',b't',b'1234',b'reserved',b'0/8',b'0/20',b'f',b'',b'false',b'false',b'0/30']
        for index,bad in ((0,b'wal2json'),(1,b'physical'),(2,b'12'),(3,b't'),(4,b'f'),(5,None),(5,b'0'),(6,b'lost'),(7,b'0/21'),(7,None),(7,b'0/0'),(8,b'0/7'),(8,b'0/31'),(8,None),(8,b'0/0'),(9,b't'),(10,b'wal_removed'),(11,b'true'),(12,b'true'),(13,b'80000000/20'),(13,None),(13,b'0/0')):
            changed=slot.copy();changed[index]=bad
            with self.subTest(slot=index,bad=bad),SourceFixture(self,'active_lag',{'slot':[changed]}) as fixture:self.assertEqual(fixture.run(),1);self.assertEqual(fixture.output,[])
        identity=[b'123',b'11',b'1',b'160006',b'23',b'33',b'33',b't',b'off']
        for index in range(9):
            changed=identity.copy();changed[index]=b'f' if index==7 else b'99'
            with self.subTest(identity=index),SourceFixture(self,rows={'identity-2':[changed]}) as fixture:self.assertEqual(fixture.run(),1);self.assertEqual(fixture.output,[])
        with SourceFixture(self) as fixture,mock.patch.object(P,'qualified_resolver_policy',side_effect=P.Refusal('capability')):
            self.assertEqual(fixture.run(),1);self.assertEqual(fixture.output,[]);self.assertNotIn('source-open',fixture.events)
        with SourceFixture(self,rows={'publication-2':[[b'89',b'33']]}) as fixture:
            self.assertEqual(fixture.run(),1);self.assertEqual(fixture.output,[])
        for major,row in ((16,[b'logical',b'-1',None,None]),(16,[b'logical',b'0',b'-1',None]),(18,[b'logical',b'-1',None,None]),(18,[b'logical',b'-1',None,b'60']),(18,[b'replica',b'-1',None,b'0'])):
            with SourceFixture(self,'active_lag',rows={'retention':[row]},major=major) as fixture:self.assertEqual(fixture.run(),1);self.assertEqual(fixture.output,[])
        with SourceFixture(self,rows={'head':[[b'0/7']]}) as fixture:self.assertEqual(fixture.run(),1);self.assertEqual(fixture.output,[])

    def test_SourceObservationIO(self):
        for mode in ('empty','truncated','extra','overflow','read-failure','read-stall','bad-work'):
            with self.subTest(mode=mode),SourceFixture(self) as fixture:
                if mode=='empty':fixture.parts=[b'']
                elif mode=='truncated':fixture.parts=[fixture.raw[:-5],b'']
                elif mode=='extra':fixture.parts=[fixture.raw,b'{}',b'']
                elif mode=='overflow':fixture.parts=[b' '*8192]*8+[b' ',b'']
                elif mode=='bad-work':os.environ['RPGMIGRATE_WORKDIR_ID']='foreign'
                if mode=='read-failure':
                    with mock.patch.object(P.os,'read',side_effect=OSError('secret-sentinel')):self.assertEqual(fixture.run(),1)
                elif mode=='read-stall':
                    def stalled(*args):fixture.clock=126.;return False
                    with mock.patch.object(P.os,'read',side_effect=BlockingIOError()),mock.patch.object(P,'wait_fd',side_effect=stalled):self.assertEqual(fixture.run(),1)
                else:self.assertEqual(fixture.run(),1)
                self.assertEqual(fixture.output,[]);self.assertNotIn('source-open',fixture.events)
        with SourceFixture(self) as fixture:
            first=True
            def reading(fd,cap):
                nonlocal first
                if first:first=False;raise BlockingIOError()
                return fixture.parts.pop(0)
            with mock.patch.object(P.os,'read',side_effect=reading),mock.patch.object(P,'wait_fd',return_value=True) as wait:
                self.assertEqual(fixture.run(),0);self.assertEqual(wait.call_count,1);self.assertEqual(wait.call_args.args[:3],(0,False,125.))
        with SourceFixture(self) as fixture:
            first=True
            def writing(fd,raw):
                nonlocal first
                if first:first=False;raise BlockingIOError()
                fixture.output.append(bytes(raw[:7]));return min(7,len(raw))
            with mock.patch.object(P.os,'write',side_effect=writing),mock.patch.object(P,'wait_fd',return_value=True) as wait:
                self.assertEqual(fixture.run(),0);self.assertEqual(json.loads(b''.join(fixture.output))['kind'],'source_head');wait.assert_called_once_with(1,True,130.)
        with SourceFixture(self) as fixture:
            def stopped(*args):fixture.clock=131.;return False
            with mock.patch.object(P.os,'write',side_effect=BlockingIOError()),mock.patch.object(P,'wait_fd',side_effect=stopped):self.assertEqual(fixture.run(),1)
            self.assertEqual(fixture.output,[])

    def test_NativePreflightDispatch(self):
        self.assertIsNotNone(getattr(P,'run_native_preflight',None),'fixed native preflight role is absent')
        self.assertEqual(getattr(P,'run_native_preflight',None).__annotations__,{'operation_id':str,'program_sha256':str,'return':int})
        with mock.patch.object(P,'run_native_preflight',return_value=7) as preflight,mock.patch.object(P,'run_completed_evidence',return_value=8) as evidence:
            self.assertEqual(P.main(['program','native_preflight','5'*32,'6'*64]),7)
            self.assertEqual(P.main(['program','completed_evidence','5'*32,'6'*64]),8)
            for argv in (['program'],['program','native_preflight','5'*32,'6'*64,'SQL'],['program','other','5'*32,'6'*64],['program','native_preflight','A'*32,'6'*64],['program','native_preflight','5'*32,'G'*64]):self.assertEqual(P.main(argv),1)
            preflight.assert_called_once_with('5'*32,'6'*64);evidence.assert_called_once_with('5'*32,'6'*64)
        for follow in ('','true','01','2','0\n'):
            with PreflightFixture(self,follow=follow) as fixture:
                self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture);self.assertNotIn('source-open',fixture.events)

    def test_NativePreflightClosedResult(self):
        clone='connections identity source_primary target_writable versions tools temporary_privilege extensions clone_rights rls'.split()
        for source,target,follow in ((16,16,'0'),(18,18,'0'),(16,18,'0'),(16,18,'1')):
            with self.subTest(source=source,target=target,follow=follow),PreflightFixture(self,follow,source,target) as fixture:
                self.assertEqual(fixture.run(),0)
                raw=b''.join(fixture.output);result=json.loads(raw)
                self.assertEqual(set(result),set('version kind operation_id program_sha256 source_major target_major pg_dump_major pg_restore_major checks source_closed target_closed materialization_closed diagnostics_clean'.split()))
                self.assertEqual(result['version'],1);self.assertEqual(result['kind'],'native_preflight');self.assertEqual(result['operation_id'],'5'*32)
                self.assertEqual(result['program_sha256'],hashlib.sha256((HERE/'evidence_program.py').read_bytes()).hexdigest())
                self.assertEqual((result['source_major'],result['target_major'],result['pg_dump_major'],result['pg_restore_major']),(source,target,target,target))
                self.assertEqual(result['checks'],clone+('wal publication slots replication replica_role unlogged replica_identity'.split() if follow=='1' else []))
                for key in ('source_closed','target_closed','materialization_closed','diagnostics_clean'):self.assertIs(result[key],True)
                for role in ('source','target'):
                    self.assertEqual(sum(1 for r,sql,*_ in fixture.queries if r==role and "'server_version_num'" in sql),2)
                    self.assertEqual(sum(1 for r,sql,*_ in fixture.queries if r==role and 'CURRENT_USER, SESSION_USER' in sql),2)
                    self.assertEqual(fixture.events.count(role+'-open'),1)
                self.assertEqual(fixture.deadlines,[160.,110.,110.]);self.assertLessEqual(len(raw),65536);self.assertTrue(raw.endswith(b'\n'))
                for canary in (b'private-role',b'private-db',b'private-host',b'secret-sentinel'):self.assertNotIn(canary,raw)
                predicates={
                    'pg_available_extensions':('installed.extname IS NOT NULL OR available.default_version IS NOT NULL',"@pg_has_role(CURRENT_USER,installed.extowner,'USAGE')",'SELECT rolsuper'),
                    'has_schema_privilege':("@has_database_privilege(CURRENT_USER,@current_database(),'CREATE')","@pg_has_role(CURRENT_USER,(SELECT datdba", "@has_schema_privilege(CURRENT_USER,n.oid,'CREATE')"),
                    'relrowsecurity':("c.relkind='r' AND c.relrowsecurity","d.deptype='e'","c.relforcerowsecurity OR NOT @pg_has_role(CURRENT_USER,c.relowner,'USAGE')",'SELECT rolsuper OR rolbypassrls'),
                }
                if follow=='1':predicates.update({
                    "'wal_level'":("='logical'","name='wal_sender_timeout'",'setting::bigint=0 OR setting::bigint>=60000'),
                    'max_replication_slots':('SELECT @count(*) FROM @pg_replication_slots',')>=1'),
                    'rolreplication':('rolreplication OR rolsuper',),
                    "c.relpersistence='u'":("c.relkind IN ('r','p')",'SELECT NOT EXISTS'),
                    'relreplident':("c.relreplident='n'", "c.relreplident='d' AND NOT EXISTS",'i.indrelid=c.oid AND i.indisprimary'),
                })
                for selector,required in predicates.items():
                    queries=[sql for _,sql,*_ in fixture.queries if selector in sql];self.assertEqual(len(queries),1)
                    for predicate in required:self.assertIn(predicate,queries[0])
                for role,sql,params,*_ in fixture.queries:
                    self.assertIn(sql.split()[0].rstrip(';'),('SELECT','SET','BEGIN','ROLLBACK'))
                    self.assertNotIn('replication_origin',sql);self.assertNotIn('wal2json',sql);self.assertNotIn('pgcopydb',sql)
                    self.assertNotIn('secret-sentinel',repr(params))
                    if 'pg_available_extensions' in sql:self.assertEqual([json.loads(value) for value in params],[['plpgsql','example'],['example']])
                    if 'has_schema_privilege' in sql:self.assertEqual([json.loads(value) for value in params],[['public']])

    def test_NativePreflightRefusals(self):
        with PreflightFixture(self,follow='1') as fixture:self.assertEqual(fixture.run(),0);count=len(fixture.queries)
        faults=['query-'+str(i) for i in range(1,count+1)]+['runtime','prepare','source-open','target-open','material-check','source-check','target-check','resolver-policy','resolver-check','diagnostics-check','source-close','target-close','resolver-close','material-close','diagnostics-close','tools','expire','late-query-'+str(count),'cleanup-expire-source-close']
        for fault in faults:
            with self.subTest(fault=fault),PreflightFixture(self,follow='1',fault=fault) as fixture:
                self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture)
                if 'material-open' in fixture.events:self.assertIn('material-close',fixture.events)
        for key in ('connections','temporary_privilege','extensions','clone_rights','rls','wal','publication','slots','replication','unlogged','replica_identity'):
            for bad in ([],[[None]],[[b'f']],[[b't',b't']],[[b't'],[b't']]):
                with self.subTest(key=key,bad=bad),PreflightFixture(self,follow='1',rows={key:bad}) as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture)
        identity=[b'23',b'11',b'33',b'33',b'160006',b't',b'off']
        for index,bad in ((0,b'24'),(1,b'0'),(2,b'0'),(3,None),(4,b'160000.0'),(4,b'0170000'),(4,b'170006'),(5,b'f')):
            row=identity.copy();row[index]=bad
            with self.subTest(index=index,bad=bad),PreflightFixture(self,rows={'source-identity':[row]}) as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture)
        target=[b'24',b'11',b'33',b'33',b'180006',b't',b'on']
        with PreflightFixture(self,rows={'target-identity':[target]}) as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture)
        for index in range(7):
            row=identity.copy();row[index]=b'180006' if index==4 else b'99'
            with self.subTest(recheck=index),PreflightFixture(self,rows={'source-identity-2':[row]}) as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture)
            row=identity.copy();row[0]=b'24';row[4]=b'180006';row[index]=b'160006' if index==4 else b'99'
            with self.subTest(target_recheck=index),PreflightFixture(self,rows={'target-identity-2':[row]}) as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture)
        for bad in ([],None,[[None]*7],[identity,identity],[identity[:-1]],[identity+[b'1']]):
            with PreflightFixture(self,rows={'source-identity':bad}) as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture)
        for source,target in ((18,16),(17,18),(16,17),(15,16),(18,19)):
            with PreflightFixture(self,source=source,target=target) as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture)
        for key,bad in (('schemas',[[b'null']]),('schemas',[[b'["x","x"]']]),('extensions-source',[[b'[["x",1]]']]),('extensions-source',[[b'[["x",true],["x",false]]']])):
            with PreflightFixture(self,rows={key:bad}) as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture)

    def assertPreflightRefused(self, fixture, error_class=None):
        self.assertEqual(len(fixture.output),1); line=fixture.output[0]
        self.assertTrue(line.endswith(b'\n') and line.count(b'\n')==1 and len(line)<=96)
        value=json.loads(line); self.assertEqual(set(value),{'version','kind','error_class'})
        self.assertEqual((value['version'],value['kind']),(1,'native_preflight_failure')); self.assertIn(value['error_class'],P.ERROR_CLASSES)
        if error_class is not None: self.assertEqual(value['error_class'],error_class)
        for canary in (b'private-role',b'private-db',b'private-host',b'secret-sentinel',b'42501'): self.assertNotIn(canary,line)

    def test_NativePreflightFailureLine(self):
        self.assertEqual(P.ERROR_CLASSES,('admission','capability','input','timeout','connection','diagnostic','result','transaction_state','unknown_commit','closure','internal'))
        for error_class in P.ERROR_CLASSES:
            with self.subTest(error_class=error_class),PreflightFixture(self,follow='1') as fixture,mock.patch.object(P,'preflight_checks',side_effect=P.Refusal(error_class,'42501')):
                self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture,error_class)
        for failure in (P.Refusal('secret-sentinel'),RuntimeError('secret-sentinel'),KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__),PreflightFixture(self) as fixture,mock.patch.object(P,'preflight_checks',side_effect=failure):
                self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture,'internal')
        with PreflightFixture(self,follow='') as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture,'capability')
        with PreflightFixture(self,fault='query-1') as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture,'result')
        with PreflightFixture(self,fault='expire') as fixture:self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture,'timeout')

    def test_NativePreflightResolverClosed(self):
        with PreflightFixture(self) as fixture,mock.patch.object(P,'qualified_resolver_policy',side_effect=P.Refusal('capability')):
            self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture);self.assertNotIn('source-open',fixture.events)
        with self.assertRaises(P.Refusal):P.qualified_resolver_policy(object())

    def test_NativePreflightTools(self):
        from types import SimpleNamespace
        diag=SimpleNamespace(check=lambda:None)
        for major in (16,18):
            seen=[]
            def capture(path,arg,deadline,diagnostics):seen.append((path,arg));return (os.path.basename(path)+' (PostgreSQL) '+str(major)+'.6 (package)\n').encode()
            with mock.patch.dict(os.environ,{'LANG':'C','LC_ALL':'C','PATH':'/other'},clear=True),mock.patch.object(P.os.path,'exists',return_value=True),mock.patch.object(P.os.path,'isfile',return_value=True),mock.patch.object(P,'preflight_capture',side_effect=capture):
                self.assertEqual(P.preflight_tools(major,time.monotonic()+5,diag),(major,major))
            self.assertEqual(seen,[(f'/usr/lib/postgresql/{major}/bin/pg_dump','--version'),(f'/usr/lib/postgresql/{major}/bin/pg_restore','--version')])
        for output in (b'',b'pg_restore (PostgreSQL) 18.6\n',b'pg_dump (PostgreSQL) 17.6\n',b'pg_dump (PostgreSQL) 18\n',b'pg_dump (PostgreSQL) 18.6',b'pg_dump (PostgreSQL) 18.6\nextra\n',b'pg_dump (PostgreSQL) 18.6\n\n',b'\xff\n'):
            with mock.patch.dict(os.environ,{'PG_BINDIR':'/explicit'},clear=True),mock.patch.object(P.os.path,'isfile',return_value=True),mock.patch.object(P,'preflight_capture',return_value=output):
                with self.assertRaises(P.Refusal):P.preflight_tools(18,time.monotonic()+3,diag)
        with mock.patch.dict(os.environ,{'PG_BINDIR':'','PG_CONFIG':'/ignored'},clear=True),mock.patch.object(P,'preflight_capture') as capture:
            with self.assertRaises(P.Refusal):P.preflight_tools(18,time.monotonic()+3,diag)
            capture.assert_not_called()
        seen=[]
        def configured(path,arg,deadline,diagnostics):
            seen.append((path,arg));return b'/explicit\n' if arg=='--bindir' else (os.path.basename(path)+' (PostgreSQL) 16.6\n').encode()
        with mock.patch.dict(os.environ,{'PG_CONFIG':'/config','PATH':'/wrong'},clear=True),mock.patch.object(P.os.path,'isfile',return_value=True),mock.patch.object(P,'preflight_capture',side_effect=configured):
            self.assertEqual(P.preflight_tools(16,time.monotonic()+3,diag),(16,16))
        self.assertEqual(seen,[('/config','--bindir'),('/explicit/pg_dump','--version'),('/explicit/pg_restore','--version')])
        for dump,restore in ((16,18),(18,16)):
            def mismatched(path,*args):return (os.path.basename(path)+' (PostgreSQL) '+str(dump if path.endswith('pg_dump') else restore)+'.6\n').encode()
            with mock.patch.dict(os.environ,{'PG_BINDIR':'/explicit'},clear=True),mock.patch.object(P.os.path,'isfile',return_value=True),mock.patch.object(P,'preflight_capture',side_effect=mismatched):
                with self.assertRaises(P.Refusal):P.preflight_tools(16,time.monotonic()+3,diag)
        seen=[]
        def from_path(path,*args):seen.append(path);return (os.path.basename(path)+' (PostgreSQL) 16.6\n').encode()
        with mock.patch.dict(os.environ,{'PATH':'/incomplete:/selected'},clear=True),mock.patch.object(P.os.path,'exists',return_value=False),mock.patch.object(P.os.path,'isfile',side_effect=lambda path:not path.startswith('/incomplete')),mock.patch.object(P,'preflight_capture',side_effect=from_path):
            self.assertEqual(P.preflight_tools(16,time.monotonic()+3,diag),(16,16))
        self.assertEqual(seen,['/selected/pg_dump','/selected/pg_restore'])
        for body in ('print("safe")','import sys;sys.stderr.write("secret-sentinel")','print("x"*4097)','raise SystemExit(1)'):
            with tempfile.TemporaryDirectory() as directory:
                tool=Path(directory)/'tool';tool.write_text('#!'+sys.executable+'\n'+body+'\n');tool.chmod(0o700)
                if body=='print("safe")':self.assertEqual(P.preflight_capture(str(tool),'--version',time.monotonic()+3,diag),b'safe\n')
                else:
                    with self.assertRaises(P.Refusal) as failure:P.preflight_capture(str(tool),'--version',time.monotonic()+3,diag)
                    self.assertNotIn('secret-sentinel',str(failure.exception))

    def test_NativePreflightBounds(self):
        source=(HERE/'evidence_program.py').read_bytes()
        self.assertLessEqual(len(source),122880)
        prefix=b'exec /usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B - '
        delimiter=b'RPGMIGRATE_NATIVE_EVIDENCE_PY'
        for role in (b'native_preflight',b'completed_evidence',b'target_retry'):
            main=prefix+role+b' '+b'5'*32+b' '+hashlib.sha256(source).hexdigest().encode()+b" <<'"+delimiter+b"'\n"+source+delimiter+b'\n'
            self.assertLessEqual(len(main)+1,126976)
            result=P.subprocess.run(['/bin/sh','-n','-c',main.decode()],capture_output=True,timeout=3)
            self.assertEqual((result.returncode,result.stdout,result.stderr),(0,b'',b''))
        with PreflightFixture(self) as fixture,mock.patch.object(P.os,'write',side_effect=OSError('secret-sentinel')):
            self.assertEqual(fixture.run(),1);self.assertEqual(fixture.output,[])
        with PreflightFixture(self) as fixture:
            calls=[]
            def partial(fd,raw):
                calls.append(bytes(raw))
                if len(calls)==1:raise BlockingIOError()
                fixture.output.append(bytes(raw[:7]));return min(7,len(raw))
            with mock.patch.object(P.os,'write',side_effect=partial),mock.patch.object(P,'wait_fd',return_value=True) as wait:
                self.assertEqual(fixture.run(),0)
                result=json.loads(b''.join(fixture.output));self.assertEqual(result['kind'],'native_preflight')
                wait.assert_called_once_with(1,True,190.)
        with PreflightFixture(self) as fixture,mock.patch.object(P.os,'set_blocking',side_effect=OSError('secret-sentinel')):
            self.assertEqual(fixture.run(),1);self.assertPreflightRefused(fixture)
        with PreflightFixture(self,fault='cleanup-last-source-close') as fixture:
            self.assertEqual(fixture.run(),0);self.assertEqual(fixture.clock,189.)
            self.assertEqual(len(fixture.queries),24)
        with PreflightFixture(self) as fixture:
            def stalled(*args):fixture.clock=191.;return False
            with mock.patch.object(P.os,'write',side_effect=BlockingIOError()),mock.patch.object(P,'wait_fd',side_effect=stalled):
                self.assertEqual(fixture.run(),1);self.assertEqual(fixture.output,[])

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
        with self.assertRaises(P.Refusal) as failure: P.qualified_resolver_policy(object())
        self.assertEqual(failure.exception.error_class, 'capability')

    def test_QualifiedResolverTuple(self):
        from types import SimpleNamespace
        kubelet = b'search default.svc.cluster.local svc.cluster.local cluster.local\nnameserver 10.96.0.10\noptions ndots:5\n'
        def packaged(machine):
            libc, runtime = P.QUALIFIED_RUNTIME[machine]
            return {f'/usr/lib/{machine}-linux-gnu/libc.so.6': libc, '/etc/nsswitch.conf': P.QUALIFIED_NSS,
                    '/usr/share/rpgmigrate/python-libpq-runtime.json': runtime}
        def policy(raw, machine='aarch64', path='/etc/resolv.conf', digests=None):
            with tempfile.TemporaryDirectory() as directory:
                file = Path(directory) / 'resolv.conf'
                file.write_bytes(raw)
                resolver = P.StableFile(path, P.CAP, opener=lambda _: os.open(file, os.O_RDONLY))
                try:
                    with mock.patch.object(P.os, 'uname', return_value=SimpleNamespace(machine=machine)), \
                         mock.patch.object(P, 'packaged_sha256', side_effect=(digests or packaged(machine if machine in P.QUALIFIED_RUNTIME else 'aarch64')).__getitem__):
                        P.qualified_resolver_policy(resolver)
                finally:
                    resolver.close()
        def refused(*args, **kwargs):
            with self.assertRaises(P.Refusal) as failure:
                policy(*args, **kwargs)
            self.assertEqual(failure.exception.error_class, 'capability')
        for machine in ('aarch64', 'x86_64'):
            with self.subTest(admitted=machine):
                policy(kubelet, machine)
        sixth = b'search ' + b' '.join([b'%cx' % (97 + i) + b'x' * 40 for i in range(5)] + [b'f' * 40])
        for name, raw in (('first six fill 256 bytes', sixth + b'\n'), ('seventh entry past the copy', sixth + b' ' + b'z' * 300 + b'\n'),
                          ('tab separator', b'search\tcluster.local\n'), ('comments', b'# domain x\n;search y\nsearch cluster.local\n')):
            with self.subTest(admitted=name):
                policy(raw)
        self.assertEqual(sum(len(entry) + 1 for entry in sixth.split()[1:]), 256)
        for name, raw in (('first six exceed 256 bytes', sixth + b'x\n'), ('single long entry', b'search ' + b'a' * 256 + b'\n'),
                          ('no search line', b'nameserver 10.96.0.10\n'), ('empty search', b'search\n'), ('empty search with space', b'search \n'),
                          ('two search lines', kubelet + b'search other\n'), ('domain line', kubelet + b'domain cluster.local\n'),
                          ('indented search', b' search cluster.local\n'), ('carriage return', b'search cluster.local\r\n'),
                          ('vertical tab', b'search a\x0bb\n'), ('non-ASCII', b'search caf\xc3\xa9\n'), ('NUL', b'search a\0b\n')):
            with self.subTest(refused=name):
                refused(raw)
        with self.subTest(refused='unqualified architecture'):
            refused(kubelet, 'ppc64le')
        with self.subTest(refused='resolver path'):
            refused(kubelet, path='/tmp/resolv.conf')
        for path in packaged('aarch64'):
            with self.subTest(refused=path):
                refused(kubelet, digests=dict(packaged('aarch64'), **{path: '0' * 64}))
        for resolver in (None, object()):
            with self.subTest(refused=type(resolver).__name__), self.assertRaises(P.Refusal):
                P.qualified_resolver_policy(resolver)
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(P, 'root_ancestors') as ancestors:
            file = Path(directory) / 'libc.so.6'
            file.write_bytes(b'packaged')
            self.assertEqual(P.packaged_sha256(str(file)), hashlib.sha256(b'packaged').hexdigest())
            ancestors.assert_called_once_with(str(file))

    def test_QualifiedResolverPolicy(self):
        import inspect
        self.assertEqual(tuple(inspect.signature(P.qualified_resolver_policy).parameters), ('resolver',))
        self.assertEqual(P.qualified_resolver_policy.__annotations__, {'resolver':'StableFile','return':None})
        with self.assertRaises(P.Refusal) as failure:
            P.qualified_resolver_policy(object())
        self.assertEqual(failure.exception.error_class, 'capability')
        for fixture in (PreflightFixture(self),SourceFixture(self),SourceFixture(self,'active_lag'),RetryFixture(self)):
            with self.subTest(role=type(fixture).__name__),fixture:
                opened=[];calls=[];opener=P.StableFile
                def capture(*args):
                    resolver=opener(*args);opened.append(resolver);return resolver
                def policy(resolver):
                    self.assertEqual(len(opened),1);self.assertIs(resolver,opened[0]);calls.append(resolver)
                with mock.patch.object(P,'StableFile',side_effect=capture),mock.patch.object(P,'qualified_resolver_policy',side_effect=policy):
                    self.assertEqual(fixture.run(),0)
                self.assertEqual(len(calls),2)

    def test_ConnectionHomePolicy(self):
        import pwd
        self.assertEqual(P.connection_home.__annotations__, {'envb':dict,'return':bytes|None})
        with mock.patch.object(pwd,'getpwuid',side_effect=AssertionError('unqualified NSS lookup')) as lookup:
            for home in (b'/private/home',b'relative home ',b'/nonutf8-\xff',b'/'*1023):
                with self.subTest(home=home):
                    self.assertIs(P.connection_home({b'HOME':home}),home)
            for env in ({},{b'HOME':b''},{b'HOME':'/text'},{b'HOME':b'bad\0home'},{b'HOME':b'/'*1024}):
                with self.subTest(env=env),self.assertRaises(P.Refusal) as failure:
                    P.connection_home(env)
                self.assertEqual(failure.exception.error_class,'capability')
            lookup.assert_not_called()

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

    def test_PrivateInputSetgidParent(self):
        # Reproduces kubelet fsGroup: the new directory inherits S_ISGID from a setgid /tmp.
        real_mkdir=os.mkdir
        def inherited(name,mode,*,dir_fd):
            real_mkdir(name,mode,dir_fd=dir_fd);os.chown(name,-1,os.getegid(),dir_fd=dir_fd);os.chmod(name,0o2700,dir_fd=dir_fd)
            if not os.stat(name,dir_fd=dir_fd).st_mode&stat.S_ISGID:os.rmdir(name,dir_fd=dir_fd);self.skipTest('setgid not settable here')
        spec=importlib.util.spec_from_file_location('native_evidence_writer',HERE/'evidence_request_writer.py');writer=importlib.util.module_from_spec(spec);spec.loader.exec_module(writer)
        with mock.patch.object(P.os,'mkdir',inherited):channel=P.InputChannel(os.urandom(16).hex(),time.monotonic()+3)
        try:
            self.assertEqual(os.stat(channel.path,follow_symlinks=False).st_mode,stat.S_IFDIR|0o700)
            probe=os.open(channel.path,os.O_RDONLY|os.O_DIRECTORY)
            try:self.assertEqual(writer.private_dir(probe),P.identity(os.fstat(probe)))
            finally:os.close(probe)
            channel.check()
        finally:channel.close()
        self.assertFalse(os.path.exists(channel.path))
        with mock.patch.object(P.os,'mkdir',inherited),mock.patch.object(P.os,'fchmod',lambda fd,mode:None):
            with self.assertRaises(P.Refusal):P.InputChannel(os.urandom(16).hex(),time.monotonic()+3)

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

        inventory=json.loads((base/'native_m8_source_manifest.json').read_bytes())
        self.assertEqual((inventory['native_format'],inventory['metadata_version']),(9,7))
        self.assertEqual(inventory['source_commit'],'daeac05a3360defab2671f41e60493bc8168838d')
        self.assertEqual(inventory['source_sha256'],'1f9e2336817939689ef739e37b46377ba10d41586b1bfdade617373df1a2d9d1')
        for name,digest in inventory['files'].items(): self.assertEqual(hashlib.sha256((base/name).read_bytes()).hexdigest(),digest)
        for major in (16,18):
            for kind,marker in (('catalog',False),('marker_catalog',True),('large_object_catalog','large_object')):
                expected=' '.join(line.strip() for line in (base/f'native_m8_{kind}_{major}.sql').read_text().splitlines() if not line.startswith('--'))
                self.assertEqual(P.catalog_sql(marker,major,7),expected)
        for version in (5,8,True,7.0):
            with self.subTest(version=version),self.assertRaises(P.Refusal): P.catalog_sql(False,16,version)

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

    def test_SessionTransactionState(self):
        class PQ:
            # PQtransactionStatus follows the statements sent, as libpq does: BEGIN opens, COMMIT/ROLLBACK close.
            def __init__(self): self.state=0; self.pending=None; self.sent=[]
            def PQstatus(self,_): return 0
            def PQtransactionStatus(self,_): return self.state
            def PQbackendPID(self,_): return 23
            def PQsendQueryParams(self,conn,sql,*args): self.pending=sql.decode(); self.sent.append(self.pending); return 1
            def PQflush(self,_): return 0
            def PQconsumeInput(self,_): return 1
            def PQisBusy(self,_): return 0
            def PQgetResult(self,_):
                sql,self.pending=self.pending,None
                if sql is None: return None
                tag=sql.split()[0].rstrip(';')
                if tag=='BEGIN': self.state=2
                elif tag in ('COMMIT','ROLLBACK'): self.state=0
                return dict(status=1,tag=tag.encode())
            def PQresultStatus(self,r): return r['status']
            def PQresultErrorField(self,r,_): return None
            def PQcmdStatus(self,r): return r['tag']
            def PQcmdTuples(self,r): return b''
            def PQntuples(self,r): return 0
            def PQnfields(self,r): return 0
            def PQclear(self,r): pass
            def PQfinish(self,_): pass
        def session():
            from types import SimpleNamespace
            s=P.Session.__new__(P.Session); s.lib=PQ(); s.conn=1; s.deadline=time.monotonic()+3
            s.diag=SimpleNamespace(check=lambda:None); s.notice=False; s.sqlstate=''
            s.connection=SimpleNamespace(check=lambda:None,close=lambda:None); s.claimed=True
            s.commit_dispatched=False; s.commit_ack=False; s.backend=23; s.props={}; s.connection_facts=lambda:{}
            return s
        # The preflight replica_role probe, verbatim from preflight_checks.
        s=session()
        s.query('BEGIN READ ONLY;',tag='BEGIN',state=2,rows=0,width=0)
        s.query("SET LOCAL session_replication_role TO 'replica';",tag='SET',state=2,rows=0,width=0)
        s.query('ROLLBACK;',tag='ROLLBACK',rows=0,width=0)
        self.assertEqual((s.lib.state,len(s.lib.sent)),(0,3)); s.close()
        s=session()
        s.query('BEGIN READ WRITE;',tag='BEGIN',state=2,rows=0,width=0)
        s.query('COMMIT;',tag='COMMIT',rows=0,width=0,commit=True)
        self.assertTrue(s.commit_ack); s.close()
        for prior,sql,arguments in [(0,'ROLLBACK;',dict(tag='ROLLBACK',rows=0,width=0)),(0,'COMMIT;',dict(tag='COMMIT',rows=0,width=0,commit=True)),(2,'BEGIN;',dict(tag='BEGIN',state=2,rows=0,width=0)),(0,'SET x TO 1;',dict(tag='SET',state=2,rows=0,width=0)),(2,'SET x TO 1;',dict(tag='SET',rows=0,width=0))]:
            s=session(); s.lib.state=prior
            with self.assertRaises(P.Refusal) as failure: s.query(sql,**arguments)
            self.assertEqual((failure.exception.error_class,s.lib.sent,s.commit_dispatched),(P.TX_STATE,[],False)); s.close(False)

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

        from types import SimpleNamespace
        req, candidate, endpos, row = candidate_fixture(os.stat(HERE))
        target = P.validate_manifest(candidate, req, os.stat(HERE))
        state = SimpleNamespace(manifest=candidate, target=target, req=req)
        self.assertEqual(P.validate_control_row(row, state)['drain_end_lsn'], '0/20')
        self.assertEqual(P.validate_source_manifest(candidate, req, os.stat(HERE))[3], 'fe1781120b07a00ef1128150a37c78e97584c53d585ee972367ff67a745f9b59')
        for version, metadata in ((8, 7), (9, 6), (10, 7), (999, 7), (True, 7), (9.0, 7), (9, True), (9, 7.0)):
            changed = copy.deepcopy(candidate); changed['format_version'] = version
            changed['target_provenance'][0]['record']['intent']['binding']['metadata_version'] = metadata
            for validate in (P.validate_manifest, P.validate_source_manifest):
                with self.subTest(version=version, metadata=metadata), self.assertRaises(P.Refusal): validate(changed, req, os.stat(HERE))
        for key in ('large_object_table_oid', 'large_object_index_oid'):
            for value in (0, 55, 66, 77, 78, True):
                if key == 'large_object_table_oid' and value == 78: continue
                changed = copy.deepcopy(candidate); changed['target_provenance'][0]['record'][key] = value
                with self.subTest(key=key, value=value), self.assertRaises(P.Refusal): P.validate_manifest(changed, req, os.stat(HERE))
            changed = copy.deepcopy(candidate); del changed['target_provenance'][0]['record'][key]
            with self.assertRaises(P.Refusal): P.validate_manifest(changed, req, os.stat(HERE))
        for key in ('copy_policy', 'copy_tables'):
            changed = copy.deepcopy(candidate); del changed['clone_state']['plan'][key]
            with self.assertRaises(P.Refusal): P.validate_manifest(changed, req, os.stat(HERE))
        populated=copy.deepcopy(candidate);clone=populated['clone_state'];plan=clone['plan']
        table=dict(source_oid=101,name=dict(schema='public',table='records'),writable_columns=['id'],all_columns=['id'],identity_modes=[0],partition_identity_modes=None)
        plan['tables']=[table]
        plan['copy_tables']=[dict(source_oid=101,format='binary',parts=[dict(number=1,count=2,start_block=0,end_block=10),dict(number=2,count=2,start_block=10,end_block=None)])]
        clone['owned_objects']=[dict(class_oid=1259,object_oid=102,object_sub_id=0,namespace_oid=103,namespace_name='public',name='records',kind='r',owner_oid=33)]
        clone['owned_tables']=[dict(table=table,target_oid=102,target_namespace_oid=103,target_owner_oid=33,generation=1)]
        self.assertEqual(P.validate_manifest(populated,req,os.stat(HERE)),target)
        self.assertEqual(P.validate_source_manifest(populated,req,os.stat(HERE))[3],P.validate_source_manifest(candidate,req,os.stat(HERE))[3])
        for format in ('text','binary'):
            singleton=copy.deepcopy(plan);singleton['copy_tables'][0].update(format=format,parts=[dict(number=1,count=1,start_block=0,end_block=None)])
            P.copy_profile(singleton,9)
        bad_profiles=[lambda p:p.update(copy_tables=[]),lambda p:p['copy_tables'].append(copy.deepcopy(p['copy_tables'][0])),lambda p:p['copy_tables'][0].update(source_oid=999),lambda p:p['copy_tables'][0].update(format='unknown'),lambda p:p['copy_tables'][0].update(parts=[]),lambda p:p['copy_tables'][0]['parts'][0].update(number=True),lambda p:p['copy_tables'][0]['parts'][0].update(count=1),lambda p:p['copy_tables'][0]['parts'][0].update(start_block=1),lambda p:p['copy_tables'][0]['parts'][0].update(end_block=None),lambda p:p['copy_tables'][0]['parts'][0].update(end_block=0),lambda p:p['copy_tables'][0]['parts'][1].update(start_block=9),lambda p:p['copy_tables'][0]['parts'][1].update(end_block=11),lambda p:p['copy_tables'][0]['parts'][1].pop('end_block'),lambda p:p['copy_policy'].update(use_copy_binary=False),lambda p:p['copy_policy'].update(split_max_parts=1),lambda p:p['copy_policy'].update(split_max_parts=0),lambda p:p['copy_policy'].update(split_max_parts=2**31),lambda p:p['copy_policy'].update(split_tables_larger_than=0),lambda p:p['copy_policy'].update(skip_large_objects=1)]
        for mutate in bad_profiles:
            changed=copy.deepcopy(populated);mutate(changed['clone_state']['plan'])
            for validate in (P.validate_manifest,P.validate_source_manifest):
                with self.subTest(mutate=mutate),self.assertRaises(P.Refusal):validate(changed,req,os.stat(HERE))
        boundary=copy.deepcopy(plan);boundary['copy_policy']['split_max_parts']=4096
        boundary['copy_tables'][0]['parts']=[dict(number=i+1,count=4096,start_block=i,end_block=i+1 if i<4095 else None) for i in range(4096)]
        P.copy_profile(boundary,9)
        boundary['copy_tables'][0]['parts'].append(dict(number=4097,count=4097,start_block=4096,end_block=None))
        with self.assertRaises(P.Refusal):P.copy_profile(boundary,9)
        ordered=copy.deepcopy(plan);second=copy.deepcopy(table);second['source_oid']=104;second['name']['table']='other'
        ordered['tables'].append(second);execution=copy.deepcopy(ordered['copy_tables'][0]);execution['source_oid']=104;ordered['copy_tables'].append(execution)
        P.copy_profile(ordered,9);ordered['copy_tables'].reverse()
        with self.assertRaises(P.Refusal):P.copy_profile(ordered,9)
        controls = [dict(prepared_generation=2), dict(prepared_generation=True), dict(large_objects=dict(generation=2, initial=dict(objects=0, bytes=0), final_sync=dict(objects=0, bytes=0))), dict(large_objects=dict(generation=1, initial=None, final_sync=dict(objects=0, bytes=0))), dict(large_objects=dict(generation=1, initial=dict(objects=True, bytes=0), final_sync=dict(objects=0, bytes=0)))]
        for updates in controls:
            changed = list(row); changed[22] = json.dumps(dict(json.loads(row[22]), **updates))
            with self.subTest(control=updates), self.assertRaises(P.Refusal): P.validate_control_row(changed, state)
        for key in ('prepared_generation', 'large_objects'):
            changed = list(row); control = json.loads(row[22]); del control[key]; changed[22] = json.dumps(control)
            with self.assertRaises(P.Refusal): P.validate_control_row(changed, state)
        control=json.loads(row[22]);control['large_objects']['initial']=dict(objects=2,bytes=2**64-1);control['large_objects']['final_sync']=dict(objects=3,bytes=17)
        changed=list(row);changed[22]=json.dumps(control);P.validate_control_row(changed,state)
        for value in (dict(objects=2**64,bytes=0),dict(objects=0,bytes=-1),dict(objects=0,bytes=0,extra=True)):
            changed=list(row);control=json.loads(row[22]);control['large_objects']['initial']=value;changed[22]=json.dumps(control)
            with self.subTest(receipt=value),self.assertRaises(P.Refusal):P.validate_control_row(changed,state)
        control=json.loads(row[22]);control['prepared_generation']=1
        changed=list(row);changed[22]=json.dumps(control);P.validate_control_row(changed,state)
        skipped=copy.deepcopy(candidate);skipped['clone_state']['plan']['copy_policy']['skip_large_objects']=True
        skip_state=SimpleNamespace(manifest=skipped,target=target,req=req)
        with self.assertRaises(P.Refusal):P.validate_control_row(row,skip_state)
        control['large_objects'].update(initial=None,final_sync=None);changed[22]=json.dumps(control)
        P.validate_control_row(changed,skip_state)
        with self.assertRaises(P.Refusal):P.validate_control_row(changed,state)
        for key in ('initial','final_sync'):
            control=json.loads(row[22]);del control['large_objects'][key];changed=list(row);changed[22]=json.dumps(control)
            with self.assertRaises(P.Refusal):P.validate_control_row(changed,state)
        changed = list(row); changed[1] = '6'
        with self.assertRaises(P.Refusal): P.validate_control_row(changed, state)

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
        events=[]; fault=[None]; catalogs=[]; locks=[]
        class Session:
            def __init__(self,library,conninfo,deadline,diag):
                self.role=conninfo; self.backend=23; self.props={'PQuser':b'fixture','PQdb':b'database','ssl':0}
                if conninfo==b'target':
                    if events[-1] != ('close',b'source',True): raise AssertionError('target opened before positive source closure')
                events.append(('open',conninfo))
            def quote(self,value): return '"'+value+'"'
            def query(self,sql,params=(),tag='SELECT 1',state=0,rows=1,width=1,commit=False):
                events.append(('query',self.role,tag))
                if sql.startswith(('SELECT n.nspowner','SELECT c.relnamespace')): catalogs.append((sql,params))
                if sql.startswith('LOCK TABLE ONLY'): locks.append(sql)
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

            events.clear(); catalogs.clear(); locks.clear(); fault[0]=None
            req,manifest,endpos,row=candidate_fixture(os.stat(HERE))
            state=SimpleNamespace(req=req,manifest=manifest,target=manifest['target_provenance'][0]['record'],revalidate=lambda:None)
            checkpoint,guards=P.observe(state,None,materials,time.monotonic()+25,None)
            self.assertEqual(guards,P.GUARDS)
            self.assertEqual([params for _,params in catalogs],[(44,55,22,33,'_rpgmigrate_'+req['execution']['work']['run_id'],'checkpoint',66,77,78,79),(66,77,44,33),(78,79,44,33)])
            self.assertEqual([sql for sql,_ in catalogs],[P.catalog_sql(kind,18,7) for kind in (False,True,'large_object')])
            self.assertEqual(len(locks),3);self.assertIn('"large_object_done"',locks[-1])

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
        for fault in ('none','unknown_commit','resolver_closure'):
            unknown=fault=='unknown_commit';resolver_failure=fault=='resolver_closure'
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
            resolver=SimpleNamespace(raw=b'nameserver 127.0.0.1\n',check=lambda:events.append('resolver-check'),close=lambda:events.append('resolver'))
            diagnostics=SimpleNamespace(close=lambda:events.append('diagnostics'))
            def policy(actual):
                self.assertIs(actual,resolver);events.append('resolver-policy')
                if resolver_failure and events.count('resolver-policy')==2:raise P.Refusal('capability')
            with mock.patch.dict(os.environ,{'LANG':'C','LC_ALL':'C'},clear=True),mock.patch.object(P.os,'geteuid',return_value=65532),mock.patch.object(P.resource,'setrlimit'),mock.patch.object(P,'InputChannel',Channel),mock.patch.object(P,'PrivateState',return_value=state),mock.patch.object(P,'Materialization',return_value=materials),mock.patch.object(P,'StableFile',return_value=resolver),mock.patch.object(P,'Diagnostics',return_value=diagnostics),mock.patch.object(P,'load_runtime'),mock.patch.object(P,'qualified_resolver_policy',side_effect=policy),mock.patch.object(P,'observe',side_effect=observe) as operation,mock.patch.object(P.os,'write') as ordinary:
                self.assertEqual(P.run_completed_evidence('5'*32,'6'*64),1 if unknown or resolver_failure else 0)
                operation.assert_called_once();ordinary.assert_not_called()
            self.assertEqual(events.count('consume'),1);self.assertEqual(events.count('publish'),1)
            if unknown:self.assertEqual(payloads[0]['error_class'],'unknown_commit');self.assertNotIn('checkpoint',payloads[0])
            elif resolver_failure:
                self.assertEqual(payloads[0]['error_class'],'capability');self.assertNotIn('checkpoint',payloads[0])
                self.assertEqual(events.count('resolver-policy'),2);self.assertNotIn('resolver-check',events)
            else:
                self.assertIs(payloads[0]['private_closed'],True)
                self.assertEqual(events.count('resolver-policy'),2)
                self.assertEqual(events[events.index('target')+1:events.index('resolver')],['resolver-policy','resolver-check'])

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

    def test_FileKeyLogRefusal(self):
        self.require_file_api()
        from types import SimpleNamespace
        base={b'password':b'password-canary',b'sslmode':b'disable'}
        pin=mock.Mock(side_effect=AssertionError('credential pin before refusal'))
        material=SimpleNamespace(pin=pin,check=lambda:None)
        cases=[
            (b"password=password-canary sslmode=disable sslkeylogfile='/private/key-log-canary'",b'/private/key-log-canary',None,None),
            (b'postgresql://u:password-canary@host/db?sslmode=disable&sslkeylogfile=%2Fprivate%2Fkey-log-canary',b'/private/key-log-canary',None,None),
            (b'fixture-selected-option',None,b'FIXTURE_KEYLOG_OPTION',b'/private/key-log-canary'),
            (b'fixture-compiled-option',None,None,b'/private/key-log-canary'),
            (b'fixture-before-file-pin',b'/private/key-log-canary',None,None),
            (b'fixture-nonempty-whitespace',b' ',None,None),
        ]
        for raw,explicit,environment,fallback in cases:
            with self.subTest(case=raw):
                library=FileOptionsLibrary({raw:dict(base)});library.defaults=dict(library.defaults)
                if raw==b'fixture-before-file-pin':library.cases[raw][b'sslmode']=b'require'
                library.defaults[b'sslkeylogfile']=(explicit,environment,fallback if environment is None else None)
                env={b'HOME':b'/private/default-canary'}
                if environment is not None:env[environment]=fallback
                library.PQconnectStartParams=mock.Mock();library.PQsendQueryParams=mock.Mock()
                connection=None
                try:
                    with mock.patch.object(P,'SealedFile') as snapshot,self.assertRaises(P.Refusal) as failure:
                        connection=P.ConnectionInput(library,raw,env,material,time.monotonic()+3)
                    self.assertEqual(failure.exception.error_class,'capability')
                    self.assertNotIn('canary',str(failure.exception));snapshot.assert_not_called()
                    self.assertEqual(P.failure_envelope('5'*32,failure.exception),dict(version=1,kind='completed_evidence_failure',operation_id='5'*32,error_class='capability',sqlstate=''))
                    self.assertEqual(library.parsed,[raw]);self.assertEqual(library.options_freed,1)
                    pin.assert_not_called();library.PQconnectStartParams.assert_not_called();library.PQsendQueryParams.assert_not_called()
                finally:
                    if connection is not None:connection.close()
        for explicit,environment,selected in ((b'',None,None),(None,None,None),(b'',b'FIXTURE_KEYLOG_OPTION',b'ignored'),(None,b'FIXTURE_KEYLOG_OPTION',b'')):
            raw=b'postgresql://u:password-canary@first,second/db%3Dname?sslkeylogfile=&gssencmode=prefer&require_auth=password%2Cscram-sha-256&oauth_client_secret=provider-canary&sslmode=disable'
            library=FileOptionsLibrary({raw:base});library.defaults=dict(library.defaults)
            library.defaults[b'sslkeylogfile']=(explicit,environment,None)
            env={b'UNRELATED_OPTION':b'provider-canary'}
            if environment is not None:env[environment]=selected
            before=dict(env);connection=P.ConnectionInput(library,raw,env,material,time.monotonic()+3)
            try:
                self.assertEqual(list(connection.keywords),[b'dbname',None]);self.assertEqual(list(connection.values),[raw,None])
                self.assertIs(connection.raw,raw);self.assertEqual(library.parsed,[raw]);self.assertEqual(env,before)
            finally:connection.close()
        for refused in ('source','target',None):
            source=b'source-opaque';target=b'target-opaque';parsed=[]
            values={source:dict(base),target:dict(base)}
            if refused is not None:values[source if refused=='source' else target][b'sslkeylogfile']=b'/private/key-log-canary'
            library=FileOptionsLibrary(values);library.PQsendQueryParams=mock.Mock()
            library.PQconnectStartParams=mock.Mock(side_effect=lambda keys,values,expand:(parsed.append((list(library.parsed),values[0],expand)) or None))
            owner=P.Materialization.__new__(P.Materialization);owner.roles={'source':source,'target':target};owner.envb={};owner.inputs={};owner.prepared=False;owner.check=lambda:None;owner.pin=pin
            if refused is not None:
                with self.assertRaises(P.Refusal) as failure:
                    owner.prepare(library,time.monotonic()+3)
                    P.Session(library,owner.connection('source'),time.monotonic()+3,SimpleNamespace(check=lambda:None))
                self.assertEqual(failure.exception.error_class,'capability');self.assertFalse(owner.prepared)
                self.assertEqual(library.parsed,[source] if refused=='source' else [source,target])
                self.assertTrue(all(item.closed for item in owner.inputs.values()));library.PQconnectStartParams.assert_not_called()
            else:
                owner.prepare(library,time.monotonic()+3)
                for role,raw in (('source',source),('target',target)):
                    with self.assertRaises(P.Refusal) as failure:P.Session(library,owner.connection(role),time.monotonic()+3,SimpleNamespace(check=lambda:None))
                    self.assertEqual(failure.exception.error_class,'connection')
                self.assertEqual(parsed,[([source,target],source,1),([source,target],target,1)])
            library.PQsendQueryParams.assert_not_called();pin.assert_not_called()

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
        material=P.Materialization.__new__(P.Materialization);material.source_only=False;material.target_only=False
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
            material=P.Materialization.__new__(P.Materialization);material.source_only=False;material.target_only=False;material.env=dict(os.environ);material.cwd=cwd;material.cwd_id=P.identity(os.fstat(cwd));material.paths={};material.absences=set();material.files=[];material.inputs={}
            def open_file(raw,fd):return os.open(raw,os.O_RDONLY|os.O_NONBLOCK,dir_fd=fd)
            try:
                with mock.patch.object(P,'open_connection_file',side_effect=open_file):
                    pinned=material.pin(os.fsencode(path/'projection'));self.assertEqual(pinned.raw,b'original');material.check()
                    file.write_bytes(b'changed')
                    with self.assertRaises(P.Refusal):material.check()
            finally:material.close()
        cwd=os.open('.',os.O_RDONLY|os.O_DIRECTORY)
        material=P.Materialization.__new__(P.Materialization);material.source_only=False;material.target_only=False;material.env=dict(os.environ);material.cwd=cwd;material.cwd_id=P.identity(os.fstat(cwd));material.files=[];material.absences=set()
        dead=SimpleNamespace(check=lambda:(_ for _ in ()).throw(AssertionError('closed source snapshot reopened')))
        live=mock.Mock()
        material.inputs={'source':SimpleNamespace(closed=True,absent=False,snapshots=[dead]),'target':SimpleNamespace(closed=False,absent=False,snapshots=[live])}
        try:material.check();live.check.assert_called_once()
        finally:os.close(cwd)


if __name__ == '__main__':
    unittest.main(verbosity=2)

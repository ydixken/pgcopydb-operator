# Private operator-owned evidence program; no report can construct this operation's input.
import ctypes as C
import os
import stat
import fcntl
import select
import time
import json
import hashlib
import struct
import sys
import binascii
import resource
import errno
import subprocess
CAP = 65536
CAP_ERR='capability'
RESULT='result'
INPUT='input'
CLOSE='closure'
CONN='connection'
TX_STATE='transaction_state'
U64 = (1 << 64)-1
GUARDS = 'target_settings target_identity configure_durability target_fence idle_guard begin lock_checkpoint lock_marker catalog_checkpoint catalog_marker row_read same_value_update deferred_effects row_comparison precommit_guard commit idle_guard unlock closure_guard'.split()
BANNED = {'LOCALDOMAIN','RES_OPTIONS','LANGUAGE','LOCPATH','GCONV_PATH','NLSPATH'}
class Refusal(Exception):
 def __init__(self, error_class='admission', sqlstate=''):
  self.error_class = error_class
  self.sqlstate = sqlstate if sqlstate and len(sqlstate)==5 and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789' for c in sqlstate) else ''
  super().__init__('native evidence refused')
def need(value, kind='admission'):
 if not value: raise Refusal(kind)
def same(a,b): return json.dumps(a,sort_keys=True)==json.dumps(b,sort_keys=True)
def keys(value, want):
 need(type(value) is dict and set(value)==set(want.split()))
def text(value, cap=512, empty=False):
 need(type(value) is str)
 need((empty or bool(value)) and len(value.encode('utf-8'))<=cap and '\0' not in value)
 return value
def uint(value, maximum=U64, positive=False):
 need(type(value) is int and (1 if positive else 0)<=value<=maximum)
 return value
def boolean(value):
 need(type(value) is bool)
 return value
def hextext(value, size):
 text(value, size)
 need(len(value)==size and all(c in '0123456789abcdef' for c in value))
 return value
def strict_json(raw, cap):
 need(type(raw) is bytes and 0<len(raw)<=cap, INPUT)
 def pairs(items):
  result = {}
  for key, value in items:
   need(key not in result, INPUT); result[key] = value
  return result
 def no_number(_): raise Refusal(INPUT)
 def integer(s):
  value=int(s); need(-2147483648<=value<=U64,INPUT); return value
 try:
  decoded = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_float=no_number, parse_constant=no_number, parse_int=integer)
  stack=[(decoded,0)]
  while stack:
   value, depth=stack.pop(); need(depth<=32,INPUT)
   if type(value) is str: value.encode('utf-8')
   elif type(value) is dict:
    for key, item in value.items(): key.encode('utf-8'); stack.append((item,depth+1))
   elif type(value) is list: stack.extend((item,depth+1) for item in value)
  return decoded
 except Refusal: raise
 except (ValueError, UnicodeError, RecursionError, OverflowError): raise Refusal(INPUT) from None
def lsn(value):
 text(value,17); parts=value.split('/')
 need(len(parts)==2 and all(0<len(p)<=8 and all(c in '0123456789abcdefABCDEF' for c in p) for p in parts))
 return (int(parts[0],16)<<32)|int(parts[1],16)
def lsn_text(value): return '%X/%X' % (value>>32, value&0xffffffff)
def work_identity(work):
 keys(work,'migration_uid run_id workdir_id checkpoint_generation')
 uid=text(work['migration_uid'],36)
 need(len(uid)==36 and [i for i,c in enumerate(uid) if c=='-']==[8,13,18,23])
 need(hextext(work['run_id'],32)==uid.replace('-','') and work['checkpoint_generation']==1)
 uint(work['checkpoint_generation'],1,True)
 label=text(work['workdir_id'],256)
 need(label.strip()==label and not any(ord(c)<32 or 127<=ord(c)<=159 or c in '/\\' for c in label))
def validate_request(raw, op, sha):
 value=strict_json(raw,CAP)
 keys(value,'version kind operation_id program_sha256 execution pod_name container saved_e')
 need(uint(value['version'],1,True)==1 and value['kind']=='completed_evidence',INPUT)
 need(hextext(value['operation_id'],32)==op and hextext(value['program_sha256'],64)==sha,INPUT)
 need(lsn(value['saved_e'])>0,INPUT)
 execution=value['execution']
 keys(execution,'namespace migration_name pvc_name job_name migration_uid pvc_uid job_uid worker_job_uid migration_generation attempt role work image_ref platform_image_id template_sha256 connection_reference_sha256')
 for key in 'namespace migration_name pvc_name job_name migration_uid pvc_uid job_uid worker_job_uid image_ref platform_image_id'.split(): text(execution[key],1024)
 for key in 'namespace migration_name pvc_name job_name'.split():
  name=execution[key]; need(len(name)<=253 and all(c in 'abcdefghijklmnopqrstuvwxyz0123456789-.' for c in name) and name[0].isalnum() and name[-1].isalnum())
 uint(execution['migration_generation'],(1<<63)-1,True); uint(execution['attempt'],(1<<31)-1)
 need(execution['role']=='evidence'); work_identity(execution['work'])
 need(execution['work']['migration_uid']==execution['migration_uid'] and execution['work']['workdir_id']==execution['pvc_uid'])
 hextext(execution['template_sha256'],64); hextext(execution['connection_reference_sha256'],64)
 container=value['container']; keys(container,'pod_uid container_name container_id restart_count image_id template_sha256')
 for key in 'pod_uid container_name container_id image_id'.split(): text(container[key],1024)
 uint(container['restart_count'],(1<<31)-1); hextext(container['template_sha256'],64)
 need(container['template_sha256']==execution['template_sha256'] and container['image_id']==execution['platform_image_id'])
 text(value['pod_name'],253)
 return value
def request_digests(raw):
 digest=hashlib.sha256(raw).digest()
 return digest.hex(), hashlib.sha256(b'rpgmigrate.native.execution.v1\0'+digest).hexdigest()
def validate_guards(guards): need(guards==GUARDS,RESULT)
def exact_true(rows):
 need(rows==[[b't']],RESULT)
 return True
def failure_envelope(op, failure):
 allowed={'admission',CAP_ERR,INPUT,'timeout',CONN,'diagnostic',RESULT,TX_STATE,'unknown_commit',CLOSE,'internal'}
 return dict(version=1,kind='completed_evidence_failure',operation_id=op,error_class=failure.error_class if failure.error_class in allowed else 'internal',sqlstate=failure.sqlstate)
def validate_environment(env):
 need(not any(k.startswith('LD_') or k in BANNED for k in env),CAP_ERR)
 need(env.get('LC_ALL')=='C' and env.get('LANG')=='C',CAP_ERR)
def qualified_resolver_policy():
 # The packaged libc/resolver bounds need independent qualification before activation.
 raise Refusal(CAP_ERR)
def identity(info): return (info.st_dev,info.st_ino)
def stamp(info):
 return (info.st_dev,info.st_ino,info.st_mode,info.st_nlink,info.st_uid,info.st_gid,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
def fdopen(path, directory=False, parent=None):
 flags=os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW|os.O_CLOEXEC
 if directory: flags|=os.O_DIRECTORY
 return os.open(path,flags,dir_fd=parent)
def checked_close(fd):
 try: os.close(fd)
 except OSError: raise Refusal(CLOSE) from None
def read_bytes(fd, cap):
 raw=bytearray()
 while True:
  part=os.read(fd,min(8192,cap+1-len(raw)))
  if not part: break
  raw.extend(part); need(len(raw)<=cap)
 return bytes(raw)
def private_type(fd,directory=False):
 info=os.fstat(fd)
 need(info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==(0o700 if directory else 0o600))
 need(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode) and info.st_nlink==1)
 return info
def wait_fd(fd, writing, deadline, diag=None):
 remaining=deadline-time.monotonic(); need(remaining>0,'timeout')
 reads=[fd] if not writing else []
 if diag is not None: reads.append(diag.fd)
 ready, writable, _=select.select(reads,[fd] if writing else [],[],min(remaining,0.05))
 if diag is not None: diag.check()
 return fd in (writable if writing else ready)
class InputChannel:
 names=('request.fifo','stdout.fifo','stderr.fifo')
 def __init__(self,op,deadline):
  hextext(op,32); self.path='/tmp/rpgmigrate-evidence-'+op; self.deadline=deadline
  self.parent=self.directory=None; self.readers={}; self.writers={}; self.ids={}; self.remaining=set(); self.routed=False; self.consumed=False
  try:
   self.parent=fdopen(os.path.realpath('/tmp'),True)
   os.mkdir('rpgmigrate-evidence-'+op,0o700,dir_fd=self.parent)
   self.directory=fdopen('rpgmigrate-evidence-'+op,True,self.parent); self.dir_id=identity(private_type(self.directory,True))
   for name in self.names:
    os.mkfifo(name,0o600,dir_fd=self.directory); self.remaining.add(name)
   for name in self.names[1:]+self.names[:1]:
    fd=os.open(name,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=self.directory)
    self.readers[name]=fd; self.ids[name]=identity(os.fstat(fd))
    self.writers[name]=os.open(name,os.O_WRONLY|os.O_NONBLOCK|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=self.directory)
   self.check()
  except BaseException:
   self.close(); raise
 def check(self):
  need(self.directory is not None and identity(os.stat(self.path,follow_symlinks=False))==self.dir_id,INPUT)
  need(identity(private_type(self.directory,True))==self.dir_id and set(os.listdir(self.directory))==self.remaining,INPUT)
  for name in self.remaining:
   info=os.stat(name,dir_fd=self.directory,follow_symlinks=False)
   need(identity(info)==self.ids[name] and stat.S_ISFIFO(info.st_mode) and info.st_nlink==1 and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o600,INPUT)
  for collection in (self.readers,self.writers):
   for name,fd in collection.items(): need(identity(os.fstat(fd))==self.ids[name],INPUT)
 def route(self):
  os.dup2(self.writers['stdout.fifo'],1); checked_close(self.writers['stdout.fifo']); self.writers['stdout.fifo']=1
  os.dup2(self.writers['stderr.fifo'],2); self.routed=True
 def drop(self,collection,name): checked_close(collection.pop(name))
 def consume(self):
  need(not self.consumed,INPUT); self.consumed=True; data=bytearray(); size=None
  while True:
   self.check(); need(time.monotonic()<self.deadline,'timeout')
   try: part=os.read(self.readers['request.fifo'],8192)
   except BlockingIOError:
    wait_fd(self.readers['request.fifo'],False,self.deadline); continue
   if not part:
    need(size is not None and 'request.fifo' not in self.writers and len(data)==size+4,INPUT)
    try: fcntl.flock(self.directory,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError: pass
    else:
     fcntl.flock(self.directory,fcntl.LOCK_UN); raise Refusal(INPUT)
    self.check(); self.drop(self.readers,'request.fifo'); os.unlink('request.fifo',dir_fd=self.directory); self.remaining.remove('request.fifo')
    for name in ('stdout.fifo','stderr.fifo'): self.drop(self.readers,name)
    return bytes(data[4:])
   data.extend(part); need(len(data)<=CAP+4,INPUT)
   if size is None and len(data)>=4:
    size=struct.unpack('!I',data[:4])[0]; need(0<size<=CAP,INPUT)
   if size is not None:
    need(len(data)<=size+4,INPUT)
    if len(data)==size+4 and 'request.fifo' in self.writers: self.drop(self.writers,'request.fifo')
 def finish(self,payload=None,deadline=None):
  if self.directory is None:
   if self.parent is not None: checked_close(self.parent); self.parent=None
   return
  failed=False
  try:
   self.check()
   if self.routed: os.dup2(self.writers['stderr.fifo'],2)
   for name in self.remaining: os.unlink(name,dir_fd=self.directory)
   self.remaining.clear(); need(os.listdir(self.directory)==[],CLOSE); os.rmdir(self.path)
   if payload is not None:
    need(len(payload)<=CAP,RESULT); at=0; limit=deadline or time.monotonic()+5
    while at<len(payload):
     need(time.monotonic()<limit,'timeout')
     try:
      count=os.write(self.writers['stdout.fifo'],payload[at:]); need(count>0,CLOSE); at+=count
     except BlockingIOError: wait_fd(self.writers['stdout.fifo'],True,limit)
  except BaseException: failed=True
  for collection in (self.readers,self.writers):
   for fd in collection.values():
    try: checked_close(fd)
    except BaseException: failed=True
   collection.clear()
  if self.routed:
   try: checked_close(2)
   except BaseException: failed=True
  limit=min(time.monotonic()+5,self.deadline+60)
  try:
   while True:
    need(time.monotonic()<limit,'timeout')
    try: fcntl.flock(self.directory,fcntl.LOCK_EX|fcntl.LOCK_NB); break
    except BlockingIOError: select.select([],[],[],min(.02,max(0,limit-time.monotonic())))
  except BaseException: failed=True
  for name in ('directory','parent'):
   fd=getattr(self,name); setattr(self,name,None)
   if fd is not None:
    try: checked_close(fd)
    except BaseException: failed=True
  need(not failed,CLOSE)
 def close(self): self.finish()
class StableFile:
 def __init__(self,path,cap,private=False,parent=None,opener=None,validate=None):
  self.path=path; self.parent=parent; self.cap=cap; self.opener=opener; self.fd=opener(path) if opener else fdopen(path,parent=parent)
  try:
   self.before=os.fstat(self.fd)
   if validate: validate(self.before)
   need(stat.S_ISREG(self.before.st_mode) and self.before.st_nlink==1 and self.before.st_size<=cap)
   if private: private_type(self.fd)
   self.raw=read_bytes(self.fd,cap); self.check()
  except BaseException:
   checked_close(self.fd); self.fd=None; raise
 def check(self):
  need(self.fd is not None and stamp(os.fstat(self.fd))==stamp(self.before))
  if self.opener:
   try: other=self.opener(self.path)
   except OSError: raise Refusal('admission') from None
   try: need(stamp(os.fstat(other))==stamp(self.before))
   finally: checked_close(other)
  else: need(stamp(os.stat(self.path,dir_fd=self.parent,follow_symlinks=False))==stamp(self.before))
  os.lseek(self.fd,0,0); need(read_bytes(self.fd,self.cap)==self.raw)
  need(stamp(os.fstat(self.fd))==stamp(self.before))
 def close(self):
  if self.fd is not None:
   fd=self.fd; self.fd=None; checked_close(fd)
def root_ancestors(path):
 need(path.startswith('/') and os.path.normpath(path)==path,CAP_ERR)
 parts=path.split('/')[1:-1]; current='/'
 for part in ['']+parts:
  if part: current=os.path.join(current,part)
  info=os.stat(current,follow_symlinks=False)
  need(stat.S_ISDIR(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022,CAP_ERR)
def runtime_metadata(value,machine):
 keys(value,'version architecture library_path library_sha256 libpq_version package_name package_version')
 need(uint(value['version'],1,True)==1,CAP_ERR)
 architecture={'aarch64':'arm64','x86_64':'amd64'}.get(machine)
 need(architecture is not None and value['architecture']==architecture,CAP_ERR)
 prefix={'arm64':'/usr/lib/aarch64-linux-gnu/','amd64':'/usr/lib/x86_64-linux-gnu/'}[architecture]
 path=text(value['library_path'],256); base=path.removeprefix(prefix)
 need(path.startswith(prefix) and base.startswith('libpq.so.5.') and base[11:].isascii() and base[11:].isdigit(),CAP_ERR)
 hextext(value['library_sha256'],64); need(180000<=uint(value['libpq_version'],189999,True)<190000,CAP_ERR)
 package=text(value['package_version'],256)
 need(value['package_name']=='libpq5' and package.isascii() and all(33<=ord(c)<=126 for c in package),CAP_ERR)
 return path
PTR=C.c_void_p
INT=C.c_int
STR=C.c_char_p
OID=C.c_uint
NOTICE=C.CFUNCTYPE(None,PTR,PTR)
class ConnOption(C.Structure):
 _fields_=[('keyword',STR),('envvar',STR),('compiled',STR),('val',STR),('label',STR),('dispchar',STR),('dispsize',INT)]
ABI={
'PQlibVersion':(INT,[]), 'PQconninfoParse':(C.POINTER(ConnOption),[STR,C.POINTER(PTR)]),
'PQconnectStartParams':(PTR,[C.POINTER(STR),C.POINTER(STR),INT]), 'PQconnectPoll':(INT,[PTR]),
'PQsetnonblocking':(INT,[PTR,INT]), 'PQsocket':(INT,[PTR]), 'PQstatus':(INT,[PTR]),
'PQtransactionStatus':(INT,[PTR]), 'PQbackendPID':(INT,[PTR]),
'PQsetNoticeReceiver':(PTR,[PTR,NOTICE,PTR]), 'PQresultErrorField':(STR,[PTR,INT]),
'PQsendQueryParams':(INT,[PTR,STR,INT,C.POINTER(OID),C.POINTER(STR),C.POINTER(INT),C.POINTER(INT),INT]),
'PQflush':(INT,[PTR]), 'PQconsumeInput':(INT,[PTR]), 'PQisBusy':(INT,[PTR]), 'PQgetResult':(PTR,[PTR]),
'PQresultStatus':(INT,[PTR]), 'PQcmdStatus':(STR,[PTR]), 'PQcmdTuples':(STR,[PTR]),
'PQntuples':(INT,[PTR]), 'PQnfields':(INT,[PTR]), 'PQgetisnull':(INT,[PTR,INT,INT]),
'PQgetlength':(INT,[PTR,INT,INT]), 'PQgetvalue':(PTR,[PTR,INT,INT]),
'PQclear':(None,[PTR]), 'PQfinish':(None,[PTR]), 'PQescapeIdentifier':(PTR,[PTR,STR,C.c_size_t]),
'PQfreemem':(None,[PTR]), 'PQconninfo':(C.POINTER(ConnOption),[PTR]),
'PQconninfoFree':(None,[C.POINTER(ConnOption)]), 'PQhost':(STR,[PTR]), 'PQhostaddr':(STR,[PTR]),
'PQport':(STR,[PTR]), 'PQdb':(STR,[PTR]), 'PQuser':(STR,[PTR]), 'PQsslInUse':(INT,[PTR]),
'PQparameterStatus':(STR,[PTR,STR])
}
def bind_abi(library,version):
 try:
  for name,(result,args) in ABI.items():
   function=getattr(library,name); function.argtypes=args; function.restype=result
  need(library.PQlibVersion()==version==180006,CAP_ERR)
  return library
 except (AttributeError,TypeError,ValueError): raise Refusal(CAP_ERR) from None
def load_runtime():
 path='/usr/share/rpgmigrate/python-libpq-runtime.json'; root_ancestors(path)
 metadata=StableFile(path,4096)
 try:
  info=os.fstat(metadata.fd)
  need(info.st_uid==0 and info.st_gid==0 and stat.S_IMODE(info.st_mode)==0o444,CAP_ERR)
  value=strict_json(metadata.raw,4096); library_path=runtime_metadata(value,os.uname().machine)
  root_ancestors(library_path); library=StableFile(library_path,64*1024*1024)
  try:
   info=os.fstat(library.fd)
   need(info.st_uid==0 and not info.st_mode&0o022,CAP_ERR)
   need(hashlib.sha256(library.raw).hexdigest()==value['library_sha256'],CAP_ERR)
   metadata.check(); library.check()
   loaded=bind_abi(C.CDLL(library_path),value['libpq_version'])
   library.check(); metadata.check(); root_ancestors(library_path); root_ancestors(path)
   return loaded
  finally: library.close()
 finally: metadata.close()
def parse_connection_options(library, raw: bytes) -> dict:
    need(type(raw) is bytes and b'\0' not in raw, CONN)
    error = PTR()
    options = None
    try:
        options = library.PQconninfoParse(raw, C.byref(error))
        need(options and not error.value, CONN)
        result = {}
        for index in range(256):
            entry = options[index]
            if entry.keyword is None:
                return result
            key = bytes(entry.keyword)
            need(key and key not in result, CAP_ERR)
            result[key] = tuple(None if value is None else bytes(value)
                                for value in (entry.val, entry.envvar, entry.compiled))
        raise Refusal(CAP_ERR)
    finally:
        if options:
            library.PQconninfoFree(options)
        if error.value:
            library.PQfreemem(error)


def selected_option(options: dict, keyword: bytes, envb: dict) -> bytes | None:
    need(keyword in options, CAP_ERR)
    explicit, environment, compiled = options[keyword]
    if explicit is not None:
        return explicit
    if environment is not None and environment in envb:
        return envb[environment]
    return compiled


def connection_home(envb: dict) -> bytes | None:
    home = envb.get(b'HOME')
    if home:
        need(type(home) is bytes and b'\0' not in home and len(home) < 1024, CAP_ERR)
        return home
    # NSS can perform network lookups; its image-specific policy is not qualified.
    raise Refusal(CAP_ERR)


class OpenHow(C.Structure):
    _fields_ = [('flags', C.c_uint64), ('mode', C.c_uint64), ('resolve', C.c_uint64)]


class LinuxStatFS(C.Structure):
    _fields_ = [('f_type', C.c_long), ('f_bsize', C.c_long),
                ('counts', C.c_ulong * 5), ('fsid', C.c_int * 2),
                ('f_namelen', C.c_long), ('f_frsize', C.c_long),
                ('f_flags', C.c_long), ('spare', C.c_long * 4)]


_FILE_API = None
# Kernel pseudo-files are not ordinary credential files, even when stat says regular.
_UNQUALIFIED_FILESYSTEMS = {0x9fa0, 0x62656572, 0x1cd1, 0x27e0eb, 0x63677270,
                           0x64626720, 0x74726163, 0x73636673, 0xcafe4a11,
                           0x6e736673, 0x09041934}


def file_api():
    global _FILE_API
    need(sys.platform == 'linux' and os.uname().machine in ('x86_64', 'aarch64'), CAP_ERR)
    need(C.sizeof(C.c_long) == 8 and C.sizeof(PTR) == 8 and
         C.sizeof(OpenHow) == 24 and C.sizeof(LinuxStatFS) == 120, CAP_ERR)
    if _FILE_API is None:
        try:
            library = C.CDLL(None, use_errno=True)
            library.syscall.argtypes = [C.c_long, INT, STR, C.POINTER(OpenHow), C.c_size_t]
            library.syscall.restype = C.c_long
            library.fstatfs.argtypes = [INT, C.POINTER(LinuxStatFS)]
            library.fstatfs.restype = INT
            _FILE_API = library
        except (AttributeError, OSError, TypeError):
            raise Refusal(CAP_ERR) from None
    return _FILE_API


def open_connection_file(path: bytes, directory: int) -> int:
    need(type(path) is bytes and path and b'\0' not in path, CAP_ERR)
    library = file_api()
    how = OpenHow(os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC, 0, 0x02)
    fd = library.syscall(437, directory, path, C.byref(how), C.sizeof(how))
    if fd < 0:
        code = C.get_errno()
        if code in (errno.ENOENT, errno.ENOTDIR):
            raise OSError(code, '')
        raise Refusal(CONN if code == errno.EACCES else CAP_ERR)
    try:
        info = os.fstat(fd)
        need(stat.S_ISREG(info.st_mode), CONN)
        filesystem = LinuxStatFS()
        need(library.fstatfs(fd, C.byref(filesystem)) == 0 and filesystem.f_type > 0 and
             filesystem.f_type not in _UNQUALIFIED_FILESYSTEMS, CAP_ERR)
        return fd
    except BaseException:
        checked_close(fd)
        raise


def original_file_permissions(info, keyword):
    need(stat.S_ISREG(info.st_mode), CONN)
    if keyword == b'passfile':
        need(not info.st_mode & 0o077, CONN)
    elif keyword == b'sslkey':
        forbidden = 0o037 if info.st_uid == 0 else 0o077
        need(not info.st_mode & forbidden, CONN)


ABSENT_FILE = b'/usr/share/rpgmigrate/.native-file-absent'


def stable_absence() -> bytes:
    try:
        root_ancestors(os.fsdecode(ABSENT_FILE))
        readonly = getattr(os, 'ST_RDONLY', None)
        need(readonly is not None and os.statvfs('/').f_flag & readonly and
             os.statvfs('/usr/share/rpgmigrate').f_flag & readonly, CAP_ERR)
        try:
            os.lstat(ABSENT_FILE)
        except OSError as error:
            need(error.errno in (errno.ENOENT, errno.ENOTDIR), CAP_ERR)
        else:
            raise Refusal(CAP_ERR)
        root_ancestors(os.fsdecode(ABSENT_FILE))
        return ABSENT_FILE
    except OSError:
        raise Refusal(CAP_ERR) from None


class SealedFile:
    def __init__(self, raw: bytes, deadline: float):
        self.fd = None
        self.original = None
        self.raw = raw
        self.deadline = deadline
        try:
            need(type(raw) is bytes and len(raw) <= 1048576, CAP_ERR)
            need(time.monotonic() < deadline, 'timeout')
            need(hasattr(os, 'memfd_create') and hasattr(os, 'MFD_CLOEXEC') and
                 hasattr(os, 'MFD_ALLOW_SEALING'), CAP_ERR)
            flags = os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING | getattr(os, 'MFD_NOEXEC_SEAL', 0)
            seals = ('F_SEAL_WRITE', 'F_SEAL_GROW', 'F_SEAL_SHRINK', 'F_SEAL_SEAL')
            need(all(hasattr(fcntl, name) for name in seals + ('F_ADD_SEALS', 'F_GET_SEALS')), CAP_ERR)
            self.seals = 0
            for name in seals:
                self.seals |= getattr(fcntl, name)
            self.fd = os.memfd_create('rpgmigrate-file', flags)
            self.original = identity(os.fstat(self.fd))
            at = 0
            while at < len(raw):
                need(time.monotonic() < deadline, 'timeout')
                count = os.write(self.fd, raw[at:])
                need(count > 0, CAP_ERR)
                at += count
            os.fchmod(self.fd, 0o400)
            os.lseek(self.fd, 0, 0)
            fcntl.fcntl(self.fd, fcntl.F_ADD_SEALS, self.seals)
            self.path = b'/proc/self/fd/' + str(self.fd).encode('ascii')
            self.check()
        except BaseException as error:
            self.close()
            if isinstance(error, Refusal):
                raise
            raise Refusal(CAP_ERR) from None

    def check(self) -> None:
        need(self.fd is not None, CAP_ERR)
        need(time.monotonic() < self.deadline, 'timeout')
        try:
            info = os.fstat(self.fd)
            need(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
                 stat.S_IMODE(info.st_mode) == 0o400 and info.st_nlink == 0 and
                 info.st_size == len(self.raw) and identity(info) == self.original, CAP_ERR)
            need(not os.get_inheritable(self.fd) and
                 fcntl.fcntl(self.fd, fcntl.F_GET_SEALS) & self.seals == self.seals, CAP_ERR)
            need(identity(os.stat(self.path)) == self.original, CAP_ERR)
            os.lseek(self.fd, 0, 0)
            need(read_bytes(self.fd, 1048576) == self.raw, CAP_ERR)
            os.lseek(self.fd, 0, 0)
        except OSError:
            raise Refusal(CAP_ERR) from None

    def close(self) -> None:
        if self.fd is not None:
            fd = self.fd
            self.fd = None
            try:
                need(self.original is None or identity(os.fstat(fd)) == self.original, CLOSE)
            except OSError:
                raise Refusal(CLOSE) from None
            checked_close(fd)


FILE_OPTIONS = (b'passfile', b'sslrootcert', b'sslcert', b'sslkey', b'sslcrl')
FILE_DEFAULTS = (b'.pgpass', b'.postgresql/root.crt', b'.postgresql/postgresql.crt',
                 b'.postgresql/postgresql.key', b'.postgresql/root.crl')


class ConnectionInput:
    def __init__(self, library, raw: bytes, envb: dict, materialization, deadline: float):
        self.raw = raw
        self.materialization = materialization
        self.deadline = deadline
        self.closed = False
        self.started = False
        self.snapshots = []
        self.overrides = {}
        self.absent = False
        try:
            need(not any(key in envb for key in (b'OPENSSL_CONF', b'OPENSSL_MODULES', b'OPENSSL_ENGINES')), CAP_ERR)
            options = parse_connection_options(library, raw)
            need(selected_option(options, b'service', envb) is None, CAP_ERR)
            tls = selected_option(options, b'sslmode', envb) != b'disable'
            if tls:
                need(not selected_option(options, b'sslcrldir', envb), CAP_ERR)
                need(selected_option(options, b'sslrootcert', envb) != b'system', CAP_ERR)
                key = selected_option(options, b'sslkey', envb)
                need(not key or b':' not in key, CAP_ERR)
            for keyword, default in zip(FILE_OPTIONS, FILE_DEFAULTS):
                if keyword == b'passfile':
                    if selected_option(options, b'password', envb):
                        continue
                elif not tls:
                    continue
                need(time.monotonic() < deadline, 'timeout')
                path = selected_option(options, keyword, envb)
                if not path:
                    home = connection_home(envb)
                    path = home + b'/' + default if home is not None else None
                    need(path is None or len(path) < 1024, CAP_ERR)
                if path is not None and keyword != b'passfile':
                    need(len(path) < 1024, CAP_ERR)
                if path is None:
                    original = None
                else:
                    need(type(path) is bytes and b'\0' not in path, CAP_ERR)
                    try:
                        original = materialization.pin(path)
                    except OSError as error:
                        if error.errno not in (errno.ENOENT, errno.ENOTDIR):
                            raise Refusal(CONN) from None
                        original = None
                if original is None:
                    self.overrides[keyword] = stable_absence()
                    self.absent = True
                else:
                    original_file_permissions(original.before, keyword)
                    original.check()
                    snapshot = SealedFile(original.raw, deadline)
                    self.snapshots.append(snapshot)
                    self.overrides[keyword] = snapshot.path
            need(len(self.snapshots) <= 5, CAP_ERR)
            self.keywords = (STR * (len(self.overrides) + 2))(b'dbname', *self.overrides, None)
            self.values = (STR * (len(self.overrides) + 2))(raw, *self.overrides.values(), None)
            self.check()
        except BaseException:
            self.close()
            raise

    def check(self) -> None:
        need(not self.closed, CAP_ERR)
        need(time.monotonic() < self.deadline, 'timeout')
        self.materialization.check()
        if self.absent:
            stable_absence()
        for snapshot in self.snapshots:
            snapshot.check()

    def claim(self) -> tuple:
        need(not self.started, CAP_ERR)
        self.check()
        self.started = True
        return self.keywords, self.values

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        failed = False
        for snapshot in reversed(self.snapshots):
            try:
                snapshot.close()
            except BaseException:
                failed = True
        self.snapshots = []
        self.keywords = self.values = None
        need(not failed, CLOSE)


class Diagnostics:
 def __init__(self):
  self.fd,self.writer=os.pipe2(os.O_NONBLOCK|os.O_CLOEXEC)
  self.dirty=False; self.eof=False
  os.dup2(self.writer,2); checked_close(self.writer); self.writer=2
 def check(self):
  if self.eof: need(not self.dirty,'diagnostic'); return
  while True:
   try: raw=os.read(self.fd,8192)
   except BlockingIOError: break
   if not raw: self.eof=True; break
   self.dirty=True; break
  need(not self.dirty,'diagnostic')
 def close(self):
  if self.writer is not None:
   writer=self.writer; self.writer=None; checked_close(writer)
  try:
   self.check(); need(self.eof,CLOSE)
  finally:
   fd=self.fd; self.fd=None; checked_close(fd)
class Session:
 def __init__(self,library,connection:ConnectionInput,deadline:float,diag) -> None:
  self.lib=library; self.conn=None; self.connection=connection; self.claimed=False; self.deadline=deadline; self.diag=diag
  self.notice=False; self.sqlstate=''; self.commit_dispatched=False; self.commit_ack=False
  self.callback=NOTICE(self.receive_notice)
  try:
   keywords,values=connection.claim(); self.claimed=True
   self.conn=library.PQconnectStartParams(keywords,values,1)
   need(self.conn,CONN)
   library.PQsetNoticeReceiver(self.conn,self.callback,None)
   while True:
    self.check(True)
    state=library.PQconnectPoll(self.conn)
    if state==3: break
    need(state in (1,2),CONN)
    self.wait(state==2,True)
   need(library.PQsetnonblocking(self.conn,1)==0,CONN)
   self.check(); need(library.PQtransactionStatus(self.conn)==0,TX_STATE)
   self.backend=library.PQbackendPID(self.conn); need(self.backend>0,CONN)
   self.props=self.connection_facts()
   need(all(self.props.get(key)==value for key,value in connection.overrides.items()),CONN)
  except BaseException:
   self.close(False); raise
 def receive_notice(self,_argument,result):
  self.notice=True
  try:
   raw=self.lib.PQresultErrorField(result,ord('C'))
   state=bytes(raw).decode('ascii') if raw else ''
   if len(state)==5 and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789' for c in state): self.sqlstate=state
  except BaseException: self.sqlstate=''
 def check(self,connecting=False):
  need(time.monotonic()<self.deadline,'timeout'); self.diag.check(); self.connection.check()
  need(not self.notice,'diagnostic')
  need(self.conn and (connecting or self.lib.PQstatus(self.conn)==0),CONN)
 def wait(self,writing=False,connecting=False):
  fd=self.lib.PQsocket(self.conn); need(fd>=0,CONN)
  while not wait_fd(fd,writing,self.deadline,self.diag): self.check(connecting)
 def connection_facts(self):
  values={}
  for name in ('PQhost','PQhostaddr','PQport','PQdb','PQuser'):
   raw=getattr(self.lib,name)(self.conn)
   need(raw is not None,CONN); values[name]=bytes(raw)
  need(values['PQhost'] and values['PQport'] and values['PQdb'] and values['PQuser'],CONN)
  options=self.lib.PQconninfo(self.conn); need(options,CONN)
  try:
   for index in range(256):
    entry=options[index]
    if not entry.keyword: break
    key=bytes(entry.keyword); need(key not in values,CONN)
    values[key]=None if entry.val is None else bytes(entry.val)
   else: raise Refusal(CONN)
  finally: self.lib.PQconninfoFree(options)
  need(values.get(b'client_encoding') in (None,b'',b'UTF8',b'UTF-8'),CONN)
  need(self.lib.PQparameterStatus(self.conn,b'client_encoding')==b'UTF8',CONN)
  need(self.lib.PQparameterStatus(self.conn,b'server_encoding')==b'UTF8',CONN)
  values['ssl']=self.lib.PQsslInUse(self.conn)
  return values
 def quote(self,value):
  raw=value.encode('utf-8'); pointer=self.lib.PQescapeIdentifier(self.conn,raw,len(raw)); need(pointer,RESULT)
  try: return C.string_at(pointer).decode('utf-8')
  finally: self.lib.PQfreemem(pointer)
 def query(self,sql,params=(),tag='SELECT 1',state=0,rows=1,width=1,commit=False):
  self.check(); before=0 if tag=='BEGIN' else 2 if commit else state
  need(self.lib.PQtransactionStatus(self.conn)==before,TX_STATE)
  encoded=[None if value is None else str(value).encode('utf-8') for value in params]
  need(not any(value is not None and b'\0' in value for value in encoded),RESULT)
  arguments=(STR*len(encoded))(*encoded)
  results=[]; failure=None
  try:
   if commit: self.commit_dispatched=True
   sent=self.lib.PQsendQueryParams(self.conn,sql.replace('@','pg_catalog.').encode('utf-8'),len(encoded),None,arguments,None,None,0)
   need(sent==1,CONN)
   while True:
    self.check(); pending=self.lib.PQflush(self.conn); need(pending in (0,1),CONN)
    if not pending: break
    self.wait(True)
   while True:
    self.check()
    need(self.lib.PQconsumeInput(self.conn)==1,CONN)
    if self.lib.PQisBusy(self.conn): self.wait(); continue
    result=self.lib.PQgetResult(self.conn)
    if not result: break
    try:
     status=self.lib.PQresultStatus(result)
     raw_state=self.lib.PQresultErrorField(result,ord('C'))
     sqlstate=bytes(raw_state).decode('ascii') if raw_state else ''
     if status not in (1,2):
      failure=Refusal(RESULT,sqlstate); continue
     command=self.lib.PQcmdStatus(result); count=self.lib.PQcmdTuples(result)
     need(command is not None and count is not None,RESULT)
     nr=self.lib.PQntuples(result); nf=self.lib.PQnfields(result)
     need(0<=nr<=2 and 0<=nf<=29,RESULT)
     copied=[]; size=0
     for r in range(nr):
      row=[]
      for f in range(nf):
       null=self.lib.PQgetisnull(result,r,f); need(null in (0,1),RESULT)
       if null: row.append(None); continue
       length=self.lib.PQgetlength(result,r,f); need(0<=length<=262144,RESULT)
       pointer=self.lib.PQgetvalue(result,r,f); need(pointer,RESULT)
       size+=length; need(size<=300000,RESULT)
       value=C.string_at(pointer,length); value.decode('utf-8'); need(b'\0' not in value,RESULT); row.append(value)
      copied.append(row)
     need(len(results)<2,RESULT); results.append((status,bytes(command),bytes(count),copied,nf))
    except (Refusal,UnicodeError) as error:
     failure=error if isinstance(error,Refusal) else Refusal(RESULT)
    finally: self.lib.PQclear(result)
   self.check()
   if failure: raise failure
   need(len(results)==1,RESULT)
   status,command,count,copied,nf=results[0]
   need(command==tag.encode() and status==(2 if rows else 1),RESULT)
   expected_count=b'1' if tag in ('SELECT 1','UPDATE 1') else b''
   need(len(copied)==rows and nf==width and count==expected_count,RESULT)
   need(self.lib.PQtransactionStatus(self.conn)==state,TX_STATE)
   need(self.lib.PQbackendPID(self.conn)==self.backend and self.connection_facts()==self.props,CONN)
   if commit: self.commit_ack=True
   return copied
  except BaseException as error:
   if self.commit_dispatched and not self.commit_ack:
    raise Refusal('unknown_commit',error.sqlstate if isinstance(error,Refusal) else '') from None
   raise
 def close(self,healthy=True):
  pointer=self.conn; self.conn=None; claimed=self.claimed; self.claimed=False; failure=None
  if pointer:
   try:
    if healthy:
     need(self.lib.PQstatus(pointer)==0 and self.lib.PQtransactionStatus(pointer)==0,CLOSE)
     need(not self.notice,'diagnostic'); self.diag.check()
   except BaseException as error: failure=error
   try: self.lib.PQfinish(pointer)
   except BaseException: failure=Refusal(CLOSE)
  if claimed:
   try: self.connection.close()
   except BaseException: failure=Refusal(CLOSE)
  self.diag.check()
  if failure: raise failure
def nonce(value):
 need(type(value) is list and len(value)==16)
 for item in value: uint(item,255)
 need(any(value)); return bytes(value)
def name(value): return text(value,63)
def source_identity(value):
 keys(value,'system_identifier database_oid'); uint(value['system_identifier'],U64,True); uint(value['database_oid'],(1<<32)-1,True)
def stream(value):
 keys(value,'run source generation'); nonce(value['run']); source_identity(value['source']); need(uint(value['generation'],U64,True)==1)
def qualified(value):
 keys(value,'schema table'); name(value['schema']); name(value['table'])
def bounded_array(value,cap):
 need(type(value) is list and len(value)<=cap); return value
def slot_record(value):
 keys(value,'intent consistent_point snapshot_token'); keys(value['intent'],'binding nonce'); nonce(value['intent']['nonce'])
 binding=value['intent']['binding']; keys(binding,'stream slot_name plugin catalog_epoch'); stream(binding['stream'])
 slot=name(binding['slot_name']); need(all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in slot) and binding['plugin']=='pgoutput')
 uint(binding['catalog_epoch'],U64,True); uint(value['consistent_point'],U64,True); text(value['snapshot_token'],1024)
def publication(value):
 keys(value,'name oid owner_oid'); name(value['name']); uint(value['oid'],(1<<32)-1,True); uint(value['owner_oid'],(1<<32)-1,True)
def table_plan(value):
 keys(value,'source_oid name writable_columns all_columns identity_modes partition_identity_modes')
 uint(value['source_oid'],(1<<32)-1,True); qualified(value['name'])
 for key in ('all_columns','writable_columns'):
  columns=bounded_array(value[key],1600)
  for column in columns: name(column)
  need(len(set(columns))==len(columns))
 need(set(value['writable_columns'])<=set(value['all_columns']))
 for key in ('identity_modes','partition_identity_modes'):
  if key=='partition_identity_modes' and value[key] is None: continue
  modes=bounded_array(value[key],1600); need(len(modes)==len(value['all_columns']))
  for mode in modes: need(uint(mode,255) in (0,97,100))
def validate_manifest(manifest,req,dir_info):
 m=manifest; keys(m,'magic format_version run canonical_path directory_device directory_inode copy_complete target_provenance source_slot clone_state follow_state operator')
 need(m['magic']=='RPGMIGRATE' and uint(m['format_version'],8)==8 and m['canonical_path']=='/work/pgcopydb')
 run=nonce(m['run']); work=req['execution']['work']
 need(run.hex()==work['run_id'])
 need((uint(m['directory_device']),uint(m['directory_inode']))==identity(dir_info))
 operator=m['operator']; keys(operator,'migration_uid run workdir_id checkpoint_generation')
 need(same(operator,dict(migration_uid=work['migration_uid'],run=list(run),workdir_id=work['workdir_id'],checkpoint_generation=1)))
 uint(operator['checkpoint_generation'],1,True)
 if m['copy_complete'] is not None:
  keys(m['copy_complete'],'tables rows bytes')
  for count in m['copy_complete'].values(): uint(count)
 targets=bounded_array(m['target_provenance'],1); need(len(targets)==1)
 keys(targets[0],'phase record'); need(targets[0]['phase']=='confirmed'); target=targets[0]['record']
 keys(target,'intent schema_oid table_oid marker_table_oid marker_index_oid'); keys(target['intent'],'binding nonce'); nonce(target['intent']['nonce'])
 binding=target['intent']['binding']; keys(binding,'stream target_system_identifier target_database_oid schema_name table_name owner_oid metadata_version')
 stream(binding['stream']); need(binding['stream']['run']==m['run'])
 need(uint(binding['metadata_version'],6,True)==6 and binding['schema_name']=='_rpgmigrate_'+work['run_id'] and binding['table_name']=='checkpoint')
 name(binding['schema_name']); name(binding['table_name']); uint(binding['target_system_identifier'],U64,True)
 for key in ('target_database_oid','owner_oid'): uint(binding[key],(1<<32)-1,True)
 for key in ('schema_oid','table_oid','marker_table_oid','marker_index_oid'): uint(target[key],(1<<32)-1,True)
 need(len({target[k] for k in ('table_oid','marker_table_oid','marker_index_oid')})==3)
 keys(m['source_slot'],'phase record'); need(m['source_slot']['phase']=='confirmed'); slot_record(m['source_slot']['record'])
 need(m['source_slot']['record']['intent']['binding']['stream']==binding['stream'])
 clone=m['clone_state']; keys(clone,'plan ticket phase owned_objects owned_tables completion'); need(clone['phase']=='analyzing' and clone['completion'] is None)
 plan=clone['plan']; keys(plan,'source target generation tables sequences policy')
 keys(plan['source'],'identity timeline major'); keys(plan['target'],'identity major')
 source_identity(plan['source']['identity']); source_identity(plan['target']['identity'])
 need(plan['source']['identity']==binding['stream']['source'] and plan['target']['identity']==dict(system_identifier=binding['target_system_identifier'],database_oid=binding['target_database_oid']))
 uint(plan['source']['timeline'],(1<<32)-1,True); uint(plan['generation'],U64,True)
 for side in ('source','target'): need(uint(plan[side]['major'],18,True) in (16,18))
 need(plan['source']['major']<=plan['target']['major'])
 tables=bounded_array(plan['tables'],4096); oids=set(); names=set()
 for table in tables:
  table_plan(table); identifier=(table['name']['schema'],table['name']['table'])
  need(table['source_oid'] not in oids and identifier not in names); oids.add(table['source_oid']); names.add(identifier)
 for sequence in bounded_array(plan['sequences'],65536): qualified(sequence)
 keys(plan['policy'],'no_owner no_acl no_comments no_tablespaces')
 for flag in plan['policy'].values(): boolean(flag)
 keys(clone['ticket'],'token source generation'); text(clone['ticket']['token'],1024)
 need(same(clone['ticket']['source'],plan['source']) and same(clone['ticket']['generation'],plan['generation']))
 objects=bounded_array(clone['owned_objects'],65536); object_ids=set()
 for obj in objects:
  keys(obj,'class_oid object_oid object_sub_id namespace_oid namespace_name name kind owner_oid')
  for key in ('class_oid','object_oid','owner_oid'): uint(obj[key],(1<<32)-1,True)
  uint(obj['object_sub_id'],(1<<31)-1); text(obj['name'],1048576); text(obj['kind'],256)
  if obj['namespace_oid'] is not None: need(uint(obj['namespace_oid'],(1<<32)-1,True)!=target['schema_oid'])
  if obj['namespace_name'] is not None: text(obj['namespace_name'],1048576,True)
  need(obj['class_oid']!=2615 or obj['object_oid']!=target['schema_oid'])
  identifier=tuple(obj[k] for k in ('class_oid','object_oid','object_sub_id')); need(identifier not in object_ids); object_ids.add(identifier)
 owned=bounded_array(clone['owned_tables'],4096); need(len(owned)==len(tables)); targets_seen=set(); sources_seen=set()
 for obj in owned:
  keys(obj,'table target_oid target_namespace_oid target_owner_oid generation'); table_plan(obj['table'])
  for key in ('target_oid','target_namespace_oid','target_owner_oid'): uint(obj[key],(1<<32)-1,True)
  need(same(obj['generation'],plan['generation']) and obj['table'] in tables)
  need(obj['target_oid'] not in targets_seen and obj['table']['source_oid'] not in sources_seen)
  targets_seen.add(obj['target_oid']); sources_seen.add(obj['table']['source_oid'])
  need(any(o['class_oid']==1259 and o['object_oid']==obj['target_oid'] and o['object_sub_id']==0 and o['namespace_oid']==obj['target_namespace_oid'] and o['namespace_name']==obj['table']['name']['schema'] and o['name']==obj['table']['name']['table'] and o['kind']=='r' and o['owner_oid']==obj['target_owner_oid'] for o in objects))
 follow=m['follow_state']; keys(follow,'cleanup origin_name publication binding restart preliminary')
 need(follow['cleanup'] is None and follow['restart'] is None and follow['preliminary'] is None)
 if follow['origin_name'] is not None: name(follow['origin_name'])
 fb=follow['binding']; keys(fb,'slot publication source_timeline copy_generation source_types'); slot_record(fb['slot']); publication(fb['publication'])
 uint(fb['source_timeline'],(1<<32)-1,True); uint(fb['copy_generation'],U64,True)
 need(fb['slot']==m['source_slot']['record'] and fb['copy_generation']==plan['generation'] and fb['source_timeline']==plan['source']['timeline'])
 need(clone['ticket']['token']==fb['slot']['snapshot_token'])
 types=bounded_array(fb['source_types'],16384); types_seen=set()
 for item in types:
  keys(item,'oid schema name'); uint(item['oid'],(1<<32)-1,True); name(item['schema']); name(item['name'])
  need(item['oid'] not in types_seen); types_seen.add(item['oid'])
 pub=follow['publication']; keys(pub,'phase intent receipt'); need(pub['phase']=='confirmed')
 keys(pub['intent'],'run name nonce'); nonce(pub['intent']['nonce'])
 need(same(pub['intent']['run'],m['run']) and pub['intent']['name']==fb['publication']['name'] and same(pub['receipt'],fb['publication']))
 need(len(json.dumps(fb,separators=(',',':'),ensure_ascii=False).encode())<=262144)
 need(len(json.dumps(types,separators=(',',':'),ensure_ascii=False).encode())+8192<=262144)
 need(len(json.dumps(dict(binding=fb,operator=operator,origin_name=follow['origin_name']),separators=(',',':'),ensure_ascii=False).encode())<=262144)
 need(len(json.dumps(clone,separators=(',',':'),ensure_ascii=False).encode())<=1048576-4096)
 return target
def validate_control(value,manifest,req):
 keys(value,'version stream copy_generation slot_nonce target_nonce end_lsn sealed')
 need(uint(value['version'],1,True)==1)
 target=manifest['target_provenance'][0]['record']
 need(same(value['stream'],target['intent']['binding']['stream']) and value['copy_generation']==manifest['clone_state']['plan']['generation'])
 uint(value['copy_generation'],U64,True)
 nonce(value['slot_nonce']); nonce(value['target_nonce'])
 need(value['slot_nonce']==manifest['source_slot']['record']['intent']['nonce'] and value['target_nonce']==target['intent']['nonce'])
 need(uint(value['end_lsn'],U64,True)==lsn(req['saved_e']) and boolean(value['sealed']))
class PrivateState:
 def __init__(self,req):
  self.req=req; self.fds=[]; self.files=[]; self.locked=False
  try:
   self.parent=self.open('/work',True)
   self.root=self.open('pgcopydb',True,self.parent)
   fcntl.flock(self.root,fcntl.LOCK_EX|fcntl.LOCK_NB)
   self.control=self.open('control',True,self.root)
   self.lock=self.open('lock',False,self.control)
   self.lock_control()
   self.entries()
   self.manifest_file=StableFile('manifest.json',1048576,True,self.root); self.files.append(self.manifest_file)
   self.end_file=StableFile('endpos.json',4096,True,self.control); self.files.append(self.end_file)
   self.manifest=strict_json(self.manifest_file.raw,1048576)
   self.target=validate_manifest(self.manifest,req,os.fstat(self.root))
   validate_control(strict_json(self.end_file.raw,4096),self.manifest,req)
   self.unlock_control(); self.revalidate()
  except BaseException:
   self.close(False); raise
 def open(self,path,directory,parent=None):
  fd=fdopen(path,directory,parent); self.fds.append(fd); private_type(fd,directory)
  return fd
 def entries(self):
  need(sorted(os.listdir(self.root))==['control','manifest.json'])
  need(sorted(os.listdir(self.control))==['endpos.json','lock'])
 def lock_control(self):
  fcntl.flock(self.lock,fcntl.LOCK_EX|fcntl.LOCK_NB); self.locked=True
 def unlock_control(self):
  if self.locked:
   fcntl.flock(self.lock,fcntl.LOCK_UN); self.locked=False
 def revalidate(self):
  need(identity(private_type(self.parent,True))==identity(os.stat('/work',follow_symlinks=False)))
  for fd,parent,path,directory in [(self.root,self.parent,'pgcopydb',True),(self.control,self.root,'control',True),(self.lock,self.control,'lock',False)]:
   need(identity(private_type(fd,directory))==identity(os.stat(path,dir_fd=parent,follow_symlinks=False)))
  self.lock_control()
  try:
   self.entries()
   for file in self.files: file.check()
  finally: self.unlock_control()
 def resources(self):
  follow=self.manifest['follow_state']; slot=self.manifest['source_slot']['record']['intent']['binding']['slot_name']
  result=[dict(kind='replication_slot',name=slot,owned=True,retain=False),dict(kind='publication',name=follow['binding']['publication']['name'],owned=True,retain=False),dict(kind='workdir',name='/work/pgcopydb',owned=True,retain=True)]
  if follow['origin_name'] is not None: result.append(dict(kind='replication_origin',name=follow['origin_name'],owned=False,retain=False))
  return result
 def close(self,recheck=True):
  failed=False
  if recheck and self.fds:
   try: self.revalidate()
   except BaseException: failed=True
  try: self.unlock_control()
  except BaseException: failed=True
  for file in reversed(self.files):
   try: file.close()
   except BaseException: failed=True
  self.files=[]
  for fd in reversed(self.fds):
   try: checked_close(fd)
   except BaseException: failed=True
  self.fds=[]; need(not failed,CLOSE)
def decimal(raw,maximum=U64,positive=False):
 need(type(raw) is str and raw.isascii() and raw.isdecimal() and (raw=='0' or raw[0]!='0'),RESULT)
 return uint(int(raw),maximum,positive)
def validate_control_row(row,state):
 need(type(row) is list and len(row)==29,RESULT)
 for cell in row:
  if cell is not None: text(cell,262144,True)
 m=state.manifest; target=state.target; b=target['intent']['binding']; slot=m['source_slot']['record']; plan=m['clone_state']['plan']
 want=['rpgmigrate','6',bytes(m['run']).hex(),bytes(target['intent']['nonce']).hex()]+[str(n) for n in (b['stream']['source']['system_identifier'],b['stream']['source']['database_oid'],b['stream']['generation'],b['target_system_identifier'],b['target_database_oid'],b['owner_oid'],target['schema_oid'],target['table_oid'])]
 need(row[:12]==want,RESULT); order=decimal(row[12]); applied=0
 if not order: need(all(cell is None for cell in row[13:17]),RESULT)
 else:
  decimal(row[13],(1<<32)-1); begin=decimal(row[14],U64,True); commit=lsn(row[15]); applied=lsn(row[16]); need(0<commit==begin<=applied,RESULT)
 epoch=decimal(row[17]); counter=decimal(row[18],U64,True)
 need(row[19]==slot['snapshot_token'] and len(row[19].encode())<=1024,RESULT)
 anchor=lsn(row[20]); need(anchor>0 and anchor==slot['consistent_point'] and row[21]==bytes(slot['intent']['nonce']).hex(),RESULT)
 clone=strict_json(row[22].encode(),4096); keys(clone,'generation phase snapshot_token source_timeline table_count')
 need(clone==dict(generation=plan['generation'],phase='analyzing',snapshot_token=m['clone_state']['ticket']['token'],source_timeline=plan['source']['timeline'],table_count=len(plan['tables'])),RESULT)
 for key in ('generation','source_timeline','table_count'): uint(clone[key])
 certified=lsn(row[23]) if row[23] is not None else 0
 need(row[23] is None or certified>0,RESULT)
 follow=strict_json(row[24].encode(),262144); keys(follow,'binding operator origin_name')
 need(same(follow,dict(binding=m['follow_state']['binding'],operator=m['operator'],origin_name=m['follow_state']['origin_name'])),RESULT)
 end=lsn(row[25]); witness=lsn(row[26]); kind=row[27]
 need(end==lsn(state.req['saved_e']) and 0<anchor<=end<=witness and kind in ('keepalive','later_begin'),RESULT)
 need(certified>=witness if kind=='keepalive' else witness>end,RESULT)
 need(row[28]=='t',RESULT)
 return dict(checkpoint_id='1:%d:%d'%(epoch,counter),certified_lsn=lsn_text(certified),durable_replay_lsn=lsn_text(max(applied,anchor,certified)),drain_end_lsn=lsn_text(end),drain_replay_lsn=lsn_text(witness),drain_kind=kind,barrier_confirmed=True)
# Generated from the pinned native-8 metadata fixtures; tests verify the source hashes.
CATALOG_FRAGMENTS=[('~a', 'SELECT 1 FROM pg_catalog.'), ('~b', '::pg_catalog.'), ('~c', ' AND NOT EXISTS ('), ('~d', ' FROM pg_catalog'), ('~e', ' AND pg_catalog.has_table_privilege(c'), ('~f', ' JOIN pg_catalog'), ('~g', "dep.classid = 'pg_catalog"), ('~h', ' AND NOT EXISTS(~'), ('~i', '.indisexclusion AND i.indimmediate AND i.indisvalid AND i.indisready AND i.indislive AND i.indnatts'), ('~j', ' NOT p.condeferrable AND NOT p.condeferred AND p.convalidated AND p.conislocal AND p.coninhcount'), ('~k', "', 1700, 1310724,"), ('~l', "', 3220, -1, false)"), ('~m', "(SELECT oid~d.pg_opclass WHERE opcnamespace='pg_catalog'~bregnamespace AND opcname='"), ('~n', "pg_type'~bregclass AND (dep.objid IN (c.reltype, rowtype.typarray) OR dep.objid IN"), ('~o', ' AND a.attacl IS NULL AND a.attoptions IS NULL AND a.attfdwoptions IS NULL AND'), ('~p', 'WHERE t.typrelid=$7::oid OR t.oid IN (SELECT typarray~d.pg_type WHERE typrelid'), ('~q', ' a.attisdropped AND NOT a.atthasdef AND a.attislocal AND a.attinhcount'), ('~r', ' (c.oid, idx.oid, $7::oid, $8::oid'), ('~s', "pg_class'~bregclass AND dep.objid IN~r)) OR (~g.~n (SELECT t.oid"), ('~t', "', 26, -1, true)"), ('~u', 'apg_constraint con WHERE con.'), ('~v', ' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity'), ('~w', ' i.indexprs IS NULL AND i.indpred IS NULL AND i.indkey'), ('~x', "', 25, -1, false)"), ('~y', 'acl.grantee <> writer.oid OR acl.privilege_type'), ('~z', ' AND i.indisunique AND i.indisprimary AND NOT'), ('~A', '~bint2vector AND'), ('~B', ') AND (SELECT count(*)~'), ('~C', 'pg_catalog.acldefault('), ('~D', 'aaclexplode(COALESCE('), ('~E', '.relnamespace = n.oid'), ('~F', '.relam=(SELECT oid~d.pg_am WHERE amname='), ('~G', ' AND opcmethod=idx.relam AND opcdefault'), ('~H', "',1700,1310724,true)"), ('~I', ' AND EXISTS (~apg_am am WHERE am.oid'), ('~J', '.relowner AND'), ('~K', ' NOT c.relisshared AND c.reloftype'), ('~L', ' THEN \'pg_catalog."C"\'~bregcollation'), ('~M', '~Bd.pg_index ix WHERE ix.indrelid'), ('~N', '.oid AND a.attnum'), ('~O', '~f.pg_constraint p ON p.conrelid'), ('~P', '(~g.~s~d.pg_type t ~p=$7::oid)))'), ('~Q', "pg_namespace'~bregclass AND dep"), ('~R', "0 0'~boidvector AND i.indoption"), ('~S', ' CASE key.position WHEN 1 THEN'), ('~T', '))~c~apg_depend dep WHERE dep'), ('~U', '.is_grantable))'), ('~V', 'pg_attribute a '), ('~W', '.modifier AND a.attnotnull'), ('~X', '))) acl WHERE acl.grantor '), ('~Y', '.oid~f.pg_class idx ON idx'), ('~Z', ' AND NOT c.relispartition'), ('~0', 'SELECT 1 FROM (VALUES (1,'), ('~1', ' i.indexrelid'), ('~2', ".relam AND am.amname = '")]
CATALOG_16="SELECT n.nspowner = $4~boid AND c.relowner = $4~boid AND n.nspname = $5~btext AND c.relname = $6~btext AND EXISTS (~apg_database d WHERE d.datname = pg_catalog.current_database() AND d.oid = $3~boid) AND c.relkind = 'r' AND c.relpersistence = 'p'~I = c~2heap' AND am.oid < 16384)~Z~v AND~K = 0 AND c.relnatts = 29 AND c.relchecks = 0~c~apg_policy policy WHERE policy.polrelid = c.oid)~c~apg_trigger t WHERE t.tgrelid = c.oid)~c~apg_rewrite r WHERE r.ev_class = c.oid)~c~apg_inherits h WHERE h.inhrelid = c.oid OR h.inhparent = c.oid)~c~apg_foreign_table f WHERE f.ftrelid = c.oid) AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')~e.oid, 'SELECT')~e.oid, 'INSERT')~e.oid, 'UPDATE')~c ~~Dn.nspacl, ~C'n', n.nspowner~X<> n.nspowner OR (acl.grantee <> n.nspowner AND (~y <> 'USAGE' OR acl~U)~c ~~Dc.relacl, ~C'r', c.relowner~X<> c.relowner OR (acl.grantee <> c~J (~y NOT IN ('SELECT', 'INSERT', 'UPDATE') OR acl~U~Bd.~VWHERE a.attrelid = c~N > 0) = 29~c ~0 'magic', 25, -1, true), (2, 'version', 23, -1, true), (3, 'run_id', 17, -1, true), (4, 'nonce', 17, -1, true), (5, 'source_system~k true), (6, 'source_database~t, (7, 'generation~k true), (8, 'target_system~k false), (9, 'target_database~t, (10, 'owner_oid~t, (11, 'schema_oid~t, (12, 'table_oid~t, (13, 'applied_order~k true), (14, 'last_xid', 26, -1, false), (15, 'begin_ordinal~k false), (16, 'source_commit_lsn~l, (17, 'source_end_lsn~l, (18, 'barrier_epoch~k true), (19, 'barrier_counter~k true), (20, 'copy_snapshot_token~x, (21, 'copy_consistent_point~l, (22, 'copy_plan_nonce', 17, -1, false), (23, 'clone_state~x, (24, 'certified_lsn~l, (25, 'follow_binding~x, (26, 'drain_end_lsn~l, (27, 'drain_witness_lsn~l, (28, 'drain_kind~x, (29, 'follow_completed', 16, -1, true)) expected(num, name, type_oid, modifier, required) LEFT~f.~VON a.attrelid = c~N = expected.num WHERE NOT COALESCE(a.attname = expected.name AND a.atttypid = expected.type_oid~boid AND a.atttypmod = expected~W = expected.required AND NOT~q = 0 AND a.attndims = 0~o a.attcollation = CASE WHEN a.atttypid = 25~L~boid ELSE 0 END AND a.attidentity = '' AND a.attgenerated = '', false))~c~apg_attrdef a WHERE a.adrelid = c.oid) AND p.contype = 'p' AND p.connamespace = n.oid AND~j = 0 AND p.contypid = 0 AND p.confrelid = 0 AND p.conbin IS NULL AND p.conkey = ARRAY[3,5,6,7]~bint2[]~c~~uconrelid = c.oid AND NOT (con.oid = p.oid )~M = c.oid) = 1 AND idx.relkind = 'i' AND idx.relpersistence = 'p' AND idx~E AND idx.relowner = c~J idx.relacl IS NULL~I = idx~2btree' AND am.oid < 16384)~z i~i = 4 AND i.indnkeyatts = 4 AND~w = '3 5 6 7'~A i.indcollation = '0 0 ~R = '0 0 0 0'~A pg_catalog.cardinality(i.indclass~boid[]) = 4~c ~aunnest(i.indclass~boid[]) WITH ORDINALITY key(opclass, position) LEFT~f.pg_opclass op ON op.oid = key.opclass WHERE NOT COALESCE(op.oid < 16384 AND op.opcnamespace = 'pg_catalog'~bregnamespace AND op.opcmethod = idx.relam AND op.opcdefault AND op.opckeytype = 0 AND op.opcname =~S 'bytea_ops' WHEN 3 THEN 'oid_ops' ELSE 'numeric_ops' END AND op.opcintype =~S 17 WHEN 3 THEN 26 ELSE 1700 END~boid, false))~c~apg_class other WHERE other~E AND other.oid NOT IN~r~T.refclassid = 'pg_catalog.~Q.refobjid = n.oid AND NOT (~P) OR (~g.pg_constraint'~bregclass AND EXISTS (~~uoid = dep.objid AND con.conrelid IN (c.oid,$7::oid)))~T.deptype = 'e' AND ((~g.~Q.objid = n.oid) OR ~P)))~d.pg_namespace n~f.pg_roles writer ON writer.rolname = CURRENT_USER~f.pg_class c ON c~E~f.pg_type rowtype ON rowtype.oid = c.reltype~O = c.oid AND p.contype = 'p'~f.pg_index i ON~1 = p.conindid AND i.indrelid = c~Y.oid =~1 WHERE n.oid = $1~boid AND c.oid = $2~boid;"
MARKER_16="SELECT c.relnamespace=$3::oid AND c.relowner=$4::oid AND c.relname='copy_done' AND c.relkind='r' AND c.relpersistence='p' AND c~F'heap')~v~Z AND~K=0 AND c.relnatts=11 AND c.relchecks=0~e.oid,'SELECT')~e.oid,'INSERT')~e.oid,'DELETE')~h~Dc.relacl,~C'r',c.relowner))) a WHERE a.grantor<>c.relowner OR (a.grantee<>c~J (a.grantee<>(SELECT oid~d.pg_roles WHERE rolname=CURRENT_USER) OR a.privilege_type NOT IN('SELECT','INSERT','DELETE') OR a~U)~ha~VWHERE a.attrelid=c~N>0 AND NOT EXISTS(~0'magic',25,-1,true),(2,'version',23,-1,true),(3,'run_id',17,-1,true),(4,'nonce',17,-1,true),(5,'source_table',26,-1,true),(6,'target_table',26,-1,true),(7,'copy_generation~H,(8,'rows~H,(9,'bytes~H,(10,'target_namespace',26,-1,true),(11,'target_owner',26,-1,true)) e(num,name,ty,modifier,required) WHERE a.attnum=e.num AND a.attname=e.name AND a.atttypid=e.ty::oid AND a.atttypmod=e~W=e.required AND NOT~q=0 AND a.attndims=0~o a.attidentity='' AND a.attgenerated='' AND a.attcollation=CASE WHEN a.atttypid=25~L::oid ELSE 0 END))~hapg_attrdef a WHERE a.adrelid=c.oid)~hapg_trigger t WHERE t.tgrelid=c.oid)~hapg_policy p WHERE p.polrelid=c.oid)~hapg_rewrite r WHERE r.ev_class=c.oid)~hapg_inherits h WHERE h.inhrelid=c.oid OR h.inhparent=c.oid~M=c.oid)=1 AND~1=$2::oid~z i~i=2 AND i.indnkeyatts=2 AND~w='7 5'~A i.indcollation='~R='0 0'~A idx.relkind='i' AND idx.relnamespace=c.relnamespace AND idx.relowner=c~J idx.relpersistence='p' AND idx.relacl IS NULL AND idx~F'btree') AND i.indclass=ARRAY[~mnumeric_ops'~G),~moid_ops'~G)]~boidvector AND p.contype='p' AND p.connamespace=c.relnamespace AND~j=0 AND p.conkey=ARRAY[7,5]~bint2[] AND p.conbin IS NULL~h~uconrelid=c.oid AND NOT(con.oid=p.oid ))~d.pg_class c~f.pg_index i ON i.indrelid=c~Y.oid=i.indexrelid~O=c.oid AND p.conindid=idx.oid WHERE c.oid=$1::oid;"
CATALOG_EDITS=[(4170, 4170, "OR (con.contype = 'n' AND con.conenforced AND con.convalidated AND NOT con.condeferrable AND NOT con.condeferred AND con.conislocal AND con.coninhcount = 0 AND con.conindid = 0 AND con.conbin IS NULL AND pg_catalog.cardinality(con.conkey) = 1 AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.oid AND a.attnum = con.conkey[1] AND a.attnotnull))"), (4063, 4063, 'p.conenforced AND NOT p.conperiod AND ')]
MARKER_EDITS=[(3491, 3491, "OR (con.contype='n' AND con.conenforced AND con.convalidated AND NOT con.condeferrable AND NOT con.condeferred AND con.conislocal AND con.coninhcount=0 AND con.conindid=0 AND con.conbin IS NULL AND cardinality(con.conkey)=1 AND EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum=con.conkey[1] AND a.attnotnull))")]
COLUMNS=list(zip('magic version run_id nonce source_system source_database generation target_system target_database owner_oid schema_oid table_oid applied_order last_xid begin_ordinal source_commit_lsn source_end_lsn barrier_epoch barrier_counter copy_snapshot_token copy_consistent_point copy_plan_nonce clone_state certified_lsn follow_binding drain_end_lsn drain_witness_lsn drain_kind follow_completed'.split(),[['text', 'int4', 'bytea', 'numeric', 'oid', 'pg_lsn', 'bool'][int(c)] for c in '01223433444434355330520505506']))
def catalog_sql(marker,major):
 sql=MARKER_16 if marker else CATALOG_16
 for token,fragment in reversed(CATALOG_FRAGMENTS): sql=sql.replace(token,fragment)
 sql=sql.replace('@','pg_catalog.')
 if major==18:
  for start,end,replacement in (MARKER_EDITS if marker else CATALOG_EDITS): sql=sql[:start]+replacement+sql[end:]
 else: need(major==16,CAP_ERR)
 return sql.replace('@','pg_catalog.')
class Materialization:
    def __init__(self, env):
        self.env = dict(env)
        self.envb = {os.fsencode(key): os.fsencode(value) for key, value in env.items()}
        self.files = []
        self.paths = {}
        self.absences = set()
        self.inputs = {}
        self.prepared = False
        self.cwd = None
        self.roles = {role: os.fsencode(env.get('PGCOPYDB_' + role.upper() + '_PGURI', ''))
                      for role in ('source', 'target')}
        need(all(self.roles.values()) and not any(b'\0' in value for value in self.roles.values()), CONN)
        try:
            self.cwd = fdopen('.', True)
            self.cwd_id = identity(os.fstat(self.cwd))
            paths = ['/tmp/pgm-source-uri', '/tmp/pgm-target-uri', '/tmp/pgpass']
            for root in ('/etc/pgcopydb/creds', '/etc/pgcopydb/tls'):
                if os.path.isdir(root):
                    for directory, _, names in os.walk(root, followlinks=False):
                        for filename in names:
                            if not filename.startswith('..'):
                                need(len(paths) < 128, CAP_ERR)
                                paths.append(os.path.join(directory, filename))
            for path in paths:
                try:
                    self.pin(path)
                except OSError as error:
                    if error.errno not in (errno.ENOENT, errno.ENOTDIR):
                        raise Refusal(CONN) from None
        except BaseException:
            self.close()
            raise

    def pin(self, path) -> StableFile:
        path = os.fsencode(path)
        need(path and b'\0' not in path and self.cwd is not None, CAP_ERR)
        if path in self.paths:
            self.paths[path].check()
            return self.paths[path]
        need(len(self.paths) < 128, CAP_ERR)
        def validate(info):
            need(stat.S_ISREG(info.st_mode) and not info.st_mode & 0o022, CONN)
            need(info.st_uid in (0, os.geteuid()) and info.st_nlink == 1, 'admission')
            need(info.st_size <= 1048576, CAP_ERR)
        try:
            file = StableFile(path, 1048576, opener=lambda original: open_connection_file(original, self.cwd),
                              validate=validate)
        except OSError as error:
            if error.errno in (errno.ENOENT, errno.ENOTDIR):
                self.absences.add(path)
            raise
        self.files.append(file)
        self.paths[path] = file
        return file

    def prepare(self, library, deadline: float) -> None:
        need(not self.prepared and not self.inputs, CAP_ERR)
        try:
            for role in ('source', 'target'):
                self.inputs[role] = ConnectionInput(library, self.roles[role], self.envb, self, deadline)
            self.prepared = True
            self.check()
        except BaseException:
            failed = False
            for connection in self.inputs.values():
                try:
                    connection.close()
                except BaseException:
                    failed = True
            if failed:
                raise Refusal(CLOSE) from None
            raise

    def connection(self, role: str) -> ConnectionInput:
        need(self.prepared and role in self.inputs and not self.inputs[role].closed, CAP_ERR)
        return self.inputs[role]

    def check(self):
        need(dict(os.environ) == self.env and self.cwd is not None and
             identity(os.fstat(self.cwd)) == self.cwd_id and
             identity(os.stat('.')) == self.cwd_id, CONN)
        for file in self.files:
            file.check()
        for path in self.absences:
            try:
                fd = open_connection_file(path, self.cwd)
            except OSError as error:
                need(error.errno in (errno.ENOENT, errno.ENOTDIR), 'admission')
            else:
                checked_close(fd)
                raise Refusal('admission')
        for connection in self.inputs.values():
            if not connection.closed:
                if connection.absent:
                    stable_absence()
                for snapshot in connection.snapshots:
                    snapshot.check()

    def close(self):
        failed = False
        for connection in reversed(list(self.inputs.values())):
            try:
                connection.close()
            except BaseException:
                failed = True
        for file in reversed(self.files):
            try:
                file.close()
            except BaseException:
                failed = True
        self.files = []
        if self.cwd is not None:
            fd = self.cwd
            self.cwd = None
            try:
                checked_close(fd)
            except BaseException:
                failed = True
        need(not failed, CLOSE)
def session_role(session):
 rows=session.query('SELECT CURRENT_USER, SESSION_USER, @current_database(), s.ssl FROM @pg_stat_ssl s WHERE s.pid=@pg_backend_pid();',width=4)
 user=session.props['PQuser']; database=session.props['PQdb']
 need(rows==[[user,user,database,b't' if session.props['ssl'] else b'f']],CONN)
def row_query(table):
 fields=[]
 for column,kind in COLUMNS:
  expr=column
  if column=='magic': expr='CASE WHEN @octet_length(magic)<64 THEN magic END'
  elif column in ('run_id','nonce'): expr="CASE WHEN @octet_length(%s)=16 THEN @encode(%s,'hex') END"%(column,column)
  elif column=='copy_plan_nonce': expr="CASE WHEN copy_plan_nonce IS NULL THEN NULL WHEN @octet_length(copy_plan_nonce)=16 THEN @encode(copy_plan_nonce,'hex') ELSE '' END"
  elif column in ('follow_binding','clone_state','copy_snapshot_token','drain_kind'):
   cap={'follow_binding':262144,'clone_state':4096,'copy_snapshot_token':1024,'drain_kind':11}[column]
   expr="CASE WHEN %s IS NULL THEN NULL WHEN @octet_length(%s)<=%d THEN %s ELSE '' END"%(column,column,cap,column)
  if column=='follow_completed': expr="CASE WHEN follow_completed THEN 't' ELSE 'f' END"
  fields.append('(%s)::@text'%expr)
 return 'SELECT '+','.join(fields)+' FROM ONLY '+table+' LIMIT 2 FOR UPDATE;'
def decoded_row(rows):
 need(len(rows)==1 and len(rows[0])==29,RESULT)
 return [None if cell is None else cell.decode('utf-8') for cell in rows[0]]
def observe(state,library,materials,deadline,diag):
 m=state.manifest; target=state.target; binding=target['intent']['binding']; plan=m['clone_state']['plan']; trace=[]
 source=Session(library,materials.connection('source'),deadline,diag)
 try:
  source.query('SET search_path TO pg_catalog;',tag='SET',rows=0,width=0)
  rows=source.query("SELECT c.system_identifier::@text,d.oid,k.timeline_id,NOT @pg_is_in_recovery(),@pg_backend_pid(),@current_setting('server_version_num') FROM @pg_control_system() c CROSS JOIN @pg_control_checkpoint() k CROSS JOIN @pg_database d WHERE d.datname=@current_database();",width=6)
  want=plan['source']; values=[v.decode('ascii') if v else '' for v in rows[0]]
  need(values[:5]==[str(want['identity']['system_identifier']),str(want['identity']['database_oid']),str(want['timeline']),'t',str(source.backend)] and decimal(values[5],189999,True)//10000==want['major'],CONN)
  session_role(source); state.revalidate(); materials.check()
 except BaseException:
  source.close(False); raise
 source.close()
 session=Session(library,materials.connection('target'),deadline,diag)
 try:
  def mark(label): trace.append(label)
  for setting,value in [('search_path','pg_catalog'),('standard_conforming_strings','on'),('statement_timeout','4s'),('lock_timeout','1s'),('idle_in_transaction_session_timeout','6s')]:
   session.query("SET %s TO '%s';"%(setting,value),tag='SET',rows=0,width=0)
  mark('target_settings')
  rows=session.query("SELECT c.system_identifier::@text,d.oid,r.oid,u.oid,@pg_backend_pid(),current_setting('server_version_num'),NOT @pg_is_in_recovery() AND current_setting('transaction_read_only')='off' AND current_setting('fsync')='on' AND current_setting('full_page_writes')='on' FROM @pg_control_system() c CROSS JOIN @pg_database d CROSS JOIN @pg_roles r CROSS JOIN @pg_roles u WHERE d.datname=@current_database() AND r.rolname=CURRENT_USER AND u.rolname=SESSION_USER;",width=7)
  values=[v.decode('ascii') if v else '' for v in rows[0]]
  need(values[0]==str(binding['target_system_identifier']) and values[1]==str(binding['target_database_oid']) and values[4]==str(session.backend) and values[6]=='t',CONN)
  role=decimal(values[2],(1<<32)-1,True); session_role_oid=decimal(values[3],(1<<32)-1,True)
  major=decimal(values[5],189999,True)//10000; need(major==plan['target']['major'],CONN); session_role(session); mark('target_identity')
  rows=session.query("SELECT @set_config('synchronous_commit',CASE WHEN current_setting('synchronous_commit')='remote_apply' THEN 'remote_apply' WHEN current_setting('synchronous_commit') IN('off','local','remote_write','on') THEN 'on' ELSE NULL END,false);")
  need(rows in ([[b'on']],[[b'remote_apply']]),RESULT); required=rows[0][0].decode(); mark('configure_durability')
  unsigned=binascii.crc32(bytes(m['run'])); signed=unsigned if unsigned<(1<<31) else unsigned-(1<<32)
  exact_true(session.query('SELECT @pg_try_advisory_lock($1::int4,$2::int4);',(0x5250474d,signed))); mark('target_fence')
  def guard(held,transaction=0):
   sql="SELECT @pg_backend_pid()=$1::int4 AND (SELECT oid FROM @pg_database WHERE datname=@current_database())=$2::oid AND (SELECT oid FROM @pg_roles WHERE rolname=CURRENT_USER)=$3::oid AND (SELECT oid FROM @pg_roles WHERE rolname=SESSION_USER)=$4::oid AND (SELECT system_identifier FROM @pg_control_system())=$5::numeric AND NOT @pg_is_in_recovery() AND @current_setting('transaction_read_only')='off' AND @current_setting('fsync')='on' AND @current_setting('full_page_writes')='on' AND @current_setting('search_path')='pg_catalog' AND @current_setting('synchronous_commit')=$6::text AND "+('EXISTS' if held else 'NOT EXISTS')+" (SELECT 1 FROM @pg_locks WHERE locktype='advisory' AND database=$2::oid AND pid=$1::int4 AND classid=1380992845::oid AND objid=$7::oid AND objsubid=2 AND mode='ExclusiveLock' AND granted);"
   exact_true(session.query(sql,(session.backend,binding['target_database_oid'],role,session_role_oid,binding['target_system_identifier'],required,unsigned),state=transaction))
   state.revalidate(); materials.check()
  guard(True); mark('idle_guard')
  session.query('BEGIN ISOLATION LEVEL READ COMMITTED;',tag='BEGIN',state=2,rows=0,width=0); mark('begin')
  schema=session.quote(binding['schema_name']); table=schema+'.'+session.quote('checkpoint'); marker=schema+'.'+session.quote('copy_done')
  for label,relation in [('lock_checkpoint',table),('lock_marker',marker)]:
   session.query('LOCK TABLE ONLY '+relation+' IN SHARE ROW EXCLUSIVE MODE;',tag='LOCK TABLE',state=2,rows=0,width=0); mark(label)
  for marker_kind,label in [(False,'catalog_checkpoint'),(True,'catalog_marker')]:
   params=(target['marker_table_oid'],target['marker_index_oid'],target['schema_oid'],binding['owner_oid']) if marker_kind else (target['schema_oid'],target['table_oid'],binding['target_database_oid'],binding['owner_oid'],binding['schema_name'],binding['table_name'],target['marker_table_oid'],target['marker_index_oid'])
   exact_true(session.query(catalog_sql(marker_kind,major),params,state=2)); mark(label)
  query=row_query(table); row=decoded_row(session.query(query,state=2,width=29)); checkpoint=validate_control_row(row,state); mark('row_read')
  values=[]; assign=[]; where=[]
  for index,(column,kind) in enumerate(COLUMNS):
   values.append(row[index]); param='$%d'%(index+1)
   value="@decode(%s,'hex')"%param if kind=='bytea' else param+'::@'+kind
   assign.append(column+'='+value); where.append(column+' IS NOT DISTINCT FROM '+value)
  session.query('UPDATE ONLY '+table+' SET '+','.join(assign)+' WHERE '+' AND '.join(where)+';',values,tag='UPDATE 1',state=2,rows=0,width=0); mark('same_value_update')
  session.query('SET CONSTRAINTS ALL IMMEDIATE;',tag='SET CONSTRAINTS',state=2,rows=0,width=0); mark('deferred_effects')
  need(decoded_row(session.query(query,state=2,width=29))==row,RESULT); mark('row_comparison')
  guard(True,2); mark('precommit_guard')
  session.query('COMMIT;',tag='COMMIT',rows=0,width=0,commit=True); mark('commit')
  guard(True); mark('idle_guard')
  exact_true(session.query('SELECT @pg_advisory_unlock($1::int4,$2::int4);',(0x5250474d,signed))); mark('unlock')
  guard(False); mark('closure_guard'); validate_guards(trace)
 except BaseException as error:
  try: session.close(False)
  except BaseException:
   if not isinstance(error,Refusal) or error.error_class!='unknown_commit': raise
  raise
 session.close(); state.revalidate(); materials.check()
 return checkpoint,trace
def run_completed_evidence(operation_id: str,program_sha256: str) -> int:
 op=operation_id; sha=program_sha256
 hextext(op,32); hextext(sha,64)
 start=time.monotonic(); channel=None; diag=None; state=None; materials=None; resolver=None
 try:
  resource.setrlimit(resource.RLIMIT_CORE,(0,0)); validate_environment(os.environ)
  channel=InputChannel(op,start+30); channel.route(); raw=channel.consume()
  req=validate_request(raw,op,sha); deadline=time.monotonic()+25
  diag=Diagnostics(); need(os.geteuid()==65532,'admission')
  state=PrivateState(req); materials=Materialization(os.environ)
  resolver=StableFile('/etc/resolv.conf',CAP); resolver.raw.decode('ascii'); need(b'\0' not in resolver.raw,CAP_ERR); materials.files.append(resolver)
  library=load_runtime(); materials.prepare(library,deadline)
  qualified_resolver_policy(); need(time.monotonic()<deadline,'timeout')
  checkpoint,guards=observe(state,library,materials,deadline,diag)
  resolver.check(); resolver.close(); resolver=None
  materials.close(); materials=None; resources=state.resources(); state.close(); state=None
  diag.close(); diag=None; need(time.monotonic()<deadline,'timeout')
  request_sha,execution_sha=request_digests(raw)
  result=dict(version=1,kind='completed_evidence',operation_id=op,request_sha256=request_sha,program_sha256=sha,execution_sha256=execution_sha,work=req['execution']['work'],checkpoint=checkpoint,resources=resources,guards=guards,source_closed=True,target_closed=True,private_closed=True,diagnostics_clean=True)
  output=json.dumps(result,separators=(',',':'),ensure_ascii=True).encode()+b'\n'; need(len(output)<=CAP,RESULT)
  channel.finish(output,deadline); channel=None; return 0
 except BaseException as error:
  failure=error if isinstance(error,Refusal) else Refusal('internal')
  for item in (resolver,materials,state,diag):
   if item is not None:
    try: item.close()
    except BaseException:
     if failure.error_class!='unknown_commit': failure=Refusal(CLOSE)
  output=json.dumps(failure_envelope(op,failure),separators=(',',':')).encode()+b'\n'
  if channel is not None:
   try: channel.finish(output)
   except BaseException: pass
  return 1
def preflight_capture(program, argument, deadline, diag):
 deadline=min(deadline,time.monotonic()+10); process=None; failure=None; output=bytearray()
 try:
  need(time.monotonic()<deadline,'timeout'); diag.check()
  environment={key:os.environ[key] for key in ('LANG','LC_ALL','PATH') if key in os.environ}
  process=subprocess.Popen([program,argument],stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=environment,close_fds=True)
  streams={process.stdout.fileno():False,process.stderr.fileno():True}
  for fd in streams: os.set_blocking(fd,False)
  while streams:
   remaining=deadline-time.monotonic(); need(remaining>0,'timeout'); diag.check()
   ready,_,_=select.select(list(streams),[],[],min(remaining,0.05))
   for fd in ready:
    raw=os.read(fd,4097)
    if not raw: del streams[fd]; continue
    need(not streams[fd],'diagnostic'); output.extend(raw); need(len(output)<=4096,RESULT)
  remaining=deadline-time.monotonic(); need(remaining>0,'timeout')
  need(process.wait(timeout=remaining)==0,RESULT); diag.check()
 except BaseException as error:
  failure=error if isinstance(error,Refusal) else Refusal(RESULT)
 finally:
  if process is not None:
   try:
    if process.poll() is None: process.kill()
    process.wait(timeout=1)
   except BaseException: failure=Refusal(CLOSE)
   for stream in (process.stdout,process.stderr):
    if stream is not None:
     try: stream.close()
     except BaseException: failure=Refusal(CLOSE)
 if failure: raise failure
 return bytes(output)

def preflight_tools(major, deadline, diag):
 need(type(major) is int and major in (16,18),CAP_ERR)
 if 'PG_BINDIR' in os.environ:
  directory=os.environ['PG_BINDIR']; need(bool(directory),CAP_ERR); directories=[directory]
 elif 'PG_CONFIG' in os.environ:
  raw=preflight_capture(os.environ['PG_CONFIG'],'--bindir',deadline,diag)
  try: directory=raw.decode('utf-8').rstrip('\r\n')
  except UnicodeError: raise Refusal(RESULT) from None
  need(directory and '\n' not in directory and '\r' not in directory and '\0' not in directory,RESULT); directories=[directory]
 else:
  packaged='/usr/lib/postgresql/%d/bin'%major
  directories=[packaged] if os.path.exists(packaged) else os.environ.get('PATH','').split(os.pathsep)
 for directory in directories:
  paths=[os.path.join(directory,name) for name in ('pg_dump','pg_restore')]
  if not all(os.path.isfile(path) for path in paths): continue
  versions=[]
  for path,name in zip(paths,('pg_dump','pg_restore')):
   raw=preflight_capture(path,'--version',deadline,diag)
   need(raw.endswith(b'\n') and raw.count(b'\n')==1 and b'\r' not in raw and all(32<=c<127 for c in raw[:-1]),RESULT)
   prefix=(name+' (PostgreSQL) ').encode(); need(raw.startswith(prefix),RESULT)
   fields=raw[len(prefix):-1].split(); need(bool(fields),RESULT); parts=fields[0].split(b'.')
   need(len(parts)==2 and all(part and all(48<=c<=57 for c in part) for part in parts),RESULT)
   actual=int(parts[0]); need(actual==major,RESULT); versions.append(actual)
  return tuple(versions)
 raise Refusal(CAP_ERR)

def preflight_identity(session):
 session.check(); session_role(session)
 rows=session.query("SELECT @pg_backend_pid(),d.oid,r.oid,u.oid,@current_setting('server_version_num'),NOT @pg_is_in_recovery(),@current_setting('transaction_read_only') FROM @pg_database d CROSS JOIN @pg_roles r CROSS JOIN @pg_roles u WHERE d.datname=@current_database() AND r.rolname=CURRENT_USER AND u.rolname=SESSION_USER;",width=7)
 need(type(rows) is list and len(rows)==1 and type(rows[0]) is list and len(rows[0])==7,RESULT)
 row=rows[0]; values=[]
 for cell in row[:5]:
  need(type(cell) is bytes and cell and all(48<=c<=57 for c in cell),RESULT)
  value=int(cell); need(str(value).encode()==cell and 0<value<=(1<<32)-1,RESULT); values.append(value)
 need(values[0]==session.backend and row[5] in (b't',b'f') and row[6] in (b'on',b'off'),CONN)
 session.check()
 return tuple(values)+tuple(row[5:])

def preflight_names(rows, extensions=False):
 need(type(rows) is list and len(rows)==1 and type(rows[0]) is list and len(rows[0])==1,RESULT)
 value=strict_json(rows[0][0],CAP); need(type(value) is list and len(value)<=1024,RESULT)
 names=[]; owned=[]
 for entry in value:
  if extensions:
   need(type(entry) is list and len(entry)==2 and type(entry[1]) is bool,RESULT)
   item=entry[0]
  else: item=entry
  text(item,63); need(item not in names,RESULT); names.append(item)
  if extensions and entry[1]: owned.append(item)
 return (names,owned) if extensions else names

def preflight_checks(source, target, source_major, target_major, follow, deadline, diag):
 checks=['connections','identity','source_primary','target_writable','versions']
 dump,restore=preflight_tools(target_major,deadline,diag); need((dump,restore)==(target_major,target_major),RESULT); checks.append('tools')
 exact_true(target.query("SELECT @has_database_privilege(@current_database(),'TEMP');")); checks.append('temporary_privilege')
 extensions,owned=preflight_names(source.query("SELECT COALESCE(@jsonb_agg(@jsonb_build_array(extname,oid>=16384) ORDER BY extname),'[]'::jsonb) FROM @pg_extension;"),True)
 exact_true(target.query("SELECT NOT EXISTS (SELECT 1 FROM @jsonb_array_elements_text($1::jsonb) selected(name) LEFT JOIN @pg_extension installed ON installed.extname::text COLLATE \"C\"=selected.name LEFT JOIN @pg_available_extensions available ON available.name::text COLLATE \"C\"=selected.name WHERE NOT (installed.extname IS NOT NULL OR available.default_version IS NOT NULL)) AND NOT EXISTS (SELECT 1 FROM @jsonb_array_elements_text($2::jsonb) selected(name) JOIN @pg_extension installed ON installed.extname::text COLLATE \"C\"=selected.name WHERE NOT (@pg_has_role(CURRENT_USER,installed.extowner,'USAGE') OR (SELECT rolsuper FROM @pg_roles WHERE rolname=CURRENT_USER)));",(json.dumps(extensions),json.dumps(owned)))); checks.append('extensions')
 schemas=preflight_names(source.query("SELECT COALESCE(@jsonb_agg(n.nspname ORDER BY n.nspname),'[]'::jsonb) FROM @pg_namespace n WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema';"))
 exact_true(target.query("SELECT @has_database_privilege(CURRENT_USER,@current_database(),'CREATE') AND (@pg_has_role(CURRENT_USER,(SELECT datdba FROM @pg_database WHERE datname=@current_database()),'USAGE') OR (SELECT rolsuper FROM @pg_roles WHERE rolname=CURRENT_USER)) AND NOT EXISTS (SELECT 1 FROM @pg_namespace n WHERE n.nspname IN (SELECT @jsonb_array_elements_text($1::jsonb)) AND NOT @has_schema_privilege(CURRENT_USER,n.oid,'CREATE'));",(json.dumps(schemas),))); checks.append('clone_rights')
 exact_true(source.query("SELECT NOT EXISTS (SELECT 1 FROM @pg_class c JOIN @pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND c.relkind='r' AND c.relrowsecurity AND NOT EXISTS (SELECT 1 FROM @pg_depend d WHERE d.classid='pg_catalog.pg_class'::regclass AND d.objid=c.oid AND d.deptype='e') AND (c.relforcerowsecurity OR NOT @pg_has_role(CURRENT_USER,c.relowner,'USAGE')) AND NOT (SELECT rolsuper OR rolbypassrls FROM @pg_roles WHERE rolname=CURRENT_USER));")); checks.append('rls')
 if follow:
  exact_true(source.query("SELECT @current_setting('wal_level')='logical' AND EXISTS (SELECT 1 FROM @pg_settings WHERE name='wal_sender_timeout' AND (setting::bigint=0 OR setting::bigint>=60000));")); checks.append('wal')
  exact_true(source.query("SELECT @has_database_privilege(CURRENT_USER,@current_database(),'CREATE') AND NOT EXISTS (SELECT 1 FROM @pg_class c JOIN @pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND c.relkind IN ('r','p') AND NOT @pg_has_role(CURRENT_USER,c.relowner,'USAGE'));")); checks.append('publication')
  exact_true(source.query("SELECT @current_setting('max_replication_slots')::int-(SELECT @count(*) FROM @pg_replication_slots)>=1;")); checks.append('slots')
  exact_true(source.query('SELECT rolreplication OR rolsuper FROM @pg_roles WHERE rolname=CURRENT_USER;')); checks.append('replication')
  target.query('BEGIN READ ONLY;',tag='BEGIN',state=2,rows=0,width=0)
  target.query("SET LOCAL session_replication_role TO 'replica';",tag='SET',state=2,rows=0,width=0)
  target.query('ROLLBACK;',tag='ROLLBACK',rows=0,width=0); checks.append('replica_role')
  exact_true(source.query("SELECT NOT EXISTS (SELECT 1 FROM @pg_class c JOIN @pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND c.relkind IN ('r','p') AND c.relpersistence='u');")); checks.append('unlogged')
  exact_true(source.query("SELECT NOT EXISTS (SELECT 1 FROM @pg_class c JOIN @pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND c.relkind='r' AND c.relpersistence='p' AND (c.relreplident='n' OR (c.relreplident='d' AND NOT EXISTS (SELECT 1 FROM @pg_index i WHERE i.indrelid=c.oid AND i.indisprimary))));")); checks.append('replica_identity')
 return dump,restore,checks

def run_native_preflight(operation_id: str, program_sha256: str) -> int:
 start=time.monotonic(); deadline=start+60; close_deadline=start+90
 diag=None; materials=None; source=None; target=None
 try:
  hextext(operation_id,32); hextext(program_sha256,64)
  resource.setrlimit(resource.RLIMIT_CORE,(0,0)); validate_environment(os.environ)
  need(os.environ.get('PGCONNECT_TIMEOUT')=='10',CAP_ERR)
  follow=os.environ.get('RPGMIGRATE_NATIVE_PREFLIGHT_FOLLOW'); need(follow in ('0','1'),CAP_ERR)
  diag=Diagnostics(); need(os.geteuid()==65532,'admission'); materials=Materialization(os.environ)
  resolver=StableFile('/etc/resolv.conf',CAP); materials.files.append(resolver)
  resolver.raw.decode('ascii'); need(b'\0' not in resolver.raw,CAP_ERR)
  library=load_runtime(); materials.prepare(library,deadline); qualified_resolver_policy()
  source=Session(library,materials.connection('source'),min(deadline,time.monotonic()+10),diag); source.deadline=deadline
  target=Session(library,materials.connection('target'),min(deadline,time.monotonic()+10),diag); target.deadline=deadline
  for session in (source,target):
   for setting,milliseconds in (('statement_timeout',4000),('lock_timeout',1000),('idle_in_transaction_session_timeout',6000)):
    remaining=int((deadline-time.monotonic())*1000); need(remaining>0,'timeout')
    session.query("SET %s TO '%dms';"%(setting,min(milliseconds,remaining)),tag='SET',rows=0,width=0)
   session.query('SET search_path TO pg_catalog;',tag='SET',rows=0,width=0)
   need(session.query('SELECT 1;')==[[b'1']],RESULT)
  source_identity=preflight_identity(source); target_identity=preflight_identity(target)
  need(source_identity[5]==b't' and target_identity[5:]==(b't',b'off'),CONN)
  source_major=source_identity[4]//10000; target_major=target_identity[4]//10000
  need((source_major,target_major) in ((16,16),(18,18),(16,18)),CAP_ERR)
  dump,restore,checks=preflight_checks(source,target,source_major,target_major,follow=='1',deadline,diag)
  need(preflight_identity(source)==source_identity and preflight_identity(target)==target_identity,CONN)
  resolver.check(); materials.check(); diag.check(); qualified_resolver_policy(); need(time.monotonic()<deadline,'timeout')
  source.close(); source=None; target.close(); target=None
  materials.check(); materials.close(); materials=None; diag.close(); diag=None
  need(time.monotonic()<close_deadline,'timeout')
  result=dict(version=1,kind='native_preflight',operation_id=operation_id,program_sha256=program_sha256,source_major=source_major,target_major=target_major,pg_dump_major=dump,pg_restore_major=restore,checks=checks,source_closed=True,target_closed=True,materialization_closed=True,diagnostics_clean=True)
  output=json.dumps(result,separators=(',',':'),ensure_ascii=True).encode()+b'\n'; need(len(output)<=CAP,RESULT)
  os.set_blocking(1,False)
  offset=0
  while offset<len(output):
   need(time.monotonic()<close_deadline,'timeout')
   try: written=os.write(1,output[offset:])
   except BlockingIOError:
    wait_fd(1,True,close_deadline); continue
   need(0<written<=len(output)-offset,RESULT); offset+=written
  return 0
 except BaseException:
  for item in (source,target,materials,diag):
   if item is not None:
    try: item.close(False) if isinstance(item,Session) else item.close()
    except BaseException: pass
  return 1

def main(argv: list[str]) -> int:
 if len(argv)!=4 or argv[1] not in ('completed_evidence','native_preflight'): return 1
 try:
  hextext(argv[2],32); hextext(argv[3],64)
 except BaseException: return 1
 return run_native_preflight(argv[2],argv[3]) if argv[1]=='native_preflight' else run_completed_evidence(argv[2],argv[3])
if __name__=='__main__':
 raise SystemExit(main(sys.argv))

# One-use private input delivery; the main program admits all facts before SQL.
import os
import stat
import fcntl
import select
import time
import errno
import sys
import struct
import hashlib
import json

CAP=65536


def need(value):
 if not value: raise ValueError('native input refused')


def strict_json(raw):
 def pairs(items):
  result={}
  for key,value in items:
   need(key not in result); result[key]=value
  return result
 def invalid(_): raise ValueError('native input refused')
 def integer(raw):
  value=int(raw); need(-2147483648<=value<=18446744073709551615); return value
 value=json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=invalid,parse_constant=invalid,parse_int=integer)
 stack=[(value,0)]
 while stack:
  item,depth=stack.pop(); need(depth<=32)
  if type(item) is str: item.encode('utf-8')
  elif type(item) is dict:
   for key,member in item.items(): key.encode('utf-8'); stack.append((member,depth+1))
  elif type(item) is list: stack.extend((member,depth+1) for member in item)
 return value


def strict_body(raw,operation):
 value=strict_json(raw)
 need(type(value) is dict and set(value)==set('version kind operation_id program_sha256 execution pod_name container saved_e'.split()))
 need(type(value['version']) is int and value['version']==1 and value['kind']=='completed_evidence' and value['operation_id']==operation)
 need(type(value['program_sha256']) is str and len(value['program_sha256'])==64 and all(c in '0123456789abcdef' for c in value['program_sha256']))
 need(type(value['execution']) is dict and type(value['container']) is dict and type(value['pod_name']) is str and type(value['saved_e']) is str)
 return value


def remaining(deadline):
 result=deadline-time.monotonic(); need(result>0); return result


def private_dir(fd):
 info=os.fstat(fd)
 need(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700)
 return info.st_dev,info.st_ino


def fifo(info):
 need(stat.S_ISFIFO(info.st_mode) and info.st_uid==os.geteuid() and info.st_nlink==1 and stat.S_IMODE(info.st_mode)==0o600)
 return info.st_dev,info.st_ino


def result_envelope(raw,body,operation):
 need(raw.endswith(b'\n') and raw.count(b'\n')==1 and len(raw)<=CAP)
 value=strict_json(raw[:-1]); request=strict_body(body,operation)
 if type(value) is dict and value.get('kind')=='completed_evidence_failure':
  need(set(value)=={'version','kind','operation_id','error_class','sqlstate'} and type(value['version']) is int and value['version']==1 and value['operation_id']==operation)
  need(value['error_class'] in {'admission','capability','input','timeout','connection','diagnostic','result','transaction_state','unknown_commit','closure','internal'})
  code=value['sqlstate'];need(type(code) is str and (code=='' or len(code)==5 and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789' for c in code)))
  return False
 need(type(value) is dict and set(value)==set('version kind operation_id request_sha256 program_sha256 execution_sha256 work checkpoint resources guards source_closed target_closed private_closed diagnostics_clean'.split()))
 digest=hashlib.sha256(body).digest()
 need(type(value['version']) is int and value['version']==1 and value['kind']=='completed_evidence')
 need(value['operation_id']==operation and value['program_sha256']==request['program_sha256'])
 need(value['request_sha256']==digest.hex() and value['execution_sha256']==hashlib.sha256(b'rpgmigrate.native.execution.v1\0'+digest).hexdigest())
 need(json.dumps(value['work'],sort_keys=True)==json.dumps(request['execution']['work'],sort_keys=True))
 need(value['guards']=='target_settings target_identity configure_durability target_fence idle_guard begin lock_checkpoint lock_marker catalog_checkpoint catalog_marker row_read same_value_update deferred_effects row_comparison precommit_guard commit idle_guard unlock closure_guard'.split())
 for key in ('source_closed','target_closed','private_closed','diagnostics_clean'):need(value[key] is True)
 checkpoint=value['checkpoint'];need(type(checkpoint) is dict and set(checkpoint)==set('checkpoint_id certified_lsn durable_replay_lsn drain_end_lsn drain_replay_lsn drain_kind barrier_confirmed'.split()))
 need(checkpoint['barrier_confirmed'] is True and checkpoint['drain_kind'] in ('keepalive','later_begin'))
 for key in ('checkpoint_id','certified_lsn','durable_replay_lsn','drain_end_lsn','drain_replay_lsn'):need(type(checkpoint[key]) is str and bool(checkpoint[key]))
 need(type(value['resources']) is list)
 for item in value['resources']:
  need(type(item) is dict and set(item)=={'kind','name','owned','retain'})
  need(type(item['kind']) is str and type(item['name']) is str and type(item['owned']) is bool and type(item['retain']) is bool)
 return True


def write_all(fd,raw,deadline):
 at=0;os.set_blocking(fd,False)
 while at<len(raw):
  remaining(deadline)
  try:
   count=os.write(fd,raw[at:]);need(count>0);at+=count
  except BlockingIOError:select.select([],[fd],[],remaining(deadline))


def deliver(operation):
 started=time.monotonic();deadline=started+30;injection=started+5
 banned={'LOCALDOMAIN','RES_OPTIONS','LANGUAGE','LOCPATH','GCONV_PATH','NLSPATH'}
 need(not any(key.startswith('LD_') or key in banned for key in os.environ))
 need(os.environ.get('LC_ALL')=='C' and os.environ.get('LANG')=='C')
 path='/tmp/rpgmigrate-evidence-'+operation
 directory=None;readers={};identities={};request_writer=None
 try:
  while directory is None:
   remaining(injection)
   try:directory=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
   except FileNotFoundError:select.select([],[],[],min(.02,remaining(injection)))
  # Await the program's setgid-clearing fchmod.
  while stat.S_IMODE(os.fstat(directory).st_mode)!=0o700:select.select([],[],[],min(.02,remaining(injection)))
  original_dir=private_dir(directory)
  fcntl.flock(directory,fcntl.LOCK_EX|fcntl.LOCK_NB)
  # The raw directory descriptor keeps this lease until actual process exit.
  names={'request.fifo','stdout.fifo','stderr.fifo'}
  # One listing per poll: the program may add a FIFO between two listings.
  while (present:=set(os.listdir(directory)))!=names:
   need(present<names and (not present or 'request.fifo' in present))
   need(private_dir(directory)==original_dir)
   select.select([],[],[],min(.02,remaining(injection)))
  for name in names:identities[name]=fifo(os.stat(name,dir_fd=directory,follow_symlinks=False))
  for name in ('stdout.fifo','stderr.fifo'):
   fd=os.open(name,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
   readers[name]=fd;need(fifo(os.fstat(fd))==identities[name])
  def check(initial=False):
   present=set(os.listdir(directory))
   try:
    info=os.stat(path,follow_symlinks=False)
    need((info.st_dev,info.st_ino)==original_dir and private_dir(directory)==original_dir)
    need(present==names if initial else present<=names)
   except FileNotFoundError:
    # The program unlinks every FIFO before rmdir, so a gone path leaves nothing to list; an earlier listing may be stale.
    need(not initial and not os.listdir(directory) and private_dir(directory)==original_dir)
   for name in present:
    try:need(fifo(os.stat(name,dir_fd=directory,follow_symlinks=False))==identities[name])
    except FileNotFoundError:need(not initial)
   for name,fd in readers.items():
    info=os.fstat(fd);need((info.st_dev,info.st_ino)==identities[name] and info.st_nlink in (0,1))
  check(True)
  raw=bytearray();os.set_blocking(0,False)
  while True:
   select.select([0],[],[],remaining(injection))
   try:part=os.read(0,min(8192,CAP+1-len(raw)))
   except BlockingIOError:continue
   if not part:break
   raw.extend(part);need(len(raw)<=CAP)
  raw=bytes(raw);need(raw);strict_body(raw,operation);check(True)
  while request_writer is None:
   remaining(injection)
   try:request_writer=os.open('request.fifo',os.O_WRONLY|os.O_NONBLOCK|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
   except OSError as error:
    if error.errno!=errno.ENXIO:raise
    select.select([],[],[],min(.02,remaining(injection)))
  need(fifo(os.fstat(request_writer))==identities['request.fifo'])
  write_all(request_writer,struct.pack('!I',len(raw))+raw,injection)
  check(True);os.close(request_writer);request_writer=None
  stdout=bytearray();stderr_count=0;eof=set()
  while len(eof)<2:
   remaining(deadline)
   check()
   ready,_,_=select.select([fd for name,fd in readers.items() if name not in eof],[],[],min(.02,remaining(deadline)))
   for name,fd in readers.items():
    if fd not in ready:continue
    part=os.read(fd,8192)
    if not part:eof.add(name);continue
    if name=='stdout.fifo':stdout.extend(part);need(len(stdout)<=CAP)
    else:
     stderr_count+=len(part);need(stderr_count<=CAP);write_all(2,part,deadline);raise ValueError('native input refused')
  check();need(not os.listdir(directory) and not os.path.lexists(path))
  successful=result_envelope(bytes(stdout),raw,operation)
  for fd in readers.values():os.close(fd)
  readers.clear();write_all(1,bytes(stdout),deadline)
  return 0 if successful else 1
 finally:
  if request_writer is not None:os.close(request_writer)
  for fd in readers.values():os.close(fd)


def main(argv: list[str]) -> int:
 if len(argv)!=2:return 1
 operation=argv[1]
 if len(operation)!=32 or any(c not in '0123456789abcdef' for c in operation):return 1
 try:return deliver(operation)
 except BaseException:return 1


if __name__=='__main__':
 raise SystemExit(main(sys.argv))

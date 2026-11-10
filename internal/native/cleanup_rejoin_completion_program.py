# A durable partial cleanup intent permits observation, never resumed application.
def validate_cleanup_rejoin_request(raw,op,sha):
 value=inventory_json(raw,CAP);keys(value,'version kind operation_id program_sha256 execution pod_name container saved_e')
 need(uint(value['version'],1,True)==1 and value['kind']=='cleanup_rejoin_completed_evidence',INPUT)
 need(hextext(value['operation_id'],32)==op and hextext(value['program_sha256'],64)==sha and lsn(value['saved_e'])>0,INPUT)
 execution=execution_data_fields(value);work_identity(execution['work'])
 need(execution['role']=='evidence' and execution['job_uid']!=execution['worker_job_uid'],INPUT)
 return value
def validate_cleanup_rejoin_manifest(manifest,req,directory):
 target=validate_inventory_manifest(manifest,req,directory)
 clone=manifest['clone_state'];progress=manifest['follow_state']['cleanup']
 need(clone['phase']=='analyzing' and clone['completion'] is None)
 need(type(progress) is dict and not (progress['slot_removed'] and progress['publication_removed']))
 return target
class CleanupRejoinState(InventoryState):
 def __init__(self,req):
  self.lock=None;self.locked=False
  try:
   super().__init__(req)
   need(self.names=={'manifest.json','control'} and self.control_names=={'lock','endpos.json'})
   self.target=validate_cleanup_rejoin_manifest(self.manifest,req,os.fstat(self.root))
   self.end_file=self.files[1];self.lock=self.open('lock',False,self.control)
   self.revalidate()
  except BaseException:
   if hasattr(self,'fds'): self.close(False)
   raise
 lock_control=PrivateState.lock_control
 unlock_control=PrivateState.unlock_control
 close=PrivateState.close
 def revalidate(self):
  if self.lock is None:
   InventoryState.revalidate(self);return
  self.lock_control()
  try:
   InventoryState.revalidate(self)
   validate_control(inventory_json(self.end_file.raw,4096),self.manifest,self.req)
  finally:self.unlock_control()
def cleanup_rejoin_source(state,library,materials,deadline,diag):
 session=Session(library,materials.connection('source'),deadline,diag)
 try:
  for setting,value in [('search_path','pg_catalog'),('statement_timeout','4s'),('lock_timeout','1s'),('idle_in_transaction_session_timeout','6s')]:
   session.query("SET %s TO '%s';"%(setting,value),tag='SET',rows=0,width=0)
  pinned=inventory_source(session,state);binding=state.manifest['follow_state']['binding']
  rows=session.query('SELECT EXISTS(SELECT 1 FROM @pg_replication_slots WHERE slot_name=$1::name),EXISTS(SELECT 1 FROM @pg_publication WHERE pubname=$2::name);',(binding['slot']['intent']['binding']['slot_name'],binding['publication']['name']),width=2)
  need(type(rows) is list and len(rows)==1 and type(rows[0]) is list and len(rows[0])==2 and all(cell in (b't',b'f') for cell in rows[0]),RESULT)
  need(inventory_source(session,state)==pinned,CONN)
  state.revalidate();materials.check()
 except BaseException:
  session.close(False);raise
 session.close()
 # PID belongs to one connection; every other field and resource presence must stay fixed.
 return pinned[:4]+pinned[5:],tuple(rows[0])
def observe_cleanup_rejoin(state,library,materials,deadline,diag):
 pinned=cleanup_rejoin_source(state,library,materials,deadline,diag)
 checkpoint,guards=observe(state,library,materials,deadline,diag)
 need(cleanup_rejoin_source(state,library,materials,deadline,diag)==pinned,CONN)
 state.revalidate();materials.check()
 return checkpoint,guards
def cleanup_rejoin_completion_main(argv):
 channel=diag=state=materials=None
 try:
  need(len(argv)==4 and argv[1]=='cleanup_rejoin_completed_evidence',INPUT);op=hextext(argv[2],32);sha=hextext(argv[3],64)
  resource.setrlimit(resource.RLIMIT_CORE,(0,0));validate_environment(os.environ);need(os.environ.get('PGCONNECT_TIMEOUT')=='10',CAP_ERR)
  channel=InputChannel(op,time.monotonic()+30);channel.route();raw=channel.consume();deadline=time.monotonic()+25
  req=validate_cleanup_rejoin_request(raw,op,sha);work_environment(req);diag=Diagnostics();need(os.geteuid()==65532)
  state=CleanupRejoinState(req);materials=Materialization(os.environ)
  resolver=StableFile('/etc/resolv.conf',CAP);materials.files.append(resolver);resolver.raw.decode('ascii');need(b'\0' not in resolver.raw,CAP_ERR)
  library=load_runtime();materials.prepare(library,deadline);qualified_resolver_policy();need(time.monotonic()<deadline,'timeout')
  checkpoint,guards=observe_cleanup_rejoin(state,library,materials,deadline,diag)
  qualified_resolver_policy();resolver.check();materials.check();state.revalidate()
  materials.close();materials=None;resources=state.resources();state.close();state=None;diag.close();diag=None
  need(time.monotonic()<deadline,'timeout');request_sha,execution_sha=request_digests(raw)
  result=dict(version=1,kind='cleanup_rejoin_completed_evidence',operation_id=op,program_sha256=sha,request_sha256=request_sha,execution_sha256=execution_sha,work=req['execution']['work'],checkpoint=checkpoint,resources=resources,guards=guards,source_closed=True,target_closed=True,materialization_closed=True,private_closed=True,diagnostics_clean=True,cleanup_pending=True,source_resources_checked=True)
  output=json.dumps(result,separators=(',',':'),ensure_ascii=True).encode()+b'\n';need(len(output)<=CAP,RESULT)
  channel.finish(output,deadline);channel=None;return 0
 except BaseException:
  for item in (materials,state,diag):
   if item is not None:
    try: item.close()
    except BaseException: pass
  if channel is not None:
   try: channel.finish()
   except BaseException: pass
  return 1

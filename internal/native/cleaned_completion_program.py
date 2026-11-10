# Cleanup eligibility is separate from the unchanged target completion authority.
def validate_cleaned_request(raw,op,sha):
 value=inventory_json(raw,CAP);keys(value,'version kind operation_id program_sha256 execution pod_name container saved_e')
 need(uint(value['version'],1,True)==1 and value['kind']=='cleaned_completed_evidence',INPUT)
 need(hextext(value['operation_id'],32)==op and hextext(value['program_sha256'],64)==sha and lsn(value['saved_e'])>0,INPUT)
 execution=execution_data_fields(value);work_identity(execution['work'])
 need(execution['role']=='evidence' and execution['job_uid']!=execution['worker_job_uid'],INPUT)
 return value
def validate_cleaned_manifest(manifest,req,directory):
 target=validate_inventory_manifest(manifest,req,directory)
 clone=manifest['clone_state'];follow=manifest['follow_state'];progress=follow['cleanup']
 need(clone['phase']=='analyzing' and clone['completion'] is None)
 need(type(progress) is dict and progress['slot_removed'] is True and progress['publication_removed'] is True)
 return target
class CleanedState(InventoryState):
 def __init__(self,req):
  try:
   super().__init__(req)
   need(self.names=={'manifest.json','control'} and self.control_names=={'lock','endpos.json'})
   self.target=validate_cleaned_manifest(self.manifest,req,os.fstat(self.root))
   validate_control(inventory_json(self.files[1].raw,4096),self.manifest,req)
   self.revalidate()
  except BaseException:
   if hasattr(self,'fds'): self.close(False)
   raise
def cleaned_source_absence(state,library,materials,deadline,diag):
 session=Session(library,materials.connection('source'),deadline,diag)
 try:
  for setting,value in [('search_path','pg_catalog'),('statement_timeout','4s'),('lock_timeout','1s'),('idle_in_transaction_session_timeout','6s')]:
   session.query("SET %s TO '%s';"%(setting,value),tag='SET',rows=0,width=0)
  pinned=inventory_source(session,state)
  state.revalidate();materials.check()
 except BaseException:
  session.close(False);raise
 session.close()
 # Each connection authenticates its own PID; only the other identity fields span sessions.
 return pinned[:4]+pinned[5:]
def observe_cleaned(state,library,materials,deadline,diag):
 pinned=cleaned_source_absence(state,library,materials,deadline,diag)
 checkpoint,guards=observe(state,library,materials,deadline,diag)
 need(cleaned_source_absence(state,library,materials,deadline,diag)==pinned,CONN)
 state.revalidate();materials.check()
 return checkpoint,guards
def cleaned_completion_main(argv):
 channel=diag=state=materials=None
 try:
  need(len(argv)==4 and argv[1]=='cleaned_completed_evidence',INPUT);op=hextext(argv[2],32);sha=hextext(argv[3],64)
  resource.setrlimit(resource.RLIMIT_CORE,(0,0));validate_environment(os.environ);need(os.environ.get('PGCONNECT_TIMEOUT')=='10',CAP_ERR)
  channel=InputChannel(op,time.monotonic()+30);channel.route();raw=channel.consume();deadline=time.monotonic()+25
  req=validate_cleaned_request(raw,op,sha);work_environment(req);diag=Diagnostics();need(os.geteuid()==65532)
  state=CleanedState(req);materials=Materialization(os.environ)
  resolver=StableFile('/etc/resolv.conf',CAP);materials.files.append(resolver);resolver.raw.decode('ascii');need(b'\0' not in resolver.raw,CAP_ERR)
  library=load_runtime();materials.prepare(library,deadline);qualified_resolver_policy();need(time.monotonic()<deadline,'timeout')
  checkpoint,guards=observe_cleaned(state,library,materials,deadline,diag)
  qualified_resolver_policy();resolver.check();materials.check();state.revalidate()
  materials.close();materials=None;resources=state.resources();state.close();state=None;diag.close();diag=None
  need(time.monotonic()<deadline,'timeout');request_sha,execution_sha=request_digests(raw)
  result=dict(version=1,kind='cleaned_completed_evidence',operation_id=op,program_sha256=sha,request_sha256=request_sha,execution_sha256=execution_sha,work=req['execution']['work'],checkpoint=checkpoint,resources=resources,guards=guards,source_closed=True,target_closed=True,materialization_closed=True,private_closed=True,diagnostics_clean=True,cleanup_complete=True,source_resources_absent=True)
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

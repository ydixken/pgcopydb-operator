# Read-only inventory policy.
def inventory_json(raw,cap):
 value=strict_json(raw,cap)
 def integer(s):
  need(s!='-0',INPUT);return int(s)
 json.loads(raw.decode('utf-8'),parse_int=integer)
 return value
def validate_inventory_request(raw,op,sha):
 value=inventory_json(raw,CAP);keys(value,'version kind operation_id program_sha256 execution pod_name container')
 need(uint(value['version'],1,True)==1 and value['kind']=='owned_inventory',INPUT)
 need(hextext(value['operation_id'],32)==op and hextext(value['program_sha256'],64)==sha,INPUT)
 execution=execution_data_fields(value);work_identity(execution['work'])
 need(execution['role']=='cleanup' and execution['job_uid']!=execution['worker_job_uid'],INPUT)
 return value
def validate_inventory_manifest(m,req,d):
 keys(m,'magic format_version run canonical_path directory_device directory_inode copy_complete target_provenance source_slot clone_state follow_state operator')
 work=req['execution']['work'];need(m['magic']=='RPGMIGRATE' and uint(m['format_version'],8)==8 and m['canonical_path']=='/work/pgcopydb')
 run=nonce(m['run']);need(run.hex()==work['run_id'] and (uint(m['directory_device']),uint(m['directory_inode']))==identity(d))
 operator=m['operator'];keys(operator,'migration_uid run workdir_id checkpoint_generation');uint(operator['checkpoint_generation'],1,True)
 need(same(operator,dict(migration_uid=work['migration_uid'],run=list(run),workdir_id=work['workdir_id'],checkpoint_generation=1)))
 if m['copy_complete'] is not None:
  keys(m['copy_complete'],'tables rows bytes')
  for count in m['copy_complete'].values(): uint(count)
 targets=bounded_array(m['target_provenance'],1);need(len(targets)==1)
 keys(targets[0],'phase record');need(targets[0]['phase']=='confirmed');target=targets[0]['record']
 keys(target,'intent schema_oid table_oid marker_table_oid marker_index_oid');keys(target['intent'],'binding nonce');nonce(target['intent']['nonce'])
 b=target['intent']['binding'];keys(b,'stream target_system_identifier target_database_oid schema_name table_name owner_oid metadata_version')
 stream(b['stream']);need(b['stream']['run']==m['run'])
 need(uint(b['metadata_version'],6,True)==6 and b['schema_name']=='_rpgmigrate_'+work['run_id'] and b['table_name']=='checkpoint')
 name(b['schema_name']);name(b['table_name']);uint(b['target_system_identifier'],U64,True)
 for key in ('target_database_oid','owner_oid'): uint(b[key],(1<<32)-1,True)
 for key in ('schema_oid','table_oid','marker_table_oid','marker_index_oid'): uint(target[key],(1<<32)-1,True)
 need(len({target[k] for k in ('table_oid','marker_table_oid','marker_index_oid')})==3)
 keys(m['source_slot'],'phase record');need(m['source_slot']['phase']=='confirmed');slot=m['source_slot']['record'];slot_record(slot)
 need(slot['intent']['binding']['stream']==b['stream'])
 clone=m['clone_state'];keys(clone,'plan ticket phase owned_objects owned_tables completion')
 need(clone['phase'] in 'fresh pre_data_pending copying post_data_pending post_data_complete sequences_pending sequences_complete analyzing complete'.split())
 p=clone['plan'];keys(p,'source target generation tables sequences policy')
 keys(p['source'],'identity timeline major');keys(p['target'],'identity major')
 source_identity(p['source']['identity']);source_identity(p['target']['identity'])
 need(p['source']['identity']==b['stream']['source'] and p['target']['identity']==dict(system_identifier=b['target_system_identifier'],database_oid=b['target_database_oid']))
 uint(p['source']['timeline'],(1<<32)-1,True);uint(p['generation'],U64,True)
 need(slot['intent']['binding']['catalog_epoch']==p['generation'])
 for side in ('source','target'): need(uint(p[side]['major'],18,True) in (16,18))
 need(p['source']['major']<=p['target']['major'])
 tables=bounded_array(p['tables'],4096);oids=set();names=set()
 for table in tables:
  table_plan(table);identifier=(table['name']['schema'],table['name']['table'])
  need(table['source_oid'] not in oids and identifier not in names);oids.add(table['source_oid']);names.add(identifier)
 for sequence in bounded_array(p['sequences'],65536): qualified(sequence)
 keys(p['policy'],'no_owner no_acl no_comments no_tablespaces')
 for flag in p['policy'].values(): boolean(flag)
 keys(clone['ticket'],'token source generation');text(clone['ticket']['token'],1024)
 need(same(clone['ticket']['source'],p['source']) and uint(clone['ticket']['generation'],U64,True)==p['generation'] and clone['ticket']['token']==slot['snapshot_token'])
 objects=bounded_array(clone['owned_objects'],65536);object_ids=set()
 for obj in objects:
  keys(obj,'class_oid object_oid object_sub_id namespace_oid namespace_name name kind owner_oid')
  for key in ('class_oid','object_oid','owner_oid'): uint(obj[key],(1<<32)-1,True)
  uint(obj['object_sub_id'],(1<<31)-1);text(obj['name'],1048576);text(obj['kind'],256,True)
  if obj['namespace_oid'] is not None: need(uint(obj['namespace_oid'],(1<<32)-1,True)!=target['schema_oid'])
  if obj['namespace_name'] is not None: text(obj['namespace_name'],1048576,True)
  need(obj['class_oid']!=2615 or obj['object_oid']!=target['schema_oid'])
  identifier=tuple(obj[k] for k in ('class_oid','object_oid','object_sub_id'));need(identifier not in object_ids);object_ids.add(identifier)
 owned=bounded_array(clone['owned_tables'],4096);early=clone['phase'] in ('fresh','pre_data_pending')
 need(not objects and not owned if early else len(owned)==len(tables));targets_seen=set();sources_seen=set()
 for obj in owned:
  keys(obj,'table target_oid target_namespace_oid target_owner_oid generation');table_plan(obj['table'])
  for key in ('target_oid','target_namespace_oid','target_owner_oid'): uint(obj[key],(1<<32)-1,True)
  need(uint(obj['generation'],U64,True)==p['generation'] and obj['table'] in tables)
  need(obj['target_oid'] not in targets_seen and obj['table']['source_oid'] not in sources_seen)
  targets_seen.add(obj['target_oid']);sources_seen.add(obj['table']['source_oid'])
  need(any(o['class_oid']==1259 and o['object_oid']==obj['target_oid'] and o['object_sub_id']==0 and o['namespace_oid']==obj['target_namespace_oid'] and o['namespace_name']==obj['table']['name']['schema'] and o['name']==obj['table']['name']['table'] and o['kind']=='r' and o['owner_oid']==obj['target_owner_oid'] for o in objects))
 need((clone['phase']=='complete')==(clone['completion'] is not None))
 if clone['completion'] is not None:
  keys(clone['completion'],'tables rows bytes')
  for count in clone['completion'].values(): uint(count)
  need(clone['completion']['tables']==len(owned))
 follow=m['follow_state'];keys(follow,'cleanup origin_name publication binding restart preliminary')
 need(follow['restart'] is None and follow['preliminary'] is None)
 if follow['cleanup'] is not None:
  keys(follow['cleanup'],'slot_removed publication_removed')
  for flag in follow['cleanup'].values(): boolean(flag)
 if follow['origin_name'] is not None: name(follow['origin_name'])
 fb=follow['binding'];keys(fb,'slot publication source_timeline copy_generation source_types');slot_record(fb['slot']);publication(fb['publication'])
 need(same(fb['slot'],slot) and uint(fb['copy_generation'],U64,True)==p['generation'] and uint(fb['source_timeline'],(1<<32)-1,True)==p['source']['timeline'])
 types=bounded_array(fb['source_types'],16384);types_seen=set()
 for item in types:
  keys(item,'oid schema name');oid=uint(item['oid'],(1<<32)-1,True);name(item['schema']);name(item['name'])
  need(oid not in types_seen);types_seen.add(oid)
 pub=follow['publication'];keys(pub,'phase intent receipt');need(pub['phase']=='confirmed')
 keys(pub['intent'],'run name nonce');nonce(pub['intent']['nonce'])
 need(same(pub['intent']['run'],m['run']) and pub['intent']['name']==fb['publication']['name'] and same(pub['receipt'],fb['publication']))
 for value,cap in ((fb,262144),(types,262144-8192),(dict(binding=fb,operator=operator,origin_name=follow['origin_name']),262144),(clone,1048576-4096)):
  need(len(json.dumps(value,separators=(',',':'),ensure_ascii=False).encode())<=cap)
 return target
class InventoryState:
 def __init__(self,req):
  self.req=req;self.fds=[];self.files=[];self.nodes=[]
  try:
   self.parent=self.open('/work',True);self.root=self.open('pgcopydb',True,self.parent)
   fcntl.flock(self.root,fcntl.LOCK_EX|fcntl.LOCK_NB)
   self.names=set(os.listdir(self.root));need(self.names in ({'manifest.json'},{'manifest.json','control'}))
   self.file=StableFile('manifest.json',1048576,True,self.root);self.files.append(self.file)
   self.manifest=inventory_json(self.file.raw,1048576)
   self.target=validate_inventory_manifest(self.manifest,req,os.fstat(self.root))
   self.control=None;self.control_names=set()
   if 'control' in self.names:
    self.control=self.open('control',True,self.root);self.control_names=set(os.listdir(self.control))
    need(self.control_names in (set(),{'lock'},{'lock','endpos.json'}))
    for path in sorted(self.control_names): self.files.append(StableFile(path,4096 if path=='endpos.json' else 0,True,self.control))
   self.revalidate()
  except BaseException:
   self.close(False);raise
 def open(self,path,d,parent=None):
  fd=fdopen(path,d,parent);self.fds.append(fd)
  self.nodes.append((fd,parent,path,stamp(private_type(fd,d))));return fd
 def revalidate(self):
  work_environment(self.req)
  for fd,parent,path,before in self.nodes:
   need(stamp(os.fstat(fd))==before and stamp(os.stat(path,dir_fd=parent,follow_symlinks=False))==before)
  need(set(os.listdir(self.root))==self.names)
  if self.control is not None: need(set(os.listdir(self.control))==self.control_names)
  for file in self.files: file.check()
 resources=PrivateState.resources
 close=SourceState.close
def inventory_source(session,state):
 p=state.manifest['clone_state']['plan'];follow=state.manifest['follow_state'];b=follow['binding']
 pinned=source_session_identity(session,p['source'])
 exact_true(session.query("SELECT @current_setting('fsync')='on' AND @current_setting('full_page_writes')='on';"))
 progress=follow['cleanup'] or dict(slot_removed=False,publication_removed=False)
 exact_true(session.query("SELECT count(*)<=1 AND NOT ($2::boolean AND count(*)>0) AND COALESCE(bool_and(s.plugin='pgoutput' AND s.slot_type='logical' AND s.datoid=$3::oid AND NOT s.temporary AND NOT s.active AND s.active_pid IS NULL AND COALESCE(@to_jsonb(s)->>'failover','false')='false' AND COALESCE(@to_jsonb(s)->>'synced','false')='false'),true) FROM @pg_replication_slots s WHERE s.slot_name=$1::name;",(b['slot']['intent']['binding']['slot_name'],progress['slot_removed'],p['source']['identity']['database_oid'])))
 pub=b['publication']
 exact_true(session.query("SELECT count(*)<=1 AND NOT ($2::boolean AND count(*)>0) AND COALESCE(bool_and(p.oid=$3::oid AND p.pubowner=$4::oid AND NOT p.puballtables AND p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate AND NOT p.pubviaroot AND NOT EXISTS(SELECT 1 FROM @pg_publication_namespace n WHERE n.pnpubid=p.oid) AND COALESCE(@to_jsonb(p)->>'pubgencols','n')='n'),true) FROM @pg_publication p WHERE p.pubname=$1::name;",(pub['name'],progress['publication_removed'],pub['oid'],pub['owner_oid'])))
 expected=[[t['source_oid'],t['name']['schema'],t['name']['table'],t['all_columns']] for t in sorted(p['tables'],key=lambda t:t['source_oid'])]
 exact_true(session.query("SELECT NOT EXISTS(SELECT 1 FROM @pg_publication WHERE pubname=$1::name) OR (count(*)<=4096 AND COALESCE(bool_and(p.rowfilter IS NULL AND NOT EXISTS(SELECT 1 FROM @pg_attribute a WHERE a.attrelid=p.oid AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated<>'')),true) AND COALESCE(@jsonb_agg(@jsonb_build_array(p.oid,p.schemaname,p.tablename,p.attnames) ORDER BY p.oid),'[]'::jsonb)=$2::jsonb) FROM (SELECT c.oid,p.* FROM @pg_publication_tables p JOIN @pg_namespace n ON n.nspname=p.schemaname JOIN @pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename WHERE p.pubname=$1::name ORDER BY c.oid LIMIT 4097) p;",(pub['name'],json.dumps(expected,separators=(',',':')))))
 return pinned
def inventory_target(session,state):
 target=state.target;b=target['intent']['binding'];p=state.manifest['clone_state']['plan']
 session_role(session)
 rows=session.query("SELECT c.system_identifier::@text,d.oid,r.oid,u.oid,@pg_backend_pid(),@current_setting('server_version_num'),NOT @pg_is_in_recovery() AND @current_setting('fsync')='on' AND @current_setting('full_page_writes')='on' FROM @pg_control_system() c CROSS JOIN @pg_database d CROSS JOIN @pg_roles r CROSS JOIN @pg_roles u WHERE d.datname=@current_database() AND r.rolname=CURRENT_USER AND u.rolname=SESSION_USER;",width=7)
 need(len(rows)==1 and len(rows[0])==7,RESULT);values=[v.decode('ascii') if type(v) is bytes else '' for v in rows[0]]
 need(values[:2]==[str(b['target_system_identifier']),str(b['target_database_oid'])] and values[4]==str(session.backend) and decimal(values[5],189999,True)//10000==p['target']['major'] and values[6]=='t',CONN)
 decimal(values[2],(1<<32)-1,True);decimal(values[3],(1<<32)-1,True)
 for marker in (False,True):
  params=(target['marker_table_oid'],target['marker_index_oid'],target['schema_oid'],b['owner_oid']) if marker else (target['schema_oid'],target['table_oid'],b['target_database_oid'],b['owner_oid'],b['schema_name'],b['table_name'],target['marker_table_oid'],target['marker_index_oid'])
  exact_true(session.query(catalog_sql(marker,p['target']['major']),params))
 return values
def observe_inventory(state,library,mat,deadline,diag):
 sessions=[]
 try:
  for role in ('source','target'):
   session=Session(library,mat.connection(role),min(deadline,time.monotonic()+10),diag);sessions.append(session);session.deadline=deadline
   for setting,milliseconds in (('statement_timeout',4000),('lock_timeout',1000),('idle_in_transaction_session_timeout',6000)):
    remaining=int((deadline-time.monotonic())*1000);need(remaining>0,'timeout')
    session.query("SET %s TO '%dms';"%(setting,min(milliseconds,remaining)),tag='SET',rows=0,width=0)
   session.query('SET search_path TO pg_catalog;',tag='SET',rows=0,width=0)
  pinned=(inventory_source(sessions[0],state),inventory_target(sessions[1],state))
  state.revalidate();mat.check();diag.check()
  need((inventory_source(sessions[0],state),inventory_target(sessions[1],state))==pinned,CONN)
  state.revalidate();mat.check();diag.check()
  while sessions: sessions.pop().close()
 except BaseException:
  for session in reversed(sessions):
   try: session.close(False)
   except BaseException: pass
  raise
def owned_inventory_main(argv):
 channel=diag=state=mat=None
 try:
  need(len(argv)==4 and argv[1]=='owned_inventory',INPUT);op=hextext(argv[2],32);sha=hextext(argv[3],64)
  resource.setrlimit(resource.RLIMIT_CORE,(0,0));validate_environment(os.environ);need(os.environ.get('PGCONNECT_TIMEOUT')=='10',CAP_ERR)
  channel=InputChannel(op,time.monotonic()+30);channel.route();raw=channel.consume();deadline=time.monotonic()+25
  req=validate_inventory_request(raw,op,sha);work_environment(req);diag=Diagnostics();need(os.geteuid()==65532)
  state=InventoryState(req);mat=Materialization(os.environ)
  resolver=StableFile('/etc/resolv.conf',CAP);mat.files.append(resolver);resolver.raw.decode('ascii');need(b'\0' not in resolver.raw,CAP_ERR)
  library=load_runtime();mat.prepare(library,deadline);qualified_resolver_policy();need(time.monotonic()<deadline,'timeout')
  observe_inventory(state,library,mat,deadline,diag);qualified_resolver_policy();resolver.check();mat.check();state.revalidate()
  mat.close();mat=None;resources=state.resources();state.close();state=None;diag.close();diag=None
  need(time.monotonic()<deadline,'timeout');request_sha,execution_sha=request_digests(raw)
  result=dict(version=1,kind='owned_inventory',operation_id=op,program_sha256=sha,request_sha256=request_sha,execution_sha256=execution_sha,work=req['execution']['work'],resources=resources,source_closed=True,target_closed=True,materialization_closed=True,private_closed=True,diagnostics_clean=True)
  output=json.dumps(result,separators=(',',':'),ensure_ascii=True).encode()+b'\n';need(len(output)<=CAP,RESULT)
  channel.finish(output,deadline);channel=None;return 0
 except BaseException:
  for item in (mat,state,diag):
   if item is not None:
    try: item.close()
    except BaseException: pass
  if channel is not None:
   try: channel.finish()
   except BaseException: pass
  return 1

# Fixed cleaned-completion policy for the existing one-use transport.
def ck(v,k):need(type(v)is dict and set(v)==set(k.split()))
def ch(v,n):need(type(v)is str and len(v)==n and all(c in '0123456789abcdef' for c in v))
def ct(v):need(type(v)is str and 0<len(v.encode())<=1024 and '\0'not in v)
def cj(r):
 v=strict_json(r)
 def n(s):need(s!='-0');return int(s)
 json.loads(r,parse_int=n);return v
def cl(v):
 ct(v);p=v.split('/');need(len(p)==2 and all(0<len(s)<=8 and all(c in '0123456789abcdefABCDEF'for c in s)for s in p));return(int(p[0],16)<<32)|int(p[1],16)
def cv(v,o):need(type(v['version'])is int and v['version']==1 and v['kind']=='cleaned_completed_evidence'and v['operation_id']==o)
def strict_body(r,o):
 v=cj(r);ck(v,'version kind operation_id program_sha256 execution pod_name container saved_e');cv(v,o);ch(v['program_sha256'],64);need(cl(v['saved_e'])>0)
 e=v['execution'];c=v['container'];w=e['work']
 ck(e,'namespace migration_name pvc_name job_name migration_uid pvc_uid job_uid worker_job_uid migration_generation attempt role work image_ref platform_image_id template_sha256 connection_reference_sha256')
 ck(c,'pod_uid container_name container_id restart_count image_id template_sha256');ck(w,'migration_uid run_id workdir_id checkpoint_generation')
 for d,skip in ((e,('work','attempt','migration_generation')),(c,('restart_count',)),(w,('checkpoint_generation',))):
  for k in d:
   if k not in skip:ct(d[k])
 for k in ('attempt','migration_generation'):need(type(e[k])is int and e[k]>0)
 need(e['role']=='evidence'and e['job_uid']!=e['worker_job_uid']and type(c['restart_count'])is int and c['restart_count']==0 and type(w['checkpoint_generation'])is int and w['checkpoint_generation']==1)
 need(w['migration_uid']==e['migration_uid']and w['workdir_id']==e['pvc_uid']and c['template_sha256']==e['template_sha256']and c['image_id']==e['platform_image_id']and c['container_name']=='pgcopydb')
 ch(w['run_id'],32);ch(e['template_sha256'],64);ch(e['connection_reference_sha256'],64);ct(v['pod_name']);return v
def result_envelope(r,b,o):
 need(r.endswith(b'\n')and r.count(b'\n')==1 and len(r)<=CAP)
 v=cj(r);q=strict_body(b,o);ck(v,'version kind operation_id program_sha256 request_sha256 execution_sha256 work checkpoint resources guards source_closed target_closed materialization_closed private_closed diagnostics_clean cleanup_complete source_resources_absent');cv(v,o)
 h=hashlib.sha256(b).digest();need(v['program_sha256']==q['program_sha256']and v['request_sha256']==h.hex()and v['execution_sha256']==hashlib.sha256(b'rpgmigrate.native.execution.v1\0'+h).hexdigest()and json.dumps(v['work'],sort_keys=True)==json.dumps(q['execution']['work'],sort_keys=True))
 for k in 'source_closed target_closed materialization_closed private_closed diagnostics_clean cleanup_complete source_resources_absent'.split():need(v[k]is True)
 need(v['guards']=='target_settings target_identity configure_durability target_fence idle_guard begin lock_checkpoint lock_marker catalog_checkpoint catalog_marker row_read same_value_update deferred_effects row_comparison precommit_guard commit idle_guard unlock closure_guard'.split())
 c=v['checkpoint'];ck(c,'checkpoint_id certified_lsn durable_replay_lsn drain_end_lsn drain_replay_lsn drain_kind barrier_confirmed')
 ct(c['checkpoint_id']);p=c['checkpoint_id'].split(':');need(len(p)==3 and p[0]=='1'and all(s.isascii()and s.isdecimal()and str(int(s))==s and int(s)<2**64 for s in p)and int(p[2])>0)
 a,d,e,w=(cl(c[k])for k in ('certified_lsn','durable_replay_lsn','drain_end_lsn','drain_replay_lsn'))
 need(c['barrier_confirmed']is True and d>=a and e==cl(q['saved_e'])and (c['drain_kind']=='keepalive'and e<=w<=a or c['drain_kind']=='later_begin'and w>e))
 a=v['resources'];need(type(a)is list and len(a)in(3,4));seen=set()
 for x in a:
  ck(x,'kind name owned retain');k=x['kind'];ct(x['name']);need(k not in seen);seen.add(k)
  need(k in('replication_slot','publication','workdir','replication_origin')and x['owned']is(k!='replication_origin')and x['retain']is(k=='workdir'))
  need(x['name']=='/work/pgcopydb'if k=='workdir'else len(x['name'].encode())<=63)
 need({'replication_slot','publication','workdir'}<=seen);return True

# Fixed inventory-only policy for the private writer.
def ik(v,k):need(type(v)is dict and set(v)==set(k.split()))
def ix(v):need(type(v)is str and 0<len(v.encode())<=1024 and '\0'not in v)
def ij(r):
 v=strict_json(r)
 def n(s):need(s!='-0');return int(s)
 json.loads(r,parse_int=n);return v
def iv(v,o):need(type(v['version'])is int and v['version']==1 and v['kind']=='owned_inventory'and v['operation_id']==o)
def strict_body(r,o):
 v=ij(r);ik(v,'version kind operation_id program_sha256 execution pod_name container')
 iv(v,o)
 h=v['program_sha256'];need(type(h)is str and len(h)==64 and all(c in '0123456789abcdef'for c in h))
 e=v['execution'];c=v['container'];w=e['work']
 ik(e,'namespace migration_name pvc_name job_name migration_uid pvc_uid job_uid worker_job_uid migration_generation attempt role work image_ref platform_image_id template_sha256 connection_reference_sha256')
 ik(c,'pod_uid container_name container_id restart_count image_id template_sha256');ik(w,'migration_uid run_id workdir_id checkpoint_generation')
 for d,s in ((e,('work','attempt','migration_generation')),(c,('restart_count',)),(w,('checkpoint_generation',))):
  for k in d:
   if k not in s:ix(d[k])
 for k in ('attempt','migration_generation'):need(type(e[k])is int and e[k]>0)
 need(e['role']=='cleanup' and e['job_uid']!=e['worker_job_uid'] and type(c['restart_count'])is int and c['restart_count']==0 and type(w['checkpoint_generation'])is int and w['checkpoint_generation']==1);ix(v['pod_name']);return v
def result_envelope(r,b,o):
 need(r.endswith(b'\n')and r.count(b'\n')==1 and len(r)<=CAP)
 v=ij(r);q=strict_body(b,o);ik(v,'version kind operation_id program_sha256 request_sha256 execution_sha256 work resources source_closed target_closed materialization_closed private_closed diagnostics_clean')
 iv(v,o);need(v['program_sha256']==q['program_sha256'])
 h=hashlib.sha256(b).digest();need(v['request_sha256']==h.hex()and v['execution_sha256']==hashlib.sha256(b'rpgmigrate.native.execution.v1\0'+h).hexdigest()and json.dumps(v['work'],sort_keys=True)==json.dumps(q['execution']['work'],sort_keys=True))
 for k in 'source_closed target_closed materialization_closed private_closed diagnostics_clean'.split():need(v[k]is True)
 a=v['resources'];need(type(a)is list and len(a)in(1,3,4));s=set()
 for x in a:
  ik(x,'kind name owned retain');k=x['kind'];ix(x['name']);need(k not in s);s.add(k)
  need(k in('replication_slot','publication','workdir','replication_origin')and x['owned']is(k!='replication_origin')and x['retain']is(k=='workdir'))
  need(x['name']=='/work/pgcopydb'if k=='workdir'else len(x['name'].encode())<=63)
 need(s=={'workdir'}or{'replication_slot','publication','workdir'}<=s);return True

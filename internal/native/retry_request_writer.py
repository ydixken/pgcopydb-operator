# Fixed target-retry policy for the private writer.
def rk(v,k):need(type(v)is dict and set(v)==set(k.split()))
def rx(v):need(type(v)is str and 0<len(v.encode())<=1024 and '\0'not in v)
def rh(v,n):need(type(v)is str and len(v)==n and all(c in '0123456789abcdef'for c in v))
def rj(r):
 v=strict_json(r)
 def n(s):need(s!='-0');return int(s)
 json.loads(r,parse_int=n);return v
def rv(v,o):need(type(v['version'])is int and v['version']==1 and v['kind']=='target_retry'and v['operation_id']==o)
def strict_body(r,o):
 v=rj(r);rk(v,'version kind operation_id program_sha256 execution pod_name container inputs')
 rv(v,o);rh(v['program_sha256'],64)
 e=v['execution'];c=v['container'];w=e['work'];i=v['inputs']
 rk(e,'namespace migration_name pvc_name job_name migration_uid pvc_uid job_uid worker_job_uid migration_generation attempt role work image_ref platform_image_id template_sha256 connection_reference_sha256')
 rk(c,'pod_uid container_name container_id restart_count image_id template_sha256');rk(w,'migration_uid run_id workdir_id checkpoint_generation');rk(i,'commit_test expect_local')
 for d,s in ((e,('work','attempt','migration_generation')),(c,('restart_count',)),(w,('checkpoint_generation',))):
  for k in d:
   if k not in s:rx(d[k])
 for k in ('attempt','migration_generation'):need(type(e[k])is int and e[k]>0)
 need(e['role']=='retry_gate'and e['job_uid']!=e['worker_job_uid']and type(c['restart_count'])is int and c['restart_count']==0 and type(w['checkpoint_generation'])is int and w['checkpoint_generation']==1)
 need(all(type(x)is bool for x in i.values()));rx(v['pod_name']);return v
def result_envelope(r,b,o):
 need(r.endswith(b'\n')and r.count(b'\n')==1 and len(r)<=CAP)
 v=rj(r);q=strict_body(b,o);rk(v,'version kind operation_id program_sha256 request_sha256 execution_sha256 work observed_at_unix census_known total syncrep gate_waiting oldest_syncrep_wait_unix application_attribution_known target_synchronous_commit synchronous_commit_admitted probe_attempted probe_committed target_closed materialization_closed diagnostics_clean')
 rv(v,o);need(v['program_sha256']==q['program_sha256'])
 h=hashlib.sha256(b).digest();need(v['request_sha256']==h.hex()and v['execution_sha256']==hashlib.sha256(b'rpgmigrate.native.execution.v1\0'+h).hexdigest()and json.dumps(v['work'],sort_keys=True)==json.dumps(q['execution']['work'],sort_keys=True))
 for k in 'observed_at_unix total syncrep gate_waiting oldest_syncrep_wait_unix'.split():need(type(v[k])is int and v[k]>=0)
 for k in 'application_attribution_known synchronous_commit_admitted probe_attempted probe_committed'.split():need(type(v[k])is bool)
 for k in 'census_known target_closed materialization_closed diagnostics_clean'.split():need(v[k]is True)
 need(v['target_synchronous_commit']in('on','off','local','remote_write','remote_apply'));return True

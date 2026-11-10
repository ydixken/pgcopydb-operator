import contextlib
import io
import json
import os
import pathlib
import signal
import sys
import tempfile
import types
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).parent
SCOPE = {'__name__': '_comparison_test'}
exec(compile((ROOT / 'evidence_program.py').read_bytes(), '<canonical>', 'exec'), SCOPE)
exec(compile((ROOT / 'comparison_program.py').read_bytes(), '<comparison>', 'exec'), SCOPE)

MEASUREMENT = {'rowcount': 1, 'checksum': '1234abcd'}
WORK = {'migration_uid': '01234567-89ab-cdef-0123-456789abcdef', 'run_id': '0123456789abcdef0123456789abcdef', 'workdir_id': 'pvc-owned', 'checkpoint_generation': 1}

def table(index, different=False):
 return dict(schema='public', name='table_' + str(index), source=MEASUREMENT, target=MEASUREMENT,
             status='different' if different else 'match')

def progress(count, differing):
 return dict(planned_tables=count, compared_tables=count, planned_bytes=0, compared_bytes=0,
             compared_rows=count, differing_tables=differing, failed_tables=0, elapsedMs=1,
             final=True, inFlightCount=0, inFlight=[])

def diagnostic():
 return dict(level='error', category='BadState', exit_code=3, error_severity='ERROR',
             error_origin=dict(file='lib.rs', line=181, column=9), message='native:BadState: comparison found differences')

class ComparisonTests(unittest.TestCase):
 def run_child(self, program, check='data'):
  with tempfile.TemporaryDirectory() as directory:
   fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
   try:
    result = SCOPE['capture_comparison']([sys.executable, '-I', '-B', '-c', program], check, 'a'*32, 'b'*64, WORK, fd, dict(os.environ))
    self.assertEqual(os.listdir(directory), [])
    return result
   finally: os.close(fd)

 def capture(self, report, check='data', exit_code=0, stderr=None, report_cap=None):
  if stderr is None:
   values = json.loads(report)
   if check == 'data':
    differing = sum(item['status'] != 'match' for item in values)
    stderr = 'compare progress: ' + json.dumps(progress(len(values), differing)) + '\n'
   else: stderr = ''
   if exit_code == 3: stderr += json.dumps(diagnostic()) + '\n'
  with tempfile.TemporaryDirectory() as directory:
   program = 'import sys;sys.stdout.buffer.write(' + repr(report) + ');sys.stdout.flush();sys.stderr.buffer.write(' + repr(stderr.encode()) + ');sys.stderr.flush();sys.exit(' + str(exit_code) + ')'
   fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
   old = SCOPE['COMPARE_REPORT_CAP']
   if report_cap is not None: SCOPE['COMPARE_REPORT_CAP'] = report_cap
   try:
    result = SCOPE['capture_comparison']([sys.executable, '-I', '-B', '-c', program], check, 'a'*32, 'b'*64, WORK, fd, dict(os.environ))
    self.assertEqual(os.listdir(directory), [])
    return result
   finally:
    SCOPE['COMPARE_REPORT_CAP'] = old
    os.close(fd)

 def test_large_report(self):
  for position in (0, 1000, 1999):
   values = [table(index, index == position) for index in range(2000)]
   report = json.dumps(values, separators=(',', ':')).encode() + b'\n'
   self.assertGreater(len(report), 65536)
   result = self.capture(report, exit_code=3)
   self.assertFalse(result['matched']); self.assertEqual(result['engine_exit'], 3)
   self.assertEqual(result['table_count'], 2000); self.assertEqual(result['report_bytes'], len(report))
   self.assertNotIn('public', json.dumps(result)); self.assertNotIn('1234abcd', json.dumps(result))
  self.assertTrue(self.capture(json.dumps([table(0)]).encode()+b'\n')['matched'])
  for matched in (True, False):
   result = self.capture(json.dumps(dict(matched=matched)).encode()+b'\n', check='schema', exit_code=0 if matched else 3)
   self.assertEqual(result['matched'], matched); self.assertEqual(result['table_count'], 0)

 def test_refusals(self):
  valid = json.dumps([table(0)]).encode()+b'\n'
  cases = [
   (b'[]\n', 0, None), (valid[:-1], 0, None), (valid+b'canary', 0, ''),
   (valid, 3, None), (valid, 3, '{"message":"private-canary"}\n'),
   (json.dumps([table(0, True)]).encode()+b'\n', 0, None),
   (json.dumps([table(0), table(0)]).encode()+b'\n', 0, None),
   (valid.replace(b'"rowcount": 1', b'"rowcount": null', 1), 0, ''),
   (valid.replace(b'"rowcount": 1', b'"rowcount": 1.0', 1), 0, ''),
   (valid.replace(b'"status": "match"', b'"status": "matched"'), 0, ''),
   (valid, 0, 'compare progress: '+json.dumps(progress(2,0))+'\n'),
   (valid, 0, 'private-diagnostic-canary\n'), (valid, 2, ''),
   (valid, 0, 'compare progress: '+json.dumps(progress(1,0))),
  ]
  for report, exit_code, stderr in cases:
   with self.subTest(report=report[:30], exit_code=exit_code):
    with self.assertRaises(Exception): self.capture(report, exit_code=exit_code, stderr=stderr)
  self.assertTrue(self.capture(valid, report_cap=len(valid))['matched'])
  with self.assertRaises(Exception): self.capture(valid, report_cap=len(valid)-1)

 def test_report_stream(self):
  for report in (b'[{', b'[true]\n', b'[[[]]]\n', b'[{"schema":"x","schema":"y"}]\n', b'[null]\n', b'\xff\n'):
   with self.subTest(report=report):
    with self.assertRaises(Exception): SCOPE['classify_comparison_report'](io.BytesIO(report), 'data')
  value = table(0, True); value['source'] = None
  self.assertEqual(SCOPE['classify_comparison_report'](io.BytesIO(json.dumps([value]).encode()), 'data'), (False, 1, 1))
  value['target'] = None
  with self.assertRaises(Exception): SCOPE['classify_comparison_report'](io.BytesIO(json.dumps([value]).encode()), 'data')

 def test_production_guard(self):
  output = io.StringIO()
  with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
   self.assertEqual(SCOPE['comparison_main'](['-', 'native_comparison', 'data', 'a'*32, 'b'*64]), 1)
  self.assertEqual(output.getvalue(), '')

 def test_main_resolver_policy(self):
  original_policy = SCOPE['qualified_resolver_policy']
  original_stat = os.stat
  for fault in (None, 'admission', 'closure', 'closed_policy'):
   events = []; output = []
   resolver = types.SimpleNamespace(raw=b'fixture')
   materials = types.SimpleNamespace(files=[], prepare=lambda *a:None, check=lambda:None, close=lambda:events.append('materials'))
   diag = types.SimpleNamespace(check=lambda:None, close=lambda:events.append('diagnostics'))
   def policy(retained):
    self.assertIs(retained, resolver); self.assertEqual(materials.files, [resolver])
    phase = 'admission' if not events else 'closure'; events.append(phase)
    if fault == 'closed_policy': original_policy(retained)
    if fault == phase: raise SCOPE['Refusal']('capability')
   def capture(*args):
    self.assertEqual(events, ['admission']); events.append('child'); return dict(matched=True)
   def write(fd, raw):
    self.assertEqual(fd, 1); output.append(raw); return len(raw)
   with tempfile.TemporaryDirectory() as directory, self.subTest(fault=fault), mock.patch.dict(SCOPE, comparison_work=lambda:WORK, Diagnostics=lambda:diag, Materialization=lambda *a:materials, StableFile=lambda *a:resolver, load_runtime=lambda:None, qualified_resolver_policy=policy, comparison_child_files=lambda *a:({}, ()), fdopen=lambda *a:os.open(directory, os.O_RDONLY | os.O_DIRECTORY), compare_tmp_budget=lambda *a:None, capture_comparison=capture), mock.patch.dict(os.environ, LANG='C', LC_ALL='C', PGCONNECT_TIMEOUT='10', clear=True), mock.patch.object(SCOPE['resource'], 'setrlimit'), mock.patch.object(os, 'geteuid', return_value=65532), mock.patch.object(os, 'stat', side_effect=lambda path, **kw:original_stat(directory if path=='/tmp' else path, **kw)), mock.patch.object(os, 'set_blocking'), mock.patch.object(os, 'write', side_effect=write):
    code = SCOPE['comparison_main'](['-', 'native_comparison', 'data', 'a'*32, 'b'*64])
   if fault is None:
    self.assertEqual(code, 0); self.assertEqual(events, ['admission', 'child', 'closure', 'materials', 'diagnostics'])
    self.assertEqual(b''.join(output), b'{"matched":true}\n')
   else:
    self.assertEqual(code, 1); self.assertEqual(output, [])
    self.assertEqual('child' in events, fault == 'closure')

 def test_report_cap(self):
  cap = SCOPE['COMPARE_REPORT_CAP']
  prefix = b'{"matched":true}'
  for extra in (0, 1):
   program = 'import sys\np='+repr(prefix)+'\nsys.stdout.buffer.write(p)\nn='+str(cap-len(prefix)-1+extra)+'\nwhile n:\n b=min(n,8192);sys.stdout.buffer.write(b" "*b);n-=b\nsys.stdout.buffer.write(b"\\n")\n'
   if extra:
    with self.assertRaises(Exception): self.run_child(program, 'schema')
   else:
    result = self.run_child(program, 'schema')
    self.assertEqual(result['report_bytes'], cap); self.assertTrue(result['matched'])

 def test_private_closure(self):
  program = 'import subprocess,sys\nsubprocess.Popen([sys.executable,"-c","import time;time.sleep(.1)"],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)\nsys.stdout.write(\'{"matched":true}\\n\')\n'
  with self.assertRaises(Exception): self.run_child(program, 'schema')
  with tempfile.TemporaryDirectory() as directory:
   path = pathlib.Path(directory) / 'pgpass'
   path.write_bytes(b'x' * (SCOPE['COMPARE_TMP_RESERVE'] + 1)); path.chmod(0o600)
   fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
   try:
    with self.assertRaises(Exception):
     SCOPE['capture_comparison']([sys.executable, '-c', 'print(\'{"matched":true}\')'], 'schema', 'a'*32, 'b'*64, WORK, fd, dict(os.environ))
    self.assertEqual(os.listdir(directory), ['pgpass'])
   finally: os.close(fd)
  original_os = SCOPE['os']
  class ClosingReader:
   def __init__(self, reader): self.reader = reader
   def __enter__(self): return self.reader
   def __exit__(self, *args):
    self.reader.close()
    raise OSError('private-report-close-canary')
  class FileAPI:
   def __getattr__(self, name): return getattr(original_os, name)
   def fdopen(self, *args): return ClosingReader(original_os.fdopen(*args))
  SCOPE['os'] = FileAPI()
  try:
   with self.assertRaises(Exception): self.run_child('print(\'{"matched":true}\')', 'schema')
   self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))
  finally: SCOPE['os'] = original_os

 def test_phase_lifetime(self):
  original = signal.getsignal(signal.SIGALRM)
  for failure in ('expiry', 'close failure'):
   with self.assertRaises(Exception):
    with SCOPE['comparison_phase']():
     self.assertGreater(signal.getitimer(signal.ITIMER_REAL)[0], 0)
     if failure == 'expiry': signal.getsignal(signal.SIGALRM)(signal.SIGALRM, None)
     else: raise OSError('private-close-canary')
   self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))
   self.assertIs(signal.getsignal(signal.SIGALRM), original)

 def test_selected_fds_and_long_scan(self):
  import fcntl
  with tempfile.TemporaryFile() as selected, tempfile.TemporaryFile() as unselected:
   selected.write(b'private-selected-file'); selected.flush()
   other = fcntl.fcntl(unselected.fileno(), fcntl.F_DUPFD_CLOEXEC, 100)
   path = b'/proc/self/fd/' + str(selected.fileno()).encode()
   snapshot = types.SimpleNamespace(path=path, fd=selected.fileno(), check=lambda: None)
   connections = {role: types.SimpleNamespace(check=lambda: None, snapshots=[snapshot], overrides={b'passfile':path,b'sslcert':path}) for role in ('source','target')}
   original_env = dict(os.environ)
   materials = types.SimpleNamespace(env=original_env, connection=lambda role:connections[role])
   env, descriptors = SCOPE['comparison_child_files'](materials)
   self.assertEqual(descriptors, (selected.fileno(),)); self.assertEqual(materials.env, original_env)
   for name in SCOPE['COMPARE_FILE_ENVS']:
    self.assertEqual(json.loads(env[name]), dict(passfile=selected.fileno(), sslcert=selected.fileno()))
   clock = SCOPE['time']; process = SCOPE['subprocess']; offset = [0]
   def launch(*args, **kwargs):
    self.assertEqual(kwargs['pass_fds'], descriptors)
    self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))
    child = process.Popen(*args, **kwargs); offset[0] = 3600
    return child
   SCOPE['time'] = types.SimpleNamespace(monotonic=lambda:clock.monotonic()+offset[0])
   SCOPE['subprocess'] = types.SimpleNamespace(Popen=launch, DEVNULL=process.DEVNULL, PIPE=process.PIPE)
   try:
    connection = SCOPE['ConnectionInput'].__new__(SCOPE['ConnectionInput'])
    connection.closed=False; connection.deadline=float('inf'); connection.materialization=types.SimpleNamespace(check=lambda:None)
    connection.snapshots=[]; connection.absent=False
    with SCOPE['comparison_phase'](): connection.check()
    program = 'import os,sys\nos.fstat('+str(selected.fileno())+')\ntry:os.fstat('+str(other)+')\nexcept OSError:pass\nelse:sys.exit(2)\nsys.stdout.write(\'{"matched":true}\\n\')\n'
    with tempfile.TemporaryDirectory() as directory:
     fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
     try:
      result = SCOPE['capture_comparison']([sys.executable,'-I','-B','-c',program], 'schema', 'a'*32, 'b'*64, WORK, fd, env, descriptors)
     finally: os.close(fd)
    with SCOPE['comparison_phase'](): connection.check()
    self.assertTrue(result['matched']); self.assertEqual(offset[0], 3600)
    self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))
   finally:
    SCOPE['time']=clock; SCOPE['subprocess']=process; os.close(other)

if __name__ == '__main__': unittest.main()

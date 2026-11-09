import signal
import selectors
import contextlib

COMPARE_REPORT_CAP = 60 * 1024 * 1024
COMPARE_TMP_RESERVE = 3 * 1024 * 1024
COMPARE_CLOSE_SECONDS = 30
COMPARE_FILE_ENVS = ('RPGMIGRATE_INTERNAL_COMPARE_SOURCE_FILES', 'RPGMIGRATE_INTERNAL_COMPARE_TARGET_FILES')

@contextlib.contextmanager
def comparison_phase():
 need(signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0), CAP_ERR)
 def expired(number, frame): raise Refusal('timeout')
 original = signal.signal(signal.SIGALRM, expired)
 try:
  signal.setitimer(signal.ITIMER_REAL, 25)
  yield
 finally:
  try: signal.setitimer(signal.ITIMER_REAL, 0)
  finally: signal.signal(signal.SIGALRM, original)

def compare_json():
 def pairs(items):
  value = {}
  for key, item in items:
   need(key not in value, RESULT); value[key] = item
  return value
 def integer(value):
  number = int(value)
  need(str(number) == value, RESULT)
  return number
 def invalid(value): raise Refusal(RESULT)
 return json.JSONDecoder(object_pairs_hook=pairs, parse_int=integer,
                         parse_float=invalid, parse_constant=invalid)

def compare_uint(value, maximum=(1 << 63)-1):
 need(type(value) is int and 0 <= value <= maximum, RESULT)
 return value

def compare_keys(value, expected):
 need(type(value) is dict and set(value) == set(expected.split()), RESULT)

def compare_name(value):
 need(type(value) is str and 0 < len(value.encode('utf-8')) <= 63 and '\0' not in value, RESULT)

class ComparisonReport:
 def __init__(self, reader):
  self.reader = reader; self.buffer = ''; self.eof = False
  self.decoder = compare_json()
  import codecs
  self.utf8 = codecs.getincrementaldecoder('utf-8')('strict')
 def fill(self):
  block = self.reader.read(8192)
  self.eof = not block
  self.buffer += self.utf8.decode(block, self.eof)
  need(len(self.buffer.encode('utf-8')) <= CAP + 8192, RESULT)
 def space(self):
  while True:
   self.buffer = self.buffer.lstrip(' \t\r\n')
   if self.buffer or self.eof: return
   self.fill()
 def character(self, char):
  self.space(); need(self.buffer.startswith(char), RESULT); self.buffer = self.buffer[1:]
 def value(self):
  self.space()
  while True:
   try:
    value, offset = self.decoder.raw_decode(self.buffer)
    need(len(self.buffer[:offset].encode('utf-8')) <= CAP, RESULT)
    self.buffer = self.buffer[offset:]
    return value
   except json.JSONDecodeError:
    need(not self.eof and len(self.buffer.encode('utf-8')) <= CAP, RESULT)
    self.fill()
 def finish(self):
  self.space(); need(self.eof and not self.buffer, RESULT)

def compare_measurement(value):
 if value is None: return
 compare_keys(value, 'rowcount checksum')
 compare_uint(value['rowcount'], U64)
 hextext(value['checksum'], 8)

def classify_comparison_report(reader, check):
 report = ComparisonReport(reader)
 if check == 'schema':
  value = report.value(); compare_keys(value, 'matched')
  need(type(value['matched']) is bool, RESULT); report.finish()
  return value['matched'], 0, 0
 need(check == 'data', INPUT)
 report.character('['); seen = set(); differing = 0
 report.space(); need(not report.buffer.startswith(']'), RESULT)
 while True:
  value = report.value(); compare_keys(value, 'schema name source target status')
  compare_name(value['schema']); compare_name(value['name'])
  key = (value['schema'], value['name']); need(key not in seen, RESULT); seen.add(key)
  source, target = value['source'], value['target']
  compare_measurement(source); compare_measurement(target)
  need(source is not None or target is not None, RESULT)
  status = value['status']; need(status in ('match', 'different'), RESULT)
  if status == 'match': need(source is not None and target is not None and source == target, RESULT)
  else: differing += 1
  report.space()
  if report.buffer.startswith(']'):
   report.character(']'); break
  report.character(',')
 report.finish()
 return differing == 0, len(seen), differing

class ComparisonDiagnostics:
 def __init__(self, check):
  self.check = check; self.buffer = bytearray(); self.difference = False; self.final = None
 def frame(self, raw):
  prefix = b'compare progress: '
  if raw.startswith(prefix):
   need(self.check == 'data' and self.final is None and not self.difference, RESULT)
   value = compare_json().decode(raw[len(prefix):].decode('utf-8'))
   compare_keys(value, 'planned_tables compared_tables planned_bytes compared_bytes compared_rows differing_tables failed_tables elapsedMs final inFlightCount inFlight')
   for field in ('planned_tables', 'compared_tables', 'planned_bytes', 'compared_bytes', 'compared_rows', 'differing_tables', 'failed_tables', 'elapsedMs', 'inFlightCount'):
    compare_uint(value[field])
   need(value['compared_tables'] <= value['planned_tables'] and value['compared_bytes'] <= value['planned_bytes'] and value['differing_tables'] <= value['compared_tables'] and value['failed_tables'] == 0, RESULT)
   need(type(value['final']) is bool and type(value['inFlight']) is list and len(value['inFlight']) <= 1 and len(value['inFlight']) == value['inFlightCount'], RESULT)
   for item in value['inFlight']:
    compare_keys(item, 'schema name bytes elapsedMs'); compare_name(item['schema']); compare_name(item['name'])
    compare_uint(item['bytes']); compare_uint(item['elapsedMs'])
   if value['final']:
    need(not value['inFlight'] and value['compared_tables'] == value['planned_tables'] and value['compared_bytes'] == value['planned_bytes'], RESULT)
    self.final = value
  else:
   need(not self.difference, RESULT)
   value = compare_json().decode(raw.decode('utf-8'))
   compare_keys(value, 'level category exit_code error_severity error_origin message')
   need(value['level'] == 'error' and value['category'] == 'BadState' and type(value['exit_code']) is int and value['exit_code'] == 3 and value['error_severity'] == 'ERROR' and value['message'] == 'native:BadState: comparison found differences', RESULT)
   origin = value['error_origin']; compare_keys(origin, 'file line column')
   need(origin['file'] == 'lib.rs' and compare_uint(origin['line']) > 0 and compare_uint(origin['column']) > 0, RESULT)
   self.difference = True
 def feed(self, block):
  self.buffer.extend(block)
  while b'\n' in self.buffer:
   frame, _, remaining = self.buffer.partition(b'\n'); self.buffer = bytearray(remaining)
   need(0 < len(frame) <= CAP, RESULT); self.frame(frame)
  need(len(self.buffer) <= CAP, RESULT)
 def finish(self, matched, count, differing, exit_code):
  need(not self.buffer and exit_code == (0 if matched else 3) and self.difference == (not matched), RESULT)
  if self.check == 'data':
   need(self.final is not None and self.final['planned_tables'] == count and self.final['differing_tables'] == differing, RESULT)

def compare_tmp_budget(directory):
 total = 0
 for name in os.listdir(directory):
  info = os.stat(name, dir_fd=directory, follow_symlinks=False)
  need(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and info.st_nlink == 1 and stat.S_IMODE(info.st_mode) == 0o600, CAP_ERR)
  total += info.st_size
  need(total <= COMPARE_TMP_RESERVE, CAP_ERR)

def capture_comparison(argv, check, operation_id, program_sha256, work, directory, env, pass_fds=()):
 report_fd = None; child = None; selector = None; name = 'rpgmigrate-compare-' + operation_id
 group = None
 directory = os.dup(directory)
 old_handlers = {}
 def cancelled(number, frame): raise Refusal(CLOSE)
 try:
  for number in (signal.SIGINT, signal.SIGTERM):
   old_handlers[number] = signal.signal(number, cancelled)
  compare_tmp_budget(directory)
  before_names = set(os.listdir(directory))
  report_fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
  original = os.fstat(report_fd); size = 0; diagnostics = ComparisonDiagnostics(check)
  need(signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0), CAP_ERR)
  child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           env=env, close_fds=True, pass_fds=pass_fds, start_new_session=True)
  group = child.pid
  selector = selectors.DefaultSelector()
  for stream in (child.stdout, child.stderr):
   os.set_blocking(stream.fileno(), False); selector.register(stream, selectors.EVENT_READ)
  closing = None
  while selector.get_map():
   if child.poll() is not None:
    if closing is None: closing = time.monotonic() + COMPARE_CLOSE_SECONDS
    need(time.monotonic() < closing, CLOSE)
   for event, _ in selector.select(.1):
    stream = event.fileobj; block = os.read(stream.fileno(), 8192)
    if not block:
     selector.unregister(stream); stream.close(); continue
    if stream is child.stderr: diagnostics.feed(block)
    else:
     need(size + len(block) <= COMPARE_REPORT_CAP, RESULT)
     at = 0
     while at < len(block):
      count = os.write(report_fd, block[at:]); need(count > 0, RESULT); at += count
     size += len(block)
  selector.close(); selector = None
  exit_code = child.wait(timeout=COMPARE_CLOSE_SECONDS); child = None
  try:
   os.killpg(group, 0)
  except ProcessLookupError: group = None
  need(group is None, CLOSE)
  with comparison_phase():
   info = os.fstat(report_fd)
   need(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o600 and info.st_nlink == 1 and identity(info) == identity(original) and info.st_size == size and size > 0, RESULT)
   need(stamp(os.stat(name, dir_fd=directory, follow_symlinks=False)) == stamp(info), RESULT)
   os.lseek(report_fd, -1, os.SEEK_END); need(os.read(report_fd, 1) == b'\n', RESULT)
   os.lseek(report_fd, 0, os.SEEK_SET)
   with os.fdopen(os.dup(report_fd), 'rb') as reader:
    matched, count, differing = classify_comparison_report(reader, check)
   diagnostics.finish(matched, count, differing, exit_code)
   need(stamp(os.fstat(report_fd)) == stamp(info) and stamp(os.stat(name, dir_fd=directory, follow_symlinks=False)) == stamp(info), RESULT)
   need(set(os.listdir(directory)) == before_names | {name}, CLOSE)
   os.unlink(name, dir_fd=directory); need(os.fstat(report_fd).st_nlink == 0, CLOSE)
   checked_close(report_fd); report_fd = None
   need(set(os.listdir(directory)) == before_names, CLOSE)
   checked_close(directory); directory = None
   return dict(version=1, kind='native_comparison', operation_id=operation_id, program_sha256=program_sha256,
               work=work, check=check, matched=matched, engine_exit=exit_code, report_bytes=size,
               table_count=count, child_closed=True, report_closed=True, private_closed=True, diagnostics_clean=True)
 finally:
  try:
   with comparison_phase():
    if child is not None:
     try:
      if child.poll() is None:
       need(os.getpgid(child.pid) == child.pid, CLOSE)
       os.killpg(child.pid, signal.SIGKILL)
      child.wait(timeout=COMPARE_CLOSE_SECONDS)
     except BaseException: pass
     for stream in (child.stdout, child.stderr):
      if stream is not None: stream.close()
    if selector is not None: selector.close()
    if report_fd is not None:
     try:
      if identity(os.stat(name, dir_fd=directory, follow_symlinks=False)) == identity(os.fstat(report_fd)):
       os.unlink(name, dir_fd=directory)
     finally: checked_close(report_fd)
    if directory is not None: checked_close(directory)
  finally:
   for number, handler in old_handlers.items(): signal.signal(number, handler)

def comparison_child_files(materials):
 env = dict(materials.env); selected = set()
 need(not any(name in env for name in COMPARE_FILE_ENVS), INPUT)
 for role, name in zip(('source', 'target'), COMPARE_FILE_ENVS):
  connection = materials.connection(role); connection.check(); mapping = {}
  snapshots = {snapshot.path: snapshot for snapshot in connection.snapshots}
  for option, path in connection.overrides.items():
   need(option in FILE_OPTIONS, CAP_ERR)
   if path in snapshots:
    snapshot = snapshots[path]; snapshot.check()
    need(type(snapshot.fd) is int and snapshot.fd >= 3 and not os.get_inheritable(snapshot.fd), CAP_ERR)
    mapping[option.decode('ascii')] = snapshot.fd; selected.add(snapshot.fd)
   else:
    need(path == stable_absence(), CAP_ERR); mapping[option.decode('ascii')] = None
  value = json.dumps(mapping, separators=(',', ':'), ensure_ascii=True)
  need(len(value.encode('ascii')) <= 1024, CAP_ERR); env[name] = value
 return env, tuple(sorted(selected))

def comparison_work():
 uid = text(os.environ.get('RPGMIGRATE_MIGRATION_UID'), 36)
 run = hextext(os.environ.get('RPGMIGRATE_RUN_ID'), 32)
 need(len(uid) == 36 and [i for i,c in enumerate(uid) if c == '-'] == [8,13,18,23] and uid.replace('-', '') == run and any(c != '0' for c in run), INPUT)
 workdir = text(os.environ.get('RPGMIGRATE_WORKDIR_ID'), 256)
 need(not any(c in '/\\' or ord(c) < 33 or ord(c) > 126 for c in workdir) and bool(workdir.strip('0-')), INPUT)
 generation = decimal(os.environ.get('RPGMIGRATE_CHECKPOINT_GENERATION'), (1 << 63)-1, True)
 return dict(migration_uid=uid, run_id=run, workdir_id=workdir, checkpoint_generation=generation)

def comparison_main(argv):
 materials = None; diag = None; directory = None
 try:
  need(len(argv) == 5 and argv[1] == 'native_comparison' and argv[2] in ('schema', 'data'), INPUT)
  hextext(argv[3], 32); hextext(argv[4], 64)
  with comparison_phase():
   resource.setrlimit(resource.RLIMIT_CORE, (0,0)); validate_environment(os.environ)
   need(os.geteuid() == 65532 and os.environ.get('PGCONNECT_TIMEOUT') == '10', CAP_ERR)
   need(sum(len(os.fsencode(k)) + len(os.fsencode(v)) + 2 for k,v in os.environ.items()) <= CAP, INPUT)
   work = comparison_work(); diag = Diagnostics(); materials = Materialization(os.environ)
   resolver = StableFile('/etc/resolv.conf', CAP); materials.files.append(resolver)
   resolver.raw.decode('ascii'); need(b'\0' not in resolver.raw, CAP_ERR)
   library = load_runtime()
   # These owners survive a scan; each operation on them has a separate phase alarm.
   materials.prepare(library, float('inf'))
   qualified_resolver_policy(); materials.check(); diag.check()
   env, selected = comparison_child_files(materials)
   directory = fdopen('/tmp', True); tmp_identity = identity(os.fstat(directory))
   compare_tmp_budget(directory)
  check, operation_id, program_sha256 = argv[2:]
  args = ['/usr/local/bin/rpgmigrate', 'compare', check]
  if check == 'data': args.append('--json')
  args += ['--dir', '/work/pgcopydb']
  result = capture_comparison(args, check, operation_id, program_sha256, work, directory, env, selected)
  with comparison_phase():
   need(identity(os.fstat(directory)) == tmp_identity == identity(os.stat('/tmp', follow_symlinks=False)), CLOSE)
   materials.check(); qualified_resolver_policy(); diag.check()
   materials.close(); materials = None
   checked_close(directory); directory = None
   diag.close(); diag = None
  with comparison_phase():
   output = json.dumps(result, separators=(',', ':'), ensure_ascii=True).encode('ascii') + b'\n'
   need(0 < len(output) <= CAP, RESULT); os.set_blocking(1, False); at = 0; deadline = time.monotonic() + 25
   while at < len(output):
    try:
     count = os.write(1, output[at:]); need(count > 0, CLOSE); at += count
    except BlockingIOError: wait_fd(1, True, deadline)
  return 0
 except BaseException:
  return 1
 finally:
  if materials is not None or directory is not None or diag is not None:
   try:
    with comparison_phase():
     if materials is not None: materials.close()
     if directory is not None: checked_close(directory)
     if diag is not None: diag.close()
   except BaseException: pass

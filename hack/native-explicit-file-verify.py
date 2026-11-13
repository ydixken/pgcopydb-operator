#!/usr/bin/env python3
"""In-container verifier for the native explicit-file binding (M6 S1).

It runs inside the exact runner image and drives the real evidence_program.py classes against
stock libpq18. hack/native-explicit-file-qualify.py starts it; output is outcome labels and
counts only, never credentials, paths of fixtures or libpq text.
"""
import ctypes as C
import errno
import fcntl
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import time


def load(path, sha):
    with open(path, 'rb') as handle:
        raw = handle.read()
    if hashlib.sha256(raw).hexdigest() != sha:
        raise SystemExit('evidence program digest mismatch')
    spec = importlib.util.spec_from_file_location('native_evidence_program', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def outcome(P, action):
    try:
        action()
        return 'ok'
    except P.Refusal as error:
        return 'refused:' + error.error_class
    except OSError as error:
        return 'errno:' + errno.errorcode.get(error.errno, '?')


def deadline(seconds=25):
    return time.monotonic() + seconds


def private_file(path, raw, mode=0o600):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
    try:
        os.write(fd, raw)
        os.fchmod(fd, mode)
    finally:
        os.close(fd)


class Checks:
    def __init__(self):
        self.rows = []

    def add(self, name, observed, expected):
        # Byte values are reduced to their length so no fixture byte reaches the receipt.
        shown = json.loads(json.dumps(observed, default=lambda value: '<%d bytes>' % len(value)))
        self.rows.append({'case': name, 'observed': shown, 'pass': observed == expected})

    def report(self, **extra):
        passed = sum(row['pass'] for row in self.rows)
        print(json.dumps(dict(extra, cases=self.rows, total=len(self.rows), passed=passed), sort_keys=True))
        return 0 if self.rows and passed == len(self.rows) else 1


def errno_of(action):
    try:
        action()
        return 'ok'
    except OSError as error:
        return errno.errorcode.get(error.errno, '?')
    except ValueError:
        return 'ValueError'


def stage_library(P):
    c = Checks()
    library = P.load_runtime()
    c.add('runtime_metadata_and_abi_180007', library.PQlibVersion(), 180007)

    parse = lambda raw: P.parse_connection_options(library, raw)
    uri = parse(b'postgresql://u%40x:p%2Bw@localhost:5432/db%20n?application_name=a+b&options=-c%20x%3Dy')
    c.add('uri_percent_and_literal_plus', [uri[k][0] for k in (b'user', b'password', b'dbname', b'application_name', b'options')],
          [b'u@x', b'p+w', b'db n', b'a+b', b'-c x=y'])
    dsn = parse(b"host=localhost dbname='a b\\'c' passfile=''")
    c.add('dsn_quote_backslash_and_explicit_empty', [dsn[b'dbname'][0], dsn[b'passfile'][0]], [b"a b'c", b''])
    c.add('newline_and_trailing_space', parse(b"dbname='x\n ' user=u")[b'dbname'][0], b'x\n ')
    c.add('duplicate_keyword_last_wins', parse(b'dbname=a dbname=b')[b'dbname'][0], b'b')
    env = {b'PGPASSFILE': b'/env/passfile'}
    c.add('explicit_empty_beats_environment', P.selected_option(dsn, b'passfile', env), b'')
    c.add('environment_when_not_explicit', P.selected_option(parse(b'host=h'), b'passfile', env), b'/env/passfile')
    c.add('unset_environment_uses_compiled', P.selected_option(parse(b'host=h'), b'passfile', {}), None)
    c.add('equals_in_dbname_not_expanded', parse(b"dbname='host=evil port=1'")[b'host'][0], None)
    c.add('malformed_refused', outcome(P, lambda: parse(b"dbname='unterminated")), 'refused:connection')
    for _ in range(500):
        parse(b'postgresql://u@h/d?sslmode=require')
    c.add('parse_allocation_loop', 'ok', 'ok')

    materials = P.Materialization({'PGCOPYDB_SOURCE_PGURI': 'x'}, source_only=True)
    home = {b'HOME': b'/work'}
    start = lambda raw, envb=home: P.ConnectionInput(library, raw, envb, materials, deadline()).close()
    for name, raw, envb in (
            ('explicit_service', b'service=x sslmode=disable password=p', home),
            ('empty_pgservice_is_active', b'sslmode=disable password=p', {**home, b'PGSERVICE': b''}),
            ('sslrootcert_system', b'sslmode=verify-full sslrootcert=system password=p', home),
            ('sslcrldir', b'sslmode=require sslcrldir=/x password=p', home),
            ('engine_sslkey', b'sslmode=require sslkey=engine:key password=p', home),
            ('openssl_conf_override', b'sslmode=disable password=p', {**home, b'OPENSSL_CONF': b'/x'}),
            ('sslkeylogfile', b'sslmode=require sslkeylogfile=/tmp/k password=p', home),
            ('missing_home_unqualified_nss', b'sslmode=require password=p', {})):
        c.add('preflight_' + name, outcome(P, lambda: start(raw, envb)), 'refused:capability')
    c.add('preflight_path_at_maxpgpath', outcome(P, lambda: start(b'sslmode=require password=p sslrootcert=/' + b'a' * 1023)),
          'refused:capability')
    c.add('stage1_no_network_namespace_route', errno_of(lambda: __import__('socket').create_connection(('192.0.2.1', 9), 1)),
          'ENETUNREACH')
    materials.close()
    return c, library


def stage_memfd(P, c):
    raw = b'passfile-bytes:not-a-credential\n'
    snapshot = P.SealedFile(raw, deadline())
    info = os.fstat(snapshot.fd)
    c.add('memfd_mode_0400', oct(info.st_mode & 0o7777), '0o400')
    c.add('memfd_owner_euid_65532', [info.st_uid, os.geteuid()], [65532, 65532])
    c.add('memfd_anonymous_nlink_0', info.st_nlink, 0)
    want = fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL
    c.add('memfd_seals_write_grow_shrink_seal', fcntl.fcntl(snapshot.fd, fcntl.F_GET_SEALS) & want, want)
    c.add('memfd_cloexec', os.get_inheritable(snapshot.fd), False)
    c.add('memfd_path_is_procfd', snapshot.path.startswith(b'/proc/self/fd/'), True)
    c.add('sealed_write_refused', errno_of(lambda: os.pwrite(snapshot.fd, b'x', 0)), 'EPERM')
    c.add('sealed_grow_refused', errno_of(lambda: os.ftruncate(snapshot.fd, len(raw) + 1)), 'EPERM')
    c.add('sealed_shrink_refused', errno_of(lambda: os.ftruncate(snapshot.fd, 0)), 'EPERM')
    c.add('sealed_shared_writable_map_refused', errno_of(lambda: __import__('mmap').mmap(snapshot.fd, len(raw))), 'EPERM')
    c.add('second_fd_open_writable_refused', errno_of(lambda: os.close(os.open(snapshot.path, os.O_RDWR))), 'EACCES')
    second = os.open(snapshot.path, os.O_RDONLY | os.O_CLOEXEC)
    c.add('second_fd_read_identical', os.pread(second, 4096, 0) == raw, True)
    os.close(second)
    c.add('add_seal_after_seal_refused', errno_of(lambda: fcntl.fcntl(snapshot.fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_FUTURE_WRITE)), 'EPERM')
    c.add('procfd_exec_refused', errno_of(lambda: os.execv(snapshot.path, [b'x'])), 'EACCES')
    c.add('noexec_seal_exposed', hasattr(os, 'MFD_NOEXEC_SEAL'), False)
    c.add('check_passes', outcome(P, snapshot.check), 'ok')
    snapshot.close()
    c.add('closed_snapshot_check_refused', outcome(P, snapshot.check), 'refused:capability')
    empty = P.SealedFile(b'', deadline())
    c.add('empty_file_is_empty_snapshot', os.fstat(empty.fd).st_size, 0)
    empty.close()
    c.add('oversize_snapshot_refused', outcome(P, lambda: P.SealedFile(b'x' * 1048577, deadline())), 'refused:capability')
    c.add('expired_budget_refused', outcome(P, lambda: P.SealedFile(raw, time.monotonic() - 1)), 'refused:timeout')


def stage_absence(P, c, library):
    c.add('stable_absence_route', outcome(P, P.stable_absence), 'ok')
    c.add('absent_path_absent', errno_of(lambda: os.lstat(P.ABSENT_FILE)), 'ENOENT')
    c.add('absent_ancestor_readonly', bool(os.statvfs('/usr/share/rpgmigrate').f_flag & os.ST_RDONLY), True)
    os.makedirs('/tmp/fx', 0o700)
    private_file('/tmp/fx/pgpass', b'localhost:*:*:u:p\n')
    private_file('/tmp/fx/empty.crt', b'', 0o644)
    private_file('/tmp/fx/insecure', b'localhost:*:*:u:p\n', 0o640)
    private_file('/tmp/fx/unreadable', b'x', 0o000)
    env = {b'HOME': b'/work', b'PGPASSFILE': b'/tmp/fx/pgpass'}
    os.environ.update({'HOME': '/work', 'PGPASSFILE': '/tmp/fx/pgpass', 'PGCOPYDB_SOURCE_PGURI': 'x'})

    def prepared(raw, envb=env):
        materials = P.Materialization(os.environ, source_only=True)
        return materials, P.ConnectionInput(library, raw, envb, materials, deadline())

    def kinds(connection):
        return {key.decode(): 'absent' if value == P.ABSENT_FILE else 'snapshot' if value.startswith(b'/proc/self/fd/') else 'other'
                for key, value in connection.overrides.items()}
    materials, connection = prepared(b'host=localhost sslmode=require')
    c.add('optional_tls_files_absent_passfile_snapshot', kinds(connection),
          {'passfile': 'snapshot', 'sslrootcert': 'absent', 'sslcert': 'absent', 'sslkey': 'absent', 'sslcrl': 'absent'})
    keywords, values = connection.claim()
    c.add('arrays_first_pair_is_original_dbname', [keywords[0], values[0]], [b'dbname', b'host=localhost sslmode=require'])
    c.add('arrays_null_terminated', [keywords[len(connection.overrides) + 1], values[len(connection.overrides) + 1]], [None, None])
    c.add('claim_once', outcome(P, connection.claim), 'refused:capability')
    connection.close()
    materials.close()
    materials, connection = prepared(b'host=localhost sslmode=disable')
    c.add('sslmode_disable_skips_tls_files', kinds(connection), {'passfile': 'snapshot'})
    connection.close()
    materials.close()
    materials, connection = prepared(b'host=localhost sslmode=disable password=p')
    c.add('explicit_password_skips_passfile', kinds(connection), {})
    connection.close()
    materials.close()
    materials, connection = prepared(b'host=localhost sslmode=require sslrootcert=/tmp/fx/empty.crt')
    c.add('present_empty_root_is_snapshot_not_absence', kinds(connection)['sslrootcert'], 'snapshot')
    connection.close()
    materials.close()
    for name, raw in (('insecure_passfile_0640', b'host=localhost sslmode=disable passfile=/tmp/fx/insecure'),
                      ('insecure_sslkey_0640_nonroot', b'host=localhost sslmode=require password=p sslkey=/tmp/fx/insecure'),
                      ('eacces_is_not_absence', b'host=localhost sslmode=require password=p sslrootcert=/tmp/fx/unreadable')):
        c.add(name + '_refused', outcome(P, lambda: prepared(raw)), 'refused:connection')

    materials, connection = prepared(b'host=localhost sslmode=disable')
    snapshot = connection.snapshots[0]
    os.close(snapshot.fd)
    reuse = os.open('/tmp/fx/empty.crt', os.O_RDONLY)
    c.add('fault_snapshot_fd_closed_and_reused', [reuse == snapshot.fd, outcome(P, connection.check)], [True, 'refused:capability'])
    os.close(reuse)
    snapshot.fd = None
    connection.close()
    materials.close()
    materials, connection = prepared(b'host=localhost sslmode=disable')
    with open('/tmp/fx/pgpass', 'r+b') as handle:
        handle.write(b'X')
    c.add('fault_original_bytes_changed_after_capture', outcome(P, connection.claim), 'refused:admission')
    connection.close()
    materials.close()
    os.unlink('/tmp/fx/pgpass')
    private_file('/tmp/fx/pgpass', b'localhost:*:*:u:p\n')
    materials, connection = prepared(b'host=localhost sslmode=disable')
    private_file('/tmp/fx/swap', b'localhost:*:*:u:other\n')
    os.rename('/tmp/fx/swap', '/tmp/fx/pgpass')
    c.add('fault_original_renamed_over_after_capture', outcome(P, connection.check), 'refused:admission')
    connection.close()
    materials.close()
    materials, connection = prepared(b'host=localhost sslmode=disable')
    connection.deadline = time.monotonic() - 1
    c.add('fault_budget_expired_before_start', outcome(P, connection.claim), 'refused:timeout')
    connection.close()
    materials.close()


def run_stage1(P):
    c, library = stage_library(P)
    stage_memfd(P, c)
    stage_absence(P, c, library)
    return c.report(stage='stage1', uid=os.geteuid(), machine=os.uname().machine,
                    page_size=os.sysconf('SC_PAGE_SIZE'), memfd_noexec=open('/proc/sys/vm/memfd_noexec').read().strip())


def run_absence_only(P):
    print(json.dumps({'stage': 'absence', 'observed': outcome(P, P.stable_absence)}))
    return 0


def run_kernel():
    c = Checks()
    page = os.sysconf('SC_PAGE_SIZE')
    single = 32 * page
    exe = '/bin/true'
    run = lambda argv, env: errno_of(lambda: subprocess.run(argv, env=env, check=True))
    c.add('max_arg_strlen_minus_nul_executes', run([exe, 'a' * (single - 1)], {}), 'ok')
    c.add('max_arg_strlen_plus_one_e2big', run([exe, 'a' * single], {}), 'E2BIG')
    c.add('single_env_string_e2big', run([exe], {'X': 'a' * single}), 'E2BIG')
    arg_max = os.sysconf('SC_ARG_MAX')
    many = {'V%05d' % i: 'a' * 65536 for i in range(arg_max // 65536 + 2)}
    c.add('aggregate_env_exhaustion_e2big', run([exe], many), 'E2BIG')
    import resource
    return c.report(stage='kernel', page_size=page, max_arg_strlen=single, arg_max=arg_max,
                    rlimit_stack=resource.getrlimit(resource.RLIMIT_STACK)[0])


def connect_child(P, mode):
    """One libpq connection attempt, isolated in its own process so env and stderr stay per case."""
    if mode == 'raw':
        library = P.load_runtime()
        library.PQconnectdb.argtypes = [C.c_char_p]
        library.PQconnectdb.restype = C.c_void_p
        conn = library.PQconnectdb(os.environb[b'PGCOPYDB_SOURCE_PGURI'])
        ok = bool(conn) and library.PQstatus(conn) == 0
        ssl = bool(conn) and library.PQsslInUse(conn) == 1
        if conn:
            library.PQfinish(conn)
        print(json.dumps({'observed': 'ok' if ok else 'fail', 'ssl': ssl}))
        return 0
    diag = P.Diagnostics()
    library = P.load_runtime()
    materials = P.Materialization(os.environ, source_only=True)
    result = {'stage': 'prepare'}
    try:
        materials.prepare(library, deadline())
        connection = materials.connection('source')
        result['overrides'] = {key.decode(): 'absent' if value == P.ABSENT_FILE else 'snapshot' if value.startswith(b'/proc/self/fd/') else 'other'
                               for key, value in connection.overrides.items()}
        result['stage'] = 'connect'
        session = P.Session(library, connection, deadline(), diag)
        version = session.query("SELECT @current_setting('server_version_num');")[0][0]
        result.update(stage='closed', ssl=session.props['ssl'] == 1, server_major=int(version) // 10000)
        session.close()
        result['snapshots_released'] = connection.closed and not connection.snapshots
        result['observed'] = 'ok'
    except P.Refusal as error:
        result['observed'] = 'refused:' + error.error_class
    finally:
        materials.close()
    try:
        diag.close()
        result['diagnostics'] = 'clean'
    except P.Refusal as error:
        result['diagnostics'] = 'refused:' + error.error_class
    print(json.dumps(result))
    return 0


def run_matrix(P, program, sha, cases_path):
    with open('/fixtures/secrets.json', 'rb') as handle:
        secrets = json.load(handle)
    with open(cases_path) as handle:
        cases = json.load(handle)
    canaries = [value.encode() for value in secrets.values()]
    c = Checks()
    leaked = 0
    for case in cases:
        uri = case['uri'].replace('{pw}', secrets['password']).replace('{keypass}', secrets['keypass'])
        uri = uri.replace('{wrongkeypass}', 'not-the-key-password')
        env = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C', 'LANG': 'C', 'HOME': '/work', 'PGCONNECT_TIMEOUT': '5',
               'PGCOPYDB_SOURCE_PGURI': uri}
        env.update(case.get('env', {}))
        observed = {}
        for mode in ('binding', 'raw'):
            # A Job container has no stdin or TTY, so an OpenSSL passphrase prompt reads EOF here too.
            child = subprocess.run([sys.executable, '-I', '-B', __file__, 'connect', program, sha, mode], env=env,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=40, cwd='/work')
            leaked += sum(canary in child.stdout or canary in child.stderr for canary in canaries)
            try:
                observed[mode] = json.loads(child.stdout)
            except ValueError:
                observed[mode] = {'observed': 'child_exit_%d' % child.returncode}
        binding = observed['binding']
        c.add(case['name'] + '/binding', {k: binding.get(k) for k in case['expect']}, case['expect'])
        c.add(case['name'] + '/raw_control', observed['raw']['observed'], case['raw'])
    c.add('no_canary_in_child_output', leaked, 0)
    return c.report(stage='matrix')


def run_integration(P):
    diag = P.Diagnostics()
    c = Checks()
    # Host and port fields only; the hosts carry no escaped colon.
    with open('/tmp/pgpass') as handle:
        c.add('passfile_host_ports', [line.split(':')[:3] for line in handle.read().splitlines()],
              [['localhost', '5418', '*'], ['localhost', '5416', '*']])
    library = P.load_runtime()
    materials = P.Materialization(os.environ)
    try:
        materials.prepare(library, deadline())
        majors = []
        for role in ('source', 'target'):
            connection = materials.connection(role)
            kinds = sorted(key.decode() + ':' + ('snapshot' if value.startswith(b'/proc/self/fd/') else 'absent' if value == P.ABSENT_FILE else 'other')
                           for key, value in connection.overrides.items())
            c.add(role + '_overrides', kinds, ['passfile:snapshot', 'sslcert:snapshot', 'sslcrl:absent', 'sslkey:snapshot', 'sslrootcert:snapshot'])
            session = P.Session(library, connection, deadline(), diag)
            majors.append(int(session.query("SELECT @current_setting('server_version_num');")[0][0]) // 10000)
            c.add(role + '_tls_in_use', session.props['ssl'], 1)
            session.close()
            c.add(role + '_input_released', connection.closed, True)
        c.add('source_then_target_majors', majors, [18, 16])
    except P.Refusal as error:
        c.add('integration', 'refused:' + error.error_class, 'ok')
    finally:
        materials.close()
    diag.close()
    return c.report(stage='integration', pgpass_mode=oct(os.stat('/tmp/pgpass').st_mode & 0o777))


def main(argv):
    command = argv[1]
    if command == 'kernel':
        return run_kernel()
    P = load(argv[2], argv[3])
    if command == 'stage1':
        return run_stage1(P)
    if command == 'absence':
        return run_absence_only(P)
    if command == 'connect':
        return connect_child(P, argv[4])
    if command == 'matrix':
        return run_matrix(P, argv[2], argv[3], argv[4])
    if command == 'integration':
        return run_integration(P)
    raise SystemExit('unknown command')


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))

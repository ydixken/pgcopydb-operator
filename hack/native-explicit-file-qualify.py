#!/usr/bin/env python3
"""Run the M6 explicit-file binding qualification in capped Podman containers (no cluster).

Usage: native-explicit-file-qualify.py --image REF@sha256:... --pg16 IMAGE --pg18 IMAGE --evidence DIR --owner ID
Every container and volume carries io.rpgmigrate.owner=<ID> and is removed on exit.
Exit status 0 means every stage ran a nonempty inventory and every case passed.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
PROGRAM = ROOT / 'internal/native/evidence_program.py'
VERIFIER = 'native-explicit-file-verify.py'
CAPS = ['--cpus', '1', '--memory', '512m', '--pids-limit', '256']
# The production evidence container profile: UID 65532, read-only rootfs, no capabilities.
RUNNER = ['--user', '65532:65532', '--read-only', '--cap-drop', 'all', '--security-opt', 'no-new-privileges',
          '--tmpfs', '/tmp:rw,size=16m,mode=1777',
          '--mount', 'type=tmpfs,destination=/work,tmpfs-size=16m,tmpfs-mode=0700,U=true', '-w', '/work']
PORTS = {18: 5418, 16: 5416}

FIXTURES_SH = r'''set -eu
cd /out
umask 077
mkdir ca
: > ca/index.txt
echo 01 > ca/crlnumber
printf '[ca]\ndefault_ca=d\n[d]\ndatabase=/out/ca/index.txt\nnew_certs_dir=/out/ca\ncertificate=/out/ca.crt\nprivate_key=/out/ca.key\ndefault_md=sha256\ndefault_crl_days=2\ncrlnumber=/out/ca/crlnumber\n' > ca.cnf
key() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null; }
key ca.key
openssl req -x509 -new -key ca.key -out ca.crt -days 2 -subj /CN=rpgmigrate-explicit-ca -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign
printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n' > server.ext
printf 'extendedKeyUsage=clientAuth\n' > client.ext
for name in server client; do
  key $name.key
  subject=/CN=localhost
  [ $name = client ] && subject=/CN=certuser
  openssl req -new -key $name.key -out $name.csr -subj $subject
  openssl x509 -req -in $name.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out $name.crt -days 2 -extfile $name.ext 2>/dev/null
done
openssl pkey -in client.key -aes256 -passout file:keypass.txt -out client-enc.key
openssl ca -config ca.cnf -gencrl -out crl-empty.pem 2>/dev/null
openssl ca -config ca.cnf -revoke server.crt 2>/dev/null
openssl ca -config ca.cnf -gencrl -out crl-server.pem 2>/dev/null
: > empty.crt
'''

POPULATE_SH = r'''set -eu
cd /fx
chmod 0755 /fx
install -o 0 -g 0 -m 0644 /in/ca.crt /in/empty.crt /in/crl-empty.pem /in/crl-server.pem .
install -o 65532 -g 65532 -m 0600 /in/client.crt /in/client.key /in/client-enc.key /in/pgpass-good /in/pgpass-wrong /in/secrets.json .
install -o 0 -g 65532 -m 0640 /in/client.key client-root.key
install -o 65532 -g 65532 -m 0640 /in/client.key client-insecure.key
install -o 65532 -g 65532 -m 0640 /in/pgpass-good pgpass-insecure
for role in source target; do
  install -d -o 0 -g 0 -m 0755 tls/$role creds/$role-mnt
  install -o 0 -g 65532 -m 0440 /in/ca.crt tls/$role/ca.crt
  install -o 0 -g 65532 -m 0440 /in/client.crt tls/$role/tls.crt
  install -o 0 -g 65532 -m 0440 /in/client.key tls/$role/tls.key
  install -o 0 -g 65532 -m 0440 /in/password-$role.txt creds/$role-mnt/$role-password
done
install -d -o 0 -g 0 -m 0755 shadow
install -o 0 -g 0 -m 0444 /dev/null shadow/.native-file-absent
'''

SERVER_SH = r'''set -eu
mkdir -p /srv/tls
cp /in/server.crt /in/server.key /in/ca.crt /in/pg_hba.conf /srv/tls/
chown -R postgres /srv/tls
chmod 0600 /srv/tls/server.key
install -m 0644 /in/init.sql /docker-entrypoint-initdb.d/init.sql
exec docker-entrypoint.sh postgres -c listen_addresses=localhost -c ssl=on -c ssl_cert_file=/srv/tls/server.crt -c ssl_key_file=/srv/tls/server.key -c ssl_ca_file=/srv/tls/ca.crt -c hba_file=/srv/tls/pg_hba.conf
'''

HBA = '''local all all trust
hostssl all certuser 127.0.0.1/32 cert
hostssl all certuser ::1/128 cert
host all pwuser 127.0.0.1/32 scram-sha-256
host all pwuser ::1/128 scram-sha-256
'''


def run(argv, timeout=120, check=True, **kwargs):
    result = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, **kwargs)
    if check and result.returncode:
        raise SystemExit('command failed (%d): %s' % (result.returncode, ' '.join(argv[:4])))
    return result


def private(path, text, mode=0o600):
    path.write_text(text)
    path.chmod(mode)


def make_fixtures(args, fx):
    fx.mkdir(mode=0o700)
    values = {'password': secrets.token_hex(16), 'keypass': secrets.token_hex(16), 'superuser': secrets.token_hex(16),
              'target_password': secrets.token_hex(16)}
    private(fx / 'secrets.json', json.dumps(values))
    private(fx / 'password-source.txt', values['password'])
    private(fx / 'password-target.txt', values['target_password'])
    private(fx / 'keypass.txt', values['keypass'])
    private(fx / 'server.env', 'POSTGRES_PASSWORD=%s\nPGDATA=/var/lib/postgresql/data/pg\n' % values['superuser'])
    lines = ''.join('%s:*:*:pwuser:%s\n' % (host, '%s') for host in ('localhost', '127.0.0.1'))
    private(fx / 'pgpass-good', lines.replace('%s', values['password']))
    private(fx / 'pgpass-wrong', lines.replace('%s', secrets.token_hex(16)))
    private(fx / 'pg_hba.conf', HBA)
    private(fx / 'init.sql', "CREATE ROLE pwuser LOGIN PASSWORD '%s';\nCREATE ROLE certuser LOGIN;\n"
            "CREATE DATABASE app OWNER pwuser;\n" % values['password'])
    run(['podman', 'run', '--rm', '--label', args.label, '--network', 'none', '--user', '0:0', *CAPS,
         '-v', '%s:/out' % fx, args.image, 'sh', '-c', FIXTURES_SH])
    return values


def container(args, name, image, argv, extra=(), timeout=120, check=False):
    return run(['podman', 'run', '--rm', '--name', '%s-%s' % (args.prefix, name), '--label', args.label, *CAPS, *extra,
                image, *argv], timeout=timeout, check=check)


def verifier(args, stage, extra=(), tail=(), image=None):
    mounts = ['-v', '%s:/src:ro' % PROGRAM.parent, '-v', '%s:/verifier:ro' % (ROOT / 'hack')]
    result = container(args, stage.replace('_', '-'), image or args.image,
                       ['/usr/bin/python3', '-I', '-B', '/verifier/' + VERIFIER, *tail],
                       [*RUNNER, *mounts, *extra], timeout=600)
    try:
        return json.loads(result.stdout), result.returncode
    except ValueError:
        return {'stage': stage, 'error': 'no verifier output', 'exit': result.returncode}, result.returncode


def start_servers(args, fx):
    pg18 = '%s-pg18' % args.prefix
    common = ['--label', args.label, *CAPS, '--image-volume', 'tmpfs', '--env-file', str(fx / 'server.env'),
              '-v', '%s:/in:ro' % fx, '--entrypoint', 'sh']
    for major, image in ((18, args.pg18), (16, args.pg16)):
        network = ['--network', 'none'] if major == 18 else ['--network', 'container:' + pg18]
        run(['podman', 'run', '-d', '--name', '%s-pg%d' % (args.prefix, major), *common, *network,
             '-e', 'PGPORT=%d' % PORTS[major], image, '-c', SERVER_SH])
    for major in PORTS:
        limit = time.monotonic() + 120
        while run(['podman', 'exec', '%s-pg%d' % (args.prefix, major), 'pg_isready', '-q', '-h', 'localhost',
                   '-p', str(PORTS[major])], check=False).returncode:
            if time.monotonic() > limit:
                raise SystemExit('server pg%d not ready' % major)
            time.sleep(1)
    return pg18


def matrix_cases():
    f = '/fixtures/'
    good, wrong = {'PGPASSFILE': f + 'pgpass-good'}, {'PGPASSFILE': f + 'pgpass-wrong'}
    absent_tls = {'sslcert': 'absent', 'sslkey': 'absent', 'sslcrl': 'absent'}
    cases = []
    for major, port in PORTS.items():
        base = 'host=localhost port=%d dbname=app ' % port
        pw = base + 'user=pwuser '
        cert = base + 'user=certuser sslmode=verify-full sslrootcert=%sca.crt sslcert=%sclient.crt ' % (f, f)
        ok = {'observed': 'ok', 'server_major': major}
        refused = {'observed': 'refused:connection', 'stage': 'connect'}
        early = {'observed': 'refused:connection', 'stage': 'prepare'}
        tls = dict(ok, ssl=True)
        verify = pw + 'sslmode=verify-full sslrootcert=%sca.crt ' % f
        for name, uri, env, expect, raw in (
                ('pw_passfile_good', pw + 'sslmode=disable', good, dict(ok, overrides={'passfile': 'snapshot'}), 'ok'),
                ('pw_passfile_wrong', pw + 'sslmode=disable', wrong, refused, 'fail'),
                ('pw_explicit_password_over_wrong_passfile', pw + 'sslmode=disable password={pw}', wrong, dict(ok, overrides={}), 'ok'),
                ('pw_passfile_insecure_0640', pw + 'sslmode=disable', {'PGPASSFILE': f + 'pgpass-insecure'}, early, 'fail'),
                ('pw_default_passfile_absent', pw + 'sslmode=disable', {}, dict(refused, overrides={'passfile': 'absent'}), 'fail'),
                ('pw_uri_explicit_empty_passfile', 'postgresql://pwuser@localhost:%d/app?sslmode=disable&passfile=' % port, good,
                 dict(refused, overrides={'passfile': 'absent'}), 'fail'),
                ('pw_multihost_order_kept', 'host=localhost,localhost port=5999,%d dbname=app user=pwuser sslmode=disable' % port, good, ok, 'ok'),
                ('tls_verify_full', verify, good, dict(tls, overrides=dict(absent_tls, passfile='snapshot', sslrootcert='snapshot')), 'ok'),
                ('tls_uri_encoded_paths', 'postgresql://pwuser@localhost:%d/app?sslmode=verify-full&sslrootcert=%%2Ffixtures%%2Fca.crt' % port, good, tls, 'ok'),
                ('tls_verify_full_name_mismatch', verify.replace('host=localhost', 'host=127.0.0.1'), good, refused, 'fail'),
                ('tls_hostaddr_keeps_host_name', verify + 'hostaddr=127.0.0.1', good, tls, 'ok'),
                ('tls_require_absent_root', pw + 'sslmode=require', good, dict(tls, overrides=dict(absent_tls, passfile='snapshot', sslrootcert='absent')), 'ok'),
                ('tls_verify_ca_absent_root', pw + 'sslmode=verify-ca', good, refused, 'fail'),
                ('tls_empty_root_not_absence', pw + 'sslmode=verify-full sslrootcert=%sempty.crt' % f, good, refused, 'fail'),
                ('tls_crl_empty', verify + 'sslcrl=%scrl-empty.pem' % f, good, tls, 'ok'),
                ('tls_crl_revokes_server', verify + 'sslcrl=%scrl-server.pem' % f, good, refused, 'fail'),
                ('tls_crl_missing_is_absent', verify + 'sslcrl=%sno-such.crl' % f, good, dict(tls, overrides=dict(absent_tls, passfile='snapshot', sslrootcert='snapshot')), 'ok'),
                ('cert_user_key_0600', cert + 'sslkey=%sclient.key' % f, {}, dict(tls, overrides={'passfile': 'absent', 'sslrootcert': 'snapshot', 'sslcert': 'snapshot', 'sslkey': 'snapshot', 'sslcrl': 'absent'}), 'ok'),
                ('cert_root_key_0640', cert + 'sslkey=%sclient-root.key' % f, {}, tls, 'ok'),
                ('cert_user_key_0640_insecure', cert + 'sslkey=%sclient-insecure.key' % f, {}, early, 'fail'),
                ('cert_missing_key', cert + 'sslkey=%sno-such.key' % f, {}, refused, 'fail'),
                ('cert_no_client_cert', base + 'user=certuser sslmode=verify-full sslrootcert=%sca.crt' % f, {}, refused, 'fail'),
                ('cert_encrypted_key_password', cert + 'sslkey=%sclient-enc.key sslpassword={keypass}' % f, {}, tls, 'ok'),
                ('cert_encrypted_key_wrong_password', cert + 'sslkey=%sclient-enc.key sslpassword={wrongkeypass}' % f, {}, refused, 'fail'),
                # OpenSSL's default passphrase prompt writes to stderr, which the helper refuses as a diagnostic.
                ('cert_encrypted_key_no_password', cert + 'sslkey=%sclient-enc.key' % f, {}, dict(refused, observed='refused:diagnostic'), 'fail')):
            cases.append({'name': 'pg%d/%s' % (major, name), 'uri': uri, 'env': env, 'expect': expect, 'raw': raw})
    return cases


def argv_cases(args, export, vol):
    rows = []
    mounts = []
    for role in ('source', 'target'):
        mounts += ['--mount', 'type=volume,src=%s,dst=/etc/pgcopydb/tls/%s,ro=true,subpath=tls/%s' % (vol, role, role),
                   '--mount', 'type=volume,src=%s,dst=/etc/pgcopydb/creds/%s-mnt,ro=true,subpath=creds/%s-mnt' % (vol, role, role)]
    env = []
    for item in export['integration']['env']:
        env += ['-e', '%s=%s' % (item['name'], item['value'])]
    for case in export['cases']:
        command = case['command']
        result = container(args, 'argv-' + case['name'].replace('_', '-'), args.image, [*command[1:], *case['args']],
                           [*RUNNER, *mounts, *env, '--network', 'none', '--entrypoint', command[0]], timeout=120)
        try:
            out = json.loads(result.stdout)
        except ValueError:
            out = {}
        lengths = out.get('argv', [])
        # main is "exec /usr/bin/env ... - completed_evidence <operation> <sha> <<'...'"; Python sees ['-', kind, operation, sha].
        operation = case['args'][1].split(' ', 12)[11]
        rows.append({'case': 'argv/' + case['name'], 'pass': result.returncode == 0 and out.get('marker') == case['marker']
                     and lengths == [1, 18, len(operation), 64] and out.get('pgpass') is True,
                     'observed': {'exit': result.returncode, 'python_argv_bytes': lengths, 'env_bytes': out.get('env_bytes'),
                                  'env_count': out.get('env_count'), 'main_bytes_with_nul': case['main_bytes_with_nul'],
                                  'prelude_bytes_with_nul': case['prelude_bytes_with_nul'], 'source_bytes': case['source_bytes']}})
    for name, extra in (('single_env_string_e2big', ['-e', 'RPGMIGRATE_E2BIG=' + 'a' * 131072]),):
        result = container(args, 'start-' + name.replace('_', '-'), args.image, ['/bin/true'], [*RUNNER, '--network', 'none', *extra])
        rows.append({'case': 'start/' + name, 'pass': result.returncode != 0 and b'argument list too long' in result.stderr.lower(),
                     'observed': {'exit': result.returncode}})
    return rows, mounts, env


def split_target_password(args, values):
    # Same host and user, different port and password: a wildcard-port passfile gives the target the source's line.
    sql = "ALTER ROLE pwuser PASSWORD '%s';\n" % values['target_password']
    run(['podman', 'exec', '-i', '%s-pg16' % args.prefix, 'psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres',
         '-p', str(PORTS[16]), '-d', 'postgres', '-f', '-'], input=sql.encode())


def integration(args, export, mounts, env, pg18, sha, values):
    command = export['integration']['command']
    main = ('exec /usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B /verifier/%s integration '
            '/src/evidence_program.py %s' % (VERIFIER, sha))
    src = ['-v', '%s:/src:ro' % PROGRAM.parent, '-v', '%s:/verifier:ro' % (ROOT / 'hack')]
    result = container(args, 'integration', args.image, [*command[1:], '-c', main],
                       [*RUNNER, *mounts, *env, *src, '--network', 'container:' + pg18, '--entrypoint', command[0]], timeout=120)
    try:
        out = json.loads(result.stdout)
    except ValueError:
        return {'stage': 'integration', 'error': 'no verifier output', 'exit': result.returncode}
    leaked = sum(value.encode() in result.stdout + result.stderr for value in values.values())
    out.setdefault('cases', []).append({'case': 'no_canary_in_output', 'observed': leaked, 'pass': leaked == 0})
    out['total'], out['passed'] = len(out['cases']), sum(row['pass'] for row in out['cases'])
    return out


def go_tests(evidence):
    env = dict(os.environ, RPGMIGRATE_EXPLICIT_ARGV_EXPORT=str(evidence / 'argv-export.json'))
    runs = {}
    for name, pattern in (
            ('caps', '^TestNative(EvidenceProgramEmbedding|EvidenceProgramLimits|EvidenceJobSize|AdmissionObjects|PythonCredentialsPrivate|ImmutableAbsenceMountShadow|EvidenceArgvRuntimeExport)$'),
            ('file_binding', '^TestNativePython(FileOptions|FileOptionPrecedence|FileSources|FileSnapshots|FileAbsence|FileConnectionStart|FileCleanupOrder|FileKernelOpen|FileMaterialization|FileKeyLogRefusal)$'),
            ('session', '^TestNative(PythonABI|PythonConnectPolling|PythonObservationOrder|PythonUnknownCommit)$')):
        result = run(['go', 'test', './internal/native', '-run', pattern, '-count=1', '-v'], timeout=600, check=False, cwd=ROOT, env=env)
        (evidence / ('go-%s.log' % name)).write_bytes(result.stdout + result.stderr)
        lines = result.stdout.decode().splitlines()
        runs[name] = {'exit': result.returncode, 'pass': sum(l.startswith('--- PASS') for l in lines),
                      'fail': sum(l.startswith('--- FAIL') for l in lines), 'skip': sum(l.startswith('--- SKIP') for l in lines)}
    return runs


def cleanup(args):
    # pg16 and the verifiers share pg18's network namespace, so removal can need more than one pass.
    listings = (['ps', '-a'], ['volume', 'ls'])
    removers = (['rm', '-f', '-t', '5'], ['volume', 'rm', '-f'])
    owned = lambda listing: run(['podman', *listing, '-q', '--filter', 'label=' + args.label], check=False).stdout.decode().split()
    limit = time.monotonic() + 180
    while any(owned(listing) for listing in listings) and time.monotonic() < limit:
        for listing, remover in zip(listings, removers):
            ids = owned(listing)
            if ids:
                run(['podman', *remover, *ids], check=False, timeout=180)
        time.sleep(1)
    return not any(owned(listing) for listing in listings)


def main():
    parser = argparse.ArgumentParser()
    for flag in ('--image', '--pg16', '--pg18', '--evidence', '--owner'):
        parser.add_argument(flag, required=True)
    args = parser.parse_args()
    if '@sha256:' not in args.image:
        raise SystemExit('runner image must be pinned by digest')
    evidence = Path(args.evidence)
    evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    args.label = 'io.rpgmigrate.owner=' + args.owner
    args.prefix = 'rpgmigrate-%s-%s' % (args.owner.lower(), secrets.token_hex(3))
    sha = hashlib.sha256(PROGRAM.read_bytes()).hexdigest()
    receipt = {'schema': 'rpgmigrate.native-explicit-file-receipt/v1', 'owner': args.owner, 'started': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
               'operator_head': run(['git', 'rev-parse', 'HEAD'], cwd=ROOT).stdout.decode().strip(),
               'operator_tree_clean': not run(['git', 'status', '--porcelain'], cwd=ROOT).stdout.strip(),
               'program_sha256': sha, 'runner_image': args.image,
               'verifier_sha256': hashlib.sha256((ROOT / 'hack' / VERIFIER).read_bytes()).hexdigest(),
               'driver_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               'servers': {str(m): run(['podman', 'image', 'inspect', '--format', '{{.Id}}', img]).stdout.decode().strip()
                           for m, img in ((16, args.pg16), (18, args.pg18))}}
    stages = {}
    try:
        receipt['go'] = go_tests(evidence)
        export = json.loads((evidence / 'argv-export.json').read_text())
        if export['program_sha256'] != sha:
            raise SystemExit('Go embedding and program file disagree')
        receipt['go_refusals'] = export['refusals']
        fx = evidence / 'fixtures'
        values = make_fixtures(args, fx)
        vol = '%s-fixtures' % args.prefix
        run(['podman', 'volume', 'create', '--label', args.label, vol])
        container(args, 'populate', args.image, ['sh', '-c', POPULATE_SH],
                  ['--network', 'none', '--user', '0:0', '-v', '%s:/in:ro' % fx, '-v', '%s:/fx' % vol], check=True)
        lib = ['/src/evidence_program.py', sha]
        stages['stage1'], _ = verifier(args, 'stage1', ['--network', 'none', '-e', 'HOME=/work'], ['stage1', *lib])
        stages['kernel'], _ = verifier(args, 'kernel', ['--network', 'none'], ['kernel'])
        absence = []
        for name, extra, expect in (
                ('readonly_rootfs', [], 'ok'),
                ('writable_rootfs', ['--read-only=false'], 'refused:capability'),
                ('writable_shadow_mount', ['--tmpfs', '/usr/share/rpgmigrate:rw,mode=0755'], 'refused:capability'),
                ('shadow_with_marker_file', ['--mount', 'type=volume,src=%s,dst=/usr/share/rpgmigrate,ro=true,subpath=shadow' % vol], 'refused:capability'),
                # A read-only empty shadow is invisible in process; only the admission mount-shadow refusal stops it.
                ('readonly_empty_shadow_needs_admission', ['--mount', 'type=tmpfs,dst=/usr/share/rpgmigrate,ro=true,tmpfs-mode=0755'], 'ok')):
            out, _ = verifier(args, 'absence-' + name, ['--network', 'none', *extra], ['absence', *lib])
            absence.append({'case': 'absence/' + name, 'observed': out.get('observed'), 'pass': out.get('observed') == expect})
        stages['absence'] = {'cases': absence}
        rows, mounts, env = argv_cases(args, export, vol)
        stages['argv'] = {'cases': rows}
        pg18 = start_servers(args, fx)
        cases_file = evidence / 'matrix-cases.json'
        cases_file.write_text(json.dumps(matrix_cases(), indent=1))
        cases_file.chmod(0o644)
        stages['matrix'], _ = verifier(args, 'matrix', ['--network', 'container:' + pg18, '-v', '%s:/fixtures:ro' % vol,
                                                         '-v', '%s:/cases.json:ro' % cases_file], ['matrix', *lib, '/cases.json'])
        split_target_password(args, values)
        stages['integration'] = integration(args, export, mounts, env, pg18, sha, values)
    finally:
        receipt['stages'] = stages
        receipt['cleanup_verified'] = cleanup(args)
        receipt['finished'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        counts = {name: (sum(row['pass'] for row in stage.get('cases', [])), len(stage.get('cases', []))) for name, stage in stages.items()}
        go_ok = all(r['exit'] == 0 and r['pass'] > 0 and r['fail'] == 0 and r['skip'] == 0 for r in receipt.get('go', {}).values())
        receipt['counts'] = counts
        receipt['passed'] = (go_ok and len(counts) == 6 and all(total and passed == total for passed, total in counts.values())
                             and receipt['cleanup_verified'] and receipt['operator_tree_clean'])
        (evidence / 'receipt.json').write_text(json.dumps(receipt, indent=1, sort_keys=True) + '\n')
        # Fixtures are synthetic but include keys and passwords; the receipt keeps only outcomes.
        if (evidence / 'fixtures').exists():
            run(['rm', '-rf', str(evidence / 'fixtures')])
        sums = sorted(p for p in evidence.rglob('*') if p.is_file() and p.name != 'SHA256SUMS')
        (evidence / 'SHA256SUMS').write_text(''.join('%s  %s\n' % (hashlib.sha256(p.read_bytes()).hexdigest(), p.relative_to(evidence)) for p in sums))
    print(json.dumps({'passed': receipt['passed'], 'counts': counts, 'go': receipt.get('go')}))
    return 0 if receipt['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())

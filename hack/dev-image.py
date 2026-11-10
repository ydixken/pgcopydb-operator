#!/usr/bin/env python3
"""Publish one native manager development image from the admitted Actions branch."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

REPOSITORY = 'https://github.com/ydixken/pgcopydb-operator'
PACKAGE = 'ghcr.io/ydixken/pgcopydb-operator'
BRANCH = 'feat/rpgmigrate-native-operator'
MANIFEST = 'application/vnd.oci.image.manifest.v1+json'
CONFIG = 'application/vnd.oci.image.config.v1+json'
LAYER = 'application/vnd.oci.image.layer.v1.tar+gzip'
INDEX = 'application/vnd.oci.image.index.v1+json'
MANIFEST_TYPES = {MANIFEST, 'application/vnd.docker.distribution.manifest.v2+json'}
CONFIG_TYPES = {CONFIG, 'application/vnd.docker.container.image.v1+json'}
LAYER_TYPES = {LAYER, 'application/vnd.oci.image.layer.v1.tar',
               'application/vnd.oci.image.layer.v1.tar+zstd',
               'application/vnd.docker.image.rootfs.diff.tar.gzip'}
SOURCE_KEYS = {'repository', 'commit', 'git_tree', 'payload_inventory_sha256'}
HEX = r'[0-9a-f]{64}'
DIGEST = r'sha256:' + HEX
MAX_JSON = 1048576
MAX_BLOB = 536870912


class Refusal(Exception):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def decode(value):
    if not isinstance(value, (bytes, str)) or not 0 < len(value) <= MAX_JSON:
        raise Refusal('JSON exceeds bound or is empty')
    def unique(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise Refusal('duplicate JSON field')
            result[key] = item
        return result
    try:
        return json.loads(value, object_pairs_hook=unique)
    except (ValueError, UnicodeError, TypeError) as error:
        raise Refusal('invalid bounded JSON') from error


def command(argv, *, cwd=None, timeout=120, env=None, combined=False):
    child_env = {key: os.environ[key] for key in ('PATH', 'HOME', 'XDG_RUNTIME_DIR', 'LANG', 'LC_ALL', 'TMPDIR')
                 if key in os.environ}
    child_env['PYTHONDONTWRITEBYTECODE'] = '1'
    child_env.update(env or {})
    try:
        result = subprocess.run(argv, cwd=cwd, env=child_env, capture_output=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise Refusal('command unavailable or timed out') from error
    if result.returncode or len(result.stdout) > MAX_JSON or len(result.stderr) > MAX_JSON:
        raise Refusal('command failed or output exceeded bound')
    return result.stdout + result.stderr if combined else result.stdout


def source_identity(root):
    if command(['git', 'status', '--porcelain=v1', '--untracked-files=all'], cwd=root):
        raise Refusal('publication requires a clean complete source checkout')
    commit = command(['git', 'rev-parse', 'HEAD'], cwd=root).decode().strip()
    tree = command(['git', 'rev-parse', 'HEAD^{tree}'], cwd=root).decode().strip()
    committed = {}
    for entry in command(['git', 'ls-tree', '-r', '-z', 'HEAD'], cwd=root).decode().split('\0'):
        if not entry:
            continue
        identity, name = entry.split('\t', 1)
        mode, kind, oid = identity.split()
        if kind != 'blob' or mode not in ('100644', '100755'):
            raise Refusal('committed source contains unsupported input')
        committed[name] = (int(mode, 8) & 0o777, oid)
    inventory = []
    total = 0
    names = command(['git', 'ls-files', '-z'], cwd=root).decode().split('\0')
    if len(names) > 8192 or len(names) != len(set(names)):
        raise Refusal('source inventory exceeds bound or has duplicates')
    for name in sorted(names):
        if not name:
            continue
        if name.startswith('/') or '..' in Path(name).parts or any(ord(char) < 32 for char in name):
            raise Refusal('source path is invalid')
        path = root / name
        if path.is_symlink() or not path.is_file() or path.resolve() != path or path.stat().st_nlink != 1:
            raise Refusal('source inventory contains missing or linked input')
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode not in (0o644, 0o755):
            raise Refusal('source inventory contains unsupported mode')
        info = path.stat()
        if info.st_size > 16 * MAX_JSON:
            raise Refusal('source file exceeds bound')
        payload = path.read_bytes()
        total += len(payload)
        blob_id = hashlib.sha1(b'blob ' + str(len(payload)).encode() + b'\0' + payload).hexdigest()
        if committed.get(name) != (mode, blob_id):
            raise Refusal('source payload differs from exact committed Git tree')
        if total > 128 * MAX_JSON or len(payload) != info.st_size or path.stat() != info:
            raise Refusal('source inventory changed or exceeded bound')
        inventory.append([name, mode, sha(payload)])
    if not inventory or {item[0] for item in inventory} != set(committed):
        raise Refusal('source inventory is empty or differs from committed tree')
    if command(['git', 'rev-parse', 'HEAD'], cwd=root).decode().strip() != commit:
        raise Refusal('source commit changed during inventory')
    if command(['git', 'status', '--porcelain=v1', '--untracked-files=all'], cwd=root):
        raise Refusal('source must remain clean during inventory')
    source = dict(repository=REPOSITORY, commit=commit, git_tree=tree,
                  payload_inventory_sha256=sha(json.dumps(inventory, separators=(',', ':')).encode()))
    tag_for(source, 'amd64')
    return source


def copy_build_context(root, directory):
    # Ignored private files must never enter Dockerfile's COPY context.
    for name in command(['git', 'ls-files', '-z'], cwd=root).decode().split('\0'):
        if not name:
            continue
        source = root / name
        if not source.is_file() or source.is_symlink() or source.resolve() != source or source.stat().st_nlink != 1:
            raise Refusal('build input is missing or linked')
        target = directory / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        target.chmod(stat.S_IMODE(source.stat().st_mode))


def tag_for(source, architecture):
    if (set(source) != SOURCE_KEYS or source['repository'] != REPOSITORY or
            not all(re.fullmatch(r'[0-9a-f]{40}', str(source[key])) for key in ('commit', 'git_tree')) or
            not re.fullmatch(HEX, str(source['payload_inventory_sha256'])) or architecture not in ('arm64', 'amd64')):
        raise Refusal('wrong source repository, clean commit identity, or native architecture')
    return 'dev-' + source['commit'] + '-' + architecture


def build_labels(source, build_input):
    return {'org.opencontainers.image.source': REPOSITORY,
            'org.opencontainers.image.revision': source['commit'],
            'org.opencontainers.image.licenses': 'Apache-2.0',
            'io.rpgmigrate.source.git-tree': source['git_tree'],
            'io.rpgmigrate.source.payload-inventory-sha256': source['payload_inventory_sha256'],
            'io.rpgmigrate.build-input-sha256': build_input,
            'org.opencontainers.image.version': 'm7-' + source['payload_inventory_sha256'][:12]}


def build_input_sha(source, architecture):
    return sha(canonical({'source': source, 'architecture': architecture,
                          'dockerfile': 'Dockerfile', 'version': 'm7-' + source['payload_inventory_sha256'][:12],
                          'build_format': 'oci'}))


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, url):
        target = urllib.parse.urlsplit(url)
        if (target.scheme != 'https' or target.username or target.password or
                not (target.hostname == 'ghcr.io' or str(target.hostname).endswith('.githubusercontent.com'))):
            raise Refusal('registry redirect refused')
        redirected = super().redirect_request(request, fp, code, message, headers, url)
        if redirected is not None:
            redirected.remove_header('Authorization')
        return redirected


class Registry:
    def __init__(self, username=None, token=None, *, read_only=False):
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), SafeRedirect())
        self.deadline = time.monotonic() + 2400
        self.github_token = token
        headers = {}
        if username is not None and token is not None:
            headers['Authorization'] = 'Basic ' + base64.b64encode((username + ':' + token).encode()).decode()
        scope = 'repository:ydixken/pgcopydb-operator:pull' + (',push' if headers and not read_only else '')
        status, _, payload = self.request('https://ghcr.io/token?' + urllib.parse.urlencode(
            {'service': 'ghcr.io', 'scope': scope}), headers, MAX_JSON)
        decoded = decode(payload)
        if status != 200 or not isinstance(decoded, dict) or not isinstance(decoded.get('token'), str) or not decoded['token']:
            raise Refusal('registry authorization unavailable')
        self.token = decoded['token']

    def request(self, url, headers, limit):
        try:
            response = self.opener.open(urllib.request.Request(url, headers=headers), timeout=45)
        except urllib.error.HTTPError as error:
            response = error
        except (OSError, ValueError) as error:
            raise Refusal('registry request failed') from error
        try:
            with response:
                chunks, size = [], 0
                while True:
                    if time.monotonic() >= self.deadline:
                        raise Refusal('registry response deadline exceeded')
                    chunk = response.read(min(65536, limit + 1 - size))
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > limit:
                        raise Refusal('registry response exceeded bound')
                    chunks.append(chunk)
                return response.status, dict(response.headers), b''.join(chunks)
        except (OSError, ValueError) as error:
            raise Refusal('registry response failed') from error

    def __call__(self, path, *, blob=False):
        return self.request('https://ghcr.io/v2/ydixken/pgcopydb-operator' + path,
                            {'Authorization': 'Bearer ' + self.token,
                             'Accept': ', '.join(sorted(MANIFEST_TYPES))}, MAX_BLOB if blob else MAX_JSON)

    def owner(self):
        headers = {'Authorization': 'Bearer ' + self.github_token, 'Accept': 'application/vnd.github+json',
                   'X-GitHub-Api-Version': '2022-11-28'}
        user_status, _, user = self.request('https://api.github.com/user', headers, MAX_JSON)
        repo_status, _, repository = self.request('https://api.github.com/repos/ydixken/pgcopydb-operator', headers, MAX_JSON)
        require_owner(user_status, user, repo_status, repository)

    def namespace(self):
        if not self.github_token:
            raise Refusal('authenticated exact namespace proof is required')
        status, _, payload = self.request('https://api.github.com/users/ydixken/packages/container/pgcopydb-operator',
            {'Authorization': 'Bearer ' + self.github_token, 'Accept': 'application/vnd.github+json',
             'X-GitHub-Api-Version': '2022-11-28'}, MAX_JSON)
        require_package(status, payload)
        status, _, payload = self('/tags/list?n=1')
        body = decode(payload)
        if status != 200 or not isinstance(body, dict) or body.get('name') != 'ydixken/pgcopydb-operator':
            raise Refusal('exact existing package namespace is inaccessible')


def require_owner(user_status, user, repo_status, repository):
    user, repository = decode(user), decode(repository)
    if (user_status != 200 or repo_status != 200 or not isinstance(user, dict) or user.get('login') != 'ydixken' or
            not isinstance(repository, dict) or repository.get('full_name') != 'ydixken/pgcopydb-operator' or
            not isinstance(repository.get('permissions'), dict) or repository['permissions'].get('admin') is not True):
        raise Refusal('local verification requires the exact owner and repository authority')


def require_package(status, payload):
    package = decode(payload)
    if (status != 200 or not isinstance(package, dict) or package.get('name') != 'pgcopydb-operator' or
            package.get('package_type') != 'container' or package.get('visibility') != 'public' or
            not isinstance(package.get('repository'), dict) or
            package['repository'].get('full_name') != 'ydixken/pgcopydb-operator'):
        raise Refusal('exact manager package is inaccessible, private, or linked to another repository')


def tag_state(fetch, tag):
    if not re.fullmatch(r'dev-[0-9a-f]{40}-(arm64|amd64)', tag):
        raise Refusal('only exact development tags are admitted')
    status, _, payload = fetch('/manifests/' + tag)
    body = decode(payload)
    if status == 404 and isinstance(body, dict) and body.get('errors') and all(
            isinstance(error, dict) and error.get('code') == 'MANIFEST_UNKNOWN' for error in body['errors']):
        return None
    if status != 200 or not isinstance(body, dict) or body.get('mediaType') not in MANIFEST_TYPES:
        raise Refusal('exact tag lookup is uncertain or is not one runnable manifest')
    return 'sha256:' + sha(payload)


def descriptor(value, media_types):
    if (not isinstance(value, dict) or value.get('mediaType') not in media_types or
            not re.fullmatch(DIGEST, str(value.get('digest', ''))) or
            type(value.get('size')) is not int or not 0 < value['size'] <= MAX_BLOB or value.get('urls')):
        raise Refusal('invalid typed image descriptor')


def read_graph(fetch, manifest_digest, architecture, labels, *, blobs=False):
    if not re.fullmatch(DIGEST, str(manifest_digest)):
        raise Refusal('invalid selected manifest digest')
    status, headers, manifest_raw = fetch('/manifests/' + manifest_digest)
    manifest = decode(manifest_raw)
    if (status != 200 or 'sha256:' + sha(manifest_raw) != manifest_digest or
            not isinstance(manifest, dict) or type(manifest.get('schemaVersion')) is not int or manifest['schemaVersion'] != 2 or
            manifest.get('mediaType') not in MANIFEST_TYPES or 'manifests' in manifest or 'subject' in manifest or
            not isinstance(manifest.get('layers'), list) or not manifest['layers'] or len(manifest['layers']) > 128):
        raise Refusal('selected runnable manifest graph is invalid')
    content_type = next((value.split(';')[0] for key, value in headers.items() if key.lower() == 'content-type'), None)
    if content_type is not None and content_type != manifest['mediaType']:
        raise Refusal('manifest response media type mismatch')
    advertised = next((value for key, value in headers.items() if key.lower() == 'docker-content-digest'), None)
    if advertised is not None and advertised != manifest_digest:
        raise Refusal('manifest response digest mismatch')
    config_descriptor = manifest.get('config')
    descriptor(config_descriptor, CONFIG_TYPES)
    status, _, config_raw = fetch('/blobs/' + config_descriptor['digest'])
    if status != 200 or len(config_raw) != config_descriptor['size'] or 'sha256:' + sha(config_raw) != config_descriptor['digest']:
        raise Refusal('manifest config descriptor does not match bytes')
    config = decode(config_raw)
    variant = config.get('variant') if isinstance(config, dict) else None
    if (not isinstance(config, dict) or config.get('os') != 'linux' or config.get('architecture') != architecture or
            architecture not in ('amd64', 'arm64') or variant not in ((None, 'v8') if architecture == 'arm64' else (None,))):
        raise Refusal('image configuration platform mismatch')
    runtime = config.get('config', {})
    if (not isinstance(runtime, dict) or runtime.get('User') != '65532:65532' or runtime.get('Entrypoint') != ['/manager'] or
            not isinstance(runtime.get('Labels'), dict) or any(runtime['Labels'].get(key) != value for key, value in labels.items())):
        raise Refusal('image source labels, user, or entrypoint mismatch')
    layers = []
    for layer in manifest['layers']:
        descriptor(layer, LAYER_TYPES)
        layers.append(dict(digest=layer['digest'], size=layer['size'], mediaType=layer['mediaType']))
        if blobs:
            status, _, payload = fetch('/blobs/' + layer['digest'], blob=True)
            if status != 200 or len(payload) != layer['size'] or 'sha256:' + sha(payload) != layer['digest']:
                raise Refusal('public layer blob differs from typed descriptor')
    graph = dict(manifest_digest=manifest_digest, config_digest=config_descriptor['digest'], index_digest=None,
                 os='linux', architecture=architecture, variant=variant,
                 manifest_base64=base64.b64encode(manifest_raw).decode(),
                 config_base64=base64.b64encode(config_raw).decode(), layers=layers)
    if blobs:
        return dict(graph=graph, blobs=[dict(digest=layer['digest'], size=layer['size']) for layer in layers])
    return graph


def require_reusable(receipt, source, architecture, graph, *, allow_pending=False):
    if (not isinstance(receipt, dict) or receipt.get('status') not in (('PUBLIC', 'PUBLICITY_PENDING') if allow_pending else ('PUBLIC',)) or
            receipt.get('source') != source or receipt.get('architecture') != architecture or
            any(not re.fullmatch(HEX, str(receipt.get(key, ''))) for key in
                ('build_input_sha256', 'executable_sha256', 'smoke_receipt_sha256'))):
        raise Refusal('existing development tag has no exact retained public receipt; overwrite refused')
    registry_graph = graph['graph']
    expected = publication_receipt(source, architecture, receipt['build_input_sha256'], receipt['executable_sha256'],
                                   receipt['smoke_receipt'], registry_graph,
                                   graph if receipt['status'] == 'PUBLIC' else None)
    if receipt != expected:
        raise Refusal('retained receipt differs from exact typed publication identity')


def push_once(push, readback):
    try:
        return push()
    except Refusal as error:
        try:
            readback()
        except Refusal:
            pass
        raise Refusal('PUSH_OUTCOME_UNKNOWN: no retry permitted; retained identity requires review') from error


def require_smoke(smoke, source, architecture, image_id, executable):
    keys = {'schema_version', 'image_id', 'os', 'architecture', 'executable_sha256', 'source_version',
            'help_passed', 'read_only', 'user', 'network', 'build_info_observed'}
    if (not isinstance(smoke, dict) or set(smoke) != keys or type(smoke['schema_version']) is not int or
            smoke['schema_version'] != 1 or smoke['image_id'] != image_id or smoke['os'] != 'linux' or
            smoke['architecture'] != architecture or smoke['executable_sha256'] != executable or
            not re.fullmatch(HEX, str(executable)) or
            smoke['source_version'] != 'm7-' + source['payload_inventory_sha256'][:12] or
            smoke['help_passed'] is not True or smoke['read_only'] is not True or smoke['user'] != '65532:65532' or
            smoke['network'] != 'none' or smoke['build_info_observed'] is not False):
        raise Refusal('retained manager smoke body is not the exact sanitized role projection')


def publication_receipt(source, architecture, build_input, executable, smoke, graph, public):
    require_smoke(smoke, source, architecture, graph['config_digest'], executable)
    receipt = dict(schema_version=1, status='PUBLIC' if public is not None else 'PUBLICITY_PENDING', role='manager',
                   tag=tag_for(source, architecture), source=source, os='linux', architecture=architecture,
                   variant=graph['variant'], index_digest=None, manifest_digest=graph['manifest_digest'],
                   config_digest=graph['config_digest'], build_input_sha256=build_input,
                   executable_sha256=executable, smoke_receipt_sha256=sha(canonical(smoke)), smoke_receipt=smoke,
                   authority={'visibility': 'public'}, registry_receipt=graph,
                   public_receipt=public, resolved_image=None)
    if public is not None:
        if public != dict(graph=graph, blobs=[dict(digest=layer['digest'], size=layer['size']) for layer in graph['layers']]):
            raise Refusal('public and authenticated image graphs differ')
        receipt['resolved_image'] = dict(role='manager', ref=PACKAGE + '@' + receipt['manifest_digest'], os='linux',
            architecture=architecture, variant=graph['variant'], digest=receipt['manifest_digest'], index_digest=None,
            config_digest=receipt['config_digest'], source=dict(source, build_input_sha256=build_input,
            executable_sha256=executable, smoke_receipt_sha256=sha(canonical(smoke))), provenance=dict(origin_ref=PACKAGE + ':' + receipt['tag'],
            registry_receipt_sha256=sha(canonical(graph)), public_receipt_sha256=sha(canonical(public))))
    return receipt


def private_directory(directory, root):
    if directory.is_relative_to(root):
        raise Refusal('publication evidence must be outside source checkout')
    directory.mkdir(mode=0o700, parents=False, exist_ok=True)
    info = directory.lstat()
    if directory.is_symlink() or directory.resolve() != directory or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise Refusal('publication directory must be private, owned, and unlinked')


def write_private(path, value):
    data = canonical(value) + b'\n'
    temporary = path.with_name(path.name + '.new')
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except OSError as error:
        raise Refusal('private publication receipt write failed') from error


def read_private(path):
    info = path.lstat()
    if (path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or
            stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > MAX_JSON):
        raise Refusal('retained receipt must be a bounded private owned file')
    return decode(path.read_bytes())


def smoke(image_id, source, architecture, directory, *, env=None):
    if not re.fullmatch(DIGEST, image_id):
        raise Refusal('smoke requires an immutable image ID')
    inspect = decode(command(['podman', 'image', 'inspect', image_id], env=env))
    if (not isinstance(inspect, list) or len(inspect) != 1 or inspect[0].get('Id') != image_id or
            inspect[0].get('Os') != 'linux' or inspect[0].get('Architecture') != architecture or
            inspect[0].get('Config', {}).get('User') != '65532:65532' or
            inspect[0].get('Config', {}).get('Entrypoint') != ['/manager']):
        raise Refusal('native smoke image identity or runtime configuration mismatch')
    help_output = command(['podman', 'run', '--rm', '--pull=never', '--network=none', '--read-only', '--cap-drop=ALL',
                           '--security-opt=no-new-privileges', '--user=65532:65532', '--tmpfs=/tmp:rw,noexec,nosuid,size=16m',
                           image_id, '--help'], env=env, combined=True)
    if b'--metrics-bind-address' not in help_output or b'--health-probe-bind-address' not in help_output:
        raise Refusal('native manager help smoke failed')
    container = command(['podman', 'create', '--pull=never', '--network=none', image_id], env=env).decode().strip()
    if not re.fullmatch(r'[0-9a-f]{64}', container):
        raise Refusal('smoke container identity invalid')
    binary = directory / 'manager-smoke-binary'
    try:
        command(['podman', 'cp', container + ':/manager', str(binary)], env=env)
        if binary.is_symlink() or not binary.is_file() or binary.stat().st_size > MAX_BLOB:
            raise Refusal('manager executable is not a bounded regular file')
        metadata = command(['go', 'version', '-m', str(binary)], env=env).decode()
        settings = {}
        for line in metadata.splitlines():
            if line.startswith('\tbuild\t') and '=' in line:
                key, value = line[len('\tbuild\t'):].split('=', 1)
                settings[key] = value.strip('"')
        version = 'm7-' + source['payload_inventory_sha256'][:12]
        if (settings.get('GOOS') != 'linux' or settings.get('GOARCH') != architecture or
                settings.get('CGO_ENABLED') != '0' or settings.get('-ldflags') != '-X main.version=' + version or
                'github.com/ydixken/pgcopydb-operator' not in metadata):
            raise Refusal('manager executable build metadata differs from admitted source version')
        return dict(schema_version=1, image_id=image_id, os='linux', architecture=architecture,
                    executable_sha256=sha(binary.read_bytes()), source_version=version, help_passed=True,
                    read_only=True, user='65532:65532', network='none', build_info_observed=False)
    finally:
        command(['podman', 'rm', container], env=env)
        binary.unlink(missing_ok=True)


def trusted_context(source, architecture):
    if (os.environ.get('GITHUB_ACTIONS') != 'true' or os.environ.get('GITHUB_EVENT_NAME') != 'push' or
            os.environ.get('GITHUB_REPOSITORY') != 'ydixken/pgcopydb-operator' or
            os.environ.get('GITHUB_REF') != 'refs/heads/' + BRANCH or
            os.environ.get('GITHUB_SHA') != source['commit']):
        raise Refusal('publisher requires the exact trusted feature-branch Actions source')
    native_architecture(architecture)
    if b'--retry' not in command(['podman', 'push', '--help']):
        raise Refusal('Podman lacks explicit no-retry publication support')


def native_architecture(architecture):
    if platform.system() != 'Linux' or {'aarch64': 'arm64', 'x86_64': 'amd64'}.get(platform.machine()) != architecture:
        raise Refusal('publication and executable verification require the native Linux architecture')


def local_verification_registry(source, architecture, retained, root):
    if (not isinstance(retained, dict) or retained.get('status') != 'PUBLICITY_PENDING' or retained.get('source') != source or
            retained.get('tag') != tag_for(source, architecture) or retained.get('architecture') != architecture):
        raise Refusal('local verification requires an exact retained pending publication')
    origin = command(['git', 'remote', 'get-url', 'origin'], cwd=root).decode().strip()
    if origin not in (REPOSITORY, REPOSITORY + '.git', 'git@github.com:ydixken/pgcopydb-operator.git'):
        raise Refusal('local verification source repository differs')
    token = os.environ.get('GH_TOKEN') or command(['gh', 'auth', 'token', '--hostname', 'github.com']).decode().strip()
    if not token:
        raise Refusal('local owner verification credential unavailable')
    registry = Registry('ydixken', token, read_only=True)
    registry.owner()
    return registry


def anonymous_smoke(receipt, source, architecture, directory):
    with tempfile.TemporaryDirectory(prefix='anonymous-', dir=directory) as temporary:
        auth = Path(temporary) / 'auth.json'
        write_private(auth, {'auths': {}})
        env = {'DOCKER_CONFIG': temporary, 'REGISTRY_AUTH_FILE': str(auth)}
        ref = PACKAGE + '@' + receipt['graph']['manifest_digest']
        command(['podman', 'pull', '--retry=0', '--tls-verify=true', '--authfile', str(auth), ref], timeout=600, env=env)
        observed = decode(command(['podman', 'image', 'inspect', ref], env=env))
        if not isinstance(observed, list) or len(observed) != 1 or observed[0].get('Id') != receipt['graph']['config_digest']:
            raise Refusal('anonymous digest pull config identity mismatch')
        return smoke(receipt['graph']['config_digest'], source, architecture, directory, env=env)


def publish(args):
    root = Path(__file__).resolve().parents[1]
    source = source_identity(root)
    architecture = args.architecture
    tag = tag_for(source, architecture)
    directory = Path(args.output_dir).absolute()
    private_directory(directory, root)
    build_input = build_input_sha(source, architecture)
    labels = build_labels(source, build_input)
    retained = read_private(Path(args.retained_receipt)) if args.retained_receipt else None
    local_verification = args.verify_only and os.environ.get('GITHUB_ACTIONS') != 'true'
    if local_verification:
        registry = local_verification_registry(source, architecture, retained, root)
    else:
        trusted_context(source, architecture)
        token = os.environ.get('GITHUB_TOKEN')
        actor = os.environ.get('GITHUB_ACTOR')
        if not token or not actor:
            raise Refusal('repository workflow publication credential unavailable')
        registry = Registry(actor, token)
    registry.namespace()
    existing = tag_state(registry, tag)
    if existing is not None:
        graph = read_graph(registry, existing, architecture, labels)
        public = read_graph(Registry(), existing, architecture, labels, blobs=True)
        require_reusable(retained, source, architecture, public, allow_pending=args.verify_only)
        observed = retained['smoke_receipt'] if local_verification else anonymous_smoke(public, source, architecture, directory)
        if (observed['executable_sha256'] != retained['executable_sha256'] or sha(canonical(observed)) != retained['smoke_receipt_sha256'] or
                build_input != retained['build_input_sha256'] or graph != retained['registry_receipt']):
            raise Refusal('retained tag executable, smoke, or build identity changed')
        verified = publication_receipt(source, architecture, build_input, observed['executable_sha256'],
                                       observed, graph, public)
        if source_identity(root) != source or tag_state(registry, tag) != existing:
            raise Refusal('source or exact retained tag changed during verification')
        write_private(directory / 'publication-receipt.json', verified)
        return
    if args.verify_only:
        raise Refusal('verification-only mode cannot create an absent development tag')
    iid_file = directory / 'image-id'
    if iid_file.exists():
        raise Refusal('image-ID evidence path already exists')
    argv = ['podman', 'build', '--format=oci', '--pull=always', '--platform=linux/' + architecture,
            '--build-arg', 'VERSION=m7-' + source['payload_inventory_sha256'][:12], '--build-arg', 'TARGETARCH=' + architecture,
            '--cpu-period=100000', '--cpu-quota=300000', '--memory=5g', '--memory-swap=5g', '--jobs=1', '--iidfile', str(iid_file)]
    for key, value in sorted(labels.items()):
        argv += ['--label', key + '=' + value]
    with tempfile.TemporaryDirectory(prefix='build-context-', dir=directory) as temporary:
        context = Path(temporary)
        copy_build_context(root, context)
        command(argv + [str(context)], cwd=root, timeout=2400)
    image_id = iid_file.read_text().strip()
    observed = smoke(image_id, source, architecture, directory)
    smoke_sha = sha(canonical(observed))
    write_private(directory / 'smoke-receipt.json', observed)
    write_private(directory / 'build-receipt.json', dict(source=source, architecture=architecture, image_id=image_id,
                  build_input_sha256=build_input, executable_sha256=observed['executable_sha256'], smoke_receipt_sha256=smoke_sha))
    if source_identity(root) != source:
        raise Refusal('source changed after build and smoke')
    if tag_state(registry, tag) is not None:
        raise Refusal('development tag appeared before push; overwrite refused')
    with tempfile.TemporaryDirectory(prefix='publish-auth-', dir=directory) as temporary:
        auth = Path(temporary) / 'auth.json'
        write_private(auth, {'auths': {'ghcr.io': {'auth': base64.b64encode((actor + ':' + token).encode()).decode()}}})
        digest_file = Path(temporary) / 'digest'
        def readback():
            current, state = None, 'uncertain'
            try:
                current = tag_state(registry, tag)
                state = 'present' if current is not None else 'absent'
            finally:
                write_private(directory / 'push-outcome.json', dict(status='PUSH_OUTCOME_UNKNOWN', source=source,
                              tag=tag, config_digest=image_id, observed_manifest_digest=current, readback=state))
        def push_command():
            command(['podman', 'push', '--retry=0', '--tls-verify=true', '--authfile', str(auth),
                     '--digestfile', str(digest_file), image_id, 'docker://' + PACKAGE + ':' + tag], timeout=600,
                    env={'DOCKER_CONFIG': temporary})
            try:
                pushed = digest_file.read_text().strip()
            except OSError as error:
                raise Refusal('push digest was not retained') from error
            if not re.fullmatch(DIGEST, pushed):
                raise Refusal('push digest is invalid')
            return pushed
        pushed = push_once(push_command, readback)
    if not re.fullmatch(DIGEST, pushed) or tag_state(registry, tag) != pushed:
        raise Refusal('published development tag readback differs from returned digest')
    graph = read_graph(registry, pushed, architecture, labels)
    if graph['config_digest'] != image_id:
        raise Refusal('published config differs from built and smoked immutable image')
    pending = publication_receipt(source, architecture, build_input, observed['executable_sha256'], observed, graph, None)
    write_private(directory / 'publication-receipt.json', pending)
    public = read_graph(Registry(), pushed, architecture, labels, blobs=True)
    public_smoke = anonymous_smoke(public, source, architecture, directory)
    if public_smoke != observed:
        raise Refusal('anonymous digest executable or smoke differs from retained immutable image')
    receipt = publication_receipt(source, architecture, build_input, observed['executable_sha256'], observed, graph, public)
    if source_identity(root) != source or tag_state(registry, tag) != pushed:
        raise Refusal('source or exact development tag changed during public verification')
    write_private(directory / 'publication-receipt.json', receipt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--architecture', required=True, choices=('arm64', 'amd64'))
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--retained-receipt')
    parser.add_argument('--verify-only', action='store_true')
    args = parser.parse_args()
    try:
        publish(args)
    except (Refusal, OSError, UnicodeError, ValueError, KeyError, TypeError):
        print('{"status":"REFUSED","detail":"publication failed; inspect sanitized retained receipts"}')
        return 1
    print('{"status":"PUBLIC","role":"manager"}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

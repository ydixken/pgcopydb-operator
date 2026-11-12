#!/usr/bin/env python3
"""Regenerate the twelve-role kind image lock from public registry digests and hosted receipts.

Upstream roles keep the digests of a pinning lock and are re-read anonymously from their registries.
The manager and native runner come from the re-downloadable publication receipts of their hosted runs.
"""
import argparse
import base64
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid

# Importing the guard must not leave bytecode, or the clean-checkout check refuses the next prepare.
sys.dont_write_bytecode = True
_spec = importlib.util.spec_from_file_location('e2e_kind', Path(__file__).with_name('e2e-kind.py'))
kind = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(kind)
registry = kind.registry
Refusal = registry.Refusal

REGISTRIES = {'ghcr.io': ('https://ghcr.io', 'https://ghcr.io/token?service=ghcr.io&scope=repository:{}:pull'),
              'docker.io': ('https://registry-1.docker.io',
                            'https://auth.docker.io/token?service=registry.docker.io&scope=repository:{}:pull')}
ACCEPT = ', '.join(sorted(registry.MANIFEST_TYPES | registry.INDEX_TYPES))
LIMIT = 4 * 1024 ** 2
HOSTED = {'manager': ('ydixken/pgcopydb-operator', 'manager-publication-{architecture}', 'publication-receipt.json'),
          'native-runner': ('ydixken/rpgmigrate', 'native-publication-{architecture}-{sha}', 'publication.json')}


class NoCredentialRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, url):
        if urllib.parse.urlsplit(url).scheme != 'https':
            raise Refusal('registry redirect left HTTPS')
        redirected = super().redirect_request(request, fp, code, message, headers, url)
        if redirected is not None:
            redirected.remove_header('Authorization')
        return redirected


def anonymous_fetch(repository, path, method='GET'):
    host, name = repository.split('/', 1)
    base, token_url = REGISTRIES[host]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoCredentialRedirect())

    def request(url, headers, method='GET'):
        try:
            response = opener.open(urllib.request.Request(url, headers=headers, method=method), timeout=60)
        except urllib.error.HTTPError as error:
            response = error
        except (OSError, ValueError) as error:
            raise Refusal('anonymous registry request failed: ' + type(error).__name__) from None
        with response:
            body = response.read(LIMIT + 1)
            if len(body) > LIMIT:
                raise Refusal('anonymous registry response exceeds its bound')
            return response.status, {key.lower(): value for key, value in response.headers.items()}, body

    status, _, body = request(token_url.format(name), {})
    token = registry.decode(body, 'registry token').get('token') if status == 200 else None
    if not isinstance(token, str) or not token:
        raise Refusal('anonymous registry token is unavailable')
    return request(base + '/v2/' + name + path, {'Authorization': 'Bearer ' + token, 'Accept': ACCEPT}, method)


def public_receipts(pin, fetch):
    repository = registry.repository(pin['ref'])

    def raw(prefix, digest):
        status, _, body = fetch(repository, prefix + digest)
        if status != 200 or 'sha256:' + registry.sha(body) != digest:
            raise Refusal(pin['role'] + ': public registry bytes differ from the pinned digest')
        return base64.b64encode(body).decode()

    graph = {'schema_version': 1, 'index_base64': raw('/manifests/', pin['index_digest']) if pin['index_digest'] else None,
             'manifest_base64': raw('/manifests/', pin['digest']), 'config_base64': raw('/blobs/', pin['config_digest'])}
    layers = registry.validate_graph(pin, graph)['layers']
    for layer in layers:
        status, headers, _ = fetch(repository, '/blobs/' + layer['digest'], 'HEAD')
        if status != 200 or headers.get('content-length', str(layer['size'])) != str(layer['size']):
            raise Refusal(pin['role'] + ': a layer blob is not anonymously public')
    return graph, {'graph': graph, 'blobs': [{'digest': layer['digest'], 'size': layer['size']} for layer in layers]}


def hosted_download(role, run, architecture):
    repository, artifact, name = HOSTED[role]
    sha = ''
    if '{sha}' in artifact:
        sha = subprocess.run(['gh', 'run', 'view', str(run), '-R', repository, '--json', 'headSha', '--jq', '.headSha'],
                             check=True, capture_output=True, text=True, timeout=60).stdout.strip()
    with tempfile.TemporaryDirectory(dir=kind.EVIDENCE_ROOT) as directory:
        subprocess.run(['gh', 'run', 'download', str(run), '-R', repository, '-n',
                        artifact.format(architecture=architecture, sha=sha), '-D', directory],
                       check=True, capture_output=True, timeout=300)
        return (Path(directory) / name).read_bytes()


def generate(guard, pins_path, anchor_paths, fetch, download):
    """Write the lock and every receipt into the guard's private directory and return the lock."""
    architecture = guard.architecture()
    pins = registry.validate_lock(registry.decode(guard.external_bytes(Path(pins_path), maximum=LIMIT), 'pinning lock'),
                                  architecture)
    outputs, images, publications = {}, {}, {}
    for role in registry.ROLES:
        if role in registry.DEV_ROLES:
            raw = download(role)
            receipt = registry.decode(raw, role + ' publication receipt')
            image = receipt.get('resolved_image') if isinstance(receipt, dict) else None
            registry.image(image, role, architecture)
            graph, public = receipt['registry_receipt'], receipt['public_receipt']
            registry.validate_publication(image, receipt, graph, public)
            outputs[role + '-publication-receipt.json'] = raw
            publications[role] = registry.sha(raw)
        else:
            image = {key: value for key, value in pins['images'][role].items()}
            graph, public = public_receipts(image, fetch)
            image['provenance'] = dict(image['provenance'], registry_receipt_sha256=registry.sha(registry.canonical(graph)),
                                       public_receipt_sha256=registry.sha(registry.canonical(public)))
        if (registry.sha(registry.canonical(graph)) != image['provenance']['registry_receipt_sha256'] or
                registry.sha(registry.canonical(public)) != image['provenance']['public_receipt_sha256']):
            raise Refusal(role + ': receipt hashes differ from the resolved image provenance')
        images[role] = image
        outputs[role + '-registry-receipt.json'] = registry.canonical(graph) + b'\n'
        outputs[role + '-public-receipt.json'] = registry.canonical(public) + b'\n'
    source = lambda role: {key: images[role]['source'][key] for key in registry.SOURCE_KEYS}
    lock = registry.validate_lock({'schema_version': 2, 'transport': 'public-registry', 'operator_source': source('manager'),
        'engine_source': source('native-runner'), 'images': images, 'publication_receipts_sha256': publications}, architecture)
    registry.validate_catalog(registry.decode((kind.CONFIG / 'images.json').read_bytes(), 'static catalog'), lock)
    pin = registry.decode((kind.CONFIG / 'images.json').read_bytes(), 'static catalog')['legacy_qualification']
    originals = {}
    for path in anchor_paths:
        raw = guard.external_bytes(Path(path), maximum=65536)
        matches = [name for name, _ in kind.LEGACY_ORIGINALS if registry.sha(raw) == pin['original_' + name + '_sha256']]
        if len(matches) != 1 or matches[0] in originals:
            raise Refusal('a legacy anchor matches no admitted original or repeats one')
        originals[matches[0]] = raw
    legacy = guard.legacy_receipt(lock, originals, registry.decode(outputs['legacy-runner-registry-receipt.json'], 'graph'))
    outputs['legacy-runner-executable-receipt.json'] = registry.canonical(legacy) + b'\n'
    for name, suffix in kind.LEGACY_ORIGINALS:
        outputs['legacy-runner-original-' + name + suffix] = originals[name]
    outputs['image-lock.json'] = json.dumps(lock, indent=2, sort_keys=True).encode() + b'\n'
    for name, raw in outputs.items():
        guard.write_private(name, raw, exclusive=True)
    manifest = {name: registry.sha(raw) for name, raw in sorted(outputs.items())}
    guard.write_private('lock-manifest.json', registry.canonical(manifest) + b'\n', exclusive=True)
    return lock


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pins', type=Path, required=True, help='earlier schema 2 lock whose upstream digests stay pinned')
    parser.add_argument('--manager-run', type=int, required=True, help='hosted run of the manager publication')
    parser.add_argument('--native-run', type=int, required=True, help='hosted run of the native runner publication')
    parser.add_argument('--legacy-anchor', type=Path, action='append', required=True, help='original legacy evidence file')
    parser.add_argument('--output', type=Path, help='fresh private directory, by default under /var/tmp')
    args = parser.parse_args()
    output = args.output or kind.EVIDENCE_ROOT / ('rpgmigrate-m7-lock-' + uuid.uuid4().hex)
    try:
        if output.exists():
            raise Refusal('lock output must be a fresh directory')
        guard = kind.Guard(output)
        runs = {'manager': args.manager_run, 'native-runner': args.native_run}
        lock = generate(guard, args.pins, args.legacy_anchor, anonymous_fetch,
                        lambda role: hosted_download(role, runs[role], guard.architecture()))
    except (Refusal, subprocess.SubprocessError, OSError) as error:
        message = str(error) if isinstance(error, Refusal) else type(error).__name__
        print(json.dumps({'command': 'lock', 'result': 'refused', 'error': message}), file=sys.stderr)
        return 1
    print(json.dumps({'command': 'lock', 'result': 'passed', 'output': str(output),
                      'image_lock_sha256': registry.sha(guard.private_bytes('image-lock.json')),
                      'operator_commit': lock['operator_source']['commit'], 'engine_commit': lock['engine_source']['commit']}))
    return 0


if __name__ == '__main__':
    sys.exit(main())

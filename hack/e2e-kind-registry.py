"""Validate the closed public image lock and typed registry metadata."""
import base64
import hashlib
import json
import re

ROLES = ('node', 'cnpg-controller', 'postgresql-16', 'postgresql-17', 'postgresql-18', 'manager',
         'legacy-runner', 'native-runner', 'seed-tool-runner', 'otel-collector', 'storage-helper', 'storage-provisioner')
PULL_ROLES = ('manager', 'cnpg-controller', 'postgresql-17', 'legacy-runner', 'seed-tool-runner',
              'otel-collector', 'storage-helper', 'storage-provisioner')
DEV_ROLES = ('manager', 'native-runner')
SOURCE_KEYS = {'repository', 'commit', 'git_tree', 'payload_inventory_sha256'}
IMAGE_KEYS = {'role', 'ref', 'os', 'architecture', 'variant', 'digest', 'index_digest', 'config_digest', 'provenance'}
BINDING_KEYS = {'node_id', 'requested_ref', 'manifest_digest', 'config_digest', 'os', 'architecture', 'variant', 'accepted_image_ids'}
MANIFEST_TYPES = {'application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json'}
INDEX_TYPES = {'application/vnd.oci.image.index.v1+json', 'application/vnd.docker.distribution.manifest.list.v2+json'}
CONFIG_TYPES = {'application/vnd.oci.image.config.v1+json', 'application/vnd.docker.container.image.v1+json'}
DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')
HASH = re.compile(r'[0-9a-f]{64}\Z')
COMMIT = re.compile(r'[0-9a-f]{40}\Z')
REPOSITORIES = {'node': 'docker.io/kindest/node', 'cnpg-controller': 'ghcr.io/cloudnative-pg/cloudnative-pg',
    'manager': 'ghcr.io/ydixken/pgcopydb-operator', 'native-runner': 'ghcr.io/ydixken/rpgmigrate',
    'legacy-runner': 'ghcr.io/ydixken/pgcopydb-operator/runner', 'seed-tool-runner': 'ghcr.io/ydixken/pgcopydb-operator/runner',
    'otel-collector': 'ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector',
    'storage-helper': 'docker.io/kindest/local-path-helper', 'storage-provisioner': 'docker.io/kindest/local-path-provisioner',
    **{'postgresql-' + str(n): 'ghcr.io/cloudnative-pg/postgresql' for n in (16, 17, 18)}}


class Refusal(Exception):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def decode(raw, description):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate')
            result[key] = value
        return result
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda value: (_ for _ in ()).throw(ValueError('number')))
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise Refusal(description + ': invalid or duplicate JSON') from None


def closed(value, keys, what):
    if not isinstance(value, dict) or set(value) != keys:
        raise Refusal(what + ': unexpected or missing fields')


def source(value, repository=None):
    closed(value, SOURCE_KEYS, 'source')
    if (repository is not None and value['repository'] != repository or
            value['repository'] not in ('https://github.com/ydixken/pgcopydb-operator', 'https://github.com/ydixken/rpgmigrate') or
            any(not isinstance(value[k], str) or not COMMIT.fullmatch(value[k]) for k in ('commit', 'git_tree')) or
            not isinstance(value['payload_inventory_sha256'], str) or not HASH.fullmatch(value['payload_inventory_sha256'])):
        raise Refusal('source identity is invalid')


def platform(os_name, architecture, variant):
    if os_name != 'linux' or architecture not in ('arm64', 'amd64') or not (variant is None or architecture == 'arm64' and variant == 'v8'):
        raise Refusal('unsupported image platform')


def repository(ref):
    if not isinstance(ref, str) or not re.fullmatch(r'[a-z0-9][a-zA-Z0-9._:/@-]*', ref) or '://' in ref:
        raise Refusal('invalid public reference')
    result = ref.split('@')[0]
    return result.rsplit(':', 1)[0] if ':' in result.rsplit('/', 1)[-1] else result


def image(value, role, architecture):
    closed(value, IMAGE_KEYS | ({'source'} if role in DEV_ROLES else set()), 'resolved image')
    platform(value['os'], value['architecture'], value['variant'])
    if (value['role'] != role or value['architecture'] != architecture or
            any(not isinstance(value[k], str) or not DIGEST.fullmatch(value[k]) for k in ('digest', 'config_digest')) or
            value['digest'] == value['config_digest'] or
            value['index_digest'] is not None and (not isinstance(value['index_digest'], str) or not DIGEST.fullmatch(value['index_digest']) or
                                                  value['index_digest'] in (value['digest'], value['config_digest']))):
        raise Refusal('resolved image digest, role or platform mismatch')
    repo = REPOSITORIES[role]
    expected = repo + (':' + role.rsplit('-', 1)[-1] if role.startswith('postgresql-') else '') + '@' + value['digest']
    if value['ref'] != expected:
        raise Refusal('resolved image repository or selected manifest mismatch')
    p = value['provenance']
    closed(p, {'origin_ref', 'registry_receipt_sha256', 'public_receipt_sha256'}, 'image provenance')
    if repository(p['origin_ref']) != repo or any(not isinstance(p[k], str) or not HASH.fullmatch(p[k]) for k in ('registry_receipt_sha256', 'public_receipt_sha256')):
        raise Refusal('image public provenance mismatch')
    if role in DEV_ROLES:
        s = value['source']
        closed(s, SOURCE_KEYS | {'build_input_sha256', 'executable_sha256', 'smoke_receipt_sha256'}, 'development source')
        source({k: s[k] for k in SOURCE_KEYS}, 'https://github.com/ydixken/' + ('rpgmigrate' if role == 'native-runner' else 'pgcopydb-operator'))
        if (value['index_digest'] is not None or p['origin_ref'] != repo + ':dev-' + s['commit'] + '-' + architecture or
                any(not isinstance(s[k], str) or not HASH.fullmatch(s[k]) for k in ('build_input_sha256', 'executable_sha256', 'smoke_receipt_sha256'))):
            raise Refusal('development publication identity mismatch')
    elif role.startswith('postgresql-') and p['origin_ref'] != repo + ':' + role.rsplit('-', 1)[-1]:
        raise Refusal('PostgreSQL origin major mismatch')


def validate_lock(lock, architecture):
    closed(lock, {'schema_version', 'transport', 'operator_source', 'engine_source', 'images', 'publication_receipts_sha256'}, 'image lock')
    if type(lock['schema_version']) is not int or lock['schema_version'] != 2 or lock['transport'] != 'public-registry':
        raise Refusal('schema 2 public-registry lock is required')
    source(lock['operator_source'], 'https://github.com/ydixken/pgcopydb-operator')
    source(lock['engine_source'], 'https://github.com/ydixken/rpgmigrate')
    closed(lock['images'], set(ROLES), 'image role map')
    closed(lock['publication_receipts_sha256'], set(DEV_ROLES), 'publication receipt hashes')
    for role in ROLES:
        image(lock['images'][role], role, architecture)
        if role in DEV_ROLES and ({k: lock['images'][role]['source'][k] for k in SOURCE_KEYS} != lock['operator_source' if role == 'manager' else 'engine_source'] or
                not isinstance(lock['publication_receipts_sha256'][role], str) or not HASH.fullmatch(lock['publication_receipts_sha256'][role])):
            raise Refusal('development source or full receipt hash mismatch')
    if lock['images']['seed-tool-runner']['ref'] != lock['images']['legacy-runner']['ref']:
        raise Refusal('seed runner must use the exact legacy executable')
    return lock


def validate_catalog(catalog, lock):
    closed(catalog, {'schema', 'cnpg', 'legacy_baseline', 'legacy_qualification', 'storage_manifest', 'images'}, 'static image catalog')
    if type(catalog['schema']) is not int or catalog['schema'] != 2 or catalog['legacy_baseline'] != '86bf908adca02d87063849dae775a02c1659c829' or not isinstance(catalog['images'], list):
        raise Refusal('schema 2 static catalog is required')
    seen, selected = set(), {}
    for item in catalog['images']:
        fields = {'role', 'os', 'architecture', 'variant', 'repository', 'origin_ref', 'digest', 'index_digest', 'config_digest'}
        closed(item, fields, 'static image entry')
        platform(item['os'], item['architecture'], item['variant'])
        role = item['role']
        if role not in ROLES or item['repository'] != REPOSITORIES[role]:
            raise Refusal('static image role or repository mismatch')
        key = (role, item['os'], item['architecture'], item['variant'])
        if key in seen:
            raise Refusal('duplicate static image platform')
        seen.add(key)
        if item['architecture'] == lock['images']['node']['architecture']:
            if role in selected:
                raise Refusal('ambiguous static image platform')
            selected[role] = item
    if set(selected) != set(ROLES):
        raise Refusal('static image role coverage is incomplete')
    for role, expected in selected.items():
        resolved = lock['images'][role]
        for k in ('digest', 'index_digest', 'config_digest'):
            if expected[k] is not None and expected[k] != resolved[k]:
                raise Refusal('resolved image differs from static pin')
        if expected['origin_ref'] is not None and expected['origin_ref'] != resolved['provenance']['origin_ref']:
            raise Refusal('resolved image differs from static public origin')
        if role in DEV_ROLES and any(expected[k] is not None for k in ('origin_ref', 'digest', 'index_digest', 'config_digest')):
            raise Refusal('development digests cannot be written into the source catalog')


def raw64(value):
    if not isinstance(value, str) or len(value) > 6 * 1024 ** 2:
        raise Refusal('raw registry metadata is oversized or unavailable')
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, TypeError):
        raise Refusal('invalid raw registry metadata encoding') from None
    if not raw or len(raw) > 4 * 1024 ** 2:
        raise Refusal('raw registry metadata size is invalid')
    return raw


def descriptor(value, types):
    if (not isinstance(value, dict) or not isinstance(value.get('digest'), str) or not DIGEST.fullmatch(value['digest']) or
            type(value.get('size')) is not int or value['size'] <= 0 or value.get('mediaType') not in types or
            any(k in value for k in ('urls', 'data'))):
        raise Refusal('unsupported typed registry descriptor')


def selected_platform(raw, architecture, variant):
    value = decode(raw, 'registry index')
    if not isinstance(value, dict) or type(value.get('schemaVersion')) is not int or value['schemaVersion'] != 2 or value.get('mediaType') not in INDEX_TYPES or not isinstance(value.get('manifests'), list) or not value['manifests']:
        raise Refusal('invalid registry index')
    selected, identities, digests, runnable, attestations = [], set(), set(), {}, []
    for item in value['manifests']:
        descriptor(item, MANIFEST_TYPES)
        if item['digest'] in digests:
            raise Refusal('duplicate index descriptor')
        digests.add(item['digest'])
        p = item.get('platform')
        if not isinstance(p, dict) or set(p) - {'os', 'architecture', 'variant', 'os.version', 'os.features'} or not isinstance(p.get('os'), str) or not isinstance(p.get('architecture'), str):
            raise Refusal('unknown index platform descriptor')
        if p == {'os': 'unknown', 'architecture': 'unknown'}:
            closed(item, {'mediaType', 'digest', 'size', 'platform', 'annotations'}, 'nonrunnable attestation descriptor')
            annotations = item['annotations']
            closed(annotations, {'vnd.docker.reference.type', 'vnd.docker.reference.digest'}, 'nonrunnable attestation markers')
            target = annotations['vnd.docker.reference.digest']
            if (item['mediaType'] != 'application/vnd.oci.image.manifest.v1+json' or
                    annotations['vnd.docker.reference.type'] != 'attestation-manifest' or
                    not isinstance(target, str) or not DIGEST.fullmatch(target)):
                raise Refusal('unsupported nonrunnable index descriptor')
            attestations.append(target)
            continue
        if p['os'] not in ('linux', 'windows') or p['architecture'] not in ('arm64', 'amd64', 'arm', '386', 'ppc64le', 's390x', 'riscv64') or p.get('variant') is not None and not isinstance(p['variant'], str):
            raise Refusal('unknown index platform')
        annotations = item.get('annotations', {})
        if not isinstance(annotations, dict) or any(key.startswith('vnd.docker.reference.') for key in annotations):
            raise Refusal('attestation marker cannot identify a runnable image')
        key = (p['os'], p['architecture'], p.get('variant'))
        if key in identities:
            raise Refusal('duplicate index platform descriptor')
        identities.add(key)
        runnable[item['digest']] = item
        if p['os'] == 'linux' and p['architecture'] == architecture:
            platform(p['os'], p['architecture'], p.get('variant'))
            selected.append(item)
    if len(attestations) != len(set(attestations)) or any(target not in runnable for target in attestations):
        raise Refusal('nonrunnable attestation target is missing, duplicated or cyclic')
    if len(selected) != 1 or selected[0]['platform'].get('variant') != variant:
        raise Refusal('registry index has no unique exact runnable platform')
    return selected[0]


def validate_graph(image, graph, development=False):
    if development:
        closed(graph, {'manifest_digest', 'config_digest', 'index_digest', 'os', 'architecture', 'variant', 'manifest_base64', 'config_base64', 'layers'}, 'development registry graph')
        if any(graph[k] != image[k if k != 'manifest_digest' else 'digest'] for k in ('manifest_digest', 'config_digest', 'index_digest', 'os', 'architecture', 'variant')):
            raise Refusal('development graph identity differs from lock')
        index = None
    else:
        closed(graph, {'schema_version', 'index_base64', 'manifest_base64', 'config_base64'}, 'upstream registry graph')
        if type(graph['schema_version']) is not int or graph['schema_version'] != 1:
            raise Refusal('upstream graph schema is invalid')
        index = raw64(graph['index_base64']) if graph['index_base64'] is not None else None
    raw_manifest, raw_config = raw64(graph['manifest_base64']), raw64(graph['config_base64'])
    if 'sha256:' + sha(raw_manifest) != image['digest'] or 'sha256:' + sha(raw_config) != image['config_digest']:
        raise Refusal('registry raw manifest or config digest mismatch')
    if index is not None:
        if 'sha256:' + sha(index) != image['index_digest']:
            raise Refusal('registry raw index digest mismatch')
        selected = selected_platform(index, image['architecture'], image['variant'])
        if selected['digest'] != image['digest'] or selected['size'] != len(raw_manifest):
            raise Refusal('registry selected manifest link mismatch')
    elif image['index_digest'] is not None:
        raise Refusal('registry index evidence is missing')
    manifest, config = decode(raw_manifest, 'manifest'), decode(raw_config, 'config')
    if not isinstance(manifest, dict) or type(manifest.get('schemaVersion')) is not int or manifest['schemaVersion'] != 2 or manifest.get('mediaType') not in MANIFEST_TYPES or any(key in manifest for key in ('manifests', 'subject', 'artifactType')):
        raise Refusal('runnable manifest schema is invalid')
    if index is not None and selected['mediaType'] != manifest['mediaType']:
        raise Refusal('selected descriptor media type differs from runnable manifest')
    descriptor(manifest.get('config'), CONFIG_TYPES)
    if manifest['config']['digest'] != image['config_digest'] or manifest['config']['size'] != len(raw_config):
        raise Refusal('manifest config descriptor mismatch')
    if not isinstance(config, dict) or config.get('os') != image['os'] or config.get('architecture') != image['architecture'] or config.get('variant') is not None and config['variant'] != image['variant']:
        raise Refusal('image config platform mismatch')
    layers = manifest.get('layers')
    if not isinstance(layers, list) or not layers:
        raise Refusal('manifest image layers are absent')
    for layer in layers:
        descriptor(layer, {'application/vnd.oci.image.layer.v1.tar', 'application/vnd.oci.image.layer.v1.tar+gzip', 'application/vnd.oci.image.layer.v1.tar+zstd', 'application/vnd.docker.image.rootfs.diff.tar.gzip'})
    projected = [{k: layer[k] for k in ('mediaType', 'digest', 'size')} for layer in layers]
    if development and graph['layers'] != projected:
        raise Refusal('registry layer inventory mismatch')
    return {'index': index, 'manifest': raw_manifest, 'config': raw_config, 'layers': projected}


def validate_public(image, graph, public):
    closed(public, {'graph', 'blobs'}, 'anonymous public receipt')
    metadata = validate_graph(image, graph, image['role'] in DEV_ROLES)
    if public['graph'] != graph or public['blobs'] != [{'digest': p['digest'], 'size': p['size']} for p in metadata['layers']]:
        raise Refusal('anonymous full-layer receipt is incomplete')
    return metadata


def validate_binding(binding, image, node_id):
    closed(binding, BINDING_KEYS, 'node image binding')
    expected = {'node_id': node_id, 'requested_ref': image['ref'], 'manifest_digest': image['digest'],
        'config_digest': image['config_digest'], 'os': image['os'], 'architecture': image['architecture'], 'variant': image['variant']}
    ids = binding['accepted_image_ids']
    if (any(binding[k] != v for k, v in expected.items()) or not isinstance(ids, list) or not ids or
            any(not isinstance(v, str) for v in ids) or ids != sorted(set(ids)) or
            not {image['digest'], image['config_digest']} <= set(ids)):
        raise Refusal('node image binding identity mismatch')
    repo = repository(image['ref'])
    allowed = {image['digest'], image['config_digest'], image['ref'], repo + '@' + image['digest']}
    if image['index_digest'] is not None:
        allowed.add(repo + '@' + image['index_digest'])
    if any(value not in allowed for value in ids):
        raise Refusal('node image binding contains an unproved raw image ID')
    return binding


def validate_publication(image, receipt, graph, public):
    closed(receipt, {'schema_version', 'status', 'role', 'source', 'tag', 'architecture', 'smoke_receipt',
        'smoke_receipt_sha256', 'authority', 'os', 'variant', 'index_digest', 'manifest_digest', 'config_digest',
        'registry_receipt', 'build_input_sha256', 'executable_sha256', 'public_receipt', 'resolved_image'}, 'publication receipt')
    expected_source = {k: image['source'][k] for k in SOURCE_KEYS}
    if (type(receipt['schema_version']) is not int or receipt['schema_version'] != 1 or receipt['status'] != 'PUBLIC' or
            receipt['role'] != image['role'] or receipt['source'] != expected_source or receipt['resolved_image'] != image or
            receipt['tag'] != 'dev-' + expected_source['commit'] + '-' + image['architecture'] or
            receipt['registry_receipt'] != graph or receipt['public_receipt'] != public or
            any(receipt[k] != image[k if k != 'manifest_digest' else 'digest'] for k in ('manifest_digest', 'config_digest', 'index_digest', 'os', 'architecture', 'variant')) or
            any(receipt[k] != image['source'][k] for k in ('build_input_sha256', 'executable_sha256', 'smoke_receipt_sha256')) or
            sha(canonical(receipt['smoke_receipt'])) != receipt['smoke_receipt_sha256']):
        raise Refusal('PUBLIC development receipt differs from the admitted lock')
    authority = receipt['authority']
    if authority != {'visibility': 'public'}:
        closed(authority, {'visibility', 'authority'}, 'first package authority')
        proof = authority['authority']
        closed(proof, {'owner', 'package_status', 'repository_admin', 'package_write_scope', 'checked_at'}, 'first package authority proof')
        if (image['role'] != 'native-runner' or authority['visibility'] != 'absent' or proof['owner'] != 'ydixken' or
                type(proof['package_status']) is not int or proof['package_status'] != 404 or proof['repository_admin'] is not True or
                proof['package_write_scope'] is not True or type(proof['checked_at']) is not int or proof['checked_at'] <= 0):
            raise Refusal('development package authority is invalid')
    metadata = validate_public(image, graph, public)
    config = decode(metadata['config'], 'development config')
    settings = config.get('config')
    if not isinstance(settings, dict) or not isinstance(settings.get('Labels'), dict):
        raise Refusal('development config source labels are unavailable')
    labels = settings['Labels']
    if (labels.get('org.opencontainers.image.source') != expected_source['repository'] or
            labels.get('org.opencontainers.image.revision') != expected_source['commit'] or
            labels.get('org.opencontainers.image.licenses') != 'Apache-2.0'):
        raise Refusal('development config source labels differ from clean publication')
    smoke = receipt['smoke_receipt']
    if image['role'] == 'manager':
        expected = {'schema_version': 1, 'image_id': image['config_digest'], 'os': 'linux', 'architecture': image['architecture'],
            'executable_sha256': receipt['executable_sha256'], 'source_version': 'm7-' + expected_source['payload_inventory_sha256'][:12],
            'help_passed': True, 'read_only': True, 'user': '65532:65532', 'network': 'none', 'build_info_observed': False}
        if smoke != expected:
            raise Refusal('manager smoke qualification is incomplete')
    else:
        closed(smoke, {'runtime_binary_sha256', 'python', 'source_inputs', 'image_id', 'architecture', 'effective_uid',
            'version', 'libpq', 'clients', 'default_psql', 'build_inputs_sha256', 'mounts', 'checks'}, 'native smoke')
        if (smoke['image_id'] != image['config_digest'] or smoke['architecture'] != image['architecture'] or
                smoke['runtime_binary_sha256'] != receipt['executable_sha256'] or smoke['build_inputs_sha256'] != receipt['build_input_sha256'] or
                type(smoke['effective_uid']) is not int or smoke['effective_uid'] != 65532 or smoke['checks'] != {'read_only_rootfs': True, 'work_write': True, 'tmp_write': True, 'root_write_denied': True, 'notices': True, 'gnu_utilities': True}):
            raise Refusal('native executable smoke identity is invalid')
        closed(smoke['python'], {'version', 'isolated', 'modules', 'symbols', 'metadata', 'metadata_sha256', 'mapped_library',
            'ctypes', 'private_input', 'packages', 'python_license', 'dependency_notices', 'notice_sha256', 'license_sha256'}, 'native Python smoke')
        if (smoke['python']['isolated'] is not True or
                any(not isinstance(smoke['python'][k], str) or not HASH.fullmatch(smoke['python'][k]) for k in ('notice_sha256', 'license_sha256'))):
            raise Refusal('native Python smoke or license qualification is invalid')
        validate_native_smoke(smoke, image)


def validate_native_smoke(smoke, image):
    inputs = smoke['source_inputs']
    if not isinstance(inputs, list) or not inputs or any(not isinstance(row, str) for row in inputs):
        raise Refusal('native build source inventory is unavailable')
    files = {}
    for row in inputs:
        match = re.fullmatch(r'([0-9a-f]{64})  ([A-Za-z0-9_./-]+)', row)
        if not match or match[2].startswith('/') or '..' in match[2].split('/') or match[2] in files:
            raise Refusal('native build source inventory is malformed or duplicated')
        files[match[2]] = match[1]
    if (not {'Cargo.toml', 'Cargo.lock', 'build.rs', 'LICENSE', 'NOTICE', 'Dockerfile', '.dockerignore'} <= files.keys() or
            not any(name.startswith('src/') for name in files) or
            sha(('\n'.join(inputs) + '\n').encode()) != smoke['build_inputs_sha256']):
        raise Refusal('native build input digest or complete source inventory differs')
    python = smoke['python']
    if (any(python[k] is not True for k in ('isolated', 'ctypes', 'private_input')) or
            not isinstance(python['version'], str) or not re.fullmatch(r'3\.[0-9]+\.[0-9]+', python['version']) or
            python['license_sha256'] != files['LICENSE'] or python['notice_sha256'] != files['NOTICE'] or
            not isinstance(python['metadata_sha256'], str) or not HASH.fullmatch(python['metadata_sha256'])):
        raise Refusal('native Python, NOTICE or LICENSE qualification differs from build inputs')
    modules = ['ctypes', '_ctypes', 'os', 'stat', 'fcntl', 'selectors', 'select', 'time', 'json', 'hashlib',
        'struct', 'errno', 'sys', 'binascii', 'resource']
    symbols = ['PQlibVersion', 'PQconnectStart', 'PQconnectPoll', 'PQsetnonblocking', 'PQsocket', 'PQstatus',
        'PQtransactionStatus', 'PQbackendPID', 'PQsetNoticeReceiver', 'PQresultErrorField', 'PQsendQueryParams',
        'PQflush', 'PQconsumeInput', 'PQisBusy', 'PQgetResult', 'PQresultStatus', 'PQcmdStatus', 'PQcmdTuples',
        'PQntuples', 'PQnfields', 'PQgetisnull', 'PQgetlength', 'PQgetvalue', 'PQclear', 'PQfinish',
        'PQescapeIdentifier', 'PQfreemem']
    if python['modules'] != modules or python['symbols'] != symbols:
        raise Refusal('native Python import or libpq symbol qualification is incomplete')
    metadata = python['metadata']
    closed(metadata, {'version', 'architecture', 'library_path', 'library_sha256', 'libpq_version', 'package_name', 'package_version'}, 'native libpq metadata')
    closed(smoke['libpq'], {'runtime_version', 'ldd', 'resolved_path', 'package_version', 'package_owner'}, 'native libpq smoke')
    if (type(metadata['version']) is not int or metadata['version'] != 1 or metadata['architecture'] != image['architecture'] or
            metadata['package_name'] != 'libpq5' or type(metadata['libpq_version']) is not int or not 180000 <= metadata['libpq_version'] < 190000 or
            python['mapped_library'] != metadata['library_path'] or smoke['libpq']['resolved_path'] != metadata['library_path'] or
            smoke['libpq']['runtime_version'] != metadata['libpq_version'] or smoke['libpq']['package_version'] != metadata['package_version'] or
            not isinstance(metadata['library_sha256'], str) or not HASH.fullmatch(metadata['library_sha256'])):
        raise Refusal('native Python and executable libpq identities differ')
    packages = python['packages']
    minor = python['version'].split('.')[1]
    required = {'python3', 'python3-minimal', 'python3.' + minor, 'python3.' + minor + '-minimal',
        'libpython3.' + minor + '-stdlib', 'libpython3.' + minor + '-minimal', 'libffi8', 'libc6', 'libpq5',
        'libssl3t64', 'postgresql-client-16', 'postgresql-client-18'}
    if not isinstance(packages, dict) or not required <= packages.keys():
        raise Refusal('native runtime package inventory is incomplete')
    for item in packages.values():
        closed(item, {'version', 'architecture', 'source', 'source_version', 'copyright_path', 'copyright_sha256'}, 'runtime package')
        if (any(not isinstance(item[k], str) or not item[k] for k in item) or item['architecture'] not in (image['architecture'], 'all') or
                not item['copyright_path'].startswith('/usr/share/doc/') or not HASH.fullmatch(item['copyright_sha256'])):
            raise Refusal('native package source or copyright qualification is invalid')
    closed(python['python_license'], {'path', 'sha256'}, 'Python license')
    if (packages['libpq5']['version'] != metadata['package_version'] or not python['python_license']['path'].startswith('/usr/') or
            not HASH.fullmatch(python['python_license']['sha256']) or not isinstance(python['dependency_notices'], dict) or
            'unicode-ident-1.0.27/LICENSE-UNICODE' not in python['dependency_notices'] or
            any(not isinstance(value, str) or not HASH.fullmatch(value) for value in python['dependency_notices'].values())):
        raise Refusal('native package or third-party license identity differs')
    closed(smoke['version'], {'protocol_version', 'engine_version', 'capabilities'}, 'native version protocol')
    version = smoke['version']
    if (type(version['protocol_version']) is not int or version['protocol_version'] != 1 or
            not isinstance(version['engine_version'], str) or not version['engine_version'] or
            not isinstance(version['capabilities'], list) or not version['capabilities'] or
            any(not isinstance(value, str) or not value for value in version['capabilities']) or
            len(version['capabilities']) != len(set(version['capabilities']))):
        raise Refusal('native version smoke protocol is invalid')
    closed(smoke['clients'], {'16', '18'}, 'native PostgreSQL client inventory')
    for major, clients in smoke['clients'].items():
        closed(clients, {'psql', 'pg_dump', 'pg_restore'}, 'native PostgreSQL client tools')
        if any(not isinstance(value, str) or not re.match(re.escape(tool) + r' \(PostgreSQL\) ' + major + r'\.[0-9]+\b', value) for tool, value in clients.items()):
            raise Refusal('native PostgreSQL client major is invalid')
    closed(smoke['mounts'], {'modes', 'work_owner', 'tmp_owner'}, 'native smoke mounts')
    closed(smoke['mounts']['modes'], {'/', '/work', '/tmp'}, 'native smoke mount modes')
    if (smoke['mounts']['work_owner'] != '65532:65532:700' or smoke['mounts']['tmp_owner'] != '65532:65532:700' or
            any(not isinstance(smoke['mounts']['modes'][path], str) or mode not in smoke['mounts']['modes'][path].split(',') for path, mode in (('/', 'ro'), ('/work', 'rw'), ('/tmp', 'rw'))) or
            not isinstance(smoke['default_psql'], str) or not re.match(r'psql \(PostgreSQL\) 18\.[0-9]+\b', smoke['default_psql'])):
        raise Refusal('native smoke isolation or default client is invalid')

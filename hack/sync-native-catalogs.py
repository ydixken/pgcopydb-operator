#!/usr/bin/env python3
"""Render the v7 receipt catalog fixtures from engine src/apply/metadata.rs at one commit.

Usage: hack/sync-native-catalogs.py ENGINE_CHECKOUT COMMIT
It writes internal/native/testdata/evidence/native_m8_* and refuses when the engine
builds its catalog SQL in a way this renderer does not mirror.
"""
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / 'internal' / 'native' / 'testdata' / 'evidence'
SOURCE = 'src/apply/metadata.rs'
# The Rust lines whose behaviour render_receipt and render_control reproduce by hand.
MIRRORED = [
    'format!(\n                "({}, \'{}\', {}, {}, {})",',
    '.join(", ");\n    let width = COLUMNS.len();',
    'let width = columns.len();\n    let key_count = keys.len();',
    'let zero_vector = vec!["0"; key_count].join(" ");',
    'format!("({},\'{name}\',{ty},{modifier},{required})", i + 1)',
    '.collect::<Vec<_>>().join(",");\n    let attributes = columns',
]


def fail(message):
    raise SystemExit('sync-native-catalogs: ' + message)


def git(repo, *args):
    out = subprocess.run(['git', '-C', repo, *args], capture_output=True, check=False)
    if out.returncode:
        fail('git %s failed' % ' '.join(args))
    return out.stdout


def body(source, name):
    match = re.search(r'\nfn %s\(.*?\n}\n' % name, source, re.S)
    if not match:
        fail('no fn %s' % name)
    return match.group(0)


def literal(text):
    if '\\' in text:
        fail('escaped Rust string literal')
    return text


def columns(source, name):
    match = re.search(r'const %s: \[\(&str, &str, u32, i32, bool\); (\d+)\] = \[(.*?)\];' % name, source, re.S)
    if not match:
        fail('no const %s' % name)
    numeric = re.search(r'const NUMERIC_U64: i32 = \(20 << 16\) \+ 4;', source) and (20 << 16) + 4
    rows = [(n, t, int(o), numeric if m == 'NUMERIC_U64' else int(m), r == 'true') for n, t, o, m, r in
            re.findall(r'\("(\w+)", "(\w+)", (\d+), (-1|NUMERIC_U64), (true|false)\)', match.group(2))]
    if len(rows) != int(match.group(1)) or not numeric:
        fail('cannot read %s' % name)
    return rows


def fill(template, values):
    if '{{' in template or '}}' in template:
        fail('escaped braces in a catalog template')
    sql = re.sub(r'\{(\w+)\}', lambda m: values[m.group(1)] if m.group(1) in values else fail('unknown {%s}' % m.group(1)), template)
    return ' '.join(line.strip() for line in sql.splitlines()) + ';'


def template(fn):
    found = re.findall(r'format!\(\s*r#"(.*?)"#', fn, re.S)
    if len(found) != 1:
        fail('expected one raw catalog template')
    return found[0]


def switches(fn, version):
    pattern = r'let (\w+) = if version >= (\d+) \{\s*"(.*?)"\s*\} else \{\s*"(.*?)"\s*\};'
    return {name: literal(new if version >= int(since) else old) for name, since, new, old in re.findall(pattern, fn, re.S)}


def constants(source):
    return {name: literal(value) for name, value in re.findall(r'^const (\w+): &str = "(.*?)";$', source, re.M)}


def render_control(source, version):
    fn = body(source, 'catalog_sql')
    rows = columns(source, 'COLUMNS')
    values = dict(constants(source), **switches(fn, version), width=str(len(rows)),
                  attributes=', '.join("(%d, '%s', %d, %d, %s)" % (i + 1, n, o, m, str(r).lower()) for i, (n, _, o, m, r) in enumerate(rows)))
    return fill(template(fn), values)


def render_receipt(source, version, large_objects):
    fn = body(source, 'validate_receipt_catalog')
    shapes = re.findall(r'\(\s*"(\w+)",\s*(\w+)\.as_slice\(\),\s*vec!\[([\d, ]+)\],\s*vec!\[(.*?)\],', fn, re.S)
    opclass = re.findall(r'format!\("(\(SELECT oid FROM pg_catalog\.pg_opclass [^"]*)"\)', fn)
    if [s[0] for s in shapes] != ['large_object_done', 'copy_done'] or len(opclass) != 1:
        fail('cannot read the receipt shapes')
    name, cols, keys, classes = shapes[0 if large_objects else 1]
    rows, keys, classes = columns(source, cols), keys.replace(' ', '').split(','), re.findall(r'"(\w+)"', classes)
    values = dict(constants(source), **switches(fn, version), name=name, width=str(len(rows)), key_count=str(len(keys)),
                  key_vector=' '.join(keys), key_array=','.join(keys), zero_vector=' '.join('0' * len(keys)),
                  opclasses=','.join(opclass[0].replace('{class}', c) for c in classes),
                  attributes=','.join("(%d,'%s',%d,%d,%s)" % (i + 1, n, o, m, str(r).lower()) for i, (n, _, o, m, r) in enumerate(rows)))
    return fill(template(fn), values)


def main(repo, ref):
    commit = git(repo, 'rev-parse', '--verify', ref + '^{commit}').decode().strip()
    raw = git(repo, 'show', commit + ':' + SOURCE)
    source = raw.decode()
    missing = [snippet for snippet in MIRRORED if snippet not in source]
    if missing:
        fail('engine changed code this renderer mirrors: %r' % missing[0][:60])
    header = '-- Generated from candidate %s at %s.\n' % (SOURCE, commit)
    files = {}
    for major in (16, 18):
        for kind, sql in (('catalog', render_control(source, major * 10000)),
                          ('marker_catalog', render_receipt(source, major * 10000, False)),
                          ('large_object_catalog', render_receipt(source, major * 10000, True))):
            files['native_m8_%s_%d.sql' % (kind, major)] = (header + sql + '\n').encode()
    shape = {table: [[n, t, o, m, str(r).lower()] for n, t, o, m, r in columns(source, name)]
             for table, name in (('checkpoint', 'COLUMNS'), ('copy_done', 'MARKER_COLUMNS'), ('large_object_done', 'LARGE_OBJECT_COLUMNS'))}
    files['native_m8_columns.json'] = (json.dumps(shape, indent=2) + '\n').encode()
    manifest = dict(generated=True, runtime_qualified=False, source=SOURCE, source_commit=commit,
                    source_tree=git(repo, 'rev-parse', commit + '^{tree}').decode().strip(),
                    source_sha256=hashlib.sha256(raw).hexdigest(), native_format=9, metadata_version=7,
                    files={name: hashlib.sha256(data).hexdigest() for name, data in files.items()})
    files['native_m8_source_manifest.json'] = (json.dumps(manifest, indent=2) + '\n').encode()
    for name, data in files.items():
        (OUT / name).write_bytes(data)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        fail('usage: hack/sync-native-catalogs.py ENGINE_CHECKOUT COMMIT')
    main(sys.argv[1], sys.argv[2])

import os
from pathlib import Path
import secrets
import subprocess
import sys
import unittest

# Executes the shipped receipt catalog statements on the server PGCOPYDB_TEST_PGURI names.
if sys.flags.optimize:
    raise SystemExit('optimized tests refused')

HERE = Path(__file__).resolve().parent
SCOPE = {'__name__': '_evidence_sql_live'}
exec(compile((HERE / 'evidence_program.py').read_bytes(), '<canonical>', 'exec'), SCOPE)
exec(compile((HERE / 'owned_inventory_program.py').read_bytes(), '<inventory>', 'exec'), SCOPE)
URI = os.environ.get('PGCOPYDB_TEST_PGURI', '')
SCHEMA = '_rpgmigrate_evidence_sql'
# Rendered from engine src/apply/metadata.rs receipt_create_sql at 2b9b11d, with the REVOKE the engine runs next.
RECEIPTS = f'''CREATE SCHEMA {SCHEMA};
CREATE TABLE {SCHEMA}.copy_done (magic pg_catalog.text COLLATE pg_catalog."C" NOT NULL, version pg_catalog.int4 NOT NULL, run_id pg_catalog.bytea NOT NULL, nonce pg_catalog.bytea NOT NULL, source_table pg_catalog.oid NOT NULL, target_table pg_catalog.oid NOT NULL, copy_generation pg_catalog.numeric(20,0) NOT NULL, rows pg_catalog.numeric(20,0) NOT NULL, bytes pg_catalog.numeric(20,0) NOT NULL, target_namespace pg_catalog.oid NOT NULL, target_owner pg_catalog.oid NOT NULL, copy_format pg_catalog.text COLLATE pg_catalog."C" NOT NULL, part_number pg_catalog.int4 NOT NULL, part_count pg_catalog.int4 NOT NULL, start_block pg_catalog.int8 NOT NULL, end_block pg_catalog.int8, PRIMARY KEY(run_id, nonce, copy_generation, source_table, target_table, part_number)) USING heap;
REVOKE ALL ON TABLE {SCHEMA}.copy_done FROM PUBLIC;
CREATE TABLE {SCHEMA}.large_object_done (magic pg_catalog.text COLLATE pg_catalog."C" NOT NULL, version pg_catalog.int4 NOT NULL, run_id pg_catalog.bytea NOT NULL, nonce pg_catalog.bytea NOT NULL, copy_generation pg_catalog.numeric(20,0) NOT NULL, object_oid pg_catalog.oid NOT NULL, target_owner pg_catalog.oid NOT NULL, target_acl pg_catalog.text COLLATE pg_catalog."C", target_comment pg_catalog.text COLLATE pg_catalog."C", source_metadata pg_catalog.text COLLATE pg_catalog."C" NOT NULL, logical_bytes pg_catalog.numeric(20,0) NOT NULL, content_hash pg_catalog.bytea NOT NULL, sync_phase pg_catalog.text COLLATE pg_catalog."C" NOT NULL, PRIMARY KEY(run_id, nonce, copy_generation, object_oid)) USING heap;
REVOKE ALL ON TABLE {SCHEMA}.large_object_done FROM PUBLIC;
'''
NULL = '\x01'


def psql(uri, script):
    out = subprocess.run(['psql', uri, '-XAtq', '-v', 'ON_ERROR_STOP=1', '-F', '\x1f', '-P', 'null=' + NULL, '-f', '-'],
                         input=script.encode(), capture_output=True, timeout=30, check=False)
    if out.returncode:
        raise AssertionError(out.stderr.decode(errors='replace'))
    return out.stdout.decode()


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


class LiveSession:
    """Sends each statement as Session.query does, through PREPARE and EXECUTE with untyped parameters."""

    def __init__(self, uri):
        self.uri = uri
        self.results = []

    def query(self, sql, params=(), tag='SELECT 1', state=0, rows=1, width=1, commit=False):
        body = sql.replace('@', 'pg_catalog.').rstrip().rstrip(';')
        execute = 'EXECUTE q(%s);' % ','.join(map(literal, params)) if params else 'EXECUTE q;'
        out = psql(self.uri, 'PREPARE q AS %s;\n%s\n' % (body, execute))
        result = [[None if cell == NULL else cell.encode() for cell in line.split('\x1f')] for line in out.splitlines()]
        self.results.append(result)
        return result


class LiveDatabase:
    def __init__(self):
        if not URI:
            raise AssertionError('PGCOPYDB_TEST_PGURI is required')
        self.name = 'rpgm_evidence_sql_' + secrets.token_hex(6)
        psql(URI, 'CREATE DATABASE %s;' % self.name)
        base, _, query = URI.partition('?')
        self.uri = base.rsplit('/', 1)[0] + '/' + self.name + ('?' + query if query else '')

    def scalar(self, sql):
        return psql(self.uri, sql).strip().splitlines()[-1]

    def drop(self):
        psql(URI, "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name LIKE '%s%%';\nDROP DATABASE %s WITH (FORCE);" % (self.name, self.name))


class EvidenceSQLLive(unittest.TestCase):
    def setUp(self):
        self.db = LiveDatabase()
        self.addCleanup(self.db.drop)
        self.major = int(self.db.scalar('SHOW server_version_num;')) // 10000
        self.assertIn(self.major, (16, 18))

    def test_ReceiptCatalogVerdicts(self):
        psql(self.db.uri, RECEIPTS)
        oid = lambda sql: int(self.db.scalar(sql))
        index = lambda table: oid("SELECT indexrelid FROM pg_index WHERE indrelid='%s.%s'::regclass;" % (SCHEMA, table))
        target = dict(intent=dict(binding=dict(owner_oid=oid("SELECT relowner FROM pg_class WHERE oid='%s.copy_done'::regclass;" % SCHEMA), metadata_version=7)),
                      schema_oid=oid("SELECT '%s'::regnamespace::oid;" % SCHEMA),
                      marker_table_oid=oid("SELECT '%s.copy_done'::regclass::oid;" % SCHEMA), marker_index_oid=index('copy_done'),
                      large_object_table_oid=oid("SELECT '%s.large_object_done'::regclass::oid;" % SCHEMA), large_object_index_oid=index('large_object_done'))
        verdict = lambda kind, major, target: LiveSession(self.db.uri).query(SCOPE['catalog_sql'](kind, major, 7), SCOPE['catalog_params'](target, kind))
        for kind in (True, 'large_object'):
            with self.subTest(kind=kind):
                self.assertEqual(verdict(kind, self.major, target), [[b't']])
                other = dict(target, intent=dict(binding=dict(target['intent']['binding'], owner_oid=target['intent']['binding']['owner_oid'] + 1)))
                self.assertEqual(verdict(kind, self.major, other), [[b'f']])
                prefix = 'large_object' if kind == 'large_object' else 'marker'
                swapped = dict(target, **{prefix + '_index_oid': target[('marker' if kind == 'large_object' else 'large_object') + '_index_oid']})
                self.assertEqual(verdict(kind, self.major, swapped), [[b'f']])
                if self.major == 18:
                    # Without the 18 clause, the engine's NOT NULL constraints read as foreign.
                    self.assertEqual(verdict(kind, 16, target), [[b'f']])


if __name__ == '__main__':
    unittest.main()

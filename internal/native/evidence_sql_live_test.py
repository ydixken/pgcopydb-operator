import os
from pathlib import Path
import secrets
import subprocess
import sys
import unittest

# Executes the shipped inventory and receipt catalog statements on the server PGCOPYDB_TEST_PGURI names.
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
        self.slot = self.db.name + '_s'
        self.pub = self.db.name + '_p'
        psql(self.db.uri, 'CREATE TABLE public.t1(id int PRIMARY KEY, v text); CREATE TABLE public.t2(id int, w int); CREATE PUBLICATION %s FOR TABLE public.t1, public.t2;' % self.pub)
        self.patch = mock_identity()
        self.addCleanup(self.patch)

    def state(self, cleanup=None):
        oid = lambda sql: int(self.db.scalar(sql))
        tables = [dict(source_oid=oid("SELECT 'public.%s'::regclass::oid;" % t), name=dict(schema='public', table=t), all_columns=cols)
                  for t, cols in (('t1', ['id', 'v']), ('t2', ['id', 'w']))]
        plan = dict(source=dict(identity=dict(database_oid=oid('SELECT oid FROM pg_database WHERE datname=current_database();'))), tables=tables)
        pub = dict(name=self.pub, oid=oid("SELECT oid FROM pg_publication WHERE pubname='%s';" % self.pub),
                   owner_oid=oid("SELECT pubowner FROM pg_publication WHERE pubname='%s';" % self.pub))
        binding = dict(slot=dict(intent=dict(binding=dict(slot_name=self.slot))), publication=pub)
        manifest = dict(clone_state=dict(plan=plan), follow_state=dict(binding=binding, cleanup=cleanup))

        class State:
            pass
        state = State()
        state.manifest = manifest
        return state

    def inventory(self, state):
        session = LiveSession(self.db.uri)
        try:
            return SCOPE['inventory_source'](session, state), session.results
        except SCOPE['Refusal'] as refusal:
            return refusal, session.results

    def assertRefusedAt(self, outcome, index):
        verdict, results = outcome
        self.assertIsInstance(verdict, SCOPE['Refusal'])
        self.assertEqual(results[1:], [[[b't']]] * (index - 1) + [[[b'f']]])

    def test_InventoryLogicalSlotAndPublication(self):
        self.assertEqual(self.db.scalar('SHOW wal_level;'), 'logical')
        psql(self.db.uri, "SELECT pg_catalog.pg_create_logical_replication_slot('%s', 'pgoutput');" % self.slot)
        verdict, results = self.inventory(self.state())
        self.assertEqual((verdict, len(results)), ('pinned', 4))
        state = self.state()
        state.manifest['clone_state']['plan']['source']['identity']['database_oid'] += 1
        self.assertRefusedAt(self.inventory(state), 1)
        self.assertRefusedAt(self.inventory(self.state(dict(slot_removed=True, publication_removed=False))), 1)

    def test_InventoryPublicationVerdicts(self):
        verdict, results = self.inventory(self.state())
        self.assertEqual((verdict, len(results)), ('pinned', 4))
        state = self.state()
        state.manifest['follow_state']['binding']['publication']['owner_oid'] += 1
        self.assertRefusedAt(self.inventory(state), 2)
        for table in (0, 1):
            with self.subTest(table=table):
                state = self.state()
                state.manifest['clone_state']['plan']['tables'][table]['source_oid'] += 1
                self.assertRefusedAt(self.inventory(state), 3)
        state = self.state()
        state.manifest['clone_state']['plan']['tables'][1]['all_columns'] = ['id']
        self.assertRefusedAt(self.inventory(state), 3)
        psql(self.db.uri, "SELECT pg_catalog.pg_create_physical_replication_slot('%s');" % self.slot)
        self.assertRefusedAt(self.inventory(self.state()), 1)

    def test_ReceiptCatalogVerdicts(self):
        psql(self.db.uri, RECEIPTS)
        oid = lambda sql: int(self.db.scalar(sql))
        index = lambda table: oid("SELECT indexrelid FROM pg_index WHERE indrelid='%s.%s'::regclass;" % (SCHEMA, table))
        target = dict(intent=dict(binding=dict(owner_oid=oid("SELECT relowner FROM pg_class WHERE oid='%s.copy_done'::regclass;" % SCHEMA), metadata_version=7)),
                      schema_oid=oid("SELECT '%s'::regnamespace::oid;" % SCHEMA),
                      marker_table_oid=oid("SELECT '%s.copy_done'::regclass::oid;" % SCHEMA), marker_index_oid=index('copy_done'),
                      large_object_table_oid=oid("SELECT '%s.large_object_done'::regclass::oid;" % SCHEMA), large_object_index_oid=index('large_object_done'))
        verdict = lambda kind, major, target: LiveSession(self.db.uri).query(SCOPE['catalog_sql'](kind, major, 7), SCOPE['catalog_params'](target, kind))
        # PGDG 16.16 and 18.7 stamp a server-owned pg_attver on every new column; earlier minors leave attoptions NULL.
        stamps = int(self.db.scalar('SHOW server_version_num;')) >= {16: 160016, 18: 180007}[self.major]
        for kind, table in ((True, 'copy_done'), ('large_object', 'large_object_done')):
            with self.subTest(kind=kind):
                options = "SELECT count(*) FILTER (WHERE attoptions IS NULL), count(*) FILTER (WHERE attoptions::text ~ '^[{]pg_attver=[0-9]+[}]$') FROM pg_attribute WHERE attrelid='%s.%s'::regclass AND attnum>0;" % (SCHEMA, table)
                width = int(self.db.scalar("SELECT relnatts FROM pg_class WHERE oid='%s.%s'::regclass;" % (SCHEMA, table)))
                unchanged = '0\x1f%d' % width if stamps else '%d\x1f0' % width
                self.assertEqual(self.db.scalar(options), unchanged)
                self.assertEqual(verdict(kind, self.major, target), [[b't']])
                psql(self.db.uri, 'ALTER TABLE %s.%s ALTER COLUMN magic SET (n_distinct = 1);' % (SCHEMA, table))
                self.assertEqual(verdict(kind, self.major, target), [[b'f']])
                psql(self.db.uri, 'ALTER TABLE %s.%s ALTER COLUMN magic RESET (n_distinct);' % (SCHEMA, table))
                self.assertEqual(self.db.scalar(options), unchanged)
                self.assertEqual(verdict(kind, self.major, target), [[b't']])
                other = dict(target, intent=dict(binding=dict(target['intent']['binding'], owner_oid=target['intent']['binding']['owner_oid'] + 1)))
                self.assertEqual(verdict(kind, self.major, other), [[b'f']])
                prefix = 'large_object' if kind == 'large_object' else 'marker'
                swapped = dict(target, **{prefix + '_index_oid': target[('marker' if kind == 'large_object' else 'large_object') + '_index_oid']})
                self.assertEqual(verdict(kind, self.major, swapped), [[b'f']])
                if self.major == 18:
                    # Without the 18 clause, the engine's NOT NULL constraints read as foreign.
                    self.assertEqual(verdict(kind, 16, target), [[b'f']])


def mock_identity():
    original = SCOPE['source_session_identity']
    SCOPE['source_session_identity'] = lambda session, source: 'pinned'
    return lambda: SCOPE.__setitem__('source_session_identity', original)


if __name__ == '__main__':
    unittest.main()

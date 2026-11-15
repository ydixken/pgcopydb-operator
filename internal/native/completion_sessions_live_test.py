import ctypes
import ctypes.util
import importlib.util
import json
import os
from pathlib import Path
import secrets
import sys
import time
import unittest

# Runs both completion observations on PGCOPYDB_TEST_PGURI with the programs' own input layout,
# the real one-use ConnectionInput and the real Session over the host libpq.
if sys.flags.optimize:
    raise SystemExit('optimized tests refused')

HERE = Path(__file__).resolve().parent


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


R = load('_completion_live_rejoin', 'cleanup_rejoin_completion_program_test.py')
C, S = R.C, R.S
LIVE = load('_completion_live_sql', 'evidence_sql_live_test.py')
# Rendered from engine src/apply/metadata.rs create_table_sql at 2b9b11d, with the REVOKEs the engine runs next.
CHECKPOINT = '''CREATE SCHEMA {schema} AUTHORIZATION CURRENT_USER CREATE TABLE {schema}."checkpoint" (magic pg_catalog.text COLLATE pg_catalog."C" NOT NULL, version pg_catalog.int4 NOT NULL, run_id pg_catalog.bytea NOT NULL, nonce pg_catalog.bytea NOT NULL, source_system pg_catalog.numeric(20,0) NOT NULL, source_database pg_catalog.oid NOT NULL, generation pg_catalog.numeric(20,0) NOT NULL, target_system pg_catalog.numeric(20,0), target_database pg_catalog.oid NOT NULL, owner_oid pg_catalog.oid NOT NULL, schema_oid pg_catalog.oid NOT NULL, table_oid pg_catalog.oid NOT NULL, applied_order pg_catalog.numeric(20,0) NOT NULL, last_xid pg_catalog.oid, begin_ordinal pg_catalog.numeric(20,0), source_commit_lsn pg_catalog.pg_lsn, source_end_lsn pg_catalog.pg_lsn, barrier_epoch pg_catalog.numeric(20,0) NOT NULL, barrier_counter pg_catalog.numeric(20,0) NOT NULL, copy_snapshot_token pg_catalog.text COLLATE pg_catalog."C", copy_consistent_point pg_catalog.pg_lsn, copy_plan_nonce pg_catalog.bytea, clone_state pg_catalog.text COLLATE pg_catalog."C", certified_lsn pg_catalog.pg_lsn, follow_binding pg_catalog.text COLLATE pg_catalog."C", drain_end_lsn pg_catalog.pg_lsn, drain_witness_lsn pg_catalog.pg_lsn, drain_kind pg_catalog.text COLLATE pg_catalog."C", follow_completed pg_catalog.bool NOT NULL, PRIMARY KEY (run_id, source_system, source_database, generation)) USING heap;
REVOKE ALL ON SCHEMA {schema} FROM PUBLIC;
REVOKE ALL ON TABLE {schema}."checkpoint" FROM PUBLIC;
'''


def runtime():
    directory = os.environ.get('PQ_LIB_DIR')
    names = [os.path.join(directory, name) for name in ('libpq.so.5', 'libpq.5.dylib')] if directory else [ctypes.util.find_library('pq')]
    path = next((name for name in names if name and (not directory or os.path.exists(name))), None)
    if path is None:
        raise AssertionError('libpq is required: set PQ_LIB_DIR to the directory that holds it')
    library = ctypes.CDLL(path)
    # bind_abi also pins the runner's exact libpq; the host library only has to offer the same ABI.
    for name, (result, args) in S['ABI'].items():
        function = getattr(library, name)
        function.argtypes, function.restype = args, result
    if library.PQlibVersion() < 160000:
        raise AssertionError('libpq 16 or later is required')
    return library


class Pair:
    """A source and a target database on the test server; the program sees each through its own URI."""

    def __init__(self):
        if not LIVE.URI:
            raise AssertionError('PGCOPYDB_TEST_PGURI is required')
        base, _, query = LIVE.URI.partition('?')
        if ':' not in base.split('//', 1)[-1].split('@', 1)[0]:
            raise AssertionError('PGCOPYDB_TEST_PGURI must carry its password; a passfile needs the runner filesystem')
        # TLS file pins need the runner filesystem as well.
        options = '&'.join([item for item in query.split('&') if item and not item.startswith('sslmode=')] + ['sslmode=disable'])
        tag = secrets.token_hex(5)
        self.names = dict(source='rpgm_cs_%s_s' % tag, target='rpgm_cs_%s_t' % tag)
        self.slot, self.publication = 'rpgm_cs_%s_slot' % tag, 'rpgm_cs_%s_pub' % tag
        self.uris = {role: base.rsplit('/', 1)[0] + '/' + name + '?' + options for role, name in self.names.items()}
        for name in self.names.values():
            LIVE.psql(LIVE.URI, 'CREATE DATABASE %s;' % name)

    def scalar(self, role, sql):
        return LIVE.psql(self.uris[role], sql).strip().splitlines()[-1]

    def drop(self):
        LIVE.psql(LIVE.URI, "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = '%s';\n" % self.slot +
                  ''.join('DROP DATABASE IF EXISTS %s WITH (FORCE);\n' % name for name in self.names.values()))


def rewrite(value, swaps):
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                rewrite(item, swaps)
            elif type(item) in (int, str) and (key, item) in swaps:
                value[key] = swaps[(key, item)]
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, (dict, list)):
                rewrite(item, swaps)


class CompletionSessionsLive(unittest.TestCase):
    def setUp(self):
        self.pair = Pair()
        self.addCleanup(self.pair.drop)
        self.major = int(self.pair.scalar('target', 'SHOW server_version_num;')) // 10000
        self.assertIn(self.major, (16, 18))
        self.library = runtime()

    def observe(self, program, flags, slot, publication):
        """Builds the private state and both databases, then runs the program's observation once."""
        pair = self.pair
        if slot:
            pair.scalar('source', "SELECT pg_catalog.pg_create_logical_replication_slot('%s', 'pgoutput');" % pair.slot)
        # The fixture plan holds one table, public.records(id), which the publication must carry.
        LIVE.psql(pair.uris['source'], 'CREATE TABLE public.records (id pg_catalog.int4 PRIMARY KEY);' + ('CREATE PUBLICATION %s FOR TABLE public.records;' % pair.publication if publication else ''))
        with C.tree() as t:
            if program == 'cleaned':
                C.prepare(t)
            else:
                R.prepare(t, flags)
            C.candidate(t)
            schema = t.value['target_provenance'][0]['record']['intent']['binding']['schema_name']
            LIVE.psql(pair.uris['target'], CHECKPOINT.format(schema=schema) + LIVE.RECEIPTS.split('\n', 1)[1].replace(LIVE.SCHEMA, schema))
            oid = lambda role, sql: int(pair.scalar(role, sql))
            relation = lambda name: oid('target', "SELECT '%s.%s'::regclass::oid;" % (schema, name))
            index = lambda name: oid('target', "SELECT indexrelid FROM pg_index WHERE indrelid='%s.%s'::regclass;" % (schema, name))
            system = oid('source', 'SELECT system_identifier FROM pg_control_system();')
            timeline = oid('source', 'SELECT timeline_id FROM pg_control_checkpoint();')
            user = oid('target', 'SELECT oid FROM pg_roles WHERE rolname=CURRENT_USER;')
            database = {role: oid(role, 'SELECT oid FROM pg_database WHERE datname=current_database();') for role in ('source', 'target')}
            swaps = {('system_identifier', 123): system, ('database_oid', 11): database['source'], ('system_identifier', 456): system,
                     ('database_oid', 22): database['target'], ('target_system_identifier', 456): system, ('target_database_oid', 22): database['target'],
                     ('owner_oid', 33): user, ('target_owner_oid', 33): user, ('source_oid', 101): oid('source', "SELECT 'public.records'::regclass::oid;"), ('major', 16): self.major, ('major', 18): self.major, ('timeline', 1): timeline, ('source_timeline', 1): timeline,
                     ('slot_name', 'native_slot'): pair.slot, ('name', 'native_publication'): pair.publication,
                     ('schema_oid', 44): oid('target', "SELECT '%s'::regnamespace::oid;" % schema), ('table_oid', 55): relation('"checkpoint"'),
                     ('marker_table_oid', 66): relation('copy_done'), ('marker_index_oid', 77): index('copy_done'),
                     ('large_object_table_oid', 78): relation('large_object_done'), ('large_object_index_oid', 79): index('large_object_done')}
            if publication:
                swaps[('oid', 88)] = oid('source', "SELECT oid FROM pg_publication WHERE pubname='%s';" % pair.publication)
            rewrite(t.value, swaps)
            t.write()
            endpos = t.root / 'control' / 'endpos.json'
            control = json.loads(endpos.read_bytes())
            rewrite(control, swaps)
            endpos.write_bytes(json.dumps(control).encode())
            m, target = t.value, t.value['target_provenance'][0]['record']
            _, _, _, row = C.I.FIXTURE.candidate_fixture(os.stat(HERE))
            binding = target['intent']['binding']
            row[4:12] = [str(n) for n in (system, database['source'], 1, system, database['target'], user, target['schema_oid'], target['table_oid'])]
            clone = json.loads(row[22])
            clone.update(source_timeline=timeline, table_count=len(m['clone_state']['plan']['tables']))
            row[22], row[24] = json.dumps(clone), json.dumps(dict(binding=m['follow_state']['binding'], operator=m['operator'], origin_name=None))
            cells = ['NULL' if cell is None else "pg_catalog.decode(%s, 'hex')" % LIVE.literal(cell) if kind == 'bytea' else LIVE.literal(cell) + '::pg_catalog.' + kind
                     for (_, kind), cell in zip(S['COLUMNS'], row)]
            LIVE.psql(pair.uris['target'], 'INSERT INTO %s."checkpoint" (%s) VALUES (%s);' % (schema, ', '.join(c for c, _ in S['COLUMNS']), ', '.join(cells)))
            self.assertEqual(binding['table_name'], 'checkpoint')
            state = S['CleanedState' if program == 'cleaned' else 'CleanupRejoinState'](t.req)
            quiet, materials = C.Quiet(), None
            try:
                materials = C.prepared(S['cleaned_inputs' if program == 'cleaned' else 'rejoin_inputs'], dict(source=pair.uris['source'].encode(), target=pair.uris['target'].encode()), self.library)
                observe = S['observe_cleaned' if program == 'cleaned' else 'observe_cleanup_rejoin']
                try:
                    return observe(state, self.library, materials, time.monotonic() + 25, quiet), materials
                except S['Refusal'] as refusal:
                    return refusal, materials
            finally:
                state.close()
                quiet.close()
                for item in (materials.inputs.values() if materials else ()):
                    item.close()

    def assertVerdict(self, outcome):
        verdict, materials = outcome
        self.assertNotIsInstance(verdict, S['Refusal'], getattr(verdict, 'error_class', ''))
        checkpoint, guards = verdict
        self.assertEqual((checkpoint['checkpoint_id'], checkpoint['drain_end_lsn'], guards), ('1:0:7', '0/20', S['GUARDS']))
        C.assert_spent(self, materials)

    def assertRefused(self, outcome):
        verdict, _ = outcome
        self.assertIsInstance(verdict, S['Refusal'])
        self.assertEqual(verdict.error_class, S['RESULT'])

    def test_CleanedVerdicts(self):
        self.assertVerdict(self.observe('cleaned', None, False, False))

    def test_CleanedRefusesPresentPublication(self):
        self.assertRefused(self.observe('cleaned', None, False, True))

    def test_RejoinPublicationVerdict(self):
        self.assertVerdict(self.observe('rejoin', (True, False), False, True))

    def test_RejoinLogicalSlotVerdicts(self):
        self.assertEqual(self.pair.scalar('source', 'SHOW wal_level;'), 'logical')
        self.assertVerdict(self.observe('rejoin', (False, True), True, False))

    def test_RejoinLogicalSlotAndPublicationVerdict(self):
        self.assertVerdict(self.observe('rejoin', (False, False), True, True))

    def test_RejoinRefusesRemovedLogicalSlot(self):
        self.assertRefused(self.observe('rejoin', (True, False), True, True))


if __name__ == '__main__':
    unittest.main()

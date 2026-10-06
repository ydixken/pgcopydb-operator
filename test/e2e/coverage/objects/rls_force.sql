-- coverage: group=own  expect=preflight_refuses_rls  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.docs (id int PRIMARY KEY, tenant text NOT NULL, body text);
INSERT INTO ${schema}.docs SELECT g, 'tenant' || (g % 3), 'doc ' || g FROM generate_series(1, 30) g;
ALTER TABLE ${schema}.docs ENABLE ROW LEVEL SECURITY;
-- FORCE subjects the owner, the migration role, to the policies: it sees the tenant0 rows only.
ALTER TABLE ${schema}.docs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant0_read ON ${schema}.docs FOR SELECT TO PUBLIC USING (tenant = 'tenant0');
CREATE POLICY reader_all ON ${schema}.docs AS PERMISSIVE FOR SELECT TO cov_reader USING (true);
CREATE POLICY any_insert ON ${schema}.docs AS PERMISSIVE FOR INSERT TO PUBLIC WITH CHECK (true);
CREATE POLICY no_tenant2_write ON ${schema}.docs AS RESTRICTIVE FOR INSERT TO PUBLIC WITH CHECK (tenant <> 'tenant2');
-- @follow
-- The preflight refuses the table before this runs: the migration role could not see the row.
INSERT INTO ${schema}.docs VALUES (31, 'tenant1', 'written during follow');

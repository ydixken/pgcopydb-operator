-- coverage: group=own  expect=preflight_refuses_unlogged  schemas=1  min_pg=14
-- @setup
CREATE UNLOGGED TABLE ${schema}.cache (id int PRIMARY KEY, val text);
INSERT INTO ${schema}.cache SELECT g, 'v' || g FROM generate_series(1, 10) g;
-- @follow
-- These writes reach no WAL, so follow would lose them; the preflight refuses the table before they run.
INSERT INTO ${schema}.cache VALUES (11, 'live');
UPDATE ${schema}.cache SET val = 'upd' WHERE id = 1;
DELETE FROM ${schema}.cache WHERE id = 2;

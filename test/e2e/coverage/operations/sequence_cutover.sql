-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id bigserial PRIMARY KEY, note text);
CREATE SEQUENCE ${schema}.standalone;
INSERT INTO ${schema}.t (note) SELECT 'seed' FROM generate_series(1, 10);
SELECT setval('${schema}.standalone', 100);
-- @follow
-- The target keeps its base-copy values while streaming; cutover re-syncs them.
-- setval backwards, is_called false, and a jump: values the target must not reuse.
SELECT setval('${schema}.standalone', 50, false);
SELECT setval('${schema}.t_id_seq', 1000);
INSERT INTO ${schema}.t (note) VALUES ('after jump');

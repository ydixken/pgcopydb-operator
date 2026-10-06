-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, name text);
INSERT INTO ${schema}.t SELECT g, 'n' || g FROM generate_series(1, 20) g;
-- @follow
-- pgoutput publishes TRUNCATE; the rows inserted after it must be all that is left.
TRUNCATE ${schema}.t;
INSERT INTO ${schema}.t VALUES (1, 'after truncate'), (2, 'after truncate');

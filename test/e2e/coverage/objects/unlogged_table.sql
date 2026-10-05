-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE UNLOGGED TABLE ${schema}.cache (id int PRIMARY KEY, val text);
CREATE INDEX ON ${schema}.cache (val);
INSERT INTO ${schema}.cache SELECT g, 'v' || g FROM generate_series(1, 20) g;

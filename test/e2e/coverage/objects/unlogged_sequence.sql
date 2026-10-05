-- coverage: group=clone  expect=identical  schemas=1  min_pg=15
-- @setup
CREATE UNLOGGED SEQUENCE ${schema}.cache_seq;
CREATE SEQUENCE ${schema}.logged_seq;
SELECT nextval('${schema}.cache_seq') FROM generate_series(1, 3);
SELECT nextval('${schema}.logged_seq') FROM generate_series(1, 2);

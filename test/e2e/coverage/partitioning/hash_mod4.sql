-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.sessions (
  id bigint PRIMARY KEY,
  token text NOT NULL
) PARTITION BY HASH (id);
CREATE TABLE ${schema}.sessions_0 PARTITION OF ${schema}.sessions FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE ${schema}.sessions_1 PARTITION OF ${schema}.sessions FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE ${schema}.sessions_2 PARTITION OF ${schema}.sessions FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE ${schema}.sessions_3 PARTITION OF ${schema}.sessions FOR VALUES WITH (MODULUS 4, REMAINDER 3);
INSERT INTO ${schema}.sessions SELECT g, md5(g::text) FROM generate_series(1, 40) g;

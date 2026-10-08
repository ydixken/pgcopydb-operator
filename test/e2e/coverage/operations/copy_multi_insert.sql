-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
-- COPY writes one multi-insert WAL record per heap page, and every row in it shares that record's LSN.
-- The defaults are constants: a volatile default would make COPY insert row by row.
CREATE TABLE ${schema}.copied (id int PRIMARY KEY, payload text NOT NULL DEFAULT repeat('c', 32));
CREATE TABLE ${schema}.routed (id int PRIMARY KEY, payload text NOT NULL DEFAULT repeat('r', 32))
  PARTITION BY RANGE (id);
CREATE TABLE ${schema}.routed_low PARTITION OF ${schema}.routed FOR VALUES FROM (MINVALUE) TO (1000);
CREATE TABLE ${schema}.routed_high PARTITION OF ${schema}.routed FOR VALUES FROM (1000) TO (MAXVALUE);
CREATE TABLE ${schema}.inserted (id int PRIMARY KEY, payload text NOT NULL DEFAULT repeat('i', 32));
-- @follow
-- psql sends \copy as COPY FROM STDIN; 2000 of these rows fill more than ten heap pages.
\copy ${schema}.copied (id) FROM PROGRAM 'seq 1 2000'
\copy ${schema}.routed (id) FROM PROGRAM 'seq 1 2000'
-- The control: INSERT ... SELECT writes one WAL record per row.
INSERT INTO ${schema}.inserted (id) SELECT g FROM generate_series(1, 2000) g;

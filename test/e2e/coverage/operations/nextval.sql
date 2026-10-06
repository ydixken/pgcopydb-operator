-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id serial PRIMARY KEY, note text);
CREATE TABLE ${schema}.ident (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, note text);
CREATE SEQUENCE ${schema}.standalone START 500;
INSERT INTO ${schema}.t (note) SELECT 'seed' FROM generate_series(1, 10);
INSERT INTO ${schema}.ident (note) SELECT 'seed' FROM generate_series(1, 10);
-- @follow
INSERT INTO ${schema}.t (note) SELECT 'live' FROM generate_series(1, 5);
INSERT INTO ${schema}.ident (note) SELECT 'live' FROM generate_series(1, 5);
-- nextval with no row written: only the sequence moves.
SELECT nextval('${schema}.standalone') FROM generate_series(1, 7);
SELECT nextval('${schema}.t_id_seq');

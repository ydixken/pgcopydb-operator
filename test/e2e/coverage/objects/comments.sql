-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, val text CONSTRAINT val_len CHECK (length(val) < 100));
CREATE INDEX t_val_idx ON ${schema}.t (val);
CREATE VIEW ${schema}.v AS SELECT id FROM ${schema}.t;
CREATE FUNCTION ${schema}.f() RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE TYPE ${schema}.e AS ENUM ('x');
CREATE SEQUENCE ${schema}.s;
COMMENT ON TABLE ${schema}.t IS 'a table';
COMMENT ON COLUMN ${schema}.t.val IS 'a column with ''quotes'' and a
newline';
COMMENT ON INDEX ${schema}.t_val_idx IS 'an index';
COMMENT ON CONSTRAINT val_len ON ${schema}.t IS 'a constraint';
COMMENT ON VIEW ${schema}.v IS 'a view';
COMMENT ON FUNCTION ${schema}.f() IS 'a function';
COMMENT ON TYPE ${schema}.e IS 'a type';
COMMENT ON SEQUENCE ${schema}.s IS 'a sequence';
INSERT INTO ${schema}.t SELECT g, 'v' || g FROM generate_series(1, 10) g;

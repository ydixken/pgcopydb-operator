-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
-- cov_reader exists on both sides; the harness creates it before any case runs.
CREATE TABLE ${schema}.t (id int PRIMARY KEY, public_col text, secret_col text);
CREATE SEQUENCE ${schema}.s;
CREATE FUNCTION ${schema}.f() RETURNS int LANGUAGE sql AS 'SELECT 1';
GRANT USAGE ON SCHEMA ${schema} TO cov_reader;
GRANT SELECT (id, public_col), UPDATE (public_col) ON ${schema}.t TO cov_reader;
GRANT INSERT ON ${schema}.t TO cov_reader WITH GRANT OPTION;
GRANT USAGE ON SEQUENCE ${schema}.s TO cov_reader;
REVOKE EXECUTE ON FUNCTION ${schema}.f() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ${schema}.f() TO cov_reader;
INSERT INTO ${schema}.t SELECT g, 'p' || g, 's' || g FROM generate_series(1, 10) g;

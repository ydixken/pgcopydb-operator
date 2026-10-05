-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE FUNCTION ${schema}.add(a int, b int DEFAULT 1) RETURNS int
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS 'SELECT a + b';
CREATE FUNCTION ${schema}.sql_body(x text) RETURNS text
  LANGUAGE sql STABLE
  BEGIN ATOMIC SELECT upper(x) || '!'; END;
CREATE FUNCTION ${schema}.counter(n int, OUT total bigint, OUT label text)
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $fn$
BEGIN
  total := 0;
  FOR i IN 1..n LOOP total := total + i; END LOOP;
  label := format('sum to %s', n);
END
$fn$;
CREATE PROCEDURE ${schema}.noop(INOUT v int) LANGUAGE plpgsql AS $p$ BEGIN v := v + 1; END $p$;
CREATE TABLE ${schema}.t (id int PRIMARY KEY, v int DEFAULT ${schema}.add(40, 2));
INSERT INTO ${schema}.t (id) SELECT g FROM generate_series(1, 10) g;

-- coverage: group=follow  expect=identical  schemas=1  min_pg=18
-- @setup
-- PostgreSQL 18 makes VIRTUAL the default kind of generated column.
CREATE TABLE ${schema}.t (
  id int PRIMARY KEY,
  first text, last text,
  full_name text GENERATED ALWAYS AS (first || ' ' || last) VIRTUAL
);
INSERT INTO ${schema}.t (id, first, last) SELECT g, 'f' || g, 'l' || g FROM generate_series(1, 20) g;
-- @follow
INSERT INTO ${schema}.t (id, first, last) VALUES (21, 'new', 'row');
UPDATE ${schema}.t SET last = upper(last) WHERE id % 3 = 0;

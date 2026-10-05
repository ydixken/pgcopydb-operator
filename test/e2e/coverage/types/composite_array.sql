-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TYPE ${schema}.point3 AS (x int, y int, label text);
CREATE TYPE ${schema}.shape AS (name text, corners ${schema}.point3[]);
CREATE TABLE ${schema}.t (id int PRIMARY KEY, pts ${schema}.point3[], s ${schema}.shape, one ${schema}.point3);
INSERT INTO ${schema}.t VALUES
  (1, '{}', NULL, NULL),
  (2, ARRAY[ROW(1, 2, 'a,b')::${schema}.point3, ROW(NULL, 0, '"q"')::${schema}.point3, NULL], ROW('tri', ARRAY[ROW(0, 0, NULL)::${schema}.point3])::${schema}.shape, ROW(NULL, NULL, NULL));
INSERT INTO ${schema}.t SELECT g, ARRAY[ROW(g, -g, 'p' || g)::${schema}.point3, ROW(g * 2, g, NULL)::${schema}.point3], ROW('s' || g, ARRAY[ROW(g, g, 'c')::${schema}.point3])::${schema}.shape, ROW(g, g, 'one')::${schema}.point3 FROM generate_series(3, 30) g;

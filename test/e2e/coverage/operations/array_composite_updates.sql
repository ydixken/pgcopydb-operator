-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TYPE ${schema}.addr AS (street text, zip int);
CREATE TABLE ${schema}.t (id int PRIMARY KEY, tags text[], grid int[][], home ${schema}.addr, past ${schema}.addr[]);
INSERT INTO ${schema}.t SELECT g, ARRAY['a' || g], ARRAY[[g, g], [0, 1]], ROW('street ' || g, 10000 + g)::${schema}.addr, ARRAY[ROW('old', g)::${schema}.addr] FROM generate_series(1, 20) g;
-- @follow
UPDATE ${schema}.t SET tags = tags || ARRAY['b', NULL, 'with "quote"'] WHERE id <= 5;
UPDATE ${schema}.t SET grid[1][2] = 42 WHERE id = 6;
UPDATE ${schema}.t SET home.zip = 99999 WHERE id = 7;
UPDATE ${schema}.t SET home = NULL, past = past || ROW('moved, away', NULL)::${schema}.addr WHERE id = 8;
INSERT INTO ${schema}.t VALUES (21, '{}', '{}', ROW(NULL, NULL), '{}');

-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE DOMAIN ${schema}.tags AS text[] NOT NULL DEFAULT '{}' CHECK (cardinality(VALUE) <= 5);
CREATE DOMAIN ${schema}.small_ints AS int[] CHECK (VALUE IS NULL OR 0 <= ALL (VALUE));
CREATE TABLE ${schema}.t (id int PRIMARY KEY, tags ${schema}.tags, nums ${schema}.small_ints);
INSERT INTO ${schema}.t (id) VALUES (1);
INSERT INTO ${schema}.t VALUES (2, '{"with space","quote\"d",NULL,""}', '{0}'), (3, '{a,b,c,d,e}', NULL);
INSERT INTO ${schema}.t SELECT g, ARRAY['tag' || g, 'x'], ARRAY[g, g * 2] FROM generate_series(4, 30) g;

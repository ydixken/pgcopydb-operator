-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, body text, doc tsvector, q tsquery);
INSERT INTO ${schema}.t VALUES (1, 'weighted', 'a:1A fat:2B,4C cat:5D', 'fat & (rat | cat)'), (2, 'empty', '', NULL);
INSERT INTO ${schema}.t SELECT g, 'the quick brown fox ' || g, to_tsvector('english', 'the quick brown foxes jumped ' || g), to_tsquery('english', 'fox & ' || g) FROM generate_series(3, 30) g;
CREATE INDEX ON ${schema}.t USING gin (doc);

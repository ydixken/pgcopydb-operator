-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, counter int, big text, doc jsonb);
-- Both big and doc are toasted; pgoutput sends an unchanged toasted value as a placeholder.
INSERT INTO ${schema}.t SELECT g, 0, (SELECT string_agg(md5(g::text || i::text), '') FROM generate_series(1, 300) i), (SELECT jsonb_agg(md5(i::text || g::text)) FROM generate_series(1, 300) i) FROM generate_series(1, 10) g;
-- @follow
UPDATE ${schema}.t SET counter = counter + 1;
UPDATE ${schema}.t SET counter = counter + 1, doc = '{"replaced": true}' WHERE id = 2;
UPDATE ${schema}.t SET big = 'short now' WHERE id = 3;
UPDATE ${schema}.t SET id = id + 100 WHERE id = 4;

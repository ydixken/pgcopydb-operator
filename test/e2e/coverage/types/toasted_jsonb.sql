-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, doc jsonb, small jsonb);
-- md5 text compresses poorly, so each doc stays well past the toast threshold.
INSERT INTO ${schema}.t SELECT g, (SELECT jsonb_build_object('id', g, 'items', jsonb_agg(md5(g::text || ':' || i::text))) FROM generate_series(1, 600) i), jsonb_build_object('n', g, 'f', g / 3.0, 'nested', jsonb_build_object('nul', NULL, 'arr', jsonb_build_array(1, 'two', true))) FROM generate_series(1, 12) g;
INSERT INTO ${schema}.t VALUES (13, 'null', '{}'), (14, NULL, '[]'), (15, '"\u00e4 unicode"', '{"big": 1e400}');

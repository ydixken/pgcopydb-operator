-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, u uuid NOT NULL, maybe uuid);
INSERT INTO ${schema}.t
SELECT g, md5(g::text)::uuid, CASE WHEN g % 3 = 0 THEN NULL ELSE md5('x' || g)::uuid END
FROM generate_series(1, 30) g;
INSERT INTO ${schema}.t VALUES (31, '00000000-0000-0000-0000-000000000000', 'ffffffff-ffff-ffff-ffff-ffffffffffff');

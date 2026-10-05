-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, flags bit(12), mask varbit(40), anybits varbit);
INSERT INTO ${schema}.t VALUES (1, B'000000000000', B'', B''), (2, B'111111111111', B'1', NULL);
INSERT INTO ${schema}.t SELECT g, g::bit(12), (g * 7919)::bit(32)::varbit, repeat('10', g)::varbit FROM generate_series(3, 30) g;

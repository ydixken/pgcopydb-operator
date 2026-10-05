-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, amount money);
INSERT INTO ${schema}.t VALUES (1, '-92233720368547758.08'), (2, '92233720368547758.07'), (3, '0'), (4, NULL), (5, '12.34');
INSERT INTO ${schema}.t SELECT g, (g * 1.11)::numeric::money FROM generate_series(6, 30) g;

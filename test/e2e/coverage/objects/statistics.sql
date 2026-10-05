-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, city text, zip text, a int, b int);
INSERT INTO ${schema}.t SELECT g, 'city' || (g % 5), 'zip' || (g % 5), g % 7, (g % 7) * 2 FROM generate_series(1, 40) g;
CREATE STATISTICS ${schema}.city_zip (ndistinct, dependencies) ON city, zip FROM ${schema}.t;
CREATE STATISTICS ${schema}.ab_mcv (mcv) ON a, b FROM ${schema}.t;
CREATE STATISTICS ${schema}.expr_stats ON (a + b), (lower(city)) FROM ${schema}.t;
ALTER STATISTICS ${schema}.ab_mcv SET STATISTICS 500;

-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.log (at int, msg text);
-- Exact duplicates: a comparator keyed on anything but the whole row cannot tell them apart.
INSERT INTO ${schema}.log SELECT g % 10, 'msg ' || (g % 10) FROM generate_series(1, 30) g;
INSERT INTO ${schema}.log VALUES (NULL, NULL), (NULL, NULL);
CREATE TABLE ${schema}.uniq_only (code text UNIQUE, n int);
INSERT INTO ${schema}.uniq_only SELECT 'c' || g, g FROM generate_series(1, 10) g;
INSERT INTO ${schema}.uniq_only VALUES (NULL, 0), (NULL, 0);

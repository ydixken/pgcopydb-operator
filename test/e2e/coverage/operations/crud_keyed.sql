-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, name text, qty int);
CREATE TABLE ${schema}.pair (a int, b text, val text, PRIMARY KEY (a, b));
INSERT INTO ${schema}.t SELECT g, 'n' || g, g FROM generate_series(1, 30) g;
INSERT INTO ${schema}.pair SELECT g % 5, 'k' || g, 'v' || g FROM generate_series(1, 20) g;
-- @follow
INSERT INTO ${schema}.t SELECT g, 'new' || g, g FROM generate_series(31, 40) g;
UPDATE ${schema}.t SET qty = qty * 10 WHERE id % 3 = 0;
-- Key updates: the target must locate the row by its old key.
UPDATE ${schema}.t SET id = id + 1000 WHERE id BETWEEN 1 AND 5;
UPDATE ${schema}.pair SET b = b || '-moved' WHERE a = 1;
DELETE FROM ${schema}.t WHERE id BETWEEN 20 AND 25;
DELETE FROM ${schema}.pair WHERE a = 2;
-- Several changes to one row in one transaction.
BEGIN;
UPDATE ${schema}.t SET name = 'first' WHERE id = 30;
UPDATE ${schema}.t SET name = 'second', id = 3000 WHERE id = 30;
COMMIT;

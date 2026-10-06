-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
-- The identity is a unique index, not a key; the target must keep it, not fall back to DEFAULT.
CREATE TABLE ${schema}.t (id int, code text NOT NULL, val text);
CREATE UNIQUE INDEX t_code_key ON ${schema}.t (code);
ALTER TABLE ${schema}.t REPLICA IDENTITY USING INDEX t_code_key;
INSERT INTO ${schema}.t SELECT g, 'c' || g, 'v' || g FROM generate_series(1, 30) g;
-- @follow
INSERT INTO ${schema}.t VALUES (31, 'c31', 'new'), (NULL, 'c-null-id', 'id is not part of the identity');
UPDATE ${schema}.t SET val = 'upd' WHERE id % 4 = 0;
UPDATE ${schema}.t SET code = code || '-renamed' WHERE id = 7;
DELETE FROM ${schema}.t WHERE id = 9;

-- coverage: group=own  expect=apply_fails_on_ddl  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, name text);
INSERT INTO ${schema}.t SELECT g, 'n' || g FROM generate_series(1, 10) g;
-- @follow
-- DDL is not replicated: the target never gets the column the next insert names.
ALTER TABLE ${schema}.t ADD COLUMN extra int DEFAULT 7;
INSERT INTO ${schema}.t (id, name, extra) VALUES (11, 'after ddl', 42);
UPDATE ${schema}.t SET extra = 1 WHERE id = 1;

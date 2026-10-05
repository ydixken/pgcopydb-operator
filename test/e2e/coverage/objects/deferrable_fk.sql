-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.parent (id int PRIMARY KEY, name text);
CREATE TABLE ${schema}.child (
  id int PRIMARY KEY,
  parent_id int REFERENCES ${schema}.parent (id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  other_id int CONSTRAINT child_other_fk REFERENCES ${schema}.parent (id) DEFERRABLE INITIALLY IMMEDIATE
);
-- A self reference: the table cannot be loaded row by row in key order without deferral.
CREATE TABLE ${schema}.node (id int PRIMARY KEY, next_id int REFERENCES ${schema}.node (id) DEFERRABLE);
INSERT INTO ${schema}.parent SELECT g, 'p' || g FROM generate_series(1, 10) g;
INSERT INTO ${schema}.child SELECT g, 1 + g % 10, 1 + (g * 3) % 10 FROM generate_series(1, 30) g;
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO ${schema}.node SELECT g, CASE WHEN g < 20 THEN g + 1 ELSE 1 END FROM generate_series(1, 20) g;
COMMIT;
ALTER TABLE ${schema}.child ADD CONSTRAINT child_notvalid_fk FOREIGN KEY (other_id) REFERENCES ${schema}.parent (id) NOT VALID;

-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, name text, touched int NOT NULL DEFAULT 0);
CREATE TABLE ${schema}.audit (id int, op text);
CREATE FUNCTION ${schema}.touch() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  NEW.touched := NEW.touched + 1;
  INSERT INTO ${schema}.audit VALUES (NEW.id, TG_OP);
  RETURN NEW;
END
$fn$;
CREATE TRIGGER t_touch BEFORE INSERT OR UPDATE OF name ON ${schema}.t
  FOR EACH ROW WHEN (NEW.id > 0) EXECUTE FUNCTION ${schema}.touch();
CREATE TRIGGER t_disabled AFTER DELETE ON ${schema}.t FOR EACH ROW EXECUTE FUNCTION ${schema}.touch();
ALTER TABLE ${schema}.t DISABLE TRIGGER t_disabled;
-- A copy that fired the trigger again would change touched and the audit row count.
INSERT INTO ${schema}.t (id, name) SELECT g, 'n' || g FROM generate_series(1, 20) g;
UPDATE ${schema}.t SET name = name || '!' WHERE id <= 5;

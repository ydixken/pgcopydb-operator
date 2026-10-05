-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE SEQUENCE ${schema}.wrap AS smallint MINVALUE -3 MAXVALUE 5 START WITH 4 INCREMENT BY 2 CYCLE CACHE 1;
CREATE SEQUENCE ${schema}.down INCREMENT BY -1 MINVALUE -1000 MAXVALUE -1 START -1 NO CYCLE;
CREATE SEQUENCE ${schema}.unused;
-- 4, -3 (wrapped), -1, 1, 3, 5, -3 ...: the stored value is mid-cycle.
SELECT nextval('${schema}.wrap') FROM generate_series(1, 7);
SELECT nextval('${schema}.down') FROM generate_series(1, 3);
CREATE TABLE ${schema}.t (id serial PRIMARY KEY, w smallint DEFAULT nextval('${schema}.wrap'));
INSERT INTO ${schema}.t DEFAULT VALUES;
INSERT INTO ${schema}.t DEFAULT VALUES;

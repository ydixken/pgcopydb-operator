-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
-- No key, so the target finds a row by every column, and identical rows look alike.
CREATE TABLE ${schema}.log (at int, msg text, payload jsonb);
ALTER TABLE ${schema}.log REPLICA IDENTITY FULL;
INSERT INTO ${schema}.log SELECT g % 10, 'msg ' || g, jsonb_build_object('g', g) FROM generate_series(1, 30) g;
INSERT INTO ${schema}.log VALUES (99, 'dup', '{"d": 1}'), (99, 'dup', '{"d": 1}'), (98, NULL, NULL);
INSERT INTO ${schema}.log VALUES (97, 'dup', '{"d": 2}'), (97, 'dup', '{"d": 2}');
INSERT INTO ${schema}.log VALUES (96, 'dup', '{"d": 3}'), (96, 'dup', '{"d": 3}'), (96, 'dup', '{"d": 3}');
-- @follow
INSERT INTO ${schema}.log VALUES (100, 'live', '{"live": true}');
UPDATE ${schema}.log SET msg = msg || ' updated' WHERE at = 3;
UPDATE ${schema}.log SET msg = 'was null' WHERE msg IS NULL;
DELETE FROM ${schema}.log WHERE at = 4;
-- One of two identical rows: the target must change exactly one.
DELETE FROM ${schema}.log WHERE ctid = (SELECT min(ctid) FROM ${schema}.log WHERE at = 99);
UPDATE ${schema}.log SET msg = 'one of two' WHERE ctid = (SELECT min(ctid) FROM ${schema}.log WHERE at = 97);
-- Two of three in one statement: consecutive deletes must not merge into one that takes all three.
DELETE FROM ${schema}.log WHERE ctid IN (SELECT ctid FROM ${schema}.log WHERE at = 96 LIMIT 2);

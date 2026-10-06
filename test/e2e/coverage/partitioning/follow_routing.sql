-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.ledger (
  id int NOT NULL,
  bucket int NOT NULL,
  amount numeric(12,2),
  PRIMARY KEY (bucket, id)
) PARTITION BY RANGE (bucket);
CREATE TABLE ${schema}.ledger_a PARTITION OF ${schema}.ledger FOR VALUES FROM (0) TO (10);
CREATE TABLE ${schema}.ledger_b PARTITION OF ${schema}.ledger FOR VALUES FROM (10) TO (20);
CREATE TABLE ${schema}.ledger_rest PARTITION OF ${schema}.ledger DEFAULT;
INSERT INTO ${schema}.ledger SELECT g, g % 20, g * 2.5 FROM generate_series(1, 30) g;
-- @follow
INSERT INTO ${schema}.ledger VALUES (101, 3, 1.00), (102, 15, 2.00), (103, 42, 3.00);
UPDATE ${schema}.ledger SET amount = amount + 1 WHERE bucket = 5;
-- Moves a row from ledger_a to ledger_b: a DELETE on one leaf and an INSERT on another.
UPDATE ${schema}.ledger SET bucket = 12 WHERE id = 1 AND bucket = 1;
-- Moves a row into the DEFAULT leaf.
UPDATE ${schema}.ledger SET bucket = 99 WHERE id = 2 AND bucket = 2;
DELETE FROM ${schema}.ledger WHERE id = 3 AND bucket = 3;

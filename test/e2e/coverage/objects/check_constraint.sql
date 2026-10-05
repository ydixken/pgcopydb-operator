-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (
  id int PRIMARY KEY,
  qty int CHECK (qty >= 0),
  lo int, hi int,
  CONSTRAINT lo_le_hi CHECK (lo <= hi),
  CONSTRAINT never_validated CHECK (qty < 1000) NOT VALID
);
INSERT INTO ${schema}.t SELECT g, g, g, g + 1 FROM generate_series(1, 20) g;
ALTER TABLE ${schema}.t ADD CONSTRAINT hi_small CHECK (hi < 100) NOT VALID;
ALTER TABLE ${schema}.t ADD CONSTRAINT qty_noinherit CHECK (qty IS NOT NULL) NO INHERIT;

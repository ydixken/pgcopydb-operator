-- coverage: group=clone  expect=identical  schemas=2  min_pg=14
-- @setup
CREATE TABLE ${schema}.metrics (
  id int NOT NULL,
  bucket int NOT NULL,
  value double precision,
  PRIMARY KEY (bucket, id)
) PARTITION BY RANGE (bucket);
CREATE TABLE ${schema}.metrics_low PARTITION OF ${schema}.metrics FOR VALUES FROM (0) TO (10);
-- The leaf lives in the second schema; both schemas are in the filter.
CREATE TABLE ${schema2}.metrics_high PARTITION OF ${schema}.metrics FOR VALUES FROM (10) TO (20);
INSERT INTO ${schema}.metrics SELECT g, g % 20, g / 4.0 FROM generate_series(1, 40) g;

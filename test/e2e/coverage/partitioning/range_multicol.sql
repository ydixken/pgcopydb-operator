-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.readings (
  region int NOT NULL,
  day date NOT NULL,
  id int NOT NULL,
  reading numeric(10,2),
  PRIMARY KEY (region, day, id)
) PARTITION BY RANGE (region, day);
CREATE TABLE ${schema}.readings_r1_h1 PARTITION OF ${schema}.readings
  FOR VALUES FROM (1, '2026-01-01') TO (1, '2026-07-01');
CREATE TABLE ${schema}.readings_r1_h2 PARTITION OF ${schema}.readings
  FOR VALUES FROM (1, '2026-07-01') TO (2, MINVALUE);
CREATE TABLE ${schema}.readings_r2 PARTITION OF ${schema}.readings
  FOR VALUES FROM (2, MINVALUE) TO (3, MINVALUE);
INSERT INTO ${schema}.readings
SELECT 1 + (g % 2), date '2026-01-01' + (g * 7), g, g * 1.25
FROM generate_series(1, 40) g;

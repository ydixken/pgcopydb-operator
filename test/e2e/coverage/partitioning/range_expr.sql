-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.orders (
  id int NOT NULL,
  placed_at date NOT NULL,
  note text
) PARTITION BY RANGE ((extract(year FROM placed_at)));
CREATE TABLE ${schema}.orders_2025 PARTITION OF ${schema}.orders FOR VALUES FROM (2025) TO (2026);
CREATE TABLE ${schema}.orders_2026 PARTITION OF ${schema}.orders FOR VALUES FROM (2026) TO (2027);
CREATE INDEX ON ${schema}.orders (id);
INSERT INTO ${schema}.orders
SELECT g, date '2025-06-01' + (g * 11), 'order ' || g FROM generate_series(1, 40) g;

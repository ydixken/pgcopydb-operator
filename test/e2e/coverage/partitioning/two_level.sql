-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.events (
  id int NOT NULL,
  happened date NOT NULL,
  kind text NOT NULL,
  payload text
) PARTITION BY RANGE (happened);
CREATE TABLE ${schema}.events_2026 PARTITION OF ${schema}.events
  FOR VALUES FROM ('2026-01-01') TO ('2027-01-01') PARTITION BY LIST (kind);
CREATE TABLE ${schema}.events_2026_click PARTITION OF ${schema}.events_2026 FOR VALUES IN ('click');
CREATE TABLE ${schema}.events_2026_view PARTITION OF ${schema}.events_2026 FOR VALUES IN ('view');
CREATE TABLE ${schema}.events_2026_other PARTITION OF ${schema}.events_2026 DEFAULT;
CREATE TABLE ${schema}.events_default PARTITION OF ${schema}.events DEFAULT;
INSERT INTO ${schema}.events
SELECT g, date '2025-12-01' + (g * 13), (ARRAY['click', 'view', 'buy'])[1 + g % 3], 'event ' || g
FROM generate_series(1, 40) g;

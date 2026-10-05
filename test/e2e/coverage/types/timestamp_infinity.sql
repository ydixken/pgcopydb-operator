-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, ts timestamp, tstz timestamptz, d date, t3 timestamp(3));
INSERT INTO ${schema}.t VALUES
  (1, 'infinity', 'infinity', 'infinity', 'infinity'),
  (2, '-infinity', '-infinity', '-infinity', '-infinity'),
  (3, '4713-01-01 00:00:00 BC', '4713-01-01 00:00:00+00 BC', '4713-01-01 BC', '0001-01-01 00:00:00'),
  (4, '294276-12-31 23:59:59.999999', '294276-12-31 23:59:59.999999+00', '5874897-12-31', '2026-10-05 12:34:56.789'),
  (5, NULL, NULL, NULL, NULL);
INSERT INTO ${schema}.t SELECT g, timestamp '2026-01-01' + g * interval '1 day 1.5 seconds', timestamptz '2026-03-29 00:30:00+00' + g * interval '17 minutes', date '2026-01-01' + g, timestamp '2026-01-01' + g * interval '1.234 seconds' FROM generate_series(6, 30) g;

-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, i interval, i_sec interval second(2), i_ym interval year to month);
INSERT INTO ${schema}.t VALUES
  (1, '1 year 2 mons 3 days 04:05:06.789', '12.345 seconds', '3 years 4 months'),
  (2, '-1 day +02:00', '-0.5 seconds', '-2 months'),
  (3, '1000000 hours', '59.999 seconds', '178000000 years'),
  (4, '0', '0', '0'),
  (5, NULL, NULL, NULL),
  (6, '1 mon -30 days', '1 day', '1 year');
INSERT INTO ${schema}.t SELECT g, make_interval(days => g, secs => g / 7.0), make_interval(secs => g / 3.0), make_interval(months => g) FROM generate_series(7, 30) g;

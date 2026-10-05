-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
-- numeric with a typmod rejects infinity (PostgreSQL), so n_scaled carries its extremes instead.
CREATE TABLE ${schema}.t (id int PRIMARY KEY, n numeric, n_scaled numeric(20,4), f8 double precision, f4 real);
INSERT INTO ${schema}.t VALUES
  (1, 'NaN', 'NaN', 'NaN', 'NaN'),
  (2, 'Infinity', '9999999999999999.9999', 'Infinity', 'Infinity'),
  (3, '-Infinity', '-9999999999999999.9999', '-Infinity', '-Infinity'),
  (4, '-0', '0.0000', '-0', '-0'),
  (5, '123456789012345678901234567890.123456789', '1234567890123456.1234', 1e308, 3.4e38),
  (6, 0.1, 0.1, 0.1, 0.1),
  (7, NULL, NULL, NULL, NULL);
INSERT INTO ${schema}.t SELECT g, g / 7.0, g / 3.0, g / 7.0, g / 3.0 FROM generate_series(8, 30) g;

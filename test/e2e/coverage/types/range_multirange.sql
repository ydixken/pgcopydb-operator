-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TYPE ${schema}.floatrange AS RANGE (subtype = float8, subtype_diff = float8mi);
CREATE TABLE ${schema}.t (
  id int PRIMARY KEY,
  ir int4range, nr numrange, dr daterange, tr tstzrange,
  fr ${schema}.floatrange,
  imr int4multirange, dmr datemultirange, fmr ${schema}.floatmultirange
);
INSERT INTO ${schema}.t VALUES
  (1, 'empty', '(,)', '[2026-01-01,infinity)', '[2026-01-01 00:00+00,2026-01-02 00:00+00)', '[1.5,2.5]', '{}', '{[2026-01-01,2026-02-01), [2026-03-01,2026-04-01)}', '{[1,2], (3,4)}'),
  (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO ${schema}.t SELECT g, int4range(g, g * 2), numrange(g / 3.0, g), daterange(date '2026-01-01' + g, date '2026-01-01' + g * 2), tstzrange(timestamptz '2026-01-01 00:00+00' + g * interval '1 hour', NULL), ${schema}.floatrange(g, g + 0.5), int4multirange(int4range(1, g), int4range(g + 2, g + 5)), NULL, ${schema}.floatmultirange(${schema}.floatrange(0, g)) FROM generate_series(3, 30) g;

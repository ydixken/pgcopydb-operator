-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.bookings (
  id int PRIMARY KEY,
  during tsrange NOT NULL,
  EXCLUDE USING gist (during WITH &&)
);
INSERT INTO ${schema}.bookings SELECT g, tsrange(timestamp '2026-01-01' + g * interval '1 hour', timestamp '2026-01-01' + (g + 1) * interval '1 hour') FROM generate_series(1, 20) g;

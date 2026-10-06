-- coverage: group=own  expect=large_objects_not_replicated  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.cov_large_objects (name text PRIMARY KEY, lo oid NOT NULL);
INSERT INTO ${schema}.cov_large_objects
  SELECT 'blob' || g, lo_from_bytea(0, convert_to(repeat('x', g * 100), 'UTF8')) FROM generate_series(1, 3) g;
-- @follow
-- Logical decoding does not publish pg_largeobject: only the rows of cov_large_objects replicate.
INSERT INTO ${schema}.cov_large_objects VALUES ('live', lo_from_bytea(0, convert_to('created during follow', 'UTF8')));
SELECT lo_put(lo, 0, convert_to('PATCHED', 'UTF8')) FROM ${schema}.cov_large_objects WHERE name = 'blob1';
-- The row stays, so the fingerprint sees the object gone on the source and kept on the target.
SELECT lo_unlink(lo) FROM ${schema}.cov_large_objects WHERE name = 'blob2';

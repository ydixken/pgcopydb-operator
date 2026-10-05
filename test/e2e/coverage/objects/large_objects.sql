-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
-- The fingerprint hashes lo_get and the owner of every OID listed in cov_large_objects.
CREATE TABLE ${schema}.cov_large_objects (name text PRIMARY KEY, lo oid NOT NULL);
INSERT INTO ${schema}.cov_large_objects SELECT 'blob' || g, lo_from_bytea(0, convert_to(repeat(md5(g::text), g * 50), 'UTF8')) FROM generate_series(1, 5) g;
INSERT INTO ${schema}.cov_large_objects VALUES ('empty', lo_from_bytea(0, ''::bytea));
INSERT INTO ${schema}.cov_large_objects SELECT 'binary', lo_from_bytea(0, decode(string_agg(lpad(to_hex(b), 2, '0'), ''), 'hex')) FROM generate_series(0, 255) b;

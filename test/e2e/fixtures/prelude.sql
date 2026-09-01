-- Shared session setup for every stage that loads data. Included with \ir,
-- which resolves against the including file rather than the working directory.
--
-- The seed marker is NOT checked here. \quit inside an included file ends only
-- that file, not the psql session, so a guard placed here would let the stage
-- run on regardless. run.sh checks the marker once, after the schema exists,
-- and skips every stage from there.

-- psql does not interpolate variables inside quoted strings, so the scale
-- travels into e2e_scaled() (used inside the batched INSERTs) as a session
-- setting. Each stage is its own psql process and needs its own copy.
SELECT set_config('e2e.scale', :'scale', false);
-- The extra-tables stage reads its own two settings the same way. Defaulted
-- here so every stage can read them whether or not the caller passed them.
SELECT set_config('e2e.extra_tables', :'extra_tables', false);
SELECT set_config('e2e.extra_mb', :'extra_mb', false);

-- One Time: line per table, which is what makes the Job log a per-phase
-- profile of the seed instead of a list of table names (issue #146).
\timing on

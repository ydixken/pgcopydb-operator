-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TYPE ${schema}.mood AS ENUM ('sad', 'ok');
-- Each statement commits on its own: a value added by ALTER TYPE cannot be used in the same transaction.
ALTER TYPE ${schema}.mood ADD VALUE 'happy' AFTER 'ok';
ALTER TYPE ${schema}.mood ADD VALUE 'meh' BEFORE 'ok';
ALTER TYPE ${schema}.mood ADD VALUE 'furious' BEFORE 'sad';
CREATE TABLE ${schema}.t (id int PRIMARY KEY, m ${schema}.mood, ms ${schema}.mood[]);
INSERT INTO ${schema}.t SELECT g, (enum_range(NULL::${schema}.mood))[1 + g % 5], enum_range(NULL::${schema}.mood, (enum_range(NULL::${schema}.mood))[1 + g % 5]) FROM generate_series(1, 30) g;
CREATE INDEX ON ${schema}.t (m);

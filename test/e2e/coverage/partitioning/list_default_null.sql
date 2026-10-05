-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.tickets (
  id int NOT NULL,
  status text,
  title text
) PARTITION BY LIST (status);
CREATE TABLE ${schema}.tickets_open PARTITION OF ${schema}.tickets FOR VALUES IN ('open', 'reopened');
-- NULL is a list member, not the DEFAULT partition's business.
CREATE TABLE ${schema}.tickets_unset PARTITION OF ${schema}.tickets FOR VALUES IN ('new', NULL);
CREATE TABLE ${schema}.tickets_other PARTITION OF ${schema}.tickets DEFAULT;
INSERT INTO ${schema}.tickets
SELECT g, (ARRAY['open', 'reopened', 'new', NULL, 'closed', 'archived'])[1 + g % 6], 'ticket ' || g
FROM generate_series(1, 36) g;

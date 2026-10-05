-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, c_name text COLLATE "C", posix_name varchar(40) COLLATE "POSIX", plain text);
CREATE INDEX ON ${schema}.t (c_name);
CREATE INDEX ON ${schema}.t (posix_name COLLATE "C" DESC);
INSERT INTO ${schema}.t SELECT g, (ARRAY['b', 'B', 'a', 'A', '_z', 'Zeta', 'zeta', 'äpfel'])[1 + g % 8] || g, 'Name ' || g, 'plain' || g FROM generate_series(1, 32) g;

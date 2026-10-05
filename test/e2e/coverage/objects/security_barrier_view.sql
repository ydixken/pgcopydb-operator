-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.accounts (id int PRIMARY KEY, owner text, balance numeric);
INSERT INTO ${schema}.accounts SELECT g, 'user' || (g % 4), g * 10 FROM generate_series(1, 20) g;
CREATE VIEW ${schema}.mine WITH (security_barrier, check_option = local) AS
  SELECT id, balance FROM ${schema}.accounts WHERE owner = 'user1';

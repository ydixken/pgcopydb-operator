-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, host inet, net cidr, mac macaddr);
INSERT INTO ${schema}.t VALUES
  (1, '192.168.1.5/24', '192.168.1.0/24', '08:00:2b:01:02:03'),
  (2, '::1', '2001:db8::/32', 'ff:ff:ff:ff:ff:ff'),
  (3, '10.0.0.1', '10.0.0.0/8', '00:00:00:00:00:00'),
  (4, NULL, NULL, NULL);
INSERT INTO ${schema}.t SELECT g, ('10.1.' || g || '.7/16')::inet, ('172.16.' || g || '.0/24')::cidr, ('08:00:2b:00:00:' || lpad(to_hex(g), 2, '0'))::macaddr FROM generate_series(5, 30) g;

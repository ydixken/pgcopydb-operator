-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (id int PRIMARY KEY, doc xml, frag xml);
INSERT INTO ${schema}.t VALUES
  (1, '<?xml version="1.0"?><root a="1"><child>x &amp; y</child></root>', 'text only'),
  (2, '<root xmlns="urn:example"><n>2</n></root>', '<a/><b/>'),
  (3, NULL, '<![CDATA[ <raw> ]]>');
INSERT INTO ${schema}.t SELECT g, xmlelement(name item, xmlattributes(g AS id), 'value ' || g), xmlconcat(xmlcomment('c' || g), xmlelement(name x)) FROM generate_series(4, 30) g;

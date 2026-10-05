-- Fingerprint of the coverage case schemas: one JSON array of rows
-- {schema, aspect, key, value} without OIDs, so two servers compare as text.
-- readFingerprint replaces ${schemas} with a text[] of the schema names.
-- Run it as a superuser: FORCE ROW LEVEL SECURITY would hide rows from anyone else.
SET TimeZone = 'UTC';
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET extra_float_digits = 3;
SET bytea_output = 'hex';
SET lc_monetary = 'C';
SET search_path = pg_catalog;

CREATE FUNCTION pg_temp.cov_rows(rel regclass, only_rel boolean) RETURNS text
LANGUAGE plpgsql AS $fp$
DECLARE result text;
BEGIN
  EXECUTE format('SELECT count(*) || '' '' || md5(coalesce(string_agg(h, '''' ORDER BY h COLLATE "C"), ''''))'
    ' FROM (SELECT md5(t::text) AS h FROM %s %s t) s', CASE WHEN only_rel THEN 'ONLY' ELSE '' END, rel)
  INTO result;
  RETURN result;
END
$fp$;

CREATE FUNCTION pg_temp.cov_seq(rel regclass) RETURNS text
LANGUAGE plpgsql AS $fp$
DECLARE result text;
BEGIN
  EXECUTE format('SELECT last_value || '' '' || is_called FROM %s', rel) INTO result;
  RETURN result;
END
$fp$;

-- A case that creates large objects lists them in <schema>.cov_large_objects(name, lo).
CREATE FUNCTION pg_temp.cov_lo(nsp name) RETURNS TABLE (key text, value text)
LANGUAGE plpgsql AS $fp$
BEGIN
  IF to_regclass(format('%I.cov_large_objects', nsp)) IS NULL THEN
    RETURN;
  END IF;
  RETURN QUERY EXECUTE format('SELECT r.name::text, CASE WHEN m.oid IS NULL THEN ''missing'''
    ' ELSE md5(lo_get(r.lo)) || '' owner='' || pg_get_userbyid(m.lomowner)'
    ' || '' acl='' || pg_temp.cov_acl(coalesce(m.lomacl, acldefault(''L'', m.lomowner)), m.lomowner) END'
    ' FROM %I.cov_large_objects r LEFT JOIN pg_largeobject_metadata m ON m.oid = r.lo', nsp);
END
$fp$;

-- Grants by name, sorted, PUBLIC as grantee 0. The owner's own entries are
-- left out: their default set grows between majors (MAINTAIN in 17).
CREATE FUNCTION pg_temp.cov_acl(acl aclitem[], owner oid) RETURNS text
LANGUAGE sql AS $fp$
  SELECT coalesce(string_agg(item, ',' ORDER BY item COLLATE "C"), '')
  FROM (SELECT pg_get_userbyid(a.grantor) || '>' ||
          CASE WHEN a.grantee = 0 THEN 'public' ELSE pg_get_userbyid(a.grantee) END || ':' ||
          a.privilege_type || CASE WHEN a.is_grantable THEN '*' ELSE '' END AS item
        FROM aclexplode(acl) a WHERE a.grantee <> owner) items
$fp$;

WITH nsp AS (
  SELECT oid, nspname, nspowner, nspacl FROM pg_namespace WHERE nspname = ANY (${schemas})
), rel AS (
  SELECT c.*, n.nspname FROM pg_class c JOIN nsp n ON n.oid = c.relnamespace
), typ AS (
  SELECT t.*, n.nspname FROM pg_type t JOIN nsp n ON n.oid = t.typnamespace
  WHERE t.typtype IN ('e', 'd', 'r')
     OR (t.typtype = 'c' AND (SELECT relkind FROM pg_class WHERE oid = t.typrelid) = 'c')
), fn AS (
  SELECT p.*, n.nspname FROM pg_proc p JOIN nsp n ON n.oid = p.pronamespace
), fingerprint (schema, aspect, key, value) AS (
  SELECT nspname, 'data', relname::text, pg_temp.cov_rows(oid, relkind = 'r')
  FROM rel WHERE relkind IN ('r', 'p')
  UNION ALL
  SELECT nspname, 'relation', relname::text, format(
    'kind=%s persistence=%s strategy=%s key=%s bound=%s parents=%s identity=%s rls=%s force=%s options=%s',
    relkind, relpersistence,
    (SELECT partstrat FROM pg_partitioned_table WHERE partrelid = rel.oid),
    CASE WHEN relkind = 'p' THEN pg_get_partkeydef(oid) END,
    pg_get_expr(relpartbound, oid),
    (SELECT string_agg(inhparent::regclass::text, ',' ORDER BY inhseqno) FROM pg_inherits WHERE inhrelid = rel.oid),
    relreplident, relrowsecurity, relforcerowsecurity, reloptions)
  FROM rel WHERE relkind NOT IN ('i', 'I')
  UNION ALL
  SELECT r.nspname, 'column', r.relname || '.' || a.attname, format(
    'position=%s type=%s collation=%s default=%s identity=%s generated=%s notnull=%s',
    row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum),
    format_type(a.atttypid, a.atttypmod),
    CASE WHEN a.attcollation <> 0 THEN a.attcollation::regcollation::text END,
    pg_get_expr(d.adbin, d.adrelid), a.attidentity, a.attgenerated, a.attnotnull)
  FROM rel r
  JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum > 0 AND NOT a.attisdropped
  LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
  WHERE r.relkind IN ('r', 'p', 'v', 'm', 'c', 'f')
  UNION ALL
  -- PostgreSQL 18 records NOT NULL as constraints too; the column aspect covers them on every major.
  SELECT n.nspname, 'constraint',
    coalesce((SELECT relname FROM pg_class WHERE oid = c.conrelid), (SELECT typname FROM pg_type WHERE oid = c.contypid))
      || '.' || c.conname,
    pg_get_constraintdef(c.oid)
  FROM pg_constraint c JOIN nsp n ON n.oid = c.connamespace
  WHERE c.contype <> 'n'
  UNION ALL
  SELECT r.nspname, 'index', ic.relname::text,
    pg_get_indexdef(x.indexrelid) || CASE WHEN x.indisreplident THEN ' (replica identity)' ELSE '' END
  FROM pg_index x JOIN rel r ON r.oid = x.indrelid JOIN pg_class ic ON ic.oid = x.indexrelid
  UNION ALL
  -- Only the definition on the parent: leaf clones are internal on some majors and not on others.
  SELECT r.nspname, 'trigger', r.relname || '.' || t.tgname, pg_get_triggerdef(t.oid) || ' enabled=' || t.tgenabled::text
  FROM pg_trigger t JOIN rel r ON r.oid = t.tgrelid
  WHERE NOT t.tgisinternal AND t.tgparentid = 0
  UNION ALL
  SELECT nspname, 'function', oid::regprocedure::text,
    CASE WHEN prokind IN ('f', 'p') THEN pg_get_functiondef(oid) ELSE 'kind=' || prokind::text END
  FROM fn
  UNION ALL
  SELECT nspname, 'view', relname::text, pg_get_viewdef(oid) FROM rel WHERE relkind IN ('v', 'm')
  UNION ALL
  SELECT r.nspname, 'policy', r.relname || '.' || p.polname, format('cmd=%s permissive=%s roles=%s using=%s check=%s',
    p.polcmd, p.polpermissive,
    (SELECT string_agg(name, ',' ORDER BY name COLLATE "C") FROM (
      SELECT CASE WHEN role = 0 THEN 'public' ELSE pg_get_userbyid(role) END AS name FROM unnest(p.polroles) role) roles),
    pg_get_expr(p.polqual, p.polrelid), pg_get_expr(p.polwithcheck, p.polrelid))
  FROM pg_policy p JOIN rel r ON r.oid = p.polrelid
  UNION ALL
  SELECT n.nspname, 'statistics', s.stxname::text, pg_get_statisticsobjdef(s.oid)
  FROM pg_statistic_ext s JOIN nsp n ON n.oid = s.stxnamespace
  UNION ALL
  SELECT nspname, 'type', typname::text, CASE typtype
    WHEN 'e' THEN 'enum ' || (SELECT string_agg(quote_literal(enumlabel), ',' ORDER BY enumsortorder)
                              FROM pg_enum WHERE enumtypid = typ.oid)
    WHEN 'd' THEN format('domain %s notnull=%s default=%s collation=%s', format_type(typbasetype, typtypmod),
                         typnotnull, typdefault, CASE WHEN typcollation <> 0 THEN typcollation::regcollation::text END)
    WHEN 'r' THEN (SELECT format('range %s collation=%s', format_type(rngsubtype, NULL),
                                CASE WHEN rngcollation <> 0 THEN rngcollation::regcollation::text END)
                   FROM pg_range WHERE rngtypid = typ.oid)
    ELSE 'composite'
  END
  FROM typ
  UNION ALL
  SELECT r.nspname, 'sequence', r.relname::text, format(
    'type=%s start=%s increment=%s min=%s max=%s cache=%s cycle=%s value=%s',
    s.seqtypid::regtype, s.seqstart, s.seqincrement, s.seqmin, s.seqmax, s.seqcache, s.seqcycle,
    pg_temp.cov_seq(r.oid))
  FROM pg_sequence s JOIN rel r ON r.oid = s.seqrelid
  UNION ALL
  -- pg_identify_object gives triggers, policies and rules no schema, so take their table's.
  SELECT s.nspname, 'comment', o.type || ' ' || o.identity, d.description
  FROM pg_description d, pg_identify_object(d.classoid, d.objoid, d.objsubid) o,
    LATERAL (SELECT coalesce(o.schema, CASE WHEN o.type = 'schema' THEN o.identity END,
      (SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = CASE d.classoid
        WHEN 'pg_trigger'::regclass THEN (SELECT tgrelid FROM pg_trigger WHERE oid = d.objoid)
        WHEN 'pg_policy'::regclass THEN (SELECT polrelid FROM pg_policy WHERE oid = d.objoid)
        WHEN 'pg_rewrite'::regclass THEN (SELECT ev_class FROM pg_rewrite WHERE oid = d.objoid) END)) AS nspname) s
  WHERE s.nspname = ANY (${schemas})
  UNION ALL
  SELECT nspname, 'owner', 'schema', pg_get_userbyid(nspowner) FROM nsp
  UNION ALL
  SELECT nspname, 'owner', 'relation ' || relname, pg_get_userbyid(relowner) FROM rel WHERE relkind NOT IN ('i', 'I')
  UNION ALL
  SELECT nspname, 'owner', 'function ' || oid::regprocedure, pg_get_userbyid(proowner) FROM fn
  UNION ALL
  SELECT nspname, 'owner', 'type ' || typname, pg_get_userbyid(typowner) FROM typ
  UNION ALL
  SELECT nspname, 'acl', 'schema', pg_temp.cov_acl(coalesce(nspacl, acldefault('n', nspowner)), nspowner) FROM nsp
  UNION ALL
  SELECT nspname, 'acl', 'relation ' || relname,
    pg_temp.cov_acl(coalesce(relacl, acldefault((CASE WHEN relkind = 'S' THEN 's' ELSE 'r' END)::"char", relowner)), relowner)
  FROM rel WHERE relkind NOT IN ('i', 'I', 'c')
  UNION ALL
  SELECT r.nspname, 'acl', 'column ' || r.relname || '.' || a.attname, pg_temp.cov_acl(a.attacl, r.relowner)
  FROM rel r JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum > 0 AND NOT a.attisdropped
  WHERE a.attacl IS NOT NULL
  UNION ALL
  SELECT nspname, 'acl', 'function ' || oid::regprocedure, pg_temp.cov_acl(coalesce(proacl, acldefault('f', proowner)), proowner)
  FROM fn
  UNION ALL
  SELECT nspname, 'acl', 'type ' || typname, pg_temp.cov_acl(coalesce(typacl, acldefault('T', typowner)), typowner) FROM typ
  UNION ALL
  SELECT n.nspname, 'largeobject', lo.key, lo.value FROM nsp n, pg_temp.cov_lo(n.nspname) lo
)
SELECT coalesce(json_agg(json_build_object('schema', schema, 'aspect', aspect, 'key', key, 'value', coalesce(value, ''))
  ORDER BY schema COLLATE "C", aspect COLLATE "C", key COLLATE "C"), '[]')
FROM fingerprint;

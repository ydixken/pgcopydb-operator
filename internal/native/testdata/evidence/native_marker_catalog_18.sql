-- Generated test fixture from native-8 src/apply/metadata.rs.
-- Source SHA-256: ebef9685b217811281233b8f116a69a000acf1ff86982d1c155461007ee53aa2
SELECT c.relnamespace=$3::oid AND c.relowner=$4::oid AND c.relname='copy_done'
        AND c.relkind='r' AND c.relpersistence='p' AND c.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap')
        AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT c.relispartition AND NOT c.relisshared
        AND c.reloftype=0 AND c.relnatts=11 AND c.relchecks=0
        AND pg_catalog.has_table_privilege(c.oid,'SELECT') AND pg_catalog.has_table_privilege(c.oid,'INSERT') AND pg_catalog.has_table_privilege(c.oid,'DELETE')
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) a WHERE a.grantor<>c.relowner OR (a.grantee<>c.relowner AND (a.grantee<>(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=CURRENT_USER) OR a.privilege_type NOT IN('SELECT','INSERT','DELETE') OR a.is_grantable)))
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT EXISTS(SELECT 1 FROM (VALUES (1,'magic',25,-1,true),(2,'version',23,-1,true),(3,'run_id',17,-1,true),(4,'nonce',17,-1,true),(5,'source_table',26,-1,true),(6,'target_table',26,-1,true),(7,'copy_generation',1700,1310724,true),(8,'rows',1700,1310724,true),(9,'bytes',1700,1310724,true),(10,'target_namespace',26,-1,true),(11,'target_owner',26,-1,true)) e(num,name,ty,modifier,required) WHERE a.attnum=e.num AND a.attname=e.name AND a.atttypid=e.ty::oid AND a.atttypmod=e.modifier AND a.attnotnull=e.required AND NOT a.attisdropped AND NOT a.atthasdef AND a.attislocal AND a.attinhcount=0 AND a.attndims=0 AND a.attacl IS NULL AND a.attoptions IS NULL AND a.attfdwoptions IS NULL AND a.attidentity='' AND a.attgenerated='' AND a.attcollation=CASE WHEN a.atttypid=25 THEN 'pg_catalog."C"'::pg_catalog.regcollation::oid ELSE 0 END))
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attrdef a WHERE a.adrelid=c.oid)
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid)
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_policy p WHERE p.polrelid=c.oid)
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class=c.oid)
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits h WHERE h.inhrelid=c.oid OR h.inhparent=c.oid)
        AND (SELECT count(*) FROM pg_catalog.pg_index ix WHERE ix.indrelid=c.oid)=1
        AND i.indexrelid=$2::oid AND i.indisunique AND i.indisprimary AND NOT i.indisexclusion AND i.indimmediate AND i.indisvalid AND i.indisready AND i.indislive
        AND i.indnatts=2 AND i.indnkeyatts=2 AND i.indexprs IS NULL AND i.indpred IS NULL
        AND i.indkey='7 5'::pg_catalog.int2vector AND i.indcollation='0 0'::pg_catalog.oidvector AND i.indoption='0 0'::pg_catalog.int2vector
        AND idx.relkind='i' AND idx.relnamespace=c.relnamespace AND idx.relowner=c.relowner AND idx.relpersistence='p' AND idx.relacl IS NULL
        AND idx.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='btree')
        AND i.indclass=ARRAY[(SELECT oid FROM pg_catalog.pg_opclass WHERE opcnamespace='pg_catalog'::pg_catalog.regnamespace AND opcname='numeric_ops' AND opcmethod=idx.relam AND opcdefault),(SELECT oid FROM pg_catalog.pg_opclass WHERE opcnamespace='pg_catalog'::pg_catalog.regnamespace AND opcname='oid_ops' AND opcmethod=idx.relam AND opcdefault)]::pg_catalog.oidvector
        AND p.contype='p' AND p.connamespace=c.relnamespace AND NOT p.condeferrable AND NOT p.condeferred AND p.convalidated AND p.conislocal AND p.coninhcount=0 AND p.conkey=ARRAY[7,5]::pg_catalog.int2[] AND p.conbin IS NULL
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint con WHERE con.conrelid=c.oid AND NOT(con.oid=p.oid OR (con.contype='n' AND con.conenforced AND con.convalidated AND NOT con.condeferrable AND NOT con.condeferred AND con.conislocal AND con.coninhcount=0 AND con.conindid=0 AND con.conbin IS NULL AND cardinality(con.conkey)=1 AND EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum=con.conkey[1] AND a.attnotnull))))
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON i.indrelid=c.oid JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid JOIN pg_catalog.pg_constraint p ON p.conrelid=c.oid AND p.conindid=idx.oid WHERE c.oid=$1::oid;

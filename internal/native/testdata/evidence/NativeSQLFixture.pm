package NativeSQLFixture;
use strict;
use warnings;
use PSQLEvidencePrototype;
use EvidencePrototype;
use JSON::PP ();
my $json=JSON::PP->new->canonical;
sub _phase {my($s,$sql,$check)=@_;$s->phase($sql,'',$check)or die "fixture_sql_refused\n"}
sub _row {
    my($raw,$width)=@_;my @lines=split(/\n/,$raw,-1);@lines==2&&$lines[1]eq''or die "fixture_cardinality\n";my @v=split(/\|/,$lines[0],-1);@v==$width or die "fixture_cardinality\n";return \@v;
}
sub _number {EvidencePrototype::_decimal($_[0],$_[1]//'18446744073709551615',1)}
sub _ddl {
    my($file,$schema)=@_;$file eq'native_checkpoint_ddl.sql'||$file eq'native_marker_ddl.sql'or die "fixture_file\n";open(my $in,'<','/probe/'.$file)or die "fixture_file\n";my $raw=EvidencePrototype::_bytes($in,16384);close($in);$raw=~s/^--[^\n]*\n//mg;$raw=~s/\n/ /g;$raw=~s/\@SCHEMA\@/"$schema"/g;return $raw;
}
sub prepare {
    my($caller,$m,$stream,$source,$plan,$target,$publication)=@_;my $run=$caller->{work}{run_id};$run=~/\A[0-9a-f]{32}\z/&&$run ne('0'x32)or die "fixture_run\n";
    my $source_database=$ENV{NATIVE_EVIDENCE_SOURCE_DATABASE}//die "fixture_source_database\n";$source_database=~/\Am6_source_[0-9a-f]+\z/ or die "fixture_source_database\n";
    $ENV{PGCOPYDB_SOURCE_PGURI}=$source_database;
    my $src=PSQLEvidencePrototype->new(8192,'source');my($system,$database,$timeline,$source_major,$source_owner);
    my $source_ok=eval{
        _phase($src,q{SELECT c.system_identifier::text,d.oid,k.timeline_id,current_setting('server_version_num'),r.oid FROM pg_catalog.pg_control_system() c CROSS JOIN pg_catalog.pg_control_checkpoint() k CROSS JOIN pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_roles r WHERE d.datname=current_database() AND r.rolname=CURRENT_USER;},sub{my $v=_row($_[0],5);($system,$database,$timeline,$source_owner)=map{_number($_)}@$v[0,1,2,4];$source_major=int(_number($v->[3])/10000);($source_major==16||$source_major==18)});1;
    };$src->{failed}=1 unless $source_ok;my $source_closed=$src->finish();$source_ok&&$source_closed or die "fixture_source\n";
    my $schema='_rpgmigrate_'.$run;$target->{intent}{binding}{schema_name}=$schema;
    my @run=unpack('C*',pack('H*',$run));$stream->{run}=\@run;$m->{run}=\@run;$m->{operator}{run}=\@run;$m->{operator}{migration_uid}=$caller->{work}{migration_uid};$m->{operator}{workdir_id}=$caller->{work}{workdir_id};$m->{follow_state}{publication}{intent}{run}=\@run;
    $stream->{source}={system_identifier=>$system,database_oid=>$database};$source->{identity}={system_identifier=>$system,database_oid=>$database};$source->{timeline}=$timeline;$source->{major}=$source_major;$m->{follow_state}{binding}{source_timeline}=$timeline;$publication->{owner_oid}=$source_owner;
    $caller->{source}={system_identifier=>$system,database_oid=>$database,timeline=>$timeline,major=>$source_major};
    my $s=PSQLEvidencePrototype->new(16384,'target');my($target_system,$target_db,$owner,$major);
    my $ok=eval{
        _phase($s,'SET synchronous_commit=off;',sub{$_[0]eq"SET\n"});
        _phase($s,q{SELECT c.system_identifier::text,d.oid,r.oid,current_setting('server_version_num') FROM pg_catalog.pg_control_system() c CROSS JOIN pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_roles r WHERE d.datname=current_database() AND r.rolname=CURRENT_USER;},sub{my $v=_row($_[0],4);($target_system,$target_db,$owner)=map{_number($_)}@$v[0,1,2];$major=int(_number($v->[3])/10000);($major==16||$major==18)&&$source_major<=$major});
        _phase($s,'CREATE SCHEMA "'.$schema.'";',sub{$_[0]eq"CREATE SCHEMA\n"});
        _phase($s,'REVOKE ALL ON SCHEMA "'.$schema.'" FROM PUBLIC;',sub{$_[0]eq"REVOKE\n"});
        _phase($s,_ddl('native_checkpoint_ddl.sql',$schema),sub{$_[0]eq"CREATE TABLE\n"});
        _phase($s,_ddl('native_marker_ddl.sql',$schema),sub{$_[0]eq"CREATE TABLE\n"});
        _phase($s,'REVOKE ALL ON ALL TABLES IN SCHEMA "'.$schema.'" FROM PUBLIC;',sub{$_[0]eq"REVOKE\n"});
        _phase($s,"SELECT n.oid,c.oid,m.oid,i.oid FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname='checkpoint' JOIN pg_catalog.pg_class m ON m.relnamespace=n.oid AND m.relname='copy_done' JOIN pg_catalog.pg_class i ON i.relnamespace=n.oid AND i.relname='copy_done_pkey' WHERE n.nspname='$schema';",sub{my $v=_row($_[0],4);@$target{qw(schema_oid table_oid marker_table_oid marker_index_oid)}=map{_number($_,'4294967295')}@$v;1});1;
    };$s->{failed}=1 unless $ok;my $closed=$s->finish();$ok&&$closed or die "fixture_target\n";
    my $b=$target->{intent}{binding};$b->{target_system_identifier}=$target_system;$b->{target_database_oid}=$target_db;$b->{owner_oid}=$owner;$plan->{target}={identity=>{system_identifier=>$target_system,database_oid=>$target_db},major=>$major};
    $caller->{target}={system_identifier=>$target_system,database_oid=>$target_db,major=>$major,owner_oid=>$owner,(map{$_=>$target->{$_}}qw(schema_oid table_oid marker_table_oid marker_index_oid)),nonce=>$target->{intent}{nonce}};
    return {schema=>$schema,major=>$major,system=>$system,database=>$database};
}
sub write_checkpoint {
    my($m,$target,$kind)=@_;my $b=$target->{intent}{binding};my $clone=$m->{clone_state};my $follow=$m->{follow_state};my $certified=$kind eq'keepalive'?'0/30':'0/10';
    my $columns=EvidencePrototype::_columns();my @cells=('rpgmigrate','6',unpack('H*',pack('C*',@{$m->{run}})),unpack('H*',pack('C*',@{$target->{intent}{nonce}})),map{"$_"}($b->{stream}{source}{system_identifier},$b->{stream}{source}{database_oid},$b->{stream}{generation},$b->{target_system_identifier},$b->{target_database_oid},$b->{owner_oid},$target->{schema_oid},$target->{table_oid}));
    push @cells,'1','7','9','0/9','0/10','0','7',$clone->{ticket}{token},'0/8',unpack('H*',pack('C*',@{$m->{source_slot}{record}{intent}{nonce}})), $json->encode({generation=>$clone->{plan}{generation},phase=>'analyzing',snapshot_token=>$clone->{ticket}{token},source_timeline=>$clone->{plan}{source}{timeline},table_count=>0}),$certified,$json->encode({binding=>$follow->{binding},operator=>$m->{operator},origin_name=>$follow->{origin_name}}),'0/20','0/30',$kind,'t';
    @cells==29 or die "fixture_width\n";my @values=map{EvidencePrototype::_sql_literal($cells[$_],$columns->[$_])}0..28;
    my $table='"'.$b->{schema_name}.'"."checkpoint"';my $s=PSQLEvidencePrototype->new(16384,'target');my $ok=eval{_phase($s,'SET synchronous_commit=off;',sub{$_[0]eq"SET\n"});_phase($s,'INSERT INTO '.$table.' VALUES('.join(',',@values).');',sub{$_[0]eq"INSERT 0 1\n"});1};$s->{failed}=1 unless $ok;my $closed=$s->finish();$ok&&$closed or die "fixture_row\n";return \@cells;
}
sub mutate {
    my($case,$m)=@_;my $schema=$m->{target_provenance}[0]{record}{intent}{binding}{schema_name};my $table='"'.$schema.'"."checkpoint"';my @commands;
    if($case eq'missing_relation'){@commands=("DROP TABLE $table;")}
    elsif($case eq'replaced_relation'){@commands=("ALTER TABLE $table RENAME TO checkpoint_old;",_ddl('native_checkpoint_ddl.sql',$schema))}
    elsif($case eq'unlogged'){@commands=("ALTER TABLE $table SET UNLOGGED;")}
    elsif($case eq'extra_table'){@commands=('CREATE TABLE "'.$schema.'".unexpected(id int);')}
    elsif($case eq'extra_index'){@commands=('CREATE INDEX unexpected ON '.$table.'(barrier_counter);')}
    elsif($case eq'dropped_column'){@commands=("ALTER TABLE $table DROP COLUMN certified_lsn;")}
    elsif($case eq'missing_row'){@commands=("TRUNCATE $table;")}
    elsif($case eq'multiple_rows'){@commands=("INSERT INTO $table SELECT magic,version,pg_catalog.decode('ffffffffffffffffffffffffffffffff','hex'),nonce,source_system,source_database,generation,target_system,target_database,owner_oid,schema_oid,table_oid,applied_order,last_xid,begin_ordinal,source_commit_lsn,source_end_lsn,barrier_epoch,barrier_counter,copy_snapshot_token,copy_consistent_point,copy_plan_nonce,clone_state,certified_lsn,follow_binding,drain_end_lsn,drain_witness_lsn,drain_kind,follow_completed FROM ONLY $table;")}
    elsif($case eq'wrong_row_nonce'){@commands=("UPDATE ONLY $table SET nonce=pg_catalog.decode('ffffffffffffffffffffffffffffffff','hex');")}
    elsif($case eq'wrong_row_generation'){@commands=("UPDATE ONLY $table SET generation=2;")}
    elsif($case eq'counter_overflow'){@commands=("UPDATE ONLY $table SET barrier_counter=18446744073709551616;")}
    elsif($case eq'keepalive_low_cert'){@commands=("UPDATE ONLY $table SET drain_kind='keepalive',certified_lsn='0/10';")}
    elsif($case eq'uncompleted'){@commands=("UPDATE ONLY $table SET follow_completed=false;")}
    return unless @commands;
    my $s=PSQLEvidencePrototype->new(16384,'target');my $ok=eval{_phase($s,'SET synchronous_commit=off;',sub{$_[0]eq"SET\n"});for my $sql(@commands){_phase($s,$sql,sub{$_[0]=~/\A(?:DROP TABLE|ALTER TABLE|CREATE TABLE|CREATE INDEX|TRUNCATE TABLE|UPDATE 1|INSERT 0 1)\n\z/})}1};$s->{failed}=1 unless $ok;my $closed=$s->finish();$ok&&$closed or die "fixture_mutation\n";
}
sub token {
    my($m)=@_;my $schema=$m->{target_provenance}[0]{record}{intent}{binding}{schema_name};my $s=PSQLEvidencePrototype->new(4096,'target');my $token;
    my $ok=eval{_phase($s,'SELECT generation,barrier_epoch,barrier_counter FROM ONLY "'.$schema.'".checkpoint;',sub{my $v=_row($_[0],3);$token=join(':',@$v);$token eq'1:0:7'});1};$s->{failed}=1 unless $ok;my $closed=$s->finish();return $ok&&$closed?$token:undef;
}
1;

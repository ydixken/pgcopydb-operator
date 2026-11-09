use strict;
use warnings;
use lib '/probe';
use EvidencePrototype;
use JSON::PP ();
use IO::Handle ();
use Fcntl qw(:DEFAULT :flock :mode);
$SIG{ALRM}=sub { die "probe_timeout\n" }; alarm 15; umask 0077;
my $case = shift // die "case\n";
alarm($case=~/\Asql_/?28:15);
my $caller_raw=shift;defined($caller_raw)or die "caller\n";
my $caller = EvidencePrototype::strict_json($caller_raw,4096);
my $execution = { migration_uid=>'00112233-4455-6677-8899-aabbccddeeff', job_controller_uid=>'00112233-4455-6677-8899-aabbccddeeff', pod_controller_uid=>'job-fixture', job_uid=>'job-fixture', pod_uid=>'pod-fixture', pvc_uid=>'pvc-fixture', attempt=>1, container=>'pgcopydb', endpoint_binding=>'source-ref/target-ref' };
if($case eq'observe_existing'){
    require ComposedFaultFixture;
    my $execution_raw=shift;my $fault_mode=shift;my $id=shift;
    defined($execution_raw)&&defined($fault_mode)&&$fault_mode=~/\A(?:none|sync_cancel|lost_commit|post_commit_loss)\z/&&defined($id)&&$id=~/\A[0-9a-f]{32}\z/ or die "exec_input\n";
    $execution=EvidencePrototype::strict_json($execution_raw,4096);
    my %trace=(stage=>'admission',stages=>[]);my $held=eval{EvidencePrototype::admit($caller,$execution)};my $controller;
    my %options=(trace=>\%trace);
    $options{before_commit}=sub{$controller=ComposedFaultFixture::start_cancel($_[0]{backend})}if $fault_mode eq'sync_cancel';
    my $observation=eval{$held or die "fixture_admission\n";EvidencePrototype::observe($held,$caller,\%options)};
    my $constructor=length($@)&&($trace{stage}eq'target_open'||$trace{stage}eq'source_identity')?ComposedFaultFixture::constructor_discriminator($@):'';
    if($fault_mode eq'sync_cancel'){defined($controller)&&ComposedFaultFixture::finish_fault($controller)or die "cancel_unconfirmed\n"}
    alarm 0;
    print JSON::PP->new->canonical->encode({id=>$id,pid=>$$,admitted=>$held?JSON::PP::true:JSON::PP::false,observed=>$observation?JSON::PP::true:JSON::PP::false,position_present=>$observation?JSON::PP::true:JSON::PP::false,source_closed=>$trace{source_closed}?JSON::PP::true:JSON::PP::false,constructor_discriminator=>$constructor,stages=>$trace{stages},refusal_stage=>$observation?'':$trace{stage},failure=>$observation?'':($trace{failure}//'validation'),all_eof=>$observation&&$observation->{all_eof}?JSON::PP::true:JSON::PP::false,exit=>$trace{exit}//-1,signal=>$trace{signal}//0,stderr_bytes=>$trace{stderr_bytes}//0,token_preserved=>JSON::PP::false,row_preserved=>JSON::PP::false,fault_events=>$controller?$controller->{events}:[]}),"\n";
    exit 0;
}
mkdir('/work/pgcopydb',0700) or die "fixture_work\n";
mkdir('/work/pgcopydb/control',0700) or die "fixture_control\n";
my @run = unpack('C*',pack('H*','00112233445566778899aabbccddeeff'));
my $stream = {run=>\@run,source=>{system_identifier=>123,database_oid=>11},generation=>1};
my $operator = {migration_uid=>'00112233-4455-6677-8899-aabbccddeeff',run=>\@run,workdir_id=>'pvc-fixture',checkpoint_generation=>1};
my $slot = {intent=>{binding=>{stream=>$stream,slot_name=>'native_slot',plugin=>'pgoutput',catalog_epoch=>1},nonce=>[(1)x16]},consistent_point=>8,snapshot_token=>'snapshot'};
my $publication = {name=>'native_publication',oid=>88,owner_oid=>33};
my $target = {intent=>{binding=>{stream=>$stream,target_system_identifier=>456,target_database_oid=>22,schema_name=>'_rpgmigrate_00112233445566778899aabbccddeeff',table_name=>'checkpoint',owner_oid=>33,metadata_version=>6},nonce=>[(2)x16]},schema_oid=>44,table_oid=>55,marker_table_oid=>66,marker_index_oid=>77};
my $source = {identity=>{system_identifier=>123,database_oid=>11},timeline=>1,major=>16};
my $plan = {source=>$source,target=>{identity=>{system_identifier=>456,database_oid=>22},major=>18},generation=>1,tables=>[],sequences=>[],policy=>{no_owner=>JSON::PP::false,no_acl=>JSON::PP::false,no_comments=>JSON::PP::false,no_tablespaces=>JSON::PP::false}};
my $binding = {slot=>$slot,publication=>$publication,source_timeline=>1,copy_generation=>1,source_types=>[{oid=>25,schema=>'pg_catalog',name=>'text'}]};
my @stat = stat('/work/pgcopydb');
my $manifest = {magic=>'RPGMIGRATE',format_version=>8,run=>\@run,canonical_path=>'/work/pgcopydb',directory_device=>$stat[0],directory_inode=>$stat[1],copy_complete=>undef,target_provenance=>[{phase=>'confirmed',record=>$target}],source_slot=>{phase=>'confirmed',record=>$slot},clone_state=>{plan=>$plan,ticket=>{token=>'snapshot',source=>$source,generation=>1},phase=>'analyzing',owned_objects=>[],owned_tables=>[],completion=>undef},follow_state=>{cleanup=>undef,origin_name=>undef,publication=>{phase=>'confirmed',intent=>{run=>\@run,name=>'native_publication',nonce=>[(3)x16]},receipt=>$publication},binding=>$binding,restart=>undef,preliminary=>undef},operator=>$operator};
my $endpos = {version=>1,stream=>$stream,copy_generation=>1,slot_nonce=>[(1)x16],target_nonce=>[(2)x16],end_lsn=>32,sealed=>JSON::PP::true};
if($case eq 'wrong_migration'){$manifest->{operator}{migration_uid}='ffeeddcc-bbaa-9988-7766-554433221100'}
if($case eq 'wrong_pvc'){$manifest->{operator}{workdir_id}='other-pvc'}
if($case eq 'wrong_job'){$execution->{job_uid}='other-job'}
if($case eq 'wrong_job_owner'){$execution->{job_controller_uid}='wrong-owner'}
if($case eq 'wrong_pod_owner'){$execution->{pod_controller_uid}='wrong-owner'}
if($case eq 'wrong_pod'){$execution->{pod_uid}='other-pod'}
if($case eq 'wrong_attempt'){$execution->{attempt}=2}
if($case eq 'wrong_endpoint'){$execution->{endpoint_binding}='other-reference'}
if($case eq 'wrong_source'){$stream->{source}{system_identifier}=124}
if($case eq 'wrong_target'){$target->{intent}{binding}{target_database_oid}=23}
if($case eq 'wrong_nonce'){$target->{intent}{nonce}=[(9)x16]}
if($case eq 'wrong_generation'){$stream->{generation}=2}
if($case eq 'wrong_directory'){$manifest->{directory_inode}++}
if($case eq 'provisioning_target'){$manifest->{target_provenance}=[{phase=>'provisioning',record=>$target->{intent}}]}
if($case eq 'extra_target'){push @{$manifest->{target_provenance}},$manifest->{target_provenance}[0]}
if($case eq 'missing_target'){$manifest->{target_provenance}=[]}
if($case eq 'unknown_key'){$manifest->{unrecognized}=1}
if($case eq 'too_many_tables'){$plan->{tables}=[map {{source_oid=>$_,name=>{schema=>'public',table=>'t'.$_},writable_columns=>[],all_columns=>[],identity_modes=>[],partition_identity_modes=>undef}}1..4097]}
if($case eq 'too_many_columns'){$plan->{tables}=[{source_oid=>99,name=>{schema=>'public',table=>'t'},writable_columns=>[],all_columns=>[map{'c'.$_}1..1601],identity_modes=>[(0)x1601],partition_identity_modes=>undef}]}
if($case eq 'too_many_types'){$binding->{source_types}=[map{{oid=>$_,schema=>'s',name=>'t'}}1..16385]}
if($case eq 'many_records'){$plan->{sequences}=[map{{schema=>'s',table=>'q'.$_}}1..20000]}
if($case eq 'u64_max'){$manifest->{copy_complete}={tables=>18446744073709551615,rows=>18446744073709551615,bytes=>18446744073709551615}}
if($case eq 'unicode_pair'){$binding->{source_types}[0]{name}=chr(0x1f600)}
my $fixture_major;
my $sql_case=$case=~/\Asql_(.+)\z/?$1:undef;
if(defined($sql_case)){
    require NativeSQLFixture;
    $execution->{migration_uid}=$caller->{work}{migration_uid};
    $execution->{job_controller_uid}=$caller->{work}{migration_uid};
    if($sql_case eq'large_binding'){$binding->{source_types}=[map{{oid=>$_,schema=>'source_schema',name=>'source_type_'.$_}}1..2400]}
    $fixture_major=NativeSQLFixture::prepare($caller,$manifest,$stream,$source,$plan,$target,$publication)->{major};
    my $kind=$sql_case eq'keepalive'?'keepalive':'later_begin';
    NativeSQLFixture::write_checkpoint($manifest,$target,$kind);
    NativeSQLFixture::mutate($sql_case,$manifest);
    if($sql_case eq'wrong_source_endpoint'){$ENV{PGCOPYDB_SOURCE_PGURI}='postgres'}
    if($sql_case eq'wrong_target_endpoint'){$ENV{PGCOPYDB_TARGET_PGURI}=$ENV{NATIVE_EVIDENCE_SOURCE_DATABASE}}
    if($sql_case eq'remote_apply'){$ENV{PGOPTIONS}='-c synchronous_commit=remote_apply'}
    if($sql_case eq'upgrade_sync'){$ENV{PGOPTIONS}='-c synchronous_commit=off'}
}
my $json = JSON::PP->new->canonical->ascii;
my $raw = $json->encode($manifest);
my $control_raw = $json->encode($endpos);
if($case eq 'escaped_duplicate'){$raw =~ s/"magic":/"\\u006dagic":"RPGMIGRATE","magic":/ or die "mutation\n"}
if($case eq 'nested_duplicate'){$raw =~ s/"metadata_version":6/"metadata_version":6,"metadata_version":6/ or die "mutation\n"}
if($case eq 'invalid_utf8'){$raw =~ s/"magic":"RPGMIGRATE"/"magic":"\xff"/ or die "mutation\n"}
if($case eq 'lone_surrogate'){$raw =~ s/"magic":"RPGMIGRATE"/"magic":"\\ud800"/ or die "mutation\n"}
if($case eq 'truncated'){chop $raw}
if($case eq 'fractional_integer'){$raw =~ s/"format_version":8/"format_version":8.0/ or die "mutation\n"}
if($case eq 'exponent_integer'){$raw =~ s/"format_version":8/"format_version":8e0/ or die "mutation\n"}
if($case eq 'string_integer'){$raw =~ s/"format_version":8/"format_version":"8"/ or die "mutation\n"}
if($case eq 'u64_overflow'){$raw =~ s/"directory_inode":\d+/"directory_inode":18446744073709551616/ or die "mutation\n"}
if($case eq 'oversized_manifest'){$raw .= ' 'x1048577}
if($case eq 'oversized_control'){$control_raw .= ' 'x4097}
if($case eq 'depth_33'){$raw='{ "unexpected":'.('['x33).'0'.(']'x33).'}'}
sub write_private { my($name,$bytes)=@_;sysopen(my $out,$name,O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW,0600)or die "fixture_write\n";print {$out}$bytes or die "fixture_write\n";$out->sync or die "fixture_sync\n";close($out)or die "fixture_close\n"; }
write_private('/work/pgcopydb/manifest.json',$raw);
write_private('/work/pgcopydb/control/lock','');
write_private('/work/pgcopydb/control/endpos.json',$control_raw);
if($case eq 'pending_manifest'){write_private('/work/pgcopydb/.manifest.tmp','{}')}
if($case eq 'pending_control'){write_private('/work/pgcopydb/control/.endpos.tmp','{}')}
if($case eq 'root_symlink'){rename('/work/pgcopydb','/work/original')or die "fixture_rename\n";symlink('/work/original','/work/pgcopydb')or die "fixture_symlink\n"}
if($case eq 'hardlink'){link('/work/pgcopydb/manifest.json','/work/alias')or die "fixture_link\n"}
my $competitor;
if($case eq 'owner_busy'){sysopen($competitor,'/work/pgcopydb',O_RDONLY|O_DIRECTORY)or die "fixture_open\n";flock($competitor,LOCK_EX|LOCK_NB)or die "fixture_lock\n"}
if($case eq 'control_busy'){sysopen($competitor,'/work/pgcopydb/control/lock',O_RDONLY)or die "fixture_open\n";flock($competitor,LOCK_EX|LOCK_NB)or die "fixture_lock\n"}
my $hook = sub {
    if($case eq 'replaced_manifest'){rename('/work/pgcopydb/manifest.json','/work/prior')or die "fixture_rename\n";write_private('/work/pgcopydb/manifest.json',$raw)}
    if($case eq 'root_replaced'){rename('/work/pgcopydb','/work/prior')or die "fixture_rename\n";mkdir('/work/pgcopydb',0700)or die "fixture_directory\n"}
    if($case eq 'control_replaced'){rename('/work/pgcopydb/control','/work/prior')or die "fixture_rename\n";mkdir('/work/pgcopydb/control',0700)or die "fixture_directory\n"}
    if($case eq 'lock_replaced'){rename('/work/pgcopydb/control/lock','/work/prior')or die "fixture_rename\n";write_private('/work/pgcopydb/control/lock','')}
    if($case eq 'interrupted_same_size'){sysopen(my $change,'/work/pgcopydb/manifest.json',O_WRONLY|O_NOFOLLOW)or die "fixture_open\n";syswrite($change,'[')==1 or die "fixture_write\n";close($change)or die "fixture_close\n"}
};
if(defined($sql_case)&&$sql_case eq'missing_manifest'){unlink('/work/pgcopydb/manifest.json')or die "fixture_unlink\n"}
if(defined($sql_case)&&$sql_case=~/\Acomposed_(sync_cancel|lost_commit|post_commit_loss)(?:\/(wrong_owner|wrong_pvc|wrong_attempt|missing_manifest|owner_busy|fence_busy|replaced_relation|wrong_source_endpoint|wrong_target_endpoint|second_commit_fault))?\z/){
    require ComposedFaultFixture;
    my($mode,$retry)=($1,$2//'healthy');my $name=$mode.($retry eq'healthy'?'':'/'.$retry);
    my $before=ComposedFaultFixture::row_bytes($manifest);
    my $attempt=sub{
        my($fault)=@_;my($proxy,$r,$ok);my %launch;
        if($fault eq'sync_cancel'){ComposedFaultFixture::synchronous_wait(1)}
        $ok=eval{
            local %ENV = %ENV;
            if($fault eq'lost_commit'||$fault eq'post_commit_loss'){$proxy=ComposedFaultFixture::start_proxy($fault);$ENV{PGCOPYDB_TARGET_PGURI}='host=127.0.0.1 port='.$proxy->{port}.' dbname=postgres sslmode=disable'}
            $r=ComposedFaultFixture::reader_exec($caller,$execution,$fault,\%launch);1;
        };
        ComposedFaultFixture::synchronous_wait(0)if $fault eq'sync_cancel';
        my $confirmed=$proxy?(ComposedFaultFixture::finish_fault($proxy)&&ComposedFaultFixture::committed_idle($proxy)):$ok;
        if(!$ok||!$confirmed){ComposedFaultFixture::emit_partial($name,\%launch,$proxy);die "composed_fault_unconfirmed\n"}$r->{fault_events}=$proxy->{events}if $proxy;return $r;
    };
    my $first=$attempt->($mode);
    my $unchanged=ComposedFaultFixture::row_bytes($manifest)eq$before;
    $first->{row_preserved}=$first->{token_preserved}=$unchanged?JSON::PP::true:JSON::PP::false;
    $first->{admitted}&&$first->{source_closed}&&!$first->{observed}&&!$first->{position_present}or die "composed_first_not_refused\n";
    if($retry eq'wrong_owner'){$execution->{job_controller_uid}='other-owner'}
    if($retry eq'wrong_pvc'){$execution->{pvc_uid}='other-pvc'}
    if($retry eq'wrong_attempt'){$execution->{attempt}=2}
    if($retry eq'missing_manifest'){rename('/work/pgcopydb/manifest.json','/work/prior_manifest')or die "retry_manifest\n"}
    my($owner,$fencer,$fence);
    if($retry eq'owner_busy'){sysopen($owner,'/work/pgcopydb',O_RDONLY|O_DIRECTORY)or die "retry_owner\n";flock($owner,LOCK_EX|LOCK_NB)or die "retry_owner_lock\n"}
    if($retry eq'fence_busy'){
        my(undef,$slot)=EvidencePrototype::_fence($manifest->{run});$fence="1380992845,$slot";$fencer=PSQLEvidencePrototype->new(4096,'target');
        $fencer->phase("SELECT pg_catalog.pg_try_advisory_lock($fence);",'',sub{$_[0]eq"t\n"})or die "retry_fence\n";
    }
    NativeSQLFixture::mutate('replaced_relation',$manifest)if $retry eq'replaced_relation';
    my $second;
    {
        local %ENV = %ENV;
        $ENV{PGCOPYDB_SOURCE_PGURI}='postgres'if $retry eq'wrong_source_endpoint';
        $ENV{PGCOPYDB_TARGET_PGURI}=$ENV{NATIVE_EVIDENCE_SOURCE_DATABASE}if $retry eq'wrong_target_endpoint';
        $second=$attempt->($retry eq'second_commit_fault'?'lost_commit':'none');
    }
    if($fencer){$fencer->phase("SELECT pg_catalog.pg_advisory_unlock($fence);",'',sub{$_[0]eq"t\n"})&&$fencer->finish()or die "retry_fence_close\n"}
    close($owner)or die "retry_owner_close\n"if $owner;
    $unchanged=ComposedFaultFixture::row_bytes($manifest,$retry eq'replaced_relation'?'checkpoint_old':'checkpoint')eq$before;
    $second->{row_preserved}=$second->{token_preserved}=$unchanged?JSON::PP::true:JSON::PP::false;
    alarm 0;
    print $json->encode({case=>$name,server_major=>$fixture_major,mocked_kubernetes_owner=>JSON::PP::true,fault_confirmed=>JSON::PP::true,attempts=>[$first,$second]}),"\n";
    exit 0;
}
my $admitted = eval { EvidencePrototype::admit($caller,$execution,$hook) };
if(defined($sql_case)){
    my %trace=(stage=>'admission');my %options=(trace=>\%trace);
    if($sql_case eq'predecessor_changed'){$options{before_update}=sub{$_[1][18]='6'}}
    my $observation=eval{$admitted or die "fixture_admission\n";EvidencePrototype::observe($admitted,$caller,\%options)};
    my $token=defined($observation)?NativeSQLFixture::token($manifest):undef;
    alarm 0;
    print $json->encode({observed=>$observation?JSON::PP::true:JSON::PP::false,server_major=>$fixture_major,reader_major=>$observation?$observation->{server_major}:0,token_preserved=>defined($token)&&$token eq'1:0:7'?JSON::PP::true:JSON::PP::false,all_eof=>$observation&&$observation->{all_eof}?JSON::PP::true:JSON::PP::false,durable=>$observation?$observation->{positions}{durable}:'',mocked_kubernetes_owner=>JSON::PP::true,refusal_stage=>$observation?'':$trace{stage},refusal_kind=>$observation?'':($trace{failure}//'validation'),sqlstate=>$observation?'':($trace{sqlstate}//'')}),"\n";
    exit 0;
}
if($case=~/\Arow_/){
    my $clone={generation=>1,phase=>'analyzing',snapshot_token=>'snapshot',source_timeline=>1,table_count=>0};
    my $follow={binding=>$binding,operator=>$operator,origin_name=>undef};
    my $row=['rpgmigrate','6','00112233445566778899aabbccddeeff',('02'x16),'123','11','1','456','22','33','44','55','1','7','9','0/9','0/10','0','7','snapshot','0/8',('01'x16),$json->encode($clone),'0/10',$json->encode($follow),'0/20','0/30','later_begin','t'];
    if($case eq 'row_keepalive'){$row->[23]='0/30';$row->[27]='keepalive'}
    if($case eq 'row_keepalive_low_cert'){$row->[27]='keepalive'}
    if($case eq 'row_crossing_txn'){$row->[16]='0/28'}
    if($case eq 'row_empty_applied'){$row->[12]='0';@$row[13..16]=(undef)x4}
    if($case eq 'row_uncertified_later_begin'){$row->[23]=undef}
    if($case eq 'row_counter_max'){$row->[18]='18446744073709551615'}
    if($case eq 'row_later_begin_not_strict'){$row->[26]='0/20'}
    if($case eq 'row_end_mismatch'){$row->[25]='0/21'}
    if($case eq 'row_commit_after_end'){$row->[15]='0/20'}
    if($case eq 'row_counter_overflow'){$row->[18]='18446744073709551616'}
    if($case eq 'row_counter_zero'){$row->[18]='0'}
    if($case eq 'row_epoch_overflow'){$row->[17]='18446744073709551616'}
    if($case eq 'row_wrong_version'){$row->[1]='5'}
    if($case eq 'row_wrong_run'){$row->[2]='f'x32}
    if($case eq 'row_wrong_nonce'){$row->[3]='f'x32}
    if($case eq 'row_wrong_source'){$row->[4]='124'}
    if($case eq 'row_wrong_db'){$row->[8]='23'}
    if($case eq 'row_wrong_schema'){$row->[10]='45'}
    if($case eq 'row_wrong_table'){$row->[11]='56'}
    if($case eq 'row_missing_column'){pop @$row}
    if($case eq 'row_extra_column'){push @$row,'extra'}
    if($case eq 'row_anchor_partial'){$row->[21]=undef}
    if($case eq 'row_drain_partial'){$row->[26]=undef}
    if($case eq 'row_uncompleted'){$row->[28]='f'}
    if($case eq 'row_clone_generation'){$clone->{generation}=2;$row->[22]=$json->encode($clone)}
    if($case eq 'row_follow_operator'){$follow->{operator}{workdir_id}='wrong-pvc';$row->[24]=$json->encode($follow)}
    my $positions=eval { $admitted or die "admission\n";EvidencePrototype::validate_control_row($row,$admitted,$caller) };
    alarm 0;
    print $json->encode({row_valid=>$positions?JSON::PP::true:JSON::PP::false,durable=>$positions?$positions->{durable}:''}),"\n";
    exit 0;
}
alarm 0;
print $json->encode({case=>$case,accepted=>$admitted?JSON::PP::true:JSON::PP::false,canonical=>JSON::PP::true,lifetime_held=>$admitted&&$admitted->{lifetime_held}?JSON::PP::true:JSON::PP::false,control_released=>$admitted&&$admitted->{control_released}?JSON::PP::true:JSON::PP::false}),"\n";

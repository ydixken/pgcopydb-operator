package EvidencePrototype;
use strict;
use warnings;
use JSON::PP ();
use Encode qw(decode encode FB_CROAK LEAVE_SRC);
use B ();
use Fcntl qw(:DEFAULT :flock :mode);

# Test-only native-8 admission; this module cannot create operator checkpoint evidence.
my $json = JSON::PP->new->canonical->allow_nonref;
my $u64 = '18446744073709551615';
sub _fail { die "native_admission_refused\n" }
sub _keys { my($v,@keys)=@_; ref($v)eq'HASH' && join(',',sort keys %$v)eq join(',',sort @keys) or _fail() }
sub _uint {
    my($v,$max,$positive)=@_; defined($v)&&!ref($v)&&(B::svref_2object(\$v)->FLAGS&B::SVp_IOK()) or _fail();
    my $s="$v"; $s=~/\A(?:0|[1-9][0-9]*)\z/ && (length($s)<length($max)||length($s)==length($max)&&$s le $max) && (!$positive||$s ne'0') or _fail(); return $v;
}
sub _str { my($v)=@_;defined($v)&&!ref($v)&&!(B::svref_2object(\$v)->FLAGS&(B::SVp_IOK()|B::SVp_NOK()))or _fail();return $v }
sub _bool { JSON::PP::is_bool($_[0])or _fail() }
sub _name { my($v)=@_;_str($v);length(encode('UTF-8',$v))>0&&length(encode('UTF-8',$v))<=63&&index($v,"\0")==-1 or _fail() }
sub _same { $json->encode($_[0])eq$json->encode($_[1])or _fail() }
sub _array { ref($_[0])eq'ARRAY'&&@{$_[0]}<=$_[1]or _fail() }
sub _nonce { _array($_[0],16);@{$_[0]}==16 or _fail();my $nonzero=0;for(@{$_[0]}){_uint($_,'255',0);$nonzero||=$_!=0}$nonzero or _fail() }
sub _source { _keys($_[0],qw(system_identifier database_oid));_uint($_[0]{system_identifier},$u64,1);_uint($_[0]{database_oid},'4294967295',1) }
sub _stream { _keys($_[0],qw(run source generation));_nonce($_[0]{run});_source($_[0]{source});_uint($_[0]{generation},$u64,1)==1 or _fail() }
sub _qualified { _keys($_[0],qw(schema table));_name($_[0]{schema});_name($_[0]{table}) }
sub _unique { my %seen; for(@{$_[0]}){_str($_);!$seen{$_}++ or _fail()} }

sub strict_json {
    my($raw,$cap)=@_;defined($raw)&&defined($cap)&&length($raw)<=$cap or _fail();
    decode('UTF-8',$raw,FB_CROAK|LEAVE_SRC);my $text=$raw;my $at=0;my $tokens=0;
    my $bytes_json=JSON::PP->new->utf8->allow_nonref;
    my $space=sub { pos($text)=$at;$text=~/\G[ \t\r\n]*/gc;$at=pos($text) };
    my $string=sub { substr($text,$at,1)eq'"'or _fail();my($s,$used)=$bytes_json->decode_prefix(substr($text,$at));_str($s);$at+=$used;return $s };
    my $value;$value=sub {
        my($depth)=@_;$depth<=32 && ++$tokens<=length($raw)+1 or _fail();$space->();my $ch=substr($text,$at,1);
        if($ch eq'{'){
            $at++;$space->();my %seen;if(substr($text,$at,1)eq'}'){$at++;return}
            while(1){$space->();my $key=$string->();!$seen{$key}++or _fail();$space->();substr($text,$at++,1)eq':'or _fail();$value->($depth+1);$space->();my $end=substr($text,$at++,1);last if $end eq'}';$end eq','or _fail()}
        }elsif($ch eq'['){
            $at++;$space->();if(substr($text,$at,1)eq']'){$at++;return}
            while(1){$value->($depth+1);$space->();my $end=substr($text,$at++,1);last if $end eq']';$end eq','or _fail()}
        }elsif($ch eq'"'){$string->()}
        elsif(substr($text,$at)=~/\A(true|false|null)/){$at+=length($1)}
        else{pos($text)=$at;$text=~/\G(-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/gc or _fail();my $lex=$1;$at=pos($text);$lex!~/[.eE]/ or _fail();my $negative=$lex=~s/\A-//;my $limit=$negative?'2147483648':$u64;length($lex)<length($limit)||length($lex)==length($limit)&&$lex le $limit or _fail()}
    };
    $value->(0);$space->();$at==length($text)or _fail();return JSON::PP->new->utf8->max_depth(32)->decode($text);
}

sub _operator {
    my($v,$work)=@_;_keys($v,qw(migration_uid run workdir_id checkpoint_generation));_nonce($v->{run});
    _str($v->{migration_uid});_str($v->{workdir_id});_uint($v->{checkpoint_generation},$u64,1)==1 or _fail();
    my $run=unpack('H*',pack('C*',@{$v->{run}}));my $uuid=$v->{migration_uid};
    $uuid=~/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/&&($uuid=~s/-//gr)eq$run or _fail();
    length($v->{workdir_id})>0&&length(encode('UTF-8',$v->{workdir_id}))<=256&&$v->{workdir_id}!~/[\x00-\x20\x7f\/\\]/ or _fail();
    _same({migration_uid=>$v->{migration_uid},run_id=>$run,workdir_id=>$v->{workdir_id},checkpoint_generation=>$v->{checkpoint_generation}},$work);
}
sub _slot {
    my($v)=@_;_keys($v,qw(intent consistent_point snapshot_token));_keys($v->{intent},qw(binding nonce));_nonce($v->{intent}{nonce});
    my $b=$v->{intent}{binding};_keys($b,qw(stream slot_name plugin catalog_epoch));_stream($b->{stream});_uint($b->{catalog_epoch},$u64,1);_name($b->{slot_name});$b->{slot_name}=~/\A[a-z0-9_]+\z/&&$b->{plugin}eq'pgoutput'or _fail();
    _uint($v->{consistent_point},$u64,1);_str($v->{snapshot_token});length(encode('UTF-8',$v->{snapshot_token}))<=1024&&length($v->{snapshot_token})>0&&index($v->{snapshot_token},"\0")==-1 or _fail();
}
sub _publication { _keys($_[0],qw(name oid owner_oid));_name($_[0]{name});_uint($_[0]{oid},'4294967295',1);_uint($_[0]{owner_oid},'4294967295',1) }
sub _follow_binding {
    my($v)=@_;_keys($v,qw(slot publication source_timeline copy_generation source_types));_slot($v->{slot});_publication($v->{publication});_uint($v->{source_timeline},'4294967295',1);_uint($v->{copy_generation},$u64,1);
    _array($v->{source_types},16384);my %types;for(@{$v->{source_types}}){_keys($_,qw(oid schema name));_uint($_->{oid},'4294967295',1);!$types{$_->{oid}}++or _fail();_name($_->{schema});_name($_->{name})}
    length($json->utf8->encode($v))<=262144 or _fail();$json->utf8(0);
    length($json->utf8->encode($v->{source_types}))+8192<=262144 or _fail();$json->utf8(0);
}
sub _table {
    my($v)=@_;_keys($v,qw(source_oid name writable_columns all_columns identity_modes partition_identity_modes));_uint($v->{source_oid},'4294967295',1);_qualified($v->{name});
    _array($v->{all_columns},1600);_unique($v->{all_columns});my %all;for(@{$v->{all_columns}}){_name($_);$all{$_}=1}
    _array($v->{writable_columns},1600);_unique($v->{writable_columns});for(@{$v->{writable_columns}}){$all{$_}or _fail()}
    _array($v->{identity_modes},1600);for my $modes($v->{identity_modes},$v->{partition_identity_modes}){next unless defined($modes);_array($modes,1600);@$modes==@{$v->{all_columns}}or _fail();for(@$modes){_uint($_,'255',0);($_==0||$_==97||$_==100)or _fail()}}
}
sub _plan {
    my($p)=@_;_keys($p,qw(source target generation tables sequences policy));_keys($p->{source},qw(identity timeline major));_keys($p->{target},qw(identity major));_source($p->{source}{identity});_source($p->{target}{identity});_uint($p->{source}{timeline},'4294967295',1);_uint($p->{generation},$u64,1);
    for($p->{source}{major},$p->{target}{major}){_uint($_,'18',1);($_==16||$_==18)or _fail()}$p->{source}{major}<=$p->{target}{major}or _fail();
    _array($p->{tables},4096);my(%oids,%names);for(@{$p->{tables}}){_table($_);!$oids{$_->{source_oid}}++&&!$names{$json->encode($_->{name})}++or _fail()}
    ref($p->{sequences})eq'ARRAY'or _fail();_qualified($_)for @{$p->{sequences}};
    _keys($p->{policy},qw(no_owner no_acl no_comments no_tablespaces));_bool($_)for values %{$p->{policy}};
}
sub _clone {
    my($v,$metadata_schema)=@_;_keys($v,qw(plan ticket phase owned_objects owned_tables completion));_plan($v->{plan});$v->{phase}eq'analyzing'&&!defined($v->{completion})or _fail();
    _keys($v->{ticket},qw(token source generation));_same($v->{ticket}{source},$v->{plan}{source});_same($v->{ticket}{generation},$v->{plan}{generation});_str($v->{ticket}{token});length($v->{ticket}{token})>0&&length(encode('UTF-8',$v->{ticket}{token}))<=1024&&index($v->{ticket}{token},"\0")==-1 or _fail();
    ref($v->{owned_objects})eq'ARRAY'&&ref($v->{owned_tables})eq'ARRAY'&&@{$v->{owned_tables}}==@{$v->{plan}{tables}}or _fail();my(%objects,%targets,%sources);
    for my $o(@{$v->{owned_objects}}){_keys($o,qw(class_oid object_oid object_sub_id namespace_oid namespace_name name kind owner_oid));_uint($o->{class_oid},'4294967295',1);_uint($o->{object_oid},'4294967295',1);_uint($o->{object_sub_id},'2147483647',0);_uint($o->{owner_oid},'4294967295',1);_str($o->{name});length($o->{name})>0 or _fail();_str($o->{kind});if(defined($o->{namespace_oid})){_uint($o->{namespace_oid},'4294967295',1);$o->{namespace_oid}!=$metadata_schema or _fail()}_str($o->{namespace_name})if defined($o->{namespace_name});$o->{class_oid}!=2615||$o->{object_oid}!=$metadata_schema or _fail();!$objects{join(':',@$o{qw(class_oid object_oid object_sub_id)})}++or _fail()}
    for my $o(@{$v->{owned_tables}}){_keys($o,qw(table target_oid target_namespace_oid target_owner_oid generation));_table($o->{table});_uint($o->{target_oid},'4294967295',1);_uint($o->{target_namespace_oid},'4294967295',1);_uint($o->{target_owner_oid},'4294967295',1);_same($o->{generation},$v->{plan}{generation});!$targets{$o->{target_oid}}++&&!$sources{$o->{table}{source_oid}}++or _fail();grep{$json->encode($_)eq$json->encode($o->{table})}@{$v->{plan}{tables}}or _fail();grep{$_->{class_oid}==1259&&$_->{object_oid}==$o->{target_oid}&&$_->{object_sub_id}==0&&defined($_->{namespace_oid})&&$_->{namespace_oid}==$o->{target_namespace_oid}&&defined($_->{namespace_name})&&$_->{namespace_name}eq$o->{table}{name}{schema}&&$_->{name}eq$o->{table}{name}{table}&&$_->{kind}eq'r'&&$_->{owner_oid}==$o->{target_owner_oid}}@{$v->{owned_objects}}or _fail()}
    length($json->utf8->encode($v))<=1048576-4096 or _fail();$json->utf8(0);
}

sub _manifest {
    my($m,$caller,$root)=@_;_keys($m,qw(magic format_version run canonical_path directory_device directory_inode copy_complete target_provenance source_slot clone_state follow_state operator));
    _str($m->{magic});$m->{magic}eq'RPGMIGRATE'or _fail();_uint($m->{format_version},'8',0)==8 or _fail();_nonce($m->{run});_operator($m->{operator},$caller->{work});_same($m->{run},$m->{operator}{run});
    _str($m->{canonical_path});$m->{canonical_path}eq'/work/pgcopydb'or _fail();my @s=stat($root);_uint($m->{directory_device},$u64,0)==$s[0]&&_uint($m->{directory_inode},$u64,0)==$s[1]or _fail();
    if(defined($m->{copy_complete})){_keys($m->{copy_complete},qw(tables rows bytes));_uint($_,$u64,0)for values %{$m->{copy_complete}}}
    _array($m->{target_provenance},1);@{$m->{target_provenance}}==1 or _fail();my $tag=$m->{target_provenance}[0];_keys($tag,qw(phase record));$tag->{phase}eq'confirmed'or _fail();my $t=$tag->{record};_keys($t,qw(intent schema_oid table_oid marker_table_oid marker_index_oid));_keys($t->{intent},qw(binding nonce));_nonce($t->{intent}{nonce});_same($t->{intent}{nonce},$caller->{target}{nonce});
    my $b=$t->{intent}{binding};_keys($b,qw(stream target_system_identifier target_database_oid schema_name table_name owner_oid metadata_version));_stream($b->{stream});_same($b->{stream}{run},$m->{run});_uint($b->{metadata_version},'6',1)==6 or _fail();_name($b->{schema_name});_name($b->{table_name});$b->{schema_name}eq'_rpgmigrate_'.$caller->{work}{run_id}&&$b->{table_name}eq'checkpoint'or _fail();
    for(qw(schema_oid table_oid marker_table_oid marker_index_oid)){_uint($t->{$_},'4294967295',1);_same($t->{$_},$caller->{target}{$_})}_same($b->{owner_oid},$caller->{target}{owner_oid});_uint($b->{owner_oid},'4294967295',1);_uint($b->{target_system_identifier},$u64,1);_uint($b->{target_database_oid},'4294967295',1);_same($b->{target_system_identifier},$caller->{target}{system_identifier});_same($b->{target_database_oid},$caller->{target}{database_oid});
    my %identity;!$identity{$_}++or _fail()for @$t{qw(table_oid marker_table_oid marker_index_oid)};
    _keys($m->{source_slot},qw(phase record));$m->{source_slot}{phase}eq'confirmed'or _fail();_slot($m->{source_slot}{record});_same($m->{source_slot}{record}{intent}{binding}{stream},$b->{stream});
    _clone($m->{clone_state},$t->{schema_oid});my $p=$m->{clone_state}{plan};_same($p->{source}{identity},$b->{stream}{source});_same($p->{target}{identity},{system_identifier=>$b->{target_system_identifier},database_oid=>$b->{target_database_oid}});_same($p->{target}{major},$caller->{target}{major});_same($p->{source},{identity=>{system_identifier=>$caller->{source}{system_identifier},database_oid=>$caller->{source}{database_oid}},timeline=>$caller->{source}{timeline},major=>$caller->{source}{major}});
    my $f=$m->{follow_state};_keys($f,qw(cleanup origin_name publication binding restart preliminary));!defined($f->{restart})&&!defined($f->{preliminary})or _fail();_name($f->{origin_name})if defined($f->{origin_name});if(defined($f->{cleanup})){_keys($f->{cleanup},qw(slot_removed publication_removed));_bool($_)for values %{$f->{cleanup}}}
    _follow_binding($f->{binding});_same($f->{binding}{slot},$m->{source_slot}{record});_same($f->{binding}{copy_generation},$p->{generation});_same($f->{binding}{source_timeline},$p->{source}{timeline});_same($m->{clone_state}{ticket}{token},$f->{binding}{slot}{snapshot_token});
    _keys($f->{publication},qw(phase intent receipt));$f->{publication}{phase}eq'confirmed'or _fail();_keys($f->{publication}{intent},qw(run name nonce));_nonce($f->{publication}{intent}{nonce});_same($f->{publication}{intent}{run},$m->{run});_same($f->{publication}{intent}{name},$f->{binding}{publication}{name});_same($f->{publication}{receipt},$f->{binding}{publication});
    length($json->utf8->encode({binding=>$f->{binding},operator=>$m->{operator},origin_name=>$f->{origin_name}}))<=262144 or _fail();$json->utf8(0);
    return $t;
}
sub _control {
    my($e,$m,$caller,$target)=@_;_keys($e,qw(version stream copy_generation slot_nonce target_nonce end_lsn sealed));_uint($e->{version},'1',1)==1 or _fail();_same($e->{stream},$target->{intent}{binding}{stream});_same($e->{copy_generation},$m->{clone_state}{plan}{generation});_same($e->{slot_nonce},$m->{source_slot}{record}{intent}{nonce});_same($e->{target_nonce},$target->{intent}{nonce});_uint($e->{end_lsn},$u64,1);_same($e->{end_lsn},$caller->{end_lsn});_bool($e->{sealed});$e->{sealed}or _fail();
}
sub _open {
    my($path,$directory)=@_;sysopen(my $h,$path,O_RDONLY|O_NOFOLLOW|O_NONBLOCK|($directory?O_DIRECTORY:0))or _fail();my @s=stat($h);@s&&$s[4]==$>&&($s[2]&07777)==($directory?0700:0600)&&($directory?S_ISDIR($s[2]):S_ISREG($s[2])&&$s[3]==1)or _fail();return $h;
}
sub _id { my @s=stat($_[0]);@s or _fail();return join(':',@s[0,1]) }
sub _entries {my($h,@names)=@_;opendir(my $d,'/proc/self/fd/'.fileno($h))or _fail();my @found;while(defined(my $e=readdir($d))){next if $e eq'.'||$e eq'..';push @found,$e;@found<=@names or _fail()}closedir($d)or _fail();join(',',sort @found)eq join(',',sort @names)or _fail()}
sub _bytes {my($h,$cap)=@_;my $raw='';while(1){my $n=sysread($h,my $part,8192);defined($n)or _fail();last unless $n;$raw.=$part;length($raw)<=$cap or _fail()}return $raw}
sub _read {
    my($parent,$name,$cap,$hook)=@_;my $path='/proc/self/fd/'.fileno($parent).'/'.$name;my $h=_open($path,0);my @before=stat($h);$before[7]<=$cap or _fail();my $raw=_bytes($h,$cap);$hook->()if $hook;_id($h)eq _id(_open($path,0))or _fail();sysseek($h,0,0)==0 or _fail();_bytes($h,$cap)eq$raw or _fail();my @after=stat($h);join(':',@before[0,1,2,3,4,5,7,9,10])eq join(':',@after[0,1,2,3,4,5,7,9,10])or _fail();return($raw,$h);
}
sub admit {
    my($caller,$execution,$hook)=@_;
    _keys($caller,qw(work helper source target end_lsn));
    _keys($caller->{helper},qw(migration_uid job_uid pod_uid pvc_uid attempt container endpoint_binding job_controller_uid pod_controller_uid));
    _keys($execution,qw(migration_uid job_uid pod_uid pvc_uid attempt container endpoint_binding job_controller_uid pod_controller_uid));
    _same($caller->{helper},$execution);
    for(qw(migration_uid job_uid pod_uid pvc_uid container endpoint_binding job_controller_uid pod_controller_uid)){_str($execution->{$_});length($execution->{$_})>0&&length(encode('UTF-8',$execution->{$_}))<=512&&$execution->{$_}!~/[\x00-\x20\x7f]/ or _fail()}
    _uint($execution->{attempt},'2147483647',1);
    $execution->{container}eq'pgcopydb'&&$execution->{migration_uid}eq$caller->{work}{migration_uid}&&$execution->{job_controller_uid}eq$caller->{work}{migration_uid}&&$execution->{pod_controller_uid}eq$execution->{job_uid}&&$execution->{pvc_uid}eq$caller->{work}{workdir_id}or _fail();
    _keys($caller->{source},qw(system_identifier database_oid timeline major));
    _keys($caller->{target},qw(system_identifier database_oid major owner_oid schema_oid table_oid marker_table_oid marker_index_oid nonce));
    _uint($caller->{end_lsn},$u64,1);
    my $parent=_open('/work',1);my $root=_open('/proc/self/fd/'.fileno($parent).'/pgcopydb',1);flock($root,LOCK_EX|LOCK_NB)or _fail();_entries($root,'manifest.json','control');
    my $control=_open('/proc/self/fd/'.fileno($root).'/control',1);_entries($control,'lock','endpos.json');my $lock=_open('/proc/self/fd/'.fileno($control).'/lock',0);flock($lock,LOCK_EX|LOCK_NB)or _fail();
    my($raw,$manifest_fd)=_read($root,'manifest.json',1048576,$hook);my($end,$end_fd)=_read($control,'endpos.json',4096);my $m=strict_json($raw,1048576);my $target=_manifest($m,$caller,$root);my $e=strict_json($end,4096);_control($e,$m,$caller,$target);
    _id($parent)eq _id(_open('/work',1))&&_id($root)eq _id(_open('/work/pgcopydb',1))&&_id($control)eq _id(_open('/proc/self/fd/'.fileno($root).'/control',1))&&_id($lock)eq _id(_open('/proc/self/fd/'.fileno($control).'/lock',0))or _fail();_entries($root,'manifest.json','control');_entries($control,'lock','endpos.json');
    flock($lock,LOCK_UN)or _fail();
    my $control_check=_open('/proc/self/fd/'.fileno($control).'/lock',0);flock($control_check,LOCK_EX|LOCK_NB)or _fail();flock($control_check,LOCK_UN)or _fail();
    my $owner_check=_open('/work/pgcopydb',1);!flock($owner_check,LOCK_EX|LOCK_NB)or _fail();
    return {manifest=>$m,endpos=>$e,parent=>$parent,root=>$root,control=>$control,manifest_fd=>$manifest_fd,end_fd=>$end_fd,manifest_bytes=>$raw,endpos_bytes=>$end,lock=>$lock,lifetime_held=>1,control_released=>1};
}
sub _decimal {
    my($s,$max,$positive)=@_;_str($s);$s=~/\A(?:0|[1-9][0-9]*)\z/ or _fail();my $v=$json->decode($s);_uint($v,$max,$positive);return $v;
}
sub _lsn {
    my($s)=@_;_str($s);$s=~/\A([0-9a-fA-F]{1,8})\/([0-9a-fA-F]{1,8})\z/ or _fail();return(hex($1)<<32)|hex($2);
}
sub _lsn_text {sprintf('%X/%X',$_[0]>>32,$_[0]&0xffffffff)}
sub validate_control_row {
    my($row,$admitted,$caller)=@_;_array($row,29);@$row==29 or _fail();_str($_)for grep{defined($_)}@$row;
    my $m=$admitted->{manifest};my $target=$m->{target_provenance}[0]{record};my $b=$target->{intent}{binding};
    my @identity=('rpgmigrate','6',unpack('H*',pack('C*',@{$m->{run}})),unpack('H*',pack('C*',@{$target->{intent}{nonce}})),map{"$_"}($b->{stream}{source}{system_identifier},$b->{stream}{source}{database_oid},$b->{stream}{generation},$b->{target_system_identifier},$b->{target_database_oid},$b->{owner_oid},$target->{schema_oid},$target->{table_oid}));
    for(0..11){defined($row->[$_])&&$row->[$_]eq$identity[$_] or _fail()}
    my $order=_decimal($row->[12],$u64,0);my $applied=0;
    if(!$order){!defined($_)or _fail()for @$row[13..16]}
    else{_decimal($row->[13],'4294967295',0);my $begin=_decimal($row->[14],$u64,1);my $commit=_lsn($row->[15]);$applied=_lsn($row->[16]);$commit>0&&$commit==$begin&&$commit<=$applied or _fail()}
    my $epoch=_decimal($row->[17],$u64,0);my $counter=_decimal($row->[18],$u64,1);
    defined($row->[19])&&defined($row->[20])&&defined($row->[21]) or _fail();
    my $slot=$m->{source_slot}{record};$row->[19]eq$slot->{snapshot_token}&&length(encode('UTF-8',$row->[19]))<=1024&&index($row->[19],"\0")==-1 or _fail();
    my $anchor=_lsn($row->[20]);$anchor>0&&$anchor==$slot->{consistent_point}&&$row->[21]eq unpack('H*',pack('C*',@{$slot->{intent}{nonce}})) or _fail();
    my $clone=strict_json(encode('UTF-8',$row->[22]),4096);_keys($clone,qw(generation phase snapshot_token source_timeline table_count));
    my $p=$m->{clone_state}{plan};_same($clone,{generation=>$p->{generation},phase=>'analyzing',snapshot_token=>$m->{clone_state}{ticket}{token},source_timeline=>$p->{source}{timeline},table_count=>scalar(@{$p->{tables}})});
    my $certified=defined($row->[23])?_lsn($row->[23]):0;!defined($row->[23])||$certified>0 or _fail();
    my $follow=strict_json(encode('UTF-8',$row->[24]),262144);_keys($follow,qw(binding operator origin_name));
    my $f=$m->{follow_state};_same($follow,{binding=>$f->{binding},operator=>$m->{operator},origin_name=>$f->{origin_name}});
    defined($row->[25])&&defined($row->[26])&&defined($row->[27]) or _fail();my $end=_lsn($row->[25]);my $witness=_lsn($row->[26]);
    $end>0&&$end==$caller->{end_lsn}&&$anchor<=$end&&$witness>=$end&&($row->[27]eq'keepalive'||$row->[27]eq'later_begin') or _fail();
    ($row->[27]eq'keepalive'?$certified>=$witness:$witness>$end) or _fail();
    $row->[28]eq't' or _fail();my $durable=$applied;for($anchor,$certified){$durable=$_ if $_>$durable}
    return {token=>'1:'.$epoch.':'.$counter,certified=>_lsn_text($certified),durable=>_lsn_text($durable),end=>_lsn_text($end),witness=>_lsn_text($witness),kind=>$row->[27]};
}
sub _fence {
    my($run)=@_;my $crc=0xffffffff;for my $byte(@$run){$crc^=$byte;for(1..8){$crc=($crc>>1)^(($crc&1)?0xedb88320:0)}}$crc^=0xffffffff;return($crc,$crc>=0x80000000?$crc-4294967296:$crc);
}
sub _file {
    my($name)=@_;$name=~/\Anative_(?:catalog|marker_catalog)_(?:16|18)\.sql\z/||$name eq'native_columns.json' or _fail();open(my $in,'<','/probe/'.$name)or _fail();my $bytes=_bytes($in,32768);close($in)or _fail();return $bytes;
}
sub _hex_text {my($s)=@_;return "pg_catalog.convert_from(pg_catalog.decode('".unpack('H*',encode('UTF-8',$s))."','hex'),'UTF8')"}
sub _sql_literal {
    my($cell,$column)=@_;return 'NULL::pg_catalog.'.$column->{type}unless defined($cell);
    return "pg_catalog.decode('$cell','hex')"if $column->{type}eq'bytea';return _hex_text($cell).'::pg_catalog.'.$column->{type};
}
sub _columns {strict_json(_file('native_columns.json'),32768)->{checkpoint}}
sub _row_select {
    my($table,$columns)=@_;my @fields;
    for my $column(@$columns){my $n=$column->{name};my $expr=$n;
        if($n eq'magic'){$expr="CASE WHEN pg_catalog.octet_length(magic)<64 THEN magic END"}
        elsif($n eq'run_id'||$n eq'nonce'){$expr="CASE WHEN pg_catalog.octet_length($n)=16 THEN pg_catalog.encode($n,'hex') END"}
        elsif($n eq'follow_binding'||$n eq'clone_state'||$n eq'copy_snapshot_token'||$n eq'drain_kind'){my $cap=$n eq'follow_binding'?262144:$n eq'clone_state'?4096:$n eq'copy_snapshot_token'?1024:11;$expr="CASE WHEN $n IS NULL THEN NULL WHEN pg_catalog.octet_length($n)<=$cap THEN $n ELSE '' END"}
        elsif($n eq'copy_plan_nonce'){$expr="CASE WHEN $n IS NULL THEN NULL WHEN pg_catalog.octet_length($n)=16 THEN pg_catalog.encode($n,'hex') ELSE '' END"}
        my $text=$n eq'follow_completed'?"CASE WHEN follow_completed THEN 't' ELSE 'f' END":"($expr)::pg_catalog.text";
        push @fields,"CASE WHEN ($expr) IS NULL THEN '~' ELSE pg_catalog.encode(pg_catalog.convert_to(($text),'UTF8'),'hex') END";
    }
    return 'SELECT '.join(',',@fields).' FROM ONLY '.$table.' LIMIT 2 FOR UPDATE;';
}
sub _row_output {
    my($data)=@_;my @lines=split(/\n/,$data,-1);@lines==2&&$lines[1]eq''or _fail();my @cells=split(/\|/,$lines[0],-1);@cells==29 or _fail();my @decoded;
    for(@cells){if($_ eq'~'){push @decoded,undef;next}$_=~/\A(?:[0-9a-f]{2})*\z/ or _fail();push @decoded,decode('UTF-8',pack('H*',$_),FB_CROAK|LEAVE_SRC)}return \@decoded;
}
sub _row_cap {
    my($columns)=@_;my %text=(magic=>63,clone_state=>4096,follow_binding=>262144,copy_snapshot_token=>1024,drain_kind=>11);my $sum=29;
    for(@$columns){my $n=$_->{name};my $cap=exists($text{$n})?$text{$n}:$_->{type}eq'bytea'?32:$_->{type}eq'oid'?10:$_->{type}eq'pg_lsn'?17:$_->{type}eq'bool'?1:20;$sum+=2*$cap}
    return 2*$sum+256;
}
sub revalidate {
    my($a,$caller)=@_;_id($a->{root})eq _id(_open('/work/pgcopydb',1))&&_id($a->{parent})eq _id(_open('/work',1))or _fail();
    _id($a->{control})eq _id(_open('/proc/self/fd/'.fileno($a->{root}).'/control',1))&&_id($a->{lock})eq _id(_open('/proc/self/fd/'.fileno($a->{control}).'/lock',0))or _fail();flock($a->{lock},LOCK_EX|LOCK_NB)or _fail();
    my $ok=eval{_entries($a->{root},'manifest.json','control');_entries($a->{control},'lock','endpos.json');my($raw,$fd)=_read($a->{root},'manifest.json',1048576);my($end,$efd)=_read($a->{control},'endpos.json',4096);_id($fd)eq _id($a->{manifest_fd})&&_id($efd)eq _id($a->{end_fd})or _fail();$raw eq$a->{manifest_bytes}&&$end eq$a->{endpos_bytes}or _fail();1};
    flock($a->{lock},LOCK_UN)or _fail();$ok or _fail();return 1;
}
sub _phase {my($session,$sql,$state,$check)=@_;$session->phase($sql,$state,$check)or _fail()}
sub _source_observation {
    my($caller,$trace)=@_;require PSQLEvidencePrototype;my $s=PSQLEvidencePrototype->new(4096,'source');my $ok=eval{
        _phase($s,'SET search_path TO pg_catalog;','',sub{$_[0]eq"SET\n"});
        _phase($s,q{SELECT c.system_identifier::pg_catalog.text,d.oid,k.timeline_id,NOT pg_catalog.pg_is_in_recovery(),pg_catalog.pg_backend_pid(),pg_catalog.current_setting('server_version_num') FROM pg_catalog.pg_control_system() c CROSS JOIN pg_catalog.pg_control_checkpoint() k CROSS JOIN pg_catalog.pg_database d WHERE d.datname=pg_catalog.current_database();},'',sub{
            my @v=split(/\|/,$_[0],-1);@v==6 or _fail();$v[5]=~s/\n\z// or _fail();my $e=$caller->{source};my $major=int(_decimal($v[5],'189999',1)/10000);$v[0]eq"$e->{system_identifier}"&&$v[1]eq"$e->{database_oid}"&&$v[2]eq"$e->{timeline}"&&$v[3]eq't'&&$v[4]eq$s->{backend}&&($major==16||$major==18)&&$major==$e->{major};
        });1;
    };$s->{failed}=1 unless $ok;my $closed=$s->finish();$trace->{sqlstate}=$s->{sqlstate}//'';$trace->{failure}=$s->{failure}//'validation';$ok&&$closed or _fail();
}
sub observe {
    my($admitted,$caller,$options)=@_;$options//={};$options->{trace}//={};$options->{trace}{stages}=[];@{$options->{trace}}{qw(source_closed exit signal stderr_bytes)}=(0,-1,0,0);delete @{$options->{trace}}{qw(failure sqlstate)};$options->{trace}{stage}='source_identity';require PSQLEvidencePrototype;_source_observation($caller,$options->{trace});$options->{trace}{source_closed}=1;$options->{trace}{stage}='target_open';my $columns=_columns();@$columns==29 or _fail();my $s=PSQLEvidencePrototype->new(_row_cap($columns),'target');
    my($row,$positions,$major,$role,$session_role,$required);my $target=$admitted->{manifest}{target_provenance}[0]{record};my $b=$target->{intent}{binding};my($slot_oid,$slot)=_fence($b->{stream}{run});my $fence="1380992845,$slot";
    my $table='"'.$b->{schema_name}.'"."checkpoint"';my $marker='"'.$b->{schema_name}.'"."copy_done"';
    my $guard=sub{my($held)=@_;my $present=$held?'EXISTS':'NOT EXISTS';return "SELECT pg_catalog.pg_backend_pid()=$s->{backend} AND (SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())=$b->{target_database_oid}::oid AND (SELECT oid FROM pg_catalog.pg_roles WHERE rolname=CURRENT_USER)=${role}::oid AND (SELECT oid FROM pg_catalog.pg_roles WHERE rolname=SESSION_USER)=${session_role}::oid AND (SELECT system_identifier FROM pg_catalog.pg_control_system())=$b->{target_system_identifier}::numeric AND NOT pg_catalog.pg_is_in_recovery() AND pg_catalog.current_setting('transaction_read_only')='off' AND pg_catalog.current_setting('fsync')='on' AND pg_catalog.current_setting('full_page_writes')='on' AND pg_catalog.current_setting('search_path')='pg_catalog' AND pg_catalog.current_setting('synchronous_commit')='$required' AND $present (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='advisory' AND database=$b->{target_database_oid}::oid AND pid=$s->{backend} AND classid=1380992845::oid AND objid=${slot_oid}::oid AND objsubid=2 AND mode='ExclusiveLock' AND granted);"};
    my $step=sub{my($label,@args)=@_;$options->{trace}{stage}=$label;_phase($s,@args);push @{$options->{trace}{stages}},$label};
    my $ok=eval{
        $step->('target_settings',"SET search_path TO pg_catalog; SET standard_conforming_strings TO on; SET statement_timeout TO '4s'; SET lock_timeout TO '1s'; SET idle_in_transaction_session_timeout TO '6s';",'',sub{$_[0]eq("SET\n"x5)});
        $step->('target_identity',q{SELECT c.system_identifier::pg_catalog.text,d.oid,r.oid,u.oid,pg_catalog.pg_backend_pid(),current_setting('server_version_num'),NOT pg_catalog.pg_is_in_recovery() AND current_setting('transaction_read_only')='off' AND current_setting('fsync')='on' AND current_setting('full_page_writes')='on' FROM pg_catalog.pg_control_system() c CROSS JOIN pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_roles r CROSS JOIN pg_catalog.pg_roles u WHERE d.datname=pg_catalog.current_database() AND r.rolname=CURRENT_USER AND u.rolname=SESSION_USER;},'',sub{
            my @v=split(/\|/,$_[0],-1);@v==7&&$v[6]eq"t\n"or _fail();$v[0]eq"$b->{target_system_identifier}"&&$v[1]eq"$b->{target_database_oid}"&&$v[4]eq$s->{backend}or _fail();$role=_decimal($v[2],'4294967295',1);$session_role=_decimal($v[3],'4294967295',1);$major=int(_decimal($v[5],'189999',1)/10000);($major==16||$major==18)&&$major==$caller->{target}{major};
        });
        $step->('configure_durability',q{SELECT pg_catalog.set_config('synchronous_commit',CASE WHEN current_setting('synchronous_commit')='remote_apply' THEN 'remote_apply' WHEN current_setting('synchronous_commit') IN('off','local','remote_write','on') THEN 'on' ELSE NULL END,false);},'',sub{$_[0]=~/\A(on|remote_apply)\n\z/ or _fail();$required=$1;1});
        $step->('target_fence',"SELECT pg_catalog.pg_try_advisory_lock($fence);",'',sub{$_[0]eq"t\n"});
        $step->('idle_guard',$guard->(1),'',sub{$_[0]eq"t\n"&&revalidate($admitted,$caller)});
        $step->('begin','BEGIN ISOLATION LEVEL READ COMMITTED;','*',sub{$_[0]eq"BEGIN\n"});
        $step->('lock_checkpoint',"LOCK TABLE ONLY $table IN SHARE ROW EXCLUSIVE MODE;",'*',sub{$_[0]eq"LOCK TABLE\n"});
        $step->('lock_marker',"LOCK TABLE ONLY $marker IN SHARE ROW EXCLUSIVE MODE;",'*',sub{$_[0]eq"LOCK TABLE\n"});
        for my $kind('catalog','marker_catalog'){
            my $sql=_file("native_${kind}_${major}.sql");$sql=~s/^--[^\n]*\n//mg;$sql=~s/\n/ /g;my @params=$kind eq'catalog'?($target->{schema_oid},$target->{table_oid},$b->{target_database_oid},$b->{owner_oid},_hex_text($b->{schema_name}),_hex_text($b->{table_name}),$target->{marker_table_oid},$target->{marker_index_oid}):($target->{marker_table_oid},$target->{marker_index_oid},$target->{schema_oid},$b->{owner_oid});
            $sql=~s/\$(\d+)\b/$params[$1-1]/ge;$step->($kind eq'catalog'?'catalog_checkpoint':'catalog_marker',$sql,'*',sub{$_[0]eq"t\n"});
        }
        $step->('row_read',_row_select($table,$columns),'*',sub{$row=_row_output($_[0]);$positions=validate_control_row($row,$admitted,$caller);1});
        $options->{before_update}->($s,$row)if $options->{before_update};
        my @assign;my @where;for my $i(0..28){my $c=$columns->[$i];my $value=_sql_literal($row->[$i],$c);push @assign,$c->{name}.'='.$value;push @where,$c->{name}.' IS NOT DISTINCT FROM '.$value}
        $step->('same_value_update','UPDATE ONLY '.$table.' SET '.join(',',@assign).' WHERE '.join(' AND ',@where).';','*',sub{$_[0]eq"UPDATE 1\n"});
        $step->('deferred_effects','SET CONSTRAINTS ALL IMMEDIATE;','*',sub{$_[0]eq"SET CONSTRAINTS\n"});
        $step->('row_comparison',_row_select($table,$columns),'*',sub{_same(_row_output($_[0]),$row);1});
        $step->('precommit_guard',$guard->(1),'*',sub{$_[0]eq"t\n"&&revalidate($admitted,$caller)});
        $options->{before_commit}->($s)if $options->{before_commit};
        $step->('commit','COMMIT;','',sub{$_[0]eq"COMMIT\n"});
        $step->('idle_guard',$guard->(1),'',sub{$_[0]eq"t\n"&&revalidate($admitted,$caller)});
        $step->('unlock',"SELECT pg_catalog.pg_advisory_unlock($fence);",'',sub{$_[0]eq"t\n"});
        $step->('closure_guard',$guard->(0),'',sub{$_[0]eq"t\n"});1;
    };
    $s->{failed}=1 unless $ok;my $closed=$s->finish();$options->{trace}{exit}=$s->{exit};$options->{trace}{signal}=$s->{signal};$options->{trace}{stderr_bytes}=length($s->{errors});$options->{trace}{sqlstate}=$s->{sqlstate}//'';$options->{trace}{failure}=$s->{failure}//($ok?'closure':'validation');return undef unless $ok&&$closed;$options->{trace}{stage}='closed';revalidate($admitted,$caller);return {positions=>$positions,server_major=>$major,phases=>$s->{sequence},all_eof=>1,child_exit=>0};
}
1;

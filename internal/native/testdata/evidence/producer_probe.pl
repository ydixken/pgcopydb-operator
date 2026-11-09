use strict;
use warnings;
use lib '/probe';
use EvidencePrototype;
use JSON::PP ();
use Digest::SHA qw(sha256_hex);
use Fcntl qw(:DEFAULT :flock);
use IO::Handle ();

my $json=JSON::PP->new->utf8->canonical;
my $mode=shift // '';
my $caller_raw=shift // '';
my $execution_raw=$mode eq 'persist_goal' ? '' : shift // '';
my $operation_id=$mode eq 'observe' ? shift // '' : '';
my $root_id=shift // '';
my $trace={stages=>[],stage=>'admission',source_closed=>0,exit=>-1,signal=>0,stderr_bytes=>0};
my $receipt={operation_id=>$operation_id,caller_sha256=>sha256_hex($caller_raw),execution_sha256=>sha256_hex($execution_raw),helper_euid=>0+$>,source_major=>0,target_major=>0,admitted=>JSON::PP::false,observed=>JSON::PP::false,stages=>[],stage=>'admission',source_closed=>JSON::PP::false,all_eof=>JSON::PP::false,exit=>-1,signal=>0,stderr_bytes=>0,mocked_kubernetes_owner=>JSON::PP::true,production_admission=>JSON::PP::false};
my $refuse=sub{die "producer_probe_refused\n"};
my $root=sub{
    $root_id=~/\A(?:0|[1-9][0-9]*):[1-9][0-9]*\z/ or $refuse->();
    my $parent=EvidencePrototype::_open('/work',1);
    EvidencePrototype::_id($parent)eq$root_id or $refuse->();
    return $parent;
};
my $validate_goal=sub{
    my($value)=@_;
    EvidencePrototype::_keys($value,qw(work end_lsn));
    EvidencePrototype::_keys($value->{work},qw(migration_uid run_id workdir_id checkpoint_generation));
    EvidencePrototype::_uint($value->{end_lsn},'18446744073709551615',1);
    my $work=$value->{work};
    EvidencePrototype::_str($work->{run_id});$work->{run_id}=~/\A[0-9a-f]{32}\z/ or $refuse->();
    EvidencePrototype::_operator({migration_uid=>$work->{migration_uid},run=>[unpack('C*',pack('H*',$work->{run_id}))],workdir_id=>$work->{workdir_id},checkpoint_generation=>$work->{checkpoint_generation}},$work);
};
my $goal=sub{
    my($parent)=@_;
    my($bytes,$fd)=EvidencePrototype::_read($parent,'producer-goal.json',4096);
    my $value=EvidencePrototype::strict_json($bytes,4096);
    $validate_goal->($value);
    return($value,$bytes,$fd);
};
my $output;
$SIG{ALRM}=sub{$refuse->()};alarm 28;
my $ok=eval{
    $>==65532&&@ARGV==0&&length($caller_raw)<=4096&&length($execution_raw)<=4096&&($mode eq 'persist_goal'||$mode eq 'capture_caller'||$mode eq 'observe') or $refuse->();
    my $parent=$root->();
    my $caller=EvidencePrototype::strict_json($caller_raw,4096);
    if($mode eq 'persist_goal'){
        $validate_goal->($caller);
        my $bytes=$caller_raw;
        length($bytes)<=4096 or $refuse->();
        my $path='/proc/self/fd/'.fileno($parent).'/producer-goal.json';
        local $!;local $SIG{__WARN__}=sub{$refuse->()};umask 0077;my $created=sysopen(my $fd,$path,O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW,0600);
        if($created){
            my $at=0;while($at<length($bytes)){my $n=syswrite($fd,$bytes,length($bytes)-$at,$at);defined($n)&&$n>0 or $refuse->();$at+=$n}
            $fd->sync or $refuse->();close($fd)or $refuse->();$parent->sync or $refuse->();
        }else{$!{EEXIST}or $refuse->()}
        my($saved,$readback,$held)=$goal->($parent);
        $readback eq$bytes or $refuse->();EvidencePrototype::_same($saved,$caller);
        $held->sync or $refuse->();$parent->sync or $refuse->();
        EvidencePrototype::_id($parent)eq EvidencePrototype::_id($root->())or $refuse->();
        $output={persisted=>JSON::PP::true,goal_sha256=>sha256_hex($readback)};
    }elsif($mode eq 'capture_caller'){
        EvidencePrototype::_keys($caller,qw(work helper source target end_lsn));
        EvidencePrototype::_keys($caller->{target},qw(system_identifier database_oid major));
        my $execution=EvidencePrototype::strict_json($execution_raw,4096);
        my($saved,$goal_bytes,$goal_fd)=$goal->($parent);
        EvidencePrototype::_same($saved,{work=>$caller->{work},end_lsn=>$caller->{end_lsn}});
        my $native=EvidencePrototype::_open('/proc/self/fd/'.fileno($parent).'/pgcopydb',1);
        flock($native,LOCK_EX|LOCK_NB)or $refuse->();
        my($manifest_bytes,$manifest_fd)=EvidencePrototype::_read($native,'manifest.json',1048576);
        my $manifest=EvidencePrototype::strict_json($manifest_bytes,1048576);
        ref($manifest->{target_provenance})eq'ARRAY'&&@{$manifest->{target_provenance}}==1 or $refuse->();
        my $tag=$manifest->{target_provenance}[0];
        EvidencePrototype::_keys($tag,qw(phase record));$tag->{phase}eq'confirmed'or $refuse->();
        my $record=$tag->{record};EvidencePrototype::_keys($record,qw(intent schema_oid table_oid marker_table_oid marker_index_oid));
        EvidencePrototype::_keys($record->{intent},qw(binding nonce));
        my %target=%{$caller->{target}};
        @target{qw(owner_oid schema_oid table_oid marker_table_oid marker_index_oid nonce)}=($record->{intent}{binding}{owner_oid},@$record{qw(schema_oid table_oid marker_table_oid marker_index_oid)},$record->{intent}{nonce});
        $caller->{target}=\%target;
        EvidencePrototype::_manifest($manifest,$caller,$native);
        my $native_id=EvidencePrototype::_id($native);my $manifest_id=EvidencePrototype::_id($manifest_fd);
        flock($native,LOCK_UN)or $refuse->();
        my $admitted=EvidencePrototype::admit($caller,$execution);
        EvidencePrototype::_id($admitted->{root})eq$native_id&&EvidencePrototype::_id($admitted->{manifest_fd})eq$manifest_id&&$admitted->{manifest_bytes}eq$manifest_bytes or $refuse->();
        EvidencePrototype::revalidate($admitted,$caller);
        my($same_goal,$same_bytes,$same_fd)=$goal->($parent);
        $same_bytes eq$goal_bytes&&EvidencePrototype::_id($same_fd)eq EvidencePrototype::_id($goal_fd)or $refuse->();
        $json->encode($caller)=~/\A.{1,4096}\z/s or $refuse->();
        $output=$caller;
    }else{
        $operation_id=~/\A[0-9a-f]{32}\z/ or $refuse->();
        my $execution=EvidencePrototype::strict_json($execution_raw,4096);
        my($saved,$goal_bytes,$goal_fd)=$goal->($parent);
        EvidencePrototype::_same($saved,{work=>$caller->{work},end_lsn=>$caller->{end_lsn}});
        my $admitted=EvidencePrototype::admit($caller,$execution);
        $receipt->{admitted}=JSON::PP::true;
        my $observation=EvidencePrototype::observe($admitted,$caller,{trace=>$trace});
        $observation&&$observation->{all_eof}&&$observation->{child_exit}==0 or $refuse->();
        EvidencePrototype::revalidate($admitted,$caller);
        my($same_goal,$same_bytes,$same_fd)=$goal->($parent);
        $same_bytes eq$goal_bytes&&EvidencePrototype::_id($same_fd)eq EvidencePrototype::_id($goal_fd)or $refuse->();
        $receipt->{source_major}=$caller->{source}{major};$receipt->{target_major}=$observation->{server_major};
        $receipt->{observed}=JSON::PP::true;$receipt->{all_eof}=JSON::PP::true;
        my $positions=$observation->{positions};EvidencePrototype::_keys($positions,qw(token certified durable end witness kind));
        $receipt->{positions}={map{$_=>$positions->{$_}}qw(token certified durable end witness kind)};
        $output=$receipt;
    }
    1;
};
alarm 0;
if($mode eq 'observe'){
    $receipt->{stages}=$trace->{stages};$receipt->{stage}=$trace->{stage};$receipt->{source_closed}=$trace->{source_closed}?JSON::PP::true:JSON::PP::false;
    @{$receipt}{qw(exit signal stderr_bytes)}=@{$trace}{qw(exit signal stderr_bytes)};
    unless($ok){delete $receipt->{positions};$receipt->{observed}=JSON::PP::false;$receipt->{all_eof}=JSON::PP::false;$receipt->{source_major}=0;$receipt->{target_major}=0}
    $output=$receipt;
}else{$output={accepted=>JSON::PP::false}unless $ok}
my $bytes=eval{$json->encode($output)."\n"};
defined($bytes)&&length($bytes)<=65536 or exit 2;
print $bytes or exit 2;
exit 0;

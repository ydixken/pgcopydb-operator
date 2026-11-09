package ComposedFaultFixture;
use strict;
use warnings;
use IO::Select ();
use IPC::Open3 ();
use Symbol qw(gensym);
use Socket qw(:DEFAULT pack_sockaddr_in unpack_sockaddr_in inet_aton);
use JSON::PP ();
use Time::HiRes qw(time);
use Digest::SHA qw(sha256_hex);
use EvidencePrototype;

# The relay recognizes only COMMIT completion on the owned loopback fixture.
sub _forward {
    my($h,$bytes)=@_;while(length($bytes)){my $n=syswrite($h,$bytes);defined($n)&&$n>0 or die "proxy_write\n";substr($bytes,0,$n,'')}
}
sub _event_valid {
    my($e)=@_;EvidencePrototype::_keys($e,qw(stage pid status));
    $e->{stage}=~/\A(?:launched|listening|accepted|backend_connected|commit_complete|idle|waiting|sync_wait|cancel_sent|timeout|exit)\z/ or die "fault_event_stage\n";
    EvidencePrototype::_uint($e->{pid},'2147483647',1);EvidencePrototype::_uint($e->{status},'1',0);return 1;
}
sub _start {
    my($action)=@_;pipe(my $read,my $write)or die "fault_pipe\n";my $pid=fork();defined($pid)or die "fault_fork\n";
    if(!$pid){
        close($read);my $count=0;my $emit=sub{my($stage,$status)=@_;++$count<=8 or die "fault_event_limit\n";my $e={stage=>$stage,pid=>$$,status=>$status//0};_event_valid($e);my $line=JSON::PP->new->canonical->encode($e)."\n";length($line)<=256 or die "fault_event_limit\n";_forward($write,$line)};
        $SIG{ALRM}=sub{$emit->('timeout',1);die "fault_timeout\n"};alarm 10;
        my $ok=eval{$emit->('launched',0);$action->($emit);1};alarm 0;
        my $written=eval{$emit->('exit',$ok?0:1);1};close($write);exit($ok&&$written?0:1);
    }
    close($write);return {pid=>$pid,read=>$read,events=>[]};
}
sub finish_fault {
    my($fault)=@_;my $read=$fault->{read};my $select=IO::Select->new($read);my $bytes='';my $deadline=time()+11;
    while($select->count()&&time()<$deadline){for my $h($select->can_read(0.05)){my $n=sysread($h,my $part,256);defined($n)or die "fault_read\n";if(!$n){$select->remove($h);close($h);next}$bytes.=$part;length($bytes)<=4096 or die "fault_limit\n"}}
    kill('KILL',$fault->{pid})if $select->count();waitpid($fault->{pid},0)==$fault->{pid}or die "fault_wait\n";my $status=$?;
    my @events;my $valid=eval{my @lines=split(/\n/,$bytes,-1);pop(@lines)eq''or die "fault_partial_line\n";@lines<=8 or die "fault_event_limit\n";for(@lines){my $e=EvidencePrototype::strict_json($_,256);_event_valid($e);$e->{pid}==$fault->{pid}or die "fault_event_pid\n";push @events,$e}1};
    $fault->{events}=\@events;$fault->{exit}=$status>>8;$fault->{signal}=$status&127;
    return $valid&&$status==0&&@events&&$events[-1]{stage}eq'exit'&&$events[-1]{status}==0;
}
sub committed_idle {
    my($fault)=@_;my @want=qw(launched listening accepted backend_connected commit_complete idle exit);my $events=$fault->{events}//[];@$events==@want or return 0;
    for my $i(0..$#want){my $e=$events->[$i];$e->{stage}eq$want[$i]&&$e->{pid}==$fault->{pid}&&$e->{status}==0 or return 0}return 1;
}
sub start_proxy {
    my($mode)=@_;$mode eq'lost_commit'||$mode eq'post_commit_loss'or die "proxy_mode\n";
    socket(my $listener,AF_INET,SOCK_STREAM,0)or die "proxy_socket\n";setsockopt($listener,SOL_SOCKET,SO_REUSEADDR,pack('i',1))or die "proxy_option\n";
    bind($listener,pack_sockaddr_in(0,inet_aton('127.0.0.1')))or die "proxy_bind\n";listen($listener,1)or die "proxy_listen\n";my($port)=unpack_sockaddr_in(getsockname($listener));
    my $fault=_start(sub{
        my($emit)=@_;$emit->('listening',0);
        accept(my $frontend,$listener)or die "proxy_accept\n";$emit->('accepted',0);close($listener);socket(my $backend,AF_INET,SOCK_STREAM,0)or die "proxy_socket\n";
        connect($backend,pack_sockaddr_in(5432,inet_aton('127.0.0.1')))or die "proxy_connect\n";$emit->('backend_connected',0);my $readers=IO::Select->new($frontend,$backend);my $buffer='';my $commit=0;
        while(1){for my $h($readers->can_read(0.05)){my $n=sysread($h,my $bytes,8192);defined($n)&&$n>0 or die "proxy_eof\n";if(fileno($h)==fileno($frontend)){_forward($backend,$bytes);next}
            $buffer.=$bytes;length($buffer)<=1048576 or die "proxy_limit\n";
            while(length($buffer)>=5){my $size=unpack('N',substr($buffer,1,4));$size>=4&&$size<=1048576 or die "proxy_frame\n";last if length($buffer)<$size+1;
                my $frame=substr($buffer,0,$size+1,'');if(substr($frame,0,1)eq'C'&&substr($frame,5)eq"COMMIT\0"){$commit=1;$emit->('commit_complete',0)}
                _forward($frontend,$frame)unless $commit&&$mode eq'lost_commit';
                if($commit&&substr($frame,0,1)eq'Z'){substr($frame,5)eq'I'or die "proxy_not_idle\n";$emit->('idle',0);close($frontend);close($backend);return}
            }
        }}
    });close($listener);$fault->{port}=$port;return $fault;
}
sub admin_query {
    my($sql)=@_;my $errors=gensym();local $ENV{PGOPTIONS}='-c synchronous_commit=off';
    my $pid=IPC::Open3::open3(my $in,my $out,$errors,'/usr/lib/postgresql/18/bin/psql','-X','-n','-w','-qAt','-v','ON_ERROR_STOP=on','-c',$sql);close($in);
    my $select=IO::Select->new($out,$errors);my($ofd,$efd)=(fileno($out),fileno($errors));my %bytes=($ofd=>'',$efd=>'');
    while($select->count()){for my $h($select->can_read(0.05)){my $n=sysread($h,my $part,4096);defined($n)or die "control_read\n";if(!$n){$select->remove($h);close($h);next}$bytes{fileno($h)}.=$part;length($bytes{fileno($h)})<=4096 or die "control_limit\n"}}
    waitpid($pid,0)==$pid&&$?==0&&$bytes{$efd}eq''or die "control_query\n";return $bytes{$ofd};
}
sub synchronous_wait {
    my($enabled)=@_;my $sql=$enabled?q{ALTER SYSTEM SET synchronous_standby_names='FIRST 1 ("m6_missing")'}:'ALTER SYSTEM RESET synchronous_standby_names';
    admin_query($sql)eq''or die "control_setting\n";admin_query('SELECT pg_reload_conf();')eq"t\n"or die "control_reload\n";
    my $expected=$enabled?q{FIRST 1 ("m6_missing")}:'';my $deadline=time()+2;
    while(time()<$deadline){return if admin_query('SHOW synchronous_standby_names;')eq$expected."\n";select(undef,undef,undef,0.02)}
    die "control_setting_not_observed\n";
}
sub start_cancel {
    my($backend)=@_;$backend=~/\A[1-9][0-9]*\z/ or die "cancel_pid\n";
    return _start(sub{my($emit)=@_;$emit->('waiting',0);my $deadline=time()+5;while(time()<$deadline){if(admin_query("SELECT wait_event='SyncRep' FROM pg_catalog.pg_stat_activity WHERE pid=$backend;")eq"t\n"){$emit->('sync_wait',0);admin_query("SELECT pg_catalog.pg_cancel_backend($backend);")eq"t\n"or die "cancel_failed\n";$emit->('cancel_sent',0);return}select(undef,undef,undef,0.02)}die "cancel_not_waiting\n"});
}
sub identifier {
    open(my $in,'<','/dev/urandom')or die "exec_random\n";read($in,my $bytes,16)==16 or die "exec_random\n";close($in);return unpack('H*',$bytes);
}
sub constructor_discriminator {
    my($error)=@_;return 'connection_form'if $error eq"private_conninfo_refused_passfile_required\n";
    return 'pty'if $error eq"private_pty\n";return 'pipe'if $error eq"private_pipe\n";return 'fork'if $error eq"private_fork\n";return 'other';
}
sub _attempt_valid {
    my($r,$id,$pid)=@_;EvidencePrototype::_keys($r,qw(id pid admitted observed position_present source_closed stages refusal_stage failure all_eof exit signal stderr_bytes token_preserved row_preserved fault_events constructor_discriminator));
    $r->{id}eq$id&&$r->{pid}==$pid or die "exec_binding\n";EvidencePrototype::_uint($r->{pid},'2147483647',1);
    EvidencePrototype::_bool($r->{$_})for qw(admitted observed position_present source_closed all_eof token_preserved row_preserved);
    $r->{constructor_discriminator}=~/\A(?:|connection_form|pty|pipe|fork|other)\z/&&(!$r->{observed}||$r->{constructor_discriminator}eq'')or die "exec_constructor_kind\n";
    $r->{refusal_stage}=~/\A(?:|admission|source_identity|target_open|target_settings|target_identity|configure_durability|target_fence|idle_guard|begin|lock_checkpoint|lock_marker|catalog_checkpoint|catalog_marker|row_read|same_value_update|deferred_effects|row_comparison|precommit_guard|commit|unlock|closure_guard|closed)\z/ or die "exec_stage\n";
    $r->{failure}=~/\A(?:|validation|diagnostic|timeout|output_limit|transaction_state|closure)\z/ or die "exec_failure_kind\n";
    EvidencePrototype::_array($r->{stages},19);for(@{$r->{stages}}){/\A(?:target_settings|target_identity|configure_durability|target_fence|idle_guard|begin|lock_checkpoint|lock_marker|catalog_checkpoint|catalog_marker|row_read|same_value_update|deferred_effects|row_comparison|precommit_guard|commit|unlock|closure_guard)\z/ or die "exec_stage\n"}
    $r->{exit}>=-1&&$r->{exit}<=255 or die "exec_exit\n";EvidencePrototype::_uint($r->{signal},'127',0);EvidencePrototype::_uint($r->{stderr_bytes},'65536',0);
    EvidencePrototype::_array($r->{fault_events},8);_event_valid($_)for @{$r->{fault_events}};return 1;
}
sub reader_exec {
    my($caller,$execution,$fault_mode,$launch)=@_;$launch//={};my $id=identifier();@$launch{qw(id pid status)}=($id,0,'launching');my $errors=gensym();my $json=JSON::PP->new->canonical;
    my($pid,$in,$out);my %bytes=(out=>'',err=>'');my $reaped=0;my $script=__FILE__;$script=~s/ComposedFaultFixture\.pm\z/admission_probe.pl/ or die "exec_script\n";
    my $ok=eval{
        $pid=IPC::Open3::open3($in,$out,$errors,$^X,$script,'observe_existing',$json->encode($caller),$json->encode($execution),$fault_mode,$id);@$launch{qw(pid status)}=($pid,'running');close($in);
        my $select=IO::Select->new($out,$errors);my %names=(fileno($out)=>'out',fileno($errors)=>'err');
        while($select->count()){for my $h($select->can_read(0.05)){my $n=sysread($h,my $part,4096);defined($n)or die "exec_read\n";if(!$n){$select->remove($h);close($h);next}my $name=$names{fileno($h)};$bytes{$name}.=$part;length($bytes{$name})<=65536 or die "exec_limit\n"}}
        waitpid($pid,0)==$pid or die "exec_wait\n";$reaped=1;$?==0&&$bytes{err}eq''or die "exec_failure\n";
        my $r=EvidencePrototype::strict_json($bytes{out},65536);_attempt_valid($r,$id,$pid);$launch->{attempt}=$r;$launch->{status}=$r->{observed}?'returned_observation':'returned_refusal';1;
    };
    if(!$ok){$launch->{status}=$pid?'exec_failed':'launch_failed';if($pid&&!$reaped){kill('TERM',$pid);waitpid($pid,0)}}
    @$launch{qw(stdout_bytes stderr_bytes stdout_sha256 stderr_sha256)}=(length($bytes{out}),length($bytes{err}),sha256_hex($bytes{out}),sha256_hex($bytes{err}));
    $ok or die "exec_failure\n";return $launch->{attempt};
}
sub emit_partial {
    my($case,$launch,$fault)=@_;$case=~/\A(?:sync_cancel|lost_commit|post_commit_loss)(?:\/(?:wrong_owner|wrong_pvc|wrong_attempt|missing_manifest|owner_busy|fence_busy|replaced_relation|wrong_source_endpoint|wrong_target_endpoint|second_commit_fault))?\z/ or die "partial_case\n";
    $launch->{id}=~/\A[0-9a-f]{32}\z/ or die "partial_id\n";EvidencePrototype::_uint($launch->{pid},'2147483647',0);
    $launch->{status}=~/\A(?:launch_failed|exec_failed|returned_refusal|returned_observation)\z/ or die "partial_status\n";
    for(qw(stdout_bytes stderr_bytes)){EvidencePrototype::_uint($launch->{$_},'131072',0)}for(qw(stdout_sha256 stderr_sha256)){$launch->{$_}=~/\A[0-9a-f]{64}\z/ or die "partial_hash\n"}
    my $attempt=$launch->{attempt};_attempt_valid($attempt,$launch->{id},$launch->{pid})if $attempt;my $events=$fault?$fault->{events}:[];EvidencePrototype::_array($events,8);_event_valid($_)for @$events;
    my $p={partial=>JSON::PP::true,case=>$case,status=>'fault_unconfirmed',id=>$launch->{id},pid=>$launch->{pid},reader_status=>$launch->{status},reader_stage=>$attempt?($attempt->{observed}?'closed':$attempt->{refusal_stage}):'',constructor_discriminator=>$attempt&&length($attempt->{constructor_discriminator})?$attempt->{constructor_discriminator}:'other',(map{$_=>$launch->{$_}}qw(stdout_bytes stderr_bytes stdout_sha256 stderr_sha256)),fault_events=>$events,observed=>JSON::PP::false,position_present=>JSON::PP::false,token_present=>JSON::PP::false,frontier_present=>JSON::PP::false};
    my $raw=JSON::PP->new->canonical->encode($p);length($raw)<=8192 or die "partial_limit\n";print $raw."\n"or die "partial_write\n";return 1;
}
sub row_bytes {
    my($manifest,$name)=@_;$name//='checkpoint';$name eq'checkpoint'||$name eq'checkpoint_old'or die "snapshot_name\n";my $b=$manifest->{target_provenance}[0]{record}{intent}{binding};my $columns=EvidencePrototype::_columns();my $s=PSQLEvidencePrototype->new(EvidencePrototype::_row_cap($columns),'target');my $raw;
    my $ok=eval{$s->phase(EvidencePrototype::_row_select('"'.$b->{schema_name}.'"."'.$name.'"',$columns),'',sub{$raw=$_[0];EvidencePrototype::_row_output($raw);1})or die "snapshot_row\n";1};$s->{failed}=1 unless $ok;my $closed=$s->finish();$ok&&$closed or die "snapshot_close\n";return $raw;
}
1;

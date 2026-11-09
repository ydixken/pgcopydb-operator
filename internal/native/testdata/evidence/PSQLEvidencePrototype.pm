package PSQLEvidencePrototype;
use strict;
use warnings;
use Fcntl qw(:DEFAULT);
use POSIX ();
use IO::Select ();
use IO::Handle ();
use Time::HiRes qw(time);

sub role_database_args {
    my($role)=@_;return []unless defined($role);$role eq'source'||$role eq'target'or die "private_role\n";
    my $name=$role eq'source'?'PGCOPYDB_SOURCE_PGURI':'PGCOPYDB_TARGET_PGURI';return []unless defined($ENV{$name});my $database=$ENV{$name};
    # Only the existing fixture forms reach argv; other endpoints need trusted passfile construction.
    my $simple=$database=~/\A[a-z][a-z0-9_]{0,62}\z/;
    my $proxy=$database=~/\Ahost=127\.0\.0\.1 port=([1-9][0-9]{0,4}) dbname=postgres sslmode=disable\z/&&$1<=65535;
    $simple||$proxy or die "private_conninfo_refused_passfile_required\n";return ['--dbname',$database];
}

# Query bytes never share the terminal prompt channel or the diagnostic pipe.
sub new {
    my($class,$limit,$role)=@_;my $database_args=role_database_args($role);my $self=bless {result=>'',prompt=>'',errors=>'',prompts=>[],eof=>{},sequence=>0,limit=>$limit,deadline=>time()+25,failed=>0},$class;
    open(my $random,'<','/dev/urandom')or die "private_random\n";read($random,my $bytes,16)==16 or die "private_random\n";close($random);$self->{nonce}=unpack('H*',$bytes);
    sysopen(my $master,'/dev/ptmx',O_RDWR|O_NOCTTY)or die "private_pty\n";
    my $zero=pack('i',0);ioctl($master,0x40045431,$zero)or die "private_pty\n";my $number=pack('i',0);ioctl($master,0x80045430,$number)or die "private_pty\n";
    sysopen(my $slave,'/dev/pts/'.unpack('i',$number),O_RDWR|O_NOCTTY)or die "private_pty\n";
    my $term=POSIX::Termios->new();$term->getattr(fileno($slave));$term->setlflag($term->getlflag&~(POSIX::ECHO()|POSIX::ECHONL()|POSIX::ICANON()|POSIX::ISIG()|POSIX::IEXTEN()));$term->setiflag($term->getiflag&~(POSIX::ICRNL()|POSIX::INLCR()|POSIX::IGNCR()|POSIX::IXON()|POSIX::IXOFF()));$term->setoflag($term->getoflag&~POSIX::OPOST());$term->setcc(POSIX::VMIN(),1);$term->setcc(POSIX::VTIME(),0);$term->setattr(fileno($slave),POSIX::TCSANOW());
    pipe(my $result,my $result_writer)or die "private_pipe\n";pipe(my $error,my $error_writer)or die "private_pipe\n";fcntl($result_writer,F_SETFD,0)or die "private_pipe\n";
    my $pid=fork();defined($pid)or die "private_fork\n";
    if(!$pid){close($master);close($result);close($error);open(STDIN,'<&',$slave)or die "private_stdin\n";open(STDOUT,'>&',$slave)or die "private_stdout\n";open(STDERR,'>&',$error_writer)or die "private_stderr\n";close($slave);close($error_writer);$ENV{LC_ALL}='C';$ENV{PSQL_HISTORY}='/dev/null';exec '/usr/lib/postgresql/18/bin/psql','-X','-n','-w','-A','-t','-q','-P','pager=off',@$database_args,'-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate','-v',"PROMPT1=M6P:$self->{nonce}:%p:%x:END",'-v','PROMPT2=UNEXPECTED_CONTINUATION','-v','PROMPT3=UNEXPECTED_COPY','-o','/proc/self/fd/'.fileno($result_writer) or die "private_exec\n"}
    close($slave);close($result_writer);close($error_writer);$master->autoflush(1);
    @$self{qw(pid master result_handle error_handle)}=($pid,$master,$result,$error);
    $self->{select}=IO::Select->new($master,$result,$error);$self->{channels}={fileno($master)=>'prompt',fileno($result)=>'result',fileno($error)=>'errors'};
    $self->_until(sub{@{$self->{prompts}}==1});
    if(!$self->{failed}){$self->{backend}=$self->{prompts}[0][0];$self->{prompts}[0][1]eq''&&$self->{backend}=~/\A[1-9][0-9]*\z/ or $self->{failed}=1}
    $self->phase('\\set QUIET off','',sub{$_[0]eq''})unless $self->{failed};
    return $self;
}
sub _pump {
    my($s)=@_;time()<$s->{deadline}or do{$s->{failed}=1;$s->{failure}//='timeout';return};my @ready=$s->{select}->can_read(0.05);
    for my $h(@ready){my $fd=fileno($h);my $channel=$s->{channels}{$fd};my $n=sysread($h,my $bytes,8192);
        if(!defined($n)){if($channel eq'prompt'&&$!{EIO}){$n=0}else{$s->{failed}=1;next}}
        if(!$n){$s->{eof}{$channel}=1;$s->{select}->remove($h);close($h);next}
        $s->{$channel}.=$bytes;my $cap=$channel eq'result'?$s->{limit}:65536;if(length($s->{$channel})>$cap){$s->{failed}=1;$s->{failure}//='output_limit'}
        if($channel eq'errors'){$s->{failed}=1;$s->{failure}//='diagnostic';if($s->{errors}=~/\b(?:ERROR|FATAL|WARNING|NOTICE):[ \t]+([0-9A-Z]{5})\b/){$s->{sqlstate}//=$1}}
        if($channel eq'prompt'){
            my $nonce=$s->{nonce};while($s->{prompt}=~s/\AM6P:\Q$nonce\E:([0-9]*|\?):([*!?]?):END//){push @{$s->{prompts}},[$1,$2]}
            my $prefix='M6P:'.$nonce.':';index($prefix,$s->{prompt})==0||index($s->{prompt},$prefix)==0 or $s->{failed}=1;
        }
    }
}
sub _until {my($s,$condition)=@_;while(!$condition->()&&!$s->{failed}){$s->{select}->count()or do{$s->{failed}=1;last};$s->_pump()}return !$s->{failed}}
sub _send {my($s,$command)=@_;my $ok=eval{print {$s->{master}} $command."\n"};if(!$ok){$s->{failed}=1;$s->{failure}//='validation'}}
sub _prompt {
    my($s,$index,$state)=@_;my $p=$s->{prompts}[$index];defined($p)&&$p->[0]eq$s->{backend}&&$p->[1]eq$state or $s->{failed}=1;$s->{failure}//='transaction_state' if $s->{failed};return !$s->{failed};
}
sub phase {
    my($s,$sql,$state,$validate)=@_;return 0 if $s->{failed};my $before=scalar @{$s->{prompts}};$s->_send($sql);$s->_until(sub{@{$s->{prompts}}>=$before+1});$s->_prompt($before,$state)unless $s->{failed};
    my $marker='M6DONE_'.$s->{nonce}.'_'.++$s->{sequence};$s->_send('\\qecho '.$marker)unless $s->{failed};
    $s->_until(sub{@{$s->{prompts}}>=$before+2&&index($s->{result},$marker."\n")>=0});$s->_prompt($before+1,$state)unless $s->{failed};
    if(!$s->{failed}){my $at=index($s->{result},$marker."\n");my $data=substr($s->{result},0,$at);substr($s->{result},$at)eq$marker."\n"or $s->{failed}=1;$s->{result}='';my $ok=eval{$validate->($data)};if(!$ok){$s->{failed}=1;$s->{failure}//='validation'}}
    return !$s->{failed};
}
sub finish {
    my($s)=@_;$s->_send('\\q')unless $s->{failed};
    if($s->{failed}){kill('TERM',$s->{pid});if(!$s->{eof}{prompt}){$s->{select}->remove($s->{master});close($s->{master});$s->{eof}{prompt}=1}}
    while($s->{select}->count()&&time()<$s->{deadline}){$s->_pump()}
    if($s->{select}->count()){kill('KILL',$s->{pid});$s->{failed}=1}
    waitpid($s->{pid},0)==$s->{pid}or $s->{failed}=1;my $status=$?;
    $s->{exit}=$status>>8;$s->{signal}=$status&127;
    $status==0&&$s->{eof}{prompt}&&$s->{eof}{result}&&$s->{eof}{errors}&&$s->{prompt}eq''&&$s->{result}eq''&&$s->{errors}eq''or $s->{failed}=1;
    return !$s->{failed};
}
1;

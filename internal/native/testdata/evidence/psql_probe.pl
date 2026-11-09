use strict;
use warnings;
use Fcntl qw(:DEFAULT :flock :mode);
use POSIX ();
use IO::Select ();
use IO::Handle ();
use JSON::PP ();
use IPC::Open3 ();
use Socket qw(:DEFAULT pack_sockaddr_in unpack_sockaddr_in inet_aton);
use Symbol qw(gensym);
use Time::HiRes qw(time);

$SIG{ALRM} = sub { die "probe_timeout\n" };
alarm 15;
my $mode = shift // 'normal';
my $query = shift // 'SELECT 1;';
my %supported_mode = map { $_ => 1 } qw(normal transaction sync_cancel lost_commit post_commit_loss remote_apply upgrade_sync warning notice deferred_failure false_unlock multiple_unlock malformed_unlock prompt_in_row long_query);
$supported_mode{$mode} or die "unsupported_mode\n";
my $nonce = '4b8b986f69374feeb27bbe0c01bd4aac';
my $prompt = "M6:$nonce:%p:%x:END";
my $controller;
my $fault_confirmed = 0;
my $proxy_port;
use lib '/probe';
use ComposedFaultFixture;
my $proxy;
if ($mode eq 'lost_commit' || $mode eq 'post_commit_loss') {
    $proxy = ComposedFaultFixture::start_proxy($mode);
    $proxy_port = $proxy->{port};
}
sysopen(my $master, '/dev/ptmx', O_RDWR | O_NOCTTY) or die "pty_open\n";
my $zero = pack('i', 0);
ioctl($master, 0x40045431, $zero) or die "pty_unlock\n";
my $number = pack('i', 0);
ioctl($master, 0x80045430, $number) or die "pty_number\n";
my $slave_name = '/dev/pts/' . unpack('i', $number);
sysopen(my $slave, $slave_name, O_RDWR | O_NOCTTY) or die "pty_slave\n";
my $term = POSIX::Termios->new();
$term->getattr(fileno($slave));
$term->setlflag($term->getlflag & ~(POSIX::ECHO() | POSIX::ECHONL() | POSIX::ICANON() | POSIX::ISIG() | POSIX::IEXTEN()));
$term->setiflag($term->getiflag & ~(POSIX::ICRNL() | POSIX::INLCR() | POSIX::IGNCR() | POSIX::IXON() | POSIX::IXOFF()));
$term->setcc(POSIX::VMIN(), 1);
$term->setcc(POSIX::VTIME(), 0);
$term->setoflag($term->getoflag & ~POSIX::OPOST());
$term->setattr(fileno($slave), POSIX::TCSANOW());
pipe(my $result_read, my $result_write) or die "result_pipe\n";
pipe(my $error_read, my $error_write) or die "error_pipe\n";
fcntl($result_write, F_SETFD, 0) or die "result_inheritance\n";
my $pid = fork();
defined($pid) or die "fork\n";
if ($pid == 0) {
    close($master); close($result_read); close($error_read);
    open(STDIN, '<&', $slave) or die "stdin\n";
    open(STDOUT, '>&', $slave) or die "stdout\n";
    open(STDERR, '>&', $error_write) or die "stderr\n";
    close($slave); close($error_write);
    $ENV{PSQL_HISTORY} = '/dev/null';
    $ENV{LC_ALL} = 'C';
    if (defined($proxy_port)) { $ENV{PGPORT} = $proxy_port; $ENV{PGSSLMODE} = 'disable'; }
    exec '/usr/lib/postgresql/18/bin/psql', '-X', '-n', '-w', '-A', '-t', '-q',
        '-P', 'pager=off', '-v', 'ON_ERROR_STOP=on',
        '-v', "PROMPT1=$prompt", '-v', 'PROMPT2=UNEXPECTED_CONTINUATION',
        '-v', 'PROMPT3=UNEXPECTED_COPY', '-o', '/proc/self/fd/' . fileno($result_write) or die "exec\n";
}
close($slave); close($result_write); close($error_write);
$master->autoflush(1);
my $select = IO::Select->new($master, $result_read, $error_read);
my %channel = (fileno($master) => 'prompt', fileno($result_read) => 'result', fileno($error_read) => 'error');
my %output = (prompt => '', result => '', error => '');
my @queries = ($query);
my $random = '';
open(my $random_handle, '<', '/dev/urandom') or die "random\n";
read($random_handle, $random, 8) == 8 or die "random\n";
close($random_handle);
my $schema = 'evidence_' . unpack('H*', $random);
sub admin_query { ComposedFaultFixture::admin_query(@_) }
my $fence = '1380992845,-2079885925';
if ($mode eq 'transaction') {
    @queries = (
        'CREATE TEMP TABLE evidence_probe (id int PRIMARY KEY, token int);',
        'INSERT INTO evidence_probe VALUES (1,7);',
        "SELECT pg_catalog.pg_try_advisory_lock($fence);",
        'BEGIN;', 'SET LOCAL synchronous_commit=on;',
        'UPDATE ONLY evidence_probe SET (id,token)=(id,token) WHERE id=1 AND token=7;',
        'COMMIT;', "SELECT pg_catalog.pg_advisory_unlock($fence);",
    );
 } elsif ($mode eq 'sync_cancel' || $mode eq 'lost_commit' || $mode eq 'post_commit_loss') {
    @queries = (
        'SET synchronous_commit=off;',
        "CREATE SCHEMA $schema;", "CREATE TABLE $schema.checkpoint(id int PRIMARY KEY, token int);",
        "INSERT INTO $schema.checkpoint VALUES(1,7);",
        "SELECT pg_catalog.pg_try_advisory_lock($fence);", 'BEGIN;',
        'SET LOCAL synchronous_commit=on;',
        "UPDATE ONLY $schema.checkpoint SET (id,token)=(id,token) WHERE id=1 AND token=7;",
        'COMMIT;', 'SELECT pg_catalog.pg_backend_pid() > 0;', "SELECT pg_catalog.pg_advisory_unlock($fence);",
    );
} elsif ($mode eq 'remote_apply' || $mode eq 'upgrade_sync') {
    my $setting = $mode eq 'remote_apply' ? 'remote_apply' : 'off';
    @queries = (
        "SET synchronous_commit=$setting;",
        q{SELECT NOT pg_catalog.pg_is_in_recovery() AND current_setting('fsync')='on' AND current_setting('full_page_writes')='on' AND current_user=session_user;},
        "SELECT pg_catalog.pg_try_advisory_lock($fence);", 'BEGIN;',
        q{SELECT CASE WHEN (SELECT count(*)=1 FROM pg_catalog.pg_locks WHERE locktype='advisory' AND pid=pg_catalog.pg_backend_pid() AND classid=1380992845::oid AND objid=2215081371::oid AND objsubid=2 AND mode='ExclusiveLock' AND granted) THEN pg_catalog.set_config('synchronous_commit',CASE WHEN current_setting('synchronous_commit')='remote_apply' THEN 'remote_apply' ELSE 'on' END,true) ELSE NULL END;},
        'COMMIT;', "SELECT pg_catalog.pg_advisory_unlock($fence);",
    );
} elsif ($mode eq 'warning') {
    @queries = (q{DO $q$ BEGIN RAISE WARNING 'private test diagnostic'; END $q$;});
} elsif ($mode eq 'notice') {
    @queries = (q{DO $q$ BEGIN RAISE NOTICE 'private test diagnostic'; END $q$;});
} elsif ($mode eq 'deferred_failure') {
    @queries = (
        'CREATE TEMP TABLE evidence_probe (id int);',
        q{CREATE FUNCTION pg_temp.evidence_reject() RETURNS trigger LANGUAGE plpgsql AS $q$ BEGIN RAISE EXCEPTION 'private deferred failure'; END $q$;},
        'CREATE CONSTRAINT TRIGGER evidence_deferred AFTER INSERT ON evidence_probe DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pg_temp.evidence_reject();',
        'BEGIN;', 'INSERT INTO evidence_probe VALUES(1);', 'COMMIT;',
    );
} elsif ($mode eq 'false_unlock') {
    @queries = ("SELECT pg_catalog.pg_advisory_unlock($fence);");
} elsif ($mode eq 'multiple_unlock') {
    @queries = ("SELECT pg_catalog.pg_try_advisory_lock($fence);", "SELECT pg_catalog.pg_advisory_unlock($fence) FROM pg_catalog.generate_series(1,2);");
} elsif ($mode eq 'malformed_unlock') {
    @queries = (q{SELECT 'malformed';});
} elsif ($mode eq 'prompt_in_row') {
    @queries = ("SELECT 'M6:${nonce}::END';");
} elsif ($mode eq 'long_query') {
    @queries = ("SELECT '" . ('x' x 8192) . "'::pg_catalog.text;");
}
my @commands = ("\\set QUIET off\n", "SHOW server_version_num;\n", map("$_\n", @queries), "\\q\n");
my $sent = 0;
my @states;
my @backends;
while ($select->count()) {
    for my $handle ($select->can_read(1)) {
        my $label = $channel{fileno($handle)};
        my $count = sysread($handle, my $chunk, 8192);
        if (!defined($count)) {
            if ($label eq 'prompt' && $!{EIO}) { $select->remove($handle); close($handle); next; }
            die "read\n";
        }
        if (!$count) { $select->remove($handle); close($handle); next; }
        $output{$label} .= $chunk;
        length($output{$label}) <= 65536 or die "output_limit\n";
        if ($label eq 'prompt') {
            while ($output{prompt} =~ s/\AM6:\Q$nonce\E:(\d*|\?):([*!?]?):END//) {
                my ($backend, $state) = ($1, $2);
                push @states, $state;
                push @backends, $backend;
                if ($sent < @commands) {
                    my $command = $commands[$sent++];
                    if ($state eq '?' || $backend !~ /\A\d+\z/) { $command = "\\q\n"; $sent = scalar(@commands); }
                    if ($mode eq 'sync_cancel' && $command eq "COMMIT;\n") {
                        $controller = fork(); defined($controller) or die "control_fork\n";
                        if ($controller == 0) {
                            alarm 6;
                            my $deadline = time() + 5;
                            while (time() < $deadline) {
                                my $ready = admin_query("SELECT wait_event='SyncRep' FROM pg_catalog.pg_stat_activity WHERE pid=$backend;");
                                if ($ready eq "t\n") {
                                    admin_query("SELECT pg_catalog.pg_cancel_backend($backend);") eq "t\n" or die "cancel_failed\n";
                                    exit 0;
                                }
                                select(undef, undef, undef, 0.02);
                            }
                            exit 1;
                        }
                    }
                    print {$master} $command or die "write\n";
                }
            }
            if (length($output{prompt}) && index('M6:', $output{prompt}) != 0 && index($output{prompt}, 'M6:') != 0) {
                kill('TERM', $pid);
                $select->remove($master); close($master);
            }
        }
    }
}
waitpid($pid, 0) == $pid or die "wait\n";
my $status = $?;
if (defined($controller)) {
    waitpid($controller, 0) == $controller or die "control_wait\n";
    $fault_confirmed = $? == 0 ? 1 : 0;
}
if (defined($proxy)) {
    $fault_confirmed = ComposedFaultFixture::finish_fault($proxy) && ComposedFaultFixture::committed_idle($proxy) ? 1 : 0;
}
my $server_major = $output{result} =~ s/\A(\d+)\n// ? int($1 / 10000) : 0;
my $backend_stable = @backends && !(grep { $_ ne $backends[0] } @backends);
alarm 0;
print JSON::PP->new->canonical->encode({
    exit => $status >> 8, signal => $status & 127,
    result => $output{result}, stderr_bytes => length($output{error}),
    prompt_bytes => length($output{prompt}), states => \@states,
    all_eof => JSON::PP::true,
    server_major => $server_major,
    backend_stable => $backend_stable ? JSON::PP::true : JSON::PP::false,
    fault_confirmed => $fault_confirmed ? JSON::PP::true : JSON::PP::false,
}), "\n";

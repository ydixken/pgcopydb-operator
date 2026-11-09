use strict;
use warnings;
use Fcntl qw(:DEFAULT :flock :mode);
use IO::Handle ();
use Digest::SHA qw(sha256_hex);
use JSON::PP ();

$SIG{ALRM} = sub { die "probe_timeout\n" };
alarm 10;
umask 0077;
my $case = shift // 'valid';
my $root = '/tmp/evidence-private';
mkdir($root, 0700) or die "setup\n";
mkdir("$root/pgcopydb", 0700) or die "setup\n";
my $path = "$root/pgcopydb";
mkdir("$path/control", 0700) or die "setup\n";
sub write_private {
    my ($name, $bytes) = @_;
    sysopen(my $out, $name, O_CREAT | O_EXCL | O_WRONLY | O_NOFOLLOW, 0600) or die "setup\n";
    print {$out} $bytes or die "setup\n";
    $out->sync or die "setup_sync\n";
    close($out) or die "setup\n";
}
write_private("$path/manifest.json", '{"magic":"RPGMIGRATE","format_version":8}');
write_private("$path/control/lock", '');
write_private("$path/control/endpos.json", '{"version":1,"end_lsn":32,"sealed":true}');
my $root_identity = join(':', (stat($path))[0,1]);
if ($case eq 'missing') { unlink("$path/manifest.json") or die "setup\n"; }
if ($case eq 'symlink') { rename("$path/manifest.json", "$root/original") or die "setup\n"; symlink("$root/original", "$path/manifest.json") or die "setup\n"; }
if ($case eq 'hardlink') { link("$path/manifest.json", "$root/alias") or die "setup\n"; }
if ($case eq 'permissions') { chmod(0640, "$path/manifest.json") == 1 or die "setup\n"; }
if ($case eq 'manifest_pending') { write_private("$path/.manifest.tmp", '{}'); }
if ($case eq 'control_pending') { write_private("$path/control/.endpos.tmp", '{}'); }
if ($case eq 'unknown_entry') { write_private("$path/foreign", '{}'); }
if ($case eq 'oversized_manifest') { unlink("$path/manifest.json"); write_private("$path/manifest.json", 'x' x (1024*1024+1)); }
if ($case eq 'oversized_control') { unlink("$path/control/endpos.json"); write_private("$path/control/endpos.json", 'x' x 4097); }
if ($case eq 'truncated') { unlink("$path/manifest.json"); write_private("$path/manifest.json", '{'); }
my $competitor;
if ($case eq 'owner_busy') { sysopen($competitor, $path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW) or die "setup\n"; flock($competitor, LOCK_EX | LOCK_NB) or die "setup\n"; }
if ($case eq 'control_busy') { sysopen($competitor, "$path/control/lock", O_RDONLY | O_NOFOLLOW) or die "setup\n"; flock($competitor, LOCK_EX | LOCK_NB) or die "setup\n"; }
sub identity {
    my ($handle) = @_;
    my @s = stat($handle);
    @s or die "stat\n";
    return join(':', @s[0,1]);
}
sub open_private {
    my ($name, $directory) = @_;
    sysopen(my $handle, $name, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | ($directory ? O_DIRECTORY : 0)) or die "unsafe_open\n";
    my @s = stat($handle);
    @s && $s[4] == $> && ($s[2] & 07777) == ($directory ? 0700 : 0600) or die "unsafe_permissions\n";
    ($directory ? S_ISDIR($s[2]) : S_ISREG($s[2]) && $s[3] == 1) or die "unsafe_kind\n";
    return $handle;
}
sub entries {
    my ($handle, @want) = @_;
    opendir(my $scan, '/proc/self/fd/' . fileno($handle)) or die "scan\n";
    my @entries;
    while (defined(my $entry = readdir($scan))) { next if $entry eq '.' || $entry eq '..'; push @entries, $entry; @entries <= @want or die "namespace\n"; }
    closedir($scan) or die "scan\n";
    join('\0', sort @entries) eq join('\0', sort @want) or die "namespace\n";
}
sub read_private {
    my ($parent, $name, $cap) = @_;
    my $location = '/proc/self/fd/' . fileno($parent) . "/$name";
    my $file = open_private($location, 0);
    my @before = stat($file);
    $before[7] <= $cap or die "size\n";
    my $bytes = '';
    while (1) {
        my $n = sysread($file, my $chunk, 8192);
        defined($n) or die "read\n";
        last if !$n;
        $bytes .= $chunk;
        length($bytes) <= $cap or die "size\n";
    }
    if ($case eq 'replaced' && $name eq 'manifest.json') { rename("$path/manifest.json", "$root/replaced") or die "setup\n"; write_private("$path/manifest.json", $bytes); }
    if ($case eq 'interrupted_read' && $name eq 'manifest.json') { open(my $change, '>', "$path/manifest.json") or die "setup\n"; print {$change} '{'; close($change); }
    my $current = open_private($location, 0);
    identity($current) eq identity($file) or die "replacement\n";
    my @after = stat($file);
    join(':', @before[0,1,2,3,4,5,7,9,10]) eq join(':', @after[0,1,2,3,4,5,7,9,10]) && length($bytes) == $after[7] or die "interrupted_read\n";
    my $decoded = JSON::PP->new->max_depth(32)->max_size($cap)->decode($bytes);
    ref($decoded) eq 'HASH' or die "json\n";
    return sha256_hex($bytes);
}
my $accepted = eval {
    my $directory = open_private($path, 1);
    identity($directory) eq $root_identity or die "directory_identity\n";
    flock($directory, LOCK_EX | LOCK_NB) or die "owner_busy\n";
    entries($directory, 'manifest.json', 'control');
    my $control = open_private('/proc/self/fd/' . fileno($directory) . '/control', 1);
    entries($control, 'lock', 'endpos.json');
    my $lock = open_private('/proc/self/fd/' . fileno($control) . '/lock', 0);
    flock($lock, LOCK_EX | LOCK_NB) or die "control_busy\n";
    my $manifest_hash = read_private($directory, 'manifest.json', 1024*1024);
    my $control_hash = read_private($control, 'endpos.json', 4096);
    if ($case eq 'directory_replaced') { rename($path, "$root/prior") or die "setup\n"; mkdir($path, 0700) or die "setup\n"; }
    identity(open_private($path, 1)) eq identity($directory) or die "directory_identity\n";
    identity(open_private('/proc/self/fd/' . fileno($directory) . '/control', 1)) eq identity($control) or die "control_identity\n";
    identity(open_private('/proc/self/fd/' . fileno($control) . '/lock', 0)) eq identity($lock) or die "lock_identity\n";
    entries($directory, 'manifest.json', 'control');
    entries($control, 'lock', 'endpos.json');
    flock($lock, LOCK_UN) or die "control_unlock\n";
    flock($directory, LOCK_UN) or die "owner_unlock\n";
    1;
};
alarm 0;
print JSON::PP->new->canonical->encode({case => $case, accepted => $accepted ? JSON::PP::true : JSON::PP::false}), "\n";

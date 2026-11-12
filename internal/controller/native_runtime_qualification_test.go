package controller

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const nativeRuntimePG16Role = "postgresql-16"
const nativeRuntimePG18Role = "postgresql-18"

const nativeRuntimeLinux = "linux"
const nativeRuntimeRunKey = "run_id"

const nativeRuntimeNativeRole = "native-runner"
const nativeRuntimeARM64 = "arm64"
const nativeRuntimeTransport = "public-registry"
const nativeRuntimeManagerRole = "manager"
const nativeRuntimeVariantKey = "variant"
const nativeRuntimeCommitKey = "commit"
const nativeRuntimeStatusKey = "status"
const nativeRuntimeSchemaKey = "schema_version"
const nativeRuntimeQualificationKey = "qualification_id"

const nativeRuntimeEnvBinary = "/usr/bin/env"
const nativeRuntimePython = "/usr/bin/python3"
const nativeRuntimeLibraryPath = "LD_LIBRARY_PATH"
const nativeRuntimeImageIDKey = "image_id"
const nativeRuntimeRoleKey = "role"
const nativeRuntimePassed = "passed"
const nativeRuntimeRefused = "refused"
const nativeRuntimePartialCase = "partial"
const nativeRuntimePreload = "LD_PRELOAD"
const nativeRuntimeHTTPS = "https"
const nativeRuntimeNodeMode = "node"
const nativeRuntimePG16 = "pg16"
const nativeRuntimePG18 = "pg18"
const nativePasswordUIDEnvironment = "NATIVE_PROBE_POD_UID"

const nativeRuntimeSelector = "RPGMIGRATE_NATIVE_RUNTIME_INPUT"
const nativeRuntimeFileLimit = 1048576
const nativeRuntimeWireLimit = 65536
const nativeRuntimeRunLabel = "pgcopydb-operator.io/m7-run"

const nativeRuntimePodKind = "Pod"
const nativeRuntimeNamespaceKind = "Namespace"
const nativeRuntimeRetained = "retained"
const nativeRuntimeDeleted = "deleted"
const nativeRuntimeHome = "home"
const nativeRuntimeResolver = "resolver"
const nativeRuntimeRuntime = "runtime"
const nativeRuntimeProvider = "provider"
const nativeRuntimePhase = "metadata"

// Metadata collection makes no connection and does not change the production profile guard.
const nativeRuntimeMetadataProgram = `import ctypes,glob,hashlib,json,os,platform,resource,stat,subprocess,sys,time
resource.setrlimit(resource.RLIMIT_CORE,(0,0))
resource.setrlimit(resource.RLIMIT_CPU,(8,8))
resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))
def need(value):
 if not value:raise RuntimeError('metadata_refused')
def safe(path,maximum=1048576):
 fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
 try:
  a=os.fstat(fd);need(stat.S_ISREG(a.st_mode) and a.st_uid==0 and not a.st_mode&0o022 and a.st_nlink==1 and a.st_size<=maximum)
  raw=os.read(fd,maximum+1);b=os.fstat(fd)
  identity=lambda value:(value.st_dev,value.st_ino,value.st_uid,value.st_gid,value.st_mode,value.st_nlink,value.st_size,value.st_mtime_ns,value.st_ctime_ns)
  need(len(raw)==a.st_size and len(raw)<=maximum and identity(a)==identity(b)==identity(os.lstat(path)))
  return raw,a
 finally:os.close(fd)
def metadata(path):
 try:
  value=os.lstat(path)
  return dict(present=True,uid=value.st_uid,gid=value.st_gid,mode=stat.S_IMODE(value.st_mode),kind='file' if stat.S_ISREG(value.st_mode) else 'directory' if stat.S_ISDIR(value.st_mode) else 'link' if stat.S_ISLNK(value.st_mode) else 'other')
 except FileNotFoundError:return dict(present=False)
def image_mount(path,mounts):
 roots=[mount for mount in mounts if mount['path']=='/'];need(len(roots)==1)
 return dict(mount_id=roots[0]['id'],root_readonly='ro' in roots[0]['options'].split(','),shadow_mounts=[mount['path'] for mount in mounts if mount['path']!='/' and (path==mount['path'] or path.startswith(mount['path'].rstrip('/')+'/'))])
def image_file(path,mounts,maximum=16777216):
 mount=image_mount(path,mounts);need(mount['root_readonly'] and not mount['shadow_mounts'] and os.path.realpath(path)==path)
 raw,info=safe(path,maximum)
 return dict(mount,sha256=hashlib.sha256(raw).hexdigest(),device=info.st_dev,inode=info.st_ino,uid=info.st_uid,gid=info.st_gid,mode=stat.S_IMODE(info.st_mode),links=info.st_nlink,size=len(raw))
def admit_nss(files):
 for path,digest in [('/etc/passwd','21352194cc533bc5878721507450d867d28ccb1c2f5cd773c792251fa1e63185'),('/etc/nsswitch.conf','eec30745bade42a3f3f792e4d4192e57d2bcfe8e472433b1de426fe39a39cddb')]:
  value=files[path];need(value['sha256']==digest and value['uid']==0 and value['gid']==0 and value['mode']==0o644 and value['links']==1 and value['root_readonly'] is True and value['shadow_mounts']==[])
def configuration(path,mounts):
 value=metadata(path)
 return dict(value,**(image_file(path,mounts) if value.get('kind')=='file' else image_mount(path,mounts)))
need(os.geteuid()==65532 and os.getegid()==65532 and platform.machine()=='aarch64')
raw,info=safe('/etc/resolv.conf',65536);text=raw.decode('ascii');need('\0' not in text)
mounts=[]
mountinfo=open('/proc/self/mountinfo','rb').read(65537);need(len(mountinfo)<=65536)
for line in mountinfo.decode('ascii').splitlines():
 fields=line.split();separator=fields.index('-')
 mounts.append(dict(id=fields[0],root=fields[3],path=fields[4],options=fields[5],filesystem=fields[separator+1],source=fields[separator+2],super_options=fields[separator+3]))
resolver_mounts=[mount for mount in mounts if mount['path']=='/etc/resolv.conf']
need(len(resolver_mounts)==1 and 'ro' in resolver_mounts[0]['options'].split(',') and stat.S_IMODE(info.st_mode)==0o644)
search=[line.split()[1:] for line in text.splitlines() if line.startswith('search ')]
resolver=dict(device=info.st_dev,inode=info.st_ino,uid=info.st_uid,gid=info.st_gid,mode=stat.S_IMODE(info.st_mode),links=info.st_nlink,size=len(raw),bytes=text,sha256=hashlib.sha256(raw).hexdigest(),mount=resolver_mounts[0],search_prefix_bytes=sum(len(entry.encode('ascii'))+1 for entry in search[0][:6]) if len(search)==1 else None,closed=True)
image_paths=['/etc/passwd','/etc/nsswitch.conf','/usr/share/rpgmigrate/python-libpq-runtime.json','/usr/lib/aarch64-linux-gnu/libpq.so.5.18','/usr/lib/aarch64-linux-gnu/libc.so.6','/usr/bin/python3.13','/usr/local/bin/rpgmigrate']
files={path:image_file(path,mounts) for path in image_paths}
need(all(value['root_readonly'] and not value['shadow_mounts'] for value in files.values()))
need(not image_mount('/usr/bin/python3',mounts)['shadow_mounts'] and os.path.realpath(sys.executable)=='/usr/bin/python3.13')
admit_nss(files)
nss,_=safe('/etc/nsswitch.conf',65536);services=[]
for line in nss.decode('ascii').splitlines():
 line=line.split('#',1)[0].strip()
 if line.startswith('passwd:'):services.append(line.split(':',1)[1].split())
modules=sorted(set(glob.glob('/usr/lib/*/libnss_*.so*')+glob.glob('/lib/*/libnss_*.so*')))
lookup_safe=len(services)==1 and services[0]==['files']
need(lookup_safe)
home=os.environ.get('HOME');need(home is None or len(home.encode())<=1024 and '\0' not in home)
found=None;pwdhome=None
if lookup_safe:
 import pwd
 try:value=pwd.getpwuid(os.geteuid());found=True;pwdhome=value.pw_dir
 except KeyError:found=False
need(pwdhome is None or pwdhome.startswith('/') and len(pwdhome.encode())<=1024 and '\0' not in pwdhome)
home_result=dict(home_present='HOME' in os.environ,home=home,pwd_lookup_performed=lookup_safe,uid_found=found,uid_home=pwdhome,selected_home=home if home else pwdhome,pwd_lookup_files_only=lookup_safe)
metadata_raw,_=safe('/usr/share/rpgmigrate/python-libpq-runtime.json',4096);library_metadata=json.loads(metadata_raw)
need(library_metadata['architecture']=='arm64' and library_metadata['libpq_version']==180006)
library_path=library_metadata['library_path'];need(library_path.startswith('/usr/lib/aarch64-linux-gnu/libpq.so.5.') and os.path.realpath(library_path)==library_path)
library_raw,_=safe(library_path,16777216);need(hashlib.sha256(library_raw).hexdigest()==library_metadata['library_sha256'])
library=ctypes.CDLL(library_path);library.PQlibVersion.argtypes=[];library.PQlibVersion.restype=ctypes.c_int
need(library.PQlibVersion()==180006)
libc=ctypes.CDLL(None);libc.gnu_get_libc_version.argtypes=[];libc.gnu_get_libc_version.restype=ctypes.c_char_p
version=libc.gnu_get_libc_version().decode('ascii');need(version=='2.41')
engine=subprocess.run(['/usr/local/bin/rpgmigrate','version','--json'],capture_output=True,timeout=10,check=False)
need(engine.returncode==0 and not engine.stderr and 0<len(engine.stdout)<=65536)
engine_version=json.loads(engine.stdout)
packages=subprocess.run(['/usr/bin/dpkg-query','-W','-f=${Package}\t${Version}\t${Architecture}\t${Status}\n','libpq5','libc6','python3.13-minimal'],capture_output=True,timeout=5,check=False)
need(packages.returncode==0 and not packages.stderr and len(packages.stdout)<=4096)
package_lines=packages.stdout.decode('ascii').splitlines()
need('libpq5\t18.6-1.pgdg13+2\tarm64\tinstall ok installed' in package_lines and 'libc6\t2.41-12+deb13u4\tarm64\tinstall ok installed' in package_lines and 'python3.13-minimal\t3.13.5-2+deb13u5\tarm64\tinstall ok installed' in package_lines)
paths=['/etc/passwd','/etc/nsswitch.conf','/etc/krb5.conf','/etc/krb5.keytab','/tmp/krb5cc_65532','/usr/share/rpgmigrate/.native-file-absent','/etc/ssl/certs','/usr/lib/ssl/openssl.cnf','/etc/ssl/openssl.cnf','/etc/postgresql-common/pg_service.conf']
provider=dict(nss_passwd_services=services,nss_lookup_safe=lookup_safe,nss_modules=modules,default_paths={path:metadata(path) for path in paths},configuration_files={path:configuration(path,mounts) for path in ['/etc/krb5.conf','/etc/ssl/openssl.cnf']},environment_presence={key:key in os.environ for key in ['PGSERVICE','PGSERVICEFILE','PGSYSCONFDIR','KRB5_CONFIG','KRB5CCNAME','KRB5_KTNAME','KRB5_CLIENT_KTNAME','OPENSSL_CONF','OPENSSL_MODULES','OPENSSL_ENGINES','OAUTHBEARER_TOKEN']},authentication_qualified=False)
runtime=dict(uid=os.geteuid(),gid=os.getegid(),architecture=platform.machine(),python=platform.python_version(),glibc=version,libpq=library_metadata,packages=package_lines,engine_version=engine_version,image_files=files,observation_only=True,root_readonly=bool(os.statvfs('/').f_flag&os.ST_RDONLY),tmp_bytes=os.statvfs('/tmp').f_blocks*os.statvfs('/tmp').f_frsize)
need(runtime['root_readonly'] and runtime['tmp_bytes']<=67108864)
def self_process():
 need(os.geteuid()==65532 and os.getegid()==65532 and sorted(set(os.getgroups()+[os.getegid()]))==[65532])
 state={}
 for line in open('/proc/self/status').read(65537).splitlines():
  key,value=line.split(':',1);state[key]=value.strip()
 need(state.get('NoNewPrivs')=='1' and state.get('Seccomp')=='2')
 for key in ('CapInh','CapPrm','CapEff','CapBnd','CapAmb'):need(int(state[key],16)==0)
 fields=open('/proc/self/stat','rb').read(16385).rsplit(b') ',1)[1].split()
 return dict(pid=os.getpid(),start_ticks=int(fields[19]),mount_namespace=os.readlink('/proc/self/ns/mnt'))
def local_closure():
 need(open('/proc/self/mountinfo','rb').read(65537)==mountinfo)
 need({path:image_file(path,mounts) for path in image_paths}==files)
 admit_nss(files)
 return self_process()
process=local_closure()
print(json.dumps(dict(kind='metadata_ready',observation=dict(runtime=runtime,resolver=resolver,home=home_result,provider=provider),process=process),sort_keys=True,separators=(',',':')),flush=True)
limit=time.monotonic()+30
while True:
 need(time.monotonic()<limit)
 try:
  marker=os.lstat('/tmp/.native-metadata-release')
  need(stat.S_ISREG(marker.st_mode) and marker.st_uid==0 and stat.S_IMODE(marker.st_mode)==0o400 and marker.st_nlink==1 and marker.st_size==0)
  break
 except FileNotFoundError:time.sleep(min(.02,max(0,limit-time.monotonic())))
need(local_closure()==process)
print(json.dumps(dict(kind='metadata_complete',process=process,closure=True),sort_keys=True,separators=(',',':')),flush=True)
`

const nativeRuntimeNodeProgram = `import hashlib,json,math,os,re,selectors,stat,struct,subprocess,sys,time

def need(value):
 if not value:raise RuntimeError('active_profile_refused')

def strict(raw):
 need(type(raw) is bytes and 0<len(raw)<=1048576)
 def pairs(items):
  value={}
  for key,item in items:need(key not in value);value[key]=item
  return value
 try:value=json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _:need(False))
 except (ValueError,UnicodeError,RecursionError):raise RuntimeError('active_profile_refused') from None
 def bounded(item,depth=0):
  need(depth<=32)
  if isinstance(item,dict):
   for v in item.values():bounded(v,depth+1)
  elif isinstance(item,list):
   need(len(item)<=16384)
   for v in item:bounded(v,depth+1)
 bounded(value);need(type(value) is dict)
 return value

def fields(value,required,optional=()):
 need(type(value) is dict and set(required)<=set(value)<=set(required)|set(optional))

def stock_runtime():
 return dict(runtimeType='io.containerd.runc.v2',runtimePath='',sandboxer='podsandbox',baseRuntimeSpec='/etc/containerd/cri-base.json',PodAnnotations=None,ContainerAnnotations=None,cgroupWritable=False,privileged_without_host_devices=False,privileged_without_host_devices_all_devices_allowed=False,cniConfDir='',cniMaxConfNum=0,snapshotter='',io_type='',options=dict(NoNewKeyring=False,ShimCgroup='',IoUid=0,IoGid=0,BinaryName='',Root='',SystemdCgroup=True,CriuImagePath='',CriuWorkPath=''))

def active(raw):
 value=strict(raw);fields(value,('status','config'),('runtimeHandlers','features','golang','cniconfig','lastCNILoadStatus','lastCNILoadStatus.default'))
 status=value['status'];fields(status,('conditions',));conditions=status['conditions'];need(type(conditions) is list and len(conditions)==3)
 seen={}
 for item in conditions:
  fields(item,('type','status'),('reason','message'));kind=item['type']
  need(type(kind) is str and kind in ('RuntimeReady','NetworkReady','ContainerdHasNoDeprecationWarnings') and kind not in seen and type(item['status']) is bool)
  need(all(type(item[key]) is str for key in ('reason','message') if key in item));seen[kind]=item['status']
 need(seen.get('RuntimeReady') is True and seen.get('NetworkReady') is True)
 config=value['config'];fields(config,('containerd','containerdEndpoint'),('cni', 'enableSelinux', 'selinuxCategoryRange', 'maxContainerLogLineSize', 'disableApparmor', 'restrictOOMScoreAdj', 'disableProcMount', 'unsetSeccompProfile', 'tolerateMissingHugetlbController', 'disableHugetlbController', 'device_ownership_from_security_context', 'ignoreImageDefinedVolumes', 'netnsMountsUnderStateDir', 'enableUnprivilegedPorts', 'enableUnprivilegedICMP', 'enableCDI', 'cdiSpecDirs', 'drainExecSyncIOTimeout', 'ignoreDeprecationWarnings', 'statsCollectPeriod', 'statsRetentionPeriod', 'enableCRIU', 'enableExperimentalRestoreViaCreate', 'containerdRootDir', 'rootDir', 'stateDir'));need(config['containerdEndpoint']=='/run/containerd/containerd.sock')
 selected=config.get('containerd');need(type(selected) is dict and selected.get('defaultRuntimeName')=='runc' and type(selected.get('runtimes')) is dict)
 runtime=selected['runtimes'].get('runc');expected=stock_runtime();fields(runtime,expected)
 # Equal values alone would accept bools where the pinned schema requires integers.
 def equal(a,b):
  need(type(a) is type(b))
  if isinstance(b,dict):
   need(set(a)==set(b))
   for key in b:equal(a[key],b[key])
  else:need(a==b)
 equal(runtime,expected)
 return dict(default_runtime='runc',runtime_type='io.containerd.runc.v2',sandboxer='podsandbox',runtime_ready=True,network_ready=True,active_runtime_verified=True,stock_runtime_options=True,containerd_no_deprecation_warnings=seen['ContainerdHasNoDeprecationWarnings'])

def cri_prefix():
 return ['/usr/bin/env','-i','PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin','LC_ALL=C','LANG=C','/usr/local/bin/crictl','--config=/dev/null','--runtime-endpoint=unix:///run/containerd/containerd.sock','--image-endpoint=unix:///run/containerd/containerd.sock','--timeout=2s','--debug=false']

def expected_seccomp():
 return json.loads('{"defaultAction":"SCMP_ACT_ERRNO","architectures":["SCMP_ARCH_ARM","SCMP_ARCH_AARCH64"],"syscalls":[{"names":["accept","accept4","access","adjtimex","alarm","bind","brk","cachestat","capget","capset","chdir","chmod","chown","chown32","clock_adjtime","clock_adjtime64","clock_getres","clock_getres_time64","clock_gettime","clock_gettime64","clock_nanosleep","clock_nanosleep_time64","close","close_range","connect","copy_file_range","creat","dup","dup2","dup3","epoll_create","epoll_create1","epoll_ctl","epoll_ctl_old","epoll_pwait","epoll_pwait2","epoll_wait","epoll_wait_old","eventfd","eventfd2","execve","execveat","exit","exit_group","faccessat","faccessat2","fadvise64","fadvise64_64","fallocate","fanotify_mark","fchdir","fchmod","fchmodat","fchmodat2","fchown","fchown32","fchownat","fcntl","fcntl64","fdatasync","fgetxattr","flistxattr","flock","fork","fremovexattr","fsetxattr","fstat","fstat64","fstatat64","fstatfs","fstatfs64","fsync","ftruncate","ftruncate64","futex","futex_requeue","futex_time64","futex_wait","futex_waitv","futex_wake","futimesat","getcpu","getcwd","getdents","getdents64","getegid","getegid32","geteuid","geteuid32","getgid","getgid32","getgroups","getgroups32","getitimer","getpeername","getpgid","getpgrp","getpid","getppid","getpriority","getrandom","getresgid","getresgid32","getresuid","getresuid32","getrlimit","get_robust_list","getrusage","getsid","getsockname","getsockopt","get_thread_area","gettid","gettimeofday","getuid","getuid32","getxattr","getxattrat","inotify_add_watch","inotify_init","inotify_init1","inotify_rm_watch","io_cancel","ioctl","io_destroy","io_getevents","io_pgetevents","io_pgetevents_time64","ioprio_get","ioprio_set","io_setup","io_submit","ipc","kill","landlock_add_rule","landlock_create_ruleset","landlock_restrict_self","lchown","lchown32","lgetxattr","link","linkat","listen","listmount","listxattr","listxattrat","llistxattr","_llseek","lremovexattr","lseek","lsetxattr","lsm_get_self_attr","lsm_list_modules","lsm_set_self_attr","lstat","lstat64","madvise","membarrier","memfd_create","memfd_secret","mincore","mkdir","mkdirat","mknod","mknodat","mlock","mlock2","mlockall","map_shadow_stack","mmap","mmap2","mprotect","mq_getsetattr","mq_notify","mq_open","mq_timedreceive","mq_timedreceive_time64","mq_timedsend","mq_timedsend_time64","mq_unlink","mremap","mseal","msgctl","msgget","msgrcv","msgsnd","msync","munlock","munlockall","munmap","name_to_handle_at","nanosleep","newfstatat","_newselect","open","openat","openat2","pause","pidfd_open","pidfd_send_signal","pipe","pipe2","pkey_alloc","pkey_free","pkey_mprotect","poll","ppoll","ppoll_time64","prctl","pread64","preadv","preadv2","prlimit64","process_mrelease","pselect6","pselect6_time64","pwrite64","pwritev","pwritev2","read","readahead","readlink","readlinkat","readv","recv","recvfrom","recvmmsg","recvmmsg_time64","recvmsg","remap_file_pages","removexattr","removexattrat","rename","renameat","renameat2","restart_syscall","rmdir","rseq","rt_sigaction","rt_sigpending","rt_sigprocmask","rt_sigqueueinfo","rt_sigreturn","rt_sigsuspend","rt_sigtimedwait","rt_sigtimedwait_time64","rt_tgsigqueueinfo","sched_getaffinity","sched_getattr","sched_getparam","sched_get_priority_max","sched_get_priority_min","sched_getscheduler","sched_rr_get_interval","sched_rr_get_interval_time64","sched_setaffinity","sched_setattr","sched_setparam","sched_setscheduler","sched_yield","seccomp","select","semctl","semget","semop","semtimedop","semtimedop_time64","send","sendfile","sendfile64","sendmmsg","sendmsg","sendto","setfsgid","setfsgid32","setfsuid","setfsuid32","setgid","setgid32","setgroups","setgroups32","setitimer","setpgid","setpriority","setregid","setregid32","setresgid","setresgid32","setresuid","setresuid32","setreuid","setreuid32","setrlimit","set_robust_list","setsid","setsockopt","set_thread_area","set_tid_address","setuid","setuid32","setxattr","setxattrat","shmat","shmctl","shmdt","shmget","shutdown","sigaltstack","signalfd","signalfd4","sigprocmask","sigreturn","socketcall","socketpair","splice","stat","stat64","statfs","statfs64","statmount","statx","symlink","symlinkat","sync","sync_file_range","syncfs","sysinfo","tee","tgkill","time","timer_create","timer_delete","timer_getoverrun","timer_gettime","timer_gettime64","timer_settime","timer_settime64","timerfd_create","timerfd_gettime","timerfd_gettime64","timerfd_settime","timerfd_settime64","times","tkill","truncate","truncate64","ugetrlimit","umask","uname","unlink","unlinkat","uretprobe","utime","utimensat","utimensat_time64","utimes","vfork","vmsplice","wait4","waitid","waitpid","write","writev"],"action":"SCMP_ACT_ALLOW"},{"names":["socket"],"action":"SCMP_ACT_ALLOW","args":[{"index":0,"value":38,"op":"SCMP_CMP_LT"}]},{"names":["socket"],"action":"SCMP_ACT_ALLOW","args":[{"index":0,"value":39,"op":"SCMP_CMP_EQ"}]},{"names":["socket"],"action":"SCMP_ACT_ALLOW","args":[{"index":0,"value":40,"op":"SCMP_CMP_GT"}]},{"names":["personality"],"action":"SCMP_ACT_ALLOW","args":[{"index":0,"value":0,"op":"SCMP_CMP_EQ"}]},{"names":["personality"],"action":"SCMP_ACT_ALLOW","args":[{"index":0,"value":8,"op":"SCMP_CMP_EQ"}]},{"names":["personality"],"action":"SCMP_ACT_ALLOW","args":[{"index":0,"value":131072,"op":"SCMP_CMP_EQ"}]},{"names":["personality"],"action":"SCMP_ACT_ALLOW","args":[{"index":0,"value":131080,"op":"SCMP_CMP_EQ"}]},{"names":["personality"],"action":"SCMP_ACT_ALLOW","args":[{"index":0,"value":4294967295,"op":"SCMP_CMP_EQ"}]},{"names":["process_vm_readv","process_vm_writev","ptrace"],"action":"SCMP_ACT_ALLOW"},{"names":["arm_fadvise64_64","arm_sync_file_range","sync_file_range2","breakpoint","cacheflush","set_tls"],"action":"SCMP_ACT_ALLOW"},{"names":["clone"],"action":"SCMP_ACT_ALLOW","args":[{"index":0,"value":2114060288,"op":"SCMP_CMP_MASKED_EQ"}]},{"names":["clone3"],"action":"SCMP_ACT_ERRNO","errnoRet":38}]}')

def stock_base():
 return json.loads('{"ociVersion":"1.3.0","process":{"user":{"uid":0,"gid":0},"cwd":"/","capabilities":{"bounding":["CAP_CHOWN","CAP_DAC_OVERRIDE","CAP_FSETID","CAP_FOWNER","CAP_MKNOD","CAP_NET_RAW","CAP_SETGID","CAP_SETUID","CAP_SETFCAP","CAP_SETPCAP","CAP_NET_BIND_SERVICE","CAP_SYS_CHROOT","CAP_KILL","CAP_AUDIT_WRITE"],"effective":["CAP_CHOWN","CAP_DAC_OVERRIDE","CAP_FSETID","CAP_FOWNER","CAP_MKNOD","CAP_NET_RAW","CAP_SETGID","CAP_SETUID","CAP_SETFCAP","CAP_SETPCAP","CAP_NET_BIND_SERVICE","CAP_SYS_CHROOT","CAP_KILL","CAP_AUDIT_WRITE"],"permitted":["CAP_CHOWN","CAP_DAC_OVERRIDE","CAP_FSETID","CAP_FOWNER","CAP_MKNOD","CAP_NET_RAW","CAP_SETGID","CAP_SETUID","CAP_SETFCAP","CAP_SETPCAP","CAP_NET_BIND_SERVICE","CAP_SYS_CHROOT","CAP_KILL","CAP_AUDIT_WRITE"]},"noNewPrivileges":true},"root":{"path":"rootfs"},"mounts":[{"destination":"/proc","type":"proc","source":"proc","options":["nosuid","noexec","nodev"]},{"destination":"/dev","type":"tmpfs","source":"tmpfs","options":["nosuid","strictatime","mode=755","size=65536k"]},{"destination":"/dev/pts","type":"devpts","source":"devpts","options":["nosuid","noexec","newinstance","ptmxmode=0666","mode=0620","gid=5"]},{"destination":"/dev/shm","type":"tmpfs","source":"shm","options":["nosuid","noexec","nodev","mode=1777","size=65536k"]},{"destination":"/dev/mqueue","type":"mqueue","source":"mqueue","options":["nosuid","noexec","nodev"]},{"destination":"/sys","type":"sysfs","source":"sysfs","options":["nosuid","noexec","nodev","ro"]},{"destination":"/run","type":"tmpfs","source":"tmpfs","options":["nosuid","strictatime","mode=755","size=65536k"]}],"linux":{"resources":{"devices":[{"allow":false,"access":"rwm"}]},"cgroupsPath":"/default","namespaces":[{"type":"pid"},{"type":"ipc"},{"type":"uts"},{"type":"mount"},{"type":"network"}],"maskedPaths":["/proc/acpi","/proc/asound","/proc/kcore","/proc/keys","/proc/latency_stats","/proc/timer_list","/proc/timer_stats","/proc/sched_debug","/sys/firmware","/sys/devices/virtual/powercap","/proc/scsi"],"readonlyPaths":["/proc/bus","/proc/fs","/proc/irq","/proc/sys","/proc/sysrq-trigger"]},"hooks":{"createContainer":[{"path":"/kind/bin/mount-product-files.sh"}]}}')

def environment(rows):
 need(type(rows) is list and len(rows)<=128);result={}
 for row in rows:
  need(type(row) is str and '\0' not in row and '=' in row)
  key,value=row.split('=',1);need(re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',key) and key not in result);result[key]=value
 return result

def normalized_seccomp(value):
 fields(value,('defaultAction','architectures','syscalls'),('defaultErrnoRet','flags','listenerPath','listenerMetadata'))
 need(value.get('flags',[])==[] and value.get('listenerPath','')==value.get('listenerMetadata','')=='')
 need(type(value['architectures']) is list and type(value['syscalls']) is list)
 result=dict(defaultAction=value['defaultAction'],architectures=value['architectures'],syscalls=[],defaultErrnoRet=value.get('defaultErrnoRet'))
 for row in value['syscalls']:
  fields(row,('names','action'),('errnoRet','args'));need(type(row['names']) is list and all(type(n) is str for n in row['names']) and type(row['action']) is str)
  out=dict(names=row['names'],action=row['action'],errnoRet=row.get('errnoRet'),args=[])
  if out['errnoRet'] is not None:need(type(out['errnoRet']) is int)
  for arg in row.get('args',[]):
   fields(arg,('index','value','op'),('valueTwo',));need(type(arg['index']) is type(arg['value']) is int and type(arg.get('valueTwo',0)) is int and type(arg['op']) is str)
   out['args'].append(dict(arg,valueTwo=arg.get('valueTwo',0)))
  result['syscalls'].append(out)
 return result

def checked_spec(value,e,sandbox):
 fields(value,('ociVersion','process','root','mounts','linux','hooks','annotations'),('hostname',))
 need(value['ociVersion']=='1.3.0' and value['root']==dict(path='rootfs',readonly=True) and value['hooks']==dict(createContainer=[dict(path='/kind/bin/mount-product-files.sh')]))
 need(value.get('hostname','') in ('',e['pod_name']))
 process=value['process'];fields(process,('user','args','env','cwd','capabilities','noNewPrivileges','oomScoreAdj'),('terminal','rlimits','apparmorProfile','selinuxLabel'))
 need(process.get('terminal',False) is False and process.get('rlimits',[])==[] and process.get('apparmorProfile','')==process.get('selinuxLabel','')=='')
 need(process['noNewPrivileges'] is True and type(process['oomScoreAdj']) is int and process['oomScoreAdj']==e['oom_score_adj'])
 need(type(process['args']) is list and process['args']==e['command'] and environment(process['env'])==e['environment'])
 uid=26 if e['profile'] in ('pg16','pg18') else 65532;groups=[26,101] if uid==26 else [65532]
 user=process['user'];fields(user,('uid','gid'),('additionalGids','username'))
 need(type(user['uid']) is type(user['gid']) is int and user['uid']==user['gid']==uid and user.get('username','')=='')
 need(type(user.get('additionalGids',[])) is list and all(type(g) is int for g in user.get('additionalGids',[])) and sorted(set([uid]+user.get('additionalGids',[])))==groups)
 need(process['cwd']==('/' if uid==26 else '/work'))
 caps=process['capabilities'];fields(caps,(),('bounding','effective','inheritable','permitted','ambient'));need(all(type(v) is list and not v for v in caps.values()))
 linux=value['linux'];fields(linux,('namespaces','resources','cgroupsPath','maskedPaths','readonlyPaths','seccomp'),('devices','sysctl','rootfsPropagation','uidMappings','gidMappings'))
 need(linux.get('devices',[])==[] and linux.get('sysctl',{})=={} and linux.get('rootfsPropagation','')=='' and linux.get('uidMappings',[])==linux.get('gidMappings',[])==[])
 need(normalized_seccomp(linux['seccomp'])==normalized_seccomp(expected_seccomp()))
 baseline=stock_base()['linux']
 # kubelet v1.36.4 supplies maskedPaths (pkg/securitycontext defaultMaskedPaths), which adds /proc/interrupts to the base spec.
 masked=['/proc/asound','/proc/acpi','/proc/interrupts','/proc/kcore','/proc/keys','/proc/latency_stats','/proc/timer_list','/proc/timer_stats','/proc/sched_debug','/proc/scsi','/sys/firmware','/sys/devices/virtual/powercap']
 for name,paths in (('maskedPaths',masked),('readonlyPaths',baseline['readonlyPaths'])):
  need(type(linux[name]) is list and len(linux[name])==len(set(linux[name])) and set(linux[name])==set(paths))
 ns=linux['namespaces'];need(type(ns) is list and len(ns)==6);seen=set()
 for row in ns:
  fields(row,('type',),('path',));kind=row['type'];need(kind in ('pid','ipc','uts','mount','network','cgroup') and kind not in seen);seen.add(kind)
  expected='/proc/'+str(sandbox['sandbox_pid'])+'/ns/'+('net' if kind=='network' else kind) if kind in ('ipc','uts','network') else ''
  need(row.get('path','')==expected)
 resources=linux['resources'];fields(resources,('devices','cpu','memory'),('unified',))
 need(resources['devices']==baseline['resources']['devices'] and resources['cpu']==dict(period=100000,quota=100000,shares=256))
 need(resources['memory']==dict(limit=536870912,swap=536870912) and resources.get('unified',{}) in ({'memory.oom.group':'1'},{'memory.oom.group':'1','memory.swap.max':'0'}))
 need(linux['cgroupsPath']=='kubelet-kubepods-burstable-pod'+e['pod_uid'].replace('-','_')+'.slice:cri-containerd:'+e['container_id'])
 annotations={'io.kubernetes.cri.'+k:v for k,v in {'sandbox-id':sandbox['sandbox_id'],'sandbox-namespace':e['namespace'],'sandbox-uid':e['pod_uid'],'sandbox-name':e['pod_name'],'container-name':e['container_name'],'image-name':e['image_ref'],'container-type':'container'}.items()}
 need(value['annotations']==annotations)
 expected={m['destination']:m for m in stock_base()['mounts'] if m['destination'] not in ('/run','/dev/shm')}
 expected['/sys/fs/cgroup']=dict(destination='/sys/fs/cgroup',type='cgroup',source='cgroup',options=['nosuid','noexec','nodev','relatime','ro'])
 sid=sandbox['sandbox_id'];root='/var/lib/kubelet/pods/'+e['pod_uid'];cri='/var/lib/containerd/io.containerd.grpc.v1.cri'
 binds=[dict(destination='/dev/shm',source='/run/containerd/io.containerd.grpc.v1.cri/sandboxes/'+sid+'/shm',readonly=False),dict(destination='/etc/hostname',source=cri+'/sandboxes/'+sid+'/hostname',readonly=True),dict(destination='/etc/resolv.conf',source=cri+'/sandboxes/'+sid+'/resolv.conf',readonly=True),dict(destination='/etc/hosts',source=root+'/etc-hosts',readonly=False)]+e['mounts']
 for row in binds:
  target=row['destination'];need(target not in expected);expected[target]=dict(destination=target,type='bind',source=row['source'],options=['rbind','rprivate','ro' if row['readonly'] else 'rw'])
 mounts=value['mounts'];need(type(mounts) is list and len(mounts)==len(expected)+1);seen=set();binding=dict(termination_log_id=None,image_volume_id=None)
 for mount in mounts:
  fields(mount,('destination','type','source','options'));target=mount['destination'];need(type(target) is str and target not in seen and os.path.normpath(target)==target and target.startswith('/'));seen.add(target)
  if target=='/dev/termination-log':
   match=re.fullmatch(re.escape(root+'/containers/'+e['container_name']+'/')+'([0-9a-f]{8})',mount['source']);need(match is not None);binding['termination_log_id']=match[1]
   need(mount['type']=='bind' and mount['options']==['rbind','rprivate','rw'])
  else:need(target in expected and mount==expected[target])
 need(set(expected)<=seen and binding['termination_log_id'] is not None)
 if e.get('previous') is not None:
  for key,value in binding.items():need(e['previous'][key]==value)
 return binding

def proc_values(e,args,env,status):
 need(type(args) is bytes and type(env) is bytes and args.endswith(b'\0') and env.endswith(b'\0'))
 try:
  actual_args=[part.decode('utf-8') for part in args[:-1].split(b'\0')]
  actual_env=environment([part.decode('utf-8') for part in env[:-1].split(b'\0')])
 except UnicodeError:raise RuntimeError('active_profile_refused') from None
 need(actual_args==e['running_args'] and actual_env==e['process_environment'])
 uid=26 if e['profile'] in ('pg16','pg18') else 65532
 for key in ('Uid','Gid'):need(status.get(key,'').split()==[str(uid)]*4)
 need(sorted(set(status.get('Groups','').split()+[str(uid)]))==(['101','26'] if uid==26 else ['65532']))
 for key in ('CapInh','CapPrm','CapEff','CapBnd','CapAmb'):need(re.fullmatch('[0-9a-fA-F]{1,16}',status.get(key,'')) and int(status[key],16)==0)
 need(status.get('NoNewPrivs')=='1' and status.get('Seccomp')=='2')

def same_incarnation(before,after):
 for key in ('container_id','pod_uid','sandbox_id','pid','start_ticks','boot_id','mount_namespace'):
  need(type(before.get(key)) is type(after.get(key)) and before[key]==after[key])

def release_root(mounts,e):
 need(type(e.get('pod_uid')) is str and re.fullmatch('[a-z0-9-]{1,64}',e['pod_uid']))
 selected=[row for row in mounts if row.get('destination')=='/tmp'];need(len(selected)==1)
 root='/var/lib/kubelet/pods/'+e['pod_uid']+'/volumes/kubernetes.io~empty-dir/tmp'
 need(selected[0]==dict(destination='/tmp',source=root,type='bind',options=['rbind','rprivate','rw']))
 return root

def container_value(raw,e):
 value=strict(raw);fields(value,('status','info'));status=value['status'];need(type(status) is dict and status.get('id')==e['container_id'] and status.get('labels',{}).get('io.kubernetes.pod.uid')==e['pod_uid'])
 info=value['info'];fields(info,('sandboxID','pid','removing','snapshotKey','snapshotter','runtimeType','runtimeOptions','config','runtimeSpec'))
 need(re.fullmatch('[0-9a-f]{64}',info['sandboxID']) and info['removing'] is False and info['runtimeType']=='io.containerd.runc.v2' and info['runtimeOptions']==dict(systemd_cgroup=True) and info['snapshotter']=='overlayfs' and info['snapshotKey']==e['container_id'])
 config=info['config'];need(type(config) is dict and config.get('metadata',{}).get('name')==e['container_name'] and config.get('metadata',{}).get('attempt',0)==0 and config.get('stdin',False) is False and config.get('tty',False) is False)
 linux=config.get('linux',{});security=linux.get('security_context',linux.get('securityContext',{}));need(type(security) is dict and not security.get('privileged',False))
 return value

def sandbox_value(raw,e,sid):
 value=strict(raw);fields(value,('status','info'));status=value['status'];info=value['info']
 need(type(status) is dict and status.get('id')==sid and status.get('state')=='SANDBOX_READY' and status.get('labels',{}).get('io.kubernetes.pod.uid')==e['pod_uid'])
 need(type(info) is dict and info.get('runtimeType')=='io.containerd.runc.v2' and info.get('runtimeOptions')==dict(systemd_cgroup=True) and info.get('processStatus')=='running' and info.get('netNamespaceClosed') is False)
 pid=info.get('pid');need(type(pid) is int and 1<pid<4194304)
 config=info.get('config');need(type(config) is dict and config.get('metadata',{}).get('uid')==e['pod_uid'] and config['metadata'].get('name')==e['pod_name'] and config['metadata'].get('namespace')==e['namespace'])
 metadata=info.get('sandboxMetadata',{});need(type(metadata) is dict and metadata.get('RuntimeHandler',metadata.get('runtimeHandler','')) in ('','runc'))
 spec=info.get('runtimeSpec');need(type(spec) is dict and spec.get('hooks') in (None,{}))
 namespaces=spec.get('linux',{}).get('namespaces');need(type(namespaces) is list and len(namespaces)==5 and all(type(n) is dict for n in namespaces) and {n.get('type') for n in namespaces}=={'pid','ipc','uts','mount','network'})
 for namespace in namespaces:
  fields(namespace,('type',),('path',));path=namespace.get('path','')
  if namespace['type']=='network':need(type(path) is str and re.fullmatch('/var/run/netns/cni-[a-z0-9-]{1,64}',path))
  else:need(path=='')
 return dict(sandbox_id=sid,sandbox_pid=pid)

def terminal_result(e,value,sandbox,binding):
 previous=e['previous'];need(type(previous) is dict and value['status'].get('state')=='CONTAINER_EXITED' and value['status'].get('exitCode')==0 and value['info']['pid']==0 and previous['sandbox_id']==sandbox['sandbox_id'])
 pid=previous['pid'];need(type(pid) is int and 1<pid<4194304)
 meta=primitive('absence',pid,previous['start_ticks'])['meta'];fields(meta,('absent',));need(meta['absent']=='1')
 boot=platform_files()[2]
 need(boot==previous['boot_id'])
 result=dict(previous,terminal_pid_absent=True,live_process_verified=False,actual_mounts_verified=False,actual_environment_verified=False,resource_bounds_verified=False,last_admitted_process_verified=True,state='terminal')
 need(previous['sandbox_pid']==sandbox['sandbox_pid'])
 for key,item in binding.items():need(previous[key]==item)
 return result

def projected(e):
 first=container_value(run('inspect',e['container_id']),e);sandbox=sandbox_value(run('inspectp',first['info']['sandboxID']),e,first['info']['sandboxID'])
 binding=checked_spec(first['info']['runtimeSpec'],e,sandbox)
 if e['mode']=='terminal':return terminal_result(e,first,sandbox,binding)
 need(first['status'].get('state')=='CONTAINER_RUNNING')
 before=process_snapshot(e,first,sandbox)
 second=container_value(run('inspect',e['container_id']),e);need(second['info']['sandboxID']==sandbox['sandbox_id'] and second['info']['pid']==before['pid'] and second['status'].get('state')=='CONTAINER_RUNNING')
 checked_spec(second['info']['runtimeSpec'],e,sandbox);after=process_snapshot(e,second,sandbox);same_incarnation(before,after)
 result=dict(after,**binding,sandbox_pid=sandbox['sandbox_pid'],state='running',live_process_verified=True,terminal_pid_absent=False,final_spec_verified=True,actual_mounts_verified=True,actual_environment_verified=True,resource_bounds_verified=True,stock_hook_verified=True,release_created=False,last_admitted_process_verified=True)
 if e.get('previous') is not None:same_incarnation(e['previous'],result)
 if e['mode']=='metadata_release':
  need(e['profile']=='metadata' and type(e.get('previous')) is dict);release_marker(e,second,result)
  final=container_value(run('inspect',e['container_id']),e);checked_spec(final['info']['runtimeSpec'],e,sandbox)
  if final['status'].get('state')=='CONTAINER_RUNNING':
   same_incarnation(result,process_snapshot(e,final,sandbox))
  else:
   checked=dict(e,previous=result);result=terminal_result(checked,final,sandbox,binding)
  result['release_created']=True
 return result

def host_command(args):
 need(min(STOP_MONO-time.monotonic(),STOP_EPOCH-time.time())>0)
 child=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 data={child.stdout:bytearray(),child.stderr:bytearray()};selector=selectors.DefaultSelector()
 try:
  for stream in data:selector.register(stream,selectors.EVENT_READ)
  while selector.get_map():
   remaining=min(STOP_MONO-time.monotonic(),STOP_EPOCH-time.time());need(remaining>0)
   for key,_ in selector.select(remaining):
    raw=os.read(key.fileobj.fileno(),65536)
    if not raw:selector.unregister(key.fileobj);continue
    need(len(data[key.fileobj])+len(raw)<=1048576);data[key.fileobj].extend(raw)
  need(child.wait(timeout=max(.001,min(STOP_MONO-time.monotonic(),STOP_EPOCH-time.time())))==0 and not data[child.stderr])
  raw=bytes(data[child.stdout]);need(raw);return raw
 finally:
  selector.close()
  if child.poll() is None:child.kill();child.wait()
  child.stdout.close();child.stderr.close()

def run(action,identifier=None):
 need(action in ('info','inspect','inspectp') and (identifier is None if action=='info' else type(identifier) is str and re.fullmatch('[0-9a-f]{64}',identifier)))
 args=['podman','exec',NODE]+cri_prefix()+[action,'-o','json']+([] if identifier is None else [identifier])
 return host_command(args)

def decode_primitive(raw,action):
 need(type(raw) is bytes and len(raw)<=1048576 and raw[:6]==b'M6FD01');at=6
 def number():
  nonlocal at
  need(at+4<=len(raw));value=struct.unpack('!I',raw[at:at+4])[0];at+=4;return value
 need(number()==8)
 caps={'stat_before':16384,'status':65536,'cmdline':262144,'environ':262144,'mountinfo':65536,'cgroup':16384,'stat_after':16384,'meta':65536};result={}
 for expected,cap in caps.items():
  size=number();need(size==len(expected) and raw[at:at+size]==expected.encode('ascii'));at+=size
  length=number();need(length<=cap and at+length<=len(raw));result[expected]=raw[at:at+length];at+=length
 need(at==len(raw) and result['meta'])
 if action=='probe':need(all(result[name] for name in caps))
 else:need(all(not result[name] for name in caps if name!='meta'))
 result['meta']=strict(result['meta']);return result

def primitive(action,*values):
 counts={'platform':0,'probe':4,'release':6,'absence':2};need(action in counts and len(values)==counts[action])
 for index,value in enumerate(values):
  if index==1 and action in ('probe','release'):need(type(value) is str and re.fullmatch('[a-z0-9-]{1,64}',value))
  else:need(type(value) in (int,str) and re.fullmatch('[0-9]{1,20}',str(value)) and int(value)<=18446744073709551615)
 if action in ('probe','release','absence'):need(1<int(values[0])<4194304)
 args=['podman','exec',NODE,'/usr/bin/env','-i','PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin','LC_ALL=C','LANG=C','/usr/bin/perl','-f','-T','-e',perl_source(),action]+[str(v) for v in values]+[str(int(min(STOP_EPOCH,time.time()+STOP_MONO-time.monotonic())))]
 need(len(perl_source().encode())<=122880 and len(perl_source().encode())+1<=126976)
 return decode_primitive(host_command(args),action)

def platform_files():
 meta=primitive('platform')['meta'];fields(meta,('socket_device','socket_inode','files_verified','boot_id'))
 need(meta['files_verified']=='1' and all(type(meta[k]) is str and re.fullmatch('[0-9]{1,20}',meta[k]) for k in ('socket_device','socket_inode')) and re.fullmatch('[0-9a-f-]{36}',meta['boot_id']))
 return meta['socket_device'],meta['socket_inode'],meta['boot_id']

def process_ticks(raw):
 pieces=raw.rsplit(b') ',1);need(len(pieces)==2 and len(pieces[1].split())>19)
 value=pieces[1].split()[19];need(re.fullmatch(b'[0-9]{1,19}',value));return int(value)

def proc_mounts(raw,spec,e,meta):
 rows=[]
 for line in raw.decode('ascii').splitlines():
  words=line.split();mark=words.index('-');need(mark>=6 and len(words)>=mark+4)
  target=words[4];need(os.path.normpath(target)==target and target.startswith('/') and '\\' not in target)
  rows.append(dict(target=target,flags=words[5].split(','),fstype=words[mark+1]))
 need(0<len(rows)<=128)
 allowed={row['destination'] for row in spec['mounts']}|{'/'}|set(spec['linux']['maskedPaths'])|set(spec['linux']['readonlyPaths']);seen={}
 # runc inside the rootless kind node cannot mknod, so it bind-mounts its six default device nodes.
 devices={'/dev/null','/dev/zero','/dev/full','/dev/random','/dev/urandom','/dev/tty'}
 dmi=meta['dmi_pairs'];fields(dmi,('/sys/class/dmi/id/product_name','/sys/class/dmi/id/product_uuid','/sys/devices/virtual/dmi/id/product_name','/sys/devices/virtual/dmi/id/product_uuid'))
 for row in rows:
  target=row['target'];need(target in allowed or target in dmi or target in devices);seen[target]=seen.get(target,0)+1;need(seen[target]<=(2 if target in dmi and target.endswith('product_uuid') else 1))
  if target in devices:need(row['fstype']=='devtmpfs' and 'nosuid' in row['flags'] and 'noexec' in row['flags'])
  if target in dmi:need(dmi[target]=='1' and 'ro' in row['flags'])
 roots=[row for row in rows if row['target']=='/'];need(len(roots)==1 and roots[0]['fstype']=='overlay' and 'ro' in roots[0]['flags'])
 pairs=meta['volume_pairs'];fields(pairs,('tmp','client','server','pgdata'))
 for mount in spec['mounts']:
  actual=[row for row in rows if row['target']==mount['destination']];need(len(actual)==1);row=actual[0]
  if mount['type']=='bind':
   need(('ro' in row['flags'])==('ro' in mount['options']))
   names={'/tmp':'tmp','/etc/pgcopydb/tls/source':'client','/run/server-input':'server','/var/lib/postgresql':'pgdata'}
   if mount['destination'] in names:need(pairs[names[mount['destination']]]=='1')
  else:
   expected='cgroup2' if mount['type']=='cgroup' else mount['type'];need(row['fstype']==expected)
   for flag in ('ro','nosuid','nodev','noexec'):
    if flag in mount['options']:need(flag in row['flags'])
 need(type(meta['tmp_bytes']) is str and meta['tmp_bytes'].isdigit() and 0<int(meta['tmp_bytes'])<=67108864)
 return len(rows)

def pids_bound(scope,pod,ancestry):
 # The scope may carry systemd's TasksMax or max; only kubelet's podPidsLimit=4096 on the pod slice bounds it.
 need(pod=='4096' and type(ancestry) is list and len(ancestry)<=14);chain=[scope,pod]+ancestry
 need(all(type(v) is str and (v=='max' or re.fullmatch('[1-9][0-9]{0,18}',v)) for v in chain))
 return min(int(v) for v in chain if v!='max')

def process_snapshot(e,inspection,sandbox):
 pid=inspection['info'].get('pid');need(type(pid) is int and 1<pid<4194304)
 guard=0
 if e.get('previous') is not None:guard=e['previous']['start_ticks']
 elif e.get('self') is not None:guard=e['self']['start_ticks']
 frames=primitive('probe',pid,e['pod_uid'],sandbox['sandbox_pid'],guard);meta=frames['meta']
 fields(meta,('boot_id','mount_namespace','namespace_matches','binary_kind','tmp_device','tmp_inode','tmp_bytes','volume_pairs','dmi_pairs','cgroup_values','key_stat'))
 ticks=process_ticks(frames['stat_before']);need(ticks>0 and ticks==process_ticks(frames['stat_after']) and (guard==0 or ticks==guard))
 status={}
 for line in frames['status'].decode('ascii').splitlines():
  key,value=line.split(':',1);need(key not in status);status[key]=value.strip()
 proc_values(e,frames['cmdline'],frames['environ'],status)
 expected=e['profile'] if e['profile'] in ('pg16','pg18') else 'python313';need(meta['binary_kind']==expected)
 matches=meta['namespace_matches'];fields(matches,('mnt','pid','ipc','uts','net','cgroup','user'));need(all(value=='1' for value in matches.values()))
 need(re.fullmatch('mnt:\\[[0-9]+\\]',meta['mount_namespace']) and re.fullmatch('[0-9a-f-]{36}',meta['boot_id']))
 for key in ('tmp_device','tmp_inode'):need(type(meta[key]) is str and re.fullmatch('[0-9]{1,20}',meta[key]))
 mounts=proc_mounts(frames['mountinfo'],inspection['info']['runtimeSpec'],e,meta)
 cg=meta['cgroup_values'];fields(cg,('cpu.max','cpu.weight','memory.max','memory.swap.max','pids.max','parent.pids.max','ancestry.pids.max'))
 # runc maps 256 shares with its quadratic v2 formula: ceil(10**((64+125*8)/612-7/34)) == 35.
 need(cg['cpu.max']=='100000 100000' and cg['cpu.weight']=='35' and cg['memory.max']=='536870912' and cg['memory.swap.max']=='0')
 need(pids_bound(cg['pids.max'],cg['parent.pids.max'],cg['ancestry.pids.max'])<=4096)
 need(frames['cgroup'].decode('ascii').endswith('/kubelet-kubepods-burstable-pod'+e['pod_uid'].replace('-','_')+'.slice/cri-containerd-'+e['container_id']+'.scope\n'))
 if e['profile'] in ('pg16','pg18'):
  need(meta['key_stat']==dict(uid='0',gid='26',mode='288',links='1'))
 else:need(meta['key_stat'] is None)
 if e.get('self') is not None:need(e['self']['start_ticks']==ticks and e['self']['mount_namespace']==meta['mount_namespace'] and status.get('NSpid','').split()[-1]==str(e['self']['pid']))
 return dict(container_id=e['container_id'],pod_uid=e['pod_uid'],sandbox_id=sandbox['sandbox_id'],pid=pid,start_ticks=ticks,boot_id=meta['boot_id'],mount_namespace=meta['mount_namespace'],admitted_mount_count=mounts,tmp_device=meta['tmp_device'],tmp_inode=meta['tmp_inode'],key_stat_verified=e['profile'] in ('pg16','pg18'))

def release_marker(e,inspection,binding):
 release_root(inspection['info']['runtimeSpec']['mounts'],e)
 match=re.fullmatch('mnt:\\[([0-9]+)\\]',binding['mount_namespace']);need(match is not None)
 meta=primitive('release',binding['pid'],e['pod_uid'],binding['start_ticks'],match[1],binding['tmp_device'],binding['tmp_inode'])['meta'];fields(meta,('release_created','post_pid_absent'))
 need(meta['release_created']=='1' and meta['post_pid_absent'] in ('0','1'))

def perl_source():
 return r'''BEGIN { @INC = ('/usr/lib/aarch64-linux-gnu/perl-base'); }
use strict;
our $deadline;
$ENV{PATH}='/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin';
use Fcntl qw(:DEFAULT :mode);
sub need { die "refused\n" unless $_[0]; }
sub decimal { my ($s,$zero)=@_;need(defined($s) && $s =~ /\A(0|[1-9][0-9]{0,19})\z/);my $v=$1;need(length($v)<20 || $v le '18446744073709551615');need($zero || $v ne '0');return $v; }
sub pid { my $v=decimal($_[0],0);need($v>1 && $v<4194304);return $v; }
sub uid { need(defined($_[0]) && $_[0] =~ /\A([a-z0-9-]{1,64})\z/);return $1; }
sub jstr { my $s=$_[0];need(defined($s) && $s !~ /[^\x20-\x7e]/);$s =~ s/([\\"])/\\$1/g;return '"'.$s.'"'; }
sub json { my ($v)=@_;return 'null' unless defined($v);if(ref($v) eq 'HASH'){return '{'.join(',',map {jstr($_).':'.json($v->{$_})} sort keys %$v).'}';}if(ref($v) eq 'ARRAY'){return '['.join(',',map {json($_)} @$v).']';}return jstr($v); }
sub bounded { my ($fh,$cap)=@_;my $raw='';while(length($raw)<=$cap){my $n=sysread($fh,my $part,65536);need(defined($n));last if $n==0;$raw.=$part;need(length($raw)<=$cap);}return $raw; }
sub read_at { my ($dir,$leaf,$cap)=@_;need($leaf =~ /\A([a-z]+)\z/);my $name=$1;sysopen(my $fh,'/proc/'.$$.'/fd/'.fileno($dir).'/'.$name,O_RDONLY|O_NOFOLLOW) or need(0);my $raw=bounded($fh,$cap);close($fh) or need(0);need(length($raw)>0);return $raw; }
sub start { my $raw=$_[0];need($raw =~ /\) ([^\r\n]+)\n?\z/);my @parts=split / /,$1;need(@parts>19);return decimal($parts[19],0); }
sub directory { my $path=$_[0];need($path =~ /\A(\/var\/lib\/kubelet\/pods\/[a-z0-9-]{1,64}\/volumes\/kubernetes\.io~(?:empty-dir|secret)\/(?:tmp|pgdata|server-input|client-input))\z/);$path=$1;sysopen(my $current,'/',O_RDONLY|O_DIRECTORY|O_NOFOLLOW) or need(0);for my $part (split /\//,substr($path,1)){need($part =~ /\A([a-z0-9.~_-]+)\z/);$part=$1;sysopen(my $next,'/proc/'.$$.'/fd/'.fileno($current).'/'.$part,O_RDONLY|O_DIRECTORY|O_NOFOLLOW) or need(0);close($current) or need(0);$current=$next;}return $current; }
sub pair { my ($a,$b)=@_;my @x=stat($a);my @y=stat($b);return @x && @y && $x[0]==$y[0] && $x[1]==$y[1] ? '1':'0'; }
sub raw_file { my ($path,$cap)=@_;sysopen(my $fh,$path,O_RDONLY|O_NOFOLLOW) or need(0);my $raw=bounded($fh,$cap);close($fh) or need(0);need(length($raw)>0);return $raw; }
sub hash_fd { my ($fh)=@_;my $path='/proc/'.$$.'/fd/'.fileno($fh);open(my $pipe,'-|','/usr/bin/sha256sum',$path) or need(0);my $raw=bounded($pipe,256);close($pipe) or need(0);need($raw =~ /\A([0-9a-f]{64})  \/proc\/[0-9]+\/fd\/[0-9]+\n\z/);return $1; }
sub frame { my ($rows)=@_;my @names=qw(stat_before status cmdline environ mountinfo cgroup stat_after meta);print 'M6FD01',pack('N',8);my $total=10;my %cap=(stat_before=>16384,status=>65536,cmdline=>262144,environ=>262144,mountinfo=>65536,cgroup=>16384,stat_after=>16384,meta=>65536);for my $name (@names){my $raw=$rows->{$name}//'';need(length($raw)<=$cap{$name});$total+=8+length($name)+length($raw);need($total<=1048576);print pack('N',length($name)),$name,pack('N',length($raw)),$raw;} }
sub key_stat { my ($root)=@_;my $dir=directory($root);my @todo=('server.key');my $links=0;while(@todo){my $part=shift @todo;need($part =~ /\A([A-Za-z0-9_.-]{1,128})\z/ && $part ne '.' && $part ne '..');$part=$1;my $path='/proc/'.$$.'/fd/'.fileno($dir).'/'.$part;my @st=lstat($path);need(@st);if(S_ISLNK($st[2])){need(++$links<=8);my $link=readlink($path);need(defined($link) && $link =~ /\A([A-Za-z0-9_.\/-]{1,256})\z/ && substr($link,0,1) ne '/');my @next=split /\//,$1;need(!grep {$_ eq '..' || $_ eq '.' || $_ eq ''} @next);unshift @todo,@next;next;}if(@todo){need(S_ISDIR($st[2]));sysopen(my $next,$path,O_RDONLY|O_DIRECTORY|O_NOFOLLOW) or need(0);close($dir) or need(0);$dir=$next;next;}need(S_ISREG($st[2]) && $st[4]==0 && $st[5]==999 && ($st[2]&07777)==0440 && $st[3]==1);close($dir) or need(0);return {uid=>'0',gid=>'999',mode=>'288',links=>'1'};}need(0); }

sub probe {
 my ($p,$pod,$sandbox,$guard)=@_;sysopen(my $proc,'/proc/'.$p,O_RDONLY|O_DIRECTORY|O_NOFOLLOW) or need(0);my $before=read_at($proc,'stat',16384);my $ticks=start($before);need($guard eq '0' || $guard eq $ticks);
 my $root='/var/lib/kubelet/pods/'.$pod.'/volumes/';my $tmp=directory($root.'kubernetes.io~empty-dir/tmp');my $target='/proc/'.$p.'/root';need(pair($tmp,$target.'/tmp') eq '1');my @tmp=stat($tmp);my %pairs=(tmp=>'1');
 my %volume=(client=>['kubernetes.io~secret/client-input','/etc/pgcopydb/tls/source'],server=>['kubernetes.io~secret/server-input','/run/server-input'],pgdata=>['kubernetes.io~empty-dir/pgdata','/var/lib/postgresql']);
 for my $name (sort keys %volume){my ($source,$destination)=@{$volume{$name}};if(-d $root.$source){my $dir=directory($root.$source);$pairs{$name}=pair($dir,$target.$destination);close($dir) or need(0);}else{$pairs{$name}=undef;}}
 my $status=read_at($proc,'status',65536);my $args=read_at($proc,'cmdline',262144);my $env=read_at($proc,'environ',262144);my $mounts=read_at($proc,'mountinfo',65536);my $cgroup=read_at($proc,'cgroup',16384);
 my %ns;for my $kind(qw(mnt pid ipc uts net cgroup user)){my $link=readlink('/proc/'.$p.'/ns/'.$kind);need(defined($link) && $link =~ /\A([a-z]+:\[[0-9]+\])\z/);$ns{$kind}=$1;}
 my %matches;for my $kind(qw(net ipc uts)){my $other=readlink('/proc/'.$sandbox.'/ns/'.$kind);$matches{$kind}=defined($other) && $other eq $ns{$kind}?'1':'0';}
 for my $kind(qw(mnt pid cgroup user)){my $own=readlink('/proc/self/ns/'.$kind);$matches{$kind}=defined($own) && (($kind eq 'user')?($own eq $ns{$kind}):($own ne $ns{$kind}))?'1':'0';}
 my $exe=readlink('/proc/'.$p.'/exe');my %known=('/usr/bin/python3.13'=>['python313','273cb29e073b748379ba546affa562283ed795f84ff230349e12ce610e60975c'],'/usr/lib/postgresql/16/bin/postgres'=>['pg16','af0258f6b3cf8857f5fdd55e30f0fd07de1f0f90b8b625ca6fbf0e34f1359c0e'],'/usr/lib/postgresql/18/bin/postgres'=>['pg18','fc77699680778ae0440b4ea9669aa17773cb9641855f6a3f048c50c9fe39c765']);need(defined($exe) && exists($known{$exe}));$exe=($exe =~ /\A(\/[-a-zA-Z0-9_.\/]+)\z/)[0];
 sysopen(my $binary,'/proc/'.$p.'/exe',O_RDONLY) or need(0);my @binary=stat($binary);need(S_ISREG($binary[2]) && $binary[4]==0 && $binary[5]==0 && ($binary[2]&022)==0 && $binary[3]==1 && $binary[7]<=16777216 && pair($binary,$target.$exe) eq '1');need(hash_fd($binary) eq $known{$exe}[1]);close($binary) or need(0);
 my %dmi;for my $leaf(qw(product_name product_uuid)){for my $dir('/sys/class/dmi/id/','/sys/devices/virtual/dmi/id/'){$dmi{$dir.$leaf}=pair($target.'/'.$leaf,$target.$dir.$leaf);}}
 need($cgroup =~ /\A0::(\/[-a-zA-Z0-9_.\/@]+)\n\z/);my $path=$1;need(!grep {$_ eq '..'} split /\//,$path);my $under=$pod;$under =~ s/-/_/g;need($path =~ /\/kubelet-kubepods-burstable-pod\Q$under\E\.slice\/cri-containerd-[0-9a-f]{64}\.scope\z/);my %cg;
 for my $leaf(qw(cpu.max cpu.weight memory.max memory.swap.max pids.max)){$cg{$leaf}=raw_file('/sys/fs/cgroup'.$path.'/'.$leaf,128);$cg{$leaf} =~ s/\n\z//;}
 my $parent=$path;$parent =~ s/\/[^\/]+\z//;$cg{'parent.pids.max'}=raw_file('/sys/fs/cgroup'.$parent.'/pids.max',128);$cg{'parent.pids.max'} =~ s/\n\z//;
 my @up;my $level=$parent;while($level ne ''){$level =~ s/\/[^\/]+\z//;my $file='/sys/fs/cgroup'.$level.'/pids.max';last if $level eq '' && !-e $file;my $v=raw_file($file,128);$v =~ s/\n\z//;push @up,$v;}$cg{'ancestry.pids.max'}=\@up;
 open(my $statpipe,'-|','/usr/bin/stat','-f','-c','%b %S',$target.'/tmp') or need(0);my $space=bounded($statpipe,128);close($statpipe) or need(0);need($space =~ /\A([0-9]+) ([0-9]+)\n\z/);my $tmpbytes=$1*$2;
 my $boot=raw_file('/proc/sys/kernel/random/boot_id',64);need($boot =~ /\A([0-9a-f-]{36})\n\z/);$boot=$1;
 my $key=defined($pairs{server})?key_stat($root.'kubernetes.io~secret/server-input'):undef;
 my $after=read_at($proc,'stat',16384);need(start($after) eq $ticks);close($tmp) or need(0);close($proc) or need(0);
 my $meta={boot_id=>$boot,mount_namespace=>$ns{mnt},namespace_matches=>\%matches,binary_kind=>$known{$exe}[0],tmp_device=>"$tmp[0]",tmp_inode=>"$tmp[1]",tmp_bytes=>"$tmpbytes",volume_pairs=>\%pairs,dmi_pairs=>\%dmi,cgroup_values=>\%cg,key_stat=>$key};
 frame({stat_before=>$before,status=>$status,cmdline=>$args,environ=>$env,mountinfo=>$mounts,cgroup=>$cgroup,stat_after=>$after,meta=>json($meta)});
}
sub release {
 my ($p,$pod,$guard,$ns,$dev,$ino)=@_;sysopen(my $proc,'/proc/'.$p,O_RDONLY|O_DIRECTORY|O_NOFOLLOW) or need(0);need(start(read_at($proc,'stat',16384)) eq $guard);my $mnt=readlink('/proc/'.$p.'/ns/mnt');need(defined($mnt) && $mnt eq 'mnt:['.$ns.']');
 my $root='/var/lib/kubelet/pods/'.$pod.'/volumes/kubernetes.io~empty-dir/tmp';my $dir=directory($root);my @st=stat($dir);need("$st[0]" eq $dev && "$st[1]" eq $ino && pair($dir,'/proc/'.$p.'/root/tmp') eq '1');
 need(start(read_at($proc,'stat',16384)) eq $guard && time()<$deadline);umask 077;sysopen(my $leaf,'/proc/'.$$.'/fd/'.fileno($dir).'/.native-metadata-release',O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW,0400) or need(0);my @made=stat($leaf);need(S_ISREG($made[2]) && $made[4]==0 && ($made[2]&07777)==0400 && $made[7]==0 && $made[3]==1);close($leaf) or need(0);close($dir) or need(0);
 my $absent='0';if(-e '/proc/'.$p){need(start(read_at($proc,'stat',16384)) eq $guard);}else{my @gone=lstat('/proc/'.$p);need(!@gone && (0+$!)==2);$absent='1';}close($proc) or need(0);frame({meta=>json({release_created=>'1',post_pid_absent=>$absent})});
}
sub platform {
 my @files=(['/etc/containerd/cri-base.json','5d1898b2d65a2bb27ca989d9c59a32348b8f39f88bfbb7a65829f17757a98fe4',3648,420],['/kind/bin/mount-product-files.sh','78abf868c01046e85c39ed109a53a74705bced03feb1e524b68385f7d4b83edb',2455,493],['/usr/bin/cat','0d5a5dd543ed8d31220320eb513e4680ab303e48cb22a5edbf27a5a91561a71e',68088,493],['/usr/bin/env','a1a366aeec990c18d7ff97358e523925e449e2df224029fe048fc3ae35a97720',68464,493],['/usr/bin/perl','470d3a7bbd741953e05de861c3d06067118bd6c0991c4a7c60f008448fdf5afa',3813416,493],['/usr/bin/sha256sum','7911a1f0aec4678c540bd78ff886f48d42790c8689a82ed0ae25ed79ed5dd512',68056,493],['/usr/bin/stat','5e4061cf4a54cc95985750521258b2062799beb6ec36d5694cc0b155291d27f7',133600,493],['/usr/lib/aarch64-linux-gnu/perl-base/Carp.pm','b962eba0f4c605adb2ea941d562d7b303e1ad103b16243647cb658e59b71b48b',25399,420],['/usr/lib/aarch64-linux-gnu/perl-base/Carp/Heavy.pm','97fe3ef31e2c194a471e5ee2130ba5de716378a4dddc4908fa11c3450626416a',773,420],['/usr/lib/aarch64-linux-gnu/perl-base/Config.pm','1826e35e99ae11a46e74dd69220498f306320ad1db73e4594724f000d8ca41da',3328,420],['/usr/lib/aarch64-linux-gnu/perl-base/Config_heavy.pl','b87c9f72999728d26e6ed5e0d0e4b0eab49b6de32fcf888a696efddc18176a1c',59009,420],['/usr/lib/aarch64-linux-gnu/perl-base/DynaLoader.pm','2f4a6cd245c2d3f1b5851c334a913d3acdcaa4b32b62e54d78c8fe2d395f5777',11266,420],['/usr/lib/aarch64-linux-gnu/perl-base/Exporter.pm','e04d1732df51fbdbc27828c9bd3ce176e940ffb0c1051f404167b403170b76d4',2336,420],['/usr/lib/aarch64-linux-gnu/perl-base/Exporter/Heavy.pm','3ba07aad8c2d7a041cc6d6173a0c410df94764cb2d9fa5c708cd70599185cff2',6385,420],['/usr/lib/aarch64-linux-gnu/perl-base/Fcntl.pm','e3561ca4d473982a99e7330841b29ca2d52dfa7f1a465b304af724e89fc5a572',2218,420],['/usr/lib/aarch64-linux-gnu/perl-base/XSLoader.pm','6f3125299a06ee2f0eb2b70d54c9e3d8ca67ad1136d28b0336836e04430163af',4115,420],['/usr/lib/aarch64-linux-gnu/perl-base/auto/Fcntl/Fcntl.so','0a55b1f1da707ca3fd89400832c8f3455bcc0940943b3f125010026dea849ecd',67824,420],['/usr/lib/aarch64-linux-gnu/perl-base/constant.pm','5decbf923c0f5f065147a7a1a89fd351a26d5d5efd8799ba4ee992a1d00cd837',5737,420],['/usr/lib/aarch64-linux-gnu/perl-base/overload.pm','8abc5e5f156eda5a6d29c8d87d0546fd36ffaaaea72772e036410b507dd5fb52',4857,420],['/usr/lib/aarch64-linux-gnu/perl-base/overloading.pm','5387b33943963d2e47f05b3f37926f2d454f9ded33ca857e56ffb33b3adb999d',964,420],['/usr/lib/aarch64-linux-gnu/perl-base/strict.pm','0d071a91d5ec74ac18dd74234e19ebe7f61d9271292607cfcbd75f44b7684846',1606,420],['/usr/lib/aarch64-linux-gnu/perl-base/vars.pm','4df4ccb7cb05495f7d7b47c472bae0c92ee7b82aae0a2b31c23ca7bff9504163',1148,420],['/usr/lib/aarch64-linux-gnu/perl-base/warnings.pm','ee08e86d2ef593c2f52595c41fd8e8141642e833f63b4fe349759bbd0ec98ef4',30759,420],['/usr/lib/aarch64-linux-gnu/perl-base/warnings/register.pm','f04fde9d23befde91da2730f5c5066fb95fcc7caae499db0a62a4c989c6684f7',462,420],['/usr/local/bin/containerd','760388b0b4ada7a8c2e041a88eef37788b35716a2d1368a6e30350a3fc2757b9',40264432,493],['/usr/local/bin/containerd-shim-runc-v2','f7b7451e8ba3d65cb35a8e26103590b695f05a1e361b7f6266eb451e3508fbde',8126626,493],['/usr/local/bin/crictl','472793f1f32b45a91e4c9fd31b85a4c946e71bd39f4cf4ef9669381a99cbf647',37400042,493],['/usr/local/sbin/runc','dd65442affcecd360e214769f985bedd2848666211a6b08255252150b51c0f92',15004632,493]);
 for my $row(@files){my ($path,$digest,$size,$mode)=@$row;sysopen(my $fh,$path,O_RDONLY|O_NOFOLLOW) or need(0);my @a=stat($fh);my @path=lstat($path);need(@a && @path && S_ISREG($a[2]) && $a[4]==0 && $a[5]==0 && $a[3]==($path eq '/usr/bin/perl'?2:1) && $a[7]==$size && ($a[2]&07777)==$mode && $a[0]==$path[0] && $a[1]==$path[1]);if($path eq '/usr/bin/perl'){my @twin=lstat('/usr/bin/perl5.40.1');need(@twin && S_ISREG($twin[2]) && $twin[0]==$a[0] && $twin[1]==$a[1]);}need(hash_fd($fh) eq $digest);my @b=stat($fh);need(join(',',@a[0,1,2,3,4,5,7,9,10]) eq join(',',@b[0,1,2,3,4,5,7,9,10]));close($fh) or need(0);}
 my @null=stat('/dev/null');need(@null && S_ISCHR($null[2]) && $null[6]==259);my @socket=lstat('/run/containerd/containerd.sock');need(@socket && S_ISSOCK($socket[2]) && $socket[4]==0);
 frame({meta=>json({socket_device=>"$socket[0]",socket_inode=>"$socket[1]",files_verified=>'1',boot_id=>do{my $v=raw_file('/proc/sys/kernel/random/boot_id',64);need($v =~ /\A([0-9a-f-]{36})\n\z/);$1}})});
}
sub main {
 need(@ARGV>1);$deadline=decimal(pop @ARGV,0);need($deadline>1700000000 && $deadline<4102444800 && time()<$deadline);my $op=shift @ARGV;need($op =~ /\A(probe|release|absence|platform)\z/);$op=$1;
 if($op eq 'probe'){need(@ARGV==4);probe(pid($ARGV[0]),uid($ARGV[1]),pid($ARGV[2]),decimal($ARGV[3],1));}
 elsif($op eq 'release'){need(@ARGV==6);release(pid($ARGV[0]),uid($ARGV[1]),decimal($ARGV[2],0),decimal($ARGV[3],0),decimal($ARGV[4],1),decimal($ARGV[5],0));}
 elsif($op eq 'absence'){need(@ARGV==2);my $p=pid($ARGV[0]);decimal($ARGV[1],0);my @st=lstat('/proc/'.$p);need(!@st && (0+$!)==2);frame({meta=>json({absent=>'1'})});}
 elsif($op eq 'platform'){need(@ARGV==0);platform();}
}
local $SIG{__WARN__}=sub{die "refused\n"};local $SIG{ALRM}=sub{die "refused\n"};alarm 5;
my $ok=eval { main();1 };exit($ok?0:1);
'''

def main():
 global NODE,STOP_MONO,STOP_EPOCH
 need(len(sys.argv)==2 and re.fullmatch('[0-9a-f]{64}',sys.argv[1]));NODE=sys.argv[1]
 request=strict(sys.stdin.buffer.read(1048577));deadline=request.pop('__host_call_deadline',None);need(type(deadline) in (int,float) and math.isfinite(deadline) and 0<deadline-time.time()<=31);STOP_EPOCH=deadline;STOP_MONO=time.monotonic()+min(20,deadline-time.time());mode=request.get('mode');need(mode in ('node','running','terminal','metadata_release'))
 if mode=='node':fields(request,('mode',))
 else:
  fields(request,('mode','profile','container_id','pod_uid','pod_name','namespace','container_name','image_ref','command','running_args','environment','process_environment','oom_score_adj','mounts','previous','self'))
  need(request['profile'] in ('metadata','client16','client18','pg16','pg18') and re.fullmatch('[0-9a-f]{64}',request['container_id']) and re.fullmatch('[a-z0-9-]{1,64}',request['pod_uid']))
  for key in ('pod_name','namespace','container_name'):need(type(request[key]) is str and re.fullmatch('[a-z0-9-]{1,63}',request[key]))
  need(type(request['oom_score_adj']) is int and 3<=request['oom_score_adj']<=999 and type(request['environment']) is type(request['process_environment']) is dict)
  for key in ('command','running_args'):need(type(request[key]) is list and 1<=len(request[key])<=32 and all(type(arg) is str and '\0' not in arg and len(arg.encode())<=122880 for arg in request[key]))
 socket=platform_files();facts=active(run('info'))
 result=facts if mode=='node' else projected(request)
 need(platform_files()==socket);result.update(endpoint_verified=True,immutable_node_files_verified=True)
 print(json.dumps(result,sort_keys=True,separators=(',',':')))

if __name__=='__main__':
 try:main()
 except BaseException:os._exit(1)
`

type nativeRuntimeNode struct {
	Name        string    `json:"name"`
	ContainerID string    `json:"container_id"`
	UID         types.UID `json:"uid"`
}

type nativeRuntimeInput struct {
	SchemaVersion          int                 `json:"schema_version"`
	QualificationID        string              `json:"qualification_id"`
	Phase                  string              `json:"phase"`
	Pair                   *string             `json:"pair"`
	RunID                  string              `json:"run_id"`
	RunDir                 string              `json:"run_dir"`
	LegacySourceRoot       string              `json:"legacy_source_root"`
	LegacySourceSHA        string              `json:"legacy_source_tree_sha256"`
	LegacyGuardSHA         string              `json:"legacy_guard_sha256"`
	LegacyOwnershipSHA     string              `json:"legacy_ownership_sha256"`
	QualificationSourceSHA string              `json:"qualification_source_sha256"`
	EvidenceProgramSHA     string              `json:"evidence_program_sha256"`
	AssetsSHA              string              `json:"native_assets_sha256"`
	Kubeconfig             string              `json:"kubeconfig"`
	Context                string              `json:"context"`
	Namespace              string              `json:"namespace"`
	NamespaceUID           *types.UID          `json:"namespace_uid"`
	Nodes                  []nativeRuntimeNode `json:"nodes"`
	LeaseExpiry            float64             `json:"lease_expires_at"`
	StageDeadline          float64             `json:"stage_deadline"`
	RequiredCases          []string            `json:"required_cases"`
	ReceiptPath            string              `json:"receipt_path"`
	MetadataProgramSHA     string              `json:"metadata_program_sha256"`
}

type nativeRuntimeCleanupObject struct {
	APIVersion     string    `json:"api_version"`
	Kind           string    `json:"kind"`
	Namespace      string    `json:"namespace"`
	Name           string    `json:"name"`
	UID            types.UID `json:"uid"`
	Action         string    `json:"action"`
	VerifiedAbsent bool      `json:"verified_absent"`
}

type nativeRuntimeCleanup struct {
	Complete bool                         `json:"complete"`
	Objects  []nativeRuntimeCleanupObject `json:"objects"`
}

type nativeRuntimeCase struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	ExitCode int    `json:"exit_code"`
	Cleanup  bool   `json:"cleanup"`
}

type nativeRuntimeReceipt struct {
	SchemaVersion          int                  `json:"schema_version"`
	QualificationID        string               `json:"qualification_id"`
	Phase                  string               `json:"phase"`
	Pair                   *string              `json:"pair"`
	RunID                  string               `json:"run_id"`
	InputSHA               string               `json:"input_sha256"`
	QualificationSourceSHA string               `json:"qualification_source_sha256"`
	EvidenceProgramSHA     string               `json:"evidence_program_sha256"`
	AssetsSHA              string               `json:"native_assets_sha256"`
	MetadataProgramSHA     string               `json:"metadata_program_sha256"`
	MetadataSHA            string               `json:"metadata_sha256"`
	NamespaceUID           types.UID            `json:"namespace_uid"`
	StartedAt              float64              `json:"started_at"`
	FinishedAt             float64              `json:"finished_at"`
	Cases                  []nativeRuntimeCase  `json:"cases"`
	Cleanup                nativeRuntimeCleanup `json:"cleanup"`
}

type nativeRuntimeImageBinding struct {
	NodeID           string   `json:"node_id"`
	RequestedRef     string   `json:"requested_ref"`
	ManifestDigest   string   `json:"manifest_digest"`
	ConfigDigest     string   `json:"config_digest"`
	OS               string   `json:"os"`
	Architecture     string   `json:"architecture"`
	Variant          *string  `json:"variant"`
	AcceptedImageIDs []string `json:"accepted_image_ids"`
}

type nativeRuntimeImage struct {
	Ref          string
	Digest       string
	ConfigDigest string
	NodeBindings map[string]nativeRuntimeImageBinding
}

type nativeRuntimeOwned struct {
	object metav1.Object
	kind   string
}

func nativeRuntimeSHA(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func nativeRuntimeCases() []string {
	cases := make([]string, 0, 12)
	for index := range 3 {
		for _, name := range []string{nativeRuntimeRuntime, nativeRuntimeResolver, nativeRuntimeHome, nativeRuntimeProvider} {
			cases = append(cases, fmt.Sprintf("node_%d/%s", index, name))
		}
	}
	return cases
}

func nativeRuntimeDecode(raw []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var value func(int) error
	value = func(depth int) error {
		if depth > 32 {
			return errors.New("metadata JSON depth limit")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		seen := map[string]bool{}
		for decoder.More() {
			if delimiter == '{' {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				text, ok := key.(string)
				if !ok || seen[text] {
					return errors.New("metadata duplicate JSON field")
				}
				seen[text] = true
			}
			if err := value(depth + 1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("metadata JSON trailing value")
	}
	kind := reflect.TypeOf(output)
	if kind != nil && kind.Kind() == reflect.Pointer && kind.Elem().Kind() == reflect.Struct {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return err
		}
		shape := kind.Elem()
		if len(fields) != shape.NumField() {
			return errors.New("metadata required JSON field absent")
		}
		for field := range shape.Fields() {
			if _, exists := fields[field.Tag.Get("json")]; !exists {
				return errors.New("metadata required JSON field absent")
			}
		}
	}
	decoder = json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(output)
}

func nativeRuntimePrivate(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("metadata private path refused")
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return nil, errors.New("metadata private link refused")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, errors.New("metadata private open refused")
	}
	file := os.NewFile(uintptr(fd), path)
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, errors.New("metadata private stat refused")
	}
	unix, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || unix.Nlink != 1 || unix.Uid != uint32(os.Getuid()) || info.Size() > nativeRuntimeFileLimit {
		return nil, errors.New("metadata private identity refused")
	}
	raw, err := io.ReadAll(io.LimitReader(file, nativeRuntimeFileLimit+1))
	after, afterErr := file.Stat()
	live, liveErr := os.Lstat(path)
	if err != nil || afterErr != nil || liveErr != nil || len(raw) > nativeRuntimeFileLimit || !os.SameFile(info, live) || !os.SameFile(info, after) || info.Size() != after.Size() || !info.ModTime().Equal(after.ModTime()) {
		return nil, errors.New("metadata private file changed")
	}
	return raw, nil
}

func nativeRuntimeValidateInput(input nativeRuntimeInput, path string, now time.Time) error {
	if input.SchemaVersion != 1 || input.Phase != nativeRuntimePhase || input.Pair != nil || input.NamespaceUID != nil ||
		!regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(input.QualificationID) ||
		!regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(input.RunID) ||
		input.Namespace != "native-metadata-"+input.QualificationID[:16] ||
		input.ReceiptPath != filepath.Join(filepath.Dir(path), "metadata-receipt.json") ||
		input.Kubeconfig != filepath.Join(input.RunDir, "kubeconfig") ||
		input.Context != "kind-rpgmigrate-m7-"+input.RunID || !slices.Equal(input.RequiredCases, nativeRuntimeCases()) || len(input.Nodes) != 3 {
		return errors.New("metadata closed selection refused")
	}
	for _, digest := range []string{input.LegacySourceSHA, input.LegacyGuardSHA, input.LegacyOwnershipSHA, input.QualificationSourceSHA, input.EvidenceProgramSHA, input.AssetsSHA, input.MetadataProgramSHA} {
		if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(digest) {
			return errors.New("metadata digest binding refused")
		}
	}
	evidenceSHA := native.EvidenceProgramSHA256()
	if input.MetadataProgramSHA != nativeRuntimeSHA([]byte(nativeRuntimeMetadataProgram+"\x00"+nativeRuntimeNodeProgram)) ||
		input.EvidenceProgramSHA != hex.EncodeToString(evidenceSHA[:]) {
		return errors.New("metadata fixed program changed")
	}
	current := float64(now.UnixNano()) / 1e9
	if !(current < input.StageDeadline && input.StageDeadline <= current+600 && input.StageDeadline+600 <= input.LeaseExpiry) {
		return errors.New("metadata original finite lease or cleanup reserve refused")
	}
	seen := map[string]bool{}
	previous := ""
	for _, node := range input.Nodes {
		if node.Name <= previous || node.UID == "" || seen[string(node.UID)] || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(node.ContainerID) {
			return errors.New("metadata exact node binding refused")
		}
		previous = node.Name
		seen[string(node.UID)] = true
	}
	return nil
}

func nativeRuntimeWrite(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > nativeRuntimeFileLimit {
		return errors.New("metadata artifact cap")
	}
	fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return errors.New("metadata artifact already exists or is unavailable")
	}
	file := os.NewFile(uintptr(fd), path)
	defer func() { _ = file.Close() }()
	if _, err = file.Write(append(raw, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func nativeRuntimeCertificateOnly(cluster *clientcmdapi.Cluster, auth *clientcmdapi.AuthInfo) bool {
	if cluster == nil || auth == nil {
		return false
	}
	return !cluster.InsecureSkipTLSVerify && cluster.CertificateAuthority == "" && len(cluster.CertificateAuthorityData) > 0 && cluster.TLSServerName == "" && cluster.ProxyURL == "" &&
		auth.Exec == nil && auth.AuthProvider == nil && auth.Token == "" && auth.TokenFile == "" && auth.Username == "" && auth.Password == "" && auth.ClientCertificate == "" && auth.ClientKey == "" && len(auth.ClientCertificateData) > 0 && len(auth.ClientKeyData) > 0 && auth.Impersonate == "" && auth.ImpersonateUID == "" && len(auth.ImpersonateGroups) == 0 && len(auth.ImpersonateUserExtra) == 0
}

func nativeRuntimeClient(input nativeRuntimeInput) (*kubernetes.Clientset, error) {
	config, err := nativeRuntimeConfig(input)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

func nativeRuntimeConfig(input nativeRuntimeInput) (*rest.Config, error) {
	raw, err := nativeRuntimePrivate(input.Kubeconfig)
	if err != nil {
		return nil, err
	}
	config, err := clientcmd.Load(raw)
	if err != nil || len(config.Contexts) != 1 || len(config.Clusters) != 1 || len(config.AuthInfos) != 1 || config.CurrentContext != input.Context {
		return nil, errors.New("metadata owned kubeconfig refused")
	}
	selected := config.Contexts[input.Context]
	if selected == nil || selected.Cluster != input.Context || selected.AuthInfo != input.Context {
		return nil, errors.New("metadata owned context refused")
	}
	cluster, auth := config.Clusters[selected.Cluster], config.AuthInfos[selected.AuthInfo]
	if !nativeRuntimeCertificateOnly(cluster, auth) {
		return nil, errors.New("metadata kubeconfig TLS credential route refused")
	}
	server, err := url.Parse(cluster.Server)
	if err != nil || server.Scheme != nativeRuntimeHTTPS || server.User != nil || !net.ParseIP(server.Hostname()).IsLoopback() || server.Port() == "" || server.RawQuery != "" || server.Fragment != "" || server.Path != "" && server.Path != "/" {
		return nil, errors.New("metadata kubeconfig endpoint refused")
	}
	clientConfig, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{CurrentContext: input.Context}).ClientConfig()
	if err != nil {
		return nil, errors.New("metadata client configuration refused")
	}
	clientConfig.Timeout = 30 * time.Second
	return clientConfig, nil
}

func nativeRuntimeFields(raw []byte, names string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := nativeRuntimeDecode(raw, &fields); err != nil {
		return nil, err
	}
	keys := strings.Fields(names)
	if len(fields) != len(keys) {
		return nil, errors.New("registry closed fields differ")
	}
	for _, key := range keys {
		if _, exists := fields[key]; !exists {
			return nil, errors.New("registry required field absent")
		}
	}
	return fields, nil
}

type nativeRuntimeRegistrySource struct {
	Repository string `json:"repository"`
	Commit     string `json:"commit"`
	GitTree    string `json:"git_tree"`
	PayloadSHA string `json:"payload_inventory_sha256"`
}

func nativeRuntimeRegistryImage(raw []byte, role string) (nativeRuntimeImage, *string, error) {
	image := nativeRuntimeImage{}
	keys := "role ref os architecture variant digest index_digest config_digest provenance"
	if role == nativeRuntimeNativeRole {
		keys += " source"
	}
	fields, err := nativeRuntimeFields(raw, keys)
	if err != nil {
		return image, nil, err
	}
	var selectedRole, osName, architecture string
	var variant, index *string
	for key, target := range map[string]any{nativeRuntimeRoleKey: &selectedRole, "ref": &image.Ref, "os": &osName, "architecture": &architecture, nativeRuntimeVariantKey: &variant, "digest": &image.Digest, "index_digest": &index, "config_digest": &image.ConfigDigest} {
		if err := json.Unmarshal(fields[key], target); err != nil {
			return image, nil, errors.New("registry image field absent")
		}
	}
	repository := "ghcr.io/ydixken/rpgmigrate"
	if role != nativeRuntimeNativeRole {
		repository = "ghcr.io/cloudnative-pg/postgresql:" + strings.TrimPrefix(role, "postgresql-")
	}
	digest := regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	if selectedRole != role || osName != nativeRuntimeLinux || architecture != nativeRuntimeARM64 || variant != nil && *variant != "v8" || !digest.MatchString(image.Digest) || !digest.MatchString(image.ConfigDigest) || image.Digest == image.ConfigDigest || image.Ref != repository+"@"+image.Digest || index != nil && (!digest.MatchString(*index) || *index == image.Digest || *index == image.ConfigDigest) || role == nativeRuntimeNativeRole && index != nil {
		return image, nil, errors.New("registry selected image or platform differs")
	}
	provenance, err := nativeRuntimeFields(fields["provenance"], "origin_ref registry_receipt_sha256 public_receipt_sha256")
	if err != nil {
		return image, nil, err
	}
	for key, raw := range provenance {
		var text string
		if json.Unmarshal(raw, &text) != nil || text == "" || key != "origin_ref" && !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(text) {
			return image, nil, errors.New("public registry receipt absent")
		}
	}
	return image, index, nil
}

type nativeRuntimePreparation struct {
	SchemaVersion          int                        `json:"schema_version"`
	QualificationID        string                     `json:"qualification_id"`
	RunID                  string                     `json:"run_id"`
	LegacySourceRoot       string                     `json:"legacy_source_root"`
	LegacySourceSHA        string                     `json:"legacy_source_tree_sha256"`
	LegacyGuardSHA         string                     `json:"legacy_guard_sha256"`
	LegacyOwnershipSHA     string                     `json:"legacy_ownership_sha256"`
	QualificationSourceSHA string                     `json:"qualification_source_sha256"`
	EvidenceProgramSHA     string                     `json:"evidence_program_sha256"`
	ImageLockSHA           string                     `json:"image_lock_sha256"`
	Images                 map[string]json.RawMessage `json:"images"`
}

type nativeRuntimeRegistryLock struct {
	SchemaVersion  int                         `json:"schema_version"`
	Transport      string                      `json:"transport"`
	OperatorSource nativeRuntimeRegistrySource `json:"operator_source"`
	EngineSource   nativeRuntimeRegistrySource `json:"engine_source"`
	Images         map[string]json.RawMessage  `json:"images"`
	Publication    map[string]string           `json:"publication_receipts_sha256"`
}

type nativeRuntimeComponentReceipt struct {
	SchemaVersion          int     `json:"schema_version"`
	Transport              string  `json:"transport"`
	QualificationID        string  `json:"qualification_id"`
	RunID                  string  `json:"run_id"`
	AssetsSHA              string  `json:"native_assets_sha256"`
	ImageLockSHA           string  `json:"image_lock_sha256"`
	LegacySourceSHA        string  `json:"legacy_source_tree_sha256"`
	LegacyGuardSHA         string  `json:"legacy_guard_sha256"`
	LegacyOwnershipSHA     string  `json:"legacy_ownership_sha256"`
	QualificationSourceSHA string  `json:"qualification_source_sha256"`
	Deadline               float64 `json:"deadline"`
	MonotonicDeadline      float64 `json:"monotonic_deadline"`
	Complete               bool    `json:"complete"`
	Pulls                  []struct {
		NodeID       string `json:"node_id"`
		RequestedRef string `json:"requested_ref"`
		Status       string `json:"status"`
	} `json:"pulls"`
	Bindings map[string]map[string]json.RawMessage `json:"node_bindings"`
}

func nativeRuntimeReadPreparation(path string, input nativeRuntimeInput) (nativeRuntimePreparation, error) {
	raw, err := nativeRuntimePrivate(filepath.Join(filepath.Dir(path), "native-assets.json"))
	if err != nil || nativeRuntimeSHA(raw) != input.AssetsSHA {
		return nativeRuntimePreparation{}, errors.New("metadata native asset binding refused")
	}
	var packet nativeRuntimePreparation
	if nativeRuntimeDecode(raw, &packet) != nil || packet.SchemaVersion != 2 || packet.QualificationID != input.QualificationID || packet.RunID != input.RunID || packet.LegacySourceRoot != input.LegacySourceRoot || packet.LegacySourceSHA != input.LegacySourceSHA || packet.LegacyGuardSHA != input.LegacyGuardSHA || packet.LegacyOwnershipSHA != input.LegacyOwnershipSHA || packet.QualificationSourceSHA != input.QualificationSourceSHA || packet.EvidenceProgramSHA != input.EvidenceProgramSHA || len(packet.Images) != 3 {
		return nativeRuntimePreparation{}, errors.New("metadata registry preparation differs")
	}
	return packet, nil
}

func nativeRuntimeReadImageLock(run, lockSHA, sourceSHA string) (nativeRuntimeRegistryLock, error) {
	lockRaw, err := nativeRuntimePrivate(filepath.Join(run, "image-lock.json"))
	if err != nil || nativeRuntimeSHA(lockRaw) != lockSHA {
		return nativeRuntimeRegistryLock{}, errors.New("metadata retained image lock changed")
	}
	var lock nativeRuntimeRegistryLock
	if nativeRuntimeDecode(lockRaw, &lock) != nil || lock.SchemaVersion != 2 || lock.Transport != nativeRuntimeTransport || lock.OperatorSource.PayloadSHA != sourceSHA || len(lock.Images) != 12 || len(lock.Publication) != 2 {
		return nativeRuntimeRegistryLock{}, errors.New("metadata public source lock refused")
	}
	for _, role := range []string{nativeRuntimeNodeMode, "cnpg-controller", nativeRuntimePG16Role, "postgresql-17", nativeRuntimePG18Role, nativeRuntimeManagerRole, "legacy-runner", nativeRuntimeNativeRole, "seed-tool-runner", "otel-collector", "storage-helper", "storage-provisioner"} {
		if _, exists := lock.Images[role]; !exists {
			return nativeRuntimeRegistryLock{}, errors.New("metadata lock role missing")
		}
	}
	for _, role := range []string{nativeRuntimeManagerRole, nativeRuntimeNativeRole} {
		if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(lock.Publication[role]) {
			return nativeRuntimeRegistryLock{}, errors.New("public publication proof absent")
		}
	}
	for _, source := range []nativeRuntimeRegistrySource{lock.OperatorSource, lock.EngineSource} {
		if source.Repository == "" || !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(source.Commit) || !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(source.GitTree) || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(source.PayloadSHA) {
			return nativeRuntimeRegistryLock{}, errors.New("registry clean source identity missing")
		}
	}
	return lock, nil
}

func nativeRuntimeReadComponent(path, lockSHA string, input nativeRuntimeInput) (nativeRuntimeComponentReceipt, error) {
	ownership, err := nativeRuntimePrivate(filepath.Join(input.RunDir, "ownership.json"))
	if err != nil || nativeRuntimeSHA(ownership) != input.LegacyOwnershipSHA {
		return nativeRuntimeComponentReceipt{}, errors.New("registry ownership changed")
	}
	var ledger map[string]json.RawMessage
	var lease map[string]json.RawMessage
	var wall, monotonic float64
	if nativeRuntimeDecode(ownership, &ledger) != nil || nativeRuntimeDecode(ledger["resource_lease"], &lease) != nil || json.Unmarshal(lease["expires_at"], &wall) != nil || json.Unmarshal(lease["monotonic_expires_at"], &monotonic) != nil || wall != input.LeaseExpiry || !(monotonic > 0) {
		return nativeRuntimeComponentReceipt{}, errors.New("registry original clock binding unavailable")
	}
	receiptRaw, err := nativeRuntimePrivate(filepath.Join(filepath.Dir(path), "component-image-resolution-evidence.json"))
	if err != nil {
		return nativeRuntimeComponentReceipt{}, errors.New("fresh direct pull evidence unavailable")
	}
	var receipt nativeRuntimeComponentReceipt
	if nativeRuntimeDecode(receiptRaw, &receipt) != nil || receipt.SchemaVersion != 2 || receipt.Transport != nativeRuntimeTransport || !receipt.Complete || receipt.QualificationID != input.QualificationID || receipt.RunID != input.RunID || receipt.AssetsSHA != input.AssetsSHA || receipt.ImageLockSHA != lockSHA || receipt.LegacySourceSHA != input.LegacySourceSHA || receipt.LegacyGuardSHA != input.LegacyGuardSHA || receipt.LegacyOwnershipSHA != input.LegacyOwnershipSHA || receipt.QualificationSourceSHA != input.QualificationSourceSHA || !(receipt.Deadline > 0 && receipt.Deadline <= input.LeaseExpiry) || receipt.MonotonicDeadline != monotonic || len(receipt.Bindings) != 3 || len(receipt.Pulls) != 9 {
		return nativeRuntimeComponentReceipt{}, errors.New("complete original-source direct pull proof required")
	}
	return receipt, nil
}

func nativeRuntimePublishedSource(raw []byte, expectedSource nativeRuntimeRegistrySource) error {
	fields, _ := nativeRuntimeFields(raw, "role ref os architecture variant digest index_digest config_digest provenance source")
	source, err := nativeRuntimeFields(fields["source"], "repository commit git_tree payload_inventory_sha256 build_input_sha256 executable_sha256 smoke_receipt_sha256")
	if err != nil {
		return err
	}
	for key, expected := range map[string]string{"repository": expectedSource.Repository, nativeRuntimeCommitKey: expectedSource.Commit, "git_tree": expectedSource.GitTree, "payload_inventory_sha256": expectedSource.PayloadSHA} {
		var observed string
		if json.Unmarshal(source[key], &observed) != nil || observed != expected {
			return errors.New("native publication source differs")
		}
	}
	for _, key := range []string{"build_input_sha256", "executable_sha256", "smoke_receipt_sha256"} {
		var digest string
		if json.Unmarshal(source[key], &digest) != nil || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(digest) {
			return errors.New("native publication build or smoke proof absent")
		}
	}

	return nil
}

func nativeRuntimeBindImage(raw []byte, node nativeRuntimeNode, image nativeRuntimeImage, role string, index, variant *string) (nativeRuntimeImageBinding, error) {
	var binding nativeRuntimeImageBinding
	if nativeRuntimeDecode(raw, &binding) != nil || binding.NodeID != node.ContainerID || binding.RequestedRef != image.Ref || binding.ManifestDigest != image.Digest || binding.ConfigDigest != image.ConfigDigest || binding.OS != nativeRuntimeLinux || binding.Architecture != nativeRuntimeARM64 || !reflect.DeepEqual(binding.Variant, variant) || len(binding.AcceptedImageIDs) < 2 || !slices.IsSorted(binding.AcceptedImageIDs) || !slices.Contains(binding.AcceptedImageIDs, image.Digest) || !slices.Contains(binding.AcceptedImageIDs, image.ConfigDigest) {
		return nativeRuntimeImageBinding{}, errors.New("registry exact node image binding refused")
	}
	ref, _, _ := strings.Cut(image.Ref, "@")
	if role != nativeRuntimeNativeRole {
		ref = strings.TrimSuffix(ref, ":"+strings.TrimPrefix(role, "postgresql-"))
	}
	for number, id := range binding.AcceptedImageIDs {
		if number > 0 && id == binding.AcceptedImageIDs[number-1] || id != image.ConfigDigest && id != image.Digest && id != image.Ref && id != ref+"@"+image.Digest && (index == nil || id != ref+"@"+*index) {
			return nativeRuntimeImageBinding{}, errors.New("unproven raw registry image identity refused")
		}
	}
	return binding, nil
}

func nativeRuntimeAssetsByRole(path string, input nativeRuntimeInput) (map[string]nativeRuntimeImage, error) {
	packet, err := nativeRuntimeReadPreparation(path, input)
	if err != nil {
		return nil, err
	}
	lock, err := nativeRuntimeReadImageLock(input.RunDir, packet.ImageLockSHA, input.LegacySourceSHA)
	if err != nil {
		return nil, err
	}
	receipt, err := nativeRuntimeReadComponent(path, packet.ImageLockSHA, input)
	if err != nil {
		return nil, err
	}
	images := map[string]nativeRuntimeImage{}
	remaining := map[string]bool{}
	for _, role := range []string{nativeRuntimeNativeRole, nativeRuntimePG16Role, nativeRuntimePG18Role} {
		entry, err := nativeRuntimeFields(packet.Images[role], "image")
		if err != nil {
			return nil, err
		}
		image, index, err := nativeRuntimeRegistryImage(entry["image"], role)
		if err != nil {
			return nil, err
		}
		var prepared, locked any
		if nativeRuntimeDecode(entry["image"], &prepared) != nil || nativeRuntimeDecode(lock.Images[role], &locked) != nil || !reflect.DeepEqual(prepared, locked) {
			return nil, errors.New("registry image differs from exact retained lock")
		}
		if role == nativeRuntimeNativeRole {
			if err := nativeRuntimePublishedSource(entry["image"], lock.EngineSource); err != nil {
				return nil, err
			}
		}
		var fields map[string]json.RawMessage
		var variant *string
		if nativeRuntimeDecode(entry["image"], &fields) != nil || json.Unmarshal(fields[nativeRuntimeVariantKey], &variant) != nil {
			return nil, errors.New("registry selected variant unavailable")
		}
		bindings := receipt.Bindings[role]
		if len(bindings) != len(input.Nodes) {
			return nil, errors.New("registry exact node matrix missing")
		}
		image.NodeBindings = map[string]nativeRuntimeImageBinding{}
		for _, node := range input.Nodes {
			binding, err := nativeRuntimeBindImage(bindings[node.Name], node, image, role, index, variant)
			if err != nil {
				return nil, err
			}
			image.NodeBindings[node.Name] = binding
			remaining[node.ContainerID+"\x00"+image.Ref] = true
		}
		images[role] = image
	}
	for _, pull := range receipt.Pulls {
		key := pull.NodeID + "\x00" + pull.RequestedRef
		if pull.Status != "verified" || !remaining[key] {
			return nil, errors.New("direct pull matrix incomplete or duplicated")
		}
		delete(remaining, key)
	}
	if len(remaining) != 0 {
		return nil, errors.New("direct pull matrix missing")
	}
	return images, nil
}

func nativeRuntimeAssets(path string, input nativeRuntimeInput) (nativeRuntimeImage, error) {
	images, err := nativeRuntimeAssetsByRole(path, input)
	return images[nativeRuntimeNativeRole], err
}

func nativeRuntimeJob(input nativeRuntimeInput, node nativeRuntimeNode, image nativeRuntimeImage, index int) (*batchv1.Job, error) {
	operation := input.QualificationID
	migration := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Name: "native-metadata", Namespace: input.Namespace, UID: types.UID(operation)}, Spec: v1beta1.MigrationSpec{Engine: v1beta1.MigrationEngineRPGMigrate}}
	migration.Spec.Source = v1beta1.PostgresConnection{Host: "metadata.invalid", Username: nativeRuntimePhase, Database: nativeRuntimePhase}
	migration.Spec.Target = migration.Spec.Source
	job, err := buildNativeVersionJob(migration, image.Ref, "rpgmigrate-version-"+operation)
	if err != nil {
		return nil, err
	}
	// Derive tmpfs from the canonical materialization without delivering its connection environment.
	_, _, _, volumes, mounts, err := native.PreflightMaterialization(migration, operation)
	if err != nil || len(volumes) != 1 || len(mounts) != 1 {
		return nil, errors.New("metadata canonical tmpfs unavailable")
	}
	job.Name = fmt.Sprintf("native-metadata-%d", index)
	job.Labels = map[string]string{nativeRuntimeRunLabel: input.RunID, "rpgmigrate.io/qualification": input.QualificationID}
	job.Spec.Template.Labels = job.Labels
	pod := &job.Spec.Template.Spec
	pod.NodeName = node.Name
	pod.Volumes = volumes
	container := &pod.Containers[0]
	container.Command = []string{nativeRuntimeEnvBinary, "-u", nativeRuntimePreload, "-u", nativeRuntimeLibraryPath, nativeRuntimePython, "-I", "-B", "-c", nativeRuntimeMetadataProgram}
	container.Args = nil
	container.Env = nil
	container.VolumeMounts = mounts
	container.Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("256Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("512Mi")}}
	raw, err := json.Marshal(job)
	if err != nil || len(raw) > nativeRuntimeFileLimit || len(nativeRuntimeMetadataProgram)+1 > 126976 {
		return nil, errors.New("metadata job or argument cap")
	}
	return job, nil
}

type nativeRuntimeProfileMount struct {
	Destination string `json:"destination"`
	Source      string `json:"source"`
	Readonly    bool   `json:"readonly"`
}

type nativeRuntimeProfileResult struct {
	ContainerID         string  `json:"container_id"`
	PodUID              string  `json:"pod_uid"`
	SandboxID           string  `json:"sandbox_id"`
	PID                 int     `json:"pid"`
	StartTicks          int64   `json:"start_ticks"`
	BootID              string  `json:"boot_id"`
	MountNamespace      string  `json:"mount_namespace"`
	MountCount          int     `json:"admitted_mount_count"`
	TmpDevice           string  `json:"tmp_device"`
	TmpInode            string  `json:"tmp_inode"`
	KeyStatVerified     bool    `json:"key_stat_verified"`
	TerminationLogID    string  `json:"termination_log_id"`
	ImageVolumeID       *string `json:"image_volume_id"`
	SandboxPID          int     `json:"sandbox_pid"`
	State               string  `json:"state"`
	LiveProcess         bool    `json:"live_process_verified"`
	TerminalPIDAbsent   bool    `json:"terminal_pid_absent"`
	FinalSpec           bool    `json:"final_spec_verified"`
	ActualMounts        bool    `json:"actual_mounts_verified"`
	ActualEnvironment   bool    `json:"actual_environment_verified"`
	ResourceBounds      bool    `json:"resource_bounds_verified"`
	StockHook           bool    `json:"stock_hook_verified"`
	ReleaseCreated      bool    `json:"release_created"`
	LastAdmittedProcess bool    `json:"last_admitted_process_verified"`
	Endpoint            bool    `json:"endpoint_verified"`
	ImmutableNodeFiles  bool    `json:"immutable_node_files_verified"`
}

type nativeRuntimeProfileRequest struct {
	Mode               string                      `json:"mode"`
	Profile            string                      `json:"profile"`
	ContainerID        string                      `json:"container_id"`
	PodUID             string                      `json:"pod_uid"`
	PodName            string                      `json:"pod_name"`
	Namespace          string                      `json:"namespace"`
	ContainerName      string                      `json:"container_name"`
	ImageRef           string                      `json:"image_ref"`
	Command            []string                    `json:"command"`
	RunningArgs        []string                    `json:"running_args"`
	Environment        map[string]string           `json:"environment"`
	ProcessEnvironment map[string]string           `json:"process_environment"`
	OOMScoreAdj        int                         `json:"oom_score_adj"`
	Mounts             []nativeRuntimeProfileMount `json:"mounts"`
	Previous           map[string]any              `json:"previous"`
	Self               map[string]any              `json:"self"`
	services           []*corev1.Service
}

func nativeRuntimeServiceEnvironment(service *corev1.Service, values map[string]string) error {
	if service.UID == "" || service.DeletionTimestamp != nil || net.ParseIP(service.Spec.ClusterIP) == nil || len(service.Spec.Ports) != 1 || service.Spec.Type != corev1.ServiceTypeClusterIP || service.Spec.ExternalName != "" || len(service.Spec.ExternalIPs) != 0 {
		return errors.New("active profile Service input refused")
	}
	port := service.Spec.Ports[0]
	if port.Protocol != corev1.ProtocolTCP || service.Namespace == nativePreflightDefaultSA && (service.Name != "kubernetes" || port.Name != nativeRuntimeHTTPS || port.Port != 443) || service.Namespace != nativePreflightDefaultSA && (service.Name != nativeRuntimePG16 && service.Name != nativeRuntimePG18 || port.Name != "" || port.Port != 5432 || port.TargetPort.IntVal != 5432) {
		return errors.New("active profile Service port refused")
	}
	prefix := strings.ToUpper(strings.ReplaceAll(service.Name, "-", "_"))
	number := fmt.Sprint(port.Port)
	address := "tcp://" + net.JoinHostPort(service.Spec.ClusterIP, number)
	values[prefix+"_SERVICE_HOST"], values[prefix+"_SERVICE_PORT"] = service.Spec.ClusterIP, number
	if port.Name != "" {
		values[prefix+"_SERVICE_PORT_"+strings.ToUpper(port.Name)] = number
	}
	values[prefix+"_PORT"] = address
	link := prefix + "_PORT_" + number + "_TCP"
	values[link], values[link+"_PROTO"], values[link+"_PORT"], values[link+"_ADDR"] = address, "tcp", number, service.Spec.ClusterIP
	return nil
}

func nativeRuntimeOwnedServices(owned []nativeRuntimeOwned) []*corev1.Service {
	result := []*corev1.Service{}
	for _, item := range owned {
		if service, ok := item.object.(*corev1.Service); ok && item.kind == nativePasswordServiceKind {
			result = append(result, service.DeepCopy())
		}
	}
	return result
}

func nativeRuntimeExpectedCommand(pod *corev1.Pod, profile string, value *nativeRuntimeProfileRequest) error {
	container := pod.Spec.Containers[0]
	if strings.HasPrefix(profile, "pg") {
		major := 16
		if profile == nativeRuntimePG18 {
			major = 18
		}
		if !slices.Equal(container.Command, nativePasswordServerCommand(major)) || len(container.Env) != 0 {
			return errors.New("active profile server command refused")
		}
		binary := fmt.Sprintf("/usr/lib/postgresql/%d/bin/postgres", major)
		value.RunningArgs = []string{binary, "-D", "/var/lib/postgresql/probe-data", "-c", "listen_addresses=*", "-c", "unix_socket_directories=/tmp", "-c", "ssl=on", "-c", "ssl_cert_file=/run/server-input/server.crt", "-c", "ssl_key_file=/run/server-input/server.key", "-c", "password_encryption=scram-sha-256", "-c", "log_min_error_statement=panic"}
		// The CNPG image config carries only these two variables.
		value.Environment = map[string]string{"PATH": fmt.Sprintf("/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/postgresql/%d/bin", major), "PIP_BREAK_SYSTEM_PACKAGES": "1"}
	} else {
		program := nativeRuntimeMetadataProgram
		if profile != nativeRuntimePhase {
			var err error
			program, err = nativePasswordProgram()
			if err != nil {
				return err
			}
		}
		command := []string{nativeRuntimeEnvBinary, "-u", nativeRuntimePreload, "-u", nativeRuntimeLibraryPath, nativeRuntimePython, "-I", "-B", "-c", program}
		if profile != nativeRuntimePhase {
			command = nativePasswordCommand(program)
		}
		if !slices.Equal(container.Command, command) {
			return errors.New("active profile helper command refused")
		}
		value.RunningArgs = append([]string{}, command[5:]...)
		value.Environment["PATH"] = "/usr/local/bin:/usr/lib/postgresql/18/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"
		seen := map[string]bool{}
		for _, variable := range container.Env {
			if profile == nativeRuntimePhase || seen[variable.Name] || !slices.Contains([]string{nativeLCAllEnv, nativeLangEnv, "NATIVE_PROBE_ID", "NATIVE_PROBE_RUN", "NATIVE_PROBE_IMAGE", "NATIVE_PROBE_PROGRAM", nativePasswordUIDEnvironment}, variable.Name) {
				return errors.New("active profile helper environment refused")
			}
			seen[variable.Name] = true
			if variable.Name == nativePasswordUIDEnvironment {
				if variable.Value != "" || variable.ValueFrom == nil || variable.ValueFrom.FieldRef == nil || variable.ValueFrom.FieldRef.FieldPath != "metadata.uid" || variable.ValueFrom.FieldRef.APIVersion != "v1" {
					return errors.New("active profile UID environment refused")
				}
				value.Environment[variable.Name] = string(pod.UID)
			} else {
				if variable.ValueFrom != nil {
					return errors.New("active profile environment source refused")
				}
				value.Environment[variable.Name] = variable.Value
			}
		}
		if profile != nativeRuntimePhase && len(seen) != 7 {
			return errors.New("active profile environment inventory refused")
		}
	}
	return nil
}

func nativeRuntimeExpectedMounts(pod *corev1.Pod) ([]nativeRuntimeProfileMount, error) {
	mounts := []nativeRuntimeProfileMount{}
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		var volume *corev1.Volume
		for index := range pod.Spec.Volumes {
			if pod.Spec.Volumes[index].Name == mount.Name {
				volume = &pod.Spec.Volumes[index]
			}
		}
		if volume == nil || mount.SubPath != "" || mount.SubPathExpr != "" || mount.MountPropagation != nil || mount.RecursiveReadOnly != nil {
			return nil, errors.New("active profile volume input refused")
		}
		kind := ""
		if volume.EmptyDir != nil {
			kind = "kubernetes.io~empty-dir"
		} else if volume.Secret != nil {
			kind = "kubernetes.io~secret"
		} else {
			return nil, errors.New("active profile volume type refused")
		}
		mounts = append(mounts, nativeRuntimeProfileMount{Destination: mount.MountPath, Source: "/var/lib/kubelet/pods/" + string(pod.UID) + "/volumes/" + kind + "/" + volume.Name, Readonly: mount.ReadOnly})
	}
	return mounts, nil
}

func nativeRuntimeExpected(ctx context.Context, clientset kubernetes.Interface, pod *corev1.Pod, profile string, services []*corev1.Service) (nativeRuntimeProfileRequest, error) {
	value := nativeRuntimeProfileRequest{Mode: lifecycleWaitingMain, Profile: profile, Environment: map[string]string{}, ProcessEnvironment: map[string]string{}, Mounts: []nativeRuntimeProfileMount{}}
	if pod == nil || len(pod.Spec.Containers) != 1 || pod.UID == "" || pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC || pod.Spec.RuntimeClassName != nil || pod.Spec.HostUsers != nil && !*pod.Spec.HostUsers || pod.Spec.DNSPolicy != corev1.DNSClusterFirst || pod.Spec.DNSConfig != nil || pod.Spec.Hostname != "" {
		return value, errors.New("active profile Pod input refused")
	}
	container := pod.Spec.Containers[0]
	if !slices.Contains([]string{nativeRuntimePhase, "client16", "client18", nativeRuntimePG16, nativeRuntimePG18}, profile) || len(container.Args) != 0 {
		return value, errors.New("active profile fixture selection refused")
	}
	value.PodUID, value.PodName, value.Namespace, value.ContainerName, value.ImageRef = string(pod.UID), pod.Name, pod.Namespace, container.Name, container.Image
	value.Command = append([]string{}, container.Command...)
	if err := nativeRuntimeExpectedCommand(pod, profile, &value); err != nil {
		return value, err
	}
	value.Environment["HOSTNAME"] = pod.Name
	master, err := clientset.CoreV1().Services(nativePreflightDefaultSA).Get(ctx, "kubernetes", metav1.GetOptions{})
	if err != nil {
		return value, errors.New("independent Kubernetes Service unavailable")
	}
	value.services = append([]*corev1.Service{master.DeepCopy()}, services...)
	for _, service := range value.services {
		if service.Namespace != nativePreflightDefaultSA && service.Namespace != pod.Namespace {
			return value, errors.New("foreign Service environment refused")
		}
		if err := nativeRuntimeServiceEnvironment(service, value.Environment); err != nil {
			return value, err
		}
	}
	maps.Copy(value.ProcessEnvironment, value.Environment)
	// runc adds HOME from the image passwd at init, or "/" when the UID has no entry (65532 in the runner).
	value.ProcessEnvironment["HOME"] = "/"
	if strings.HasPrefix(profile, "pg") {
		value.ProcessEnvironment["PWD"] = "/"
		value.ProcessEnvironment["HOME"] = "/var/lib/postgresql"
	}
	value.Mounts, err = nativeRuntimeExpectedMounts(pod)
	if err != nil {
		return value, err
	}
	node, err := clientset.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil || node.Status.Capacity.Memory().Value() <= 0 {
		return value, errors.New("active profile node memory unavailable")
	}
	capacity := node.Status.Capacity.Memory().Value()
	value.OOMScoreAdj = max(3, min(999, int(1000-1000*container.Resources.Requests.Memory().Value()/capacity)))
	return value, nil
}

func nativeRuntimeProfileServices(ctx context.Context, clientset kubernetes.Interface, expected nativeRuntimeProfileRequest) error {
	for _, original := range expected.services {
		actual, err := clientset.CoreV1().Services(original.Namespace).Get(ctx, original.Name, metav1.GetOptions{})
		if err != nil || actual.UID != original.UID || actual.DeletionTimestamp != nil || !reflect.DeepEqual(actual.Spec, original.Spec) {
			return errors.New("active profile Service continuity refused")
		}
	}
	return nil
}

func nativeRuntimeProfileCommand(ctx context.Context, node nativeRuntimeNode, input any) ([]byte, error) {
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(node.ContainerID) || len(nativeRuntimeNodeProgram) > 122880 || len(nativeRuntimeNodeProgram)+1 > 126976 {
		return nil, errors.New("active profile fixed source or node refused")
	}
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, ok := call.Deadline()
	if !ok {
		return nil, errors.New("active profile call deadline absent")
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, errors.New("active profile expected input refused")
	}
	var request map[string]json.RawMessage
	if json.Unmarshal(raw, &request) != nil {
		return nil, errors.New("active profile expected object refused")
	}
	request["__host_call_deadline"], err = json.Marshal(float64(deadline.UnixNano()) / 1e9)
	if err != nil {
		return nil, errors.New("active profile call deadline refused")
	}
	raw, err = json.Marshal(request)
	if err != nil || len(raw) > nativeRuntimeFileLimit {
		return nil, errors.New("active profile expected input cap")
	}
	command := exec.CommandContext(call, "python3", "-I", "-B", "-c", nativeRuntimeNodeProgram, node.ContainerID) // #nosec G204 -- fixed host adapter and admitted owned node only.
	stdout, stderr := &nativeRuntimeBuffer{}, &nativeRuntimeBuffer{}
	command.Stdin = bytes.NewReader(raw)
	command.Stdout, command.Stderr = stdout, stderr
	command.WaitDelay = time.Second
	if err := command.Run(); err != nil || stderr.Len() != 0 || stdout.Len() == 0 {
		return nil, errors.New("active profile fixed command refused")
	}
	return stdout.Bytes(), nil
}

func nativeRuntimeProfile(ctx context.Context, clientset kubernetes.Interface, node nativeRuntimeNode, expected nativeRuntimeProfileRequest, containerID, mode string, previous, self map[string]any) (map[string]any, error) {
	id, ok := strings.CutPrefix(containerID, "containerd://")
	if !ok || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(id) || !slices.Contains([]string{lifecycleWaitingMain, probeTerminalCase, "metadata_release"}, mode) {
		return nil, errors.New("active profile invocation refused")
	}
	if err := nativeRuntimeProfileServices(ctx, clientset, expected); err != nil {
		return nil, err
	}
	expected.Mode, expected.ContainerID, expected.Previous, expected.Self = mode, id, previous, self
	raw, err := nativeRuntimeProfileCommand(ctx, node, expected)
	if err != nil {
		return nil, err
	}
	result, err := nativeRuntimeProjection(raw, id, expected.PodUID, mode)
	if err != nil {
		return nil, err
	}
	if result["key_stat_verified"] != strings.HasPrefix(expected.Profile, "pg") {
		return nil, errors.New("active profile private key metadata refused")
	}
	if err := nativeRuntimeProfileServices(ctx, clientset, expected); err != nil {
		return nil, err
	}
	return result, nil
}

func nativeRuntimeProjectionIdentityInvalid(value nativeRuntimeProfileResult, id, podUID string) bool {
	return value.ContainerID != id || value.PodUID != podUID || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(value.SandboxID) || !regexp.MustCompile(`^[0-9]{1,20}$`).MatchString(value.TmpDevice) || !regexp.MustCompile(`^[0-9]{1,20}$`).MatchString(value.TmpInode) || value.PID <= 1 || value.PID >= 4194304 || value.StartTicks <= 0 || value.SandboxPID <= 1 || value.MountCount <= 0 || value.MountCount > 128 || !regexp.MustCompile(`^[0-9a-f-]{36}$`).MatchString(value.BootID) || !regexp.MustCompile(`^mnt:\[[0-9]+\]$`).MatchString(value.MountNamespace) || !regexp.MustCompile(`^[0-9a-f]{8}$`).MatchString(value.TerminationLogID) || value.ImageVolumeID != nil && !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(*value.ImageVolumeID)
}

func nativeRuntimeProjection(raw []byte, id, podUID, mode string) (map[string]any, error) {
	var value nativeRuntimeProfileResult
	if nativeRuntimeDecode(raw, &value) != nil || nativeRuntimeProjectionIdentityInvalid(value, id, podUID) || !value.FinalSpec || !value.StockHook || !value.LastAdmittedProcess || !value.Endpoint || !value.ImmutableNodeFiles {
		return nil, errors.New("active profile closed projection refused")
	}
	if value.State == probeTerminalCase {
		if mode == lifecycleWaitingMain || value.LiveProcess || !value.TerminalPIDAbsent || value.ActualMounts || value.ActualEnvironment || value.ResourceBounds {
			return nil, errors.New("active profile terminal projection refused")
		}
	} else if value.State != lifecycleWaitingMain || mode == probeTerminalCase || !value.LiveProcess || value.TerminalPIDAbsent || !value.ActualMounts || !value.ActualEnvironment || !value.ResourceBounds {
		return nil, errors.New("active profile running projection refused")
	}
	if value.ReleaseCreated != (mode == "metadata_release") {
		return nil, errors.New("active profile release projection refused")
	}
	var result map[string]any
	if json.Unmarshal(raw, &result) != nil {
		return nil, errors.New("active profile projection decode refused")
	}
	return result, nil
}

func nativeRuntimeNodeFacts(ctx context.Context, node nativeRuntimeNode) (map[string]any, error) {
	raw, err := nativeRuntimeProfileCommand(ctx, node, map[string]any{"mode": nativeRuntimeNodeMode})
	if err != nil {
		return nil, err
	}
	var facts map[string]any
	if nativeRuntimeDecode(raw, &facts) != nil || facts["active_runtime_verified"] != true || facts["endpoint_verified"] != true || facts["immutable_node_files_verified"] != true || facts["default_runtime"] != "runc" || facts["runtime_type"] != "io.containerd.runc.v2" || facts["sandboxer"] != "podsandbox" {
		return nil, errors.New("metadata default containerd profile refused")
	}
	return facts, nil
}

func nativeRuntimeCommand(ctx context.Context, args ...string) ([]byte, error) {
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(call, args[0], args[1:]...) // #nosec G204 -- fixed test programs and admitted node IDs only.
	stdout, stderr := &nativeRuntimeBuffer{}, &nativeRuntimeBuffer{}
	command.Stdout, command.Stderr = stdout, stderr
	command.WaitDelay = time.Second
	if err := command.Run(); err != nil || stderr.Len() != 0 || stdout.Len() == 0 {
		return nil, errors.New("metadata fixed command failed")
	}
	return stdout.Bytes(), nil
}

type nativeRuntimeBuffer struct{ bytes.Buffer }

func (buffer *nativeRuntimeBuffer) Write(raw []byte) (int, error) {
	if buffer.Len()+len(raw) > nativeRuntimeFileLimit {
		return 0, errors.New("metadata command output cap")
	}
	return buffer.Buffer.Write(raw)
}

func nativeRuntimeStatus(pod *corev1.Pod, image nativeRuntimeImage, known string) (string, bool, error) {
	if pod.Status.Phase == corev1.PodFailed {
		return known, false, errors.New("metadata Pod failed")
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		if known != "" {
			return known, false, errors.New("metadata container status disappeared")
		}
		return "", false, nil
	}
	if len(pod.Status.ContainerStatuses) != 1 {
		return known, false, errors.New("metadata container inventory refused")
	}
	status := pod.Status.ContainerStatuses[0]
	if status.ContainerID == "" {
		if known != "" || status.RestartCount != 0 || status.State.Terminated != nil {
			return known, false, errors.New("metadata container lost identity")
		}
		return "", false, nil
	}
	if status.Name != workerContainer || status.RestartCount != 0 || !nativeRuntimeImageID(status.ImageID, image, pod.Spec.NodeName) || known != "" && known != status.ContainerID {
		return known, false, errors.New("metadata actual image/container changed")
	}
	if status.State.Terminated == nil {
		return status.ContainerID, false, nil
	}
	if status.State.Terminated.ExitCode != 0 || status.State.Terminated.Signal != 0 {
		return status.ContainerID, false, errors.New("metadata terminal status refused")
	}
	return status.ContainerID, true, nil
}

func nativeRuntimePod(ctx context.Context, clientset kubernetes.Interface, input nativeRuntimeInput, job *batchv1.Job, image nativeRuntimeImage) (*corev1.Pod, []byte, error) {
	var original *corev1.Pod
	knownContainer := ""
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(call context.Context) (bool, error) {
		live, err := clientset.BatchV1().Jobs(job.Namespace).Get(call, job.Name, metav1.GetOptions{})
		if err != nil || live.UID != job.UID || !reflect.DeepEqual(live.Spec, job.Spec) {
			return false, errors.New("metadata Job replaced")
		}
		if nativePreflightJobFailed(live) {
			return false, errors.New("metadata Job failed")
		}
		pods, err := clientset.CoreV1().Pods(job.Namespace).List(call, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + job.Name})
		if err != nil {
			return false, errors.New("metadata Pod list failed")
		}
		if len(pods.Items) == 0 {
			return false, nil
		}
		if len(pods.Items) != 1 {
			return false, errors.New("metadata Pod replacement refused")
		}
		pod := pods.Items[0].DeepCopy()
		if original == nil {
			original = pod.DeepCopy()
		}
		if pod.UID != original.UID || pod.Spec.NodeName != job.Spec.Template.Spec.NodeName || !nativePreflightPodMatches(job, pod) || pod.Labels[nativeRuntimeRunLabel] != input.RunID {
			return false, errors.New("metadata actual Pod identity refused")
		}
		container, terminal, err := nativeRuntimeStatus(pod, image, knownContainer)
		if err != nil {
			return false, err
		}
		knownContainer = container
		if !terminal || !nativeEvidenceWorkerComplete(live) {
			return false, nil
		}
		original = pod
		return true, nil
	})
	if err != nil {
		return original, nil, err
	}
	stream, err := clientset.CoreV1().Pods(job.Namespace).GetLogs(original.Name, &corev1.PodLogOptions{Container: workerContainer}).Stream(ctx)
	if err != nil {
		return original, nil, errors.New("metadata log stream unavailable")
	}
	raw, readErr := io.ReadAll(io.LimitReader(stream, nativeRuntimeWireLimit+1))
	closeErr := stream.Close()
	if readErr != nil || closeErr != nil || len(raw) == 0 || len(raw) > nativeRuntimeWireLimit {
		return original, nil, errors.New("metadata terminal EOF/close or wire cap refused")
	}
	final, err := clientset.CoreV1().Pods(job.Namespace).Get(ctx, original.Name, metav1.GetOptions{})
	if err != nil || final.UID != original.UID || !reflect.DeepEqual(final.Spec, original.Spec) || !reflect.DeepEqual(final.Status, original.Status) {
		return original, nil, errors.New("metadata final Pod closure refused")
	}
	return original, raw, nil
}

func nativeRuntimeImageID(observed string, image nativeRuntimeImage, node string) bool {
	binding, exists := image.NodeBindings[node]
	return exists && binding.RequestedRef == image.Ref && binding.ManifestDigest == image.Digest &&
		binding.ConfigDigest == image.ConfigDigest && slices.Contains(binding.AcceptedImageIDs, observed)
}

func nativeRuntimeCRIFacts(ctx context.Context, node nativeRuntimeNode, pod *corev1.Pod, resolver map[string]any, profile map[string]any) (map[string]any, error) {
	containerID := strings.TrimPrefix(pod.Status.ContainerStatuses[0].ContainerID, "containerd://")
	sandboxID, ok := profile["sandbox_id"].(string)
	if !ok || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(sandboxID) || profile[nativePasswordContainerKey] != containerID || profile["pod_uid"] != string(pod.UID) || profile["final_spec_verified"] != true {
		return nil, errors.New("metadata admitted CRI binding refused")
	}
	expectedSource := "/var/lib/containerd/io.containerd.grpc.v1.cri/sandboxes/" + sandboxID + "/resolv.conf"
	statRaw, err := nativeRuntimeCommand(ctx, "podman", "exec", node.ContainerID, nativeRuntimeEnvBinary, "-i", "LC_ALL=C", "LANG=C", "/usr/bin/stat", "-Lc", "%d %i %u %g %a %h %s", expectedSource)
	if err != nil {
		return nil, err
	}
	var device, inode, uid, gid, mode, links, size uint64
	if _, err := fmt.Sscanf(string(statRaw), "%d %d %d %d %o %d %d", &device, &inode, &uid, &gid, &mode, &links, &size); err != nil {
		return nil, errors.New("metadata node resolver stat refused")
	}
	for key, expected := range map[string]uint64{"device": device, "inode": inode, "uid": uid, "gid": gid, "mode": mode, "links": links, "size": size} {
		if resolver[key] != float64(expected) {
			return nil, errors.New("metadata node and helper resolver identity differs")
		}
	}
	nodeRaw, err := nativeRuntimeCommand(ctx, "podman", "exec", node.ContainerID, nativeRuntimeEnvBinary, "-i", "LC_ALL=C", "LANG=C", "/usr/bin/cat", expectedSource)
	if err != nil || nativeRuntimeSHA(nodeRaw) != resolver["sha256"] || string(nodeRaw) != resolver["bytes"] {
		return nil, errors.New("metadata node and helper resolver bytes differ")
	}
	return map[string]any{"sandbox_id": sandboxID, nativePasswordContainerKey: containerID, "resolver_source": expectedSource, "node_bytes_match": true, "node_inode_match": true}, nil
}

func nativeRuntimeChildrenAbsent(ctx context.Context, clientset kubernetes.Interface, owned nativeRuntimeOwned) bool {
	pods, err := clientset.CoreV1().Pods(owned.object.GetNamespace()).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/controller-uid=" + string(owned.object.GetUID())})
	return err == nil && len(pods.Items) == 0
}

func nativeRuntimeRemove(ctx context.Context, clientset kubernetes.Interface, owned nativeRuntimeOwned) nativeRuntimeCleanupObject {
	object := owned.object
	result := nativeRuntimeCleanupObject{APIVersion: "v1", Kind: owned.kind, Namespace: object.GetNamespace(), Name: object.GetName(), UID: object.GetUID(), Action: nativeRuntimeRetained}
	if owned.kind == lifecycleJobKind {
		result.APIVersion = probeBatchAPI
	}
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(call context.Context) (bool, error) {
		var live metav1.Object
		var err error
		switch owned.kind {
		case nativeRuntimePodKind:
			live, err = clientset.CoreV1().Pods(object.GetNamespace()).Get(call, object.GetName(), metav1.GetOptions{})
		case lifecycleJobKind:
			live, err = clientset.BatchV1().Jobs(object.GetNamespace()).Get(call, object.GetName(), metav1.GetOptions{})
		case nativeRuntimeNamespaceKind:
			live, err = clientset.CoreV1().Namespaces().Get(call, object.GetName(), metav1.GetOptions{})
		default:
			return false, errors.New("metadata cleanup kind refused")
		}
		if apierrors.IsNotFound(err) {
			result.VerifiedAbsent = true
			return true, nil
		}
		if err != nil || live.GetUID() != object.GetUID() || live.GetResourceVersion() == "" {
			return false, errors.New("metadata cleanup replacement retained")
		}
		uid, version := live.GetUID(), live.GetResourceVersion()
		foreground := metav1.DeletePropagationForeground
		options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}, PropagationPolicy: &foreground}
		switch owned.kind {
		case nativeRuntimePodKind:
			err = clientset.CoreV1().Pods(object.GetNamespace()).Delete(call, object.GetName(), options)
		case lifecycleJobKind:
			err = clientset.BatchV1().Jobs(object.GetNamespace()).Delete(call, object.GetName(), options)
		case nativeRuntimeNamespaceKind:
			err = clientset.CoreV1().Namespaces().Delete(call, object.GetName(), options)
		}
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return false, errors.New("metadata owned cleanup failed")
		}
		return false, nil
	})
	if err == nil && result.VerifiedAbsent {
		result.Action = nativeRuntimeDeleted
	}
	return result
}

type nativeRuntimeMetadataReady struct {
	Kind        string                    `json:"kind"`
	Observation map[string]map[string]any `json:"observation"`
	Process     map[string]any            `json:"process"`
}

func nativeRuntimeMetadataReadReady(reader *bufio.Reader) ([]byte, nativeRuntimeMetadataReady, error) {
	var ready nativeRuntimeMetadataReady
	readyLine, err := reader.ReadSlice('\n')
	if err != nil || len(readyLine) > nativeRuntimeWireLimit {
		return nil, ready, errors.New("metadata readiness bound refused")
	}
	readyBytes := append([]byte{}, readyLine...)
	if nativeRuntimeDecode(readyBytes, &ready) != nil || ready.Kind != "metadata_ready" || len(ready.Observation) != 4 || len(ready.Process) != 3 {
		return nil, ready, errors.New("metadata readiness fields refused")
	}
	for _, name := range []string{nativeRuntimeRuntime, nativeRuntimeResolver, nativeRuntimeHome, nativeRuntimeProvider} {
		if len(ready.Observation[name]) == 0 {
			return nil, ready, errors.New("metadata observation absent")
		}
	}
	return readyBytes, ready, nil
}

type nativeRuntimeMetadataComplete struct {
	Kind    string         `json:"kind"`
	Process map[string]any `json:"process"`
	Closure bool           `json:"closure"`
}

func nativeRuntimeMetadataReadComplete(reader *bufio.Reader, stream io.ReadCloser, readyBytes []byte, process map[string]any) ([]byte, nativeRuntimeMetadataComplete, error) {
	var complete nativeRuntimeMetadataComplete
	payload, readErr := io.ReadAll(io.LimitReader(reader, nativeRuntimeWireLimit-int64(len(readyBytes))+1))
	closeErr := stream.Close()
	if readErr != nil || closeErr != nil || len(payload)+len(readyBytes) > nativeRuntimeWireLimit {
		return nil, complete, errors.New("metadata completion wire closure refused")
	}
	if nativeRuntimeDecode(payload, &complete) != nil || complete.Kind != "metadata_complete" || !complete.Closure || !reflect.DeepEqual(process, complete.Process) {
		return nil, complete, errors.New("metadata final self proof refused")
	}
	return payload, complete, nil
}

func nativeRuntimeMetadataStart(work context.Context, clientset kubernetes.Interface, input nativeRuntimeInput, node nativeRuntimeNode, image nativeRuntimeImage, index int, owned *[]nativeRuntimeOwned, unknownCreate *bool) (*batchv1.Job, *corev1.Pod, error) {
	expected, err := nativeRuntimeJob(input, node, image, index)
	if err != nil {
		return nil, nil, errors.New("metadata helper construction refused")
	}
	job, err := clientset.BatchV1().Jobs(input.Namespace).Create(work, expected, metav1.CreateOptions{})
	if err != nil || job.UID == "" {
		*unknownCreate = true
		return nil, nil, errors.New("metadata Job Create unknown")
	}
	*owned = append(*owned, nativeRuntimeOwned{object: job, kind: lifecycleJobKind})
	if !nativePreflightJobMatches(expected, job) {
		return nil, nil, errors.New("metadata Job admission changed")
	}
	pod, err := nativePasswordClientReady(work, clientset, job, image)
	if pod != nil && pod.UID != "" {
		*owned = append(*owned, nativeRuntimeOwned{object: pod, kind: nativeRuntimePodKind})
	}
	if err != nil {
		return nil, nil, errors.New("metadata running helper unavailable")
	}
	return job, pod, nil
}

func nativeRuntimeObserve(work context.Context, clientset kubernetes.Interface, input nativeRuntimeInput, node nativeRuntimeNode, image nativeRuntimeImage, index int, owned *[]nativeRuntimeOwned, unknownCreate *bool) (map[string]any, map[string]map[string]any, error) {
	live, err := clientset.CoreV1().Nodes().Get(work, node.Name, metav1.GetOptions{})
	if err != nil || live.UID != node.UID || live.Labels[nativeRuntimeRunLabel] != input.RunID || live.Spec.Unschedulable || live.Status.NodeInfo.KubeletVersion != "v1.36.4" || live.Status.NodeInfo.ContainerRuntimeVersion != "containerd://2.3.4" || live.Status.NodeInfo.OperatingSystem != nativeRuntimeLinux || live.Status.NodeInfo.Architecture != nativeRuntimeARM64 {
		return nil, nil, errors.New("metadata owned NodeInfo profile refused")
	}
	daemon, err := nativeRuntimeNodeFacts(work, node)
	if err != nil {
		return nil, nil, errors.New("metadata active daemon profile refused")
	}
	job, pod, err := nativeRuntimeMetadataStart(work, clientset, input, node, image, index, owned, unknownCreate)
	if err != nil {
		return nil, nil, err
	}
	stream, err := clientset.CoreV1().Pods(input.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: workerContainer, Follow: true}).Stream(work)
	if err != nil {
		return nil, nil, errors.New("metadata readiness stream unavailable")
	}
	closed := false
	defer func() {
		if !closed {
			_ = stream.Close()
		}
	}()
	reader := bufio.NewReaderSize(io.LimitReader(stream, nativeRuntimeWireLimit+1), nativeRuntimeWireLimit+1)
	readyBytes, ready, err := nativeRuntimeMetadataReadReady(reader)
	if err != nil {
		return nil, nil, err
	}
	profileInput, err := nativeRuntimeExpected(work, clientset, pod, nativeRuntimePhase, nil)
	if err != nil {
		return nil, nil, err
	}
	container := pod.Status.ContainerStatuses[0].ContainerID
	before, err := nativeRuntimeProfile(work, clientset, node, profileInput, container, lifecycleWaitingMain, nil, ready.Process)
	if err != nil {
		return nil, nil, err
	}
	cri, err := nativeRuntimeCRIFacts(work, node, pod, ready.Observation[nativeRuntimeResolver], before)
	if err != nil {
		return nil, nil, err
	}
	if err = nativePasswordContinuity(work, clientset, job, pod, image, false); err != nil {
		return nil, nil, err
	}
	if err = nativePasswordNode(work, clientset, input, node); err != nil {
		return nil, nil, err
	}
	released, err := nativeRuntimeProfile(work, clientset, node, profileInput, container, "metadata_release", before, ready.Process)
	if err != nil || released["release_created"] != true {
		return nil, nil, errors.New("metadata owned tmpfs release refused")
	}
	payload, complete, err := nativeRuntimeMetadataReadComplete(reader, stream, readyBytes, ready.Process)
	closed = true
	if err != nil {
		return nil, nil, err
	}
	terminal, replay, err := nativeRuntimePod(work, clientset, input, job, image)
	if err != nil || terminal == nil || terminal.UID != pod.UID || !bytes.Equal(replay, append(readyBytes, payload...)) {
		return nil, nil, errors.New("metadata original terminal/log closure refused")
	}
	if err = nativePasswordContinuity(work, clientset, job, pod, image, true); err != nil {
		return nil, nil, err
	}
	after, err := nativeRuntimeProfile(work, clientset, node, profileInput, container, probeTerminalCase, before, complete.Process)
	if err != nil {
		return nil, nil, err
	}
	maps.Copy(after, cri)
	after["metadata_release_created"] = true
	if err = nativePasswordNode(work, clientset, input, node); err != nil {
		return nil, nil, err
	}
	observation := map[string]any{"node_name": node.Name, "node_uid": node.UID, "node_container_id": node.ContainerID, "job_name": job.Name, nativePasswordJobKey: job.UID, "pod_name": pod.Name, "pod_uid": pod.UID, "container_id": container, "image_ref": image.Ref, nativeRuntimeImageIDKey: terminal.Status.ContainerStatuses[0].ImageID, "node_info": live.Status.NodeInfo, "node_runtime": daemon, "cri": after, nativeRuntimeRuntime: ready.Observation[nativeRuntimeRuntime], nativeRuntimeResolver: ready.Observation[nativeRuntimeResolver], nativeRuntimeHome: ready.Observation[nativeRuntimeHome], nativeRuntimeProvider: ready.Observation[nativeRuntimeProvider], "logs_eof": true, "logs_closed": true, "terminal_exit_code": 0}
	return observation, ready.Observation, nil
}

func nativeRuntimeFinish(t *testing.T, clientset kubernetes.Interface, input nativeRuntimeInput, path string, deadline time.Time, receipt *nativeRuntimeReceipt, owned []nativeRuntimeOwned, observations []map[string]any, unknownCreate bool) {
	t.Helper()
	cleanup, end := context.WithDeadline(context.Background(), deadline)
	defer end()
	blockedParent := unknownCreate
	for _, kind := range []string{nativeRuntimePodKind, lifecycleJobKind, nativeRuntimeNamespaceKind} {
		for _, object := range owned {
			if object.kind != kind {
				continue
			}
			if kind == lifecycleJobKind && !nativeRuntimeChildrenAbsent(cleanup, clientset, object) {
				blockedParent = true
			}
			if kind != nativeRuntimePodKind && blockedParent {
				version := "v1"
				if kind == lifecycleJobKind {
					version = probeBatchAPI
				}
				receipt.Cleanup.Objects = append(receipt.Cleanup.Objects, nativeRuntimeCleanupObject{APIVersion: version, Kind: kind, Namespace: object.object.GetNamespace(), Name: object.object.GetName(), UID: object.object.GetUID(), Action: nativeRuntimeRetained})
				continue
			}
			result := nativeRuntimeRemove(cleanup, clientset, object)
			receipt.Cleanup.Objects = append(receipt.Cleanup.Objects, result)
			if !result.VerifiedAbsent {
				blockedParent = true
			}
		}
	}
	receipt.Cleanup.Complete = !unknownCreate && len(owned) > 0 && len(receipt.Cleanup.Objects) == len(owned)
	for _, object := range receipt.Cleanup.Objects {
		receipt.Cleanup.Complete = receipt.Cleanup.Complete && object.VerifiedAbsent && object.Action == nativeRuntimeDeleted
	}
	for index := range receipt.Cases {
		receipt.Cases[index].Cleanup = receipt.Cleanup.Complete
	}
	output := filepath.Join(filepath.Dir(path), "metadata-observations.json")
	if err := nativeRuntimeWrite(output, observations); err != nil {
		t.Error("metadata observation persistence failed")
	} else if observed, err := nativeRuntimePrivate(output); err == nil {
		receipt.MetadataSHA = nativeRuntimeSHA(observed)
	}
	receipt.FinishedAt = float64(time.Now().UnixNano()) / 1e9
	if err := nativeRuntimeWrite(input.ReceiptPath, receipt); err != nil {
		t.Error("metadata receipt persistence failed")
	}
	if !receipt.Cleanup.Complete {
		t.Error("metadata owned cleanup incomplete; retain exact resource identities")
	}
}

func TestNativePackagedRuntimeQualification(t *testing.T) {
	path, selected := os.LookupEnv(nativeRuntimeSelector)
	if !selected {
		t.Skip("packaged runtime qualification is not selected")
	}
	raw, err := nativeRuntimePrivate(path)
	if err != nil {
		t.Fatal("required qualification input unavailable")
	}
	var input nativeRuntimeInput
	if nativeRuntimeDecode(raw, &input) != nil || nativeRuntimeValidateInput(input, path, time.Now()) != nil {
		t.Fatal("required qualification input refused")
	}
	clientset, err := nativeRuntimeClient(input)
	if err != nil {
		t.Fatal("admitted private Kubernetes client unavailable")
	}
	image, err := nativeRuntimeAssets(path, input)
	if err != nil {
		t.Fatal("native public image binding unavailable")
	}
	workDeadline := time.Unix(0, int64(input.StageDeadline*1e9))
	cleanupDeadline := time.Unix(0, int64(min(input.StageDeadline+600, input.LeaseExpiry)*1e9))
	if parent, ok := t.Deadline(); ok && parent.Before(cleanupDeadline.Add(time.Second)) {
		cleanupDeadline = parent.Add(-time.Second)
		if reserved := cleanupDeadline.Add(-600 * time.Second); reserved.Before(workDeadline) {
			workDeadline = reserved
		}
	}
	if !time.Now().Before(workDeadline) {
		t.Fatal("metadata parent deadline lacks original cleanup reserve")
	}
	work, cancel := context.WithDeadline(context.Background(), workDeadline)
	defer cancel()
	receipt := nativeRuntimeReceipt{SchemaVersion: 1, QualificationID: input.QualificationID, Phase: input.Phase, Pair: input.Pair, RunID: input.RunID, InputSHA: nativeRuntimeSHA(raw), QualificationSourceSHA: input.QualificationSourceSHA, EvidenceProgramSHA: input.EvidenceProgramSHA, AssetsSHA: input.AssetsSHA, MetadataProgramSHA: input.MetadataProgramSHA, StartedAt: float64(time.Now().UnixNano()) / 1e9, Cases: []nativeRuntimeCase{}, Cleanup: nativeRuntimeCleanup{Objects: []nativeRuntimeCleanupObject{}}}
	owned := make([]nativeRuntimeOwned, 0, 7)
	unknownCreate := false
	observations := make([]map[string]any, 0, len(input.Nodes))
	defer func() {
		nativeRuntimeFinish(t, clientset, input, path, cleanupDeadline, &receipt, owned, observations, unknownCreate)
	}()
	if _, err := clientset.CoreV1().Namespaces().Get(work, input.Namespace, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("metadata requires exact namespace absence")
	}
	namespace, err := clientset.CoreV1().Namespaces().Create(work, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: input.Namespace, Labels: map[string]string{nativeRuntimeRunLabel: input.RunID, "rpgmigrate.io/qualification": input.QualificationID}}}, metav1.CreateOptions{})
	if err != nil || namespace.UID == "" {
		unknownCreate = true
		t.Fatal("metadata namespace Create unknown or failed; do not adopt")
	}
	owned = append(owned, nativeRuntimeOwned{object: namespace, kind: nativeRuntimeNamespaceKind})
	receipt.NamespaceUID = namespace.UID
	for index, node := range input.Nodes {
		observation, collected, err := nativeRuntimeObserve(work, clientset, input, node, image, index, &owned, &unknownCreate)
		if err != nil {
			t.Fatal("required actual node metadata collection refused")
		}
		observations = append(observations, observation)
		for _, name := range []string{nativeRuntimeRuntime, nativeRuntimeResolver, nativeRuntimeHome, nativeRuntimeProvider} {
			caseName := fmt.Sprintf("node_%d/%s", index, name)
			passed := t.Run(caseName, func(t *testing.T) {
				if len(collected[name]) == 0 {
					t.Fatal("required actual metadata case absent")
				}
				if name == nativeRuntimeRuntime {
					encoded, err := json.Marshal(collected[name]["engine_version"])
					if err != nil {
						t.Fatal("actual engine discovery absent")
					}
					version, err := native.ParseVersion(encoded)
					if err != nil || len(version.Capabilities) != 7 {
						t.Fatal("actual Linux-seven engine capability discovery refused")
					}
				}
			})
			status, exit := nativeRuntimeRefused, 1
			if passed {
				status, exit = nativeRuntimePassed, 0
			}
			receipt.Cases = append(receipt.Cases, nativeRuntimeCase{Name: caseName, Status: status, ExitCode: exit})
		}
	}
}

func nativeRuntimeRegistryFixture(t *testing.T) (string, nativeRuntimeInput) {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	run := filepath.Join(directory, "run")
	if err := os.Mkdir(run, 0o700); err != nil {
		t.Fatal(err)
	}
	input := nativeRuntimeInput{QualificationID: strings.Repeat("1", 32), RunID: strings.Repeat("2", 32), RunDir: run, LegacySourceRoot: "/private/source", LegacySourceSHA: strings.Repeat("3", 64), LegacyGuardSHA: strings.Repeat("4", 64), QualificationSourceSHA: strings.Repeat("5", 64), EvidenceProgramSHA: strings.Repeat("6", 64), LeaseExpiry: 10000}
	for index := range 3 {
		input.Nodes = append(input.Nodes, nativeRuntimeNode{Name: fmt.Sprintf("registry-node-%d", index), ContainerID: strings.Repeat(fmt.Sprint(index+1), 64), UID: types.UID(fmt.Sprint(index))})
	}
	if err := nativeRuntimeWrite(filepath.Join(run, "ownership.json"), map[string]any{"resource_lease": map[string]any{"authorized": true, "m7_cpus": 6, "expires_at": input.LeaseExpiry, "monotonic_expires_at": 11000}}); err != nil {
		t.Fatal(err)
	}
	ownership, _ := nativeRuntimePrivate(filepath.Join(run, "ownership.json"))
	input.LegacyOwnershipSHA = nativeRuntimeSHA(ownership)
	source := map[string]any{"repository": "https://github.com/ydixken/rpgmigrate", nativeRuntimeCommitKey: strings.Repeat("7", 40), "git_tree": strings.Repeat("8", 40), "payload_inventory_sha256": input.LegacySourceSHA}
	images := map[string]any{}
	bindings := map[string]map[string]nativeRuntimeImageBinding{}
	pulls := make([]map[string]string, 0, len(input.Nodes)*3)
	for _, role := range []string{nativeRuntimeNativeRole, nativeRuntimePG16Role, nativeRuntimePG18Role} {
		repository := "ghcr.io/ydixken/rpgmigrate"
		var registryIndex any
		if role != nativeRuntimeNativeRole {
			repository = "ghcr.io/cloudnative-pg/postgresql:" + strings.TrimPrefix(role, "postgresql-")
			registryIndex = "sha256:" + strings.Repeat("c", 64)
		}
		digest, config := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
		ref := repository + "@" + digest
		image := map[string]any{nativeRuntimeRoleKey: role, "ref": ref, "os": nativeRuntimeLinux, "architecture": nativeRuntimeARM64, nativeRuntimeVariantKey: nil, "digest": digest, "index_digest": registryIndex, "config_digest": config, "provenance": map[string]string{"origin_ref": ref, "registry_receipt_sha256": strings.Repeat("d", 64), "public_receipt_sha256": strings.Repeat("e", 64)}}
		if role == nativeRuntimeNativeRole {
			publication := maps.Clone(source)
			for _, key := range []string{"build_input_sha256", "executable_sha256", "smoke_receipt_sha256"} {
				publication[key] = strings.Repeat("f", 64)
			}
			image["source"] = publication
		}
		images[role] = map[string]any{probeImageCase: image}
		bindings[role] = map[string]nativeRuntimeImageBinding{}
		for _, node := range input.Nodes {
			ids := []string{config, digest, ref}
			slices.Sort(ids)
			bindings[role][node.Name] = nativeRuntimeImageBinding{NodeID: node.ContainerID, RequestedRef: ref, ManifestDigest: digest, ConfigDigest: config, OS: nativeRuntimeLinux, Architecture: nativeRuntimeARM64, AcceptedImageIDs: ids}
			pulls = append(pulls, map[string]string{"node_id": node.ContainerID, "requested_ref": ref, nativeRuntimeStatusKey: "verified"})
		}
	}
	locked := map[string]any{}
	for role, entry := range images {
		locked[role] = entry.(map[string]any)[probeImageCase]
	}
	for _, role := range []string{nativeRuntimeNodeMode, "cnpg-controller", "postgresql-17", nativeRuntimeManagerRole, "legacy-runner", "seed-tool-runner", "otel-collector", "storage-helper", "storage-provisioner"} {
		locked[role] = map[string]any{"fixture": true}
	}
	lock := map[string]any{nativeRuntimeSchemaKey: 2, "transport": nativeRuntimeTransport, "operator_source": source, "engine_source": source, "images": locked, "publication_receipts_sha256": map[string]string{nativeRuntimeManagerRole: strings.Repeat("a", 64), nativeRuntimeNativeRole: strings.Repeat("b", 64)}}
	if err := nativeRuntimeWrite(filepath.Join(run, "image-lock.json"), lock); err != nil {
		t.Fatal(err)
	}
	lockRaw, _ := nativeRuntimePrivate(filepath.Join(run, "image-lock.json"))
	packet := map[string]any{nativeRuntimeSchemaKey: 2, nativeRuntimeQualificationKey: input.QualificationID, nativeRuntimeRunKey: input.RunID, "legacy_source_root": input.LegacySourceRoot, "legacy_source_tree_sha256": input.LegacySourceSHA, "legacy_guard_sha256": input.LegacyGuardSHA, "legacy_ownership_sha256": input.LegacyOwnershipSHA, "qualification_source_sha256": input.QualificationSourceSHA, "evidence_program_sha256": input.EvidenceProgramSHA, "image_lock_sha256": nativeRuntimeSHA(lockRaw), "images": images}
	if err := nativeRuntimeWrite(filepath.Join(directory, "native-assets.json"), packet); err != nil {
		t.Fatal(err)
	}
	packetRaw, _ := nativeRuntimePrivate(filepath.Join(directory, "native-assets.json"))
	input.AssetsSHA = nativeRuntimeSHA(packetRaw)
	receipt := map[string]any{nativeRuntimeSchemaKey: 2, "transport": nativeRuntimeTransport, nativeRuntimeQualificationKey: input.QualificationID, nativeRuntimeRunKey: input.RunID, "native_assets_sha256": input.AssetsSHA, "image_lock_sha256": nativeRuntimeSHA(lockRaw), "legacy_source_tree_sha256": input.LegacySourceSHA, "legacy_guard_sha256": input.LegacyGuardSHA, "legacy_ownership_sha256": input.LegacyOwnershipSHA, "qualification_source_sha256": input.QualificationSourceSHA, "deadline": 9000, "monotonic_deadline": 11000, "complete": true, "pulls": pulls, "node_bindings": bindings}
	if err := nativeRuntimeWrite(filepath.Join(directory, "component-image-resolution-evidence.json"), receipt); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(directory, "metadata-input.json"), input
}

func TestNativeRuntimeMetadataRegistryAssetsRefuseStaleOrPartialBindings(t *testing.T) {
	path, input := nativeRuntimeRegistryFixture(t)
	images, err := nativeRuntimeAssetsByRole(path, input)
	if err != nil || len(images) != 3 {
		t.Fatal("exact public lock and complete bindings refused", err)
	}
	if image, err := nativeRuntimeAssets(path, input); err != nil || image.Ref != images[nativeRuntimeNativeRole].Ref {
		t.Fatal("metadata used a different image authority", err)
	}
	if images, err := nativePasswordImages(path, input); err != nil || len(images) != 2 {
		t.Fatal("password probe used a different image authority", err)
	}
	for _, change := range []string{"old-schema", nativeRuntimePartialCase, nativeRuntimeNodeMode, "missing-node", "unknown-id", nativeRuntimeVariantKey, "clock", "lock", "ownership"} {
		t.Run(change, func(t *testing.T) {
			path, input := nativeRuntimeRegistryFixture(t)
			name := filepath.Join(filepath.Dir(path), "component-image-resolution-evidence.json")
			raw, _ := nativeRuntimePrivate(name)
			var value map[string]any
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
			nodes := value["node_bindings"].(map[string]any)[nativeRuntimeNativeRole].(map[string]any)
			binding := nodes[input.Nodes[0].Name].(map[string]any)
			switch change {
			case "old-schema":
				value[nativeRuntimeSchemaKey] = 1
			case nativeRuntimePartialCase:
				value["complete"] = false
			case nativeRuntimeNodeMode:
				binding["node_id"] = strings.Repeat("0", 64)
			case "missing-node":
				delete(nodes, input.Nodes[0].Name)
			case "unknown-id":
				binding["accepted_image_ids"] = append(binding["accepted_image_ids"].([]any), "docker-pullable://foreign")
			case nativeRuntimeVariantKey:
				binding[nativeRuntimeVariantKey] = "v8"
			case "clock":
				value["monotonic_deadline"] = 11001
			case "lock":
				name = filepath.Join(input.RunDir, "image-lock.json")
				value = map[string]any{nativeRuntimeSchemaKey: 1}
			case "ownership":
				name = filepath.Join(input.RunDir, "ownership.json")
				value = map[string]any{"replaced-ownership": true}
			}
			changed, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(name, changed, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := nativeRuntimeAssetsByRole(path, input); err == nil {
				t.Fatal("stale, partial or unbound registry proof accepted")
			}
		})
	}
}

func TestNativeRuntimeMetadataRegistryImageIDsUseExactNodeBinding(t *testing.T) {
	image := nativeRuntimeImage{Ref: "ghcr.io/ydixken/rpgmigrate@sha256:" + strings.Repeat("3", 64), Digest: "sha256:" + strings.Repeat("3", 64), ConfigDigest: "sha256:" + strings.Repeat("4", 64)}
	image.NodeBindings = map[string]nativeRuntimeImageBinding{nativePasswordOwnedNode: {NodeID: strings.Repeat("5", 64), RequestedRef: image.Ref, ManifestDigest: image.Digest, ConfigDigest: image.ConfigDigest, OS: nativeRuntimeLinux, Architecture: nativeRuntimeARM64, AcceptedImageIDs: []string{image.Ref, image.Digest, image.ConfigDigest}}}
	for _, observed := range image.NodeBindings[nativePasswordOwnedNode].AcceptedImageIDs {
		if !nativeRuntimeImageID(observed, image, nativePasswordOwnedNode) {
			t.Fatal("proven exact-node image identity refused")
		}
		if nativeRuntimeImageID(observed, image, "foreign-node") {
			t.Fatal("a different node's image identity was adopted")
		}
	}
	for _, observed := range []string{"docker-pullable://" + image.Ref, "containerd://" + image.ConfigDigest, image.Ref + "extra"} {
		if nativeRuntimeImageID(observed, image, nativePasswordOwnedNode) {
			t.Fatal("unobserved or normalized image identity accepted")
		}
	}
}

func TestNativeRuntimeMetadataLocalBoundaries(t *testing.T) {
	for _, raw := range []string{`{"a":1,"a":2}`, `{} {}`, `{"a":NaN}`} {
		var value map[string]any
		if nativeRuntimeDecode([]byte(raw), &value) == nil {
			t.Fatal("ambiguous metadata JSON accepted")
		}
	}
	input := nativeRuntimeInput{QualificationID: strings.Repeat("1", 32), RunID: strings.Repeat("2", 32), Namespace: "native-metadata-" + strings.Repeat("1", 16)}
	image := nativeRuntimeImage{Ref: "localhost/rpgmigrate@sha256:" + strings.Repeat("3", 64), Digest: "sha256:" + strings.Repeat("3", 64), ConfigDigest: "sha256:" + strings.Repeat("4", 64)}
	job, err := nativeRuntimeJob(input, nativeRuntimeNode{Name: nativePasswordOwnedNode}, image, 0)
	if err != nil {
		t.Fatal(err)
	}
	pod := job.Spec.Template.Spec
	if pod.NodeName != nativePasswordOwnedNode || len(pod.Containers) != 1 || len(pod.Volumes) != 1 || pod.Volumes[0].EmptyDir == nil || pod.Volumes[0].EmptyDir.Medium != corev1.StorageMediumMemory || pod.Volumes[0].EmptyDir.SizeLimit.Value() != 64*1024*1024 || len(pod.Containers[0].Env) != 0 || pod.Containers[0].SecurityContext.ReadOnlyRootFilesystem == nil || !*pod.Containers[0].SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("metadata canonical isolation changed")
	}
	for _, unexpected := range []string{workerForeignIdentity, image.Digest, image.ConfigDigest, "docker-pullable://" + image.Ref, image.ConfigDigest + "extra", "containerd://" + image.ConfigDigest} {
		if nativeRuntimeImageID(unexpected, image, nativePasswordOwnedNode) {
			t.Fatal("unqualified image identity accepted")
		}
	}
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "input.json")
	if err := nativeRuntimeWrite(path, map[string]int{"marker": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeRuntimePrivate(path); err != nil {
		t.Fatal(err)
	}
	if nativeRuntimeWrite(path, map[string]int{"marker": 2}) == nil {
		t.Fatal("metadata evidence was overwritten")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeRuntimePrivate(path); err == nil {
		t.Fatal("unsafe private metadata mode accepted")
	}
}

func TestNativeRuntimeMetadataCleanupIdentity(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			original := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "metadata-owned", Namespace: "metadata-test", UID: "original", ResourceVersion: "1"}}
			actual := original.DeepCopy()
			if replacement {
				actual.UID = workerForeignIdentity
			}
			clientset := kubefake.NewClientset(actual)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result := nativeRuntimeRemove(ctx, clientset, nativeRuntimeOwned{object: original, kind: nativeRuntimePodKind})
			deletes := 0
			for _, action := range clientset.Actions() {
				if deletion, ok := action.(ktesting.DeleteAction); ok {
					deletes++
					preconditions := deletion.GetDeleteOptions().Preconditions
					if preconditions == nil || preconditions.UID == nil || *preconditions.UID != original.UID || preconditions.ResourceVersion == nil || *preconditions.ResourceVersion != "1" {
						t.Fatal("cleanup lacks exact observed UID/version preconditions")
					}
				}
			}
			if replacement {
				if result.VerifiedAbsent || result.Action != nativeRuntimeRetained || deletes != 0 {
					t.Fatal("cleanup adopted a replacement")
				}
			} else if !result.VerifiedAbsent || result.Action != nativeRuntimeDeleted || deletes != 1 {
				t.Fatal("owned cleanup absence was not checked")
			}
		})
	}
}

func TestNativeRuntimeMetadataInputSelection(t *testing.T) {
	evidenceSHA := native.EvidenceProgramSHA256()
	digest := strings.Repeat("1", 64)
	input := nativeRuntimeInput{SchemaVersion: 1, QualificationID: strings.Repeat("1", 32), Phase: nativeRuntimePhase, RunID: strings.Repeat("2", 32), RunDir: "/private/native-run", LegacySourceRoot: "/private/legacy-source", LegacySourceSHA: digest, LegacyGuardSHA: digest, LegacyOwnershipSHA: digest, QualificationSourceSHA: digest, EvidenceProgramSHA: hex.EncodeToString(evidenceSHA[:]), AssetsSHA: digest, Kubeconfig: "/private/native-run/kubeconfig", Context: "kind-rpgmigrate-m7-" + strings.Repeat("2", 32), Namespace: "native-metadata-" + strings.Repeat("1", 16), StageDeadline: 1600, LeaseExpiry: 2200, RequiredCases: nativeRuntimeCases(), ReceiptPath: "/private/native-qual/metadata-receipt.json", MetadataProgramSHA: nativeRuntimeSHA([]byte(nativeRuntimeMetadataProgram + "\x00" + nativeRuntimeNodeProgram))}
	for index := range 3 {
		input.Nodes = append(input.Nodes, nativeRuntimeNode{Name: fmt.Sprintf("node-%d", index), ContainerID: strings.Repeat(fmt.Sprint(index+1), 64), UID: types.UID(fmt.Sprintf("uid-%d", index))})
	}
	path := "/private/native-qual/metadata-input.json"
	now := time.Unix(1000, 0)
	if err := nativeRuntimeValidateInput(input, path, now); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"empty_inventory", "wrong_program", "stale_lease", "namespace_adoption", "missing_node", "wrong_context"} {
		t.Run(change, func(t *testing.T) {
			candidate := input
			switch change {
			case "empty_inventory":
				candidate.RequiredCases = nil
			case "wrong_program":
				candidate.MetadataProgramSHA = digest
			case "stale_lease":
				candidate.LeaseExpiry = 2199
			case "namespace_adoption":
				uid := types.UID("existing")
				candidate.NamespaceUID = &uid
			case "missing_node":
				candidate.Nodes = candidate.Nodes[:2]
			case "wrong_context":
				candidate.Context = workerForeignIdentity
			}
			if nativeRuntimeValidateInput(candidate, path, now) == nil {
				t.Fatal("unsafe metadata input accepted")
			}
		})
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "namespace_uid")
	raw, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	var candidate nativeRuntimeInput
	if nativeRuntimeDecode(raw, &candidate) == nil {
		t.Fatal("missing nullable field accepted as an explicit initial namespace selection")
	}
}

func TestNativeRuntimeActiveExpectedUsesIndependentServiceAndPodInputs(t *testing.T) {
	input := nativeRuntimeInput{QualificationID: strings.Repeat("1", 32), Namespace: "owned-namespace"}
	image := nativeRuntimeImage{Ref: "localhost/rpgmigrate@sha256:" + strings.Repeat("2", 64)}
	job, err := nativeRuntimeJob(input, nativeRuntimeNode{Name: nativePasswordOwnedNode}, image, 0)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "owned-helper", Namespace: input.Namespace, UID: "owned-pod"}, Spec: *job.Spec.Template.Spec.DeepCopy()}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "kubernetes", Namespace: nativePreflightDefaultSA, UID: "original-service"}, Spec: corev1.ServiceSpec{ClusterIP: "10.97.42.1", Type: corev1.ServiceTypeClusterIP, Ports: []corev1.ServicePort{{Name: nativeRuntimeHTTPS, Port: 443, Protocol: corev1.ProtocolTCP}}}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nativePasswordOwnedNode}, Status: corev1.NodeStatus{Capacity: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("12Gi")}}}
	clientset := kubefake.NewClientset(service, node)
	expected, err := nativeRuntimeExpected(context.Background(), clientset, pod, nativeRuntimePhase, nil)
	if err != nil || expected.Environment["KUBERNETES_SERVICE_HOST"] != "10.97.42.1" || expected.Environment["HOSTNAME"] != pod.Name || expected.Environment["HOME"] != "" {
		t.Fatal("independent Service/Pod environment derivation failed")
	}
	if expected.ProcessEnvironment["HOME"] != "/" {
		t.Fatal("runc HOME for a UID without a passwd entry is not expected")
	}
	if len(expected.Command) != 10 || !slices.Equal(expected.RunningArgs, expected.Command[5:]) || expected.OOMScoreAdj != 980 {
		t.Fatal("fixed command or pinned burstable OOM derivation changed")
	}
	pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: nativeRuntimePreload, Value: nativeCleanupTestPrivateCanary}}
	if _, err := nativeRuntimeExpected(context.Background(), clientset, pod, nativeRuntimePhase, nil); err == nil {
		t.Fatal("unqualified environment became expected authority")
	}
}

func TestNativeRuntimeActiveProjectionSeparatesTerminalAndLiveProof(t *testing.T) {
	id, podUID := strings.Repeat("1", 64), "owned-pod"
	value := nativeRuntimeProfileResult{ContainerID: id, PodUID: podUID, SandboxID: strings.Repeat("2", 64), PID: 100, StartTicks: 200, BootID: "00000000-0000-0000-0000-000000000001", MountNamespace: "mnt:[3]", MountCount: 20, TmpDevice: "1", TmpInode: "2", TerminationLogID: "0123abcd", SandboxPID: 50, State: lifecycleWaitingMain, LiveProcess: true, FinalSpec: true, ActualMounts: true, ActualEnvironment: true, ResourceBounds: true, StockHook: true, LastAdmittedProcess: true, Endpoint: true, ImmutableNodeFiles: true}
	raw, _ := json.Marshal(value)
	if _, err := nativeRuntimeProjection(raw, id, podUID, lifecycleWaitingMain); err != nil {
		t.Fatal(err)
	}
	value.State, value.LiveProcess, value.TerminalPIDAbsent = probeTerminalCase, false, true
	value.ActualMounts, value.ActualEnvironment, value.ResourceBounds = false, false, false
	raw, _ = json.Marshal(value)
	if _, err := nativeRuntimeProjection(raw, id, podUID, probeTerminalCase); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"live_claim", "pid_reuse", "extra_field", "false_proof"} {
		t.Run(change, func(t *testing.T) {
			var changed map[string]any
			if json.Unmarshal(raw, &changed) != nil {
				t.Fatal("fixture decode failed")
			}
			switch change {
			case "live_claim":
				changed["actual_environment_verified"] = true
			case "pid_reuse":
				changed["terminal_pid_absent"] = false
			case "extra_field":
				changed["raw_config"] = nativeCleanupTestPrivateCanary
			case "false_proof":
				changed["last_admitted_process_verified"] = false
			}
			candidate, _ := json.Marshal(changed)
			if _, err := nativeRuntimeProjection(candidate, id, podUID, probeTerminalCase); err == nil || strings.Contains(err.Error(), nativeCleanupTestPrivateCanary) {
				t.Fatal("unqualified terminal projection or sensitive error accepted")
			}
		})
	}
}

func TestNativeRuntimeMetadataRetainsParentsOfUnknownPod(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "metadata-job", Namespace: "metadata-test", UID: "job-original"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "metadata-replacement", Namespace: job.Namespace, UID: "unrecorded", Labels: map[string]string{batchv1.ControllerUidLabel: string(job.UID)}}}
	clientset := kubefake.NewClientset(job, pod)
	if nativeRuntimeChildrenAbsent(context.Background(), clientset, nativeRuntimeOwned{object: job, kind: lifecycleJobKind}) {
		t.Fatal("parent cleanup ignored an unrecorded replacement Pod")
	}
	for _, action := range clientset.Actions() {
		if action.GetVerb() != "list" {
			t.Fatal("parent safety check mutated a resource")
		}
	}
}

func TestNativeRuntimeMetadataContainerIncarnation(t *testing.T) {
	originalID := "containerd://metadata-original"
	image := nativeRuntimeImage{Ref: "localhost/rpgmigrate@sha256:" + strings.Repeat("3", 64), Digest: "sha256:" + strings.Repeat("3", 64), ConfigDigest: "sha256:" + strings.Repeat("4", 64)}
	image.NodeBindings = map[string]nativeRuntimeImageBinding{nativePasswordOwnedNode: {RequestedRef: image.Ref, ManifestDigest: image.Digest, ConfigDigest: image.ConfigDigest, AcceptedImageIDs: []string{image.Ref}}}
	pod := &corev1.Pod{Spec: corev1.PodSpec{NodeName: nativePasswordOwnedNode}, Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: workerContainer, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}}}}}
	known, terminal, err := nativeRuntimeStatus(pod, image, "")
	if err != nil || known != "" || terminal {
		t.Fatal("initial waiting container was treated as a replacement")
	}
	pod.Status.ContainerStatuses[0] = corev1.ContainerStatus{Name: workerContainer, ContainerID: originalID, ImageID: image.Ref, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}
	known, terminal, err = nativeRuntimeStatus(pod, image, "")
	if err != nil || known != originalID || !terminal {
		t.Fatal("actual clean terminal container was not bound")
	}
	pod.Status.ContainerStatuses[0].ContainerID = "containerd://changed"
	if _, _, err := nativeRuntimeStatus(pod, image, known); err == nil {
		t.Fatal("replacement container accepted")
	}
	pod.Status.ContainerStatuses = nil
	if _, _, err := nativeRuntimeStatus(pod, image, known); err == nil {
		t.Fatal("lost bound container status accepted")
	}
}

func TestNativeRuntimeMetadataCertificateOnly(t *testing.T) {
	cluster := &clientcmdapi.Cluster{CertificateAuthorityData: []byte("public-test-ca")}
	auth := &clientcmdapi.AuthInfo{ClientCertificateData: []byte("public-test-certificate"), ClientKeyData: []byte("unit-test-placeholder")}
	if !nativeRuntimeCertificateOnly(cluster, auth) {
		t.Fatal("certificate-only owned client rejected")
	}
	for _, change := range []string{"exec", "token", "impersonation", "tls_name", "proxy"} {
		t.Run(change, func(t *testing.T) {
			candidateCluster, candidateAuth := cluster.DeepCopy(), auth.DeepCopy()
			switch change {
			case "exec":
				candidateAuth.Exec = &clientcmdapi.ExecConfig{Command: workerForeignIdentity}
			case "token":
				candidateAuth.Token = workerForeignIdentity
			case "impersonation":
				candidateAuth.Impersonate = workerForeignIdentity
			case "tls_name":
				candidateCluster.TLSServerName = workerForeignIdentity
			case "proxy":
				candidateCluster.ProxyURL = "https://untrusted.invalid"
			}
			if nativeRuntimeCertificateOnly(candidateCluster, candidateAuth) {
				t.Fatal("alternate authentication or endpoint route accepted")
			}
		})
	}
}

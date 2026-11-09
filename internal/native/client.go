package native

import (
	"bytes"
	"context"
	"errors"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

var (
	ErrNativeControlUnavailable     = errors.New("native control unavailable")
	ErrNativeSetEndposUnknownEffect = errors.New("native endpos effect unknown; preserve saved boundary")
)

type Client struct{ transport NativeTransport }

const nativeProgressCommand = "list progress"

func NewClient(transport NativeTransport) *Client { return &Client{transport: transport} }

func (c *Client) CurrentEndLSN(ctx context.Context, m *v1beta1.Migration, want ExecutionData) (string, error) {
	v, err := c.sourceObservation(ctx, m, want, sourceHeadKind)
	if err != nil {
		return "", err
	}
	return v.HeadLSN, nil
}

func (c *Client) SampleLag(ctx context.Context, m *v1beta1.Migration, want ExecutionData) (*InformationalLag, error) {
	v, err := c.sourceObservation(ctx, m, want, activeLagKind)
	if err != nil {
		return nil, err
	}
	return &InformationalLag{valid: true, lagBytes: v.LagBytes, admissionFingerprint: sourceAdmissionFingerprint(want, v.SourceBindingSHA256)}, nil
}

func (c *Client) sourceObservation(ctx context.Context, m *v1beta1.Migration, w ExecutionData, kind string) (*sourceResultWire, error) {
	if c == nil || c.transport == nil || ctx.Err() != nil || !sourceMigrationMatches(m, w) {
		return nil, ErrNativeControlUnavailable
	}
	argv, err := sourceExecArgv(kind, w)
	if err != nil {
		return nil, ErrNativeControlUnavailable
	}
	body, err := sourceRequest(kind, w)
	if err != nil {
		return nil, ErrNativeControlUnavailable
	}
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	outcome, err := c.transport.InPodNative(callCtx, w.Namespace, w.PodName, w.Container, argv, body)
	if err != nil || !outcome.Successful() || callCtx.Err() != nil || !sourceMigrationMatches(m, w) {
		return nil, ErrNativeControlUnavailable
	}
	result, err := parseSourceResult(outcome.Payload(), body, kind, w)
	if err != nil {
		return nil, ErrNativeControlUnavailable
	}
	return result, nil
}

func sourceMigrationMatches(m *v1beta1.Migration, w ExecutionData) bool {
	return activeRoleWorker(w) && m != nil && m.Namespace == w.Namespace && m.Name == w.MigrationName && m.UID == w.MigrationUID && m.Generation == w.MigrationGeneration && m.DeletionTimestamp.IsZero() && m.Spec.Engine == v1beta1.MigrationEngineRPGMigrate && m.Spec.Follow != nil && m.Spec.Follow.Enabled && m.Status.Attempts == w.Attempt && m.Status.JobName == w.JobName
}

// Recovery reads startup materialization without replaying its file-writing prelude.
const controlRecovery = `import os,sys,stat,signal
try:
 signal.setitimer(signal.ITIMER_REAL,25)
 mode=sys.argv[1]
 if mode=='progress' and len(sys.argv)==2:
  roles=('source','target');args=['list','progress','--json','--dir','/work/pgcopydb']
 elif mode=='set_endpos' and len(sys.argv)==3:
  e=sys.argv[2];p=e.split('/')
  if len(p)!=2 or any(not x or len(x)>8 or any(c not in '0123456789abcdefABCDEF' for c in x) for x in p) or not any(int(x,16) for x in p): raise ValueError()
  roles=('target',);args=['stream','sentinel','set','endpos',e,'--json','--dir','/work/pgcopydb']
 else: raise ValueError()
 env=dict(os.environb)
 if mode=='set_endpos':env.pop(b'PGCOPYDB_SOURCE_PGURI',None)
 def stamp(s):return (s.st_dev,s.st_ino,s.st_mode,s.st_nlink,s.st_uid,s.st_gid,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
 def read(fd):
  value=bytearray()
  while True:
   block=os.read(fd,min(8192,65537-len(value)))
   if not block:return bytes(value)
   value.extend(block)
   if len(value)>65536:raise ValueError()
 for role in roles:
  path='/tmp/pgm-'+role+'-uri';key=('PGCOPYDB_'+role.upper()+'_PGURI').encode('ascii')
  try:fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW|os.O_CLOEXEC)
  except FileNotFoundError:
   if key not in env:raise ValueError()
  else:
   try:
    before=os.fstat(fd)
    if not stat.S_ISREG(before.st_mode) or before.st_uid!=os.geteuid() or stat.S_IMODE(before.st_mode)!=0o600 or before.st_nlink!=1 or before.st_size>65536:raise ValueError()
    raw=read(fd)
    if stamp(os.fstat(fd))!=stamp(before) or stamp(os.stat(path,follow_symlinks=False))!=stamp(before):raise ValueError()
    os.lseek(fd,0,0)
    if read(fd)!=raw or stamp(os.fstat(fd))!=stamp(before):raise ValueError()
    env[key]=raw
   finally:os.close(fd)
  if not env[key] or b'\0' in env[key]:raise ValueError()
 if sum(len(k)+len(v)+2 for k,v in env.items())+sum(len(x)+1 for x in args)+len('/usr/local/bin/rpgmigrate')+1>65536:raise ValueError()
 os.execve('/usr/local/bin/rpgmigrate',['/usr/local/bin/rpgmigrate']+args,env)
except BaseException:
 os._exit(1)
`

func controlArgv(w ExecutionData, mode, savedE string) ([]string, error) {
	if !activeRoleWorker(w) || len(controlRecovery) > 4096 {
		return nil, ErrNativeControlUnavailable
	}
	argv := []string{nativeEnvExecutable, "-u", nativePreloadEnv, "-u", nativeLibraryPathEnv, nativePythonExecutable, "-I", "-B", "-c", controlRecovery, mode}
	if mode == "set_endpos" {
		if end, err := sentinel.ParseLSN(savedE); err != nil || end == 0 {
			return nil, ErrNativeControlUnavailable
		}
		argv = append(argv, savedE)
	} else if mode != "progress" || savedE != "" {
		return nil, ErrNativeControlUnavailable
	}
	if !sourceExecFits(w, argv) {
		return nil, ErrNativeControlUnavailable
	}
	return argv, nil
}

func (c *Client) Progress(ctx context.Context, w ExecutionData) (*Report, error) {
	if c == nil || c.transport == nil || ctx.Err() != nil {
		return nil, ErrNativeControlUnavailable
	}
	argv, err := controlArgv(w, "progress", "")
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	outcome, err := c.transport.InPodNative(callCtx, w.Namespace, w.PodName, w.Container, argv, nil)
	if err != nil || !outcome.Successful() || callCtx.Err() != nil {
		return nil, ErrNativeControlUnavailable
	}
	raw := outcome.Payload()
	if !singleNativeReport(raw) {
		return nil, ErrNativeControlUnavailable
	}
	report, err := ParseReportLine(raw, Binding{Command: nativeProgressCommand, Work: w.Work})
	if err != nil {
		return nil, ErrNativeControlUnavailable
	}
	return report, nil
}

// A reply is data only; later independent checkpoint admission owns acknowledgment.
func (c *Client) SetEndpos(ctx context.Context, w ExecutionData, savedE string) ([]byte, error) {
	if c == nil || c.transport == nil || ctx.Err() != nil {
		return nil, ErrNativeControlUnavailable
	}
	argv, err := controlArgv(w, "set_endpos", savedE)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	outcome, err := c.transport.InPodNative(callCtx, w.Namespace, w.PodName, w.Container, argv, nil)
	if err != nil || !outcome.Successful() || callCtx.Err() != nil {
		return nil, ErrNativeSetEndposUnknownEffect
	}
	raw := outcome.Payload()
	if len(raw) == 0 || len(raw) > nativeWireLimit {
		return nil, ErrNativeSetEndposUnknownEffect
	}
	return bytes.Clone(raw), nil
}

func singleNativeReport(raw []byte) bool {
	return len(raw) > len("native-report: \n") && len(raw) <= nativeWireLimit && raw[len(raw)-1] == '\n' && !bytes.ContainsRune(raw[:len(raw)-1], '\n') && bytes.HasPrefix(raw, []byte("native-report: "))
}

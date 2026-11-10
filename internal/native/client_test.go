package native

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

type sourceAPI struct {
	*httptest.Server
	mu                 sync.Mutex
	want               ExecutionData
	migration          *v1beta1.Migration
	job                *batchv1.Job
	pod                *corev1.Pod
	t                  *testing.T
	argv               []string
	input              []byte
	targetBytes, calls int
	fault              string
	output             []byte
	after              func(*corev1.Pod)
}

func newSourceAPI(t *testing.T) (*sourceAPI, *Client) {
	t.Helper()
	w := sourceWant()
	controller := true
	job := &batchv1.Job{TypeMeta: metav1.TypeMeta{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: sourceJobKind}, ObjectMeta: metav1.ObjectMeta{Namespace: w.Namespace, Name: w.JobName, UID: w.JobUID}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: nativeContainerName, Image: w.ImageRef, Command: []string{"/usr/local/bin/rpgmigrate"}}}}}}}
	w.TemplateSHA256 = podexec.NativeTemplateSHA256(job.Spec.Template)
	w.Container.TemplateSHA256 = w.TemplateSHA256
	w.Container.ExecutionSHA256 = podexec.NativeExecutionSHA256(job.Spec.Template)
	pod := &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Namespace: w.Namespace, Name: w.PodName, UID: w.Container.PodUID, OwnerReferences: []metav1.OwnerReference{{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: sourceJobKind, Name: w.JobName, UID: w.JobUID, Controller: &controller}}}, Spec: *job.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: nativeContainerName, Image: w.ImageRef, ImageID: w.PlatformImageID, ContainerID: w.Container.ContainerID, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	m := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{Namespace: w.Namespace, Name: w.MigrationName, UID: w.MigrationUID, Generation: w.MigrationGeneration}, Spec: v1beta1.MigrationSpec{Engine: v1beta1.MigrationEngineRPGMigrate, Follow: &v1beta1.FollowOptions{Enabled: true}}, Status: v1beta1.MigrationStatus{Attempts: w.Attempt, JobName: w.JobName, NativeJobUID: string(w.WorkerJobUID), NativePVCUID: string(w.PVCUID)}}
	s := &sourceAPI{want: w, migration: m, job: job, pod: pod, t: t}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	transport, err := podexec.New(&rest.Config{Host: s.URL})
	if err != nil {
		t.Fatal(err)
	}
	return s, NewClient(transport)
}

func (s *sourceAPI) serve(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/exec") {
		s.exec(w, r)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case strings.HasSuffix(r.URL.Path, "/jobs/"+s.want.JobName):
		_ = json.NewEncoder(w).Encode(s.job)
	case strings.HasSuffix(r.URL.Path, "/pods/"+s.want.PodName):
		_ = json.NewEncoder(w).Encode(s.pod)
	default:
		http.NotFound(w, r)
	}
}

func (s *sourceAPI) exec(w http.ResponseWriter, r *http.Request) {
	upgrade := websocket.Upgrader{Subprotocols: []string{"v5.channel.k8s.io"}}
	c, err := upgrade.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = c.Close() }()
	var input []byte
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return
		}
		if bytes.Equal(raw, []byte{255, 0}) {
			break
		}
		if len(raw) == 0 || raw[0] != 0 {
			return
		}
		input = append(input, raw[1:]...)
	}
	s.mu.Lock()
	s.calls++
	s.argv = slices.Clone(r.URL.Query()["command"])
	s.input = bytes.Clone(input)
	s.targetBytes = len(r.URL.RequestURI())
	output := bytes.Clone(s.output)
	if output == nil {
		var request sourceRequestWire
		if json.Unmarshal(input, &request) != nil {
			s.t.Error("checked source request was not JSON")
		} else {
			output = sourceResult(s.t, request.Kind, s.want, input)
		}
	}
	fault := s.fault
	if s.after != nil {
		s.after(s.pod)
	}
	s.mu.Unlock()
	_ = c.WriteMessage(websocket.BinaryMessage, append([]byte{1}, output...))
	if fault == sourceStderrCase {
		_ = c.WriteMessage(websocket.BinaryMessage, []byte{2, 'x'})
	}
	status := []byte(`{"status":"Success"}`)
	if fault == readerStatusCase {
		status = []byte(`{"status":"Failure"}`)
	}
	if fault != sourceStdoutOnlyCase {
		_ = c.WriteMessage(websocket.BinaryMessage, append([]byte{3}, status...))
	}
	if fault == sourceHoldCase {
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}
	if fault != readerFaultDisconnect {
		_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
	}
}

func TestNativeInformationalLagSample(t *testing.T) {
	for _, kind := range []string{sourceHeadKind, activeLagKind} {
		t.Run(kind, func(t *testing.T) {
			s, c := newSourceAPI(t)
			if kind == sourceHeadKind {
				value, err := c.CurrentEndLSN(context.Background(), s.migration, s.want)
				if err != nil || value != fixtureEndLSN {
					t.Fatal("checked source head refused")
				}
			} else {
				value, err := c.SampleLag(context.Background(), s.migration, s.want)
				if err != nil || value == nil || value.LagBytes() == nil || *value.LagBytes() != 0 || value.AdmissionFingerprint() == ([32]byte{}) {
					t.Fatal("checked zero lag refused")
				}
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.calls != 1 || s.targetBytes > nativeWireLimit || len(s.input) > nativeWireLimit || len(s.argv) != 14 || s.argv[10] != kind {
				t.Fatal("checked source command did not preserve framing")
			}
			t.Logf("actual Go checked-exec request target=%d stdin=%d", s.targetBytes, len(s.input))
		})
	}
	for _, fault := range []string{sourceStderrCase, readerStatusCase, sourceStdoutOnlyCase, readerFaultDisconnect, readerReplacementUID, sourceRestartCase, sourceImageCase, "cancel"} {
		t.Run(fault, func(t *testing.T) {
			s, c := newSourceAPI(t)
			s.fault = fault
			switch fault {
			case readerReplacementUID:
				s.after = func(p *corev1.Pod) { p.UID = evidenceOther }
			case sourceRestartCase:
				s.after = func(p *corev1.Pod) { p.Status.ContainerStatuses[0].RestartCount++ }
			case sourceImageCase:
				s.after = func(p *corev1.Pod) { p.Status.ContainerStatuses[0].ImageID = evidenceOther }
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if fault == "cancel" {
				cancel()
			}
			value, err := c.SampleLag(ctx, s.migration, s.want)
			if err == nil || value != nil || strings.Contains(err.Error(), "secret-canary") {
				t.Fatal("unknown transport became an informational sample")
			}
		})
	}
}

func controlReport(t *testing.T, w ExecutionData) []byte {
	t.Helper()
	zero := int64(0)
	r := Report{ProtocolVersion: 1, EngineVersion: "1", Command: nativeProgressCommand, Capabilities: []Capability{CapProgress}, Work: &w.Work, Phase: sourceCopyingPhase, Copy: &CopyProgress{Tables: &Counters{Total: &zero, Done: &zero}, Indexes: &Counters{Total: &zero, Done: &zero}, Bytes: &Counters{Total: &zero, Done: &zero}}}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return append(append([]byte("native-report: "), raw...), '\n')
}

func TestNativeControlCommands(t *testing.T) {
	t.Run("progress", func(t *testing.T) {
		s, c := newSourceAPI(t)
		s.output = controlReport(t, s.want)
		r, err := c.Progress(context.Background(), s.want)
		if err != nil || r == nil || r.Command != nativeProgressCommand || r.Phase != sourceCopyingPhase {
			t.Fatal("checked native progress refused")
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.calls != 1 || len(s.argv) != 11 || s.argv[9] != controlRecovery || s.argv[10] != "progress" || len(s.input) != 0 {
			t.Fatal("progress command changed")
		}
	})
	t.Run("explicit_saved_end", func(t *testing.T) {
		s, c := newSourceAPI(t)
		s.output = []byte("arbitrary checked command output\n")
		raw, err := c.SetEndpos(context.Background(), s.want, fixtureEndLSN)
		if err != nil || !bytes.Equal(raw, s.output) {
			t.Fatal("raw setter data refused")
		}
		raw[0] ^= 1
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.calls != 1 || len(s.argv) != 12 || s.argv[10] != "set_endpos" || s.argv[11] != fixtureEndLSN || len(s.input) != 0 || s.output[0] != 'a' {
			t.Fatal("explicit setter boundary or payload copy changed")
		}
	})
	for _, end := range []string{"", sourceZeroLSN, "--current", "invalid-lsn", "0/20 --json"} {
		s, c := newSourceAPI(t)
		if raw, err := c.SetEndpos(context.Background(), s.want, end); err == nil || raw != nil || s.calls != 0 {
			t.Fatal("invalid boundary reached control effect")
		}
	}
}

func TestNativeProgressMapping(t *testing.T) {
	for _, fault := range []string{readerCaseMissing, readerCaseDuplicate, readerCaseUnknown, "drained", "unterminated", readerFaultNoise, sourceStderrCase, readerStatusCase, readerReplacementUID} {
		t.Run(fault, func(t *testing.T) {
			s, c := newSourceAPI(t)
			s.output = controlReport(t, s.want)
			switch fault {
			case readerCaseMissing:
				s.output = []byte("{}\n")
			case readerCaseDuplicate:
				s.output = append(s.output, s.output...)
			case readerCaseUnknown:
				s.output = replace(t, s.output, `"protocol_version":1`, `"protocol_version":1,"extra":true`)
			case "drained":
				s.output = replace(t, s.output, `"phase":"copying"`, `"phase":"completed","replication":{"certified_lsn":"0/20","drained":true}`)
			case "unterminated":
				s.output = s.output[:len(s.output)-1]
			case readerFaultNoise:
				s.output = append([]byte("noise\n"), s.output...)
			case readerReplacementUID:
				s.after = func(p *corev1.Pod) { p.UID = evidenceOther }
			default:
				s.fault = fault
			}
			if r, err := c.Progress(context.Background(), s.want); r != nil || !errors.Is(err, ErrNativeControlUnavailable) {
				t.Fatal("invalid or unavailable progress accepted")
			}
		})
	}
}

func TestNativeControlPrivacy(t *testing.T) {
	for _, fault := range []string{readerStatusCase, sourceStderrCase, readerFaultDisconnect, readerReplacementUID, sourceHoldCase} {
		t.Run(fault, func(t *testing.T) {
			s, c := newSourceAPI(t)
			s.output = []byte("secret-canary\n")
			s.fault = fault
			if fault == readerReplacementUID {
				s.after = func(p *corev1.Pod) { p.UID = evidenceOther }
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			raw, err := c.SetEndpos(ctx, s.want, fixtureEndLSN)
			if raw != nil || !errors.Is(err, ErrNativeSetEndposUnknownEffect) || strings.Contains(err.Error(), "secret-canary") {
				t.Fatal("unknown setter effect leaked or became acknowledgment")
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.calls != 1 || s.argv[len(s.argv)-1] != fixtureEndLSN {
				t.Fatal("setter resampled or replayed after unknown effect")
			}
		})
	}
	t.Run("byte_preserving_shim", testControlRecovery)
}

const controlRecoveryHarness = `import sys,os,stat,types,signal
from unittest.mock import patch
code,case=sys.argv[1:];mode='set_endpos' if case=='set_endpos' else 'progress'
raw=b"postgresql://private.invalid/db?password=secret-canary\n\xff"
files={'/tmp/pgm-source-uri':raw,'/tmp/pgm-target-uri':raw};offset={};opened=[];closed=[];executed=[]
env={b'PGCOPYDB_SOURCE_PGURI':b'opaque-source-env\n',b'PGCOPYDB_TARGET_PGURI':b'opaque-target-env\n'}
info=types.SimpleNamespace(st_dev=1,st_ino=2,st_mode=stat.S_IFREG|0o600,st_nlink=1,st_uid=65532,st_gid=65532,st_size=len(raw),st_mtime_ns=1,st_ctime_ns=1)
if case=='fifo':info.st_mode=stat.S_IFIFO|0o600
if case=='mode':info.st_mode=stat.S_IFREG|0o644
if case=='owner':info.st_uid=0
if case=='link':info.st_nlink=2
if case=='oversize':info.st_size=65537
if case=='empty':files={k:b'' for k in files}
if case=='nul':files={k:b'opaque\0' for k in files}
if case=='env_cap':env[b'OTHER']=b'x'*65536
def opening(path,flags):
 if flags!=os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW|os.O_CLOEXEC:raise RuntimeError()
 opened.append(path)
 if case=='fallback':raise FileNotFoundError()
 if case=='symlink':raise OSError()
 fd=len(opened)+10;offset[fd]=0;return fd
def reading(fd,size):
 at=offset[fd];value=files[opened[fd-11]][at:at+size];offset[fd]+=len(value)
 if case=='readback' and at==0 and fd in closed:return b'changed'
 return value
def seek(fd,at,whence):
 offset[fd]=0
 if case=='readback':closed.append(fd)
def st(path,follow_symlinks):
 if follow_symlinks is not False:raise RuntimeError()
 if case=='changed':return types.SimpleNamespace(**dict(vars(info),st_ino=3))
 return info
def close(fd):
 if case=='close':raise OSError()
 closed.append(fd)
def execute(path,args,values):executed.append((path,args,values))
def exiting(value):raise SystemExit(value)
sys.argv=['fixed',mode]
if mode=='set_endpos':sys.argv.append('0/20')
if case=='invalid_endpos':sys.argv=['fixed','set_endpos','--current']
if case=='unknown_mode':sys.argv=['fixed','unknown']
status=None
with patch.object(os,'environb',env),patch.object(os,'open',opening),patch.object(os,'fstat',lambda fd:info),patch.object(os,'stat',st),patch.object(os,'read',reading),patch.object(os,'lseek',seek),patch.object(os,'close',close),patch.object(os,'geteuid',lambda:65532),patch.object(os,'execve',execute),patch.object(os,'_exit',exiting),patch.object(signal,'setitimer',lambda *args:None):
 try:exec(code,{})
 except SystemExit as error:status=error.code
valid=case in ('progress','set_endpos','fallback')
if not valid:
 if status!=1 or executed:raise RuntimeError('invalid control was not refused')
else:
 if status is not None or len(executed)!=1:raise RuntimeError('control did not execute')
 path,args,values=executed[0]
 expected=['/usr/local/bin/rpgmigrate']+(['stream','sentinel','set','endpos','0/20'] if mode=='set_endpos' else ['list','progress'])+['--json','--dir','/work/pgcopydb']
 if path!='/usr/local/bin/rpgmigrate' or args!=expected:raise RuntimeError('command boundary changed')
 roles=('target',) if mode=='set_endpos' else ('source','target')
 if opened!=['/tmp/pgm-'+r+'-uri' for r in roles]:raise RuntimeError('unselected role consumed')
 for role in roles:
  key=('PGCOPYDB_'+role.upper()+'_PGURI').encode('ascii')
  if values[key]!=(env[key] if case=='fallback' else raw):raise RuntimeError('opaque bytes changed')
 if mode=='set_endpos' and b'PGCOPYDB_SOURCE_PGURI' in values:raise RuntimeError('source was retained')
print('fixed-control-check')
`

func testControlRecovery(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter absent")
	}
	for _, testCase := range []string{"progress", "set_endpos", "fallback", "symlink", "fifo", "mode", "owner", "link", "oversize", "changed", "readback", "close", fixtureEmptyCase, "nul", "env_cap", "invalid_endpos", "unknown_mode"} {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, python, "-I", "-B", "-c", controlRecoveryHarness, controlRecovery, testCase)
			output, err := command.CombinedOutput()
			if err != nil || string(output) != "fixed-control-check\n" {
				t.Fatal("fixed recovery shim failed its opaque-byte/refusal check")
			}
		})
	}
}

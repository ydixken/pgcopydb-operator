package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

const nativeDeliveryPhase = "delivery"
const nativeDeliverySelector = "RPGMIGRATE_NATIVE_DELIVERY_INPUT"
const nativeDeliveryPVCKind = "PersistentVolumeClaim"
const nativeDeliveryClaim = "delivery-work"
const (
	nativeDeliveryQuiet      = "quiet_success"
	nativeDeliveryConcurrent = "concurrent_relay_refused"
	nativeDeliveryForeign    = "foreign_lock_refused"
	nativeDeliveryTruncated  = "truncated_result_refused"
	nativeDeliveryStderr     = "stderr_refused"
	nativeDeliveryStartup    = "startup_log_refused"
	nativeDeliveryMainFail   = "main_failure_refused"
	nativeDeliveryDisconnect = "disconnect_no_replay"
	nativeDeliveryPVCCase    = "pvc/fsgroup_readonly_remount"
	nativeDeliveryStderrMode = "stderr"
)

// The main uses the canonical InputChannel; its envelope is synthetic and never reaches a Reader.
const nativeDeliveryProgram = `import hashlib,json,os,sys,time,types
SOURCE=sys.argv[1];mode,op,sha=sys.argv[2:5]
if hashlib.sha256(SOURCE.encode()).hexdigest()!=sha:raise SystemExit(2)
P=types.ModuleType('native_delivery_canonical')
exec(compile(SOURCE,'<canonical-native-evidence>','exec'),P.__dict__)
start=time.monotonic();channel=None;code=1
try:
 P.need(mode in ('ok','slow','stderr','truncated','startup_log','main_failure') and os.geteuid()==65532)
 P.validate_environment(os.environ)
 if mode=='startup_log':os.write(1,b'startup\n')
 channel=P.InputChannel(op,start+30);channel.route();raw=channel.consume()
 req=P.validate_request(raw,op,sha);deadline=time.monotonic()+25
 if mode=='slow':time.sleep(6)
 if mode=='stderr':os.write(2,b'x')
 request_sha,execution_sha=P.request_digests(raw)
 result=dict(version=1,kind='completed_evidence',operation_id=op,request_sha256=request_sha,program_sha256=sha,execution_sha256=execution_sha,work=req['execution']['work'],checkpoint=dict(checkpoint_id='delivery-qualification-synthetic',certified_lsn='0/1',durable_replay_lsn='0/1',drain_end_lsn='0/1',drain_replay_lsn='0/1',drain_kind='keepalive',barrier_confirmed=True),resources=[],guards=P.GUARDS,source_closed=True,target_closed=True,private_closed=True,diagnostics_clean=True)
 encode=lambda:json.dumps(result,separators=(',',':'),ensure_ascii=True).encode()+b'\n'
 while len(encode())<60000:result['resources'].append(dict(kind='Delivery',name='pad-%05d'%len(result['resources']),owned=True,retain=False))
 output=encode();P.need(len(output)<=P.CAP)
 if mode=='truncated':output=output[:len(output)//2]
 channel.finish(output,deadline);channel=None
 code=3 if mode=='main_failure' else 0
except BaseException:
 if channel is not None:
  try:channel.finish(json.dumps(P.failure_envelope(op,P.Refusal('internal')),separators=(',',':')).encode()+b'\n')
  except BaseException:pass
raise SystemExit(code)
`

// The holder takes the operation directory lease before any relay can.
const nativeDeliveryHolder = `import fcntl,os,sys,time
end=time.monotonic()+5
while True:
 try:d=os.open('/tmp/rpgmigrate-evidence-'+sys.argv[1],os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW);break
 except FileNotFoundError:
  if time.monotonic()>end:raise SystemExit(1)
  time.sleep(.02)
fcntl.flock(d,fcntl.LOCK_EX|fcntl.LOCK_NB);time.sleep(10);print('held')
`

const nativeDeliveryPVCProgram = `import errno,json,os,stat,sys
mode=sys.argv[1]
def mount(path):
 rows=[l.split() for l in open('/proc/self/mountinfo').read(65537).splitlines()]
 found=[r for r in rows if r[4]==path];assert len(found)==1
 r=found[0];return r[5].split(','),r[r.index('-')+1],r[r.index('-')+3].split(',')
assert os.geteuid()==65532 and os.getegid()==65532 and 65532 in os.getgroups()
assert os.statvfs('/').f_flag&os.ST_RDONLY
tmp_flags,tmp_type,tmp_super=mount('/tmp');work_flags,_,_=mount('/work')
assert tmp_type=='tmpfs' and 'size=65536k' in tmp_super and 'rw' in tmp_flags
work=os.stat('/work');marker='/work/delivery-marker'
if mode=='write':
 assert 'rw' in work_flags
 fd=os.open(marker,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o640);os.write(fd,b'delivery-pvc\n');os.fsync(fd);os.close(fd)
else:
 assert 'ro' in work_flags
 assert open(marker,'rb').read()==b'delivery-pvc\n'
 try:os.open('/work/delivery-write',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);refused=False
 except OSError as error:refused=error.errno==errno.EROFS
 assert refused
info=os.lstat(marker);assert stat.S_ISREG(info.st_mode) and info.st_uid==65532 and info.st_gid==65532 and stat.S_IMODE(info.st_mode)==0o640 and info.st_nlink==1
print(json.dumps(dict(kind='pvc_'+mode,work_uid=work.st_uid,work_gid=work.st_gid,work_mode=stat.S_IMODE(work.st_mode),marker_mode=stat.S_IMODE(info.st_mode),tmp_type=tmp_type,work_readonly='ro' in work_flags),sort_keys=True))
`

type nativeDeliveryInput struct {
	SchemaVersion  int                `json:"schema_version"`
	Runtime        nativeRuntimeInput `json:"runtime"`
	ProgramSHA     string             `json:"delivery_program_sha256"`
	ParentDeadline float64            `json:"original_parent_deadline"`
}

type nativeDeliveryObserved struct {
	Relay        bool   `json:"relay_successful"`
	SecondRelay  *bool  `json:"second_relay_successful"`
	Completed    *bool  `json:"job_completed_native"`
	Composite    bool   `json:"composite_authority"`
	PayloadBytes int    `json:"payload_bytes"`
	MainExit     *int32 `json:"main_exit_code"`
}

type nativeDeliveryCase struct {
	Name     string                 `json:"name"`
	Status   string                 `json:"status"`
	ExitCode int                    `json:"exit_code"`
	Cleanup  bool                   `json:"cleanup"`
	Observed nativeDeliveryObserved `json:"observed"`
}

type nativeDeliveryReceipt struct {
	SchemaVersion   int                  `json:"schema_version"`
	Kind            string               `json:"kind"`
	QualificationID string               `json:"qualification_id"`
	RunID           string               `json:"run_id"`
	InputSHA        string               `json:"input_sha256"`
	SourceSHA       string               `json:"qualification_source_sha256"`
	EvidenceSHA     string               `json:"evidence_program_sha256"`
	AssetsSHA       string               `json:"native_assets_sha256"`
	ProgramSHA      string               `json:"delivery_program_sha256"`
	RelaySHA        string               `json:"relay_program_sha256"`
	NamespaceUID    types.UID            `json:"namespace_uid"`
	StartedAt       float64              `json:"started_at"`
	FinishedAt      float64              `json:"finished_at"`
	Cases           []nativeDeliveryCase `json:"cases"`
	Cleanup         struct {
		Complete bool                    `json:"complete"`
		Objects  []nativePasswordCleanup `json:"objects"`
	} `json:"cleanup"`
}

var nativeDeliveryOrder = []string{nativeDeliveryQuiet, nativeDeliveryConcurrent, nativeDeliveryForeign, nativeDeliveryTruncated,
	nativeDeliveryStderr, nativeDeliveryStartup, nativeDeliveryMainFail, nativeDeliveryDisconnect}

// nativeDeliveryModes pairs each case with one fixed main mode.
var nativeDeliveryModes = map[string]string{nativeDeliveryQuiet: "ok", nativeDeliveryConcurrent: "slow", nativeDeliveryForeign: "ok",
	nativeDeliveryTruncated: "truncated", nativeDeliveryStderr: nativeDeliveryStderrMode, nativeDeliveryStartup: "startup_log",
	nativeDeliveryMainFail: "main_failure", nativeDeliveryDisconnect: "slow"}

func nativeDeliveryCases() []string {
	cases := make([]string, 0, len(nativeDeliveryOrder)+1)
	for _, name := range nativeDeliveryOrder {
		cases = append(cases, nativeDeliveryPhase+"/"+name)
	}
	return append(cases, nativeDeliveryPVCCase)
}

func nativeDeliveryValidateInput(input nativeDeliveryInput, path string, now time.Time) error {
	r := input.Runtime
	current := float64(now.UnixNano()) / 1e9
	evidence := native.EvidenceProgramSHA256()
	if input.SchemaVersion != 1 || r.SchemaVersion != 1 || r.Phase != nativeDeliveryPhase || r.Pair != nil || r.NamespaceUID != nil || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(r.QualificationID) || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(r.RunID) || r.Namespace != "native-delivery-"+r.QualificationID[:16] || r.ReceiptPath != filepath.Join(filepath.Dir(path), "delivery-receipt.json") || r.Kubeconfig != filepath.Join(r.RunDir, "kubeconfig") || r.Context != "kind-rpgmigrate-m7-"+r.RunID || !slices.Equal(r.RequiredCases, nativeDeliveryCases()) || len(r.Nodes) != 3 {
		return errors.New("fixed delivery input refused")
	}
	for _, digest := range []string{r.LegacySourceSHA, r.LegacyGuardSHA, r.LegacyOwnershipSHA, r.QualificationSourceSHA, r.AssetsSHA, r.MetadataProgramSHA} {
		if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(digest) {
			return errors.New("delivery source binding absent")
		}
	}
	if r.EvidenceProgramSHA != hex.EncodeToString(evidence[:]) || input.ProgramSHA != nativeRuntimeSHA([]byte(nativeDeliveryProgram)) || r.MetadataProgramSHA != nativeRuntimeSHA([]byte(nativeRuntimeMetadataProgram+"\x00"+nativeRuntimeNodeProgram)) || !(current < r.StageDeadline && r.StageDeadline <= current+600 && r.StageDeadline+600 <= min(r.LeaseExpiry, input.ParentDeadline)) {
		return errors.New("delivery original source or deadline changed")
	}
	previous := ""
	for _, node := range r.Nodes {
		if node.Name <= previous || node.UID == "" || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(node.ContainerID) {
			return errors.New("delivery node identity refused")
		}
		previous = node.Name
	}
	return nil
}

func nativeDeliveryCommand(program string, args ...string) ([]string, error) {
	command := append([]string{nativeRuntimeEnvBinary, "-u", nativeRuntimePreload, "-u", nativeRuntimeLibraryPath, nativeRuntimePython, "-I", "-B", "-c", program}, args...)
	for _, value := range command {
		if len(value)+1 > native.EvidenceMainArgumentLimit || strings.IndexByte(value, 0) >= 0 {
			return nil, errors.New("delivery argument cap")
		}
	}
	return command, nil
}

func nativeDeliveryJob(input nativeRuntimeInput, node nativeRuntimeNode, image nativeRuntimeImage, name string, command []string) (*batchv1.Job, error) {
	job, err := nativeRuntimeJob(input, node, image, 0)
	if err != nil {
		return nil, err
	}
	job.Name = name
	container := &job.Spec.Template.Spec.Containers[0]
	container.Command = command
	container.Env = []corev1.EnvVar{{Name: nativeLCAllEnv, Value: "C"}, {Name: nativeLangEnv, Value: "C"}}
	return job, nil
}

func nativeDeliveryCreateJob(ctx context.Context, clientset kubernetes.Interface, job *batchv1.Job, owned *[]nativeRuntimeOwned, unknown *bool) (*batchv1.Job, *corev1.Pod, error) {
	created, err := clientset.BatchV1().Jobs(job.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil || created.UID == "" {
		*unknown = true
		return nil, nil, errors.New("delivery Job Create unknown")
	}
	*owned = append(*owned, nativeRuntimeOwned{object: created, kind: lifecycleJobKind})
	var pod *corev1.Pod
	err = wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(call context.Context) (bool, error) {
		pods, err := clientset.CoreV1().Pods(job.Namespace).List(call, metav1.ListOptions{LabelSelector: batchv1.ControllerUidLabel + "=" + string(created.UID)})
		if err != nil || len(pods.Items) > 1 {
			return false, errors.New("delivery Pod inventory refused")
		}
		if len(pods.Items) == 0 {
			return false, nil
		}
		if pod == nil {
			pod = pods.Items[0].DeepCopy()
			*owned = append(*owned, nativeRuntimeOwned{object: pod, kind: nativeRuntimePodKind})
		}
		if pods.Items[0].UID != pod.UID {
			return false, errors.New("delivery Pod replaced")
		}
		statuses := pods.Items[0].Status.ContainerStatuses
		if len(statuses) == 1 && (statuses[0].State.Running != nil || statuses[0].State.Terminated != nil) && statuses[0].ContainerID != "" {
			pod = pods.Items[0].DeepCopy()
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, nil, errors.New("delivery Pod start refused")
	}
	return created, pod, nil
}

func nativeDeliveryTerminal(ctx context.Context, clientset kubernetes.Interface, pod *corev1.Pod) (*int32, error) {
	var code *int32
	err := wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(call context.Context) (bool, error) {
		live, err := clientset.CoreV1().Pods(pod.Namespace).Get(call, pod.Name, metav1.GetOptions{})
		if err != nil || live.UID != pod.UID || len(live.Status.ContainerStatuses) != 1 {
			return false, errors.New("delivery Pod identity changed")
		}
		if terminated := live.Status.ContainerStatuses[0].State.Terminated; terminated != nil {
			value := terminated.ExitCode
			code = &value
			return true, nil
		}
		return false, nil
	})
	return code, err
}

type nativeDeliveryWork struct {
	MigrationUID string `json:"migration_uid"`
	RunID        string `json:"run_id"`
	WorkdirID    string `json:"workdir_id"`
	Generation   int    `json:"checkpoint_generation"`
}

type nativeDeliveryExecution struct {
	Namespace           string             `json:"namespace"`
	MigrationName       string             `json:"migration_name"`
	PVCName             string             `json:"pvc_name"`
	JobName             string             `json:"job_name"`
	MigrationUID        string             `json:"migration_uid"`
	PVCUID              string             `json:"pvc_uid"`
	JobUID              string             `json:"job_uid"`
	WorkerJobUID        string             `json:"worker_job_uid"`
	MigrationGeneration int                `json:"migration_generation"`
	Attempt             int                `json:"attempt"`
	Role                string             `json:"role"`
	Work                nativeDeliveryWork `json:"work"`
	ImageRef            string             `json:"image_ref"`
	PlatformImageID     string             `json:"platform_image_id"`
	TemplateSHA256      string             `json:"template_sha256"`
	ConnectionSHA256    string             `json:"connection_reference_sha256"`
}

type nativeDeliveryContainer struct {
	PodUID         string `json:"pod_uid"`
	ContainerName  string `json:"container_name"`
	ContainerID    string `json:"container_id"`
	RestartCount   int32  `json:"restart_count"`
	ImageID        string `json:"image_id"`
	TemplateSHA256 string `json:"template_sha256"`
}

type nativeDeliveryRequest struct {
	Version       int                     `json:"version"`
	Kind          string                  `json:"kind"`
	OperationID   string                  `json:"operation_id"`
	ProgramSHA256 string                  `json:"program_sha256"`
	Execution     nativeDeliveryExecution `json:"execution"`
	PodName       string                  `json:"pod_name"`
	Container     nativeDeliveryContainer `json:"container"`
	SavedE        string                  `json:"saved_e"`
}

type nativeDeliveryResult struct {
	Kind          string `json:"kind"`
	OperationID   string `json:"operation_id"`
	RequestSHA256 string `json:"request_sha256"`
}

type nativeDeliveryPVCResult struct {
	Kind         string `json:"kind"`
	TmpType      string `json:"tmp_type"`
	WorkReadonly bool   `json:"work_readonly"`
	MarkerMode   int    `json:"marker_mode"`
	WorkUID      int    `json:"work_uid"`
	WorkGID      int    `json:"work_gid"`
	WorkMode     int    `json:"work_mode"`
}

func nativeDeliveryBody(namespace string, job *batchv1.Job, pod *corev1.Pod, binding podexec.NativeContainerBinding, imageRef, operation, programSHA string) ([]byte, error) {
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return nil, err
	}
	uid := func(raw []byte) string {
		text := hex.EncodeToString(raw)
		return text[:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:32]
	}
	migration, pvc := uid(random[:16]), uid(random[16:])
	template := hex.EncodeToString(binding.TemplateSHA256[:])
	body := nativeDeliveryRequest{Version: 1, Kind: finishOrdinaryEvidenceKind, OperationID: operation, ProgramSHA256: programSHA,
		Execution: nativeDeliveryExecution{Namespace: namespace, MigrationName: "delivery-qualification", PVCName: "delivery-qualification-work", JobName: job.Name,
			MigrationUID: migration, PVCUID: pvc, JobUID: string(job.UID), WorkerJobUID: string(job.UID), MigrationGeneration: 1, Role: string(native.JobRoleEvidence),
			Work:     nativeDeliveryWork{MigrationUID: migration, RunID: strings.ReplaceAll(migration, "-", ""), WorkdirID: pvc, Generation: 1},
			ImageRef: imageRef, PlatformImageID: binding.ImageID, TemplateSHA256: template, ConnectionSHA256: strings.Repeat("0", 64)},
		PodName: pod.Name, Container: nativeDeliveryContainer{PodUID: string(pod.UID), ContainerName: binding.ContainerName, ContainerID: binding.ContainerID,
			RestartCount: binding.RestartCount, ImageID: binding.ImageID, TemplateSHA256: template}, SavedE: "0/16B6C50"}
	return json.Marshal(body)
}

type nativeDeliveryRun struct {
	clientset kubernetes.Interface
	exec      *podexec.Exec
	input     nativeRuntimeInput
	image     nativeRuntimeImage
	canonical string
	owned     *[]nativeRuntimeOwned
	unknown   *bool
}

func (d nativeDeliveryRun) relay(ctx context.Context, pod *corev1.Pod, binding podexec.NativeContainerBinding, operation string, body []byte) (podexec.NativeOutcome, error) {
	argv := []string{"/usr/bin/env", "-u", "LD_PRELOAD", "-u", nativeRuntimeLibraryPath, nativeRuntimePython, "-I", "-B", "-c", string(native.EvidenceRequestWriterProgram()), operation}
	relayCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return d.exec.InPodNative(relayCtx, pod.Namespace, pod.Name, binding, argv, body)
}

// runCase follows the Reader order; composite authority needs both checked results.
func (d nativeDeliveryRun) runCase(ctx context.Context, index int, name string) (nativeDeliveryObserved, error) {
	observed := nativeDeliveryObserved{}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return observed, err
	}
	operation := hex.EncodeToString(random)
	evidence := native.EvidenceProgramSHA256()
	programSHA := hex.EncodeToString(evidence[:])
	command, err := nativeDeliveryCommand(nativeDeliveryProgram, d.canonical, nativeDeliveryModes[name], operation, programSHA)
	if err != nil {
		return observed, err
	}
	node := d.input.Nodes[index%len(d.input.Nodes)]
	desired, err := nativeDeliveryJob(d.input, node, d.image, fmt.Sprintf("delivery-%d", index), command)
	if err != nil {
		return observed, err
	}
	job, pod, err := nativeDeliveryCreateJob(ctx, d.clientset, desired, d.owned, d.unknown)
	if err != nil {
		return observed, err
	}
	status := pod.Status.ContainerStatuses[0]
	if !nativeRuntimeImageID(status.ImageID, d.image, node.Name) {
		return observed, errors.New("delivery image identity refused")
	}
	binding := podexec.NativeContainerBinding{PodUID: pod.UID, ContainerName: status.Name, ContainerID: status.ContainerID, RestartCount: status.RestartCount, ImageID: status.ImageID, TemplateSHA256: podexec.NativeTemplateSHA256(job.Spec.Template)}
	body, err := nativeDeliveryBody(pod.Namespace, job, pod, binding, d.image.Ref, operation, programSHA)
	if err != nil {
		return observed, err
	}
	var outcome podexec.NativeOutcome
	var relayErr error
	switch name {
	case nativeDeliveryConcurrent:
		first := make(chan error, 1)
		go func() {
			var err error
			outcome, err = d.relay(ctx, pod, binding, operation, body)
			first <- err
		}()
		time.Sleep(2 * time.Second)
		second, secondErr := d.relay(ctx, pod, binding, operation, body)
		value := secondErr == nil && second.Successful()
		observed.SecondRelay = &value
		relayErr = <-first
	case nativeDeliveryForeign:
		holderCommand, err := nativeDeliveryCommand(nativeDeliveryHolder, operation)
		if err != nil {
			return observed, err
		}
		held := make(chan error, 1)
		go func() {
			holderCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			_, err := d.exec.InPodNative(holderCtx, pod.Namespace, pod.Name, binding, holderCommand, nil)
			held <- err
		}()
		time.Sleep(time.Second)
		outcome, relayErr = d.relay(ctx, pod, binding, operation, body)
		if <-held != nil {
			return observed, errors.New("delivery foreign lock holder did not hold")
		}
	case nativeDeliveryDisconnect:
		// The replay runs while main still holds the consumed operation, so only the relay can refuse it.
		cancelled, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		outcome, relayErr = d.relay(cancelled, pod, binding, operation, body)
		cancel()
		replay, replayErr := d.relay(ctx, pod, binding, operation, body)
		value := replayErr == nil && replay.Successful()
		observed.SecondRelay = &value
	default:
		outcome, relayErr = d.relay(ctx, pod, binding, operation, body)
	}
	observed.Relay = relayErr == nil && outcome.Successful()
	if observed.Relay {
		payload := outcome.Payload()
		observed.PayloadBytes = len(payload)
		var result nativeDeliveryResult
		if json.Unmarshal(payload, &result) != nil || result.Kind != finishOrdinaryEvidenceKind || result.OperationID != operation || result.RequestSHA256 != nativeRuntimeSHA(body) {
			return observed, errors.New("delivery relay payload binding refused")
		}
	}
	terminalCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
	observed.MainExit, err = nativeDeliveryTerminal(terminalCtx, d.clientset, pod)
	cancel()
	if err != nil {
		return observed, errors.New("delivery main did not terminate")
	}
	completed, completedErr := d.exec.JobCompletedNative(ctx, pod.Namespace, job.UID, binding)
	value := completedErr == nil && completed.Successful() && completed.Payload() == nil
	observed.Completed = &value
	observed.Composite = observed.Relay && value
	return observed, nil
}

func nativeDeliveryExit(o nativeDeliveryObserved, code int32) bool {
	return o.MainExit != nil && *o.MainExit == code
}

func nativeDeliveryIs(value *bool, want bool) bool { return value != nil && *value == want }

// nativeDeliveryExpectations mirror the Python receipt check; a false authority anywhere fails.
var nativeDeliveryExpectations = map[string]func(nativeDeliveryObserved) bool{
	nativeDeliveryQuiet: func(o nativeDeliveryObserved) bool {
		return o.Relay && nativeDeliveryIs(o.Completed, true) && o.PayloadBytes >= 60000 && nativeDeliveryExit(o, 0) && o.SecondRelay == nil
	},
	nativeDeliveryConcurrent: func(o nativeDeliveryObserved) bool {
		return o.Relay && nativeDeliveryIs(o.SecondRelay, false) && nativeDeliveryIs(o.Completed, true) && nativeDeliveryExit(o, 0)
	},
	nativeDeliveryForeign: func(o nativeDeliveryObserved) bool {
		return !o.Relay && nativeDeliveryIs(o.Completed, false) && o.MainExit != nil && *o.MainExit != 0
	},
	nativeDeliveryTruncated: func(o nativeDeliveryObserved) bool {
		return !o.Relay && nativeDeliveryIs(o.Completed, true) && nativeDeliveryExit(o, 0)
	},
	nativeDeliveryStderr: func(o nativeDeliveryObserved) bool { return !o.Relay && o.Completed != nil && o.MainExit != nil },
	nativeDeliveryStartup: func(o nativeDeliveryObserved) bool {
		return o.Relay && nativeDeliveryIs(o.Completed, false) && nativeDeliveryExit(o, 0)
	},
	nativeDeliveryMainFail: func(o nativeDeliveryObserved) bool {
		return o.Relay && nativeDeliveryIs(o.Completed, false) && nativeDeliveryExit(o, 3)
	},
	nativeDeliveryDisconnect: func(o nativeDeliveryObserved) bool {
		return !o.Relay && nativeDeliveryIs(o.SecondRelay, false) && o.Completed != nil && o.MainExit != nil
	},
}

func nativeDeliveryExpected(name string, o nativeDeliveryObserved) bool {
	expected, known := nativeDeliveryExpectations[name]
	return known && o.Composite == (o.Relay && nativeDeliveryIs(o.Completed, true)) && expected(o)
}

func (d nativeDeliveryRun) pvc(ctx context.Context) (bool, error) {
	quantity := resource.MustParse("64Mi")
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: nativeDeliveryClaim, Namespace: d.input.Namespace, Labels: map[string]string{nativeRuntimeRunLabel: d.input.RunID}},
		Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: quantity}}}}
	created, err := d.clientset.CoreV1().PersistentVolumeClaims(d.input.Namespace).Create(ctx, claim, metav1.CreateOptions{})
	if err != nil || created.UID == "" {
		*d.unknown = true
		return false, errors.New("delivery PVC Create unknown")
	}
	*d.owned = append(*d.owned, nativeRuntimeOwned{object: created, kind: nativeDeliveryPVCKind})
	for index, mode := range []string{"write", "read"} {
		command, err := nativeDeliveryCommand(nativeDeliveryPVCProgram, mode)
		if err != nil {
			return false, err
		}
		desired, err := nativeDeliveryJob(d.input, d.input.Nodes[0], d.image, "pvc-"+mode, command)
		if err != nil {
			return false, err
		}
		desired.Spec.Template.Spec.NodeName = ""
		readOnly := index == 1
		desired.Spec.Template.Spec.Volumes = append(desired.Spec.Template.Spec.Volumes, corev1.Volume{Name: nativeControlWorkKey, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: nativeDeliveryClaim, ReadOnly: readOnly}}})
		container := &desired.Spec.Template.Spec.Containers[0]
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: nativeControlWorkKey, MountPath: "/work", ReadOnly: readOnly})
		_, pod, err := nativeDeliveryCreateJob(ctx, d.clientset, desired, d.owned, d.unknown)
		if err != nil {
			return false, err
		}
		terminalCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
		code, err := nativeDeliveryTerminal(terminalCtx, d.clientset, pod)
		cancel()
		if err != nil || code == nil || *code != 0 {
			return false, errors.New("delivery PVC helper failed")
		}
		stream, err := d.clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: pod.Spec.Containers[0].Name}).Stream(ctx)
		if err != nil {
			return false, errors.New("delivery PVC log unavailable")
		}
		raw, readErr := io.ReadAll(io.LimitReader(stream, 4097))
		closeErr := stream.Close()
		var result nativeDeliveryPVCResult
		if readErr != nil || closeErr != nil || len(raw) > 4096 || json.Unmarshal(raw, &result) != nil || result.Kind != "pvc_"+mode || result.TmpType != "tmpfs" || result.WorkReadonly != readOnly || result.MarkerMode != 0o640 {
			return false, errors.New("delivery PVC observation refused")
		}
	}
	return true, nil
}

func nativeDeliveryRemove(ctx context.Context, clientset kubernetes.Interface, owned nativeRuntimeOwned) nativePasswordCleanup {
	if owned.kind != nativeDeliveryPVCKind {
		return nativePasswordRemove(ctx, clientset, owned)
	}
	o := owned.object
	result := nativePasswordCleanup{APIVersion: "v1", Kind: owned.kind, Namespace: o.GetNamespace(), Name: o.GetName(), UID: o.GetUID(), ResourceVersion: o.GetResourceVersion(), Action: nativeRuntimeRetained}
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(call context.Context) (bool, error) {
		live, err := clientset.CoreV1().PersistentVolumeClaims(o.GetNamespace()).Get(call, o.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			result.VerifiedAbsent = true
			return true, nil
		}
		if err != nil || live.UID != o.GetUID() || live.ResourceVersion == "" {
			return false, errors.New("cleanup identity changed")
		}
		uid, version := live.UID, live.ResourceVersion
		result.ResourceVersion = version
		foreground := metav1.DeletePropagationForeground
		err = clientset.CoreV1().PersistentVolumeClaims(o.GetNamespace()).Delete(call, o.GetName(), metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}, PropagationPolicy: &foreground})
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return false, errors.New("cleanup delete refused")
		}
		return false, nil
	})
	if err == nil && result.VerifiedAbsent {
		result.Action = nativeRuntimeDeleted
	}
	return result
}

// nativeDeliveryFinish keeps Pods before Jobs, and the namespace last, under UID/RV preconditions.
func nativeDeliveryFinish(ctx context.Context, clientset kubernetes.Interface, owned []nativeRuntimeOwned, unknown bool, receipt *nativeDeliveryReceipt) {
	blocked := unknown
	for _, kind := range []string{nativeRuntimePodKind, lifecycleJobKind, nativeDeliveryPVCKind, nativeRuntimeNamespaceKind} {
		for _, object := range owned {
			if object.kind != kind {
				continue
			}
			if kind == lifecycleJobKind && !nativeRuntimeChildrenAbsent(ctx, clientset, object) {
				blocked = true
			}
			if blocked && kind != nativeRuntimePodKind {
				receipt.Cleanup.Objects = append(receipt.Cleanup.Objects, nativePasswordCleanup{APIVersion: map[bool]string{true: probeBatchAPI, false: "v1"}[kind == lifecycleJobKind], Kind: kind, Namespace: object.object.GetNamespace(), Name: object.object.GetName(), UID: object.object.GetUID(), ResourceVersion: object.object.GetResourceVersion(), Action: nativeRuntimeRetained})
				continue
			}
			result := nativeDeliveryRemove(ctx, clientset, object)
			receipt.Cleanup.Objects = append(receipt.Cleanup.Objects, result)
			if !result.VerifiedAbsent {
				blocked = true
			}
		}
	}
	// Eight relay Jobs and two PVC Jobs, each with one Pod, plus the claim and the namespace.
	receipt.Cleanup.Complete = !blocked && len(owned) == 22 && len(receipt.Cleanup.Objects) == 22
	for index := range receipt.Cases {
		receipt.Cases[index].Cleanup = receipt.Cleanup.Complete
	}
	receipt.FinishedAt = float64(time.Now().UnixNano()) / 1e9
}

func TestNativePackagedDeliveryQualification(t *testing.T) {
	path, selected := os.LookupEnv(nativeDeliverySelector)
	if !selected {
		t.Skip("delivery qualification is not selected")
	}
	raw, err := nativeRuntimePrivate(path)
	if err != nil {
		t.Fatal("private delivery input unavailable")
	}
	var input nativeDeliveryInput
	var fields map[string]json.RawMessage
	if nativeRuntimeDecode(raw, &input) != nil || json.Unmarshal(raw, &fields) != nil || nativeRuntimeDecode(fields["runtime"], &input.Runtime) != nil || nativeDeliveryValidateInput(input, path, time.Now()) != nil {
		t.Fatal("fixed delivery input refused")
	}
	r := input.Runtime
	config, err := nativeRuntimeConfig(r)
	if err != nil {
		t.Fatal("owned client unavailable")
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal("owned client unavailable")
	}
	exec, err := podexec.New(config)
	if err != nil {
		t.Fatal("checked transport unavailable")
	}
	image, err := nativeRuntimeAssets(path, r)
	if err != nil {
		t.Fatal("native image source unavailable")
	}
	workDeadline := time.Unix(0, int64(r.StageDeadline*1e9))
	cleanupDeadline := time.Unix(0, int64(min(r.StageDeadline+600, r.LeaseExpiry, input.ParentDeadline)*1e9))
	if parent, ok := t.Deadline(); ok && parent.Before(cleanupDeadline.Add(time.Second)) {
		cleanupDeadline = parent.Add(-time.Second)
		workDeadline = minTime(workDeadline, cleanupDeadline.Add(-600*time.Second))
	}
	if !time.Now().Before(workDeadline) {
		t.Fatal("original cleanup horizon unavailable")
	}
	work, cancel := context.WithDeadline(context.Background(), workDeadline)
	defer cancel()
	owned := make([]nativeRuntimeOwned, 0, 22)
	unknown := false
	relay := native.EvidenceRequestWriterSHA256()
	receipt := nativeDeliveryReceipt{SchemaVersion: 1, Kind: nativeDeliveryPhase, QualificationID: r.QualificationID, RunID: r.RunID, InputSHA: nativeRuntimeSHA(raw), SourceSHA: r.QualificationSourceSHA, EvidenceSHA: r.EvidenceProgramSHA, AssetsSHA: r.AssetsSHA, ProgramSHA: input.ProgramSHA, RelaySHA: hex.EncodeToString(relay[:]), StartedAt: float64(time.Now().UnixNano()) / 1e9, Cases: []nativeDeliveryCase{}}
	defer func() {
		cleanup, end := context.WithDeadline(context.Background(), cleanupDeadline)
		defer end()
		nativeDeliveryFinish(cleanup, clientset, owned, unknown, &receipt)
		if nativeRuntimeWrite(r.ReceiptPath, receipt) != nil {
			t.Error("delivery receipt persistence failed")
		}
		if !receipt.Cleanup.Complete {
			t.Error("exact delivery cleanup incomplete")
		}
	}()
	if _, err := clientset.CoreV1().Namespaces().Get(work, r.Namespace, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("fresh namespace absence required")
	}
	namespace, err := clientset.CoreV1().Namespaces().Create(work, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.Namespace, Labels: map[string]string{nativeRuntimeRunLabel: r.RunID, nativePasswordQualificationLabel: r.QualificationID}}}, metav1.CreateOptions{})
	if err != nil || namespace.UID == "" {
		unknown = true
		t.Fatal("namespace Create unknown")
	}
	owned = append(owned, nativeRuntimeOwned{object: namespace, kind: nativeRuntimeNamespaceKind})
	receipt.NamespaceUID = namespace.UID
	run := nativeDeliveryRun{clientset: clientset, exec: exec, input: r, image: image, canonical: string(native.EvidenceProgram()), owned: &owned, unknown: &unknown}
	for index, caseName := range nativeDeliveryCases() {
		name := strings.Split(caseName, "/")[1]
		var observed nativeDeliveryObserved
		var passed bool
		if caseName == nativeDeliveryPVCCase {
			passed, err = run.pvc(work)
		} else {
			observed, err = run.runCase(work, index, name)
			passed = err == nil && nativeDeliveryExpected(name, observed)
		}
		ok := t.Run(caseName, func(t *testing.T) {
			if !passed {
				t.Fatalf("delivery case refused: %v", err)
			}
		})
		status, exit := nativeRuntimeRefused, 1
		if ok {
			status, exit = nativeRuntimePassed, 0
		}
		receipt.Cases = append(receipt.Cases, nativeDeliveryCase{Name: caseName, Status: status, ExitCode: exit, Observed: observed})
	}
}

func TestNativeDeliveryProgramBounds(t *testing.T) {
	canonical := string(native.EvidenceProgram())
	for _, args := range [][]string{{nativeDeliveryProgram, canonical, "ok", strings.Repeat("a", 32), strings.Repeat("b", 64)}, {nativeDeliveryHolder, strings.Repeat("a", 32)}, {nativeDeliveryPVCProgram, "write"}} {
		if _, err := nativeDeliveryCommand(args[0], args[1:]...); err != nil {
			t.Fatal("delivery program exceeds the main argument cap")
		}
	}
	if _, err := nativeDeliveryCommand(strings.Repeat("x", native.EvidenceMainArgumentLimit)); err == nil {
		t.Fatal("delivery argument cap was not enforced")
	}
	if len(nativeDeliveryCases()) != len(nativeDeliveryModes)+1 || len(nativeDeliveryExpectations) != len(nativeDeliveryModes) {
		t.Fatal("delivery case inventory and main modes differ")
	}
}

func TestNativeDeliveryExpectedRefusesFalseAuthority(t *testing.T) {
	yes, no, zero, three := true, false, int32(0), int32(3)
	good := map[string]nativeDeliveryObserved{
		nativeDeliveryQuiet:      {Relay: true, Completed: &yes, Composite: true, PayloadBytes: 60000, MainExit: &zero},
		nativeDeliveryConcurrent: {Relay: true, SecondRelay: &no, Completed: &yes, Composite: true, MainExit: &zero},
		nativeDeliveryForeign:    {Completed: &no, MainExit: &three},
		nativeDeliveryTruncated:  {Completed: &yes, MainExit: &zero},
		nativeDeliveryStderr:     {Completed: &no, MainExit: &zero},
		nativeDeliveryStartup:    {Relay: true, Completed: &no, MainExit: &zero},
		nativeDeliveryMainFail:   {Relay: true, Completed: &no, MainExit: &three},
		nativeDeliveryDisconnect: {SecondRelay: &no, Completed: &yes, MainExit: &zero},
	}
	for name, observed := range good {
		if !nativeDeliveryExpected(name, observed) {
			t.Fatalf("%s expected observation refused", name)
		}
		forged := observed
		forged.Composite = !observed.Composite
		if nativeDeliveryExpected(name, forged) {
			t.Fatalf("%s accepted a flipped composite authority", name)
		}
	}
	replayed := good[nativeDeliveryDisconnect]
	replayed.SecondRelay = &yes
	if nativeDeliveryExpected(nativeDeliveryDisconnect, replayed) {
		t.Fatal("a replayed relay was accepted")
	}
	if nativeDeliveryExpected("unknown", good[nativeDeliveryQuiet]) {
		t.Fatal("an unknown case was accepted")
	}
}

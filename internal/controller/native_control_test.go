package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

const (
	nativeControlZeroEnd                    = "0/0"
	nativeControlInvalidEnd                 = "invalid"
	nativeControlDisconnectFault            = "disconnect"
	nativeControlRefusalFault               = "refusal"
	nativeControlWarningFault               = "warning"
	nativeControlRestartCase                = "restart"
	nativeControlConflictCase               = "conflict winner"
	nativeControlLostPatchCase              = "patch lost reply"
	nativeControlSourceCrashCase            = "after source before patch"
	nativeControlAttemptRaceCase            = "control attempt change"
	nativeControlProgramKey                 = "program_sha256"
	nativeControlRequestKey                 = "request_sha256"
	nativeControlExecutionKey               = "execution_sha256"
	nativeControlWorkKey                    = "work"
	nativeControlBindingKey                 = "source_binding_sha256"
	nativeControlHeadKey                    = "head_lsn"
	nativeControlSourceClosedKey            = "source_closed"
	nativeControlPrivateClosedKey           = "private_closed"
	nativeControlDiagnosticsKey             = "diagnostics_clean"
	nativeControlTestEnd                    = "0/40"
	nativeControlLaterEnd                   = "0/80"
	nativeControlWorkerUID        types.UID = "native-control-worker"
	nativeControlCanary                     = "private-control-diagnostic-canary"
)

type nativeControlFixture struct {
	t                    *testing.T
	r                    *MigrationReconciler
	store                client.WithWatch
	m                    *v1beta1.Migration
	worker               *batchv1.Job
	pvc                  *corev1.PersistentVolumeClaim
	pod                  *corev1.Pod
	control              *native.Client
	mu                   sync.Mutex
	head                 string
	heads, sets, patches int
	sentEnds             []string
	trace                []string
	fault                string
	afterExec            func(bool)
	beforeGet            func(client.Object)
	patch                func(context.Context, client.Client, client.Object, client.Patch) error
}

func newNativeControlFixture(t *testing.T) *nativeControlFixture {
	t.Helper()
	m, pvc := nativeWorkerJobInputs()
	narrowNativeWorker(m)
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	m.Spec.Cutover.Approved = true
	m.Status.Attempts = 1
	m.Status.JobName = jobName(m, 1)
	pvc.Status.Phase = corev1.ClaimBound
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, batchv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	worker, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
	if err != nil {
		t.Fatal(err)
	}
	worker.UID = nativeControlWorkerUID
	worker.Generation = 1
	worker.TypeMeta = metav1.TypeMeta{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: lifecycleJobKind}
	if controllerutil.SetControllerReference(m, worker, scheme) != nil {
		t.Fatal("worker owner fixture failed")
	}
	defaultProbeJob(worker)
	worker.Status.Active = 1
	pod := probePod(worker)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Unix(100, 0))}}
	store := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(m, pvc, worker, pod).WithStatusSubresource(&v1beta1.Migration{}).Build()
	if err := store.Get(context.Background(), client.ObjectKeyFromObject(m), m); err != nil {
		t.Fatal(err)
	}
	f := &nativeControlFixture{t: t, m: m, pvc: pvc, worker: worker, pod: pod, store: store, head: nativeControlTestEnd}
	f.r = &MigrationReconciler{Scheme: scheme, RPGMigrateRunnerImage: nativeEvidenceTestImage}
	f.r.APIReader = nativeControlAPIReader{f}
	f.r.Client = interceptor.NewClient(store, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			t.Fatal("control used cached API GET")
			return errBoom
		},
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			t.Fatal("control created a resource")
			return errBoom
		},
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			t.Fatal("control deleted a resource")
			return errBoom
		},
		SubResourcePatch: f.patchStatus,
	})
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	checked, err := podexec.New(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	f.control = native.NewClient(checked)
	return f
}

type nativeControlAPIReader struct{ f *nativeControlFixture }

func (r nativeControlAPIReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch obj.(type) {
	case *v1beta1.Migration, *batchv1.Job, *corev1.Pod, *corev1.PersistentVolumeClaim:
	default:
		r.f.t.Fatal("control read Secret/other resource")
	}
	if r.f.beforeGet != nil {
		r.f.beforeGet(obj)
	}
	return r.f.store.Get(ctx, key, obj, opts...)
}
func (r nativeControlAPIReader) List(ctx context.Context, obj client.ObjectList, opts ...client.ListOption) error {
	pods, ok := obj.(*corev1.PodList)
	if !ok {
		r.f.t.Fatal("control listed a non-Pod resource")
	}
	options := &client.ListOptions{}
	options.ApplyOptions(opts)
	if options.Namespace != r.f.m.Namespace || options.LabelSelector != nil {
		r.f.t.Fatal("control trusted labels or wrong namespace")
	}
	err := r.f.store.List(ctx, pods, opts...)
	pods.ResourceVersion = "control-snapshot"
	return err
}
func (f *nativeControlFixture) patchStatus(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	f.t.Helper()
	f.patches++
	f.trace = append(f.trace, "optimistic status patch")
	raw, err := p.Data(obj)
	if err != nil {
		f.t.Fatal(err)
	}
	var wire struct {
		Metadata struct {
			RV string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if sub != "status" || json.Unmarshal(raw, &wire) != nil || wire.Metadata.RV == "" {
		f.t.Fatal("native status write did not carry optimistic resourceVersion")
	}
	if f.patch != nil {
		return f.patch(ctx, c, obj, p)
	}
	return c.Status().Patch(ctx, obj, p, opts...)
}
func (f *nativeControlFixture) current() *v1beta1.Migration {
	f.t.Helper()
	m := &v1beta1.Migration{}
	if f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), m) != nil {
		f.t.Fatal("current Migration missing")
	}
	return m
}
func (f *nativeControlFixture) saveEnd(end string) {
	f.t.Helper()
	m := f.current()
	m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: end}
	if err := f.store.Status().Update(context.Background(), m); err != nil {
		f.t.Fatal(err)
	}
	f.m = m
}
func (f *nativeControlFixture) serve(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/exec") {
		f.exec(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	var obj client.Object
	switch {
	case strings.Contains(r.URL.Path, "/jobs/"):
		obj = &batchv1.Job{}
	case strings.Contains(r.URL.Path, "/pods/"):
		obj = &corev1.Pod{}
	default:
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(r.URL.Path, "/")
	if f.store.Get(r.Context(), client.ObjectKey{Namespace: f.m.Namespace, Name: parts[len(parts)-1]}, obj) != nil {
		http.NotFound(w, r)
		return
	}
	_ = json.NewEncoder(w).Encode(obj)
}
func (f *nativeControlFixture) exec(w http.ResponseWriter, r *http.Request) {
	conn, err := (&websocket.Upgrader{Subprotocols: []string{"v5.channel.k8s.io"}}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	var input []byte
	for {
		_, raw, err := conn.ReadMessage()
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
	source := len(input) > 0
	f.mu.Lock()
	fault := f.fault
	head := f.head
	if source {
		f.heads++
		f.trace = append(f.trace, "source sample")
	} else {
		f.sets++
		f.trace = append(f.trace, "explicit setter")
		argv := r.URL.Query()["command"]
		if len(argv) != 12 || argv[10] != "set_endpos" {
			f.t.Error("setter did not use explicit fixed operation")
		} else {
			f.sentEnds = append(f.sentEnds, argv[11])
		}
	}
	hook := f.afterExec
	f.mu.Unlock()
	output := []byte("native-report: {\"replication\":{\"drained\":true}}\n")
	if source {
		output = nativeControlSourceReply(f.t, input, head)
	}
	if hook != nil {
		hook(source)
	}
	if fault == nativeControlDisconnectFault && !source {
		return
	}
	_ = conn.WriteMessage(websocket.BinaryMessage, append([]byte{1}, output...))
	if fault == nativeControlWarningFault && !source {
		_ = conn.WriteMessage(websocket.BinaryMessage, []byte{2, 'x'})
	}
	status := []byte(`{"status":"Success"}`)
	if fault == nativeControlRefusalFault && !source {
		status = []byte(`{"status":"Failure"}`)
	}
	_ = conn.WriteMessage(websocket.BinaryMessage, append([]byte{3}, status...))
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}
func nativeControlSourceReply(t *testing.T, body []byte, head string) []byte {
	t.Helper()
	var request map[string]json.RawMessage
	if json.Unmarshal(body, &request) != nil {
		t.Error("source request invalid")
		return nil
	}
	var execution map[string]json.RawMessage
	if json.Unmarshal(request["execution"], &execution) != nil {
		t.Error("execution request invalid")
		return nil
	}
	requestSHA := sha256.Sum256(body)
	executionSHA := sha256.Sum256(append([]byte("rpgmigrate.native.execution.v1\x00"), requestSHA[:]...))
	result := map[string]any{probeTestVersionCommand: 1, lifecycleKindKey: request[lifecycleKindKey], probeOperationKey: request[probeOperationKey], nativeControlProgramKey: request[nativeControlProgramKey], nativeControlRequestKey: hex.EncodeToString(requestSHA[:]), nativeControlExecutionKey: hex.EncodeToString(executionSHA[:]), nativeControlWorkKey: execution[nativeControlWorkKey], nativeControlBindingKey: strings.Repeat("a", 64), nativeControlHeadKey: head, nativeControlSourceClosedKey: true, nativeControlPrivateClosedKey: true, nativeControlDiagnosticsKey: true}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Error("source result encoding failed")
		return nil
	}
	return append(raw, '\n')
}

func TestNativeControlAdmission(t *testing.T) {
	t.Run("pre E original worker data", testNativeControlBeforeEnd)
	for _, field := range []string{"Job UID", "PVC UID", "Pod UID during admission", nativeControlRestartCase, "terminal worker", "wrong image", "literal environment", "namespace duplicate"} {
		t.Run(field, func(t *testing.T) {
			f := newNativeControlFixture(t)
			switch field {
			case "Job UID":
				f.worker.UID = probeReplacement
			case "PVC UID":
				f.pvc.UID = probeReplacement
			case nativeControlRestartCase:
				p := f.pod.DeepCopy()
				p.Status.ContainerStatuses[0].RestartCount = 1
				if f.store.Status().Update(context.Background(), p) != nil {
					t.Fatal("restart fixture failed")
				}
			case "terminal worker":
				p := f.pod.DeepCopy()
				p.Status.Phase = corev1.PodSucceeded
				p.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}
				if f.store.Status().Update(context.Background(), p) != nil {
					t.Fatal("terminal fixture failed")
				}
			case "wrong image":
				p := f.pod.DeepCopy()
				p.Status.ContainerStatuses[0].ImageID = testRunnerImage
				if f.store.Status().Update(context.Background(), p) != nil {
					t.Fatal("image fixture failed")
				}
			case "literal environment":
				j := f.worker.DeepCopy()
				j.Spec.Template.Spec.Containers[0].Env[0].Value = nativeControlCanary
				if f.store.Update(context.Background(), j) != nil {
					t.Fatal("literal fixture failed")
				}
			case "namespace duplicate":
				p := f.pod.DeepCopy()
				p.Name += "-duplicate"
				p.UID = probeReplacement
				p.ResourceVersion = ""
				p.Labels = nil
				if f.store.Create(context.Background(), p) != nil {
					t.Fatal("duplicate fixture failed")
				}
			case "Pod UID during admission":
				reads := 0
				f.beforeGet = func(obj client.Object) {
					if _, ok := obj.(*corev1.Pod); ok {
						reads++
						if reads == 2 {
							p := f.pod.DeepCopy()
							if f.store.Get(context.Background(), client.ObjectKeyFromObject(p), p) != nil {
								t.Fatal("Pod fixture missing")
							}
							p.UID = probeReplacement
							if f.store.Update(context.Background(), p) != nil {
								t.Fatal("replacement fixture failed")
							}
						}
					}
				}
			}
			w, err := f.r.admitNativeWorkerExecution(context.Background(), f.m, f.worker, f.pvc)
			if err == nil || w != (native.ExecutionData{}) || f.heads != 0 || f.sets != 0 || f.patches != 0 {
				t.Fatal("ambiguous worker execution was admitted or reached effects")
			}
		})
	}
}

func testNativeControlBeforeEnd(t *testing.T) {
	t.Helper()

	f := newNativeControlFixture(t)
	w, err := f.r.admitNativeWorkerExecution(context.Background(), f.m, f.worker, f.pvc)
	if err != nil || w.Role != native.JobRoleWorker || w.JobUID != nativeControlWorkerUID || w.WorkerJobUID != w.JobUID || w.PVCUID != f.pvc.UID || w.Container.PodUID != f.pod.UID || w.Work.WorkdirID != string(f.pvc.UID) || nativeControlEnd(f.current()) != "" {
		t.Fatal("pre E active execution was not independently admitted", err)
	}

}

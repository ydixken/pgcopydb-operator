package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	nativeDispositionPublicationKind          = "publication"
	nativeDispositionSlotKind                 = "replication_slot"
	nativeDispositionProbeCommittedKey        = "probe_committed"
	nativeDispositionTotalKey                 = "total"
	nativeDispositionMaterializationClosedKey = "materialization_closed"

	nativeDispositionSlot              = "disposition_slot"
	nativeTargetClosedKey              = "target_closed"
	nativeDispositionPublication       = "disposition_publication"
	nativeUnknownHelperCase            = "unknown helper"
	nativeWorkerReplacementCase        = "changed worker incarnation"
	nativeMissingInventoryCase         = "missing inventory"
	nativeForeignHelperReplacementCase = "foreign replacement"
	nativeForeignHelperUID             = "replacement-compare-helper"
	nativePVCReplacementCase           = "changed PVC incarnation"
	nativeSuspendGenerationCase        = "changed suspend generation"
)

type nativeDispositionFixture struct {
	*nativeCloneFixture
	mu                       sync.Mutex
	creates, deletes, relays int
	workerCreates            int
	lastResult               ctrl.Result
	lines                    map[string][]byte
	edit                     func(map[string]any)
	afterRelay               func()
	unknownCreate            bool
	lostPinPatch             bool
	cleanupCalled            bool
	beforeDelete             func(*batchv1.Job)
	editCleanup              func([]byte) []byte
}

type nativeHistoricalDeletionFixture struct {
	*nativeCloneFixture
	deletes         []string
	metadataPatches int
	patchErr        bool
	deleteErr       bool
	keepWorkerPod   bool
	beforeDelete    func(*batchv1.Job)
}

func newNativeHistoricalDeletionFixture(t *testing.T) *nativeHistoricalDeletionFixture {
	t.Helper()
	f := &nativeHistoricalDeletionFixture{nativeCloneFixture: newNativeCloneFixture(t, true, nil)}
	f.m.Status.NativeFinalizedJobUID = f.m.Status.NativeJobUID
	f.m.Status.Phase = v1beta1.PhaseCompleted
	f.m.Status.CompletedAt = &metav1.Time{Time: time.Unix(100, 0)}
	f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: lifecycleEnd}
	f.m.Status.Conditions = []metav1.Condition{{Type: v1beta1.ConditionComplete, Status: metav1.ConditionTrue, Reason: "Finalized", LastTransitionTime: metav1.NewTime(time.Unix(100, 0))}, {Type: v1beta1.ConditionFailed, Status: metav1.ConditionFalse, Reason: "Finalized", LastTransitionTime: metav1.NewTime(time.Unix(100, 0))}}
	f.m.Finalizers = []string{finalizerName, "test.example/history"}
	f.m.Generation++
	f.m.Spec.Suspend = true
	f.m.Spec.Verification = nil
	f.m.Spec.Source = v1beta1.PostgresConnection{}
	f.m.Spec.Target = v1beta1.PostgresConnection{}
	f.m.Spec.Runner.Image = "unavailable-current-runner"
	f.replace(f.m)
	if err := f.store.Delete(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), f.m); err != nil {
		t.Fatal(err)
	}
	f.r.NativePreflight, f.r.NativeEvidence, f.r.NativeControl, f.r.NativeStop, f.r.NativeJobs = nil, nil, nil, nil, nil
	f.r.Client = interceptor.NewClient(f.store, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			t.Fatal("historical deletion used cache")
			return errBoom
		},
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			t.Fatal("historical deletion created work")
			return errBoom
		},
		SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
			t.Fatal("historical deletion rewrote status")
			return errBoom
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			job, ok := obj.(*batchv1.Job)
			if !ok {
				t.Fatal("historical deletion changed PVC retention policy")
			}
			options := &client.DeleteOptions{}
			for _, option := range opts {
				option.ApplyToDelete(options)
			}
			if options.Preconditions == nil || options.Preconditions.UID == nil || options.Preconditions.ResourceVersion == nil || options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
				t.Fatal("historical deletion lost exact UID/RV foreground guards")
			}
			actual := &batchv1.Job{}
			if f.beforeDelete != nil {
				f.beforeDelete(job)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(job), actual); err != nil {
				return err
			}
			if actual.UID != *options.Preconditions.UID || actual.ResourceVersion != *options.Preconditions.ResourceVersion {
				return nativeControlConflict()
			}
			f.deletes = append(f.deletes, job.Name)
			if f.deleteErr {
				return errBoom
			}
			if err := c.Delete(ctx, obj, opts...); err != nil {
				return err
			}
			pods := &corev1.PodList{}
			if err := c.List(ctx, pods); err != nil {
				return err
			}
			for i := range pods.Items {
				if owner := metav1.GetControllerOf(&pods.Items[i]); owner != nil && owner.UID == job.UID {
					if f.keepWorkerPod && job.UID == f.worker.UID {
						continue
					}
					if err := c.Delete(ctx, &pods.Items[i]); err != nil {
						return err
					}
				}
			}
			return nil
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			f.metadataPatches++
			raw, err := patch.Data(obj)
			if err != nil || !bytes.Contains(raw, []byte(`"resourceVersion"`)) || bytes.Contains(raw, []byte(`"status"`)) {
				t.Fatal("historical finalizer patch changed history or lost optimistic lock")
			}
			if f.patchErr {
				return nativeControlConflict()
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	return f
}

func (f *nativeHistoricalDeletionFixture) reconcile(t *testing.T) (ctrl.Result, error) {
	t.Helper()
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), f.m); err != nil {
		t.Fatal(err)
	}
	handled, result, err := f.r.reconcileNativeDisposition(context.Background(), f.m, f.m.DeepCopy(), nil, nil)
	if !handled {
		t.Fatal("recorded historical deletion was not handled")
	}
	return result, err
}

func TestNativeHistoricalDeletionAbsentResources(t *testing.T) {
	f := newNativeHistoricalDeletionFixture(t)
	before := f.m.Status.DeepCopy()
	for _, object := range []client.Object{f.worker, f.pod, f.pvc} {
		if err := f.store.Delete(context.Background(), object); err != nil {
			t.Fatal(err)
		}
	}
	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 {
		t.Fatal("historical deletion requires current resources or databases", result, err)
	}
	live := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(live, finalizerName) || !nativeFinalizationRecorded(live) || !apiequality.Semantic.DeepEqual(live.Status, *before) || len(f.deletes) != 0 || f.metadataPatches != 1 {
		t.Fatal("historical deletion changed completion or failed to release finalizer")
	}
}

func (f *nativeHistoricalDeletionFixture) installHelper(t *testing.T, check string) {
	t.Helper()
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: compareJobName(f.m, check), Namespace: f.m.Namespace, UID: types.UID("historical-" + check), OwnerReferences: f.worker.OwnerReferences}, Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	if err := f.store.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if check == compareSchema {
		f.m.Status.NativeSchemaJobUID = string(job.UID)
	} else {
		f.m.Status.NativeDataJobUID = string(job.UID)
	}
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
}

func TestNativeHistoricalDeletionKnownResources(t *testing.T) {
	f := newNativeHistoricalDeletionFixture(t)
	f.installHelper(t, compareSchema)
	f.installHelper(t, compareData)
	before := f.m.Status.DeepCopy()
	result, err := f.reconcile(t)
	if err != nil || result.RequeueAfter != 0 || !slices.Equal(f.deletes, []string{compareJobName(f.m, compareSchema), compareJobName(f.m, compareData), f.worker.Name}) {
		t.Fatal("historical cleanup did not finish saved helpers before worker", result, err, f.deletes)
	}
	live := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil {
		t.Fatal(err)
	}
	if !apiequality.Semantic.DeepEqual(live.Status, *before) || controllerutil.ContainsFinalizer(live, finalizerName) {
		t.Fatal("historical cleanup changed receipt or results")
	}
	claim := &corev1.PersistentVolumeClaim{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.pvc), claim); err != nil || claim.UID != f.pvc.UID || !apiequality.Semantic.DeepEqual(claim.OwnerReferences, f.pvc.OwnerReferences) {
		t.Fatal("work PVC was detached or deleted before owner GC", err)
	}
}

func TestNativeHistoricalDeletionForeignResources(t *testing.T) {
	for _, kind := range []string{nativeWorkerReplacementCase, "historical worker owner", nativePVCReplacementCase, "historical PVC owner", "historical active worker"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeHistoricalDeletionFixture(t)
			before := f.m.Status.DeepCopy()
			var protected client.Object
			switch kind {
			case nativeWorkerReplacementCase:
				job := f.worker.DeepCopy()
				job.UID = workerReplacementUID
				f.replace(job)
				protected = job
			case "historical worker owner":
				job := f.worker.DeepCopy()
				job.OwnerReferences[0].UID = workerForeignIdentity
				f.replace(job)
				protected = job
			case nativePVCReplacementCase:
				claim := f.pvc.DeepCopy()
				claim.UID = workerReplacementUID
				f.replace(claim)
				protected = claim
			case "historical PVC owner":
				claim := f.pvc.DeepCopy()
				claim.OwnerReferences[0].UID = workerForeignIdentity
				f.replace(claim)
				protected = claim
			case "historical active worker":
				job := f.worker.DeepCopy()
				job.Status = batchv1.JobStatus{Active: 1}
				f.replace(job)
				protected = job
			}
			result, err := f.reconcile(t)
			if err != nil || result.RequeueAfter <= 0 || f.metadataPatches != 0 {
				t.Fatal("unproved historical cleanup released finalizer", result, err)
			}
			live := &v1beta1.Migration{}
			if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil || !apiequality.Semantic.DeepEqual(live.Status, *before) || !controllerutil.ContainsFinalizer(live, finalizerName) {
				t.Fatal("GC failure revoked historical completion", err)
			}
			if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(protected), protected); err != nil {
				t.Fatal("foreign or active resource was deleted", err)
			}
		})
	}
}

func TestNativeHistoricalDeletionRequeue(t *testing.T) {
	for _, kind := range []string{"historical delete reply unknown", "historical metadata conflict", "historical worker resource version"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeHistoricalDeletionFixture(t)
			before := f.m.Status.DeepCopy()
			switch kind {
			case "historical delete reply unknown":
				f.deleteErr = true
			case "historical metadata conflict":
				f.patchErr = true
			default:
				f.beforeDelete = func(job *batchv1.Job) {
					if job.UID == f.worker.UID {
						f.beforeDelete = nil
						current := &batchv1.Job{}
						if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(job), current); err != nil {
							t.Error(err)
							return
						}
						current.Status.Active = 1
						current.Status.Succeeded = 0
						current.Status.Conditions = nil
						if err := f.store.Status().Update(context.Background(), current); err != nil {
							t.Error(err)
						}
					}
				}
			}
			result, err := f.reconcile(t)
			if err != nil || result.RequeueAfter <= 0 {
				t.Fatal("unknown historical cleanup did not requeue", result, err)
			}
			live := &v1beta1.Migration{}
			if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil || !apiequality.Semantic.DeepEqual(live.Status, *before) || !nativeFinalizationRecorded(live) || !controllerutil.ContainsFinalizer(live, finalizerName) {
				t.Fatal("pending GC changed historical status", err)
			}
		})
	}
}

func TestNativeHistoricalDeletionPodScope(t *testing.T) {
	t.Run("foreign Job UID is unrelated", func(t *testing.T) {
		f := newNativeHistoricalDeletionFixture(t)
		pod := f.pod.DeepCopy()
		pod.Name = "foreign-same-controller-name"
		pod.UID = "foreign-historical-pod"
		pod.ResourceVersion = ""
		pod.OwnerReferences[0].UID = workerReplacementUID
		if err := f.store.Create(context.Background(), pod); err != nil {
			t.Fatal(err)
		}
		if result, err := f.reconcile(t); err != nil || result.RequeueAfter != 0 {
			t.Fatal("unrelated Pod blocked historical deletion", result, err)
		}
		if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(pod), pod); err != nil {
			t.Fatal("unrelated Pod was deleted", err)
		}
	})
	t.Run("recorded Job UID children must close", func(t *testing.T) {
		f := newNativeHistoricalDeletionFixture(t)
		f.keepWorkerPod = true
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		handled, result, err := f.r.reconcileNativeDisposition(ctx, f.m, f.m.DeepCopy(), nil, nil)
		if !handled || err != nil || result.RequeueAfter <= 0 || f.metadataPatches != 0 {
			t.Fatal("owned Pod closure was treated as finished", handled, result, err)
		}
	})
}

type nativeDispositionPodReplacementReader struct {
	client.Reader
	f        *nativeCloneFixture
	podReads int
}

func (r *nativeDispositionPodReplacementReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := r.Reader.Get(ctx, key, obj, opts...)
	if pod, ok := obj.(*corev1.Pod); ok && err == nil && pod.Name == r.f.pod.Name {
		r.podReads++
		if r.podReads == 2 {
			replacement := pod.DeepCopy()
			replacement.UID = "replacement-between-disposition-phases"
			r.f.replace(replacement)
		}
	}
	return err
}

func TestNativeDispositionPodContinuity(t *testing.T) {
	f := newNativeCloneFixture(t, true, nil)
	f.m.Spec.Suspend = true
	f.replace(f.m)
	r := &nativeDispositionPodReplacementReader{Reader: f.r.APIReader, f: f}
	f.r.APIReader = r
	if handled, _, err := nativeDisposition(t, f); !handled || err == nil || f.patches != 0 || r.podReads < 2 {
		t.Fatal("same-Job replacement Pod crossed disposition phases", handled, err, f.patches, r.podReads)
	}
}

func newNativeDispositionFixture(t *testing.T, follow bool) *nativeDispositionFixture {
	t.Helper()
	f := &nativeDispositionFixture{nativeCloneFixture: newNativeCloneFixture(t, follow, nil), lines: map[string][]byte{}}
	f.m.Spec.BackoffLimit = 3
	f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: lifecycleEnd}
	f.worker.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(time.Now().Truncate(time.Second))}}}
	f.replace(f.m)
	f.replace(f.worker)
	f.pod = nativeRecoveryTestPod(f.worker, 1)
	f.replace(f.pod)
	f.r.Recorder = events.NewFakeRecorder(100)
	f.r.Client = interceptor.NewClient(f.store, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			t.Fatal("disposition used cache")
			return errBoom
		},
		Create: f.createRecoveryJob,
		Delete: f.deleteRecoveryJob,
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			f.patches++
			if f.lostPinPatch && obj.(*v1beta1.Migration).Status.Attempts > f.m.Status.Attempts {
				return errors.New("lost native receipt patch")
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
	server := httptest.NewServer(http.HandlerFunc(f.serveRecovery))
	t.Cleanup(server.Close)
	checked, err := podexec.New(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	f.r.NativePreflight, f.r.NativeJobs = checked, checked
	f.r.NativeEvidence = native.NewReader(f.r.APIReader, checked)
	return f
}

func (f *nativeDispositionFixture) serveRecovery(out http.ResponseWriter, request *http.Request) {
	if strings.HasSuffix(request.URL.Path, "/exec") {
		f.execRecovery(out, request)
		return
	}
	if strings.HasSuffix(request.URL.Path, "/log") {
		parts := strings.Split(request.URL.Path, "/")
		f.mu.Lock()
		raw := bytes.Clone(f.lines[parts[len(parts)-2]])
		f.mu.Unlock()
		_, _ = out.Write(raw)
		return
	}
	if strings.HasSuffix(request.URL.Path, "/pods") {
		opts := []client.ListOption{client.InNamespace(f.m.Namespace)}
		if key, value, ok := strings.Cut(request.URL.Query().Get("labelSelector"), "="); ok {
			opts = append(opts, client.MatchingLabels{key: value})
		}
		pods := &corev1.PodList{}
		if err := f.store.List(request.Context(), pods, opts...); err != nil {
			f.t.Error(err)
			return
		}
		pods.ResourceVersion = "disposition-snapshot"
		out.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(out).Encode(pods)
		return
	}
	f.serve(out, request)
}

func (f *nativeDispositionFixture) execRecovery(out http.ResponseWriter, request *http.Request) {
	upgrade := websocket.Upgrader{Subprotocols: []string{nativeCleanupTestProtocol}}
	ws, err := upgrade.Upgrade(out, request, nil)
	if err != nil {
		return
	}
	defer func() { _ = ws.Close() }()
	var raw []byte
	for {
		_, part, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if bytes.Equal(part, []byte{255, 0}) {
			break
		}
		if len(part) == 0 || part[0] != 0 {
			return
		}
		raw = append(raw, part[1:]...)
	}
	var req map[string]any
	if json.Unmarshal(raw, &req) != nil {
		f.t.Error("invalid actual Reader request")
		return
	}
	x := req["execution"].(map[string]any)
	requestSHA := sha256.Sum256(raw)
	executionSHA := sha256.Sum256(append([]byte("rpgmigrate.native.execution.v1\x00"), requestSHA[:]...))
	result := map[string]any{probeTestVersionCommand: 1, lifecycleKindKey: req[lifecycleKindKey], probeOperationKey: req[probeOperationKey], nativeControlProgramKey: req[nativeControlProgramKey], "request_sha256": hex.EncodeToString(requestSHA[:]), "execution_sha256": hex.EncodeToString(executionSHA[:]), nativeControlWorkKey: x[nativeControlWorkKey], nativeTargetClosedKey: true, nativeDispositionMaterializationClosedKey: true, nativeControlDiagnosticsKey: true}
	if req[lifecycleKindKey] == "target_retry" {
		maps.Copy(result, map[string]any{"observed_at_unix": time.Now().Unix(), "census_known": true, nativeDispositionTotalKey: 0, "syncrep": 0, "gate_waiting": 0, "oldest_syncrep_wait_unix": 0, "application_attribution_known": true, "target_synchronous_commit": "on", "synchronous_commit_admitted": true, "probe_attempted": true, nativeDispositionProbeCommittedKey: true})
	} else {
		result["source_closed"], result["private_closed"] = true, true
		result["resources"] = []map[string]any{{lifecycleKindKey: nativeDispositionSlotKind, lifecycleNameKey: nativeDispositionSlot, lifecycleOwnedKey: true, lifecycleRetainKey: false}, {lifecycleKindKey: nativeDispositionPublicationKind, lifecycleNameKey: nativeDispositionPublication, lifecycleOwnedKey: true, lifecycleRetainKey: false}, {lifecycleKindKey: workdirVolume, lifecycleNameKey: nativeCleanupTestWorkdir, lifecycleOwnedKey: true, lifecycleRetainKey: true}}
	}
	f.mu.Lock()
	f.relays++
	f.mu.Unlock()
	if f.edit != nil {
		f.edit(result)
	}
	job, pod := &batchv1.Job{}, &corev1.Pod{}
	if f.store.Get(request.Context(), client.ObjectKey{Namespace: f.m.Namespace, Name: x["job_name"].(string)}, job) != nil || f.store.Get(request.Context(), client.ObjectKey{Namespace: f.m.Namespace, Name: req["pod_name"].(string)}, pod) != nil {
		f.t.Error("actual helper absent")
		return
	}
	job.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	pod.Status.Phase = corev1.PodSucceeded
	pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: pod.Status.ContainerStatuses[0].ContainerID}}
	if f.store.Status().Update(request.Context(), job) != nil || f.store.Status().Update(request.Context(), pod) != nil {
		f.t.Error("could not close actual helper")
		return
	}
	if f.afterRelay != nil {
		f.afterRelay()
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		f.t.Error(err)
		return
	}
	_ = ws.WriteMessage(websocket.BinaryMessage, append([]byte{1}, append(encoded, '\n')...))
	_ = ws.WriteMessage(websocket.BinaryMessage, []byte("\x03{\"status\":\"Success\"}"))
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}

func (f *nativeDispositionFixture) reconcile(t *testing.T) error {
	t.Helper()
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), f.m); err != nil {
		t.Fatal(err)
	}

	handled, result, err := f.r.reconcileNativeDisposition(context.Background(), f.m, f.m.DeepCopy(), f.worker, f.pvc)
	f.lastResult = result
	if !handled {
		t.Fatal("disposition was not handled")
	}
	return err
}

func TestNativeRetryLifecycle(t *testing.T) {
	f := newNativeDispositionFixture(t, false)

	for i := range 3 {
		if err := f.reconcile(t); err != nil {
			t.Fatalf("retry pass %d: %v (relays=%d creates=%d)", i, err, f.relays, f.creates)
		}
		if i == 0 && f.lastResult.RequeueAfter != retryGateInterval {
			t.Fatal("failed attempt did not pace the target gate")
		}
	}
	live := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil {
		t.Fatal(err)
	}
	if live.Status.Attempts != 2 || live.Status.NativeJobUID == string(f.worker.UID) || live.Status.NativePVCUID != string(f.pvc.UID) || nativeControlEnd(live) != lifecycleEnd || f.relays != 2 || f.workerCreates != 1 {
		t.Fatal("actual target authority did not start exactly one receipted retry", live.Status, f.relays, f.workerCreates)
	}
}

type nativeDispositionConsumer interface {
	reconcileNativeDisposition(context.Context, *v1beta1.Migration, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim) (bool, ctrl.Result, error)
}

func nativeDisposition(t *testing.T, f *nativeCloneFixture) (bool, ctrl.Result, error) {
	t.Helper()
	consumer, ok := any(f.r).(nativeDispositionConsumer)
	if !ok {
		t.Fatal("native disposition handler is missing")
	}
	return consumer.reconcileNativeDisposition(context.Background(), f.m, f.m.DeepCopy(), f.worker, f.pvc)
}

func TestNativeDispositionSuccessfulWorker(t *testing.T) {
	for _, phase := range []v1beta1.MigrationPhase{v1beta1.PhaseFailed, v1beta1.PhaseCompleted, v1beta1.PhaseSuspended} {
		t.Run(string(phase), func(t *testing.T) {
			f := newNativeCloneFixture(t, true, nil)
			f.m.Status.Phase = phase
			f.m.Status.Conditions = []metav1.Condition{{Type: v1beta1.ConditionFailed, Status: metav1.ConditionTrue, Reason: "OldNativeDisplay", LastTransitionTime: metav1.Now()}}
			f.replace(f.m)
			if handled, _, err := nativeDisposition(t, f); handled || err != nil || f.patches != 0 {
				t.Fatal("actual successful worker bypassed completion route", handled, err)
			}
		})
	}
}

func TestNativeDispositionSuspendRetains(t *testing.T) {
	f := newNativeCloneFixture(t, true, nil)
	f.m.Spec.Suspend = true
	f.replace(f.m)
	if handled, _, err := nativeDisposition(t, f); !handled || err != nil {
		t.Fatal("terminal suspension refused", handled, err)
	}
	live := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil {
		t.Fatal(err)
	}
	if live.Status.Phase != v1beta1.PhaseSuspended || live.Status.NativeJobUID != string(f.worker.UID) || live.Status.NativePVCUID != string(f.pvc.UID) || meta.IsStatusConditionTrue(live.Status.Conditions, v1beta1.ConditionComplete) {
		t.Fatal("suspension lost retained execution or claimed completion")
	}
	for _, object := range []client.Object{f.worker.DeepCopy(), f.pod.DeepCopy(), f.pvc.DeepCopy()} {
		if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatal("suspension removed resources", err)
		}
	}
}

func TestNativeRetryBudget(t *testing.T) {
	t.Run("counted exhaustion", func(t *testing.T) {
		f := newNativeDispositionFixture(t, false)
		f.m.Spec.BackoffLimit = 0
		if err := f.store.Update(context.Background(), f.m); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			if err := f.reconcile(t); err != nil {
				t.Fatal(err)
			}
		}
		if f.m.Status.Phase != v1beta1.PhaseFailed || f.m.Status.RetryGate.Exemption != v1beta1.RetryGateCounted || f.workerCreates != 0 || f.relays != 1 {
			t.Fatal("counted native budget did not stop", f.m.Status.Phase, f.workerCreates, f.relays)
		}
	})
	t.Run("one exemption", func(t *testing.T) {
		f := newNativeDispositionFixture(t, false)
		f.m.Spec.BackoffLimit = 0
		if err := f.store.Update(context.Background(), f.m); err != nil {
			t.Fatal(err)
		}
		f.edit = func(v map[string]any) {
			v[nativeDispositionTotalKey], v["syncrep"], v["oldest_syncrep_wait_unix"], v["probe_attempted"], v[nativeDispositionProbeCommittedKey] = 1, 1, time.Now().Add(-2*syncRepThreshold).Unix(), false, false
		}
		for range 3 {
			if err := f.reconcile(t); err != nil {
				t.Fatal(err)
			}
		}
		if f.m.Status.ExemptAttempts != 1 || f.m.Status.RetryGate.Exemption != v1beta1.RetryGateExempt || f.workerCreates != 0 {
			t.Fatal("exemption was repeated or ignored")
		}
		f.edit = nil
		if err := f.reconcile(t); err != nil {
			t.Fatal(err)
		}
		if f.workerCreates != 1 {
			t.Fatal("fresh authority did not release exempt retry")
		}
	})
	for _, attempts := range []int32{0, -1, 1<<31 - 1} {
		m := &v1beta1.Migration{}
		m.Status.Attempts = attempts
		if nativeRetryPolicyValid(m) {
			t.Fatal("unsafe counter admitted", attempts)
		}
	}
}

func TestNativeRetryAuthorityRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"unknown commit", func(v map[string]any) { v[nativeDispositionProbeCommittedKey] = false }},
		{"missing census", func(v map[string]any) { v["census_known"] = false }},
		{"open target", func(v map[string]any) { v[nativeTargetClosedKey] = false }},
		{"wrong work", func(v map[string]any) { v[nativeControlWorkKey].(map[string]any)["workdir_id"] = workerForeignIdentity }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeDispositionFixture(t, false)
			if err := f.reconcile(t); err != nil {
				t.Fatal(err)
			}
			f.edit = tc.edit
			if err := f.reconcile(t); err == nil {
				t.Fatal("unknown target authority admitted")
			}
			if f.workerCreates != 0 || f.m.Status.NativeJobUID != string(f.worker.UID) || nativeControlEnd(f.m) != lifecycleEnd {
				t.Fatal("refusal lost original execution")
			}
		})
	}
}

func TestNativeRetryUnknownCreate(t *testing.T) {
	for _, lostPatch := range []bool{false, true} {
		t.Run(strconv.FormatBool(lostPatch), func(t *testing.T) {
			f := newNativeDispositionFixture(t, false)
			for range 2 {
				if err := f.reconcile(t); err != nil {
					t.Fatal(err)
				}
			}
			f.unknownCreate, f.lostPinPatch = !lostPatch, lostPatch
			if err := f.reconcile(t); err == nil {
				t.Fatal("unknown worker outcome admitted")
			}
			if f.workerCreates != 1 || f.m.Status.NativeJobUID != string(f.worker.UID) || f.m.Status.Attempts != 1 {
				t.Fatal("unknown worker repaired receipts")
			}
			f.unknownCreate, f.lostPinPatch = false, false
			if err := f.reconcile(t); err == nil || f.workerCreates != 1 {
				t.Fatal("existing unknown worker was adopted or replayed")
			}
		})
	}
}

func TestNativeDispositionInventoryCleanup(t *testing.T) {
	f := newNativeDispositionFixture(t, true)
	f.m.Status.Replication = nil
	f.m.Finalizers = []string{finalizerName}
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Delete(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if err := f.reconcile(t); err != nil {
		t.Fatal(err, "relays", f.relays, "cleanup", f.cleanupCalled)
	}
	if !f.cleanupCalled || f.relays != 2 || f.workerCreates != 0 || !apierrors.IsNotFound(f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), &v1beta1.Migration{})) {
		t.Fatal("deletion skipped typed no-E inventory or cleanup")
	}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.worker), &batchv1.Job{}); err != nil {
		t.Fatal("cleanup deleted retained original", err)
	}
}

func TestNativeCleanupUnsupportedDisposition(t *testing.T) {
	for _, kind := range []string{nativeMissingInventoryCase, "bad report", "missing closure"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeDispositionFixture(t, true)
			f.m.Finalizers = []string{finalizerName}
			if err := f.store.Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Delete(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			if kind == "bad report" {
				f.editCleanup = func(raw []byte) []byte { return append(raw, []byte("extra\n")...) }
			} else {
				f.edit = func(v map[string]any) {
					if kind == nativeMissingInventoryCase || f.cleanupCalled {
						v["private_closed"] = false
					}
				}
			}
			if err := f.reconcile(t); err == nil {
				t.Fatal("incomplete deletion authority accepted")
			}
			if !controllerutil.ContainsFinalizer(f.m, finalizerName) || f.workerCreates != 0 || kind == nativeMissingInventoryCase && f.cleanupCalled {
				t.Fatal("deletion refusal lost resources/finalizer")
			}
		})
	}
}

func installRecoveryCompare(t *testing.T, f *nativeDispositionFixture, check string) *batchv1.Job {
	t.Helper()
	f.m.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	job, err := buildNativeCompareJob(f.m, f.pvc, f.worker, f.r.RPGMigrateRunnerImage, check)
	if err != nil || controllerutil.SetControllerReference(f.m, job, f.r.Scheme) != nil {
		t.Fatal("build pinned helper", err)
	}
	job.UID, job.Generation = types.UID("pinned-"+check), 1
	defaultProbeJob(job)
	job.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	if err := f.store.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Create(context.Background(), probePod(job)); err != nil {
		t.Fatal(err)
	}
	if check == compareSchema {
		f.m.Status.NativeSchemaJobUID = string(job.UID)
	} else {
		f.m.Status.NativeDataJobUID = string(job.UID)
	}
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	return job
}

func TestNativeHelperPinReset(t *testing.T) {
	f := newNativeDispositionFixture(t, false)
	installRecoveryCompare(t, f, compareSchema)
	installRecoveryCompare(t, f, compareData)
	f.m.Generation++
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := f.reconcile(t); err != nil {
			t.Fatal("pin reset pass", i, err)
		}
	}
	live := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil {
		t.Fatal(err)
	}
	if live.Status.Attempts != 2 || live.Status.NativeSchemaJobUID != "" || live.Status.NativeDataJobUID != "" || nativeControlEnd(live) != lifecycleEnd {
		t.Fatal("new attempt did not atomically reset closed pins")
	}
	for _, check := range []string{compareSchema, compareData} {
		if err := f.store.Get(context.Background(), client.ObjectKey{Namespace: f.m.Namespace, Name: compareJobName(f.m, check)}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
			t.Fatal("old helper remains", err)
		}
	}
}

func TestNativeHelperPinResetRefusals(t *testing.T) {
	for _, kind := range []string{nativeForeignHelperReplacementCase, "patch conflict", nativeUnknownHelperCase} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeDispositionFixture(t, false)
			job := installRecoveryCompare(t, f, compareSchema)
			if kind == nativeUnknownHelperCase {
				f.m.Status.NativeSchemaJobUID = ""
				if err := f.store.Status().Update(context.Background(), f.m); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if err := f.reconcile(t); err != nil {
					t.Fatal(err)
				}
			}
			if kind == nativeForeignHelperReplacementCase {
				f.beforeDelete = func(actual *batchv1.Job) {
					if actual.UID == job.UID {
						f.beforeDelete = nil
						replacement := job.DeepCopy()
						replacement.UID = nativeForeignHelperUID
						f.replace(replacement)
					}
				}
			}
			if kind == "patch conflict" {
				f.lostPinPatch = true
			}
			if err := f.reconcile(t); err == nil {
				t.Fatal("unproven helper closure admitted")
			}
			live := &v1beta1.Migration{}
			if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), live); err != nil {
				t.Fatal(err)
			}
			if live.Status.Attempts != 1 || live.Status.NativeJobUID != string(f.worker.UID) || kind != nativeUnknownHelperCase && live.Status.NativeSchemaJobUID != string(job.UID) {
				t.Fatal("failed retry cleared receipts")
			}
			if kind == nativeForeignHelperReplacementCase {
				got := &batchv1.Job{}
				if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(job), got); err != nil || got.UID != nativeForeignHelperUID {
					t.Fatal("foreign helper was removed", err)
				}
			}
		})
	}
}

func TestNativeDispositionUnstarted(t *testing.T) {
	for _, orphan := range []bool{false, true} {
		t.Run(strconv.FormatBool(orphan), func(t *testing.T) {
			f := newWorkerAttemptFixture(t)
			f.m.Spec.Suspend = true
			if err := f.store.Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			if orphan {
				job := f.expected()
				job.UID = "unrecorded-worker"
				if err := f.store.Create(context.Background(), job); err != nil {
					t.Fatal(err)
				}
			}
			handled, result, err := f.r.reconcileNativeDisposition(context.Background(), f.m, f.m.DeepCopy(), nil, nil)
			if !handled || (err != nil) != orphan || f.creates != 0 || result.RequeueAfter != 0 {
				t.Fatal("prestart suspension created or adopted work", handled, err, f.creates)
			}
			if orphan && f.storedMigration().Status.Phase == v1beta1.PhaseSuspended {
				t.Fatal("unknown worker read as no work")
			}
		})
	}
}

func TestNativeRecoveryIntentRaces(t *testing.T) {
	for _, kind := range []string{nativeSuspendGenerationCase, nativeWorkerReplacementCase, nativePVCReplacementCase} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeDispositionFixture(t, false)
			if err := f.reconcile(t); err != nil {
				t.Fatal(err)
			}
			f.afterRelay = func() {
				switch kind {
				case nativeSuspendGenerationCase:
					current := &v1beta1.Migration{}
					if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), current); err != nil {
						t.Error(err)
						return
					}
					current.Spec.Suspend = true
					current.Generation++
					if err := f.store.Update(context.Background(), current); err != nil {
						t.Error(err)
					}
				case nativeWorkerReplacementCase:
					replacement := f.worker.DeepCopy()
					replacement.UID = "new-worker-incarnation"
					f.replace(replacement)
				case nativePVCReplacementCase:
					replacement := f.pvc.DeepCopy()
					replacement.UID = "new-pvc-incarnation"
					f.replace(replacement)
				}
			}
			if err := f.reconcile(t); err == nil || f.workerCreates != 0 {
				t.Fatal("changed live recovery intent or incarnation admitted")
			}
		})
	}
}

func (f *nativeDispositionFixture) createRecoveryJob(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {

	job, ok := obj.(*batchv1.Job)
	if !ok {
		f.t.Fatal("unexpected disposition creation")
	}
	f.mu.Lock()
	f.creates++
	job.UID, job.Generation = types.UID("disposition-helper-"+strconv.Itoa(f.creates)), 1
	isWorker := job.Name == jobName(f.m, f.m.Status.Attempts+1)
	if isWorker {
		f.workerCreates++
	}
	f.mu.Unlock()
	if isWorker {
		if err := c.Create(ctx, obj, opts...); err != nil {
			return err
		}
		if f.unknownCreate {
			return errors.New("unknown worker create")
		}
		return nil
	}
	defaultProbeJob(job)
	pod := probePod(job)
	if strings.HasPrefix(job.Name, "rpgmigrate-retry-") || strings.HasPrefix(job.Name, "rpgmigrate-inventory-") {
		job.Status = batchv1.JobStatus{Active: 1}
		pod.Status.Phase, pod.Status.ContainerStatuses[0].State = corev1.PodRunning, corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	} else {
		job.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	}
	f.mu.Lock()
	if strings.HasPrefix(job.Name, probeVersion) {
		f.lines[pod.Name] = []byte(`{"protocol_version":1,"engine_version":"rpgmigrate-test","capabilities":["clone.single_db","progress.v1","verify.sealed_files.v1","copy.large_objects","copy.binary_fallback","copy.split","cdc.pgoutput","cutover.v1","cleanup.v1","verify.v1"]}` + "\n")
	}
	if strings.HasPrefix(job.Name, "rpgmigrate-cleanup-") {
		f.cleanupCalled = true
		work, err := nativeWorkIdentity(f.m, f.pvc)
		if err != nil {
			f.t.Fatal(err)
		}
		complete := true
		report := native.Report{ProtocolVersion: 1, EngineVersion: "test", Command: "stream cleanup", Capabilities: []native.Capability{native.CapCleanup}, Work: &work, Phase: "completed", Cleanup: &native.Cleanup{Complete: &complete, Resources: []native.OwnedResource{{Kind: nativeDispositionSlotKind, Name: nativeDispositionSlot, Owned: ptr.To(true), Removed: ptr.To(true)}, {Kind: nativeDispositionPublicationKind, Name: nativeDispositionPublication, Owned: ptr.To(true), Removed: ptr.To(true)}, {Kind: workdirVolume, Name: nativeCleanupTestWorkdir, Owned: ptr.To(true), Removed: ptr.To(false)}}}}
		raw, err := json.Marshal(report)
		if err != nil {
			f.t.Fatal(err)
		}
		line := append(append([]byte("native-report: "), raw...), '\n')
		if f.editCleanup != nil {
			line = f.editCleanup(line)
		}
		f.lines[pod.Name] = line
	}
	f.mu.Unlock()
	if err := c.Create(ctx, obj, opts...); err != nil {
		return err
	}
	return c.Create(ctx, pod)

}

func (f *nativeDispositionFixture) deleteRecoveryJob(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {

	job, ok := obj.(*batchv1.Job)
	if !ok || job.UID == f.worker.UID {
		f.t.Fatal("deleted retained original")
	}
	if f.beforeDelete != nil {
		f.beforeDelete(job)
	}
	options := &client.DeleteOptions{}
	for _, option := range opts {
		option.ApplyToDelete(options)
	}
	actual := &batchv1.Job{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), actual); err != nil {
		return err
	}
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != actual.UID || options.Preconditions.ResourceVersion != nil && *options.Preconditions.ResourceVersion != actual.ResourceVersion {
		return apierrors.NewConflict(batchv1.Resource("jobs"), job.Name, errNativeDisposition)
	}
	if (job.Name == compareJobName(f.m, compareSchema) || job.Name == compareJobName(f.m, compareData)) && options.Preconditions.ResourceVersion == nil {
		f.t.Fatal("helper removal lacks resource version")
	}
	f.mu.Lock()
	f.deletes++
	f.mu.Unlock()
	if err := c.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods); err != nil {
		return err
	}
	for i := range pods.Items {
		if metav1.GetControllerOf(&pods.Items[i]).UID == job.UID {
			if err := c.Delete(ctx, &pods.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil

}

func TestNativeRecoveryCancellation(t *testing.T) {
	f := newNativeDispositionFixture(t, false)
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), f.m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.afterRelay = cancel
	err := f.r.runNativeRecoveryRead(ctx, f.m, f.worker, f.pvc, f.pod, native.JobRoleRetryGate, func(ctx context.Context, w native.ExecutionData) error {
		_, err := f.r.NativeEvidence.ReadRetryReadiness(ctx, w, nativeRetryInputs(f.m))
		return err
	})
	if err == nil || f.creates != 2 || f.deletes != 2 || f.workerCreates != 0 {
		t.Fatal("cancelled target authority escaped known helper closure", err, f.creates, f.deletes)
	}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.worker), &batchv1.Job{}); err != nil {
		t.Fatal("cancellation removed original evidence", err)
	}
}

func TestNativeDispositionActiveSuspend(t *testing.T) {
	f := newNativeDispositionFixture(t, false)
	f.m.Spec.Suspend = true
	f.m.Generation++
	if err := f.store.Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	f.worker.Status = batchv1.JobStatus{Active: 1}
	f.replace(f.worker)
	f.pod.Status.Phase = corev1.PodRunning
	f.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	f.replace(f.pod)
	calls := 0
	f.r.NativeStop = nativeTermFunc(func(context.Context, string, string, podexec.NativeContainerBinding) error {
		calls++
		terminal := f.worker.DeepCopy()
		terminal.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
		if err := f.store.Status().Update(context.Background(), terminal); err != nil {
			t.Error(err)
		}
		terminalPod := f.pod.DeepCopy()
		terminalPod.Status.Phase = corev1.PodFailed
		terminalPod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: f.pod.Status.ContainerStatuses[0].ContainerID, ExitCode: 1}}
		if err := f.store.Status().Update(context.Background(), terminalPod); err != nil {
			t.Error(err)
		}
		return errBoom
	})
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	current := &v1beta1.Migration{}
	if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), current); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || current.Status.Phase != v1beta1.PhaseSuspended || current.Status.NativeJobUID != string(f.worker.UID) || nativeControlEnd(current) != lifecycleEnd || f.deletes != 0 || f.creates != 0 {
		t.Fatal("active suspension lost retained evidence")
	}
}

func TestNativeRetryFinalizedMarkerRefusal(t *testing.T) {
	f := newNativeDispositionFixture(t, false)
	f.m.Status.NativeFinalizedJobUID = f.m.Status.NativeJobUID
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	if err := f.reconcile(t); err == nil {
		t.Fatal("historical marker entered retry admission")
	}
	if f.workerCreates != 0 || f.relays != 0 || f.creates != 0 || f.patches != 0 {
		t.Fatal("historical marker created retry effects")
	}
}

func TestNativeHistoricalDeletionReceiptBoundary(t *testing.T) {
	for _, kind := range []string{"empty receipt", "conflicting receipt", "incomplete historical shape"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeHistoricalDeletionFixture(t)
			switch kind {
			case "empty receipt":
				f.m.Status.NativeFinalizedJobUID = ""
			case "conflicting receipt":
				f.m.Status.NativeFinalizedJobUID = string(workerReplacementUID)
			case "incomplete historical shape":
				f.m.Status.Phase = v1beta1.PhaseVerifying
			}
			if err := f.store.Status().Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
			if _, err := f.reconcile(t); err == nil || len(f.deletes) != 0 || f.metadataPatches != 0 {
				t.Fatal("unqualified history authorized Kubernetes cleanup")
			}
		})
	}
}

const nativeFailureWinningEnd = "0/FFFF"
const nativeFailureWinningPatchCase = "winning saved E"

type nativeFailureLegacyProgress struct {
	*fakeProgress
	t *testing.T
}

func (p nativeFailureLegacyProgress) GateScript() string {
	p.t.Fatal("native failure reached legacy progress or verification stage SQL")
	return ""
}

type nativeFailureLegacyLogs struct {
	LogReader
	t *testing.T
}

func (l nativeFailureLegacyLogs) JobLogs(context.Context, string, string, int64) ([]byte, error) {
	l.t.Fatal("native failure reached legacy worker, verification or stage logs")
	return nil, errBoom
}

func (l nativeFailureLegacyLogs) JobLogsByUID(context.Context, string, types.UID, int64) ([]byte, error) {
	l.t.Fatal("native failure reached legacy retry log transport")
	return nil, errBoom
}

func (l nativeFailureLegacyLogs) JobLogsTimestamps(context.Context, string, string, int64) ([]byte, error) {
	l.t.Fatal("native failure reached legacy supervisor reaping")
	return nil, errBoom
}

func newNativeFailureFixture(t *testing.T, follow bool, category string) *nativeCloneFixture {
	t.Helper()
	f := newNativeCloneFixture(t, follow, &v1beta1.VerificationOptions{Schema: true, Data: true})
	legacyProgress := f.r.Progress.(*fakeProgress)
	f.r.Progress = nativeFailureLegacyProgress{fakeProgress: legacyProgress, t: t}
	t.Cleanup(func() { f.r.Progress = legacyProgress })
	legacyLogs := f.r.Logs
	f.r.Logs = nativeFailureLegacyLogs{LogReader: legacyLogs, t: t}
	t.Cleanup(func() { f.r.Logs = legacyLogs })
	f.m.Spec.BackoffLimit = 3
	f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: lifecycleEnd}
	f.worker.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
	f.replace(f.m)
	f.replace(f.worker)
	f.pod = nativeRecoveryTestPod(f.worker, 1)
	f.pod.Status.ContainerStatuses[0].State.Terminated.Reason = "Error"
	f.pod.Labels = maps.Clone(f.worker.Spec.Template.Labels)
	f.pod.Labels[batchv1.ControllerUidLabel] = string(f.worker.UID)
	f.replace(f.pod)
	f.raw = nativeFailureTestRecord("ERROR", "native:"+category+": postgres://user:CANARY@private/db row=CANARY")
	f.onCreate = func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
		t.Fatal("terminal classification created worker, verification, cleanup or retry work")
		return errBoom
	}
	return f
}

func assertNativeFailureRetained(t *testing.T, f *nativeCloneFixture) {
	t.Helper()
	got := f.current()
	if got.Status.Attempts != 1 || got.Status.NativeJobUID != string(f.worker.UID) || got.Status.NativePVCUID != string(f.pvc.UID) || nativeControlEnd(got) != lifecycleEnd || got.Status.CompletedAt != nil || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionComplete) {
		t.Fatal("failure changed retained execution, saved E or completion authority")
	}
	for _, obj := range []client.Object{f.worker.DeepCopy(), f.pvc.DeepCopy(), f.pod.DeepCopy()} {
		if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
			t.Fatal("failure removed a retained resource", err)
		}
	}
	if f.r.Sentinel.(*fakeSentinel).callCount() != 0 || f.r.Progress.(interface{ counts() int }).counts() != 0 {
		t.Fatal("failure reached legacy sentinel, retry, progress, verification or supervisor heuristics")
	}
}

func TestNativeFailureDisposition(t *testing.T) {
	for _, follow := range []bool{false, true} {
		for _, category := range []string{"BadArgs", "BadConfig", "BadState"} {
			t.Run(strconv.FormatBool(follow)+"/"+category, func(t *testing.T) {
				f := newNativeFailureFixture(t, follow, category)
				handled, result, err := nativeDisposition(t, f)
				got := f.current()
				condition := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ConditionFailed)
				if !handled || err != nil || result != (ctrl.Result{}) || got.Status.Phase != v1beta1.PhaseFailed || condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "Native"+category || got.Status.RetryGate != nil || f.logs != 1 || f.patches != 1 {
					t.Fatal("terminal native diagnostic did not persist fixed failure before retry", handled, result, err)
				}
				encoded, err := json.Marshal(got.Status)
				if err != nil || bytes.Contains(encoded, []byte("CANARY")) || bytes.Contains(encoded, []byte("postgres://")) {
					t.Fatal("terminal status exposed private diagnostic")
				}
				assertNativeFailureRetained(t, f)
				f.m = got
				f.raw = nil
				if handled, result, err := nativeDisposition(t, f); !handled || err != nil || result != (ctrl.Result{}) || f.patches != 1 || f.logs != 1 {
					t.Fatal("recorded terminal failure retried after logs disappeared", handled, result, err)
				}
			})
		}
	}
}

func TestNativeFailureUnknownRetainsRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"missing logs", nil},
		{"transient", nativeFailureTestRecord("ERROR", "native:Target: transient")},
		{"embedded", nativeFailureTestRecord("ERROR", "embedded native:BadState: CANARY")},
		{"malformed", []byte("{malformed\n")},
		{"partial", []byte(`{"error_severity":"ERROR","message":"native:BadState: partial"}`)},
		{"oversized", nativeFailureTestRecord("ERROR", "native:BadState: "+strings.Repeat("x", 65536))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeFailureFixture(t, true, "BadState")
			f.raw = tc.raw
			handled, result, err := nativeDisposition(t, f)
			got := f.current()
			if !handled || err != nil || result.RequeueAfter != retryGateInterval || got.Status.RetryGate == nil || got.Status.RetryGate.Failure != "Native worker failed" || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionFailed) {
				t.Fatal("unknown diagnostic bypassed controlled retry", handled, result, err)
			}
			assertNativeFailureRetained(t, f)
		})
	}
}

func TestNativeFailureAdmission(t *testing.T) {
	for _, kind := range []string{"success", "active", "pod running", "job replacement", nativePVCReplacementCase, "failure pod replacement", "container replacement", "intent changed", "status changed", "finalized history", "complete history", probeSuspendCase, nativeRecoveryDeleteCase} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeFailureFixture(t, true, "BadState")
			switch kind {
			case "success":
				f.worker.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
				f.replace(f.worker)
			case "active":
				f.worker.Status.Active = 1
				f.replace(f.worker)
			case "pod running":
				f.pod.Status.Phase = corev1.PodRunning
				f.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
				f.replace(f.pod)
			case "finalized history":
				f.m.Status.NativeFinalizedJobUID = f.m.Status.NativeJobUID
				f.replace(f.m)
			case "complete history":
				f.m.Status.Conditions = []metav1.Condition{{Type: v1beta1.ConditionComplete, Status: metav1.ConditionTrue, Reason: "Historical", LastTransitionTime: metav1.Now()}}
				f.replace(f.m)
			case probeSuspendCase:
				f.m.Spec.Suspend = true
				f.replace(f.m)
			case nativeRecoveryDeleteCase:
				f.m.Finalizers = []string{finalizerName}
				f.replace(f.m)
				if err := f.store.Delete(context.Background(), f.m); err != nil {
					t.Fatal(err)
				}
				if err := f.store.Get(context.Background(), client.ObjectKeyFromObject(f.m), f.m); err != nil {
					t.Fatal(err)
				}
				f.r.NativeEvidence = nil
			default:
				f.afterLogs = func() {
					switch kind {
					case "job replacement":
						o := f.worker.DeepCopy()
						o.UID = workerReplacementUID
						f.replace(o)
					case nativePVCReplacementCase:
						o := f.pvc.DeepCopy()
						o.UID = workerReplacementUID
						f.replace(o)
					case "failure pod replacement":
						o := f.pod.DeepCopy()
						o.UID = workerReplacementUID
						f.replace(o)
					case "container replacement":
						o := f.pod.DeepCopy()
						o.Status.ContainerStatuses[0].ContainerID = "containerd://replacement"
						f.replace(o)
					case "intent changed":
						o := f.current()
						o.Generation++
						o.Spec.Suspend = true
						f.replace(o)
					case "status changed":
						o := f.current()
						o.Status.Replication.Endpos = nativeFailureWinningEnd
						f.replace(o)
					}
				}
			}
			_, _, _ = nativeDisposition(t, f)
			got := f.current()
			if meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionFailed) || got.Status.RetryGate != nil || f.logs > 1 || (kind == probeSuspendCase || kind == nativeRecoveryDeleteCase) && f.logs != 0 {
				t.Fatal("unadmitted, successful or retained intent classified terminal")
			}
			if kind != probeSuspendCase && f.patches != 0 {
				t.Fatal("unadmitted failure changed status")
			}
		})
	}
}

func TestNativeFailureStatusPatch(t *testing.T) {
	for _, kind := range []string{"status error", "conflict", nativeFailureWinningPatchCase} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeFailureFixture(t, true, "BadConfig")
			f.patch = func(ctx context.Context, c client.Client, obj client.Object, p client.Patch) error {
				desired := obj.(*v1beta1.Migration)
				condition := meta.FindStatusCondition(desired.Status.Conditions, v1beta1.ConditionFailed)
				if desired.Status.Phase != v1beta1.PhaseFailed || condition == nil || condition.Reason != nativeFailureBadConfig {
					t.Fatal("terminal failure patch was replaced by retry bookkeeping")
				}
				if kind == nativeFailureWinningPatchCase {
					winner := f.current()
					winner.Status.Replication.Endpos = nativeFailureWinningEnd
					if err := c.Status().Update(ctx, winner); err != nil {
						t.Fatal(err)
					}
					return c.Status().Patch(ctx, obj, p)
				}
				if kind == "conflict" {
					return nativeControlConflict()
				}
				return errors.New("CANARY raw status error")
			}
			_, _, err := nativeDisposition(t, f)
			got := f.current()
			if err == nil || strings.Contains(err.Error(), "CANARY") || f.logs != 1 || f.patches != 1 || got.Status.RetryGate != nil || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionFailed) {
				t.Fatal("failed terminal patch lost error or persisted retry authority")
			}
			if kind != "status error" && !apierrors.IsConflict(err) {
				t.Fatal("optimistic conflict was hidden")
			}
			if kind == nativeFailureWinningPatchCase {
				if nativeControlEnd(got) != nativeFailureWinningEnd {
					t.Fatal("conflict overwrote winning saved E")
				}
			} else {
				assertNativeFailureRetained(t, f)
			}
		})
	}
}

func TestNativeFailureNativeDispatch(t *testing.T) {
	f := newNativeFailureFixture(t, true, "BadState")
	result, err := f.r.reconcileNative(context.Background(), f.m, f.m.DeepCopy())
	got := f.current()
	if err != nil || result != (ctrl.Result{}) || got.Status.Phase != v1beta1.PhaseFailed || !meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionFailed) || got.Status.RetryGate != nil || f.logs != 1 || f.patches != 1 {
		t.Fatal("native dispatch bypassed fixed terminal failure", result, err)
	}
	assertNativeFailureRetained(t, f)
}

func TestNativeFailureCaptureUnavailable(t *testing.T) {
	for _, kind := range []string{"failure capture unavailable", "abnormal termination"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeFailureFixture(t, true, "BadState")
			if kind == "failure capture unavailable" {
				f.r.NativeJobs = nil
			} else {
				f.pod.Status.ContainerStatuses[0].State.Terminated.Reason = "OOMKilled"
				f.replace(f.pod)
			}
			handled, result, err := nativeDisposition(t, f)
			got := f.current()
			if !handled || err != nil || result.RequeueAfter != retryGateInterval || got.Status.RetryGate == nil || meta.IsStatusConditionTrue(got.Status.Conditions, v1beta1.ConditionFailed) || f.logs != 0 {
				t.Fatal("unavailable original failure capture became terminal or bypassed retry", handled, result, err)
			}
			assertNativeFailureRetained(t, f)
		})
	}
}

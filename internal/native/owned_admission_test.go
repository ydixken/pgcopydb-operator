package native

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func newOwnedFixture(t *testing.T) retryFixture {
	t.Helper()
	f := newRetryFixture(t)
	w := f.want
	w.Role, w.ProgramSHA256 = JobRoleCleanup, OwnedInventoryProgramSHA256()
	f.base.m.Status.Replication = nil
	command, args, env, volumes, mounts, err := OwnedInventoryMaterialization(f.base.m, w)
	if err != nil {
		t.Fatal(err)
	}
	c := &f.base.job.Spec.Template.Spec.Containers[0]
	c.Command, c.Args, c.Env, c.VolumeMounts = command, args, env, mounts
	f.base.job.Spec.Template.Spec.Volumes = volumes
	f.base.pod.Spec = *f.base.job.Spec.Template.Spec.DeepCopy()
	w.TemplateSHA256 = podexec.NativeTemplateSHA256(f.base.job.Spec.Template)
	w.ConnectionReferenceSHA256, err = OwnedInventoryConnectionReferenceSHA256(f.base.m, w)
	if err != nil {
		t.Fatal(err)
	}
	w.PodName = f.base.pod.Name
	w.Container = podexec.NativeContainerBinding{PodUID: f.base.pod.UID, ContainerName: nativeContainerName, ContainerID: f.base.pod.Status.ContainerStatuses[0].ContainerID, ImageID: w.PlatformImageID, TemplateSHA256: w.TemplateSHA256, ExecutionSHA256: podexec.NativeExecutionSHA256(f.base.job.Spec.Template)}
	f.want = w
	return f
}

func newOwnedReaderAPI(t *testing.T) (*readerAPI, *Reader, ExecutionData) {
	t.Helper()
	api, r := newReaderAPI(t)
	f := newOwnedFixture(t)
	api.fixture = f.base
	api.fixture.pod.Labels = map[string]string{batchv1.ControllerUidLabel: string(f.want.JobUID)}
	api.extraPods = []corev1.Pod{*f.workerPod}
	api.mutateResult = func([]byte) []byte {
		w := f.want
		w.PodName = api.fixture.pod.Name
		w.Container = readerBinding(api.fixture.want)
		w.Container.TemplateSHA256 = f.want.TemplateSHA256
		return ownedResult(t, w, api.input)
	}
	api.Config.Handler = http.HandlerFunc(func(out http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/pods") && request.URL.Query().Get("labelSelector") == batchv1.ControllerUidLabel+"="+string(f.want.JobUID) {
			api.mu.Lock()
			defer api.mu.Unlock()
			out.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(out).Encode(corev1.PodList{Items: []corev1.Pod{*api.fixture.pod.DeepCopy()}})
			return
		}
		if strings.HasSuffix(request.URL.Path, "/pods") && request.URL.Query().Get("labelSelector") == "" {
			api.mu.Lock()
			defer api.mu.Unlock()
			out.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(out).Encode(corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: "1"}, Items: append([]corev1.Pod{*api.fixture.pod.DeepCopy()}, api.extraPods...)})
			return
		}
		if strings.HasSuffix(request.URL.Path, "/pods/"+f.workerPod.Name) {
			api.mu.Lock()
			defer api.mu.Unlock()
			out.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(out).Encode(api.extraPods[0])
			return
		}
		api.serve(out, request)
	})
	return api, r, f.want
}

func TestNativeOwnedStateAdmission(t *testing.T) {
	ownedAdmissionCheckedReads(t)
	ownedAdmissionObjectRefusals(t)
	ownedAdmissionTransportRefusals(t)
	ownedAdmissionBoundaries(t)
}

func ownedAdmissionCheckedReads(t *testing.T) {
	t.Helper()
	for _, succeeded := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed_partial_copy_without_e", true: "successful_original"}[succeeded], func(t *testing.T) {
			api, r, w := newOwnedReaderAPI(t)
			if succeeded {
				api.fixture.worker.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
				api.extraPods[0].Status.Phase = corev1.PodSucceeded
				api.extraPods[0].Status.ContainerStatuses[0].State.Terminated.ExitCode = 0
			}
			got, err := r.ReadOwnedState(context.Background(), w)
			if err != nil || got.Work() != w.Work || len(got.Resources()) != 3 {
				t.Fatalf("checked inventory refused: %v", err)
			}
			resources := got.Resources()
			resources[0].Name = "mutated_inventory_resource"
			if got.Resources()[0].Name == "mutated_inventory_resource" {
				t.Fatal("resource obligations alias admitted state")
			}
			if api.writerCalls != 1 || api.logCalls != 1 {
				t.Fatal("inventory did not join checked relay and actual quiet main")
			}
			request, err := parseOwnedRequest(api.input)
			if err != nil || request.Execution.Role != JobRoleCleanup || request.Execution.Work != w.Work {
				t.Fatal("request was not bound to actual inventory")
			}
		})
	}
}

func ownedAdmissionObjectRefusals(t *testing.T) {
	t.Helper()
	for _, tc := range []struct {
		name   string
		change func(*retryFixture)
	}{
		{"missing_follow", func(f *retryFixture) { f.base.m.Spec.Follow = nil }},
		{"plain_clone", func(f *retryFixture) { f.base.m.Spec.Follow.Enabled = false }},
		{"foreign_generation", func(f *retryFixture) { f.base.m.Generation++ }},
		{"current_attempt", func(f *retryFixture) { f.base.m.Status.Attempts++ }},
		{"oversized_helper", func(f *retryFixture) {
			f.base.job.Annotations = map[string]string{"oversize": strings.Repeat("x", EvidenceJobJSONLimit)}
		}},
		{"helper_uid", func(f *retryFixture) { f.base.job.UID = readerReplacementUID }},
		{"same_worker_helper", func(f *retryFixture) { f.want.JobUID = f.want.WorkerJobUID }},
		{"worker_running", func(f *retryFixture) {
			f.workerPod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{"worker_restarted", func(f *retryFixture) { f.workerPod.Status.ContainerStatuses[0].RestartCount++ }},
		{"worker_ready", func(f *retryFixture) { f.base.worker.Status.Ready = admissionI32(1) }},
		{"worker_terminating", func(f *retryFixture) { f.base.worker.Status.Terminating = admissionI32(1) }},
		{"worker_uncounted", func(f *retryFixture) {
			f.base.worker.Status.UncountedTerminatedPods = &batchv1.UncountedTerminatedPods{Failed: []types.UID{f.workerPod.UID}}
		}},
		{"pod_ready", func(f *retryFixture) {
			f.workerPod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		}},
		{"worker_gone", func(f *retryFixture) { f.base.worker.UID = "" }},
		{"worker_terminating_timestamp", func(f *retryFixture) { f.workerPod.DeletionTimestamp = &metav1.Time{Time: time.Now()} }},
		{"worker_spec_changed", func(f *retryFixture) {
			f.workerPod.Spec.Containers[0].Args = append(f.workerPod.Spec.Containers[0].Args, "--resume")
		}},
		{"helper_restart", func(f *retryFixture) { f.base.pod.Status.ContainerStatuses[0].RestartCount++ }},
		{"helper_program_changed", func(f *retryFixture) { f.base.pod.Spec.Containers[0].Args[8] += "\ntrue\n" }},
		{"helper_mount_changed", func(f *retryFixture) { f.base.pod.Spec.Containers[0].VolumeMounts[0].ReadOnly = false }},
		{"helper_image", func(f *retryFixture) { f.base.pod.Spec.Containers[0].Image = "other@sha256:" + strings.Repeat("2", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOwnedFixture(t)
			tc.change(&f)
			if _, err := validateOwnedObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false); err == nil {
				t.Fatal("invalid inventory execution admitted")
			}
		})
	}
	t.Run("deletion_suspend_preserved", func(t *testing.T) {
		f := newOwnedFixture(t)
		f.base.m.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		f.base.m.Spec.Suspend = true
		if _, err := validateOwnedObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false); err != nil {
			t.Fatal("retained original terminal worker during deletion/suspend refused")
		}
	})
}

func ownedAdmissionTransportRefusals(t *testing.T) {
	t.Helper()
	for _, tc := range []struct {
		name  string
		setup func(*readerAPI)
	}{
		{"missing_worker_pod", func(a *readerAPI) { a.extraPods = nil }},
		{"duplicate_worker_pod", func(a *readerAPI) {
			p := *a.extraPods[0].DeepCopy()
			p.Name = "other-worker"
			p.UID = "other-worker-pod"
			a.extraPods = append(a.extraPods, p)
		}},
		{"replaced_helper", func(a *readerAPI) { a.fixture.pod.OwnerReferences[0].UID = readerReplacementUID }},
		{"main_exit", func(a *readerAPI) { a.mainFault = evidenceExit }},
		{"main_signal", func(a *readerAPI) { a.mainFault = readerFaultSignal }},
		{"main_noise", func(a *readerAPI) { a.logNoise = []byte("unexpected") }},
		{"relay_stderr", func(a *readerAPI) { a.writerFault = sourceStderrCase }},
		{"relay_unknown_commit", func(a *readerAPI) { a.writerFault = readerFaultUnknownCommit }},
		{"relay_unclosed", func(a *readerAPI) { a.writerFault = readerFaultDisconnect }},
		{"relay_no_status", func(a *readerAPI) { a.writerFault = sourceStdoutOnlyCase }},
		{"worker_changed_after_result", func(a *readerAPI) {
			a.afterResult = func(f *admissionFixture) {
				f.worker.Spec.Template.Spec.Containers[0].Args = append(f.worker.Spec.Template.Spec.Containers[0].Args, "--resume")
			}
		}},
		{"e_changed_after_result", func(a *readerAPI) {
			a.afterResult = func(f *admissionFixture) { f.m.Status.Replication = &v1beta1.ReplicationStatus{Endpos: fixtureEndLSN} }
		}},
		{"worker_pod_replaced_after_result", func(a *readerAPI) {
			a.afterResult = func(*admissionFixture) { a.extraPods[0].UID = readerReplacementUID }
		}},
		{"generation_changed_after_result", func(a *readerAPI) { a.afterResult = func(f *admissionFixture) { f.m.Generation++ } }},
		{"deletion_changed_after_result", func(a *readerAPI) {
			a.afterResult = func(f *admissionFixture) { f.m.DeletionTimestamp = &metav1.Time{Time: time.Now()} }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, r, w := newOwnedReaderAPI(t)
			tc.setup(a)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			got, err := r.ReadOwnedState(ctx, w)
			if err == nil || got.Work() != (WorkIdentity{}) || got.Resources() != nil {
				t.Fatal("uncertain inventory admitted obligations")
			}
		})
	}
}

func ownedAdmissionBoundaries(t *testing.T) {
	t.Helper()
	t.Run("unbound_original_pod", func(t *testing.T) {
		a, r, w := newOwnedReaderAPI(t)
		w.PodName = ""
		w.Container = podexec.NativeContainerBinding{}
		if got, err := r.ReadOwnedState(context.Background(), w); err == nil || got.Resources() != nil || a.writerCalls != 0 {
			t.Fatal("unbound replacement pod admitted inventory")
		}
	})
	t.Run("caller_deadline", func(t *testing.T) {
		a, r, w := newOwnedReaderAPI(t)
		a.writerFault = sourceHoldCase
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		started := time.Now()
		if got, err := r.ReadOwnedState(ctx, w); err == nil || got.Resources() != nil || time.Since(started) > time.Second || a.writerCalls != 1 || a.logCalls != 0 {
			t.Fatal("enclosing deadline was not respected")
		}
	})
	t.Run("unfinished_original_main", func(t *testing.T) {
		a, r, w := newOwnedReaderAPI(t)
		a.blockCompletion = true
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		if got, err := r.ReadOwnedState(ctx, w); err == nil || got.Resources() != nil || a.writerCalls != 1 || a.logCalls != 0 {
			t.Fatal("running main admitted inventory")
		}
	})
	t.Run("partial_pod_census", func(t *testing.T) {
		a, r, w := newOwnedReaderAPI(t)
		handler := a.Config.Handler
		a.Config.Handler = http.HandlerFunc(func(out http.ResponseWriter, request *http.Request) {
			if strings.HasSuffix(request.URL.Path, "/pods") && request.URL.Query().Get("labelSelector") == "" {
				out.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(out).Encode(corev1.PodList{ListMeta: metav1.ListMeta{Continue: "unfinished"}, Items: []corev1.Pod{*a.fixture.pod, a.extraPods[0]}})
				return
			}
			handler.ServeHTTP(out, request)
		})
		if got, err := r.ReadOwnedState(context.Background(), w); err == nil || got.Resources() != nil || a.writerCalls != 0 {
			t.Fatal("partial census admitted inventory")
		}
	})
}

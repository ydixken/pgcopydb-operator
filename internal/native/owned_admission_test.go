package native

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
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

const ownedForeignFinalizer = "example.invalid/hold"

func cloneOwnedAdmission(a *ownedAdmission) *ownedAdmission {
	h := *a.helper
	h.migration, h.pvc, h.job, h.worker, h.pod = a.helper.migration.DeepCopy(), a.helper.pvc.DeepCopy(), a.helper.job.DeepCopy(), a.helper.worker.DeepCopy(), a.helper.pod.DeepCopy()
	return &ownedAdmission{helper: &h, workerPod: a.workerPod.DeepCopy()}
}

func TestNativeOwnedJobTrackingRelease(t *testing.T) {
	f := newOwnedFixture(t)
	base, err := validateOwnedObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false)
	if err != nil {
		t.Fatal(err)
	}
	tracking := batchv1.JobTrackingFinalizer
	ended := func(b *ownedAdmission) { b.helper.pod.Status.Phase = corev1.PodSucceeded }
	for _, tc := range []struct {
		name     string
		edit     func(a, b *ownedAdmission)
		admitted bool
	}{
		{"unchanged", func(a, b *ownedAdmission) {}, true},
		{"helper_released", func(a, b *ownedAdmission) { a.helper.pod.Finalizers = []string{tracking}; ended(b) }, true},
		{"helper_released_after_failure", func(a, b *ownedAdmission) {
			a.helper.pod.Finalizers = []string{tracking}
			b.helper.pod.Status.Phase = corev1.PodFailed
		}, true},
		{"helper_released_others_kept", func(a, b *ownedAdmission) {
			a.helper.pod.Finalizers = []string{ownedForeignFinalizer, tracking, "example.invalid/other"}
			b.helper.pod.Finalizers = []string{ownedForeignFinalizer, "example.invalid/other"}
			ended(b)
		}, true},
		{"worker_released", func(a, b *ownedAdmission) { a.workerPod.Finalizers = []string{tracking} }, true},
		{"helper_released_while_running", func(a, b *ownedAdmission) { a.helper.pod.Finalizers = []string{tracking} }, false},
		{"helper_foreign_added", func(a, b *ownedAdmission) {
			a.helper.pod.Finalizers = []string{tracking}
			b.helper.pod.Finalizers = []string{tracking, ownedForeignFinalizer}
			ended(b)
		}, false},
		{"helper_foreign_added_with_release", func(a, b *ownedAdmission) {
			a.helper.pod.Finalizers = []string{tracking}
			b.helper.pod.Finalizers = []string{ownedForeignFinalizer}
			ended(b)
		}, false},
		{"helper_foreign_removed", func(a, b *ownedAdmission) {
			a.helper.pod.Finalizers = []string{ownedForeignFinalizer, tracking}
			b.helper.pod.Finalizers = []string{tracking}
			ended(b)
		}, false},
		{"helper_foreign_removed_with_release", func(a, b *ownedAdmission) {
			a.helper.pod.Finalizers = []string{ownedForeignFinalizer, tracking}
			ended(b)
		}, false},
		{"helper_tracking_added", func(a, b *ownedAdmission) { b.helper.pod.Finalizers = []string{tracking}; ended(b) }, false},
		{"helper_released_with_label_drift", func(a, b *ownedAdmission) {
			a.helper.pod.Finalizers = []string{tracking}
			b.helper.pod.Labels = map[string]string{"drift": "x"}
			ended(b)
		}, false},
		{"helper_released_from_foreign_controller", func(a, b *ownedAdmission) {
			a.helper.pod.Finalizers = []string{tracking}
			a.helper.pod.OwnerReferences[0].UID = readerReplacementUID
			b.helper.pod.OwnerReferences[0].UID = readerReplacementUID
			ended(b)
		}, false},
		{"worker_released_judged_against_helper_job", func(a, b *ownedAdmission) {
			a.workerPod.Finalizers = []string{tracking}
			for _, p := range []*corev1.Pod{a.workerPod, b.workerPod} {
				p.OwnerReferences = a.helper.pod.DeepCopy().OwnerReferences
			}
		}, false},
		{"helper_job_released", func(a, b *ownedAdmission) { a.helper.job.Finalizers = []string{tracking} }, false},
		{"worker_job_released", func(a, b *ownedAdmission) { a.helper.worker.Finalizers = []string{tracking} }, false},
		{"migration_released", func(a, b *ownedAdmission) { a.helper.migration.Finalizers = []string{tracking} }, false},
		{"pvc_released", func(a, b *ownedAdmission) { a.helper.pvc.Finalizers = []string{tracking} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := cloneOwnedAdmission(base), cloneOwnedAdmission(base)
			a.helper.pod.Finalizers, b.helper.pod.Finalizers = nil, nil
			tc.edit(a, b)
			if a.same(b) != tc.admitted {
				t.Fatalf("same admitted %v, want %v", !tc.admitted, tc.admitted)
			}
		})
	}
}

func TestNativeJobTrackingReleaseOnlyOnPods(t *testing.T) {
	f := newOwnedFixture(t)
	owned := metav1.ObjectMeta{Name: "child", OwnerReferences: f.base.pod.DeepCopy().OwnerReferences, Finalizers: []string{batchv1.JobTrackingFinalizer}}
	released := *owned.DeepCopy()
	released.Finalizers = nil
	before, after := &batchv1.Job{ObjectMeta: owned}, &batchv1.Job{ObjectMeta: released, Status: batchv1.JobStatus{Succeeded: 1}}
	if !metav1.IsControlledBy(after, f.base.job) || finalizersHeld(before, after, f.base.job) {
		t.Fatal("tracking release admitted on a non-pod object")
	}
	pod, ended := &corev1.Pod{ObjectMeta: owned}, &corev1.Pod{ObjectMeta: released, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
	if !finalizersHeld(pod, ended, f.base.job) || finalizersHeld(pod, ended, nil) {
		t.Fatal("tracking release not bound to the controlling Job")
	}
}

// checkHelperFinalizerDrift runs one caller against the Job controller's real release and against foreign drift.
func checkHelperFinalizerDrift(t *testing.T, newAPI func(*testing.T) (*readerAPI, *Reader, ExecutionData), read func(*Reader, ExecutionData) error) {
	t.Helper()
	tracking := batchv1.JobTrackingFinalizer
	for _, tc := range []struct {
		name     string
		before   []string
		drift    func(*admissionFixture)
		admitted bool
	}{
		{"tracking_released", []string{tracking}, nil, true},
		{"tracking_released_foreign_kept", []string{ownedForeignFinalizer, tracking}, nil, true},
		{"foreign_added", []string{tracking}, func(f *admissionFixture) { f.pod.Finalizers = append(f.pod.Finalizers, ownedForeignFinalizer) }, false},
		{"foreign_removed", []string{ownedForeignFinalizer, tracking}, func(f *admissionFixture) {
			f.pod.Finalizers = slices.DeleteFunc(f.pod.Finalizers, func(s string) bool { return s == ownedForeignFinalizer })
		}, false},
		{"label_drift", []string{tracking}, func(f *admissionFixture) { f.pod.Labels["drift"] = "x" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, r, w := newAPI(t)
			api.fixture.pod.Finalizers = slices.Clone(tc.before)
			api.afterResult = tc.drift
			err := read(r, w)
			if (err == nil) != tc.admitted || api.writerCalls != 1 {
				t.Fatalf("admitted %v, want %v: %v", err == nil, tc.admitted, err)
			}
			if slices.Contains(api.fixture.pod.Finalizers, tracking) {
				t.Fatal("fixture never released job tracking")
			}
		})
	}
}

func TestNativeOwnedStateJobTrackingRelease(t *testing.T) {
	checkHelperFinalizerDrift(t, newOwnedReaderAPI, func(r *Reader, w ExecutionData) error {
		_, err := r.ReadOwnedState(context.Background(), w)
		return err
	})
}

func TestNativeCleanedJobTrackingRelease(t *testing.T) {
	checkHelperFinalizerDrift(t, newCleanedReaderAPI, func(r *Reader, w ExecutionData) error {
		_, err := r.ReadCleanedCompleted(context.Background(), w, fixtureEndLSN)
		return err
	})
}

func TestNativeCleanupRejoinJobTrackingRelease(t *testing.T) {
	checkHelperFinalizerDrift(t, newCleanupRejoinReaderAPI, func(r *Reader, w ExecutionData) error {
		_, err := r.ReadCleanupRejoinCompleted(context.Background(), w, fixtureEndLSN)
		return err
	})
}

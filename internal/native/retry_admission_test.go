package native

import (
	"bytes"
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

type retryFixture struct {
	base      admissionFixture
	want      ExecutionData
	workerPod *corev1.Pod
}

func newRetryFixture(t *testing.T) retryFixture {
	t.Helper()
	f := newAdmissionFixture(t)
	w := retryWant()
	w.Namespace, w.MigrationName, w.PVCName, w.JobName = f.want.Namespace, f.want.MigrationName, f.want.PVCName, f.want.JobName
	w.MigrationUID, w.PVCUID, w.JobUID, w.WorkerJobUID = f.want.MigrationUID, f.want.PVCUID, f.want.JobUID, f.want.WorkerJobUID
	w.Work = f.want.Work
	w.PodName, w.Container = "", podexec.NativeContainerBinding{}
	command, args, env, volumes, mounts, err := RetryMaterialization(f.m, w)
	if err != nil {
		t.Fatal(err)
	}
	f.job.Spec.Template.Spec.Containers[0].Command = command
	f.job.Spec.Template.Spec.Containers[0].Args = args
	f.job.Spec.Template.Spec.Containers[0].Env = env
	f.job.Spec.Template.Spec.Containers[0].VolumeMounts = mounts
	f.job.Spec.Template.Spec.Volumes = volumes
	f.pod.Spec = *f.job.Spec.Template.Spec.DeepCopy()
	f.worker.Spec.BackoffLimit = admissionI32(0)
	f.worker.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
	wp := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed-worker-pod", Namespace: w.Namespace, UID: "failed-pod-uid", OwnerReferences: []metav1.OwnerReference{admissionOwner("Job", f.worker.Name, f.worker.UID)}}, Spec: *f.worker.Spec.Template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: nativeContainerName, ContainerID: "containerd://failed-worker", Image: w.ImageRef, ImageID: w.PlatformImageID, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, ContainerID: "containerd://failed-worker"}}}}}}
	w.TemplateSHA256 = podexec.NativeTemplateSHA256(f.job.Spec.Template)
	w.ConnectionReferenceSHA256, err = RetryConnectionReferenceSHA256(f.m, w)
	if err != nil {
		t.Fatal(err)
	}
	return retryFixture{f, w, wp}
}

func TestNativeRetryMaterialization(t *testing.T) {
	f := newRetryFixture(t)
	m := f.base.m.DeepCopy()
	m.Spec.Source = v1beta1.PostgresConnection{SynchronousCommit: "invalid-source"}
	command, args, env, volumes, mounts, err := RetryMaterialization(m, f.want)
	if err != nil {
		t.Fatalf("target-only materialization inspected source: %v", err)
	}
	for _, e := range env {
		if strings.Contains(e.Name, "SOURCE") {
			t.Fatal("source input projected")
		}
	}
	if !strings.Contains(args[1], "target_retry "+f.want.OperationID) || strings.Contains(strings.Join(command, " "), "PGCOPYDB_SOURCE") {
		t.Fatal("wrong canonical role")
	}
	if len(volumes) < 2 || volumes[1].EmptyDir == nil || volumes[1].EmptyDir.Medium != corev1.StorageMediumMemory || volumes[1].EmptyDir.SizeLimit.Value() != 64*1024*1024 || !mounts[0].ReadOnly {
		t.Fatal("incorrect isolated storage")
	}
	old, err := RetryConnectionReferenceSHA256(f.base.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := RetryConnectionReferenceSHA256(m, f.want)
	if err != nil || changed != old {
		t.Fatal("source changed target reference scope")
	}
	for _, tc := range []struct {
		name   string
		change func(*retryFixture)
	}{
		{admissionCaseRole, func(f *retryFixture) { f.want.Role = JobRoleEvidence }},
		{admissionCaseProgram, func(f *retryFixture) { f.want.ProgramSHA256[0] ^= 1 }},
		{"foreign_work", func(f *retryFixture) { f.want.Work.WorkdirID = evidenceOther }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetryFixture(t)
			tc.change(&f)
			if _, _, _, _, _, err := RetryMaterialization(f.base.m, f.want); err == nil {
				t.Fatal("invalid materialization admitted")
			}
		})
	}
}

func TestNativeRetryAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*retryFixture)
	}{
		{"valid", func(*retryFixture) {}},
		{"worker_running", func(f *retryFixture) {
			f.workerPod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}},
		{"worker_terminating", func(f *retryFixture) { f.workerPod.DeletionTimestamp = &metav1.Time{Time: time.Now()} }},
		{"worker_restarted", func(f *retryFixture) { f.workerPod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"worker_succeeded", func(f *retryFixture) {
			f.base.worker.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
		}},
		{"worker_replaced", func(f *retryFixture) { f.base.worker.UID = readerReplacementUID }},
		{"worker_active_count", func(f *retryFixture) { f.base.worker.Status.Active = 1 }},
		{"worker_terminating_count", func(f *retryFixture) { f.base.worker.Status.Terminating = admissionI32(1) }},
		{"worker_uncounted", func(f *retryFixture) {
			f.base.worker.Status.UncountedTerminatedPods = &batchv1.UncountedTerminatedPods{Failed: []types.UID{f.workerPod.UID}}
		}},
		{"worker_ready_count", func(f *retryFixture) { f.base.worker.Status.Ready = admissionI32(1) }},
		{"worker_suspend", func(f *retryFixture) { f.base.worker.Spec.Suspend = admissionBool(true) }},
		{"same_worker_helper", func(f *retryFixture) { f.want.JobUID = f.want.WorkerJobUID }},
		{"helper_restart", func(f *retryFixture) { f.base.pod.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"helper_uid", func(f *retryFixture) { f.base.job.UID = readerReplacementUID }},
		{"pvc_replaced_before_retry", func(f *retryFixture) { f.base.pvc.UID = readerReplacementUID }},
		{"migration_generation_changed_in_retry", func(f *retryFixture) { f.base.m.Generation++ }},
		{"helper_program", func(f *retryFixture) { f.base.pod.Spec.Containers[0].Args[1] += "\ntrue\n" }},
		{"target_projection", func(f *retryFixture) {
			f.base.pod.Spec.Containers[0].Env = append(f.base.pod.Spec.Containers[0].Env, corev1.EnvVar{Name: "PGCOPYDB_SOURCE_PGURI", Value: "unused"})
		}},
		{"current_attempt", func(f *retryFixture) { f.base.m.Status.Attempts++ }},
		{"suspended", func(f *retryFixture) { f.base.m.Spec.Suspend = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetryFixture(t)
			tc.change(&f)
			_, err := validateRetryObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false)
			if (err == nil) != (tc.name == "valid") {
				t.Fatalf("unexpected admission: %v", err)
			}
		})
	}
	t.Run("later_failed_attempt", func(t *testing.T) {
		f := newRetryFixture(t)
		f.want.Attempt, f.base.m.Status.Attempts = 2, 2
		f.base.worker.Name, f.base.m.Status.JobName = "migration-run-2", "migration-run-2"
		f.workerPod.OwnerReferences[0].Name = f.base.worker.Name
		if _, err := validateRetryObjects(f.want, f.base.m, f.base.pvc, f.base.job, f.base.worker, f.base.pod, f.workerPod, false); err != nil {
			t.Fatal("independently current later failed attempt refused")
		}
	})
}

func newRetryReaderAPI(t *testing.T, in RetryInputs) (*readerAPI, *Reader, ExecutionData) {
	t.Helper()
	api, reader := newReaderAPI(t)
	f := newRetryFixture(t)
	api.fixture = f.base
	api.fixture.pod.Labels = map[string]string{batchv1.ControllerUidLabel: string(f.want.JobUID)}
	api.extraPods = []corev1.Pod{*f.workerPod}
	api.mutateResult = func([]byte) []byte {
		w := f.want
		w.PodName = api.fixture.pod.Name
		w.Container = readerBinding(api.fixture.want)
		w.Container.TemplateSHA256 = f.want.TemplateSHA256
		return retryResult(t, w, api.input, in)
	}
	api.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/pods") && r.URL.Query().Get("labelSelector") == batchv1.ControllerUidLabel+"="+string(f.want.JobUID) {
			api.mu.Lock()
			defer api.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{*api.fixture.pod.DeepCopy()}})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/pods/"+f.workerPod.Name) {
			api.mu.Lock()
			defer api.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(api.extraPods[0])
			return
		}
		api.serve(w, r)
	})
	return api, reader, f.want
}

func TestNativeRetryReadinessAdmission(t *testing.T) {
	in := RetryInputs{CommitTest: true}
	t.Run("partial_pod_inventory", func(t *testing.T) {
		api, reader, w := newRetryReaderAPI(t, in)
		handler := api.Config.Handler
		api.Config.Handler = http.HandlerFunc(func(out http.ResponseWriter, request *http.Request) {
			if strings.HasSuffix(request.URL.Path, "/pods") && request.URL.Query().Get("labelSelector") == "" {
				out.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(out).Encode(corev1.PodList{ListMeta: metav1.ListMeta{Continue: "unfinished"}, Items: []corev1.Pod{*api.fixture.pod, api.extraPods[0]}})
				return
			}
			handler.ServeHTTP(out, request)
		})
		if got, err := reader.ReadRetryReadiness(context.Background(), w, in); err == nil || got != nil || api.writerCalls != 0 {
			t.Fatal("partial worker census admitted the target operation")
		}
	})
	t.Run("checked_data", func(t *testing.T) {
		api, reader, w := newRetryReaderAPI(t, in)
		got, err := reader.ReadRetryReadiness(context.Background(), w, in)
		if err != nil || got == nil {
			t.Fatalf("checked retry data refused: %v", err)
		}
		data, ok := got.Observation()
		if !ok || !data.ProbeCommitted || data.Total != 0 {
			t.Fatal("admitted data lost")
		}
		data.Total = 99
		if copied, _ := got.Observation(); copied.Total != 0 {
			t.Fatal("observation aliases state")
		}
		if api.writerCalls != 1 || api.logCalls != 1 {
			t.Fatal("did not check original relay and terminal main")
		}
		if bytes.Contains(api.input, []byte(`"saved_e"`)) || bytes.Contains(api.input, []byte(`"checkpoint"`)) {
			t.Fatal("retry request contains completion inputs")
		}
	})
	for _, tc := range []struct {
		name  string
		setup func(*readerAPI)
	}{
		{"missing_worker_pod", func(a *readerAPI) { a.extraPods = nil }},
		{"extra_worker_pod", func(a *readerAPI) {
			p := *a.extraPods[0].DeepCopy()
			p.UID = "extra-pod"
			p.Name = "extra-worker"
			a.extraPods = append(a.extraPods, p)
		}},
		{"bytes_before_failed_main", func(a *readerAPI) { a.mainFault = evidenceExit }},
		{"relay_stderr", func(a *readerAPI) { a.writerFault = sourceStderrCase }},
		{"relay_status", func(a *readerAPI) { a.writerFault = readerStatusCase }},
		{"unclosed_stream", func(a *readerAPI) { a.writerFault = readerFaultDisconnect }},
		{"main_noise", func(a *readerAPI) { a.logNoise = []byte("unexpected") }},
		{"changed_e", func(a *readerAPI) {
			a.afterResult = func(f *admissionFixture) { f.m.Status.Replication.Endpos = fixtureEarlierLSN }
		}},
		{"changed_worker", func(a *readerAPI) {
			a.afterResult = func(f *admissionFixture) {
				f.worker.Spec.Template.Spec.Containers[0].Args = append(f.worker.Spec.Template.Spec.Containers[0].Args, "--resume")
			}
		}},
		{"replaced_worker_pod", func(a *readerAPI) {
			a.afterResult = func(*admissionFixture) { a.extraPods[0].UID = readerReplacementUID }
		}},
		{"worker_reanimated", func(a *readerAPI) {
			a.afterResult = func(*admissionFixture) {
				a.extraPods[0].Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, reader, w := newRetryReaderAPI(t, in)
			tc.setup(api)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if got, err := reader.ReadRetryReadiness(ctx, w, in); err == nil || got != nil {
				t.Fatal("uncertain retry operation admitted data")
			}
		})
	}
}

func TestNativeRetryProgramDelivery(t *testing.T) {
	w := retryWant()
	program := EvidenceProgram()
	command, args, err := renderRetryScripts(program, w.OperationID, w.ProgramSHA256, "exec \"$0\" \"$@\"")
	if err != nil || len(command) != 4 || len(args) != 2 || len(args[1])+1 > EvidenceMainArgumentLimit {
		t.Fatal("canonical retry program framing unavailable")
	}
	framed := "<<'RPGMIGRATE_NATIVE_EVIDENCE_PY'\n" + string(program) + "RPGMIGRATE_NATIVE_EVIDENCE_PY\n"
	if !strings.HasSuffix(args[1], framed) {
		t.Fatal("canonical program bytes changed during framing")
	}
	for role, original := range map[string]*batchv1.Job{
		completedEvidenceKind: newAdmissionFixture(t).job,
		"target_retry":        newRetryFixture(t).base.job,
	} {
		job := original.DeepCopy()
		job.TypeMeta = metav1.TypeMeta{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: "Job"}
		raw, err := json.Marshal(job)
		if err != nil || len(raw) > EvidenceJobJSONLimit || ValidateEvidenceJobSize(job) != nil {
			t.Fatal("canonical helper exceeds its Job JSON limit")
		}
		container := job.Spec.Template.Spec.Containers[0]
		if len(container.Args[1])+1 > EvidenceMainArgumentLimit || len(container.Command[2])+1 > EvidencePreludeArgumentLimit {
			t.Fatal("canonical helper exceeds its argument limits")
		}
		t.Logf("%s canonical=%d main_with_NUL=%d prelude_with_NUL=%d Job_JSON=%d", role, len(EvidenceProgram()), len(container.Args[1])+1, len(container.Command[2])+1, len(raw))
	}
	program[0] ^= 1
	if _, _, err := renderRetryScripts(program, w.OperationID, w.ProgramSHA256, ""); err == nil {
		t.Fatal("program mutation accepted")
	}
	if _, _, err := renderRetryScripts(EvidenceProgram(), w.OperationID, w.ProgramSHA256, strings.Repeat(" ", EvidencePreludeArgumentLimit)); err == nil {
		t.Fatal("oversized prelude accepted")
	}
}

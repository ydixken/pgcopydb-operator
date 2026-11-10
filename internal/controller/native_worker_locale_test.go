package controller

import (
	"context"
	"testing"

	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	nativeWorkerLocaleAll  = "LC_ALL"
	nativeWorkerLocaleLang = "LANG"
)

type nativeWorkerLocaleTransport struct {
	t     *testing.T
	calls int
}

func (s *nativeWorkerLocaleTransport) InPodNative(context.Context, string, string, podexec.NativeContainerBinding, []string, []byte) (podexec.NativeOutcome, error) {
	s.calls++
	return podexec.NativeOutcome{}, errBoom
}

func (s *nativeWorkerLocaleTransport) JobCompletedNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.NativeOutcome, error) {
	s.t.Fatal("deliberately failed relay reached terminal collection")
	return podexec.NativeOutcome{}, errBoom
}

func TestNativeWorkerJobLocaleAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func([]corev1.EnvVar) []corev1.EnvVar
	}{
		{"factory worker", nil},
		{"missing", func(env []corev1.EnvVar) []corev1.EnvVar {
			for i, e := range env {
				if e.Name == nativeWorkerLocaleAll {
					return append(env[:i], env[i+1:]...)
				}
			}
			return env
		}},
		{"changed", func(env []corev1.EnvVar) []corev1.EnvVar {
			for i := range env {
				if env[i].Name == nativeWorkerLocaleLang {
					env[i].Value = "C.UTF-8"
				}
			}
			return env
		}},
		{"duplicate", func(env []corev1.EnvVar) []corev1.EnvVar {
			return append(env, corev1.EnvVar{Name: nativeWorkerLocaleLang, Value: "C"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeRecoveryFixture(t, 1)
			work, err := nativeWorkIdentity(f.m, f.pvc)
			if err != nil {
				t.Fatal(err)
			}
			w := native.ExecutionData{
				Namespace: f.m.Namespace, MigrationName: f.m.Name, MigrationUID: f.m.UID, MigrationGeneration: f.m.Generation,
				PVCName: f.pvc.Name, PVCUID: f.pvc.UID, WorkerJobUID: f.worker.UID, Attempt: 1, Work: work,
				JobName: "locale-retry", JobUID: "locale-retry-uid", Role: native.JobRoleRetryGate,
				ImageRef: nativeEvidenceTestImage, PlatformImageID: nativeEvidenceTestImage,
				OperationID: probeTestOperation, ProgramSHA256: native.EvidenceProgramSHA256(),
			}
			command, args, env, volumes, mounts, err := native.RetryMaterialization(f.m, w)
			if err != nil {
				t.Fatal(err)
			}
			// Only the worker is a production factory result; the helper is canonical Reader fixture input.
			helper, err := buildNativeVersionJob(f.m, nativeEvidenceTestImage, probeTestVersionName)
			if err != nil {
				t.Fatal(err)
			}
			helper.Name, helper.UID = w.JobName, w.JobUID
			if err := controllerutil.SetControllerReference(f.m, helper, f.r.Scheme); err != nil {
				t.Fatal(err)
			}
			c := &helper.Spec.Template.Spec.Containers[0]
			c.Command, c.Args, c.Env, c.VolumeMounts = command, args, env, mounts
			helper.Spec.Template.Spec.Volumes = volumes
			helperPod := nativeRecoveryTestPod(helper, 0)
			helperPod.UID = "locale-helper-pod"
			helperPod.Status.Phase = corev1.PodRunning
			helperPod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
			w.TemplateSHA256 = podexec.NativeTemplateSHA256(helper.Spec.Template)
			w.ConnectionReferenceSHA256, err = native.RetryConnectionReferenceSHA256(f.m, w)
			if err != nil {
				t.Fatal(err)
			}
			if tc.change != nil {
				f.worker.Spec.Template.Spec.Containers[0].Env = tc.change(f.worker.Spec.Template.Spec.Containers[0].Env)
				f.pod.Spec = *f.worker.Spec.Template.Spec.DeepCopy()
			}
			f.store = clientfake.NewClientBuilder().WithScheme(f.r.Scheme).WithObjects(f.m, f.pvc, f.worker, f.pod, helper, helperPod).Build()
			transport := &nativeWorkerLocaleTransport{t: t}
			reader := native.NewReader(f.r.APIReader, transport)
			got, err := reader.ReadRetryReadiness(context.Background(), w, native.RetryInputs{})
			wantCalls := 0
			if tc.change == nil {
				wantCalls = 1
			}
			if got != nil || err == nil || transport.calls != wantCalls {
				t.Fatalf("worker admission reached relay %d times, want %d; got observation=%t error=%v", transport.calls, wantCalls, got != nil, err)
			}
		})
	}
}

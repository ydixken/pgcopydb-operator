package controller

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type nativeVerificationConsumer interface {
	ensureNativeVerification(context.Context, *v1beta1.Migration, *batchv1.Job, *corev1.PersistentVolumeClaim) (bool, error)
}

func nativeVerify(t *testing.T, f *nativeCloneFixture) (bool, error) {
	t.Helper()
	consumer, ok := any(f.r).(nativeVerificationConsumer)
	if !ok {
		t.Fatal("native verification lifecycle is missing")
	}
	return consumer.ensureNativeVerification(context.Background(), f.m, f.worker, f.pvc)
}

func nativeComparePayload(t *testing.T, f *nativeCloneFixture, check string, matched bool) []byte {
	t.Helper()
	work, err := nativeWorkIdentity(f.m, f.pvc)
	if err != nil {
		t.Fatal(err)
	}
	op, err := nativeComparisonOperationID(f.m, f.pvc, f.worker, work, check)
	if err != nil {
		t.Fatal(err)
	}
	hash := native.ComparisonProgramSHA256()
	exit, count := 3, 1
	if matched {
		exit = 0
	}
	if check == compareSchema {
		count = 0
	}
	raw, err := json.Marshal(map[string]any{probeTestVersionCommand: 1, "kind": "native_comparison", probeOperationKey: op, nativeControlProgramKey: hex.EncodeToString(hash[:]), "work": work, "check": check, "matched": matched, "engine_exit": exit, "report_bytes": 19, "table_count": count, "child_closed": true, "report_closed": true, "private_closed": true, nativeControlDiagnosticsKey: true})
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func nativeInstallCompare(t *testing.T, f *nativeCloneFixture, check string, matched bool) (*batchv1.Job, *corev1.Pod) {
	t.Helper()
	job, err := buildNativeCompareJob(f.m, f.pvc, f.worker, f.r.RPGMigrateRunnerImage, check)
	if err != nil || controllerutil.SetControllerReference(f.m, job, f.r.Scheme) != nil {
		t.Fatalf("build compare helper: %v", err)
	}
	job.UID = types.UID("native-clone-" + check + "-helper")
	job.Status = f.worker.Status
	job.Status.StartTime = &metav1.Time{Time: time.Unix(100, 0)}
	job.Status.CompletionTime = &metav1.Time{Time: time.Unix(200, 0)}
	if err := f.store.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	pod := nativeRecoveryTestPod(job, 0)
	pod.UID = types.UID("native-clone-" + check + "-pod")
	pod.Labels[batchv1.ControllerUidLabel] = string(job.UID)
	if err := f.store.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if f.outputs == nil {
		f.outputs = map[string][]byte{}
	}
	f.outputs[pod.Name] = nativeComparePayload(t, f, check, matched)
	if check == compareSchema {
		f.m.Status.NativeSchemaJobUID = string(job.UID)
	} else {
		f.m.Status.NativeDataJobUID = string(job.UID)
	}
	if err := f.store.Status().Update(context.Background(), f.m); err != nil {
		t.Fatal(err)
	}
	return job, pod
}

func TestNativeVerificationLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		options                *v1beta1.VerificationOptions
		schemaMatch, dataMatch bool
		verified               bool
	}{
		{"schema match", &v1beta1.VerificationOptions{Schema: true}, true, true, true},
		{"schema checked mismatch", &v1beta1.VerificationOptions{Schema: true}, false, true, false},
		{"data match", &v1beta1.VerificationOptions{Data: true}, true, true, true},
		{"data checked mismatch", &v1beta1.VerificationOptions{Data: true}, true, false, false},
		{"both match", &v1beta1.VerificationOptions{Schema: true, Data: true}, true, true, true},
		{"both checked mismatches", &v1beta1.VerificationOptions{Schema: true, Data: true}, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeCloneFixture(t, false, tc.options)
			checks := 0
			if tc.options.Schema {
				nativeInstallCompare(t, f, compareSchema, tc.schemaMatch)
				checks++
			}
			if tc.options.Data {
				nativeInstallCompare(t, f, compareData, tc.dataMatch)
				checks++
			}
			done, err := nativeVerify(t, f)
			if err != nil || !done || len(f.m.Status.Verification) != checks || f.logs != checks {
				t.Fatalf("done=%t checks=%d logs=%d err=%v", done, len(f.m.Status.Verification), f.logs, err)
			}
			if meta.IsStatusConditionTrue(f.m.Status.Conditions, v1beta1.ConditionVerified) != tc.verified || meta.IsStatusConditionTrue(f.m.Status.Conditions, v1beta1.ConditionComplete) || f.patches != 0 {
				t.Fatal("typed verification published completion or changed a known receipt")
			}
		})
	}
	t.Run("running helper preserves asynchronous scan", nativeVerificationRunning)
	t.Run("earlier helper remains bound across entire requested profile", nativeVerificationProfileContinuity)
	t.Run("unsupported rehearsal is refused", nativeVerificationUnsupportedProfile)
	t.Run("unavailable comparison", nativeVerificationRefusals)
	t.Run("known create persists receipt and returns before capture", nativeVerificationReceiptReturn)
	t.Run("unknown creation", nativeVerificationUnknownCreate)
}

func nativeVerificationRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*nativeCloneFixture, *batchv1.Job, *corev1.Pod)
	}{
		{"unrecorded canonical helper", func(f *nativeCloneFixture, _ *batchv1.Job, _ *corev1.Pod) {
			f.m.Status.NativeDataJobUID = ""
			if err := f.store.Status().Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
		}},
		{"different recorded UID", func(f *nativeCloneFixture, _ *batchv1.Job, _ *corev1.Pod) {
			f.m.Status.NativeDataJobUID = "foreign-helper"
			if err := f.store.Status().Update(context.Background(), f.m); err != nil {
				t.Fatal(err)
			}
		}},
		{"helper canonical command changed", func(f *nativeCloneFixture, job *batchv1.Job, _ *corev1.Pod) {
			job.Spec.Template.Spec.Containers[0].Args[9] = compareSchema
			f.replace(job)
		}},
		{"failed helper execution", func(f *nativeCloneFixture, job *batchv1.Job, _ *corev1.Pod) {
			job.Status = batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}
			f.replace(job)
		}},
		{"unchecked engine exit", func(f *nativeCloneFixture, _ *batchv1.Job, pod *corev1.Pod) {
			f.outputs[pod.Name] = []byte(strings.ReplaceAll(string(f.outputs[pod.Name]), `"engine_exit":0`, `"engine_exit":1`))
		}},
		{"private output", func(f *nativeCloneFixture, _ *batchv1.Job, pod *corev1.Pod) {
			f.outputs[pod.Name] = append([]byte("private-verification-canary\n"), f.outputs[pod.Name]...)
		}},
		{"helper Pod replaced after capture", func(f *nativeCloneFixture, _ *batchv1.Job, pod *corev1.Pod) {
			f.afterLogs = func() { pod.UID = "replacement-helper-pod"; f.replace(pod) }
		}},
		{"original Pod replaced after capture", func(f *nativeCloneFixture, _ *batchv1.Job, _ *corev1.Pod) {
			f.afterLogs = func() { p := f.pod.DeepCopy(); p.UID = "replacement-original-pod"; f.replace(p) }
		}},
		{"receipt changed after capture", func(f *nativeCloneFixture, _ *batchv1.Job, _ *corev1.Pod) {
			f.afterLogs = func() {
				m := f.current()
				m.Status.NativeDataJobUID = "changed-helper"
				if err := f.store.Status().Update(context.Background(), m); err != nil {
					f.t.Error(err)
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Data: true})
			job, pod := nativeInstallCompare(t, f, compareData, true)
			tc.change(f, job, pod)
			if done, err := nativeVerify(t, f); done || err == nil || strings.Contains(err.Error(), "private-verification-canary") || f.patches != 0 {
				t.Fatalf("unknown verification accepted: done=%t err=%v", done, err)
			}
		})
	}
}

func nativeVerificationUnknownCreate(t *testing.T) {
	for _, fault := range []string{"unknown Create", "receipt patch conflict"} {
		t.Run(fault, func(t *testing.T) {
			f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Schema: true})
			f.onCreate = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				obj.SetUID("unknown-schema-helper")
				if err := c.Create(ctx, obj, opts...); err != nil {
					return err
				}
				if fault == "unknown Create" {
					return errBoom
				}
				return nil
			}
			if fault == "receipt patch conflict" {
				f.patch = func(context.Context, client.Client, client.Object, client.Patch) error {
					return apierrors.NewConflict(batchv1.Resource("jobs"), "", errBoom)
				}
			}
			if done, err := nativeVerify(t, f); done || err == nil || f.current().Status.NativeSchemaJobUID != "" || f.logs != 0 {
				t.Fatal("unknown creation/receipt became authority")
			}
			f.m = f.current()
			f.patch = nil
			if done, err := nativeVerify(t, f); done || err == nil || f.current().Status.NativeSchemaJobUID != "" || f.logs != 0 {
				t.Fatal("later invocation adopted unrecorded helper")
			}
		})
	}
}

func nativeVerificationRunning(t *testing.T) {
	f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Data: true})
	job, pod := nativeInstallCompare(t, f, compareData, true)
	job.Status = batchv1.JobStatus{Active: 1}
	f.replace(job)
	pod.Status.Phase, pod.Status.ContainerStatuses[0].State = corev1.PodRunning, corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	f.replace(pod)
	if done, err := nativeVerify(t, f); err != nil || done || f.logs != 0 || job.Spec.ActiveDeadlineSeconds != nil {
		t.Fatalf("running scan was not pending: %v", err)
	}
}

func nativeVerificationProfileContinuity(t *testing.T) {
	f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Schema: true, Data: true})
	_, schemaPod := nativeInstallCompare(t, f, compareSchema, true)
	nativeInstallCompare(t, f, compareData, true)
	f.afterLogs = func() {
		if f.logs == 2 {
			schemaPod.UID = "replaced-earlier-schema-pod"
			f.replace(schemaPod)
		}
	}
	if done, err := nativeVerify(t, f); done || err == nil || len(f.m.Status.Verification) != 0 {
		t.Fatal("a later check invalidated an earlier captured helper")
	}
}

func nativeVerificationUnsupportedProfile(t *testing.T) {
	f := newNativeCloneFixture(t, false, nil)
	f.m.Spec.Verification = &v1beta1.VerificationOptions{Rehearsal: true}
	if done, err := nativeVerify(t, f); done || err == nil || f.logs != 0 {
		t.Fatal("unsupported requested profile was silently skipped")
	}
}

func nativeVerificationReceiptReturn(t *testing.T) {
	f := newNativeCloneFixture(t, false, &v1beta1.VerificationOptions{Schema: true})
	f.onCreate = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		obj.SetUID(nativeKnownSchemaHelper)
		return c.Create(ctx, obj, opts...)
	}
	rv := f.m.ResourceVersion
	if done, err := nativeVerify(t, f); done || err != nil || f.logs != 0 || f.patches != 1 || f.m.Status.NativeSchemaJobUID != nativeKnownSchemaHelper || f.m.ResourceVersion == rv || f.current().Status.NativeSchemaJobUID != nativeKnownSchemaHelper {
		t.Fatalf("known Create did not durably return: %v", err)
	}
}

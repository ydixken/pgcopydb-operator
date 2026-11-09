package controller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// NativeComparisonReader transports a classification without completion authority.
type NativeComparisonReader interface {
	JobComparisonNative(context.Context, string, types.UID, podexec.NativeContainerBinding) (podexec.ComparisonOutput, error)
}

func nativeComparisonOperationID(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, worker *batchv1.Job, work native.WorkIdentity, check string) (string, error) {
	if m == nil || m.Generation < 1 || m.Spec.Suspend || !m.DeletionTimestamp.IsZero() || m.Status.Attempts < 1 || worker == nil || worker.UID == "" || !worker.DeletionTimestamp.IsZero() || worker.Namespace != m.Namespace || worker.Name != m.Status.JobName || worker.Name != jobName(m, m.Status.Attempts) || !nativeEvidenceOwnedBy(worker, "Migration", m.Name, m.UID, v1beta1.GroupVersion.String()) || check != compareSchema && check != compareData {
		return "", native.ErrNativeComparisonUnavailable
	}
	want, err := nativeWorkIdentity(m, pvc)
	if err != nil || work != want {
		return "", native.ErrNativeComparisonUnavailable
	}
	for _, uid := range []types.UID{m.UID, pvc.UID, worker.UID} {
		value := string(uid)
		if len(value) > 256 || !utf8.ValidString(value) || strings.TrimSpace(value) != value || strings.Trim(value, "0-") == "" || strings.ContainsAny(value, `/\`) || strings.ContainsFunc(value, func(c rune) bool { return c < 33 || c > 126 }) {
			return "", native.ErrNativeComparisonUnavailable
		}
	}
	fields := []string{string(m.UID), strconv.FormatInt(m.Generation, 10), string(pvc.UID), string(worker.UID), strconv.FormatInt(int64(m.Status.Attempts), 10), check, work.MigrationUID, work.RunID, work.WorkdirID, strconv.FormatInt(work.CheckpointGeneration, 10)}
	hash := sha256.New()
	_, _ = hash.Write([]byte("rpgmigrate.native.compare.v1\x00"))
	for _, value := range fields {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil)[:16]), nil
}

// The caller must separately admit quiescence and the exact current worker, PVC, and execution.
func buildNativeCompareJob(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, worker *batchv1.Job, runnerImage, check string) (*batchv1.Job, error) {
	if m == nil {
		return nil, native.ErrNativeComparisonUnavailable
	}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		return nil, native.ErrNativeComparisonUnavailable
	}
	op, err := nativeComparisonOperationID(m, pvc, worker, work, check)
	if err != nil || !verificationRequested(m) || check == compareSchema && !m.Spec.Verification.Schema || check == compareData && !m.Spec.Verification.Data {
		return nil, native.ErrNativeComparisonUnavailable
	}
	job, err := buildNativeVersionJob(m, runnerImage, "rpgmigrate-version-"+op)
	if err != nil {
		return nil, native.ErrNativeComparisonUnavailable
	}
	command, args, env, volumes, mounts, err := native.ComparisonMaterialization(m, op, check)
	if err != nil {
		return nil, native.ErrNativeComparisonUnavailable
	}
	job.Name = compareJobName(m, check)
	job.Spec.ActiveDeadlineSeconds = nil
	pod := &job.Spec.Template.Spec
	pod.Volumes = append([]corev1.Volume{{Name: workdirVolume, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}}}, volumes...)
	c := &pod.Containers[0]
	c.Command, c.Args = command, args
	c.Env = append(env, nativeIdentityEnv(work)...)
	c.VolumeMounts = append([]corev1.VolumeMount{{Name: workdirVolume, MountPath: "/work"}}, mounts...)
	if native.ValidateEvidenceJobSize(job) != nil {
		return nil, native.ErrNativeComparisonUnavailable
	}
	return job, nil
}

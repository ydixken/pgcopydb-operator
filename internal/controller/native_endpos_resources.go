package controller

import (
	"errors"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var errNativeEndposJob = errors.New("native endpos job configuration invalid")

const nativeEndposTmpVolume = "tmp"
const nativeEndposDirOption = "--dir"

// The controller must admit the original worker's success and absence before creating this helper.
func buildNativeEndposJob(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, runnerImage, name, savedE string) (*batchv1.Job, error) {
	if m == nil || m.Generation <= 0 || !m.DeletionTimestamp.IsZero() || !followEnabled(m) || m.Status.Attempts < 1 || m.Status.JobName != jobName(m, m.Status.Attempts) || m.Status.Replication == nil || m.Status.Replication.Endpos != savedE {
		return nil, errNativeEndposJob
	}
	if end, err := sentinel.ParseLSN(savedE); err != nil || end == 0 || len(savedE) > 17 || strings.TrimSpace(savedE) != savedE {
		return nil, errNativeEndposJob
	}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		return nil, errNativeEndposJob
	}
	operation, ok := strings.CutPrefix(name, "rpgmigrate-endpos-")
	if !ok {
		return nil, errNativeEndposJob
	}
	job, err := buildNativeVersionJob(m, runnerImage, "rpgmigrate-version-"+operation)
	if err != nil {
		return nil, errNativeEndposJob
	}
	target, err := conn.Materialize(conn.Target, &m.Spec.Target)
	if err != nil {
		return nil, errNativeEndposJob
	}
	var preludes []string
	var passfiles []conn.Passfile
	if target.Prelude != "" {
		preludes = append(preludes, target.Prelude)
	}
	if target.Passfile != nil {
		passfiles = append(passfiles, *target.Passfile)
	}
	prelude := conn.PreludeScript(preludes, passfiles, "")
	if len(prelude)+1 > native.EvidencePreludeArgumentLimit || strings.IndexByte(prelude, 0) >= 0 {
		return nil, errNativeEndposJob
	}
	job.Name = name
	pod := &job.Spec.Template.Spec
	pod.Volumes = append([]corev1.Volume{
		{Name: workdirVolume, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}},
		{Name: nativeEndposTmpVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: resource.NewQuantity(64*1024*1024, resource.BinarySI)}}},
	}, target.Volumes...)
	c := &pod.Containers[0]
	c.Command = []string{shellPath, "-c", prelude, c.Command[0]}
	c.Args = []string{"stream", "sentinel", "set", "endpos", savedE, "--json", nativeEndposDirOption, "/work/pgcopydb"}
	c.Env = append(target.Env, corev1.EnvVar{Name: "PGCOPYDB_LOG_JSON", Value: "on"})
	if len(preludes)+len(passfiles) > 0 {
		c.Env = append(c.Env, corev1.EnvVar{Name: "PGPASSFILE", Value: conn.PgpassPath})
	}
	c.Env = append(c.Env, nativeIdentityEnv(work)...)
	c.Env = append(c.Env, corev1.EnvVar{Name: "LC_ALL", Value: "C"}, corev1.EnvVar{Name: "LANG", Value: "C"})
	c.VolumeMounts = append([]corev1.VolumeMount{{Name: workdirVolume, MountPath: "/work"}, {Name: nativeEndposTmpVolume, MountPath: "/tmp"}}, target.Mounts...)
	addConnectTimeout(job)
	if native.ValidateEvidenceJobSize(job) != nil {
		return nil, errNativeEndposJob
	}
	return job, nil
}

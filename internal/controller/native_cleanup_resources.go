package controller

import (
	"errors"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	nativeCleanupStreamArg   = "stream"
	nativeCleanupPassfileEnv = "PGPASSFILE"
	nativeCleanupJSONArg     = "--json"
	nativeCleanupTmpPath     = "/tmp"
	nativeCleanupLogJSONEnv  = "PGCOPYDB_LOG_JSON"
)

var errNativeCleanupJob = errors.New("native cleanup job configuration invalid")

func nativeCleanupArgs(work native.WorkIdentity, owned native.AdmittedOwnedState) ([]string, error) {
	resources := owned.Resources()
	if work == (native.WorkIdentity{}) || work != owned.Work() || work.RunID == strings.Repeat("0", 32) || len(resources) < 3 || len(resources) > 4 {
		return nil, errNativeCleanupJob
	}
	seen := make(map[string]bool, len(resources))
	for _, r := range resources {
		if seen[r.Kind] || r.Name == "" {
			return nil, errNativeCleanupJob
		}
		seen[r.Kind] = true
		switch r.Kind {
		case "replication_slot", "publication":
			if !r.Owned || r.Retain || len(r.Name) > 63 {
				return nil, errNativeCleanupJob
			}
		case workdirVolume:
			if r.Name != pgcopydb.WorkDir || !r.Owned || !r.Retain {
				return nil, errNativeCleanupJob
			}
		case "replication_origin":
			if r.Owned || r.Retain || len(r.Name) > 63 {
				return nil, errNativeCleanupJob
			}
		default:
			return nil, errNativeCleanupJob
		}
	}
	if !seen["replication_slot"] || !seen["publication"] || !seen[workdirVolume] {
		return nil, errNativeCleanupJob
	}
	return []string{nativeCleanupStreamArg, "cleanup", nativeCleanupJSONArg, nativeEndposDirOption, pgcopydb.WorkDir}, nil
}

// The caller must independently admit the retained terminal worker and fresh inventory before Create.
func buildNativeCleanupJob(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim, runnerImage, name string, owned native.AdmittedOwnedState) (*batchv1.Job, error) {
	if m == nil || m.Generation <= 0 || !followEnabled(m) || m.Spec.Clone.AllDatabases || m.Status.Attempts < 1 || m.Status.JobName != jobName(m, m.Status.Attempts) || pvc == nil || pvc.Status.Phase != corev1.ClaimBound {
		return nil, errNativeCleanupJob
	}
	work, err := nativeWorkIdentity(m, pvc)
	if err != nil {
		return nil, errNativeCleanupJob
	}
	args, err := nativeCleanupArgs(work, owned)
	if err != nil {
		return nil, errNativeCleanupJob
	}
	operation, ok := strings.CutPrefix(name, "rpgmigrate-cleanup-")
	if !ok {
		return nil, errNativeCleanupJob
	}
	job, err := buildNativeVersionJob(m, runnerImage, "rpgmigrate-version-"+operation)
	if err != nil {
		return nil, errNativeCleanupJob
	}
	job.Name = name
	pod := &job.Spec.Template.Spec
	pod.Volumes = []corev1.Volume{
		{Name: workdirVolume, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}},
		{Name: nativeEndposTmpVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: resource.NewQuantity(64*1024*1024, resource.BinarySI)}}},
	}
	c := &pod.Containers[0]
	c.VolumeMounts = []corev1.VolumeMount{{Name: workdirVolume, MountPath: pgcopydb.WorkMount}, {Name: nativeEndposTmpVolume, MountPath: nativeCleanupTmpPath}}
	var preludes []string
	var passfiles []conn.Passfile
	for _, side := range []struct {
		name       conn.Side
		connection *v1beta1.PostgresConnection
	}{{conn.Source, &m.Spec.Source}, {conn.Target, &m.Spec.Target}} {
		material, err := conn.Materialize(side.name, side.connection)
		if err != nil {
			return nil, errNativeCleanupJob
		}
		c.Env = append(c.Env, material.Env...)
		pod.Volumes = append(pod.Volumes, material.Volumes...)
		c.VolumeMounts = append(c.VolumeMounts, material.Mounts...)
		if material.Prelude != "" {
			preludes = append(preludes, material.Prelude)
		}
		if material.Passfile != nil {
			passfiles = append(passfiles, *material.Passfile)
		}
	}
	prelude := conn.PreludeScript(preludes, passfiles, "")
	if len(prelude)+1 > native.EvidencePreludeArgumentLimit || strings.IndexByte(prelude, 0) >= 0 {
		return nil, errNativeCleanupJob
	}
	c.Command = []string{shellPath, "-c", prelude, c.Command[0]}
	c.Args = args
	c.Env = append(c.Env, corev1.EnvVar{Name: nativeCleanupLogJSONEnv, Value: "on"})
	if len(preludes)+len(passfiles) > 0 {
		c.Env = append(c.Env, corev1.EnvVar{Name: nativeCleanupPassfileEnv, Value: conn.PgpassPath})
	}
	c.Env = append(c.Env, nativeIdentityEnv(work)...)
	c.Env = append(c.Env, corev1.EnvVar{Name: nativeLCAllEnv, Value: "C"}, corev1.EnvVar{Name: nativeLangEnv, Value: "C"})
	addConnectTimeout(job)
	if native.ValidateEvidenceJobSize(job) != nil {
		return nil, errNativeCleanupJob
	}
	return job, nil
}

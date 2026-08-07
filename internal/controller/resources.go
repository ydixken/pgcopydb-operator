/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

const (
	labelMigration = "pgcopydb-operator.io/migration"
	labelManagedBy = "app.kubernetes.io/managed-by"
	managerName    = "pgcopydb-operator"

	// runnerUID matches the runner image's non-root user (distroless
	// nonroot convention, uid 65532).
	runnerUID int64 = 65532
)

func labels(m *v1alpha1.Migration) map[string]string {
	return map[string]string{
		labelManagedBy: managerName,
		labelMigration: m.Name,
	}
}

func workPVCName(m *v1alpha1.Migration) string   { return m.Name + "-work" }
func filtersCMName(m *v1alpha1.Migration) string { return m.Name + "-filters" }
func jobName(m *v1alpha1.Migration, attempt int32) string {
	return fmt.Sprintf("%s-run-%d", m.Name, attempt)
}

// buildWorkPVC returns the work-directory claim. It holds pgcopydb's catalogs
// and is the unit of resumability: it survives Job restarts and is only
// removed with the Migration itself (ownerReference garbage collection).
func buildWorkPVC(m *v1alpha1.Migration) *corev1.PersistentVolumeClaim {
	size := m.Spec.WorkVolume.Size
	if size.IsZero() {
		size = defaultWorkVolumeSize()
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workPVCName(m),
			Namespace: m.Namespace,
			Labels:    labels(m),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: m.Spec.WorkVolume.StorageClassName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
}

// buildFiltersConfigMap renders the --filters INI, or nil when unused.
func buildFiltersConfigMap(m *v1alpha1.Migration) *corev1.ConfigMap {
	ini := pgcopydb.RenderFilters(m.Spec.Clone.Filters)
	if ini == "" {
		return nil
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      filtersCMName(m),
			Namespace: m.Namespace,
			Labels:    labels(m),
		},
		Data: map[string]string{"filters.ini": ini},
	}
}

// buildJob assembles the worker Job for one attempt. backoffLimit is always 0:
// retries are operator-driven so the attempt count and reasons live in the
// Migration status, and each retry resumes from the work-dir catalogs.
func buildJob(m *v1alpha1.Migration, runnerImage string, attempt int32) (*batchv1.Job, error) {
	src, err := conn.Materialize(conn.Source, &m.Spec.Source)
	if err != nil {
		return nil, err
	}
	tgt, err := conn.Materialize(conn.Target, &m.Spec.Target)
	if err != nil {
		return nil, err
	}

	// Attempt > 1 resumes from the catalogs. The snapshot of the failed
	// attempt is gone with its process, so --resume needs --not-consistent
	// (see docs/research/pgcopydb-cli.md, resume semantics).
	resume := attempt > 1
	args := pgcopydb.CloneArgs(&m.Spec, resume, resume)

	env := append(src.Env, tgt.Env...)
	// Structured runner logs for humans and future machine parsing.
	env = append(env, corev1.EnvVar{Name: "PGCOPYDB_LOG_JSON", Value: "on"})

	var passfiles []conn.Passfile
	for _, mat := range []*conn.Materialized{src, tgt} {
		if mat.Passfile != nil {
			passfiles = append(passfiles, *mat.Passfile)
		}
	}

	volumes := []corev1.Volume{
		{
			Name: "workdir",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: workPVCName(m),
				},
			},
		},
		// The root filesystem is read-only; /tmp holds the assembled
		// passfile and pgcopydb scratch files.
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	mounts := []corev1.VolumeMount{
		{Name: "workdir", MountPath: pgcopydb.WorkDir},
		{Name: "tmp", MountPath: "/tmp"},
	}
	volumes = append(volumes, src.Volumes...)
	volumes = append(volumes, tgt.Volumes...)
	mounts = append(mounts, src.Mounts...)
	mounts = append(mounts, tgt.Mounts...)

	if cm := buildFiltersConfigMap(m); cm != nil {
		volumes = append(volumes, corev1.Volume{
			Name: "filters",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cm.Name},
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "filters", MountPath: "/etc/pgcopydb/conf", ReadOnly: true,
		})
	}

	image := runnerImage
	if m.Spec.Runner.Image != "" {
		image = m.Spec.Runner.Image
	}

	uid := runnerUID
	runAsNonRoot := true
	noPrivEsc := false
	readOnlyRoot := true
	backoff := int32(0)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName(m, attempt),
			Namespace: m.Namespace,
			Labels:    labels(m),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: m.Spec.TTLSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(m)},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &runAsNonRoot,
						RunAsUser:    &uid,
						RunAsGroup:   &uid,
						// The PVC must be writable by the runner user.
						FSGroup:        &uid,
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					NodeSelector: m.Spec.Runner.NodeSelector,
					Tolerations:  m.Spec.Runner.Tolerations,
					Affinity:     m.Spec.Runner.Affinity,
					Volumes:      volumes,
					Containers: []corev1.Container{{
						Name:  "pgcopydb",
						Image: image,
						// sh -c '<prelude>' pgcopydb <args...>: the prelude
						// assembles the passfile and execs pgcopydb "$@",
						// where $0 is "pgcopydb" and $@ are the Args below.
						Command:      []string{"/bin/sh", "-c", conn.PreludeScript(passfiles), "pgcopydb"},
						Args:         args,
						Env:          env,
						VolumeMounts: mounts,
						Resources:    m.Spec.Runner.Resources,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noPrivEsc,
							ReadOnlyRootFilesystem:   &readOnlyRoot,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
					}},
				},
			},
		},
	}
	return job, nil
}

func defaultWorkVolumeSize() resource.Quantity {
	return resource.MustParse("10Gi")
}

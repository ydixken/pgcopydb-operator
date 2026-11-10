package controller

import (
	"errors"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
)

var errNativeCleanedJob = errors.New("native cleaned completion job configuration invalid")

// The caller binds live originals and checks the decorated Job size immediately before Create.
func buildNativeCleanedCompletionJob(m *v1beta1.Migration, runnerImage string, w native.ExecutionData) (*batchv1.Job, error) {
	if m == nil || !m.DeletionTimestamp.IsZero() || m.Spec.Suspend || m.Namespace != w.Namespace || m.Name != w.MigrationName || m.UID != w.MigrationUID || m.Generation != w.MigrationGeneration || m.Status.Attempts != w.Attempt || m.Status.JobName != jobName(m, w.Attempt) || !native.NativeReceiptsMatch(m, w.WorkerJobUID, w.PVCUID) || !nativeControlValidEnd(nativeControlEnd(m)) || w.JobName != "rpgmigrate-cleaned-"+w.OperationID || runnerImageFor(m, runnerImage) != w.ImageRef || w.PlatformImageID != w.ImageRef || w.Role != native.JobRoleEvidence || w.ProgramSHA256 != native.CleanedCompletionProgramSHA256() {
		return nil, errNativeCleanedJob
	}
	command, args, env, volumes, mounts, err := native.CleanedCompletionMaterialization(m, w)
	if err != nil {
		return nil, errNativeCleanedJob
	}
	job, err := buildNativeVersionJob(m, runnerImage, "rpgmigrate-version-"+w.OperationID)
	if err != nil {
		return nil, errNativeCleanedJob
	}
	job.Name = w.JobName
	job.Spec.TTLSecondsAfterFinished = nil
	pod := &job.Spec.Template.Spec
	pod.Volumes = volumes
	c := &pod.Containers[0]
	c.Command, c.Args, c.Env, c.VolumeMounts = command, args, env, mounts
	if native.ValidateEvidenceJobSize(job) != nil {
		return nil, errNativeCleanedJob
	}
	return job, nil
}

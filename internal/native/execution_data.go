package native

import (
	"strings"

	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// ExecutionData contains caller expectations; live canonical admission remains the controller's duty.
type ExecutionData struct {
	Namespace, MigrationName, PVCName, JobName, PodName string
	MigrationUID, PVCUID, JobUID, WorkerJobUID          types.UID
	MigrationGeneration                                 int64
	Attempt                                             int32
	Role                                                JobRole
	Work                                                WorkIdentity
	ImageRef, PlatformImageID                           string
	TemplateSHA256, ConnectionReferenceSHA256           [32]byte
	OperationID                                         string
	ProgramSHA256                                       [32]byte
	Container                                           podexec.NativeContainerBinding
}

func activeRoleWorker(w ExecutionData) bool {
	if w.Role != JobRoleWorker || w.JobUID != w.WorkerJobUID || w.MigrationGeneration <= 0 || w.Attempt < 1 || !sourceWorkIdentity(w.Work) || w.Work.MigrationUID != string(w.MigrationUID) || w.Work.WorkdirID != string(w.PVCUID) || !lowerHex(w.OperationID, 32) || w.ProgramSHA256 != EvidenceProgramSHA256() || w.ProgramSHA256 == ([32]byte{}) || w.TemplateSHA256 == ([32]byte{}) || w.ConnectionReferenceSHA256 == ([32]byte{}) {
		return false
	}
	for _, name := range []string{w.MigrationName, w.PVCName, w.JobName, w.PodName} {
		if len(validation.IsDNS1123Subdomain(name)) != 0 {
			return false
		}
	}
	if len(validation.IsDNS1123Label(w.Namespace)) != 0 {
		return false
	}
	for _, uid := range []types.UID{w.MigrationUID, w.PVCUID, w.JobUID, w.WorkerJobUID, w.Container.PodUID} {
		value := string(uid)
		if !validLabel(value) || len(value) > 256 || strings.Trim(value, "0-") == "" || strings.ContainsAny(value, `/\`) || strings.ContainsFunc(value, func(c rune) bool { return c < 33 || c > 126 }) {
			return false
		}
	}
	c := w.Container
	return pinnedImage(w.ImageRef) && platformImage(w.PlatformImageID) && c.ContainerName == nativeContainerName && validLabel(c.ContainerID) && c.RestartCount == 0 && c.ImageID == w.PlatformImageID && c.TemplateSHA256 == w.TemplateSHA256
}

func sourceWorkIdentity(w WorkIdentity) bool {
	if w.CheckpointGeneration <= 0 {
		return false
	}
	w.CheckpointGeneration = 1
	return validIdentity(w)
}

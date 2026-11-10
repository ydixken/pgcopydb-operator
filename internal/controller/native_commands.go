package controller

import (
	"errors"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func engineExecutable(m *v1beta1.Migration) (string, error) {
	engine, err := backendFor(m)
	if err != nil {
		return "", err
	}
	if engine == v1beta1.MigrationEngineRPGMigrate {
		return "/usr/local/bin/rpgmigrate", nil
	}
	return "pgcopydb", nil
}

func nativeWorkIdentity(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim) (native.WorkIdentity, error) {
	engine, err := backendFor(m)
	if err != nil {
		return native.WorkIdentity{}, err
	}
	if engine != v1beta1.MigrationEngineRPGMigrate {
		return native.WorkIdentity{}, errors.New("native identity requires the native engine")
	}
	uid := string(m.UID)
	if len(uid) != 36 || uid[8] != '-' || uid[13] != '-' || uid[18] != '-' || uid[23] != '-' {
		return native.WorkIdentity{}, errors.New("native Migration UID is not a canonical UUID")
	}
	run := strings.ReplaceAll(uid, "-", "")
	if len(run) != 32 || strings.ContainsFunc(run, func(c rune) bool {
		return (c < '0' || c > '9') && (c < 'a' || c > 'f')
	}) {
		return native.WorkIdentity{}, errors.New("native Migration UID is not a canonical UUID")
	}
	if run == "00000000000000000000000000000000" {
		return native.WorkIdentity{}, errors.New("native Migration UID must be nonzero")
	}
	if pvc == nil || pvc.Namespace != m.Namespace || pvc.Name != workPVCName(m) || !pvc.DeletionTimestamp.IsZero() {
		return native.WorkIdentity{}, errors.New("native work PVC is absent, foreign, or deleting")
	}
	owner := metav1.GetControllerOf(pvc)
	if owner == nil || owner.UID != m.UID || owner.Name != m.Name || owner.Kind != "Migration" || owner.APIVersion != v1beta1.GroupVersion.String() {
		return native.WorkIdentity{}, errors.New("native work PVC is not controlled by this Migration")
	}
	workdir := string(pvc.UID)
	if workdir == "" || len(workdir) > 256 || !utf8.ValidString(workdir) || strings.TrimSpace(workdir) != workdir ||
		strings.ContainsFunc(workdir, unicode.IsControl) || strings.ContainsAny(workdir, `/\`) {
		return native.WorkIdentity{}, errors.New("native work PVC UID is invalid")
	}
	return native.WorkIdentity{MigrationUID: uid, RunID: run, WorkdirID: workdir, CheckpointGeneration: 1}, nil
}

func nativeIdentityEnv(work native.WorkIdentity) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "RPGMIGRATE_MIGRATION_UID", Value: work.MigrationUID},
		{Name: "RPGMIGRATE_RUN_ID", Value: work.RunID},
		{Name: "RPGMIGRATE_WORKDIR_ID", Value: work.WorkdirID},
		{Name: "RPGMIGRATE_CHECKPOINT_GENERATION", Value: strconv.FormatInt(work.CheckpointGeneration, 10)},
	}
}

func nativeCloneArgs(m *v1beta1.Migration, attempt int32) ([]string, error) {
	engine, err := backendFor(m)
	if err != nil {
		return nil, err
	}
	if engine != v1beta1.MigrationEngineRPGMigrate || attempt < 1 {
		return nil, errors.New("native clone requires the native engine and a positive attempt")
	}
	// Argument construction cannot stand in for admission using actual discovery capabilities.
	if _, err := nativeConfigCapabilities(m); err != nil {
		return nil, err
	}
	fresh := attempt == 1
	args := pgcopydb.CloneArgs(&m.Spec, fresh, !fresh, !fresh)
	if split := m.Spec.Clone.SplitTablesLargerThan; split != nil && split.IsZero() {
		// Native admission permits only table-jobs before these paired legacy split selectors.
		if len(args) < 9 || args[5] != "--split-tables-larger-than" || args[6] != "0" || args[7] != "--split-max-parts" {
			return nil, errors.New("native clone split arguments are invalid")
		}
		parts, err := strconv.ParseInt(args[8], 10, 32)
		if err != nil || parts < 1 {
			return nil, errors.New("native clone split arguments are invalid")
		}
		args = append(args[:5], args[9:]...)
	}
	if followEnabled(m) {
		// The native engine persists its own resource identities; legacy names grant no ownership.
		args = append(args, "--follow", "--plugin", "pgoutput")
	}
	return args, nil
}

package controller

import (
	"errors"
	"reflect"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
)

func backendFor(m *v1beta1.Migration) (v1beta1.MigrationEngine, error) {
	if m == nil {
		return "", errors.New("migration is absent")
	}
	switch m.Spec.Engine {
	case "", v1beta1.MigrationEnginePGCopyDB:
		return v1beta1.MigrationEnginePGCopyDB, nil
	case v1beta1.MigrationEngineRPGMigrate:
		return v1beta1.MigrationEngineRPGMigrate, nil
	default:
		return "", errors.New("unsupported Migration engine")
	}
}

// This checks a qualified profile, not whether a runner actually implements its capabilities.
func validateNativeConfig(m *v1beta1.Migration, capabilities []native.Capability) error {
	engine, err := backendFor(m)
	if err != nil || engine == v1beta1.MigrationEnginePGCopyDB {
		return err
	}
	required, err := nativeConfigCapabilities(m)
	if err != nil {
		return err
	}
	return native.RequireCapabilities(capabilities, required...)
}

func nativeConfigCapabilities(m *v1beta1.Migration) ([]native.Capability, error) {
	c := m.Spec.Clone
	if c.TableJobs < 0 || c.SplitMaxParts < 0 || (c.SplitTablesLargerThan != nil && c.SplitTablesLargerThan.Sign() < 0) {
		return nil, errors.New("invalid native clone parallelism")
	}
	// Catalog admission and progress sampling use list progress, never staged list tables.
	// Every native profile can reach helpers that bind sealed comparison files.
	required := []native.Capability{native.CapCloneSingleDB, native.CapProgress, native.CapVerifySealedFiles}
	if c.UseCopyBinary == nil || *c.UseCopyBinary {
		required = append(required, native.CapBinaryFallback)
	}
	if c.SplitTablesLargerThan == nil || !c.SplitTablesLargerThan.IsZero() {
		required = append(required, native.CapSplit)
	}
	largeObjects := true
	for _, skip := range c.Skip {
		if skip != "largeObjects" {
			return nil, errors.New("unsupported native clone skip")
		}
		largeObjects = false
	}
	if largeObjects {
		required = append(required, native.CapLargeObjects)
	}
	c.TableJobs, c.SplitMaxParts = 0, 0
	c.SplitTablesLargerThan, c.UseCopyBinary, c.Skip = nil, nil, nil
	// New exposed options must be qualified before this profile accepts them.
	if !reflect.ValueOf(c).IsZero() || m.Spec.DryRun || (m.Spec.Preflight != nil && m.Spec.Preflight.RequireSameMajorVersion) ||
		m.Spec.Source.SuperuserSecretRef != nil || m.Spec.Target.SuperuserSecretRef != nil ||
		m.Spec.Source.SynchronousCommit != "" || m.Spec.Target.SynchronousCommit != "" {
		return nil, errors.New("unsupported native migration configuration")
	}
	if err := nativeFollowConfig(m, &required); err != nil {
		return nil, err
	}
	if v := m.Spec.Verification; v != nil {
		if v.Rehearsal {
			return nil, errors.New("native verification rehearsal is not qualified")
		}
		if v.Schema || v.Data {
			required = append(required, native.CapVerify)
		}
	}
	return required, nil
}

func nativeFollowConfig(m *v1beta1.Migration, required *[]native.Capability) error {
	f := m.Spec.Follow
	if f != nil {
		if f.Plugin != "" && f.Plugin != "pgoutput" {
			return errors.New("native decoding plugin is not qualified")
		}
		if f.Enabled {
			*required = append(*required, native.CapPGOutput, native.CapCutover, native.CapCleanup)
		}
		other := *f
		other.Enabled, other.Plugin, other.MaxCatchupLag = false, "", nil
		if !reflect.ValueOf(other).IsZero() || (f.MaxCatchupLag != nil && f.MaxCatchupLag.Sign() < 0) {
			return errors.New("native follow options are not qualified")
		}
	}
	if mode := m.Spec.Cutover.Mode; mode != "" && mode != v1beta1.CutoverManual && mode != v1beta1.CutoverAutomatic {
		return errors.New("invalid native cutover mode")
	}
	if (f == nil || !f.Enabled) && (m.Spec.Cutover.Approved || m.Spec.Cutover.Mode == v1beta1.CutoverAutomatic) {
		return errors.New("native cutover requires follow")
	}
	return nil
}

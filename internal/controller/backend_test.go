package controller

import (
	"os"
	"reflect"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	testExternalResourceName                    = "external"
	testSkipLargeObjects     v1beta1.SkipOption = "largeObjects"
)

func TestBackendForNormalizesOnlyEngine(t *testing.T) {
	for _, tc := range []struct {
		engine v1beta1.MigrationEngine
		want   v1beta1.MigrationEngine
	}{
		{"", v1beta1.MigrationEnginePGCopyDB},
		{v1beta1.MigrationEnginePGCopyDB, v1beta1.MigrationEnginePGCopyDB},
		{v1beta1.MigrationEngineRPGMigrate, v1beta1.MigrationEngineRPGMigrate},
	} {
		m := &v1beta1.Migration{Spec: v1beta1.MigrationSpec{Engine: tc.engine, Runner: v1beta1.RunnerSpec{Image: "anything"}, Clone: v1beta1.CloneOptions{AllDatabases: true}}}
		before := m.DeepCopy()
		got, err := backendFor(m)
		if err != nil || got != tc.want || !reflect.DeepEqual(before, m) {
			t.Fatalf("selection = %q, %v, migration changed = %v", got, err, !reflect.DeepEqual(before, m))
		}
	}
	if _, err := backendFor(nil); err == nil {
		t.Fatal("nil Migration selected a backend")
	}
	if _, err := backendFor(&v1beta1.Migration{Spec: v1beta1.MigrationSpec{Engine: testUnrecognizedEngine}}); err == nil {
		t.Fatal("unknown engine selected a backend")
	}
}

func cloneCapabilities() []native.Capability {
	return []native.Capability{native.CapCloneSingleDB, native.CapProgress, native.CapLargeObjects, native.CapBinaryFallback, native.CapSplit}
}

func nativeMigration() *v1beta1.Migration {
	return &v1beta1.Migration{Spec: v1beta1.MigrationSpec{Engine: v1beta1.MigrationEngineRPGMigrate}}
}

func TestValidateNativeConfigRequiresDefaultFeatures(t *testing.T) {
	if err := validateNativeConfig(nativeMigration(), cloneCapabilities()); err != nil {
		t.Fatal(err)
	}
	for i := range cloneCapabilities() {
		caps := cloneCapabilities()
		caps = append(caps[:i], caps[i+1:]...)
		if err := validateNativeConfig(nativeMigration(), caps); err == nil {
			t.Fatalf("accepted missing default capability %d", i)
		}
	}
	legacy := &v1beta1.Migration{Spec: v1beta1.MigrationSpec{Clone: v1beta1.CloneOptions{AllDatabases: true}}}
	if err := validateNativeConfig(legacy, nil); err != nil {
		t.Fatalf("native validation changed legacy selection: %v", err)
	}
}

func TestValidateNativeConfigRejectsUnqualifiedOptions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*v1beta1.Migration)
	}{
		{"instance mode", func(m *v1beta1.Migration) { m.Spec.Clone.AllDatabases = true }},
		{"roles", func(m *v1beta1.Migration) { m.Spec.Clone.Roles = true }},
		{"role passwords", func(m *v1beta1.Migration) { m.Spec.Clone.NoRolePasswords = true }},
		{"drop", func(m *v1beta1.Migration) { m.Spec.Clone.DropIfExists = true }},
		{"owner", func(m *v1beta1.Migration) { m.Spec.Clone.NoOwner = true }},
		{"handover", func(m *v1beta1.Migration) { m.Spec.Clone.OwnerAfterRestore = "native_owner" }},
		{"acl", func(m *v1beta1.Migration) { m.Spec.Clone.NoACL = true }},
		{"comments", func(m *v1beta1.Migration) { m.Spec.Clone.NoComments = true }},
		{"tablespaces", func(m *v1beta1.Migration) { m.Spec.Clone.NoTablespaces = true }},
		{"deferred indexes", func(m *v1beta1.Migration) { m.Spec.Clone.DeferIndexes = true }},
		{"size estimates", func(m *v1beta1.Migration) { m.Spec.Clone.EstimateTableSizes = true }},
		{"fail fast", func(m *v1beta1.Migration) { m.Spec.Clone.FailFast = true }},
		{"filters", func(m *v1beta1.Migration) { m.Spec.Clone.Filters = &v1beta1.Filters{ExcludeSchemas: []string{"audit"}} }},
		{"skips", func(m *v1beta1.Migration) { m.Spec.Clone.Skip = []v1beta1.SkipOption{"vacuum"} }},
		{"unknown skip", func(m *v1beta1.Migration) { m.Spec.Clone.Skip = []v1beta1.SkipOption{"unknown"} }},
		{"index jobs", func(m *v1beta1.Migration) { m.Spec.Clone.IndexJobs = 2 }},
		{"restore jobs", func(m *v1beta1.Migration) { m.Spec.Clone.RestoreJobs = 2 }},
		{"large object jobs", func(m *v1beta1.Migration) { m.Spec.Clone.LargeObjectsJobs = 2 }},
		{"negative jobs", func(m *v1beta1.Migration) { m.Spec.Clone.TableJobs = -1 }},
		{"negative part cap", func(m *v1beta1.Migration) { m.Spec.Clone.SplitMaxParts = -1 }},
		{"negative split size", func(m *v1beta1.Migration) {
			size := resource.MustParse("-1")
			m.Spec.Clone.SplitTablesLargerThan = &size
		}},
		{"source startup override", func(m *v1beta1.Migration) { m.Spec.Source.SynchronousCommit = "local" }},
		{"apply grouping", func(m *v1beta1.Migration) {
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, ApplyGroupTransactions: 2}
		}},
		{"custom publication", func(m *v1beta1.Migration) {
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Publication: testExternalResourceName}
		}},
		{"no-op updates", func(m *v1beta1.Migration) {
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, ReplayNoOpUpdates: true}
		}},
		{"wal2json", func(m *v1beta1.Migration) { m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: "wal2json"} }},
		{"test decoding", func(m *v1beta1.Migration) {
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: "test_decoding"}
		}},
		{"rehearsal", func(m *v1beta1.Migration) { m.Spec.Verification = &v1beta1.VerificationOptions{Rehearsal: true} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := nativeMigration()
			tc.mutate(m)
			before := m.DeepCopy()
			caps := append(cloneCapabilities(), native.CapPGOutput, native.CapCutover, native.CapVerify, native.CapCleanup)
			if err := validateNativeConfig(m, caps); err == nil {
				t.Fatal("accepted unqualified configuration")
			}
			if !reflect.DeepEqual(m, before) {
				t.Fatal("validation mutated Migration")
			}
		})
	}
}

func TestValidateNativeConfigAllowsQualifiedCopyControls(t *testing.T) {
	m := nativeMigration()
	no := false
	zero := resource.MustParse("0")
	m.Spec.Clone = v1beta1.CloneOptions{TableJobs: 8, SplitMaxParts: 4, SplitTablesLargerThan: &zero, UseCopyBinary: &no, Skip: []v1beta1.SkipOption{testSkipLargeObjects}}
	if err := validateNativeConfig(m, []native.Capability{native.CapCloneSingleDB, native.CapProgress}); err != nil {
		t.Fatalf("disabled optional copy features still required capabilities: %v", err)
	}
	threshold := resource.MustParse("1Gi")
	m.Spec.Clone.SplitTablesLargerThan = &threshold
	if err := validateNativeConfig(m, []native.Capability{native.CapCloneSingleDB, native.CapProgress}); err == nil {
		t.Fatal("enabled splitting without qualification")
	}
	if err := validateNativeConfig(m, []native.Capability{native.CapCloneSingleDB, native.CapProgress, native.CapSplit}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateNativeConfigGatesFollowAndVerification(t *testing.T) {
	m := nativeMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
	if err := validateNativeConfig(m, cloneCapabilities()); err == nil {
		t.Fatal("follow defaulted to an unqualified plugin")
	}
	caps := append(cloneCapabilities(), native.CapPGOutput, native.CapCutover, native.CapCleanup)
	if err := validateNativeConfig(m, caps); err != nil {
		t.Fatal(err)
	}
	m.Spec.Verification = &v1beta1.VerificationOptions{Data: true}
	if err := validateNativeConfig(m, caps); err == nil {
		t.Fatal("accepted unqualified verification")
	}
	if err := validateNativeConfig(m, append(caps, native.CapVerify)); err != nil {
		t.Fatal(err)
	}
}

func TestValidateNativeConfigRequiresEachFollowCapability(t *testing.T) {
	m := nativeMigration()
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
	for _, omitted := range []native.Capability{native.CapPGOutput, native.CapCutover, native.CapCleanup} {
		caps := cloneCapabilities()
		for _, c := range []native.Capability{native.CapPGOutput, native.CapCutover, native.CapCleanup} {
			if c != omitted {
				caps = append(caps, c)
			}
		}
		if err := validateNativeConfig(m, caps); err == nil {
			t.Fatalf("accepted follow without %s", omitted)
		}
	}
}

func TestValidateNativeConfigRejectsInvalidDiscoveryCapabilities(t *testing.T) {
	for _, caps := range [][]native.Capability{
		nil,
		{},
		append(cloneCapabilities(), "unknown"),
		append(cloneCapabilities(), native.CapProgress),
	} {
		if err := validateNativeConfig(nativeMigration(), caps); err == nil {
			t.Fatalf("accepted invalid discovery capabilities: %v", caps)
		}
	}
}

func TestQualifiedDiscoveryCapabilitiesAdmitOnlyNarrowedProfiles(t *testing.T) {
	raw, err := os.ReadFile("../native/testdata/v1/payloads/version-qualified-profile.json")
	if err != nil {
		t.Fatal(err)
	}
	version, err := native.ParseVersion(raw)
	if err != nil {
		t.Fatal(err)
	}
	cloneRequired := []native.Capability{native.CapCloneSingleDB, native.CapProgress}
	followRequired := append([]native.Capability{native.CapPGOutput, native.CapCutover, native.CapCleanup}, cloneRequired...)
	for _, tc := range []struct {
		name         string
		follow       bool
		verification *v1beta1.VerificationOptions
		required     []native.Capability
	}{
		{name: "narrowed clone", required: cloneRequired},
		{name: "follow", follow: true, required: followRequired},
		{name: "schema verification", verification: &v1beta1.VerificationOptions{Schema: true}, required: append(cloneRequired, native.CapVerify)},
		{name: "data verification", verification: &v1beta1.VerificationOptions{Data: true}, required: append(cloneRequired, native.CapVerify)},
		{name: "verified follow", follow: true, verification: &v1beta1.VerificationOptions{Schema: true, Data: true}, required: append(followRequired, native.CapVerify)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := nativeMigration()
			no := false
			zero := resource.MustParse("0")
			m.Spec.Clone = v1beta1.CloneOptions{SplitTablesLargerThan: &zero, UseCopyBinary: &no, Skip: []v1beta1.SkipOption{testSkipLargeObjects}}
			if tc.follow {
				m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
			}
			m.Spec.Verification = tc.verification
			before := m.DeepCopy()
			if err := validateNativeConfig(m, version.Capabilities); err != nil {
				t.Fatalf("qualified discovery refused narrowed profile: %v", err)
			}
			for _, omitted := range tc.required {
				caps := make([]native.Capability, 0, len(version.Capabilities)-1)
				for _, capability := range version.Capabilities {
					if capability != omitted {
						caps = append(caps, capability)
					}
				}
				if err := validateNativeConfig(m, caps); err == nil {
					t.Fatalf("accepted profile without %s", omitted)
				}
			}
			for _, control := range []struct {
				name   string
				mutate func(*v1beta1.Migration)
			}{
				{"default binary COPY", func(m *v1beta1.Migration) { m.Spec.Clone.UseCopyBinary = nil }},
				{"default splitting", func(m *v1beta1.Migration) { m.Spec.Clone.SplitTablesLargerThan = nil }},
				{"default large objects", func(m *v1beta1.Migration) { m.Spec.Clone.Skip = nil }},
			} {
				t.Run(control.name, func(t *testing.T) {
					candidate := m.DeepCopy()
					control.mutate(candidate)
					if err := validateNativeConfig(candidate, version.Capabilities); err == nil {
						t.Fatal("qualified discovery admitted unqualified default")
					}
				})
			}
			if !reflect.DeepEqual(m, before) {
				t.Fatal("validation mutated Migration")
			}
		})
	}
	if err := validateNativeConfig(nativeMigration(), version.Capabilities); err == nil {
		t.Fatal("qualified discovery admitted the default clone profile")
	}
}

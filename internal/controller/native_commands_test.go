package controller

import (
	"reflect"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	nativeTestNamespace = "namespace"
	nativeTestRunID     = "0123456789abcdef0123456789abcdef"
)

func TestEngineExecutable(t *testing.T) {
	for _, tc := range []struct {
		engine v1beta1.MigrationEngine
		want   string
	}{
		{"", workerContainer},
		{v1beta1.MigrationEnginePGCopyDB, workerContainer},
		{v1beta1.MigrationEngineRPGMigrate, "/usr/local/bin/rpgmigrate"},
		{testUnrecognizedEngine, ""},
	} {
		m := &v1beta1.Migration{Spec: v1beta1.MigrationSpec{Engine: tc.engine}}
		got, err := engineExecutable(m)
		if got != tc.want || (err != nil) != (tc.want == "") {
			t.Fatalf("engine %q: executable = %q, %v", tc.engine, got, err)
		}
	}
	if _, err := engineExecutable(nil); err == nil {
		t.Fatal("absent Migration selected an executable")
	}
}

func nativeIdentityObjects() (*v1beta1.Migration, *corev1.PersistentVolumeClaim) {
	m := nativeMigration()
	m.Name, m.Namespace = "migration", nativeTestNamespace
	m.UID = "01234567-89ab-cdef-0123-456789abcdef"
	controller := true
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: workPVCName(m), Namespace: m.Namespace, UID: "pvc-owned",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: v1beta1.GroupVersion.String(), Kind: "Migration",
			Name: m.Name, UID: m.UID, Controller: &controller,
		}},
	}}
	return m, pvc
}

func TestNativeWorkIdentity(t *testing.T) {
	m, pvc := nativeIdentityObjects()
	want := native.WorkIdentity{
		MigrationUID: string(m.UID), RunID: nativeTestRunID,
		WorkdirID: string(pvc.UID), CheckpointGeneration: 1,
	}
	beforeM, beforePVC := m.DeepCopy(), pvc.DeepCopy()
	got, err := nativeWorkIdentity(m, pvc)
	if err != nil || got != want {
		t.Fatalf("identity = %+v, %v", got, err)
	}
	if !reflect.DeepEqual(beforeM, m) || !reflect.DeepEqual(beforePVC, pvc) {
		t.Fatal("identity construction mutated an object")
	}
	pvc.UID = "replacement-pvc"
	replacement, err := nativeWorkIdentity(m, pvc)
	if err != nil || replacement == got || replacement.WorkdirID != string(pvc.UID) {
		t.Fatalf("replacement PVC did not change caller binding: %+v, %v", replacement, err)
	}
}

func TestNativeWorkIdentityRejectsZeroRun(t *testing.T) {
	m, pvc := nativeIdentityObjects()
	m.UID = "00000000-0000-0000-0000-000000000000"
	pvc.OwnerReferences[0].UID = m.UID
	beforeM, beforePVC := m.DeepCopy(), pvc.DeepCopy()
	work, err := nativeWorkIdentity(m, pvc)
	if err == nil || err.Error() != "native Migration UID must be nonzero" || work != (native.WorkIdentity{}) {
		t.Fatalf("zero run identity = %+v, %v", work, err)
	}
	if !reflect.DeepEqual(beforeM, m) || !reflect.DeepEqual(beforePVC, pvc) {
		t.Fatal("zero identity refusal mutated an object")
	}
	m.UID = "00000000-0000-0000-0000-000000000001"
	pvc.OwnerReferences[0].UID = m.UID
	want := native.WorkIdentity{
		MigrationUID: string(m.UID), RunID: "00000000000000000000000000000001",
		WorkdirID: string(pvc.UID), CheckpointGeneration: 1,
	}
	if work, err := nativeWorkIdentity(m, pvc); err != nil || work != want {
		t.Fatalf("nonzero boundary identity = %+v, %v", work, err)
	}
}

func TestNativeWorkIdentityRejectsUnboundObjects(t *testing.T) {
	const otherNamespace = "other"
	for _, tc := range []struct {
		name   string
		mutate func(**v1beta1.Migration, **corev1.PersistentVolumeClaim)
	}{
		{"absent migration", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) { *m = nil }},
		{"absent pvc", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) { *pvc = nil }},
		{"legacy engine", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) { (*m).Spec.Engine = "" }},
		{"missing migration uid", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) { (*m).UID = "" }},
		{"uppercase uuid", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) {
			(*m).UID = types.UID(strings.ToUpper(string((*m).UID)))
		}},
		{"compact uuid", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) {
			(*m).UID = nativeTestRunID
		}},
		{"bad uuid digit", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) {
			(*m).UID = "g1234567-89ab-cdef-0123-456789abcdef"
		}},
		{"bad uuid hyphen", func(m **v1beta1.Migration, _ **corev1.PersistentVolumeClaim) {
			(*m).UID = "0123456-789ab-cdef-0123-456789abcdef"
		}},
		{"missing pvc uid", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) { (*pvc).UID = "" }},
		{"long pvc uid", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) {
			(*pvc).UID = types.UID(strings.Repeat("x", 257))
		}},
		{"pvc uid whitespace", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) { (*pvc).UID = " pvc" }},
		{"pvc uid control", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) { (*pvc).UID = "pvc\nuid" }},
		{"pvc uid path", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) { (*pvc).UID = "pvc/uid" }},
		{"pvc uid backslash", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) { (*pvc).UID = `pvc\uid` }},
		{"pvc uid invalid utf8", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) { (*pvc).UID = types.UID("pvc\xff") }},
		{"wrong namespace", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) { (*pvc).Namespace = otherNamespace }},
		{"wrong pvc name", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) { (*pvc).Name = "foreign-work" }},
		{"missing owner", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) { (*pvc).OwnerReferences = nil }},
		{"wrong owner uid", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) {
			(*pvc).OwnerReferences[0].UID = "foreign"
		}},
		{"wrong owner name", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) {
			(*pvc).OwnerReferences[0].Name = "foreign"
		}},
		{"wrong owner kind", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) {
			(*pvc).OwnerReferences[0].Kind = "Job"
		}},
		{"wrong owner group", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) {
			(*pvc).OwnerReferences[0].APIVersion = "other/v1beta1"
		}},
		{"noncontroller owner", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) {
			(*pvc).OwnerReferences[0].Controller = nil
		}},
		{"deleting pvc", func(_ **v1beta1.Migration, pvc **corev1.PersistentVolumeClaim) {
			now := metav1.NewTime(time.Now())
			(*pvc).DeletionTimestamp = &now
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, pvc := nativeIdentityObjects()
			tc.mutate(&m, &pvc)
			if work, err := nativeWorkIdentity(m, pvc); err == nil || work != (native.WorkIdentity{}) {
				t.Fatalf("invalid identity = %+v, %v", work, err)
			}
		})
	}
}

func TestNativeIdentityEnv(t *testing.T) {
	w := native.WorkIdentity{MigrationUID: "01234567-89ab-cdef-0123-456789abcdef", RunID: nativeTestRunID, WorkdirID: "pvc-owned", CheckpointGeneration: 1}
	want := []corev1.EnvVar{
		{Name: "RPGMIGRATE_MIGRATION_UID", Value: w.MigrationUID},
		{Name: "RPGMIGRATE_RUN_ID", Value: w.RunID},
		{Name: "RPGMIGRATE_WORKDIR_ID", Value: w.WorkdirID},
		{Name: "RPGMIGRATE_CHECKPOINT_GENERATION", Value: "1"},
	}
	if got := nativeIdentityEnv(w); !reflect.DeepEqual(got, want) {
		t.Fatalf("identity env = %+v", got)
	}
}

func TestNativeCloneArgs(t *testing.T) {
	defaults := "clone --dir /work/pgcopydb --table-jobs 4 --split-tables-larger-than 536870912 --split-max-parts 8 --use-copy-binary"
	narrow := "clone --dir /work/pgcopydb --table-jobs 4 --skip-large-objects"
	narrowProfile := func(m *v1beta1.Migration) {
		no, zero := false, resource.MustParse("0")
		m.Spec.Clone = v1beta1.CloneOptions{SplitTablesLargerThan: &zero, UseCopyBinary: &no, Skip: []v1beta1.SkipOption{testSkipLargeObjects}}
	}
	for _, tc := range []struct {
		name    string
		attempt int32
		mutate  func(*v1beta1.Migration)
		want    string
	}{
		{"first attempt", 1, nil, defaults + " --restart"},
		{"retry", 2, nil, defaults + " --resume --not-consistent"},
		{"narrow first", 1, narrowProfile, narrow + " --restart"},
		{"narrow retry", 2, narrowProfile, narrow + " --resume --not-consistent"},
		{"narrow follow first", 1, func(m *v1beta1.Migration) {
			narrowProfile(m)
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true}
		}, narrow + " --restart --follow --plugin pgoutput"},
		{"narrow follow retry", 2, func(m *v1beta1.Migration) {
			narrowProfile(m)
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		}, narrow + " --resume --not-consistent --follow --plugin pgoutput"},
		{"effective zero", 1, func(m *v1beta1.Migration) {
			narrowProfile(m)
			zero := resource.MustParse("0m")
			m.Spec.Clone.SplitTablesLargerThan = &zero
		}, narrow + " --restart"},
		{"explicit zero retains other defaults", 1, func(m *v1beta1.Migration) {
			zero := resource.MustParse("0")
			m.Spec.Clone.SplitTablesLargerThan = &zero
		}, "clone --dir /work/pgcopydb --table-jobs 4 --use-copy-binary --restart"},
		{"positive split", 1, func(m *v1beta1.Migration) {
			size := resource.MustParse("1Gi")
			m.Spec.Clone.SplitTablesLargerThan, m.Spec.Clone.SplitMaxParts = &size, 3
		}, strings.Replace(defaults, "536870912 --split-max-parts 8", "1073741824 --split-max-parts 3", 1) + " --restart"},
		{"default split explicit parts", 1, func(m *v1beta1.Migration) {
			m.Spec.Clone.SplitMaxParts = 3
		}, strings.Replace(defaults, "--split-max-parts 8", "--split-max-parts 3", 1) + " --restart"},
		{"follow", 1, func(m *v1beta1.Migration) { m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true} }, defaults + " --restart --follow --plugin pgoutput"},
		{"explicit pgoutput", 2, func(m *v1beta1.Migration) {
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: pgoutputPlugin}
		}, defaults + " --resume --not-consistent --follow --plugin pgoutput"},
		{"runner cpu", 1, func(m *v1beta1.Migration) {
			m.Spec.Runner.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8500m")}
		}, strings.Replace(defaults, "--table-jobs 4", "--table-jobs 9", 1) + " --restart"},
		{"explicit controls", 1, func(m *v1beta1.Migration) {
			no, zero := false, resource.MustParse("0")
			m.Spec.Clone = v1beta1.CloneOptions{TableJobs: 7, SplitMaxParts: 3, SplitTablesLargerThan: &zero, UseCopyBinary: &no, Skip: []v1beta1.SkipOption{testSkipLargeObjects}}
		}, "clone --dir /work/pgcopydb --table-jobs 7 --skip-large-objects --restart"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := nativeMigration()
			if tc.mutate != nil {
				tc.mutate(m)
			}
			before := m.DeepCopy()
			got, err := nativeCloneArgs(m, tc.attempt)
			if err != nil || strings.Join(got, " ") != tc.want {
				t.Fatalf("clone argv = %q, %v; want %q", got, err, tc.want)
			}
			if !reflect.DeepEqual(before, m) {
				t.Fatal("argument construction mutated Migration")
			}
		})
	}
}

func TestNativeCloneArgsRejectsUnsupportedProfile(t *testing.T) {
	for _, tc := range []struct {
		name    string
		attempt int32
		mutate  func(*v1beta1.Migration)
	}{
		{"zero attempt", 0, nil}, {"negative attempt", -1, nil},
		{"legacy", 1, func(m *v1beta1.Migration) { m.Spec.Engine = "" }},
		{"instance", 1, func(m *v1beta1.Migration) { m.Spec.Clone.AllDatabases = true }},
		{"legacy drop", 2, func(m *v1beta1.Migration) { m.Spec.Clone.DropIfExists = true }},
		{"negative split parts", 1, func(m *v1beta1.Migration) {
			zero := resource.MustParse("0")
			m.Spec.Clone.SplitTablesLargerThan, m.Spec.Clone.SplitMaxParts = &zero, -1
		}},
		{"negative split threshold", 1, func(m *v1beta1.Migration) {
			size := resource.MustParse("-1")
			m.Spec.Clone.SplitTablesLargerThan = &size
		}},
		{"custom slot", 1, func(m *v1beta1.Migration) {
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, SlotName: testExternalResourceName}
		}},
		{"custom publication", 1, func(m *v1beta1.Migration) {
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Publication: testExternalResourceName}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := nativeMigration()
			if tc.mutate != nil {
				tc.mutate(m)
			}
			if args, err := nativeCloneArgs(m, tc.attempt); err == nil || args != nil {
				t.Fatalf("unsupported argv = %q, %v", args, err)
			}
		})
	}
	if _, err := nativeCloneArgs(nil, 1); err == nil {
		t.Fatal("absent Migration constructed clone arguments")
	}
}

func TestNativeCloneArgsKeepsConnectionsOutOfArgv(t *testing.T) {
	m := nativeMigration()
	m.Spec.Source.SecretRef = &v1beta1.ConnectionSecret{Name: "source-secret"}
	m.Spec.Target.SecretRef = &v1beta1.ConnectionSecret{Name: "target-secret"}
	args, err := nativeCloneArgs(m, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, arg := range args {
		if strings.Contains(arg, "secret") || arg == "--source" || arg == "--target" {
			t.Fatalf("connection material in argv: %q", args)
		}
	}
}

func TestNativeCleanupArgsRefusesMissingOwnedState(t *testing.T) {
	for _, m := range []*v1beta1.Migration{nil, nativeMigration(), {Spec: v1beta1.MigrationSpec{}}} {
		if m != nil {
			m.Status.Replication = &v1beta1.ReplicationStatus{SlotName: "legacy-name"}
		}
		if args, err := nativeCleanupArgs(m); err == nil || args != nil {
			t.Fatalf("cleanup without independently owned state = %q, %v", args, err)
		}
	}
}

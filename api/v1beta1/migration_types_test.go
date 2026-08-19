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

package v1beta1

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func secretRef(name, key string) *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: key}
}

// fullMigration populates every field of the API surface, pointers, slices,
// and maps included. The deepcopy test below depends on that: a field added
// to the types but missing from a stale zz_generated.deepcopy.go only fails
// the equality check when it carries a value here.
func fullMigration() *Migration {
	split := resource.MustParse("2Gi")
	lag := resource.MustParse("32Mi")
	sc := "fast-ssd"
	ttl := int32(600)
	lagBytes := int64(1234)
	// Fixed so two fullMigration() calls compare equal.
	now := metav1.NewTime(time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC))
	return &Migration{
		TypeMeta:   metav1.TypeMeta{Kind: "Migration", APIVersion: GroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{Name: "full", Namespace: "ns", Labels: map[string]string{"a": "b"}},
		Spec: MigrationSpec{
			Source: PostgresConnection{
				Host: "src", Port: 5433, Database: "app", Username: "mig",
				PasswordSecretRef: secretRef("src-cred", "password"),
				SSLMode:           "verify-full",
				TLS: &TLSSecretRefs{
					RootCA: secretRef("ca", "ca.crt"),
					Cert:   secretRef("client", "tls.crt"),
					Key:    secretRef("client", "tls.key"),
				},
				SuperuserSecretRef: &ConnectionSecret{
					Name: "src-admin", Endpoint: "internal",
					Keys: &ConnectionSecretKeys{Username: "admin", Password: "adminpw"},
				},
			},
			Target: PostgresConnection{URISecretRef: secretRef("dsn", "uri")},
			Clone: CloneOptions{
				TableJobs: 4, IndexJobs: 4, RestoreJobs: 2, LargeObjectsJobs: 2,
				SplitTablesLargerThan: &split, SplitMaxParts: 8,
				EstimateTableSizes: true, DropIfExists: true, Roles: true,
				NoRolePasswords: true, NoOwner: true, NoACL: true, NoComments: true,
				NoTablespaces: true, UseCopyBinary: true, FailFast: true,
				Skip: []SkipOption{"largeObjects", "analyze"},
				Filters: &Filters{
					IncludeOnlyTables:     []string{"public.a"},
					ExcludeTables:         []string{"public.b"},
					IncludeOnlySchemas:    []string{"public"},
					ExcludeSchemas:        []string{"audit"},
					ExcludeIndexes:        []string{"public.idx"},
					ExcludeTableData:      []string{"public.big"},
					IncludeOnlyExtensions: []string{"citext"},
					ExcludeExtensions:     []string{"postgis"},
				},
			},
			Follow: &FollowOptions{
				Enabled: true, Plugin: "wal2json", SlotName: "slot_1",
				Publication: "pub", Wal2jsonNumericAsString: true,
				ReplayNoOpUpdates:           true,
				AllowMissingReplicaIdentity: []string{"public.log"},
				MaxCatchupLag:               &lag,
			},
			Cutover:      CutoverSpec{Mode: CutoverAutomatic, Approved: true},
			Verification: &VerificationOptions{Schema: true, Data: true},
			WorkVolume:   WorkVolume{StorageClassName: &sc, Size: resource.MustParse("20Gi")},
			Runner: RunnerSpec{
				Image: "runner:test",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
				},
				NodeSelector: map[string]string{"zone": "a"},
				Tolerations:  []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
				Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: []corev1.NodeSelectorRequirement{{
								Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"},
							}},
						}},
					},
				}},
			},
			Suspend:                 true,
			BackoffLimit:            2,
			TTLSecondsAfterFinished: &ttl,
		},
		Status: MigrationStatus{
			ObservedGeneration: 3,
			Phase:              PhaseStreaming,
			Conditions: []metav1.Condition{{
				Type: ConditionStreaming, Status: metav1.ConditionTrue,
				Reason: "Replaying", Message: "applying", LastTransitionTime: now,
			}},
			Attempts: 2,
			Progress: &CloneProgress{
				TablesTotal: 10, TablesDone: 5, IndexesTotal: 4, IndexesDone: 1,
				BytesTotal: resource.NewQuantity(1024, resource.BinarySI),
				BytesDone:  resource.NewQuantity(512, resource.BinarySI),
			},
			Replication: &ReplicationStatus{
				SlotName: "slot_1", WriteLSN: "0/20", ReplayLSN: "0/10",
				Endpos: "0/30", LagBytes: &lagBytes,
			},
			JobName:     "full-run-2",
			StartedAt:   &now,
			CompletedAt: &now,
		},
	}
}

// mutated overwrites copy fields in the aliasing check below.
const mutated = "mutated"

// TestDeepCopy_EqualAndUnaliased pins the deepcopy contract for every API
// type: the copy is semantically equal (a stale zz_generated after adding a
// field fails here, because the field's value would be dropped) and shares no
// mutable memory with the original (a shallow copy fails the mutation check).
func TestDeepCopy_EqualAndUnaliased(t *testing.T) {
	m := fullMigration()
	list := &MigrationList{
		TypeMeta: metav1.TypeMeta{Kind: "MigrationList", APIVersion: GroupVersion.String()},
		Items:    []Migration{*m},
	}
	objs := []any{
		m, list, &m.Spec, &m.Status,
		&m.Spec.Source, m.Spec.Source.TLS, &m.Spec.Clone, m.Spec.Clone.Filters,
		m.Spec.Follow, &m.Spec.Cutover, m.Spec.Verification,
		&m.Spec.WorkVolume, &m.Spec.Runner,
		m.Status.Progress, m.Status.Replication,
	}
	for _, obj := range objs {
		v := reflect.ValueOf(obj)
		out := v.MethodByName("DeepCopy").Call(nil)[0]
		if !apiequality.Semantic.DeepEqual(obj, out.Interface()) {
			t.Errorf("%T: DeepCopy is not equal to the original", obj)
		}
		if out.Pointer() == v.Pointer() {
			t.Errorf("%T: DeepCopy returned the original", obj)
		}
	}

	// DeepCopyObject is what client-go machinery (informers, workqueues)
	// calls; it must keep the concrete types.
	if _, ok := m.DeepCopyObject().(*Migration); !ok {
		t.Error("Migration.DeepCopyObject lost its type")
	}
	if _, ok := list.DeepCopyObject().(*MigrationList); !ok {
		t.Error("MigrationList.DeepCopyObject lost its type")
	}

	// Mutate one field of every reference kind in the copy; the original must
	// not move. Compared against a fresh fullMigration so any aliasing shows.
	c := m.DeepCopy()
	c.Spec.Source.PasswordSecretRef.Key = mutated
	c.Spec.Source.TLS.RootCA.Name = mutated
	c.Spec.Clone.Skip[0] = mutated
	c.Spec.Clone.Filters.ExcludeTables[0] = mutated
	*c.Spec.Clone.SplitTablesLargerThan = resource.MustParse("9Gi")
	c.Spec.Follow.AllowMissingReplicaIdentity[0] = mutated
	*c.Spec.Follow.MaxCatchupLag = resource.MustParse("9Gi")
	c.Spec.Runner.NodeSelector["zone"] = mutated
	c.Spec.Runner.Tolerations[0].Key = mutated
	*c.Spec.TTLSecondsAfterFinished = 1
	c.Status.Conditions[0].Reason = mutated
	*c.Status.Progress.BytesDone = resource.MustParse("9Gi")
	*c.Status.Replication.LagBytes = 9
	if !apiequality.Semantic.DeepEqual(m, fullMigration()) {
		t.Error("mutating the copy changed the original: DeepCopy aliases memory")
	}
}

// TestFiltersIsEmpty walks the Filters struct by reflection so a section
// added to the type without extending IsEmpty fails here: RenderFilters keys
// off IsEmpty, and a forgotten section would silently render no INI.
func TestFiltersIsEmpty(t *testing.T) {
	if !(*Filters)(nil).IsEmpty() {
		t.Error("nil Filters must be empty")
	}
	if !(&Filters{}).IsEmpty() {
		t.Error("zero Filters must be empty")
	}
	var f Filters
	v := reflect.ValueOf(&f).Elem()
	for i := range v.NumField() {
		v.Field(i).Set(reflect.ValueOf([]string{"x"}))
		if f.IsEmpty() {
			t.Errorf("IsEmpty ignores %s", v.Type().Field(i).Name)
		}
		v.Field(i).SetZero()
	}
}

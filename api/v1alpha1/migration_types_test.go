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

package v1alpha1

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// TestAddToScheme pins the registration cmd/main depends on: both kinds
// land in a fresh scheme under this group-version.

var copyBinary = true

func TestAddToScheme(t *testing.T) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	for _, kind := range []string{"Migration", "MigrationList"} {
		if !s.Recognizes(GroupVersion.WithKind(kind)) {
			t.Errorf("scheme does not recognize %s", kind)
		}
	}
}

func secretRef(name, key string) *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: key}
}

// fullMigration populates every field to catch stale generated deepcopy code.
// It intentionally combines mutually exclusive options and MUST NOT be used for admission tests.
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
			Target: PostgresConnection{URISecretRef: secretRef("dsn", "uri"), SynchronousCommit: SynchronousCommitLocal},
			Clone: CloneOptions{
				AllDatabases: true,
				TableJobs:    4, IndexJobs: 4, RestoreJobs: 2, LargeObjectsJobs: 2,
				SplitTablesLargerThan: &split, SplitMaxParts: 8,
				EstimateTableSizes: true, DropIfExists: true, Roles: true,
				NoRolePasswords: true, NoOwner: true, NoACL: true, NoComments: true,
				NoTablespaces: true, UseCopyBinary: &copyBinary, FailFast: true,
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
				OwnerAfterRestore: "app_owner",
			},
			Follow: &FollowOptions{
				Enabled: true, Plugin: "wal2json", SlotName: "slot_1",
				Publication: "pub", Wal2jsonNumericAsString: true,
				ReplayNoOpUpdates:           true,
				ApplyGroupTransactions:      50,
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
			DryRun:                  true,
			Preflight:               &PreflightOptions{RequireSameMajorVersion: true},
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
				ObservedAt: &now,
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
	c.Status.Progress.ObservedAt.Time = time.Unix(1, 0)
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

// TestTargetImmutabilityRuleCoversEveryField closes the gap the per-field
// target rule leaves open: a field added to PostgresConnection would otherwise
// be mutable on the target without anyone deciding so.
func TestTargetImmutabilityRuleCoversEveryField(t *testing.T) {
	rules := specFieldRules(t, GroupVersion.Version, "target")
	// Every rule with this message, joined, so a CEL cost split keeps the test true.
	rule := strings.Join(rulesWithMessage(t, rules, "target is immutable except synchronousCommit"), " && ")
	sawSyncCommit := false
	for _, tag := range jsonTags(reflect.TypeFor[PostgresConnection]()) {
		if tag == "synchronousCommit" {
			sawSyncCommit = true
			if strings.Contains(rule, "self.synchronousCommit") {
				t.Errorf("the immutability rule must not compare synchronousCommit")
			}
			continue
		}
		want := fmt.Sprintf("has(self.%[1]s) == has(oldSelf.%[1]s) && (!has(self.%[1]s) || self.%[1]s == oldSelf.%[1]s)", tag)
		if !strings.Contains(rule, want) {
			t.Errorf("target rule misses %q", tag)
		}
	}
	if !sawSyncCommit {
		t.Error("PostgresConnection has no synchronousCommit JSON tag")
	}
	uri := ruleWithMessage(t, rules, "synchronousCommit cannot be combined with uriSecretRef: the operator never reads that Secret, so it cannot add the option; use secretRef or the inline fields")
	if uri != "!(has(self.synchronousCommit) && has(self.uriSecretRef))" {
		t.Errorf("target uriSecretRef rule = %q", uri)
	}
	src := ruleWithMessage(t, specFieldRules(t, GroupVersion.Version, "source"), "synchronousCommit applies to the target only")
	if src != "!has(self.synchronousCommit)" {
		t.Errorf("source rule = %q", src)
	}
}

// celRule is one x-kubernetes-validations entry of the generated CRD.
type celRule struct{ rule, message string }

// specFieldRules reads the CEL rules on spec.<field> of one served version
// from the generated CRD, the schema real clusters enforce.
func specFieldRules(t *testing.T, version, field string) []celRule {
	t.Helper()
	raw, err := os.ReadFile("../../config/crd/bases/pgcopydb-operator.io_migrations.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatal(err)
	}
	versions, _ := dig(crd, "spec", "versions").([]any)
	for _, v := range versions {
		if dig(v, "name") != version {
			continue
		}
		entries, ok := dig(v, "schema", "openAPIV3Schema", "properties", "spec", "properties", field,
			"x-kubernetes-validations").([]any)
		if !ok {
			t.Fatalf("CRD %s spec.%s has no x-kubernetes-validations", version, field)
		}
		rules := make([]celRule, 0, len(entries))
		for _, e := range entries {
			rule, _ := dig(e, "rule").(string)
			message, _ := dig(e, "message").(string)
			rules = append(rules, celRule{rule: rule, message: message})
		}
		return rules
	}
	t.Fatalf("CRD has no version %s", version)
	return nil
}

// dig walks nested YAML maps; a missing key yields nil.
func dig(v any, path ...string) any {
	for _, key := range path {
		m, _ := v.(map[string]any)
		v = m[key]
	}
	return v
}

// rulesWithMessage returns every rule carrying message, and fails when none does.
func rulesWithMessage(t *testing.T, rules []celRule, message string) []string {
	t.Helper()
	var out []string
	for _, r := range rules {
		if r.message == message {
			out = append(out, r.rule)
		}
	}
	if len(out) == 0 {
		t.Fatalf("no rule with message %q", message)
	}
	return out
}

// ruleWithMessage returns the one rule carrying message.
func ruleWithMessage(t *testing.T, rules []celRule, message string) string {
	t.Helper()
	out := rulesWithMessage(t, rules, message)
	if len(out) != 1 {
		t.Fatalf("%d rules with message %q, want 1", len(out), message)
	}
	return out[0]
}

// jsonTags lists the JSON field names of a struct type.
func jsonTags(typ reflect.Type) []string {
	tags := make([]string, 0, typ.NumField())
	for field := range typ.Fields() {
		tags = append(tags, strings.Split(field.Tag.Get("json"), ",")[0])
	}
	return tags
}

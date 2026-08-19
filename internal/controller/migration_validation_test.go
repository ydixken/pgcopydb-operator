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
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
)

// Exercises the CRD CEL rules through the envtest apiserver: the schema in
// config/crd/bases is what real clusters enforce, so this is where a broken
// rule would surface.
var _ = Describe("Migration CRD validation", func() {
	ctx := context.Background()

	It("rejects wal2jsonNumericAsString under any non-wal2json plugin", func() {
		m := validMigration("val-wal2json-reject")
		// Plugin left empty: defaulting fills in pgoutput before validation.
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Wal2jsonNumericAsString: true}
		err := k8sClient.Create(ctx, m)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("set plugin: wal2json"))
	})

	It("accepts wal2jsonNumericAsString with plugin wal2json", func() {
		m := validMigration("val-wal2json-accept")
		m.Spec.Follow = &v1beta1.FollowOptions{
			Enabled: true, Plugin: v1beta1.PluginWal2json, Wal2jsonNumericAsString: true,
		}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		// Never reconciled, so no finalizer blocks the cleanup delete.
		Expect(k8sClient.Delete(ctx, m)).To(Succeed())
	})

	// One entry per admission rule, plus accept-side controls proving a rule
	// is not overbroad. wantErr "" means the spec must be admitted.
	slot := func(name string) func(*v1beta1.Migration) {
		return func(m *v1beta1.Migration) {
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, SlotName: name}
		}
	}
	filters := func(f *v1beta1.Filters) func(*v1beta1.Migration) {
		return func(m *v1beta1.Migration) { m.Spec.Clone.Filters = f }
	}
	DescribeTable("create-time rules",
		func(name string, mutate func(*v1beta1.Migration), wantErr string) {
			m := validMigration(name)
			mutate(m)
			err := k8sClient.Create(ctx, m)
			if wantErr == "" {
				Expect(err).NotTo(HaveOccurred())
				Expect(k8sClient.Delete(ctx, m)).To(Succeed())
				return
			}
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid, got: %v", err)
			Expect(err).To(MatchError(ContainSubstring(wantErr)))
		},
		Entry("connection with both inline fields and uriSecretRef", "cel-conn-both",
			func(m *v1beta1.Migration) {
				m.Spec.Source.URISecretRef = &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "dsn"}, Key: "value",
				}
			}, "set exactly one of secretRef, uriSecretRef"),
		Entry("connection with only a database field", "cel-conn-neither",
			func(m *v1beta1.Migration) {
				m.Spec.Source = v1beta1.PostgresConnection{Database: testDB}
			}, "inline form needs both host and username"),
		Entry("connection with no fields at all", "cel-conn-empty",
			func(m *v1beta1.Migration) {
				m.Spec.Source = v1beta1.PostgresConnection{}
			}, "set exactly one of secretRef, uriSecretRef"),
		Entry("connection with host but no username", "cel-conn-inline-partial",
			func(m *v1beta1.Migration) {
				m.Spec.Source = v1beta1.PostgresConnection{Host: "partial.example.com"}
			}, "inline form needs both host and username"),
		Entry("uriSecretRef-only connections on both sides", "cel-conn-uri-only",
			func(m *v1beta1.Migration) {
				uri := func(name string) v1beta1.PostgresConnection {
					return v1beta1.PostgresConnection{URISecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: "value",
					}}
				}
				m.Spec.Source = uri("src-dsn")
				m.Spec.Target = uri("tgt-dsn")
			}, ""),
		Entry("connection with both inline fields and secretRef", "cel-conn-secretref-inline",
			func(m *v1beta1.Migration) {
				m.Spec.Source.SecretRef = &v1beta1.ConnectionSecret{Name: "conn"}
			}, "set exactly one of secretRef, uriSecretRef"),
		Entry("connection with both secretRef and uriSecretRef", "cel-conn-secretref-uri",
			func(m *v1beta1.Migration) {
				m.Spec.Source = v1beta1.PostgresConnection{
					SecretRef: &v1beta1.ConnectionSecret{Name: "conn"},
					URISecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "conn-dsn"}, Key: "conninfo",
					},
				}
			}, "set exactly one of secretRef, uriSecretRef"),
		Entry("connection with secretRef and a stray inline database", "cel-conn-secretref-stray-inline",
			func(m *v1beta1.Migration) {
				m.Spec.Source = v1beta1.PostgresConnection{
					SecretRef: &v1beta1.ConnectionSecret{Name: "stray-conn"},
					Database:  testDB,
				}
			}, "set exactly one of secretRef, uriSecretRef"),
		Entry("secretRef-only connections on both sides", "cel-conn-secretref-only",
			func(m *v1beta1.Migration) {
				m.Spec.Source = v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: "src-conn"}}
				m.Spec.Target = v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: "tgt-conn"}}
			}, ""),
		Entry("superuserSecretRef next to an inline connection", "cel-conn-superuser-inline",
			func(m *v1beta1.Migration) {
				m.Spec.Source.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: "src-admin"}
			}, ""),
		Entry("superuserSecretRef next to a secretRef connection", "cel-conn-superuser-secretref",
			func(m *v1beta1.Migration) {
				m.Spec.Source = v1beta1.PostgresConnection{
					SecretRef:          &v1beta1.ConnectionSecret{Name: "super-conn"},
					SuperuserSecretRef: &v1beta1.ConnectionSecret{Name: "conn-admin"},
				}
			}, ""),
		Entry("includeOnlyTables with excludeTables", "cel-filter-tables",
			filters(&v1beta1.Filters{IncludeOnlyTables: []string{"public.t1"}, ExcludeTables: []string{"public.t2"}}),
			"includeOnlyTables cannot be combined with excludeTables or excludeSchemas"),
		Entry("includeOnlyTables with excludeSchemas", "cel-filter-tables-schemas",
			filters(&v1beta1.Filters{IncludeOnlyTables: []string{"public.t3"}, ExcludeSchemas: []string{"legacy"}}),
			"includeOnlyTables cannot be combined with excludeTables or excludeSchemas"),
		Entry("includeOnlySchemas with excludeSchemas", "cel-filter-schemas",
			filters(&v1beta1.Filters{IncludeOnlySchemas: []string{"public"}, ExcludeSchemas: []string{"scratch"}}),
			"includeOnlySchemas cannot be combined with excludeSchemas"),
		Entry("includeOnlyExtensions with excludeExtensions", "cel-filter-extensions",
			filters(&v1beta1.Filters{IncludeOnlyExtensions: []string{"citext"}, ExcludeExtensions: []string{"postgis"}}),
			"includeOnlyExtensions cannot be combined with excludeExtensions"),
		Entry("includeOnlyTables with the compatible exclusions", "cel-filter-ok",
			filters(&v1beta1.Filters{
				IncludeOnlyTables: []string{"public.t4"},
				ExcludeIndexes:    []string{"public.idx"},
				ExcludeTableData:  []string{"public.big"},
			}), ""),
		// slotName is interpolated into SQL (origin verification, publication
		// drop guard); the charset pattern is the injection barrier.
		Entry("slot name with a hyphen", "cel-slot-hyphen", slot("my-slot"), "should match"),
		Entry("slot name with uppercase", "cel-slot-upper", slot("MySlot"), "should match"),
		Entry("slot name with a quote", "cel-slot-quote", slot(`pg_'; drop table users;--`), "should match"),
		Entry("slot name at the 63-byte limit", "cel-slot-63", slot(strings.Repeat("a", 63)), ""),
		Entry("slot name over the 63-byte limit", "cel-slot-64", slot(strings.Repeat("a", 64)), "Too long"),
		Entry("skip entry outside the allowlist", "cel-skip-unknown",
			func(m *v1beta1.Migration) { m.Spec.Clone.Skip = []v1beta1.SkipOption{"everything"} },
			"supported values"),
		Entry("skip entries from the allowlist", "cel-skip-ok",
			func(m *v1beta1.Migration) {
				m.Spec.Clone.Skip = []v1beta1.SkipOption{"largeObjects", "analyze"}
			}, ""),
		Entry("unknown logical decoding plugin", "cel-plugin-unknown",
			func(m *v1beta1.Migration) {
				m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, Plugin: "decoderbufs"}
			}, "supported values"),
	)

	// The CRD defaults are what Materialize relies on for partial keys; a bare
	// secretRef keeps keys unset and the Go-side fallback covers it instead.
	It("persists secretRef defaults for endpoint and partial keys", func() {
		m := validMigration("cel-secretref-defaults")
		m.Spec.Source = v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: "src-conn"}}
		m.Spec.Source.SuperuserSecretRef = &v1beta1.ConnectionSecret{
			Name: "src-admin",
			Keys: &v1beta1.ConnectionSecretKeys{Username: "root"},
		}
		m.Spec.Target = v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{
			Name: "tgt-conn",
			Keys: &v1beta1.ConnectionSecretKeys{Database: "custom"},
		}}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, m) })

		got := &v1beta1.Migration{}
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Name: "cel-secretref-defaults", Namespace: testNS}, got)).To(Succeed())
		Expect(got.Spec.Source.SecretRef.Endpoint).To(Equal("internal"))
		Expect(got.Spec.Source.SecretRef.Keys).To(BeNil())
		// Tied to conn.DefaultKeys: the CRD defaults and the Go fallback for
		// an absent keys object must never drift apart.
		wantKeys := conn.DefaultKeys()
		wantKeys.Database = "custom"
		Expect(got.Spec.Target.SecretRef.Keys).To(Equal(&wantKeys))
		// superuserSecretRef is the same ConnectionSecret type and must default
		// identically.
		Expect(got.Spec.Source.SuperuserSecretRef.Endpoint).To(Equal("internal"))
		wantSuper := conn.DefaultKeys()
		wantSuper.Username = "root"
		Expect(got.Spec.Source.SuperuserSecretRef.Keys).To(Equal(&wantSuper))
	})

	It("enforces source/target/follow immutability while clone stays tunable", func() {
		const name = "cel-immutable"
		m := validMigration(name)
		m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, SlotName: "fixed_slot"}
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, m) })

		fresh := func() *v1beta1.Migration {
			got := &v1beta1.Migration{}
			ExpectWithOffset(1, k8sClient.Get(ctx,
				types.NamespacedName{Name: name, Namespace: testNS}, got)).To(Succeed())
			return got
		}

		changed := fresh()
		changed.Spec.Source.Host = "elsewhere.example.com"
		Expect(k8sClient.Update(ctx, changed)).To(MatchError(ContainSubstring("source is immutable")))

		changed = fresh()
		changed.Spec.Target.Database = "other"
		Expect(k8sClient.Update(ctx, changed)).To(MatchError(ContainSubstring("target is immutable")))

		changed = fresh()
		changed.Spec.Source.SuperuserSecretRef = &v1beta1.ConnectionSecret{Name: "late-admin"}
		Expect(k8sClient.Update(ctx, changed)).To(MatchError(ContainSubstring("source is immutable")))

		changed = fresh()
		changed.Spec.Follow.SlotName = "other_slot"
		Expect(k8sClient.Update(ctx, changed)).To(MatchError(ContainSubstring("follow is immutable")))

		changed = fresh()
		changed.Spec.Follow.Enabled = false
		Expect(k8sClient.Update(ctx, changed)).To(MatchError(ContainSubstring("follow is immutable")))

		// Control: clone tuning and the cutover trigger stay mutable.
		changed = fresh()
		changed.Spec.Clone.TableJobs = 8
		changed.Spec.Cutover.Approved = true
		Expect(k8sClient.Update(ctx, changed)).To(Succeed())
	})
})

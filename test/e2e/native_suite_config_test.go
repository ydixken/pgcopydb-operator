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

package e2e

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func createThroughSuiteClient(t *testing.T, clone v1beta1.CloneOptions) v1beta1.CloneOptions {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	base := clientfake.NewClientBuilder().WithScheme(scheme).Build()
	m := newMigration("native-shape", nsE2E, clone)
	if err := (featureLabelingClient{Client: base}).Create(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	got := &v1beta1.Migration{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(m), got); err != nil {
		t.Fatal(err)
	}
	return got.Spec.Clone
}

func legacyCloneFixture() v1beta1.CloneOptions {
	return v1beta1.CloneOptions{
		TableJobs: 3, DropIfExists: true, UseCopyBinary: ptr.To(true), LargeObjectsJobs: 2,
		SplitTablesLargerThan: ptr.To(resource.MustParse("1Mi")), SplitMaxParts: 4,
		Filters: &v1beta1.Filters{ExcludeSchemas: []string{"e2e_skip"}},
	}
}

func TestNativeCloneShape(t *testing.T) {
	t.Setenv("E2E_ENGINE", string(v1beta1.MigrationEngineRPGMigrate))
	got := createThroughSuiteClient(t, legacyCloneFixture())
	want := v1beta1.CloneOptions{
		TableJobs: 3, UseCopyBinary: ptr.To(false), SplitTablesLargerThan: ptr.To(resource.MustParse("0")),
		Skip: []v1beta1.SkipOption{"largeObjects"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("native clone = %+v, want %+v", got, want)
	}
}

func TestLegacyCloneShapeUnchanged(t *testing.T) {
	for _, engine := range []string{"", string(v1beta1.MigrationEnginePGCopyDB)} {
		t.Setenv("E2E_ENGINE", engine)
		if got := createThroughSuiteClient(t, legacyCloneFixture()); !reflect.DeepEqual(got, legacyCloneFixture()) {
			t.Fatalf("E2E_ENGINE=%q changed the legacy clone: %+v", engine, got)
		}
	}
}

func TestNativeMajors(t *testing.T) {
	for _, tt := range []struct {
		source, target int
		ok             bool
	}{
		{16, 16, true}, {18, 18, true}, {16, 18, true},
		{17, 17, false}, {16, 17, false}, {17, 18, false}, {18, 16, false}, {14, 18, false}, {15, 15, false},
	} {
		err := checkMajors(v1beta1.MigrationEngineRPGMigrate, tt.source, tt.target)
		if tt.ok != (err == nil) {
			t.Errorf("native checkMajors(%d, %d) = %v, want ok=%t", tt.source, tt.target, err, tt.ok)
		}
		if err != nil && !strings.Contains(err.Error(), "16:16, 18:18 and 16:18") {
			t.Errorf("native checkMajors(%d, %d) = %v, want the allowed pairs named", tt.source, tt.target, err)
		}
	}
}

func TestNativeRefusalNamesPrerequisite(t *testing.T) {
	t.Setenv("E2E_ENGINE", string(v1beta1.MigrationEngineRPGMigrate))
	if _, err := suiteEngine(); err == nil || !strings.Contains(err.Error(), nativePrerequisite) {
		t.Fatalf("native refusal = %v, want it to name %s", err, nativePrerequisite)
	}
}

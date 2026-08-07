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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/ydixken/pgcopydb-operator/api/v1alpha1"
)

// TestBuildJob_PGPassfileInSpecEnv is the regression test for a live-found
// defect: PGPASSFILE exported only inside the prelude shell is invisible to
// commands the operator execs into the pod (sentinel reads, WAL-head query),
// which broke caught-up detection and cutover for password-based connections.
func TestBuildJob_PGPassfileInSpecEnv(t *testing.T) {
	withPassword := &v1alpha1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: v1alpha1.MigrationSpec{
			Source: v1alpha1.PostgresConnection{
				Host: "s", Database: "d", Username: "u",
				PasswordSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}, Key: "password",
				},
			},
			Target: v1alpha1.PostgresConnection{Host: "t", Database: "d", Username: "u"},
		},
	}
	job, err := buildJob(withPassword, "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := envValue(job.Spec.Template.Spec.Containers[0].Env, "PGPASSFILE"); got != "/tmp/pgpass" {
		t.Fatalf("PGPASSFILE must be in the container spec env, got %q", got)
	}

	// uriSecretRef carries credentials in the DSN itself: no passfile, no env.
	uriOnly := &v1alpha1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "m2", Namespace: "ns"},
		Spec: v1alpha1.MigrationSpec{
			Source: v1alpha1.PostgresConnection{
				URISecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "dsn"}, Key: "uri",
				},
			},
			Target: v1alpha1.PostgresConnection{
				URISecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "dsn2"}, Key: "uri",
				},
			},
		},
	}
	job, err = buildJob(uriOnly, "img", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := envValue(job.Spec.Template.Spec.Containers[0].Env, "PGPASSFILE"); got != "" {
		t.Fatalf("PGPASSFILE must be absent without password secrets, got %q", got)
	}
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

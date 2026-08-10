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
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestTruncate(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"", 5, ""},
		{"short", 10, "short"},
		{"exact", 5, "exact"},
		{"overflowing", 4, "over..."},
	}
	for _, tc := range cases {
		if got := truncate(tc.in, tc.n); got != tc.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
		}
	}
}

// defaultFailureMsg is failureReason's fallback; a test-local copy so a
// reworded fallback fails here.
const defaultFailureMsg = "worker Job failed"

func jobWithConditions(conds ...batchv1.JobCondition) *batchv1.Job {
	return &batchv1.Job{Status: batchv1.JobStatus{Conditions: conds}}
}

func TestFailureReason(t *testing.T) {
	cases := []struct {
		name string
		job  *batchv1.Job
		want string
	}{
		{"message wins", jobWithConditions(batchv1.JobCondition{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
			Reason: "BackoffLimitExceeded", Message: "pod backoff limit exceeded",
		}), "pod backoff limit exceeded"},
		{"reason backs an empty message", jobWithConditions(batchv1.JobCondition{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded",
		}), "DeadlineExceeded"},
		{"false failed condition is ignored", jobWithConditions(batchv1.JobCondition{
			Type: batchv1.JobFailed, Status: corev1.ConditionFalse, Message: "stale",
		}), defaultFailureMsg},
		{"no conditions", jobWithConditions(), defaultFailureMsg},
	}
	for _, tc := range cases {
		if got := failureReason(tc.job); got != tc.want {
			t.Errorf("%s: failureReason = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestJobFinished(t *testing.T) {
	cases := []struct {
		name     string
		job      *batchv1.Job
		done, ok bool
	}{
		{"complete", jobWithConditions(batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		}), true, true},
		{"failed", jobWithConditions(batchv1.JobCondition{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
		}), true, false},
		{"running", jobWithConditions(), false, false},
		{"false terminal conditions do not finish", jobWithConditions(
			batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionFalse},
			batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionFalse},
		), false, false},
		{"non-terminal condition types do not finish", jobWithConditions(
			batchv1.JobCondition{Type: batchv1.JobSuspended, Status: corev1.ConditionTrue},
		), false, false},
	}
	for _, tc := range cases {
		done, ok := jobFinished(tc.job)
		if done != tc.done || ok != tc.ok {
			t.Errorf("%s: jobFinished = (%v, %v), want (%v, %v)", tc.name, done, ok, tc.done, tc.ok)
		}
	}
}

// TestBuildJob_RunnerImage pins the image resolution order: spec.runner.image
// overrides the operator default, and every derived Job honors it.
func TestBuildJob_RunnerImage(t *testing.T) {
	m := passwordMigration()
	job, err := buildJob(m, "default:img", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := job.Spec.Template.Spec.Containers[0].Image; got != "default:img" {
		t.Fatalf("default image = %q", got)
	}

	m.Spec.Runner.Image = "custom/runner:v9"
	for name, build := range map[string]func() (*batchv1.Job, error){
		"worker":  func() (*batchv1.Job, error) { return buildJob(m, "default:img", 1) },
		"cleanup": func() (*batchv1.Job, error) { return buildCleanupJob(m, "default:img") },
		"verify":  func() (*batchv1.Job, error) { return buildVerifyJob(m, "default:img") },
	} {
		job, err := build()
		if err != nil {
			t.Fatal(err)
		}
		if got := job.Spec.Template.Spec.Containers[0].Image; got != "custom/runner:v9" {
			t.Errorf("%s Job image = %q, want the spec override", name, got)
		}
	}
}

// TestBuildJob_InvalidSpec pins that a spec neither inline-complete nor
// URI-based fails materialization on both sides: this error is what flips
// the Validated condition to InvalidSpec.
func TestBuildJob_InvalidSpec(t *testing.T) {
	m := passwordMigration()
	m.Spec.Source.Host = ""
	m.Spec.Source.Username = ""
	if _, err := buildJob(m, "img", 1); err == nil || !strings.Contains(err.Error(), "source") {
		t.Fatalf("want source materialization error, got %v", err)
	}
	m = passwordMigration()
	m.Spec.Target.Host = ""
	if _, err := buildJob(m, "img", 1); err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("want target materialization error, got %v", err)
	}
}

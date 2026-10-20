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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/progress"
)

// syncRepThreshold is the commit wait a sample counts as blocked. One sample can catch a
// long statement in a millisecond commit wait at this age, so CommitsBlocked needs two.
const syncRepThreshold = 30 * time.Second

// The CommitsBlocked reasons.
const (
	reasonSyncRepWait       = "SyncRepWait"
	reasonNoSyncRepWait     = "NoSyncRepWait"
	reasonWorkerSucceeded   = "WorkerSucceeded"
	reasonSyncRepUnobserved = "SyncRepUnobserved"
)

// syncRepOver reports a waiter at or over syncRepThreshold. Unknown (nil) is never over.
func syncRepOver(w *progress.SyncRepWait) bool {
	return w != nil && w.Waiting > 0 && w.OldestSeconds >= int64(syncRepThreshold/time.Second)
}

// syncRepWait is a sample's SyncRep count; no sample at all is unknown too.
func syncRepWait(s *progress.Sample) *progress.SyncRepWait {
	if s == nil {
		return nil
	}
	return s.SyncRep
}

// applyCommitsBlocked sets CommitsBlocked from the pass's sample. Only a confirmed wait raises
// it, and it stands until a parsed sample shows none: a lost sample is no proof the waits
// ended, so Unknown replaces only False or nothing.
func (r *MigrationReconciler) applyCommitsBlocked(m *v1beta1.Migration, got sampled) {
	if got.at.IsZero() {
		return
	}
	blocked := meta.IsStatusConditionTrue(m.Status.Conditions, v1beta1.ConditionCommitsBlocked)
	w := syncRepWait(got.sample)
	switch {
	case w == nil:
		if !blocked {
			r.setCondition(m, v1beta1.ConditionCommitsBlocked, metav1.ConditionUnknown, reasonSyncRepUnobserved,
				"the last progress sample could not read the target's synchronous replication waits")
		}
	case got.syncRepConfirmed:
		msg := fmt.Sprintf("target sessions of the migration role waiting for synchronous replication: %d, %d of them pgcopydb's, "+
			"the oldest for %ds; their commits wait until a synchronous standby confirms them",
			w.Waiting, w.PgcopydbNamed, w.OldestSeconds)
		if !blocked {
			r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, v1beta1.ConditionCommitsBlocked, "Sample", "%s", truncate(msg, maxDetailLen))
		}
		r.setCondition(m, v1beta1.ConditionCommitsBlocked, metav1.ConditionTrue, reasonSyncRepWait, msg)
	case !syncRepOver(w):
		r.setCondition(m, v1beta1.ConditionCommitsBlocked, metav1.ConditionFalse, reasonNoSyncRepWait,
			fmt.Sprintf("no target session of the migration role has waited %s or more for synchronous replication", syncRepThreshold))
	}
}

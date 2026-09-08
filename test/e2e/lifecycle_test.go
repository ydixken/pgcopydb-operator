package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const lifecycleCreatedUID = "created"

var _ = Describe("Migration lifecycle", func() {
	It("publishes Pending before Validating and starts preflight", func() {
		const name = "e2e-pending-lifecycle"
		m := newMigration(name, nsE2E, v1beta1.CloneOptions{DropIfExists: true})
		cfg, err := config.GetConfig()
		Expect(err).NotTo(HaveOccurred())
		reader, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		watchCtx, cancel := context.WithTimeout(ctx, migrationTimeout)
		defer cancel()
		selector := client.MatchingFields{"metadata.name": name}
		before := &v1beta1.MigrationList{}
		Expect(reader.List(watchCtx, before, client.InNamespace(nsE2E), selector)).To(Succeed())
		Expect(before.Items).To(BeEmpty(), "lifecycle fixture must not replace an existing Migration")
		Expect(before.ResourceVersion).NotTo(BeEmpty(), "list must establish the watch starting point")
		stream, err := reader.Watch(watchCtx, &v1beta1.MigrationList{}, client.InNamespace(nsE2E), selector,
			&client.ListOptions{Raw: &metav1.ListOptions{ResourceVersion: before.ResourceVersion}})
		Expect(err).NotTo(HaveOccurred())
		defer stream.Stop()
		DeferCleanup(func() {
			if m.UID != "" {
				deleteMigration(name)
			}
		})
		// Waiting for the work PVC here would miss the short, persisted Pending phase.
		Expect(k8sClient.Create(watchCtx, m)).To(Succeed())
		Expect(m.UID).NotTo(BeEmpty())
		Expect(requireFeatureMigrationOwnership(m)).To(Succeed())
		Expect(observeLifecycleUntilPreflight(watchCtx, stream.ResultChan(), m.UID, func() (bool, error) {
			events := &corev1.EventList{}
			if err := reader.List(watchCtx, events, client.InNamespace(nsE2E),
				client.MatchingFields{"involvedObject.uid": string(m.UID)}); err != nil {
				return false, errors.New("preflight event observation failed")
			}
			for _, event := range events.Items {
				if event.InvolvedObject.UID == m.UID && event.Type == corev1.EventTypeNormal &&
					event.Reason == "PreflightStarted" {
					return true, nil
				}
			}
			return false, nil
		})).To(Succeed())
	})
})

type lifecyclePhaseOrder struct {
	uid                 types.UID
	pending, validating bool
}

func (order *lifecyclePhaseOrder) observe(event watch.Event, open bool) error {
	if !open || event.Type == watch.Error || order.uid == "" {
		return errors.New("lifecycle watch lost its observation")
	}
	if event.Type == watch.Bookmark {
		return nil
	}
	m, ok := event.Object.(*v1beta1.Migration)
	if !ok {
		return errors.New("lifecycle watch received an unexpected object")
	}
	if m.UID != order.uid {
		return nil
	}
	if event.Type != watch.Added && event.Type != watch.Modified {
		return errors.New("watched Migration disappeared before lifecycle proof")
	}
	switch m.Status.Phase {
	case "":
		if order.pending {
			return errors.New("Migration status disappeared after Pending")
		}
	case v1beta1.PhasePending:
		if order.validating || m.Generation <= 0 || m.Status.ObservedGeneration != m.Generation ||
			len(m.Status.Conditions) != 0 {
			return errors.New("Pending recurred or did not preserve bootstrap-only status")
		}
		order.pending = true
	case v1beta1.PhaseValidating:
		condition := apiMeta.FindStatusCondition(m.Status.Conditions, v1beta1.ConditionValidated)
		if !order.pending || m.Status.ObservedGeneration != m.Generation || condition == nil ||
			condition.Status != metav1.ConditionUnknown || condition.Reason != "PreflightRunning" {
			return errors.New("Validating did not follow Pending with the running preflight condition")
		}
		order.validating = true
	default:
		if !order.validating {
			return errors.New("Migration advanced without observed Pending and Validating")
		}
	}
	return nil
}

func observeLifecycleUntilPreflight(watchCtx context.Context, events <-chan watch.Event, uid types.UID,
	preflightStarted func() (bool, error),
) error {
	order := lifecyclePhaseOrder{uid: uid}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-watchCtx.Done():
			return errors.New("lifecycle watch timed out before ordered phase and event evidence")
		case event, open := <-events:
			if err := order.observe(event, open); err != nil {
				return err
			}
		case <-ticker.C:
			if !order.validating {
				continue
			}
			started, err := preflightStarted()
			if err != nil {
				return err
			}
			if !started {
				continue
			}
			// Check queued transitions too; a visible Event must not hide a Pending recurrence.
			for {
				select {
				case event, open := <-events:
					if err := order.observe(event, open); err != nil {
						return err
					}
				default:
					return watchCtx.Err()
				}
			}
		}
	}
}

func TestLifecycleWatchRequiresOrderedPersistedPhases(t *testing.T) {
	pending := &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{UID: lifecycleCreatedUID, Generation: 1},
		Status: v1beta1.MigrationStatus{Phase: v1beta1.PhasePending, ObservedGeneration: 1}}
	validating := pending.DeepCopy()
	validating.Status.Phase = v1beta1.PhaseValidating
	validating.Status.Conditions = []metav1.Condition{{Type: v1beta1.ConditionValidated,
		Status: metav1.ConditionUnknown, Reason: "PreflightRunning"}}
	foreign := pending.DeepCopy()
	foreign.UID = "another-object"
	stale := pending.DeepCopy()
	stale.Status.ObservedGeneration = 0
	badValidation := validating.DeepCopy()
	badValidation.Status.Conditions[0].Status = metav1.ConditionTrue
	for _, tc := range []struct {
		name      string
		objects   []*v1beta1.Migration
		wantError bool
	}{
		{name: "ordered", objects: []*v1beta1.Migration{pending, validating}},
		{name: "repeated snapshots", objects: []*v1beta1.Migration{pending, pending, validating, validating}},
		{name: "foreign UID cannot supply Pending", objects: []*v1beta1.Migration{foreign, validating}, wantError: true},
		{name: "Validating first", objects: []*v1beta1.Migration{validating}, wantError: true},
		{name: "Pending recurs", objects: []*v1beta1.Migration{pending, validating, pending}, wantError: true},
		{name: "stale Pending", objects: []*v1beta1.Migration{stale}, wantError: true},
		{name: "wrong validation outcome", objects: []*v1beta1.Migration{pending, badValidation}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			order := lifecyclePhaseOrder{uid: lifecycleCreatedUID}
			var err error
			for _, object := range tc.objects {
				if err = order.observe(watch.Event{Type: watch.Modified, Object: object}, true); err != nil {
					break
				}
			}
			if (err != nil) != tc.wantError {
				t.Fatalf("watch error=%t, want %t", err != nil, tc.wantError)
			}
			if !tc.wantError && (!order.pending || !order.validating) {
				t.Error("ordered phase evidence was not retained")
			}
		})
	}
}

func TestLifecycleWatchRejectsMissingEvidence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event watch.Event
		open  bool
	}{
		{name: "closed"},
		{name: "server error", open: true, event: watch.Event{Type: watch.Error, Object: &metav1.Status{}}},
		{name: "deleted", open: true, event: watch.Event{Type: watch.Deleted,
			Object: &v1beta1.Migration{ObjectMeta: metav1.ObjectMeta{UID: lifecycleCreatedUID}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			order := lifecyclePhaseOrder{uid: lifecycleCreatedUID}
			if err := order.observe(tc.event, tc.open); err == nil {
				t.Error("unavailable lifecycle evidence was accepted")
			}
		})
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := observeLifecycleUntilPreflight(cancelled, make(chan watch.Event), lifecycleCreatedUID,
		func() (bool, error) { return true, nil }); err == nil {
		t.Error("timed-out watch borrowed success from the preflight event")
	}
}

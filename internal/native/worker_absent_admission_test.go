package native

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

// The four reader admissions accept an absent pod of a finished worker (design A C1d) and keep
// refusing it for a worker that runs or terminates, or whose pod comes or goes between reads.
func TestNativeReadersAdmitPodlessFinishedWorker(t *testing.T) {
	in := RetryInputs{CommitTest: true}
	readers := []struct {
		name string
		open func(*testing.T) (*readerAPI, func(context.Context) bool)
	}{
		{"retry", func(t *testing.T) (*readerAPI, func(context.Context) bool) {
			api, r, w := newRetryReaderAPI(t, in)
			return api, func(ctx context.Context) bool {
				got, err := r.ReadRetryReadiness(ctx, w, in)
				return err == nil && got != nil
			}
		}},
		{"owned", func(t *testing.T) (*readerAPI, func(context.Context) bool) {
			api, r, w := newOwnedReaderAPI(t)
			return api, func(ctx context.Context) bool {
				got, err := r.ReadOwnedState(ctx, w)
				return err == nil && got.Work() != (WorkIdentity{})
			}
		}},
		{"cleaned", func(t *testing.T) (*readerAPI, func(context.Context) bool) {
			api, r, w := newCleanedReaderAPI(t)
			return api, func(ctx context.Context) bool {
				got, err := r.ReadCleanedCompleted(ctx, w, fixtureEndLSN)
				return err == nil && got != nil
			}
		}},
		{"cleanup_rejoin", func(t *testing.T) (*readerAPI, func(context.Context) bool) {
			api, r, w := newCleanupRejoinReaderAPI(t)
			return api, func(ctx context.Context) bool {
				got, err := r.ReadCleanupRejoinCompleted(ctx, w, fixtureEndLSN)
				return err == nil && got != nil
			}
		}},
	}
	for _, reader := range readers {
		for _, tc := range []struct {
			name  string
			setup func(*readerAPI)
			admit bool
		}{
			{"absent_pod_finished_job", func(a *readerAPI) { a.extraPods = nil }, true},
			{"absent_pod_active_job", func(a *readerAPI) {
				a.extraPods = nil
				a.fixture.worker.Status.Active, a.fixture.worker.Status.Ready = 1, ptr.To(int32(0))
			}, false},
			{"absent_pod_unstarted_job", func(a *readerAPI) {
				a.extraPods = nil
				a.fixture.worker.Status = batchv1.JobStatus{}
			}, false},
			{"absent_pod_terminating_job", func(a *readerAPI) {
				a.extraPods = nil
				a.fixture.worker.Status.Terminating = ptr.To(int32(1))
			}, false},
			{"pod_appears_after_absent", func(a *readerAPI) {
				pod := a.extraPods[0]
				a.extraPods = nil
				a.afterResult = func(*admissionFixture) { a.extraPods = []corev1.Pod{pod} }
			}, false},
			{"pod_vanishes_after_present", func(a *readerAPI) {
				a.afterResult = func(*admissionFixture) { a.extraPods = nil }
			}, false},
			{"foreign_pod_with_worker_name", func(a *readerAPI) {
				a.extraPods[0].OwnerReferences[0].UID = readerReplacementUID
			}, false},
		} {
			t.Run(reader.name+"/"+tc.name, func(t *testing.T) {
				api, read := reader.open(t)
				tc.setup(api)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if got := read(ctx); got != tc.admit {
					t.Fatalf("admitted=%v, want %v", got, tc.admit)
				}
			})
		}
	}
}

// Only the retry reader follows a recorded worker Job that is gone, and only when no pod of it remains.
func TestNativeRetryReaderAdmitsGoneWorkerWithoutPod(t *testing.T) {
	in := RetryInputs{CommitTest: true}
	for _, tc := range []struct {
		name    string
		keepPod bool
		admit   bool
	}{{"gone_job_no_pod", false, true}, {"gone_job_pod_remains", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			api, r, w := newRetryReaderAPI(t, in)
			if !tc.keepPod {
				api.extraPods = nil
			}
			worker := "/jobs/" + api.fixture.m.Status.JobName
			handler := api.Config.Handler
			api.Config.Handler = http.HandlerFunc(func(out http.ResponseWriter, request *http.Request) {
				if strings.HasSuffix(request.URL.Path, worker) {
					http.NotFound(out, request)
					return
				}
				handler.ServeHTTP(out, request)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			got, err := r.ReadRetryReadiness(ctx, w, in)
			if admitted := err == nil && got != nil; admitted != tc.admit {
				t.Fatalf("admitted=%v (%v), want %v", admitted, err, tc.admit)
			}
		})
	}
	// Deletion inventories after a gone worker (foreground deletion removes it first), never beside its pod.
	for _, tc := range []struct {
		name    string
		keepPod bool
	}{{"owned_reader_admits_gone_job_without_pod", false}, {"owned_reader_refuses_gone_job_with_pod", true}} {
		t.Run(tc.name, func(t *testing.T) {
			api, r, w := newOwnedReaderAPI(t)
			if !tc.keepPod {
				api.extraPods = nil
			}
			worker := "/jobs/" + api.fixture.m.Status.JobName
			handler := api.Config.Handler
			api.Config.Handler = http.HandlerFunc(func(out http.ResponseWriter, request *http.Request) {
				if strings.HasSuffix(request.URL.Path, worker) {
					http.NotFound(out, request)
					return
				}
				handler.ServeHTTP(out, request)
			})
			got, err := r.ReadOwnedState(context.Background(), w)
			if admitted := err == nil && got.Work() != (WorkIdentity{}); admitted == tc.keepPod {
				t.Fatalf("inventory admitted=%v (%v) with keepPod=%v", admitted, err, tc.keepPod)
			}
		})
	}
}

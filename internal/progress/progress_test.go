/*
Copyright 2026 pgcopydb-operator contributors.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

package progress

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

func TestParseListProgress(t *testing.T) {
	// Shape per docs/research/pgcopydb-cli.md section 10 and upstream
	// progress.c (top-level bytes object); in-progress entries and unknown
	// keys must be ignored.
	raw := []byte(`{
		"table-jobs": 4,
		"index-jobs": 4,
		"tables": {"total": 120, "done": 87, "in-progress": [{"oid": 16385, "schema": "public", "name": "orders"}]},
		"indexes": {"total": 300, "done": 12},
		"bytes": {"total": 323888895, "done": 6162157, "in-progress": 1024, "total-pretty": "308 MB"}
	}`)
	p, err := ParseListProgress(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.TablesTotal != 120 || p.TablesDone != 87 || p.IndexesTotal != 300 || p.IndexesDone != 12 {
		t.Fatalf("bad parse: %+v", p)
	}
	if p.BytesTotal == nil || p.BytesTotal.Value() != 323888895 {
		t.Fatalf("bytesTotal: %v", p.BytesTotal)
	}
	if p.BytesDone == nil || p.BytesDone.Value() != 6162157 {
		t.Fatalf("bytesDone: %v", p.BytesDone)
	}
}

func TestParseListProgress_Malformed(t *testing.T) {
	for _, bad := range []string{
		"not json",
		`{"tables": "twelve"}`,
		`{"tables": {"total": "many"}}`,
		`[{"tables": {}}]`,
		`{"tables": {"total": 1}`,
	} {
		if _, err := ParseListProgress([]byte(bad)); err == nil {
			t.Errorf("%q: want parse error", bad)
		}
	}
}

func TestParseListProgress_MissingFields(t *testing.T) {
	// No bytes object (pgcopydb before the bytes counters, or schema drift):
	// the byte quantities must stay nil, not report fake zeros.
	p, err := ParseListProgress([]byte(`{"tables": {"total": 5, "done": 1}}`))
	if err != nil {
		t.Fatal(err)
	}
	if p.TablesTotal != 5 || p.TablesDone != 1 || p.IndexesTotal != 0 || p.IndexesDone != 0 {
		t.Fatalf("bad parse: %+v", p)
	}
	if p.BytesTotal != nil || p.BytesDone != nil {
		t.Fatalf("bytes must be nil when absent: %+v", p)
	}

	p, err = ParseListProgress([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if p.TablesTotal != 0 || p.TablesDone != 0 || p.BytesTotal != nil {
		t.Fatalf("zero values expected: %+v", p)
	}
}

func TestParseListProgress_StatusRoundTrip(t *testing.T) {
	// The parsed struct is written to status verbatim; the byte counters must
	// serialize as plain integer quantities, not lose precision.
	p, err := ParseListProgress([]byte(`{"bytes": {"total": 1073741824, "done": 999}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := p.BytesTotal.String(); got != "1Gi" {
		t.Fatalf("bytesTotal renders %q, want 1Gi", got)
	}
	if got := p.BytesDone.String(); got != "999" {
		t.Fatalf("bytesDone renders %q, want 999", got)
	}
	raw, err := json.Marshal(v1beta1.CloneProgress{BytesTotal: p.BytesTotal, BytesDone: p.BytesDone})
	if err != nil {
		t.Fatal(err)
	}
	if s := string(raw); !strings.Contains(s, `"bytesTotal":"1Gi"`) || !strings.Contains(s, `"bytesDone":"999"`) {
		t.Fatalf("status serialization: %s", s)
	}
}

// fakeAPI serves a pod list (or an error); exec requests fail because the
// server never upgrades to SPDY, which is exactly the transport failure
// CloneProgress must treat as "no sample yet".
func fakeAPI(t *testing.T, pods []corev1.Pod, fail bool) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case fail:
			http.Error(w, "boom", http.StatusInternalServerError)
		case strings.HasSuffix(r.URL.Path, "/pods"):
			w.Header().Set("Content-Type", "application/json")
			list := corev1.PodList{TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"}, Items: pods}
			_ = json.NewEncoder(w).Encode(list)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func runningPod() corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "w-0", Namespace: "ns", Labels: map[string]string{"job-name": "j"}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// poller builds a Poller the way the controller does: over a shared podexec
// transport, via NewFromExec.
func poller(t *testing.T, host string) *Poller {
	t.Helper()
	e, err := podexec.New(&rest.Config{Host: host})
	if err != nil {
		t.Fatal(err)
	}
	return NewFromExec(e)
}

func TestCloneProgress_NoRunningPod(t *testing.T) {
	p := poller(t, fakeAPI(t, nil, false))
	got, err := p.CloneProgress(context.Background(), "ns", "j")
	if err != nil || got != nil {
		t.Fatalf("no pod must mean (nil, nil), got (%v, %v)", got, err)
	}
}

func TestCloneProgress_ExecFailureIsNoSample(t *testing.T) {
	p := poller(t, fakeAPI(t, []corev1.Pod{runningPod()}, false))
	got, err := p.CloneProgress(context.Background(), "ns", "j")
	if err != nil || got != nil {
		t.Fatalf("exec failure must degrade to (nil, nil), got (%v, %v)", got, err)
	}
}

func TestCloneProgress_ListError(t *testing.T) {
	p := poller(t, fakeAPI(t, nil, true))
	if _, err := p.CloneProgress(context.Background(), "ns", "j"); err == nil {
		t.Fatal("want error when the pod list fails")
	}
}

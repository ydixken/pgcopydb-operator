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

package progress

import (
	"encoding/json"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

func TestParseListProgress(t *testing.T) {
	// Shape per upstream
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

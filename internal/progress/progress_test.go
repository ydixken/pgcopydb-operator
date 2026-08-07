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

import "testing"

func TestParseListProgress(t *testing.T) {
	// Shape per docs/research/pgcopydb-cli.md section 10; in-progress entries
	// and unknown keys must be ignored.
	raw := []byte(`{
		"table-jobs": 4,
		"index-jobs": 4,
		"tables": {"total": 120, "done": 87, "in-progress": [{"oid": 16385, "schema": "public", "name": "orders"}]},
		"indexes": {"total": 300, "done": 12}
	}`)
	p, err := ParseListProgress(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.TablesTotal != 120 || p.TablesDone != 87 || p.IndexesTotal != 300 || p.IndexesDone != 12 {
		t.Fatalf("bad parse: %+v", p)
	}
}

func TestParseListProgress_Invalid(t *testing.T) {
	if _, err := ParseListProgress([]byte("not json")); err == nil {
		t.Fatal("want error on invalid JSON")
	}
}

func TestParseListProgress_EmptyObject(t *testing.T) {
	p, err := ParseListProgress([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if p.TablesTotal != 0 || p.TablesDone != 0 {
		t.Fatalf("zero values expected: %+v", p)
	}
}

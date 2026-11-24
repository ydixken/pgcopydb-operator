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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const publicationWorkerStarted = "publication-test-pgcopydb-started"

func publicationRetryWorker(t *testing.T, slot, uri string) (*exec.Cmd, string) {
	t.Helper()
	m := passwordMigration()
	m.Spec.Source.PasswordSecretRef = nil
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, SlotName: slot}
	return preludeWorker(t, m, 2, uri, "#!/bin/sh\nprintf '%s\\n' '"+publicationWorkerStarted+"'\n")
}

// preludeWorker returns the worker command of m's attempt with pgcopydbStub as
// the first pgcopydb on PATH, and the directory that holds the stub.
func preludeWorker(t *testing.T, m *v1beta1.Migration, attempt int32, uri, pgcopydbStub string) (*exec.Cmd, string) {
	t.Helper()
	job, err := buildJob(m, "img", attempt)
	if err != nil {
		t.Fatal(err)
	}
	return jobCommand(t, job, uri, pgcopydbStub)
}

// jobCommand is preludeWorker for any Job; uri serves as source and target.
func jobCommand(t *testing.T, job *batchv1.Job, uri, pgcopydbStub string) (*exec.Cmd, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pgcopydb"), []byte(pgcopydbStub), 0o755); err != nil {
		t.Fatal(err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	commandCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(commandCtx, c.Command[0], append(c.Command[1:], c.Args...)...)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"),
		"PGCOPYDB_SOURCE_PGURI="+uri, "PGCOPYDB_TARGET_PGURI="+uri)
	return cmd, dir
}

// TestStartOverPrelude runs a start-over attempt's prelude on stub binaries:
// stream cleanup runs first, then the WAL worker sweep on source and target,
// then the publication guard, and a failed stream cleanup starts nothing.
func TestStartOverPrelude(t *testing.T) {
	for _, tc := range []struct {
		workers int32
		psql    string
	}{
		{1, "psql source\n"},
		{3, "psql source\npsql target\npsql source\n"},
	} {
		for _, code := range []int{0, 1} {
			t.Run(fmt.Sprintf("%d workers, stream cleanup exits %d", tc.workers, code), func(t *testing.T) {
				m := passwordMigration()
				m.Spec.Source.PasswordSecretRef = nil
				m.Spec.Clone.DropIfExists = true
				m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, WalWorkers: tc.workers}
				m.Status.Conditions = []metav1.Condition{{
					Type: v1beta1.ConditionCloneCompleted, Status: metav1.ConditionFalse, Reason: reasonSnapshotLost,
				}}
				calls := filepath.Join(t.TempDir(), "calls")
				cmd, dir := preludeWorker(t, m, 3, "source", fmt.Sprintf(
					"#!/bin/sh\necho \"pgcopydb $1\" >> '%s'\n[ \"$1\" != stream ] || exit %d\n", calls, code))
				cmd.Env = append(cmd.Env, "PGCOPYDB_TARGET_PGURI=target")
				if err := os.WriteFile(filepath.Join(dir, "psql"),
					[]byte("#!/bin/sh\necho \"psql $1\" >> '"+calls+"'\n"), 0o755); err != nil {
					t.Fatal(err)
				}
				want := "pgcopydb stream\n" + tc.psql + "pgcopydb clone\n"
				if code != 0 {
					want = "pgcopydb stream\n"
				}
				out, err := cmd.CombinedOutput()
				got, _ := os.ReadFile(calls)
				if (err != nil) != (code != 0) || string(got) != want {
					t.Fatalf("err=%v, calls=%q, want %q, output=%s", err, got, want, out)
				}
			})
		}
	}
}

func TestPublicationRetryShellFailure(t *testing.T) {
	for _, code := range []int{1, 2, 3, 127} {
		t.Run(fmt.Sprintf("psql exits %d", code), func(t *testing.T) {
			cmd, dir := publicationRetryWorker(t, "publication_retry_shell", "unused")
			// Even no-slot-looking stdout cannot turn a failed psql into permission to start.
			stub := fmt.Sprintf("#!/bin/sh\nprintf 'f\\n'\nexit %d\n", code)
			if err := os.WriteFile(filepath.Join(dir, "psql"), []byte(stub), 0o755); err != nil {
				t.Fatal(err)
			}
			out, err := cmd.CombinedOutput()
			if err == nil || strings.Contains(string(out), publicationWorkerStarted) {
				t.Fatalf("failed psql started pgcopydb: err=%v, output=%s", err, out)
			}
		})
	}
}

// simulateKubeletDollarExpansion reproduces the one documented rule of
// kubelet's Container.Command/Args expansion our generated scripts can hit:
// "Double $$ are reduced to a single $" (see corev1.Container.Command
// godoc). It is not a full re-implementation of kubelet's $(VAR) expander.
func simulateKubeletDollarExpansion(s string) string {
	return strings.ReplaceAll(s, "$$", "$")
}

// TestPublicationRetryKubeletDollarExpansion guards against reintroducing an
// anonymous DO $$ block. exec.Command runs the guard's raw Command/Args
// directly, so it never exercises kubelet's expansion; this test applies it
// before running the script, then proves both directions against a real
// server: the named dollar-quote tag survives untouched and executes the
// guard's SQL, while the old $$ form is corrupted into a syntax error.
func TestPublicationRetryKubeletDollarExpansion(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Skip("CI supplies PGCOPYDB_TEST_PGURI for publication retry SQL regressions")
	}
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	query := func(t *testing.T, sql string) {
		t.Helper()
		queryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if out, err := exec.CommandContext(queryCtx, psql, uri, "-XAtq", "-v", "ON_ERROR_STOP=1", "-c", sql).CombinedOutput(); err != nil {
			t.Fatalf("publication fixture SQL failed: %v\n%s", err, out)
		}
	}
	name := fmt.Sprintf("publication_retry_kubelet_%d", time.Now().UnixNano())
	table := name + "_table"
	t.Cleanup(func() {
		query(t, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='"+name+"'")
		query(t, `DROP PUBLICATION IF EXISTS "`+name+`"; DROP TABLE IF EXISTS `+table)
	})
	query(t, "CREATE TABLE "+table+" (id integer PRIMARY KEY)")
	query(t, `CREATE PUBLICATION "`+name+`" FOR TABLE `+table)
	query(t, "SELECT slot_name FROM pg_create_physical_replication_slot('"+name+"')")

	cmd, _ := publicationRetryWorker(t, name, uri)
	if got := simulateKubeletDollarExpansion(cmd.Args[2]); got != cmd.Args[2] {
		t.Fatalf("named dollar-quote tag must survive kubelet's $$ expansion unchanged:\n%s", got)
	}
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), publicationWorkerStarted) {
		t.Fatalf("named dollar-quote guard did not execute after kubelet expansion: err=%v, output=%s", err, out)
	}

	regressed, _ := publicationRetryWorker(t, name, uri)
	regressed.Args[2] = simulateKubeletDollarExpansion(
		strings.ReplaceAll(regressed.Args[2], publicationRetryDollarTag, "$$"))
	out, err = regressed.CombinedOutput()
	if err == nil || strings.Contains(string(out), publicationWorkerStarted) {
		t.Fatalf("anonymous DO $$ must fail once kubelet reduces it to DO $: err=%v, output=%s", err, out)
	}
	if !strings.Contains(string(out), "syntax error") {
		t.Fatalf("expected a SQL syntax error from the corrupted DO block, got: %s", out)
	}
}

func TestPublicationRetrySQL(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Skip("CI supplies PGCOPYDB_TEST_PGURI for publication retry SQL regressions")
	}
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	query := func(t *testing.T, sql string) string {
		t.Helper()
		queryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(queryCtx, psql, uri, "-XAtq", "-v", "ON_ERROR_STOP=1", "-c", sql).CombinedOutput()
		if err != nil {
			t.Fatalf("publication fixture SQL failed: %v\n%s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	for _, tc := range []struct {
		name                  string
		slot, publication     bool
		queryError, dropError bool
		wantError             string
	}{
		{name: "established", slot: true, publication: true},
		{name: "slot without publication", slot: true, wantError: "publication retry refused:"},
		{name: "orphan", publication: true},
		{name: "neither exists"},
		{name: "catalog query fails", publication: true, queryError: true, wantError: "does not exist"},
		{name: "drop fails", publication: true, dropError: true, wantError: "read-only transaction"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := fmt.Sprintf("publication_retry_%d", time.Now().UnixNano())
			table, other := name+"_table", name+"_other"
			t.Cleanup(func() {
				query(t, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='"+name+"'")
				query(t, `DROP PUBLICATION IF EXISTS "`+name+`", "`+other+`"; DROP TABLE IF EXISTS `+table)
			})
			query(t, "CREATE TABLE "+table+" (id integer PRIMARY KEY)")
			query(t, `CREATE PUBLICATION "`+other+`" FOR TABLE `+table)
			publicationState := func(pub string) string {
				return query(t, "SELECT p.oid::text || '|' || p.pubowner::text || '|' || p.puballtables || '|' || "+
					"COALESCE(string_agg(r.prrelid::text, ',' ORDER BY r.prrelid), '') "+
					"FROM pg_publication p LEFT JOIN pg_publication_rel r ON r.prpubid=p.oid "+
					"WHERE p.pubname='"+pub+"' GROUP BY p.oid, p.pubowner, p.puballtables")
			}
			otherBefore := publicationState(other)
			if otherBefore == "" {
				t.Fatal("unrelated publication fixture missing")
			}
			before := ""
			if tc.publication {
				query(t, `CREATE PUBLICATION "`+name+`" FOR TABLE `+table)
				before = publicationState(name)
				if before == "" {
					t.Fatal("auto publication fixture missing")
				}
			}
			if tc.slot {
				// Any source slot protects the publication. A physical slot also works on CI's wal_level=replica server.
				query(t, "SELECT slot_name FROM pg_create_physical_replication_slot('"+name+"')")
			}
			cmd, _ := publicationRetryWorker(t, name, uri)
			if tc.queryError {
				cmd.Args[2] = strings.ReplaceAll(cmd.Args[2], "pg_catalog.pg_replication_slots",
					"pg_catalog.publication_retry_missing_catalog")
			}
			if tc.dropError {
				cmd.Env = append(cmd.Env, "PGOPTIONS=-c default_transaction_read_only=on")
			}
			out, err := cmd.CombinedOutput()
			started := strings.Contains(string(out), publicationWorkerStarted)
			if tc.wantError != "" {
				if err == nil || started || !strings.Contains(string(out), tc.wantError) {
					t.Fatalf("retry must fail before pgcopydb with %q: err=%v, output=%s", tc.wantError, err, out)
				}
			} else if err != nil || !started {
				t.Fatalf("retry did not start pgcopydb: err=%v, output=%s", err, out)
			}
			want := before
			if !tc.slot && tc.wantError == "" {
				want = ""
			}
			if got := publicationState(name); got != want {
				t.Errorf("publication OID/owner/membership changed: got %q, want %q", got, want)
			}
			if got := publicationState(other); got != otherBefore {
				t.Errorf("unrelated publication changed: got %q, want %q", got, otherBefore)
			}
			if got := query(t, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='"+name+"')::text"); got != strconv.FormatBool(tc.slot) {
				t.Errorf("source slot changed: exists=%s, want %t", got, tc.slot)
			}
		})
	}
}

// liveQuery returns PGCOPYDB_TEST_PGURI and a psql runner against it, which
// serves as source and target. The test skips without the variable.
func liveQuery(t *testing.T) (string, func(t *testing.T, sql string) string) {
	t.Helper()
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Skip("CI supplies PGCOPYDB_TEST_PGURI for WAL worker cleanup SQL regressions")
	}
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	return uri, func(t *testing.T, sql string) string {
		t.Helper()
		queryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(queryCtx, psql, uri, "-XAtq", "-v", "ON_ERROR_STOP=1", "-c", sql).CombinedOutput()
		if err != nil {
			t.Fatalf("fixture SQL failed: %v\n%s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
}

// sqlNames renders fixture names, which hold no quotes, as a SQL text array.
func sqlNames(names []string) string {
	return "ARRAY['" + strings.Join(names, "','") + "']"
}

// walWorkerFixture creates physical slots (CI runs wal_level=replica), empty
// publications and replication origins, and drops them after the test. present
// lists which of them still exist, comma-joined in byte order.
func walWorkerFixture(t *testing.T, query func(*testing.T, string) string, slots, publications, origins []string) (present func() [3]string) {
	t.Helper()
	t.Cleanup(func() {
		query(t, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name::text = ANY ("+sqlNames(slots)+")")
		query(t, "SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname = ANY ("+sqlNames(origins)+")")
		for _, p := range publications {
			query(t, `DROP PUBLICATION IF EXISTS "`+p+`"`)
		}
	})
	for _, s := range slots {
		query(t, "SELECT slot_name FROM pg_create_physical_replication_slot('"+s+"')")
	}
	for _, p := range publications {
		query(t, `CREATE PUBLICATION "`+p+`"`)
	}
	for _, o := range origins {
		query(t, "SELECT pg_replication_origin_create('"+o+"')")
	}
	list := func(col, from string, names []string) string {
		return query(t, "SELECT string_agg("+col+"::text, ',' ORDER BY "+col+`::text COLLATE "C") FROM `+from+
			" WHERE "+col+"::text = ANY ("+sqlNames(names)+")")
	}
	return func() [3]string {
		return [3]string{
			list("slot_name", "pg_replication_slots", slots),
			list("pubname", "pg_publication", publications),
			list("roname", "pg_replication_origin", origins),
		}
	}
}

func sortedNames(names ...[]string) string {
	all := slices.Concat(names...)
	slices.Sort(all)
	return strings.Join(all, ",")
}

// walWorkerDecoys are names that differ from a WAL worker name of base by one
// suffix character, the suffix kind, or (look) a character LIKE's _ would match.
func walWorkerDecoys(base string) (slots, publications []string) {
	look := strings.Replace(base, "_", "x", 1)
	return []string{base + "_wx", base + "_w1_extra", base + "_t1", look + "_w1"},
		[]string{base, base + "_t", base + "_tx", base + "_t1x", base + "_w1", look + "_t1"}
}

// TestWalWorkersSweepSQL runs the cleanup Job on a stub stream cleanup against
// a real server: the sweep drops exactly the worker slots, origins and
// per-table publications of its own base name, and nothing after a failed cleanup.
func TestWalWorkersSweepSQL(t *testing.T) {
	uri, query := liveQuery(t)
	base := fmt.Sprintf("wal_sweep_%d", time.Now().UnixNano())
	slotDecoys, publicationDecoys := walWorkerDecoys(base)
	workers := []string{base + "_w1", base + "_w12"}
	tables := []string{base + "_t1", base + "_t12"}
	present := walWorkerFixture(t, query, slices.Concat(workers, slotDecoys),
		slices.Concat(tables, publicationDecoys), slices.Concat(workers, slotDecoys))

	m := passwordMigration()
	m.Spec.Source.PasswordSecretRef = nil
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, SlotName: base, WalWorkers: 3}
	job, err := buildCleanupJob(m, "img")
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []int{1, 0} {
		cmd, _ := jobCommand(t, job, uri, fmt.Sprintf("#!/bin/sh\nexit %d\n", code))
		out, err := cmd.CombinedOutput()
		if (err != nil) != (code != 0) {
			t.Fatalf("stream cleanup exit %d: err=%v, output=%s", code, err, out)
		}
		want := [3]string{sortedNames(workers, slotDecoys), sortedNames(tables, publicationDecoys), sortedNames(workers, slotDecoys)}
		if code == 0 {
			want = [3]string{sortedNames(slotDecoys), sortedNames(publicationDecoys), sortedNames(slotDecoys)}
		}
		if got := present(); got != want {
			t.Fatalf("stream cleanup exit %d left slots, publications, origins:\n%q\nwant:\n%q\noutput=%s", code, got, want, out)
		}
	}
}

// TestWalWorkersPublicationRetrySQL runs the multi-worker retry guard against a
// real server, with decoys that must neither satisfy nor trip it, nor be dropped.
func TestWalWorkersPublicationRetrySQL(t *testing.T) {
	uri, query := liveQuery(t)
	for _, tc := range []struct {
		name                string
		slots, publications func(base string) []string
		dropped             int // leading publications the guard drops
		readOnly            bool
		wantError           string
	}{
		{name: "established", slots: func(b string) []string { return []string{b, b + "_w1"} },
			publications: func(b string) []string { return []string{b + "_t1"} }},
		{name: "main slot without worker publications", slots: func(b string) []string { return []string{b} },
			wantError: "publication retry refused: source slot"},
		{name: "worker slot without the main slot", slots: func(b string) []string { return []string{b + "_w12"} },
			publications: func(b string) []string { return []string{b + "_t1"} },
			wantError:    "publication retry refused: WAL worker slots"},
		{name: "orphan publications", publications: func(b string) []string { return []string{b + "_t1", b + "_t12"} },
			dropped: 2},
		{name: "neither exists"},
		{name: "drop fails", publications: func(b string) []string { return []string{b + "_t1"} },
			readOnly: true, wantError: "read-only transaction"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fmt.Sprintf("wal_retry_%d", time.Now().UnixNano())
			slotDecoys, publicationDecoys := walWorkerDecoys(base)
			var slots, publications []string
			if tc.slots != nil {
				slots = tc.slots(base)
			}
			if tc.publications != nil {
				publications = tc.publications(base)
			}
			present := walWorkerFixture(t, query, slices.Concat(slots, slotDecoys),
				slices.Concat(publications, publicationDecoys), nil)

			m := passwordMigration()
			m.Spec.Source.PasswordSecretRef = nil
			m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, SlotName: base, WalWorkers: 3}
			cmd, _ := preludeWorker(t, m, 2, uri, "#!/bin/sh\nprintf '%s\\n' '"+publicationWorkerStarted+"'\n")
			if tc.readOnly {
				cmd.Env = append(cmd.Env, "PGOPTIONS=-c default_transaction_read_only=on")
			}
			out, err := cmd.CombinedOutput()
			started := strings.Contains(string(out), publicationWorkerStarted)
			if tc.wantError != "" {
				if err == nil || started || !strings.Contains(string(out), tc.wantError) {
					t.Fatalf("retry must fail before pgcopydb with %q: err=%v, output=%s", tc.wantError, err, out)
				}
			} else if err != nil || !started {
				t.Fatalf("retry did not start pgcopydb: err=%v, output=%s", err, out)
			}
			want := [3]string{sortedNames(slots, slotDecoys), sortedNames(publications[tc.dropped:], publicationDecoys)}
			if got := present(); got != want {
				t.Fatalf("slots, publications after the guard:\n%q\nwant:\n%q", got, want)
			}
		})
	}
}

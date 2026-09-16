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
	"strconv"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

const publicationWorkerStarted = "publication-test-pgcopydb-started"

func publicationRetryWorker(t *testing.T, slot, uri string) (*exec.Cmd, string) {
	t.Helper()
	m := passwordMigration()
	m.Spec.Source.PasswordSecretRef = nil
	m.Spec.Follow = &v1beta1.FollowOptions{Enabled: true, SlotName: slot}
	job, err := buildJob(m, "img", 2)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pgcopydb"),
		[]byte("#!/bin/sh\nprintf '%s\\n' '"+publicationWorkerStarted+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	commandCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(commandCtx, c.Command[0], append(c.Command[1:], c.Args...)...)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "PGCOPYDB_SOURCE_PGURI="+uri)
	return cmd, dir
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

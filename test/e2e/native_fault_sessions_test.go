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

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNativeSessionControl(t *testing.T) {
	const cluster = "e2e-target"
	s := nativeSessionControl{}
	if err := s.absent(cluster, "0"); err == nil || !strings.Contains(err.Error(), "proves nothing") {
		t.Fatalf("a zero before any positive control was admitted: %v", err)
	}
	for _, count := range []string{"0", "", "x", "-1"} {
		if err := s.observe(cluster, count); err == nil {
			t.Errorf("count %q admitted as a live engine", count)
		}
	}
	if err := s.absent(cluster, "0"); err == nil {
		t.Fatal("a failed positive control still let a zero count")
	}
	if err := s.observe(cluster, "2"); err != nil {
		t.Fatal(err)
	}
	if err := s.absent("e2e-source", "0"); err == nil {
		t.Fatal("the control on one cluster let a zero count on another")
	}
	if err := s.absent(cluster, "1"); err == nil {
		t.Fatal("a remaining session was admitted as absent")
	}
	if err := s.absent(cluster, "0"); err != nil {
		t.Fatal(err)
	}
}

// TestNativeFaultSessionSQL runs the session, apply, blocked-apply and lock release SQL against
// sessions named like the engine's and like decoys: the release must leave the engine's apply alive.
func TestNativeFaultSessionSQL(t *testing.T) {
	run := testPSQL(t)
	const table = "public.e2e_nfault_sessions"
	const receive, apply = "rpgmigrate[4242] receive", "rpgmigrate[4242] apply"
	decoys := []string{"e2e-decoy", "pgcopydb[4242] apply", "xrpgmigrate[4242] apply", "rpgmigrate-4242 apply"}
	names := "'" + strings.Join(append([]string{nativeFaultLockApp, receive, apply}, decoys...), "', '") + "'"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(func() {
		// A backend in pg_sleep does not notice that its client is gone.
		testQuery(t, run, "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity"+
			" WHERE application_name IN ("+names+")")
		cancel()
		testQuery(t, run, "DROP TABLE IF EXISTS "+table)
	})
	testQuery(t, run, "DROP TABLE IF EXISTS "+table+"; CREATE TABLE "+table+" (id int)")
	sleep := func(name, before string) {
		testBackground(ctx, run, "SET application_name = '"+name+"';\n"+before+"SELECT pg_sleep(600);\n")
	}
	expect := func(sql, want, why string) {
		t.Helper()
		if got := testQuery(t, run, sql); got != want {
			t.Fatalf("%s: got %q, want %q from %s", why, got, want, sql)
		}
	}
	if !strings.HasPrefix(nativeFaultBlockedApplySQL, nativeFaultApplySessionSQL+" AND ") {
		t.Fatal("the blocked-apply probe no longer narrows the apply session control")
	}
	expect(nativeFaultSessionSQL, "0", "an engine session before any started")
	for _, name := range decoys {
		sleep(name, "")
	}
	testWaitFor(t, run, "SELECT count(*) FROM pg_stat_activity WHERE application_name IN ("+names+")", "4")
	expect(nativeFaultSessionSQL, "0", "a decoy matched the engine session name")
	expect(nativeFaultApplySessionSQL, "0", "a decoy matched the engine apply name")

	testBackground(ctx, run, holdSQL(nativeFaultLockApp, "LOCK TABLE "+table+" IN ACCESS EXCLUSIVE MODE"))
	testWaitFor(t, run, "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid"+
		" WHERE a.application_name = '"+nativeFaultLockApp+"' AND l.relation = '"+table+"'::regclass AND l.granted", "1")
	sleep(receive, "")
	sleep(apply, "INSERT INTO "+table+" VALUES (1);\n")
	testWaitFor(t, run, nativeFaultSessionSQL, "2")
	testWaitFor(t, run, nativeFaultBlockedApplySQL, "1")
	expect(nativeFaultApplySessionSQL, "1", "the blocked apply is not an apply session")

	expect(nativeFaultReleaseSQL, "1", "the release did not end exactly the lock holder")
	testWaitFor(t, run, "SELECT count(*) FROM "+table, "1")
	expect(nativeFaultBlockedApplySQL, "0", "the apply still waits after the release")
	expect(nativeFaultSessionSQL, "2", "the lock release ended an engine session")

	expect("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity"+
		" WHERE application_name IN ('"+receive+"', '"+apply+"')", "2", "the engine sessions were not both live")
	testWaitFor(t, run, nativeFaultSessionSQL, "0")
}

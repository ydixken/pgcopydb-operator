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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	nativeFaultBulkTable = "public.e2e_nfault_bulk"
	// One pglz-compressed 8 KiB payload per row keeps each side's disk near 110 MB
	// while the COPY moves the full text.
	nativeFaultBulkPayloadRepeat = 256
	nativeFaultBulkPayloadBytes  = 32 * nativeFaultBulkPayloadRepeat
	// One text COPY of this shape ran at about 135 MB/s on the shared podman VM, over
	// loopback without TLS: 4 GiB is about 30 s there, still 15 s at twice that rate.
	nativeFaultBulkBytes = int64(4) << 30
	nativeFaultBulkRows  = nativeFaultBulkBytes / nativeFaultBulkPayloadBytes
	// nativeFaultBulkTotalSQL is the exact text COPY stream: id, tab, hex payload, newline.
	nativeFaultBulkTotalSQL = "SELECT count(*) || '|' ||" +
		" coalesce(sum(octet_length(id::text) + octet_length(payload) + 2), 0) FROM ONLY " + nativeFaultBulkTable
	nativeFaultBulkCopySQL = "SELECT p.bytes_processed || '|' || round(extract(epoch FROM now() - a.query_start), 3)" +
		" || '|' || coalesce(a.backend_xid::text, '') FROM pg_stat_progress_copy p JOIN pg_stat_activity a ON a.pid = p.pid" +
		" WHERE a.application_name LIKE 'rpgmigrate[%] copy' AND p.command = 'COPY FROM'" +
		" AND p.relid = to_regclass('" + nativeFaultBulkTable + "')"
)

var nativeFaultXID = regexp.MustCompile(`^[1-9][0-9]{0,9}$`)

// nativeFaultBulkSQL creates the bulk table with rows rows, replacing any earlier one.
func nativeFaultBulkSQL(rows int64) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %[1]s; CREATE TABLE %[1]s (id bigint PRIMARY KEY, payload text NOT NULL);"+
		" INSERT INTO %[1]s SELECT g, repeat(md5(g::text), %[2]d) FROM generate_series(1, %[3]d) g",
		nativeFaultBulkTable, nativeFaultBulkPayloadRepeat, rows)
}

// nativeFaultXactStatusSQL reads the commit status of a 32-bit xid in the current epoch window.
func nativeFaultXactStatusSQL(xid string) string {
	return "SELECT coalesce(pg_xact_status((CASE WHEN t.low <= " + xid + " THEN t.top - t.low + " + xid +
		" - 4294967296 ELSE t.top - t.low + " + xid + " END)::text::xid8), 'unknown') FROM (SELECT x AS top," +
		" x % 4294967296 AS low FROM (SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint) s(x)) t"
}

func parseNativeFaultBulkTotal(out string) (rows, total int64, err error) {
	fields := strings.Split(out, "|")
	if len(fields) == 2 {
		rows, err = strconv.ParseInt(fields[0], 10, 64)
		if err == nil {
			total, err = strconv.ParseInt(fields[1], 10, 64)
		}
	}
	if len(fields) != 2 || err != nil || rows <= 0 || total <= 0 {
		return 0, 0, fmt.Errorf("bulk table total returned %q", out)
	}
	return rows, total, nil
}

// nativeFaultCopySample is one nativeFaultBulkCopySQL reading; present is false when no COPY runs into the table.
type nativeFaultCopySample struct {
	present bool
	bytes   int64
	elapsed float64
	xid     string
}

func parseNativeFaultCopy(out string, total int64) (nativeFaultCopySample, error) {
	if out == "" {
		return nativeFaultCopySample{}, nil
	}
	fields := strings.Split(out, "|")
	if len(fields) != 3 || strings.Contains(out, "\n") {
		return nativeFaultCopySample{}, fmt.Errorf("COPY progress returned %q, not one COPY into the bulk table", out)
	}
	s := nativeFaultCopySample{present: true, xid: fields[2]}
	var err error
	if s.bytes, err = strconv.ParseInt(fields[0], 10, 64); err != nil || s.bytes < 0 {
		return s, fmt.Errorf("COPY progress returned bytes %q", fields[0])
	}
	if s.bytes > total {
		return s, fmt.Errorf("COPY processed %d bytes of a %d byte stream: the total is wrong", s.bytes, total)
	}
	if s.elapsed, err = strconv.ParseFloat(fields[1], 64); err != nil || s.elapsed < 0 {
		return s, fmt.Errorf("COPY progress returned elapsed %q", fields[1])
	}
	if s.xid != "" && !nativeFaultXID.MatchString(s.xid) {
		return s, fmt.Errorf("COPY progress returned xid %q", s.xid)
	}
	return s, nil
}

// midCopy is the kill condition: rows are on the way and the transaction that would commit them is known.
func (s nativeFaultCopySample) midCopy(total int64) bool {
	return s.present && s.bytes > 0 && s.bytes < total && s.xid != ""
}

func (s nativeFaultCopySample) complete(total int64) bool { return s.present && s.bytes == total }

// nativeFaultBulk is the spec-local plan table whose COPY the fault interrupts.
type nativeFaultBulk struct{ rows, total int64 }

// createNativeFaultBulk creates the bulk table on the source as the app role, which owns
// every fixture table; the drop runs after the Migration's own cleanup.
func createNativeFaultBulk() *nativeFaultBulk {
	GinkgoHelper()
	DeferCleanup(func() { psql(sourceCluster, "DROP TABLE IF EXISTS "+nativeFaultBulkTable) })
	psqlBulk(sourceCluster, "SET ROLE "+sqlIdent(appRole(sourceCluster))+"; "+nativeFaultBulkSQL(nativeFaultBulkRows))
	rows, total, err := parseNativeFaultBulkTotal(psql(sourceCluster, nativeFaultBulkTotalSQL))
	Expect(err).NotTo(HaveOccurred())
	Expect(rows).To(Equal(nativeFaultBulkRows), "the bulk table holds the wrong row count")
	Expect(total).To(BeNumerically(">=", nativeFaultBulkBytes), "the bulk table COPY is smaller than sized")
	return &nativeFaultBulk{rows: rows, total: total}
}

// waitMidCopy polls the target until the bulk table is mid-copy and returns that sample.
func (b *nativeFaultBulk) waitMidCopy(stop func(Gomega) string) nativeFaultCopySample {
	GinkgoHelper()
	var sample nativeFaultCopySample
	Eventually(func(g Gomega) {
		if reason := stop(g); reason != "" {
			StopTrying(reason + " before the bulk table was seen mid-copy").Now()
		}
		out, err := psqlDBErr(targetCluster, appDatabase(targetCluster), nativeFaultBulkCopySQL)
		g.Expect(err).NotTo(HaveOccurred())
		s, err := parseNativeFaultCopy(out, b.total)
		if err != nil {
			StopTrying(err.Error()).Now()
		}
		if s.complete(b.total) {
			StopTrying("the bulk table COPY was seen only complete").Now()
		}
		g.Expect(s.midCopy(b.total)).To(BeTrue(), "the bulk table is not mid-copy: %+v", s)
		sample = s
	}, migrationTimeout, 250*time.Millisecond).Should(Succeed())
	return sample
}

// expectAborted requires the killed COPY's transaction to end aborted, which proves the
// kill landed before the bulk table committed.
func (b *nativeFaultBulk) expectAborted(xid string) {
	GinkgoHelper()
	var status string
	Eventually(func(g Gomega) {
		out, err := psqlDBErr(targetCluster, appDatabase(targetCluster), nativeFaultXactStatusSQL(xid))
		g.Expect(err).NotTo(HaveOccurred())
		status = out
		g.Expect(status).NotTo(Equal("in progress"), "the killed COPY transaction %s is still open", xid)
	}, 2*time.Minute, time.Second).Should(Succeed())
	Expect(status).To(Equal("aborted"), "the killed COPY transaction %s ended %s: the kill missed the COPY", xid, status)
}

// expectRows holds the bulk table to its row count: always on the source, after a
// resume on the target, and all or nothing on a target that failed closed.
func (b *nativeFaultBulk) expectRows(target string, resumed bool) {
	GinkgoHelper()
	Expect(psqlInt(sourceCluster, "SELECT count(*) FROM ONLY "+nativeFaultBulkTable)).
		To(BeNumerically("==", b.rows), "the source lost bulk rows")
	if !resumed && psql(target, "SELECT to_regclass('"+nativeFaultBulkTable+"') IS NULL") == "t" {
		return
	}
	got := int64(psqlInt(target, "SELECT count(*) FROM ONLY "+nativeFaultBulkTable))
	if resumed {
		Expect(got).To(Equal(b.rows), "INVARIANT VIOLATED: %d of %d bulk rows on the target", got, b.rows)
	} else {
		Expect(got).To(BeElementOf(int64(0), b.rows), "INVARIANT VIOLATED: a partial bulk COPY committed")
	}
}

// report records a midCopy sample, so bytes is above zero.
func (b *nativeFaultBulk) report(s nativeFaultCopySample) {
	window := fmt.Sprintf("%.1fs", s.elapsed*float64(b.total)/float64(s.bytes))
	AddReportEntry("native fault copy kill", fmt.Sprintf("table=%s rows=%d total_bytes=%d bytes_at_kill=%d (%.1f%%)"+
		" copy_elapsed=%.3fs projected_window=%s xid=%s", nativeFaultBulkTable, b.rows, b.total, s.bytes,
		100*float64(s.bytes)/float64(b.total), s.elapsed, window, s.xid))
}

func TestNativeFaultCopyProgress(t *testing.T) {
	const total = 1000
	for _, tc := range []struct {
		name          string
		out           string
		mid, complete bool
	}{
		{"missing row", "", false, false},
		{"zero bytes", "0|0.250|7", false, false},
		{"partial", "400|3.000|7", true, false},
		{"partial without a transaction", "400|3.000|", false, false},
		{"last byte pending", "999|9.000|7", true, false},
		{"complete", "1000|10.000|7", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := parseNativeFaultCopy(tc.out, total)
			if err != nil {
				t.Fatal(err)
			}
			if s.midCopy(total) != tc.mid || s.complete(total) != tc.complete {
				t.Fatalf("%+v: mid %t complete %t", s, s.midCopy(total), s.complete(total))
			}
		})
	}
	for _, out := range []string{
		"1001|1.000|7", "-1|1.000|7", "x|1.000|7", "1|x|7", "1|-1|7", "1|1|0", "1|1|7;", "1|1",
		"1|1|7\n2|1|8", "1|1|7|1",
	} {
		if _, err := parseNativeFaultCopy(out, total); err == nil {
			t.Errorf("COPY progress %q admitted", out)
		}
	}
	for _, out := range []string{"", "1", "0|10", "10|0", "x|10", "10|x", "1|2|3"} {
		if _, _, err := parseNativeFaultBulkTotal(out); err == nil {
			t.Errorf("bulk total %q admitted", out)
		}
	}
	if nativeFaultBulkRows*nativeFaultBulkPayloadBytes != nativeFaultBulkBytes {
		t.Errorf("%d rows of %d bytes do not make %d", nativeFaultBulkRows, nativeFaultBulkPayloadBytes,
			nativeFaultBulkBytes)
	}
}

// TestNativeFaultCopySQL runs the bulk, progress and commit-status SQL against a real
// server: the total must equal the COPY stream, and a COPY held open must read mid-copy.
func TestNativeFaultCopySQL(t *testing.T) {
	run := testPSQL(t)
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	t.Cleanup(func() { testQuery(t, run, "DROP TABLE IF EXISTS "+nativeFaultBulkTable) })
	testQuery(t, run, nativeFaultBulkSQL(64))
	rows, total, err := parseNativeFaultBulkTotal(testQuery(t, run, nativeFaultBulkTotalSQL))
	if err != nil || rows != 64 {
		t.Fatalf("bulk total: %d rows, %v", rows, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	stream, err := exec.CommandContext(ctx, "psql", uri, "-XqtA", "-c",
		"COPY (SELECT id, payload FROM ONLY "+nativeFaultBulkTable+") TO STDOUT WITH (FORMAT text)").Output()
	if err != nil || int64(len(stream)) != total {
		t.Fatalf("COPY stream is %d bytes, total SQL says %d: %v", len(stream), total, err)
	}
	sample := func() nativeFaultCopySample {
		t.Helper()
		s, err := parseNativeFaultCopy(testQuery(t, run, nativeFaultBulkCopySQL), total)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	if s := sample(); s.present {
		t.Fatalf("a COPY into the bulk table before any started: %+v", s)
	}
	half := bytes.Index(stream[len(stream)/2:], []byte("\n")) + len(stream)/2 + 1
	for _, commit := range []bool{true, false} {
		cmd := exec.CommandContext(ctx, "psql", uri, "-Xq", "-v", "ON_ERROR_STOP=1")
		cmd.Env = append(cmd.Environ(), "PGAPPNAME=rpgmigrate[test] copy")
		stdin, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(stdin, "BEGIN;\nTRUNCATE "+nativeFaultBulkTable+";\nCOPY "+nativeFaultBulkTable+
			" (id, payload) FROM STDIN WITH (FORMAT text, FREEZE true);\n"); err != nil {
			t.Fatal(err)
		}
		if _, err := stdin.Write(stream[:half]); err != nil {
			t.Fatal(err)
		}
		var mid nativeFaultCopySample
		for deadline := time.Now().Add(30 * time.Second); !mid.midCopy(total); time.Sleep(50 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("a COPY fed %d of %d bytes never read mid-copy: %+v", half, total, mid)
			}
			mid = sample()
		}
		if mid.bytes > int64(half) {
			t.Fatalf("COPY processed %d bytes of the %d fed", mid.bytes, half)
		}
		if commit {
			_, err = io.WriteString(stdin, string(stream[half:])+"\\.\nCOMMIT;\n")
			if err == nil {
				err = stdin.Close()
			}
			if err == nil {
				err = cmd.Wait()
			}
		} else {
			err = cmd.Process.Kill()
			if waitErr := cmd.Wait(); !errors.As(waitErr, new(*exec.ExitError)) {
				t.Fatalf("killed psql exited %v", waitErr)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		want := map[bool]string{true: "committed", false: "aborted"}[commit]
		status := ""
		for deadline := time.Now().Add(30 * time.Second); status != want; time.Sleep(50 * time.Millisecond) {
			if status = testQuery(t, run, nativeFaultXactStatusSQL(mid.xid)); status != "in progress" && status != want {
				t.Fatalf("COPY transaction %s ended %q, want %q", mid.xid, status, want)
			}
			if time.Now().After(deadline) {
				t.Fatalf("COPY transaction %s still %q", mid.xid, status)
			}
		}
		if s := sample(); s.present {
			t.Fatalf("a COPY into the bulk table after the session ended: %+v", s)
		}
	}
	if got := testQuery(t, run, "SELECT count(*) FROM ONLY "+nativeFaultBulkTable); got != "64" {
		t.Fatalf("bulk table holds %s rows after one committed and one killed COPY", got)
	}
}

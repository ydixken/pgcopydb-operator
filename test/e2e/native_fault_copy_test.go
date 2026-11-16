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
	// nativeFaultBinaryFraming is the 19-byte header and 2-byte trailer of every binary COPY stream.
	nativeFaultBinaryFraming = 21
	nativeFaultCopyText      = "text"
	nativeFaultCopyBinary    = "binary"
	// nativeFaultBulkTotalSQL sizes both COPY formats exactly: a text row is id, tab, payload and
	// newline; a binary row is a field count, two lengths, the 8-byte bigint and the payload.
	nativeFaultBulkTotalSQL = "SELECT count(*) || '|' ||" +
		" coalesce(sum(octet_length(id::text) + octet_length(payload) + 2), 0) || '|' ||" +
		" coalesce(sum(octet_length(payload) + 18), 0) FROM ONLY " + nativeFaultBulkTable
	// nativeFaultBulkCopySQL reads one row per COPY into the bulk table, so split parts each show,
	// with the format the engine's statement names. The parser refuses a session not named like
	// the engine's, so an unnamed engine cannot hide its COPY. The snapshot xmax lets
	// nativeFaultFullXID widen the 32-bit xid to xid8.
	nativeFaultBulkCopySQL = "SELECT p.bytes_processed || '|' || round(extract(epoch FROM now() - a.query_start), 3)" +
		" || '|' || coalesce(a.backend_xid::text, '') || '|' || pg_snapshot_xmax(pg_current_snapshot())" +
		` || '|' || coalesce(substring(a.query from ' FROM STDIN WITH \(FORMAT (text|binary)(?:, FREEZE true)?\);?$'), '')` +
		" || '|' || a.application_name FROM pg_stat_progress_copy p JOIN pg_stat_activity a ON a.pid = p.pid" +
		" WHERE p.command = 'COPY FROM' AND p.relid = to_regclass('" + nativeFaultBulkTable + "') ORDER BY p.pid"
)

// nativeFaultBulkSQL creates the bulk table with rows rows, replacing any earlier one.
func nativeFaultBulkSQL(rows int64) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %[1]s; CREATE TABLE %[1]s (id bigint PRIMARY KEY, payload text NOT NULL);"+
		" INSERT INTO %[1]s SELECT g, repeat(md5(g::text), %[2]d) FROM generate_series(1, %[3]d) g",
		nativeFaultBulkTable, nativeFaultBulkPayloadRepeat, rows)
}

// nativeFaultXactStatusSQL reads the commit status of a full transaction ID; NULL reads as empty.
func nativeFaultXactStatusSQL(xid string) string {
	return "SELECT pg_xact_status(" + sqlLiteral(xid) + "::xid8)"
}

// nativeFaultBulkSize is the bulk table's exact COPY payload: the text stream, and the
// binary rows without the framing each binary stream adds.
type nativeFaultBulkSize struct{ text, binaryRows int64 }

// stream is the most bytes streams concurrent COPYs of format can carry between them.
func (z nativeFaultBulkSize) stream(format string, streams int) int64 {
	if format == nativeFaultCopyBinary {
		return z.binaryRows + nativeFaultBinaryFraming*int64(streams)
	}
	return z.text
}

func parseNativeFaultBulkTotal(out string) (rows int64, size nativeFaultBulkSize, err error) {
	fields := strings.Split(out, "|")
	if len(fields) == 3 {
		rows, err = strconv.ParseInt(fields[0], 10, 64)
		if err == nil {
			size.text, err = strconv.ParseInt(fields[1], 10, 64)
		}
		if err == nil {
			size.binaryRows, err = strconv.ParseInt(fields[2], 10, 64)
		}
	}
	if len(fields) != 3 || err != nil || rows <= 0 || size.text <= 0 || size.binaryRows <= 0 {
		return 0, nativeFaultBulkSize{}, fmt.Errorf("bulk table total returned %q", out)
	}
	return rows, size, nil
}

// nativeFaultCopySample sums one nativeFaultBulkCopySQL reading over every engine COPY into the
// table; present is false when none runs. total is the size for the format and stream count seen.
type nativeFaultCopySample struct {
	present        bool
	format         string
	streams        int
	bytes, total   int64
	elapsed        float64
	xid            string
	xidStreamBytes int64
}

func parseNativeFaultCopy(out string, size nativeFaultBulkSize) (nativeFaultCopySample, error) {
	s := nativeFaultCopySample{}
	if out == "" {
		return s, nil
	}
	name := ""
	for line := range strings.SplitSeq(out, "\n") {
		fields := strings.SplitN(line, "|", 6)
		if len(fields) != 6 {
			return s, fmt.Errorf("COPY progress returned %q, not one row per COPY into the bulk table", line)
		}
		if n := fields[5]; !strings.HasPrefix(n, "rpgmigrate[") || !strings.HasSuffix(n, "] copy") {
			return s, fmt.Errorf("the bulk table COPY runs as application_name %q, not rpgmigrate[<pid>] copy", n)
		}
		if name != "" && fields[5] != name {
			return s, fmt.Errorf("the bulk table COPYs run as %q and %q, not one engine", name, fields[5])
		}
		name = fields[5]
		if format := fields[4]; format != nativeFaultCopyText && format != nativeFaultCopyBinary {
			return s, fmt.Errorf("the bulk table COPY statement names format %q, not text or binary", format)
		} else if s.format != "" && format != s.format {
			return s, fmt.Errorf("the bulk table COPYs run as %s and %s, not one recorded format", s.format, format)
		}
		s.format = fields[4]
		n, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil || n < 0 || n > max(size.text, size.stream(nativeFaultCopyBinary, 1)) {
			return s, fmt.Errorf("COPY progress returned bytes %q", fields[0])
		}
		elapsed, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || elapsed < 0 {
			return s, fmt.Errorf("COPY progress returned elapsed %q", fields[1])
		}
		xmax, err := strconv.ParseUint(fields[3], 10, 64)
		if err != nil || xmax == 0 {
			return s, fmt.Errorf("COPY progress returned xmax %q", fields[3])
		}
		s.present, s.streams, s.bytes, s.elapsed = true, s.streams+1, s.bytes+n, max(s.elapsed, elapsed)
		if fields[2] == "" {
			continue
		}
		xid, err := strconv.ParseUint(fields[2], 10, 32)
		if err != nil || xid == 0 {
			return s, fmt.Errorf("COPY progress returned xid %q", fields[2])
		}
		full, err := nativeFaultFullXID(uint32(xid), xmax)
		if err != nil {
			return s, err
		}
		// The least advanced stream with rows on the way is likeliest still open at the kill.
		if n > 0 && (s.xid == "" || n < s.xidStreamBytes) {
			s.xid, s.xidStreamBytes = strconv.FormatUint(full, 10), n
		}
	}
	s.total = size.stream(s.format, s.streams)
	if s.bytes > s.total {
		return s, fmt.Errorf("%d %s COPYs processed %d bytes of a %d byte table: the total is wrong",
			s.streams, s.format, s.bytes, s.total)
	}
	return s, nil
}

// nativeFaultFullXID widens an assigned xid to xid8: an assigned xid lies within 2^31 of
// any recent snapshot xmax, above it while still in progress or below it once completed.
func nativeFaultFullXID(xid uint32, xmax uint64) (uint64, error) {
	delta := int64(int32(xid - uint32(xmax)))
	if delta < 0 && uint64(-delta) > xmax {
		return 0, fmt.Errorf("xid %d lies before transaction ID 0 at xmax %d", xid, xmax)
	}
	return xmax + uint64(delta), nil
}

// midCopy is the kill condition: rows are on the way and a transaction that would commit them is known.
func (s nativeFaultCopySample) midCopy() bool {
	return s.present && s.bytes > 0 && s.bytes < s.total && s.xid != ""
}

func (s nativeFaultCopySample) complete() bool { return s.present && s.bytes == s.total }

// nativeFaultBulk is the spec-local plan table whose COPY the fault interrupts. probeGap is
// the longest time between two probe starts, which bounds how late the probe can see the COPY.
type nativeFaultBulk struct {
	rows                int64
	size                nativeFaultBulkSize
	probes              int
	probeGap, probeExec time.Duration
}

// createNativeFaultBulk creates the bulk table on the source as the app role, which owns
// every fixture table; the drop runs after the Migration's own cleanup.
func createNativeFaultBulk() *nativeFaultBulk {
	GinkgoHelper()
	DeferCleanup(func() { psql(sourceCluster, "DROP TABLE IF EXISTS "+nativeFaultBulkTable) })
	psqlBulk(sourceCluster, "SET ROLE "+sqlIdent(appRole(sourceCluster))+"; "+nativeFaultBulkSQL(nativeFaultBulkRows))
	rows, size, err := parseNativeFaultBulkTotal(psql(sourceCluster, nativeFaultBulkTotalSQL))
	Expect(err).NotTo(HaveOccurred())
	Expect(rows).To(Equal(nativeFaultBulkRows), "the bulk table holds the wrong row count")
	Expect(size.text).To(BeNumerically(">=", nativeFaultBulkBytes), "the bulk table COPY is smaller than sized")
	return &nativeFaultBulk{rows: rows, size: size}
}

// waitMidCopy polls the target until the bulk table is mid-copy and returns that sample.
func (b *nativeFaultBulk) waitMidCopy(stop func(Gomega) string) nativeFaultCopySample {
	GinkgoHelper()
	var sample nativeFaultCopySample
	var last time.Time
	Eventually(func(g Gomega) {
		now := time.Now()
		if !last.IsZero() {
			b.probeGap = max(b.probeGap, now.Sub(last))
		}
		last = now
		b.probes++
		if reason := stop(g); reason != "" {
			StopTrying(reason + " before the bulk table was seen mid-copy").Now()
		}
		start := time.Now()
		out, err := psqlDBErr(targetCluster, appDatabase(targetCluster), nativeFaultBulkCopySQL)
		b.probeExec = max(b.probeExec, time.Since(start))
		g.Expect(err).NotTo(HaveOccurred())
		s, err := parseNativeFaultCopy(out, b.size)
		if err != nil {
			StopTrying(err.Error()).Now()
		}
		if s.complete() {
			StopTrying("the bulk table COPY was seen only complete").Now()
		}
		g.Expect(s.midCopy()).To(BeTrue(), "the bulk table is not mid-copy: %+v", s)
		sample = s
	}, suiteWait(migrationTimeout), 250*time.Millisecond).Should(Succeed())
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
	Expect(status).To(Equal("aborted"), "the killed COPY transaction %s ended %q: the kill missed the COPY", xid, status)
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

// report records a midCopy sample, so bytes is above zero. With several streams the window
// projects from their summed bytes over the longest elapsed time.
func (b *nativeFaultBulk) report(s nativeFaultCopySample) {
	window := fmt.Sprintf("%.1fs", s.elapsed*float64(s.total)/float64(s.bytes))
	AddReportEntry("native fault copy kill", fmt.Sprintf("table=%s rows=%d format=%s streams=%d total_bytes=%d"+
		" bytes_at_kill=%d (%.1f%%) copy_elapsed=%.3fs projected_window=%s %s xid=%s", nativeFaultBulkTable, b.rows,
		s.format, s.streams, s.total, s.bytes, 100*float64(s.bytes)/float64(s.total), s.elapsed, window,
		b.probeTimes(), s.xid))
}

// probeTimes reports the probe timings. A single probe has no gap, so its gap reads
// unmeasured, never 0s.
func (b *nativeFaultBulk) probeTimes() string {
	gap := "unmeasured"
	if b.probes > 1 {
		gap = b.probeGap.Round(time.Millisecond).String()
	}
	return fmt.Sprintf("probes=%d probe_gap_max=%s probe_exec_max=%s", b.probes, gap, b.probeExec.Round(time.Millisecond))
}

func TestNativeFaultBulkProbeTimes(t *testing.T) {
	first := &nativeFaultBulk{probes: 1, probeExec: 1148 * time.Millisecond}
	if got, want := first.probeTimes(), "probes=1 probe_gap_max=unmeasured probe_exec_max=1.148s"; got != want {
		t.Errorf("a first-probe hit reports %q, want %q", got, want)
	}
	later := &nativeFaultBulk{probes: 3, probeGap: 1400 * time.Millisecond, probeExec: 1148 * time.Millisecond}
	if got, want := later.probeTimes(), "probes=3 probe_gap_max=1.4s probe_exec_max=1.148s"; got != want {
		t.Errorf("a later hit reports %q, want %q", got, want)
	}
}

func TestNativeFaultCopyProgress(t *testing.T) {
	const engineCopy = "|rpgmigrate[42] copy"
	text := nativeFaultBulkSize{text: 1000, binaryRows: 2000}
	for _, tc := range []struct {
		name          string
		out           string
		mid, complete bool
	}{
		{"missing row", "", false, false},
		{"zero bytes", "0|0.250|7|8|text" + engineCopy, false, false},
		{"partial", "400|3.000|7|8|text" + engineCopy, true, false},
		{"partial without a transaction", "400|3.000||8|text" + engineCopy, false, false},
		{"last byte pending", "999|9.000|7|8|text" + engineCopy, true, false},
		{"complete", "1000|10.000|7|8|text" + engineCopy, false, true},
		{"binary one stream", "2000|9.000|7|8|binary" + engineCopy, true, false},
		{"binary one stream complete", "2021|9.000|7|8|binary" + engineCopy, false, true},
		{"text parts", "400|3.000|7|8|text" + engineCopy + "\n500|2.000|9|8|text" + engineCopy, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := parseNativeFaultCopy(tc.out, text)
			if err != nil {
				t.Fatal(err)
			}
			if s.midCopy() != tc.mid || s.complete() != tc.complete {
				t.Fatalf("%+v: mid %t complete %t", s, s.midCopy(), s.complete())
			}
		})
	}
	for _, out := range []string{
		"1001|1|7|8|text", "-1|1|7|8|text", "x|1|7|8|text", "1|x|7|8|text", "1|-1|7|8|text", "1|1|0|8|text",
		"1|1|7;|8|text", "1|1|4294967296|8|text", "1|1|7|0|text", "1|1|7|x|text", "1|1|7||text",
		"1|1|4294967295|5|text", "1|1|7|8||", "1|1|7|8|csv", "1|1|7|8|", "2022|1|7|8|binary",
		"1|1|7|8|text" + engineCopy + "\n2|1|7|8", "1|1|7|8|text" + engineCopy + "\n2|1|7|8|binary",
		"600|1|7|8|text" + engineCopy + "\n401|1|9|8|text", "1|1|7|8|text" + engineCopy + "\n",
	} {
		if _, err := parseNativeFaultCopy(out+engineCopy, text); err == nil {
			t.Errorf("COPY progress %q admitted", out+engineCopy)
		}
	}
	for _, out := range []string{"1|1|7|8", "1|1|7|8|text", "1|1|7|8|text|", "1|1|7|8|text|psql",
		"1|1|7|8|text|rpgmigrate[42] apply", "1|1|7|8|text|pgcopydb[42] copy", "1|1|7|8|text|xrpgmigrate[42] copy",
		"1|1|7|8|text|rpgmigrate[42] copy ", "1|1|7|8|text" + engineCopy + "\n1|1|7|8|text|rpgmigrate[43] copy",
		"1|1|7|8|text" + engineCopy + "\n1|1|7|8|text|e2e-decoy-copy"} {
		if _, err := parseNativeFaultCopy(out, text); err == nil {
			t.Errorf("COPY progress %q admitted", out)
		}
	}
	for _, out := range []string{
		"", "1", "0|10|20", "10|0|20", "10|20|0", "x|10|20", "10|x|20", "10|20|x", "1|2", "1|2|3|4",
	} {
		if _, _, err := parseNativeFaultBulkTotal(out); err == nil {
			t.Errorf("bulk total %q admitted", out)
		}
	}
	for _, tc := range []struct{ xid, xmax, full uint64 }{
		{4741, 4741, 4741},
		{4745, 4741, 4745},
		{4000, 4741, 4000},
		{1<<32 - 6, 1<<32 + 10, 1<<32 - 6},
		{2, 1<<32 - 3, 1<<32 + 2},
		{7, 3<<32 + 5, 3<<32 + 7},
	} {
		if full, err := nativeFaultFullXID(uint32(tc.xid), tc.xmax); err != nil || full != tc.full {
			t.Errorf("xid %d at xmax %d = %d, %v; want %d", tc.xid, tc.xmax, full, err, tc.full)
		}
	}
	if nativeFaultBulkRows*nativeFaultBulkPayloadBytes != nativeFaultBulkBytes {
		t.Errorf("%d rows of %d bytes do not make %d", nativeFaultBulkRows, nativeFaultBulkPayloadBytes,
			nativeFaultBulkBytes)
	}
}

// TestNativeFaultCopySplitBinary reads the shape the operator defaults give the bulk table:
// binary parts whose summed bytes pass the one-stream text size, so text sizing would refuse them.
func TestNativeFaultCopySplitBinary(t *testing.T) {
	const engineCopy = "|binary|rpgmigrate[42] copy"
	size := nativeFaultBulkSize{text: 1000, binaryRows: 1500}
	out := "700" + "|4.000|9|12" + engineCopy + "\n" + "520|2.500|11|12" + engineCopy + "\n" + "0|0.100||12" + engineCopy
	s, err := parseNativeFaultCopy(out, size)
	if err != nil {
		t.Fatal(err)
	}
	want := nativeFaultCopySample{present: true, format: "binary", streams: 3, bytes: 1220,
		total: 1500 + 3*nativeFaultBinaryFraming, elapsed: 4, xid: "11", xidStreamBytes: 520}
	if s != want || !s.midCopy() || s.complete() {
		t.Fatalf("split binary sample = %+v, want %+v mid-copy", s, want)
	}
	if _, err := parseNativeFaultCopy(out, nativeFaultBulkSize{text: 1000, binaryRows: 1100}); err == nil {
		t.Fatal("parts past the binary size were admitted")
	}
}

// TestNativeFaultCopySQL runs the bulk, progress and commit-status SQL against a real
// server: the total must equal the COPY stream, and a COPY held open must read mid-copy.
func TestNativeFaultCopySQL(t *testing.T) {
	run := testPSQL(t)
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	t.Cleanup(func() { testQuery(t, run, "DROP TABLE IF EXISTS "+nativeFaultBulkTable) })
	testQuery(t, run, nativeFaultBulkSQL(64))
	rows, size, err := parseNativeFaultBulkTotal(testQuery(t, run, nativeFaultBulkTotalSQL))
	if err != nil || rows != 64 {
		t.Fatalf("bulk total: %d rows, %v", rows, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	copyOut := func(format, predicate string) []byte {
		t.Helper()
		out, err := exec.CommandContext(ctx, "psql", uri, "-XqtA", "-c", "COPY (SELECT id, payload FROM ONLY "+
			nativeFaultBulkTable+predicate+") TO STDOUT WITH (FORMAT "+format+")").Output()
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	stream := copyOut("text", "")
	if int64(len(stream)) != size.text {
		t.Fatalf("COPY stream is %d bytes, total SQL says %d", len(stream), size.text)
	}
	// Two ctid parts, as the engine splits a table, carry the binary rows plus two framings.
	binary := nativeFaultCopyBinary
	first, second := len(copyOut(binary, " WHERE ctid < '(1,0)'")), len(copyOut(binary, " WHERE ctid >= '(1,0)'"))
	whole := len(copyOut(binary, ""))
	if first <= nativeFaultBinaryFraming || second <= nativeFaultBinaryFraming ||
		int64(whole) != size.stream(binary, 1) || int64(first+second) != size.stream(binary, 2) {
		t.Fatalf("binary COPY is %d bytes whole and %d+%d in two parts, total SQL says %d and %d",
			whole, first, second, size.stream(binary, 1), size.stream(binary, 2))
	}
	total := size.text
	sample := func() nativeFaultCopySample {
		t.Helper()
		s, err := parseNativeFaultCopy(testQuery(t, run, nativeFaultBulkCopySQL), size)
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
		cmd, stdin := startTestCopy(ctx, t, uri, "rpgmigrate[test] copy", stream[:half])
		var err error
		var mid nativeFaultCopySample
		for deadline := time.Now().Add(30 * time.Second); !mid.midCopy(); time.Sleep(50 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("a COPY fed %d of %d bytes never read mid-copy: %+v", half, total, mid)
			}
			mid = sample()
		}
		if mid.bytes > int64(half) || mid.format != "text" || mid.streams != 1 || mid.total != total {
			t.Fatalf("COPY of %d fed bytes read as %+v, want one text stream of %d", half, mid, total)
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
			err = killTestCopy(t, cmd)
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
	expectDecoyCopyRefused(ctx, t, run, uri, stream[:half], size)
	if got := testQuery(t, run, "SELECT count(*) FROM ONLY "+nativeFaultBulkTable); got != "64" {
		t.Fatalf("bulk table holds %s rows after one committed and one killed COPY", got)
	}
}

// startTestCopy feeds head to a COPY into the bulk table in a session named name.
func startTestCopy(ctx context.Context, t *testing.T, uri, name string, head []byte) (*exec.Cmd, io.WriteCloser) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "psql", uri, "-Xq", "-v", "ON_ERROR_STOP=1")
	cmd.Env = append(cmd.Environ(), "PGAPPNAME="+name)
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
	if _, err := stdin.Write(head); err != nil {
		t.Fatal(err)
	}
	return cmd, stdin
}

func killTestCopy(t *testing.T, cmd *exec.Cmd) error {
	t.Helper()
	err := cmd.Process.Kill()
	if waitErr := cmd.Wait(); !errors.As(waitErr, new(*exec.ExitError)) {
		t.Fatalf("killed psql exited %v", waitErr)
	}
	return err
}

// expectDecoyCopyRefused runs the COPY an engine without session names would run: the
// probe must refuse it, never read it as no COPY.
func expectDecoyCopyRefused(
	ctx context.Context, t *testing.T, run psqlScript, uri string, head []byte, size nativeFaultBulkSize,
) {
	t.Helper()
	const decoy = "e2e-decoy-copy"
	cmd, _ := startTestCopy(ctx, t, uri, decoy, head)
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		_, err := parseNativeFaultCopy(testQuery(t, run, nativeFaultBulkCopySQL), size)
		if err != nil {
			if !strings.Contains(err.Error(), `application_name "`+decoy+`"`) {
				t.Fatal(err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the probe never saw the decoy's COPY into the bulk table")
		}
	}
	if err := killTestCopy(t, cmd); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(30 * time.Second); testQuery(t, run, nativeFaultBulkCopySQL) != ""; {
		if time.Now().After(deadline) {
			t.Fatal("the decoy's COPY outlived its session")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

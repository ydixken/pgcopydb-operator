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
	"slices"
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

// expectRows holds the bulk table to the source: every row there and after a resume, and on a
// target that failed closed exactly the rows of the parts its current copy generation recorded done.
// Split parts commit one by one, so whole done parts are legitimate there.
func (b *nativeFaultBulk) expectRows(target, run string, resumed bool) {
	GinkgoHelper()
	read := func(cluster string, parts []nativeFaultDonePart) nativeFaultBulkContent {
		content, err := parseNativeFaultBulkContent(psqlDBWithin(cluster, appDatabase(cluster),
			nativeFaultBulkContentSQL(b.rows, parts), bulkWriteTimeout))
		Expect(err).NotTo(HaveOccurred())
		return content
	}
	Expect(read(sourceCluster, nil).whole(b.rows)).To(Succeed(), "the source lost or changed bulk rows")
	if !resumed && psql(target, "SELECT to_regclass('"+nativeFaultBulkTable+"') IS NULL") == "t" {
		return
	}
	if resumed {
		Expect(read(target, nil).whole(b.rows)).To(Succeed(), "INVARIANT VIOLATED: the resumed target bulk rows")
		return
	}
	source, err := strconv.ParseUint(psql(sourceCluster, nativeFaultBulkOIDSQL), 10, 32)
	Expect(err).NotTo(HaveOccurred(), "the source bulk table has no OID")
	parts, err := parseNativeFaultDoneParts(psql(target, nativeFaultDonePartsSQL(run)))
	Expect(err).NotTo(HaveOccurred(), "INVARIANT VIOLATED: the target's done parts")
	for i := range parts {
		Expect(parts[i].readSource(psql(sourceCluster, parts[i].sourceSQL()))).To(Succeed())
	}
	Expect(read(target, parts).failedClosed(parts, uint32(source))).
		To(Succeed(), "INVARIANT VIOLATED: the failed-closed target bulk rows")
}

const (
	nativeFaultBulkOID    = "to_regclass('" + nativeFaultBulkTable + "')::oid"
	nativeFaultBulkOIDSQL = "SELECT " + nativeFaultBulkOID
	// nativeFaultBulkMismatch admits a row only as one the source bulk table generated, so NULLs fail too.
	nativeFaultBulkMismatch = "NOT coalesce(id BETWEEN 1 AND %d AND payload = repeat(md5(id::text), %d), false)"
)

// nativeFaultBulkContentSQL reads the bulk table's row count, distinct ids, rows unlike any source row,
// and rows outside every one of parts' source id ranges.
func nativeFaultBulkContentSQL(rows int64, parts []nativeFaultDonePart) string {
	inside := "false"
	for _, p := range parts {
		inside += fmt.Sprintf(" OR id BETWEEN %d AND %d", p.lo, p.hi)
	}
	return "SELECT count(*) || '|' || count(DISTINCT id) || '|' || count(*) FILTER (WHERE " +
		fmt.Sprintf(nativeFaultBulkMismatch, rows, nativeFaultBulkPayloadRepeat) + ") || '|' ||" +
		" count(*) FILTER (WHERE NOT coalesce(" + inside + ", false)) FROM ONLY " + nativeFaultBulkTable
}

// nativeFaultDonePartsSQL reads the engine's owned control rows: how many clone controls the target
// holds, then every bulk table part its current copy generation recorded done.
func nativeFaultDonePartsSQL(run string) string {
	schema := sqlIdent(nativeSchemaName(run))
	return "WITH c AS (SELECT (clone_state::json->>'generation')::numeric AS generation FROM ONLY " + schema +
		".checkpoint WHERE clone_state IS NOT NULL) SELECT (SELECT count(*) FROM c) || '|' || coalesce((SELECT" +
		" string_agg(concat_ws(':', d.part_number, d.part_count, d.rows, d.source_table, d.start_block," +
		" coalesce(d.end_block::text, '')), ',' ORDER BY d.part_number) FROM ONLY " + schema + ".copy_done d" +
		" WHERE d.copy_generation = (SELECT generation FROM c) AND d.target_table = " + nativeFaultBulkOID +
		"), '')"
}

type nativeFaultBulkContent struct{ rows, distinct, foreign, outside int64 }

func parseNativeFaultBulkContent(out string) (nativeFaultBulkContent, error) {
	var c nativeFaultBulkContent
	fields := strings.Split(out, "|")
	if len(fields) != 4 {
		return c, fmt.Errorf("bulk content returned %q", out)
	}
	for i, field := range []*int64{&c.rows, &c.distinct, &c.foreign, &c.outside} {
		n, err := strconv.ParseInt(fields[i], 10, 64)
		if err != nil || n < 0 {
			return c, fmt.Errorf("bulk content returned %q", out)
		}
		*field = n
	}
	return c, nil
}

// whole is the source's bulk table, all of it and nothing else.
func (c nativeFaultBulkContent) whole(rows int64) error {
	if c.rows != rows || c.distinct != rows || c.foreign != 0 {
		return fmt.Errorf("%d bulk rows, %d distinct ids and %d unlike the source, want %d of each and none unlike",
			c.rows, c.distinct, c.foreign, rows)
	}
	return nil
}

// failedClosed requires each target row once, as the source holds it, inside a done part, and as many
// rows as those parts recorded: so a torn, duplicated, extra or misplaced part fails.
func (c nativeFaultBulkContent) failedClosed(parts []nativeFaultDonePart, source uint32) error {
	var sum int64
	seen := map[int]bool{}
	for _, p := range parts {
		if p.source != source || p.number < 1 || p.number > p.count || p.count != parts[0].count || seen[p.number] {
			return fmt.Errorf("done part %+v is not one part of source table %d", p, source)
		}
		if p.sourceRows != p.rows || p.hi-p.lo+1 != p.rows {
			return fmt.Errorf("done part %+v differs from the source rows in its blocks", p)
		}
		seen[p.number], sum = true, sum+p.rows
	}
	switch {
	case c.foreign != 0:
		return fmt.Errorf("%d target bulk rows are unlike any source row", c.foreign)
	case c.distinct != c.rows:
		return fmt.Errorf("%d target bulk rows hold %d distinct ids: rows were copied twice", c.rows, c.distinct)
	case c.outside != 0:
		return fmt.Errorf("%d target bulk rows lie outside every done part", c.outside)
	case c.rows != sum:
		return fmt.Errorf("%d target bulk rows, but the %d parts recorded done hold %d", c.rows, len(parts), sum)
	}
	return nil
}

// nativeFaultDonePart is one copy_done row of the bulk table, and the source ids its ctid blocks hold.
type nativeFaultDonePart struct {
	number, count      int
	rows               int64
	source             uint32
	start              int64
	end                string // empty: the part runs to the end of the table
	sourceRows, lo, hi int64
}

func parseNativeFaultDoneParts(out string) ([]nativeFaultDonePart, error) {
	controls, list, ok := strings.Cut(out, "|")
	if !ok || controls != "1" {
		return nil, fmt.Errorf("done parts returned %q, not one clone control", out)
	}
	var parts []nativeFaultDonePart
	for entry := range strings.SplitSeq(list, ",") {
		if list == "" {
			break
		}
		f := strings.Split(entry, ":")
		if len(f) != 6 {
			return nil, fmt.Errorf("done part %q", entry)
		}
		var p nativeFaultDonePart
		var errs [5]error
		p.number, errs[0] = strconv.Atoi(f[0])
		p.count, errs[1] = strconv.Atoi(f[1])
		p.rows, errs[2] = strconv.ParseInt(f[2], 10, 64)
		source, err := strconv.ParseUint(f[3], 10, 32)
		p.source, errs[3] = uint32(source), err
		p.start, errs[4] = strconv.ParseInt(f[4], 10, 64)
		if err := errors.Join(errs[:]...); err != nil || p.rows < 0 || p.start < 0 {
			return nil, fmt.Errorf("done part %q: %v", entry, err)
		}
		if p.end = f[5]; p.end != "" {
			if end, err := strconv.ParseInt(p.end, 10, 64); err != nil || end <= p.start {
				return nil, fmt.Errorf("done part %q ends at block %q", entry, p.end)
			}
		}
		parts = append(parts, p)
	}
	return parts, nil
}

// sourceSQL reads the source rows in the part's half-open ctid block range, as the engine copies it.
func (p nativeFaultDonePart) sourceSQL() string {
	predicate := fmt.Sprintf("ctid >= '(%d,0)'::tid", p.start)
	if p.end != "" {
		predicate += fmt.Sprintf(" AND ctid < '(%s,0)'::tid", p.end)
	}
	return "SELECT count(*) || '|' || coalesce(min(id), 0) || '|' || coalesce(max(id), -1) FROM ONLY " +
		nativeFaultBulkTable + " WHERE " + predicate
}

func (p *nativeFaultDonePart) readSource(out string) error {
	fields := strings.Split(out, "|")
	if len(fields) == 3 {
		var errs [3]error
		p.sourceRows, errs[0] = strconv.ParseInt(fields[0], 10, 64)
		p.lo, errs[1] = strconv.ParseInt(fields[1], 10, 64)
		p.hi, errs[2] = strconv.ParseInt(fields[2], 10, 64)
		if errors.Join(errs[:]...) == nil {
			return nil
		}
	}
	return fmt.Errorf("source rows of done part %+v returned %q", *p, out)
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

// TestNativeFaultFailedClosed holds a failed-closed target to whole done parts: two of three parts done.
func TestNativeFaultFailedClosed(t *testing.T) {
	const source = 16384
	part := func(number, count int, rows, lo int64) nativeFaultDonePart {
		return nativeFaultDonePart{number: number, count: count, rows: rows, source: source,
			sourceRows: rows, lo: lo, hi: lo + rows - 1}
	}
	done := []nativeFaultDonePart{part(1, 3, 300, 1), part(3, 3, 200, 501)}
	whole := nativeFaultBulkContent{rows: 500, distinct: 500}
	unsplit := part(1, 1, 900, 1)
	for _, tc := range []struct {
		name    string
		content nativeFaultBulkContent
		parts   []nativeFaultDonePart
	}{
		{"no part done and no row", nativeFaultBulkContent{}, nil},
		{"whole done parts", whole, done},
		{"one unsplit part", nativeFaultBulkContent{rows: 900, distinct: 900}, []nativeFaultDonePart{unsplit}},
		{"an empty done part", whole, append([]nativeFaultDonePart{{number: 2, count: 3, source: source, hi: -1}}, done...)},
	} {
		if err := tc.content.failedClosed(tc.parts, source); err != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	marker := func(change func(*nativeFaultDonePart)) []nativeFaultDonePart {
		parts := slices.Clone(done)
		change(&parts[1])
		return parts
	}
	for _, tc := range []struct {
		name    string
		content nativeFaultBulkContent
		parts   []nativeFaultDonePart
	}{
		{"a torn part", nativeFaultBulkContent{rows: 499, distinct: 499}, done},
		{"a duplicated part", nativeFaultBulkContent{rows: 700, distinct: 500}, done},
		{"a duplicated row in place of a torn one", nativeFaultBulkContent{rows: 500, distinct: 499}, done},
		{"an extra row of an undone part", nativeFaultBulkContent{rows: 501, distinct: 501, outside: 1}, done},
		{"an extra row unlike the source", nativeFaultBulkContent{rows: 501, distinct: 501, foreign: 1}, done},
		{"a row changed from the source", nativeFaultBulkContent{rows: 500, distinct: 500, foreign: 1}, done},
		{"a part copied from the wrong blocks", nativeFaultBulkContent{rows: 500, distinct: 500, outside: 200}, done},
		{"a count that matches no done-part sum", whole, done[:1]},
		{"rows with no part done", nativeFaultBulkContent{rows: 300, distinct: 300}, nil},
		{"a part number twice", whole, marker(func(p *nativeFaultDonePart) { p.number = 1 })},
		{"part number zero", whole, marker(func(p *nativeFaultDonePart) { p.number = 0 })},
		{"a part past its count", whole, marker(func(p *nativeFaultDonePart) { p.number = 4 })},
		{"two part counts", whole, marker(func(p *nativeFaultDonePart) { p.count = 4 })},
		{"another source table", whole, marker(func(p *nativeFaultDonePart) { p.source++ })},
		{"a marker over its blocks' rows", whole, marker(func(p *nativeFaultDonePart) { p.sourceRows-- })},
		{"blocks of scattered ids", whole, marker(func(p *nativeFaultDonePart) { p.hi++ })},
	} {
		if err := tc.content.failedClosed(tc.parts, source); err == nil {
			t.Errorf("%s admitted", tc.name)
		}
	}
	if err := whole.whole(500); err != nil {
		t.Error(err)
	}
	for _, c := range []nativeFaultBulkContent{{rows: 499, distinct: 499}, {rows: 500, distinct: 499},
		{rows: 500, distinct: 500, foreign: 1}} {
		if c.whole(500) == nil {
			t.Errorf("%+v read as the whole bulk table", c)
		}
	}
	parts, err := parseNativeFaultDoneParts("1|1:3:300:16384:0:10,3:3:200:16384:20:")
	want := []nativeFaultDonePart{{number: 1, count: 3, rows: 300, source: source, end: "10"},
		{number: 3, count: 3, rows: 200, source: source, start: 20}}
	if err != nil || !slices.Equal(parts, want) {
		t.Errorf("done parts = %+v, %v; want %+v", parts, err, want)
	}
	if parts, err := parseNativeFaultDoneParts("1|"); err != nil || len(parts) != 0 {
		t.Errorf("no done part = %+v, %v", parts, err)
	}
	for _, out := range []string{"", "0|", "2|", "|", "1", "1|1:3:300:16384:0", "1|x:3:300:16384:0:", "1|1:3:-1:16384:0:",
		"1|1:3:300:4294967296:0:", "1|1:3:300:16384:-1:", "1|1:3:300:16384:5:5", "1|1:3:300:16384:5:x",
		"1|1:3:300:16384:0:,"} {
		if _, err := parseNativeFaultDoneParts(out); err == nil {
			t.Errorf("done parts %q admitted", out)
		}
	}
	for _, out := range []string{"", "1|1|0", "1|1|0|0|0", "x|1|0|0", "1|1|-1|0"} {
		if _, err := parseNativeFaultBulkContent(out); err == nil {
			t.Errorf("bulk content %q admitted", out)
		}
	}
	p := nativeFaultDonePart{}
	for _, out := range []string{"", "1|1", "x|1|1", "1|1|1|1"} {
		if p.readSource(out) == nil {
			t.Errorf("source rows %q admitted", out)
		}
	}
}

// TestNativeFaultFailedClosedSQL runs the done-part and content SQL against a real server, over control
// rows shaped like the engine's: one current part done, a stale generation's part, and another table's.
func TestNativeFaultFailedClosedSQL(t *testing.T) {
	run := testPSQL(t)
	const id = "0123456789abcdef0123456789abcdef"
	schema := sqlIdent(nativeSchemaName(id))
	t.Cleanup(func() {
		testQuery(t, run, "DROP TABLE IF EXISTS "+nativeFaultBulkTable+"; DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})
	testQuery(t, run, nativeFaultBulkSQL(64))
	content := func(script string, parts []nativeFaultDonePart) nativeFaultBulkContent {
		t.Helper()
		c, err := parseNativeFaultBulkContent(testQuery(t, run, script+nativeFaultBulkContentSQL(64, parts)+"; ROLLBACK"))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	if err := content("BEGIN; ", nil).whole(64); err != nil {
		t.Fatal(err)
	}
	if err := content("BEGIN; UPDATE "+nativeFaultBulkTable+" SET payload = payload || 'x' WHERE id = 7; ", nil).
		whole(64); err == nil {
		t.Fatal("a changed payload read as the source's")
	}
	oid := testQuery(t, run, nativeFaultBulkOIDSQL)
	marker := func(generation, number int, start, end string) string {
		return fmt.Sprintf("INSERT INTO %s.copy_done SELECT %d, %s, %s, count(*), %d, 2, %s, %s FROM ONLY %s"+
			" WHERE ctid >= '(%s,0)'::tid AND (%[7]s IS NULL OR ctid < ('(' || %[7]s || ',0)')::tid);",
			schema, generation, oid, oid, number, start, end, nativeFaultBulkTable, start)
	}
	testQuery(t, run, "CREATE SCHEMA "+schema+"; CREATE TABLE "+schema+".checkpoint (clone_state text);"+
		" CREATE TABLE "+schema+".copy_done (copy_generation numeric, source_table oid, target_table oid, rows numeric,"+
		" part_number int4, part_count int4, start_block int8, end_block int8);"+
		" INSERT INTO "+schema+".checkpoint VALUES ('{\"generation\":2,\"phase\":\"copying\"}'), (NULL);"+
		marker(2, 1, "0", "1")+marker(1, 2, "1", "NULL")+
		" INSERT INTO "+schema+".copy_done SELECT copy_generation, source_table, 1, rows, 2, 2, 1, NULL FROM "+
		schema+".copy_done WHERE part_number = 1;")
	parts, err := parseNativeFaultDoneParts(testQuery(t, run, nativeFaultDonePartsSQL(id)))
	if err != nil || len(parts) != 1 || parts[0].number != 1 || parts[0].start != 0 || parts[0].end != "1" {
		t.Fatalf("done parts = %+v, %v; want part 1 of generation 2 alone", parts, err)
	}
	if err := parts[0].readSource(testQuery(t, run, parts[0].sourceSQL())); err != nil {
		t.Fatal(err)
	}
	if p := parts[0]; p.rows <= 0 || p.rows >= 64 || p.sourceRows != p.rows || p.lo != 1 || p.hi != p.rows {
		t.Fatalf("part 1 = %+v, want the first block's ids 1 to %d", p, p.rows)
	}
	source, _ := strconv.ParseUint(oid, 10, 32)
	// The target that failed closed holds part 1 alone; each mutation rolls back.
	testQuery(t, run, "DELETE FROM "+nativeFaultBulkTable+" WHERE ctid >= '(1,0)'::tid")
	if err := content("BEGIN; ", parts).failedClosed(parts, uint32(source)); err != nil {
		t.Fatalf("the whole done part was refused: %v", err)
	}
	bulk := nativeFaultBulkTable
	// The engine builds the primary key after the data, so a failed-closed target may lack it.
	unkeyed := "ALTER TABLE " + bulk + " DROP CONSTRAINT e2e_nfault_bulk_pkey, ALTER id DROP NOT NULL; "
	for _, mutation := range []string{
		"DELETE FROM " + bulk + " WHERE id = 1",
		unkeyed + "INSERT INTO " + bulk + " SELECT * FROM " + bulk + " WHERE id = 1",
		fmt.Sprintf("INSERT INTO %s VALUES (64, repeat(md5('64'), %d))", bulk, nativeFaultBulkPayloadRepeat),
		fmt.Sprintf("DELETE FROM %s WHERE id = 1; INSERT INTO %[1]s VALUES (64, repeat(md5('64'), %d))", bulk,
			nativeFaultBulkPayloadRepeat),
		"UPDATE " + bulk + " SET payload = 'x' WHERE id = 1",
		unkeyed + "INSERT INTO " + bulk + " SELECT NULL, payload FROM " + bulk + " WHERE id = 1",
	} {
		if err := content("BEGIN; "+mutation+"; ", parts).failedClosed(parts, uint32(source)); err == nil {
			t.Errorf("a target after %q admitted", mutation)
		}
	}
	for script, want := range map[string]string{
		"DELETE FROM " + schema + ".checkpoint": "0|",
		"UPDATE " + schema + ".checkpoint SET clone_state = '{\"generation\":3}' WHERE clone_state IS NOT NULL": "1|",
	} {
		if got := testQuery(t, run, "BEGIN; "+script+"; "+nativeFaultDonePartsSQL(id)+"; ROLLBACK"); got != want {
			t.Errorf("after %s the done parts read %q, want %q", script, got, want)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := run(ctx, "BEGIN; UPDATE "+schema+".checkpoint SET clone_state = '{\"generation\":2}';"+
		nativeFaultDonePartsSQL(id)+"; ROLLBACK"); err == nil {
		t.Errorf("two clone controls read as %q", out)
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

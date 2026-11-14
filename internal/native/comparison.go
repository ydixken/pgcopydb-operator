package native

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

// Reserve 4MiB for bounded connection files and filesystem overhead.
const ComparisonReportLimit int64 = 60 * 1024 * 1024

const (
	comparisonCheckSchema = "schema"
	comparisonCheckData   = "data"
)

var ErrNativeComparisonUnavailable = errors.New("native comparison unavailable")

// ComparisonOutcome conveys no ownership, checkpoint, or completion authority.
type ComparisonOutcome struct {
	known, matched bool
	progress       ComparisonProgress
}

// ComparisonProgress is a data compare's final progress sample, which saw every planned table compared.
type ComparisonProgress struct {
	Tables, Bytes, Rows, DifferingTables, ElapsedMs int64
}

func (o ComparisonOutcome) Result() (matched, known bool) { return o.matched, o.known }

// Progress is the final sample of a known data comparison; a schema comparison has none.
func (o ComparisonOutcome) Progress() (ComparisonProgress, bool) {
	return o.progress, o.known && o.progress.Tables > 0
}

type comparisonWire struct {
	Version          int64        `json:"version"`
	Kind             string       `json:"kind"`
	OperationID      string       `json:"operation_id"`
	ProgramSHA256    string       `json:"program_sha256"`
	Work             WorkIdentity `json:"work"`
	Check            string       `json:"check"`
	Matched          bool         `json:"matched"`
	EngineExit       int64        `json:"engine_exit"`
	ReportBytes      int64        `json:"report_bytes"`
	TableCount       int64        `json:"table_count"`
	PlannedBytes     int64        `json:"planned_bytes"`
	ComparedRows     int64        `json:"compared_rows"`
	DifferingTables  int64        `json:"differing_tables"`
	ElapsedMs        int64        `json:"elapsed_ms"`
	ChildClosed      bool         `json:"child_closed"`
	ReportClosed     bool         `json:"report_closed"`
	PrivateClosed    bool         `json:"private_closed"`
	DiagnosticsClean bool         `json:"diagnostics_clean"`
}

func ParseComparisonOutput(output podexec.ComparisonOutput, work WorkIdentity, operationID, check string, programSHA256 [32]byte) (ComparisonOutcome, error) {
	return parseComparisonPayload(output.Payload(), work, operationID, check, programSHA256)
}

func parseComparisonPayload(raw []byte, work WorkIdentity, operationID, check string, programSHA256 [32]byte) (ComparisonOutcome, error) {
	refuse := func() (ComparisonOutcome, error) { return ComparisonOutcome{}, ErrNativeComparisonUnavailable }
	if !sourceWorkIdentity(work) || !lowerHex(operationID, 32) || (check != comparisonCheckSchema && check != comparisonCheckData) || programSHA256 != ComparisonProgramSHA256() || len(raw) < 2 || len(raw) > nativeWireLimit || raw[len(raw)-1] != '\n' || bytes.ContainsRune(raw[:len(raw)-1], '\n') || !utf8.Valid(raw) {
		return refuse()
	}
	d := json.NewDecoder(bytes.NewReader(raw[:len(raw)-1]))
	d.UseNumber()
	if !comparisonValue(d, false) {
		return refuse()
	}
	if _, err := d.Token(); err != io.EOF {
		return refuse()
	}
	var result comparisonWire
	if json.Unmarshal(raw, &result) != nil || !result.matches(work, operationID, check, programSHA256) {
		return refuse()
	}
	if result.Matched && result.EngineExit != 0 || !result.Matched && result.EngineExit != 3 || check == comparisonCheckSchema && result.TableCount != 0 || check == comparisonCheckData && result.TableCount == 0 {
		return refuse()
	}
	progress := ComparisonProgress{Tables: result.TableCount, Bytes: result.PlannedBytes, Rows: result.ComparedRows, DifferingTables: result.DifferingTables, ElapsedMs: result.ElapsedMs}
	if !progress.valid(check, result.Matched) {
		return refuse()
	}
	return ComparisonOutcome{known: true, matched: result.Matched, progress: progress}, nil
}

// A schema comparison logs no progress; a data one counts its differing tables in its verdict.
func (p ComparisonProgress) valid(check string, matched bool) bool {
	if check == comparisonCheckSchema {
		return p == ComparisonProgress{}
	}
	return p.Bytes >= 0 && p.Rows >= 0 && p.ElapsedMs >= 0 && p.DifferingTables >= 0 && p.DifferingTables <= p.Tables && matched == (p.DifferingTables == 0)
}

func (result comparisonWire) matches(work WorkIdentity, operationID, check string, programSHA256 [32]byte) bool {
	return result.Version == 1 && result.Kind == "native_comparison" && result.OperationID == operationID &&
		result.ProgramSHA256 == hex.EncodeToString(programSHA256[:]) && result.Work == work && result.Check == check &&
		result.ReportBytes >= 1 && result.ReportBytes <= ComparisonReportLimit && result.TableCount >= 0 &&
		result.ChildClosed && result.ReportClosed && result.PrivateClosed && result.DiagnosticsClean
}

func comparisonValue(d *json.Decoder, work bool) bool {
	keys := "version kind operation_id program_sha256 work check matched engine_exit report_bytes table_count planned_bytes compared_rows differing_tables elapsed_ms child_closed report_closed private_closed diagnostics_clean"
	if work {
		keys = sourceWorkWireKeys
	}
	want := strings.Fields(keys)
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] || !strings.Contains(" "+keys+" ", " "+key+" ") {
			return false
		}
		seen[key] = true
		if key == "work" {
			if work || !comparisonValue(d, true) {
				return false
			}
			continue
		}
		value, err := d.Token()
		if err != nil || value == nil {
			return false
		}
		switch n := value.(type) {
		case json.Number:
			v, err := strconv.ParseInt(string(n), 10, 64)
			if err != nil || strconv.FormatInt(v, 10) != string(n) {
				return false
			}
		case string, bool:
		default:
			return false
		}
	}
	token, err = d.Token()
	return err == nil && token == json.Delim('}') && len(seen) == len(want)
}

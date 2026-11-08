package native

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

const (
	sharedFixtureRoot          = "testdata/v1"
	fixtureManifestName        = "manifest.json"
	fixtureVersionKind         = "version"
	fixtureReportKind          = "report"
	fixtureTransportKind       = "transport"
	fixtureCompareArrayKind    = "compare_array"
	fixtureCompareProgressKind = "compare_progress"
	fixtureArgvKind            = "argv"
)

type sharedCase struct {
	ID      string          `json:"id"`
	Kind    string          `json:"kind"`
	Payload string          `json:"payload"`
	Accept  bool            `json:"accept"`
	Binding fixtureBinding  `json:"binding"`
	Expect  json.RawMessage `json:"expect"`
}

type fixtureBinding struct {
	Command    string       `json:"command"`
	Work       WorkIdentity `json:"work"`
	EndLSN     string       `json:"end_lsn"`
	Checkpoint *struct {
		CheckpointID     string `json:"checkpoint_id"`
		CertifiedLSN     string `json:"certified_lsn"`
		DurableReplayLSN string `json:"durable_replay_lsn"`
		DrainEndLSN      string `json:"drain_end_lsn"`
		DrainReplayLSN   string `json:"drain_replay_lsn"`
		DrainKind        string `json:"drain_kind"`
		BarrierConfirmed bool   `json:"barrier_confirmed"`
	} `json:"checkpoint"`
	Resources []struct {
		Kind   string `json:"kind"`
		Name   string `json:"name"`
		Owned  bool   `json:"owned"`
		Retain bool   `json:"retain"`
	} `json:"resources"`
}

func (b fixtureBinding) binding() Binding {
	result := Binding{Command: b.Command, Work: b.Work, EndLSN: b.EndLSN}
	if c := b.Checkpoint; c != nil {
		result.Checkpoint = &CheckpointEvidence{
			CheckpointID: c.CheckpointID, CertifiedLSN: c.CertifiedLSN,
			DurableReplayLSN: c.DurableReplayLSN, DrainEndLSN: c.DrainEndLSN,
			DrainReplayLSN: c.DrainReplayLSN, DrainKind: c.DrainKind, BarrierConfirmed: c.BarrierConfirmed,
		}
	}
	for _, r := range b.Resources {
		result.Resources = append(result.Resources, ResourceObligation{Kind: r.Kind, Name: r.Name, Owned: r.Owned, Retain: r.Retain})
	}
	return result
}

func readSharedFile(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(sharedFixtureRoot, name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func fixtureJSON(raw []byte) bool {
	if !utf8.Valid(raw) || !bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{")) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value func(int) bool
	value = func(depth int) bool {
		token, err := decoder.Token()
		if err != nil || depth > 32 {
			return false
		}
		delimiter, container := token.(json.Delim)
		if !container {
			return true
		}
		seen := make(map[string]bool)
		for decoder.More() {
			if delimiter == '{' {
				key, err := decoder.Token()
				name, ok := key.(string)
				if err != nil || !ok || seen[name] {
					return false
				}
				seen[name] = true
			}
			if !value(depth + 1) {
				return false
			}
		}
		_, err = decoder.Token()
		return err == nil
	}
	if !value(0) {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}

func sharedCases(t *testing.T) []sharedCase {
	t.Helper()
	var manifest struct {
		ProtocolVersion int `json:"protocol_version"`
		Files           []struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}
	raw := readSharedFile(t, fixtureManifestName)
	if !fixtureJSON(raw) || json.Unmarshal(raw, &manifest) != nil || manifest.ProtocolVersion != 1 || len(manifest.Files) == 0 {
		t.Fatal("invalid or empty shared fixture manifest")
	}
	expected := make([]string, 0, len(manifest.Files))
	for _, entry := range manifest.Files {
		if !fs.ValidPath(entry.Path) || entry.Path == fixtureManifestName || slices.Contains(expected, entry.Path) {
			t.Fatal("invalid or duplicate shared fixture path")
		}
		digest := sha256.Sum256(readSharedFile(t, entry.Path))
		if hex.EncodeToString(digest[:]) != entry.SHA256 {
			t.Fatalf("changed shared fixture: %s", entry.Path)
		}
		expected = append(expected, entry.Path)
	}
	var actual []string
	err := filepath.WalkDir(sharedFixtureRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			t.Fatal("shared fixtures must not be symlinks")
		}
		if !entry.IsDir() {
			name, err := filepath.Rel(sharedFixtureRoot, path)
			if err != nil {
				return err
			}
			if name != fixtureManifestName {
				actual = append(actual, filepath.ToSlash(name))
			}
		}
		return nil
	})
	if err != nil || !slices.IsSorted(expected) || !slices.Equal(expected, actual) {
		t.Fatal("shared fixture file inventory differs")
	}
	var inventory struct {
		ProtocolVersion int          `json:"protocol_version"`
		Cases           []sharedCase `json:"cases"`
	}
	raw = readSharedFile(t, "cases.json")
	if !fixtureJSON(raw) || json.Unmarshal(raw, &inventory) != nil || inventory.ProtocolVersion != 1 || len(inventory.Cases) == 0 {
		t.Fatal("invalid or empty shared cases")
	}
	seen := make(map[string]bool)
	for _, c := range inventory.Cases {
		if c.ID == "" || seen[c.ID] || !slices.Contains([]string{fixtureVersionKind, fixtureReportKind, fixtureTransportKind, fixtureCompareArrayKind, fixtureCompareProgressKind, fixtureArgvKind}, c.Kind) {
			t.Fatal("invalid or duplicate shared case")
		}
		seen[c.ID] = true
		if c.Kind != fixtureArgvKind && !slices.Contains(expected, c.Payload) {
			t.Fatal("shared case payload is absent")
		}
	}
	return inventory.Cases
}

// Framing is a contract check; M6 owns production log-tail selection.
func fixtureTransport(raw []byte, binding Binding) bool {
	var records [][]byte
	for line := range bytes.SplitSeq(raw, []byte("\n")) {
		if payload, ok := bytes.CutPrefix(line, []byte("native-report: ")); ok {
			records = append(records, payload)
		}
	}
	if len(records) != 1 {
		return false
	}
	_, err := ParseReport(records[0], binding)
	return err == nil
}

func TestSharedNativeFixtures(t *testing.T) {
	kinds := make(map[string]int)
	for _, c := range sharedCases(t) {
		kinds[c.Kind]++
		if c.Kind == fixtureArgvKind {
			continue // The engine's parser oracle executes these inputs.
		}
		t.Run(c.ID, func(t *testing.T) {
			raw := readSharedFile(t, c.Payload)
			var accepted bool
			switch c.Kind {
			case fixtureVersionKind:
				_, err := ParseVersion(raw)
				accepted = err == nil
			case fixtureReportKind:
				_, err := ParseReport(raw, c.Binding.binding())
				accepted = err == nil
			case fixtureTransportKind:
				accepted = fixtureTransport(raw, c.Binding.binding())
			case fixtureCompareArrayKind:
				var rows []struct {
					Schema string `json:"schema"`
					Name   string `json:"name"`
					Status string `json:"status"`
					Source struct {
						Rowcount *int64 `json:"rowcount"`
						Checksum string `json:"checksum"`
					} `json:"source"`
				}
				accepted = json.Unmarshal(raw, &rows) == nil && len(rows) > 0
				for _, row := range rows {
					accepted = accepted && row.Schema != "" && row.Name != "" && row.Status == "match" && row.Source.Rowcount != nil && row.Source.Checksum != ""
				}
			case fixtureCompareProgressKind:
				got, ok := pgcopydb.LastCompareProgress(string(raw))
				var want pgcopydb.CompareProgress
				if err := json.Unmarshal(c.Expect, &want); err != nil {
					t.Fatal(err)
				}
				accepted = ok && reflect.DeepEqual(got, want) && strings.HasPrefix(string(raw), pgcopydb.CompareProgressPrefix)
			}
			if accepted != c.Accept {
				t.Fatalf("fixture acceptance = %t, want %t", accepted, c.Accept)
			}
		})
	}
	for _, kind := range []string{fixtureVersionKind, fixtureReportKind, fixtureTransportKind, fixtureCompareArrayKind, fixtureCompareProgressKind, fixtureArgvKind} {
		if kinds[kind] == 0 {
			t.Fatalf("empty shared fixture group: %s", kind)
		}
	}
}

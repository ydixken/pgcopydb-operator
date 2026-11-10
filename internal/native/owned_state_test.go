package native

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/ydixken/pgcopydb-operator/internal/podexec"
)

const ownedFixtureWorkPath = "/work/pgcopydb"
const ownedFixtureCheckpointKey = "checkpoint"

func ownedWant() ExecutionData {
	w := retryWant()
	w.Role, w.ProgramSHA256 = JobRoleCleanup, OwnedInventoryProgramSHA256()
	return w
}

func ownedResult(t *testing.T, w ExecutionData, body []byte) []byte {
	t.Helper()
	request, execution := requestDigests(body)
	v := ownedResultWire{Version: 1, Kind: ownedInventoryKind, OperationID: w.OperationID, ProgramSHA256: hex.EncodeToString(w.ProgramSHA256[:]), RequestSHA256: hex.EncodeToString(request[:]), ExecutionSHA256: hex.EncodeToString(execution[:]), Work: w.Work, Resources: []resourceWire{{Kind: evidenceReplicationSlotKind, Name: "confirmed_slot", Owned: true}, {Kind: evidencePublicationKind, Name: "confirmed_publication", Owned: true}, {Kind: resourceKindWorkdir, Name: ownedFixtureWorkPath, Owned: true, Retain: true}}, SourceClosed: true, TargetClosed: true, MaterializationClosed: true, PrivateClosed: true, DiagnosticsClean: true}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func TestNativeOwnedStateWire(t *testing.T) {
	w := ownedWant()
	body, err := ownedRequest(w)
	if err != nil {
		t.Fatal(err)
	}
	raw := ownedResult(t, w, body)
	if _, err := parseOwnedResult(raw, w, body); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"saved_e", ownedFixtureCheckpointKey, "token", "complete", "admitted"} {
		if bytes.Contains(body, []byte(`"`+key+`"`)) || bytes.Contains(raw, []byte(`"`+key+`"`)) {
			t.Fatalf("unexpected authority field %s", key)
		}
	}
	if _, err := parseSourceRequest(body); err == nil {
		t.Fatal("inventory parsed as source request")
	}
	for _, root := range []string{"request", "retry_request", "result", "target_retry", "source_head", "active_lag"} {
		var v any
		if decodeNativeWire(body, &v, root) || decodeNativeWire(bytes.TrimSuffix(raw, []byte{'\n'}), &v, root) {
			t.Fatalf("inventory parsed as %s", root)
		}
	}
	for _, tc := range []struct {
		name string
		edit func([]byte) []byte
	}{
		{readerCaseDuplicate, func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
		}},
		{readerCaseUnknown, func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1,"saved_e":"0/20"`), 1)
		}},
		{readerKindKey, func(b []byte) []byte {
			return bytes.ReplaceAll(b, []byte(ownedInventoryKind), []byte(completedEvidenceKind))
		}},
		{"fractional", func(b []byte) []byte { return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1.0`), 1) }},
		{"null", func(b []byte) []byte { return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":null`), 1) }},
		{readerCaseOverflow, func(b []byte) []byte { return append(b, bytes.Repeat([]byte{' '}, nativeWireLimit)...) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseOwnedRequest(tc.edit(body)); err == nil {
				t.Fatal("invalid request accepted")
			}
			if _, err := parseOwnedResult(tc.edit(raw), w, body); err == nil {
				t.Fatal("invalid result accepted")
			}
		})
	}
	for _, path := range [][]string{nil, {readerExecutionKey}, {readerExecutionKey, evidenceWork}, {sourceContainerKey}} {
		var object map[string]any
		_ = json.Unmarshal(body, &object)
		for _, p := range path {
			object = object[p].(map[string]any)
		}
		for key := range object {
			t.Run(strings.Join(path, ".")+"/"+key, func(t *testing.T) {
				var root map[string]any
				_ = json.Unmarshal(body, &root)
				node := root
				for _, p := range path {
					node = node[p].(map[string]any)
				}
				delete(node, key)
				mutated, _ := json.Marshal(root)
				if _, err := parseOwnedRequest(mutated); err == nil {
					t.Fatal("missing request member accepted")
				}
			})
		}
	}
	atLimit := append(bytes.Repeat([]byte{' '}, nativeWireLimit-len(raw)), raw...)
	if _, err := parseOwnedResult(atLimit, w, body); err != nil {
		t.Fatal("bounded result at exact limit refused")
	}
	if _, err := parseOwnedResult(append([]byte{' '}, atLimit...), w, body); err == nil {
		t.Fatal("oversized result accepted")
	}
	for _, bad := range [][]byte{nil, {}, raw[:len(raw)-1], append(bytes.Clone(raw), '\n'), append(bytes.Clone(raw), raw...), append([]byte("noise\n"), raw...), append(bytes.Clone(raw), 'x')} {
		if _, err := parseOwnedResult(bad, w, body); err == nil {
			t.Fatal("invalid result framing accepted")
		}
	}
	for _, key := range []string{readerOperationIDKey, preflightProgramKey, sourceRequestSHAKey, readerExecutionSHAKey} {
		var v map[string]any
		_ = json.Unmarshal(raw, &v)
		v[key] = strings.Repeat("0", 64)
		bad, _ := json.Marshal(v)
		if _, err := parseOwnedResult(append(bad, '\n'), w, body); err == nil {
			t.Fatal("wrong correlation accepted")
		}
	}
	if _, err := parseOwnedResult(raw, w, append(bytes.Clone(body), ' ')); err == nil {
		t.Fatal("result accepted different exact request bytes")
	}
}

func TestNativeOwnedStateNonAuthority(t *testing.T) {
	var zero AdmittedOwnedState
	if zero.Work() != (WorkIdentity{}) || zero.Resources() != nil {
		t.Fatal("zero state admitted")
	}
	typ := reflect.TypeFor[AdmittedOwnedState]()
	for _, name := range []string{"Checkpoint", "Evidence", "Endpos", "Completed", "Token", "ACK"} {
		if _, ok := typ.MethodByName(name); ok {
			t.Fatalf("inventory exposes %s", name)
		}
	}
	for field := range typ.Fields() {
		if field.IsExported() {
			t.Fatal("inventory state has public admission fields")
		}
	}
	w := ownedWant()
	body, err := ownedRequest(w)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseCompletedRequest(body); err == nil {
		t.Fatal("inventory request admitted completion")
	}
	for _, change := range []func(*ExecutionData){func(w *ExecutionData) { w.Role = JobRoleEvidence }, func(w *ExecutionData) { w.JobUID = w.WorkerJobUID }, func(w *ExecutionData) { w.ProgramSHA256 = EvidenceProgramSHA256() }, func(w *ExecutionData) { w.Work.CheckpointGeneration = 2 }, func(w *ExecutionData) { w.Container.RestartCount = 1 }} {
		bad := w
		change(&bad)
		if _, err := ownedRequest(bad); err == nil {
			t.Fatal("invalid execution admitted")
		}
	}
	api, r, w := newOwnedReaderAPI(t)
	r.transport = readerZeroTransport{}
	if got, err := r.ReadOwnedState(context.Background(), w); err == nil || got.Work() != (WorkIdentity{}) || got.Resources() != nil {
		t.Fatal("unchecked outcome admitted ownership")
	}
	if api.writerCalls != 0 {
		t.Fatal("zero transport reached relay")
	}
	if sha256.Sum256(EvidenceProgram()) == OwnedInventoryProgramSHA256() {
		t.Fatal("inventory reused completed program identity")
	}
	if (podexec.NativeOutcome{}).Successful() {
		t.Fatal("zero transport outcome is successful")
	}
}

func TestNativeOwnedStateClosure(t *testing.T) {
	w := ownedWant()
	body, err := ownedRequest(w)
	if err != nil {
		t.Fatal(err)
	}
	raw := ownedResult(t, w, body)
	var baseline map[string]any
	_ = json.Unmarshal(raw, &baseline)
	for key := range baseline {
		t.Run("required/"+key, func(t *testing.T) {
			var v map[string]any
			_ = json.Unmarshal(raw, &v)
			delete(v, key)
			bad, _ := json.Marshal(v)
			if _, err := parseOwnedResult(append(bad, '\n'), w, body); err == nil {
				t.Fatal("missing result field accepted")
			}
		})
	}
	for _, key := range []string{preflightSourceClosedKey, preflightTargetClosedKey, retryMaterializationClosedKey, sourcePrivateClosedKey, preflightDiagnosticsKey} {
		for _, value := range []any{false, nil, 1, "true"} {
			var v map[string]any
			_ = json.Unmarshal(raw, &v)
			v[key] = value
			bad, _ := json.Marshal(v)
			if _, err := parseOwnedResult(append(bad, '\n'), w, body); err == nil {
				t.Fatal("unclosed result admitted")
			}
		}
	}
	for _, edit := range []func([]resourceWire) []resourceWire{
		func([]resourceWire) []resourceWire { return nil }, func([]resourceWire) []resourceWire { return []resourceWire{} }, func(r []resourceWire) []resourceWire { return r[:2] }, func(r []resourceWire) []resourceWire { return append(r, r[0]) }, func(r []resourceWire) []resourceWire { r[0].Owned = false; return r }, func(r []resourceWire) []resourceWire { r[0].Name = strings.Repeat("x", 64); return r }, func(r []resourceWire) []resourceWire { r[1].Name = strings.Repeat("x", 64); return r }, func(r []resourceWire) []resourceWire { r[1].Retain = true; return r }, func(r []resourceWire) []resourceWire { r[2].Name = "/work/foreign"; return r }, func(r []resourceWire) []resourceWire {
			return append(r, resourceWire{Kind: nativeResourceOrigin, Name: "origin", Owned: true})
		}, func(r []resourceWire) []resourceWire { return append(r, resourceWire{Kind: "foreign", Name: "object"}) },
	} {
		var v ownedResultWire
		_ = json.Unmarshal(raw, &v)
		v.Resources = edit(v.Resources)
		bad, _ := json.Marshal(v)
		if _, err := parseOwnedResult(append(bad, '\n'), w, body); err == nil {
			t.Fatal("invalid resource obligations admitted")
		}
	}
	var v ownedResultWire
	_ = json.Unmarshal(raw, &v)
	v.Resources = append(v.Resources, resourceWire{Kind: nativeResourceOrigin, Name: "recorded_origin"})
	withOrigin, _ := json.Marshal(v)
	if _, err := parseOwnedResult(append(withOrigin, '\n'), w, body); err != nil {
		t.Fatal("unowned origin refused")
	}
	for _, name := range []string{strings.Repeat("o", 63), strings.Repeat("o", 64), strings.Repeat("é", 31) + "o", strings.Repeat("é", 32)} {
		v.Resources[len(v.Resources)-1].Name = name
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		_, err = parseOwnedResult(append(raw, '\n'), w, body)
		if (err == nil) != (len(name) <= 63) {
			t.Fatal("unowned origin differs from the native FollowState byte bound")
		}
	}
}

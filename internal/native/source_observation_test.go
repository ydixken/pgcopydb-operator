package native

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func sourceResult(t *testing.T, kind string, w ExecutionData, body []byte) []byte {
	t.Helper()
	r, e := requestDigests(body)
	v := map[string]any{fixtureVersionKind: 1, readerKindKey: kind, readerOperationIDKey: w.OperationID, preflightProgramKey: hex.EncodeToString(w.ProgramSHA256[:]), sourceRequestSHAKey: hex.EncodeToString(r[:]), readerExecutionSHAKey: hex.EncodeToString(e[:]), evidenceWork: w.Work, "source_binding_sha256": strings.Repeat("b", 64), preflightSourceClosedKey: true, sourcePrivateClosedKey: true, preflightDiagnosticsKey: true}
	if kind == sourceHeadKind {
		v[sourceHeadLSNKey] = fixtureEndLSN
	} else {
		v[sourceLagBytesKey] = 0
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

func mutateWire(t *testing.T, raw []byte, path []string, mutation func(map[string]any, string)) []byte {
	t.Helper()
	var v map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&v) != nil {
		t.Fatal("test document invalid")
	}
	node := v
	for _, key := range path[:len(path)-1] {
		node = node[key].(map[string]any)
	}
	mutation(node, path[len(path)-1])
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestNativeInformationalLagAdmission(t *testing.T) {
	w := sourceWant()
	fixture := sha256.Sum256([]byte(`[123,11,1,16,"00112233445566778899aabbccddeeff",1,1,"01010101010101010101010101010101",1,8,88,33]`))
	if hex.EncodeToString(fixture[:]) != "fe1781120b07a00ef1128150a37c78e97584c53d585ee972367ff67a745f9b59" {
		t.Fatal("canonical noncredential source tuple encoding changed")
	}
	for _, kind := range []string{sourceHeadKind, activeLagKind} {
		t.Run(kind, func(t *testing.T) {
			body, err := sourceRequest(kind, w)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parseSourceRequest(body); err != nil {
				t.Fatal(err)
			}
			for _, path := range [][]string{{readerExecutionKey}, {readerExecutionKey, evidenceWork}, {sourceContainerKey}} {
				sourceNestedWireMutations(t, body, path, func(raw []byte) error { _, err := parseSourceRequest(raw); return err })
			}
			sourceRequestMutations(t, body)
			raw := sourceResult(t, kind, w, body)
			if _, err := parseSourceResult(raw, body, kind, w); err != nil {
				t.Fatal(err)
			}
			sourceNestedWireMutations(t, raw, []string{evidenceWork}, func(raw []byte) error { _, err := parseSourceResult(append(raw, '\n'), body, kind, w); return err })
			sourceResultMutations(t, raw, body, kind, w)
		})
	}
}

func sourceNestedWireMutations(t *testing.T, raw []byte, path []string, parse func([]byte) error) {
	t.Helper()
	var root map[string]any
	if json.Unmarshal(raw, &root) != nil {
		t.Fatal("invalid nested wire fixture")
	}
	object := root
	for _, part := range path {
		object = object[part].(map[string]any)
	}
	for key := range object {
		for _, mode := range []string{readerCaseMissing, readerCaseNull, readerCaseFolded, readerCaseUnknown, readerCaseDuplicate} {
			t.Run(strings.Join(path, ".")+"/"+key+"/"+mode, func(t *testing.T) {
				bad := mutateWire(t, raw, append(append([]string{}, path...), key), func(m map[string]any, k string) {
					value := m[k]
					delete(m, k)
					switch mode {
					case readerCaseNull:
						m[k] = nil
					case readerCaseFolded:
						m[strings.ToUpper(k)] = value
					case readerCaseUnknown:
						m[k] = value
						m["unexpected_member"] = true
					case readerCaseDuplicate:
						m[k] = value
					}
				})
				if mode == readerCaseDuplicate {
					bad = mutateWire(t, raw, path, func(m map[string]any, k string) {
						nested, err := json.Marshal(m[k])
						if err != nil {
							t.Fatal(err)
						}
						nested = replace(t, nested, `"`+key+`":`, `"`+key+`":null,"`+key+`":`)
						m[k] = json.RawMessage(nested)
					})
				}
				if parse(bad) == nil {
					t.Fatal("nested wire requirement bypassed")
				}
			})
		}
	}
}

func TestNativeInformationalLagNonAuthority(t *testing.T) {
	if (InformationalLag{}).LagBytes() != nil || (InformationalLag{}).AdmissionFingerprint() != ([32]byte{}) {
		t.Fatal("zero lag object is valid")
	}
	typeOf := reflect.TypeFor[InformationalLag]()
	if typeOf.NumField() != 3 || typeOf.NumMethod() != 2 {
		t.Fatal("informational lag gained an authority surface")
	}
	for _, target := range []reflect.Type{reflect.TypeFor[ConfirmedCheckpoint](), reflect.TypeFor[AdmittedOwnedState](), reflect.TypeFor[CompletedObservation](), reflect.TypeFor[ExecutionRequest]()} {
		if typeOf.ConvertibleTo(target) || reflect.TypeFor[ExecutionData]().ConvertibleTo(target) {
			t.Fatal("data converts to authority")
		}
	}
	w := sourceWant()
	fingerprint := sourceAdmissionFingerprint(w, strings.Repeat("b", 64))
	w.OperationID = strings.Repeat("c", 32)
	if sourceAdmissionFingerprint(w, strings.Repeat("b", 64)) != fingerprint {
		t.Fatal("per-operation nonce changed admission fingerprint")
	}
	for _, mutate := range []func(*ExecutionData){func(w *ExecutionData) { w.Attempt++ }, func(w *ExecutionData) { w.Container.PodUID = evidenceOther }, func(w *ExecutionData) { w.ConnectionReferenceSHA256[0] ^= 1 }, func(w *ExecutionData) { w.Work.WorkdirID = evidenceOther }} {
		bad := w
		mutate(&bad)
		if sourceAdmissionFingerprint(bad, strings.Repeat("b", 64)) == fingerprint {
			t.Fatal("changed admission retained fingerprint")
		}
	}
	if sourceAdmissionFingerprint(w, strings.Repeat("d", 64)) == fingerprint {
		t.Fatal("changed source binding retained fingerprint")
	}
	lag := InformationalLag{valid: true, lagBytes: 0, admissionFingerprint: fingerprint}
	if lag.LagBytes() == nil || *lag.LagBytes() != 0 {
		t.Fatal("valid zero is unknown")
	}
	*lag.LagBytes() = 10
	if *lag.LagBytes() != 0 {
		t.Fatal("caller mutated informational value")
	}
	request := readerWant()
	request.Role = JobRoleWorker
	if validExecutionRequest(request) {
		t.Fatal("active worker bypassed successful original completed-evidence admission")
	}
}

func sourceRequestMutations(t *testing.T, body []byte) {
	t.Helper()
	for _, path := range [][]string{{fixtureVersionKind}, {readerKindKey}, {readerOperationIDKey}, {preflightProgramKey}, {"pod_name"}, {"execution"}, {sourceContainerKey}} {
		for _, mode := range []string{readerCaseMissing, readerCaseNull, readerCaseDuplicate} {
			t.Run(strings.Join(path, ".")+"_"+mode, func(t *testing.T) {
				bad := mutateWire(t, body, path, func(m map[string]any, key string) {
					if mode == readerCaseMissing {
						delete(m, key)
					} else {
						m[key] = nil
					}
				})
				if mode == readerCaseDuplicate {
					bad = replace(t, body, `"`+path[0]+`":`, `"`+path[0]+`":null,"`+path[0]+`":`)
				}
				if _, err := parseSourceRequest(bad); err == nil {
					t.Fatal("invalid request accepted")
				}
			})
		}
	}
	for key := range strings.FieldsSeq(nativeWireKeys["source_request.execution"]) {
		bad := mutateWire(t, body, []string{"execution", key}, func(m map[string]any, k string) { delete(m, k) })
		if _, err := parseSourceRequest(bad); err == nil {
			t.Fatalf("missing execution %s accepted", key)
		}
	}
	for key := range strings.FieldsSeq(nativeWireKeys["source_request.container"]) {
		bad := mutateWire(t, body, []string{sourceContainerKey, key}, func(m map[string]any, k string) { m[k] = nil })
		if _, err := parseSourceRequest(bad); err == nil {
			t.Fatalf("null container %s accepted", key)
		}
	}
	for _, change := range []struct{ old, next string }{{readerVersionJSON, `"version":1.0`}, {readerVersionJSON, `"version":true`}, {sourceAttemptJSON, `"attempt":2147483648`}, {sourceAttemptJSON, `"attempt":0`}, {`"migration_generation":1`, `"migration_generation":9223372036854775808`}, {`"restart_count":0`, `"restart_count":1`}, {`"role":"worker"`, `"role":"evidence"`}, {readerVersionJSON, sourceCaseVersionJSON}, {readerVersionJSON, `"version":1,"unknown":true`}} {
		if _, err := parseSourceRequest(replace(t, body, change.old, change.next)); err == nil {
			t.Fatal("typed request mutation accepted")
		}
	}
	for _, bad := range [][]byte{append(bytes.Clone(body), body...), append(bytes.Clone(body), 0xff), []byte(strings.Repeat(" ", nativeWireLimit+1)), append([]byte{0xef, 0xbb, 0xbf}, body...), body[:len(body)-1]} {
		if _, err := parseSourceRequest(bad); err == nil {
			t.Fatal("malformed request accepted")
		}
	}
}

func sourceResultMutations(t *testing.T, raw, body []byte, kind string, w ExecutionData) {
	t.Helper()
	for key := range strings.FieldsSeq(nativeWireKeys[kind]) {
		for _, mode := range []string{readerCaseMissing, readerCaseNull, readerCaseDuplicate} {
			bad := mutateWire(t, raw, []string{key}, func(m map[string]any, k string) {
				if mode == readerCaseMissing {
					delete(m, k)
				} else {
					m[k] = nil
				}
			})
			if mode == readerCaseDuplicate {
				bad = replace(t, raw, `"`+key+`":`, `"`+key+`":null,"`+key+`":`)
				bad = bytes.TrimSuffix(bad, []byte{'\n'})
			}
			if _, err := parseSourceResult(append(bad, '\n'), body, kind, w); err == nil {
				t.Fatalf("%s result %s accepted", mode, key)
			}
		}
	}
	for _, key := range []string{preflightSourceClosedKey, sourcePrivateClosedKey, preflightDiagnosticsKey, sourceRequestSHAKey, readerExecutionSHAKey, preflightProgramKey, "source_binding_sha256", evidenceWork, readerOperationIDKey, readerKindKey, fixtureVersionKind} {
		bad := mutateWire(t, raw, []string{key}, func(m map[string]any, k string) {
			switch key {
			case preflightSourceClosedKey, sourcePrivateClosedKey, preflightDiagnosticsKey:
				m[k] = false
			case fixtureVersionKind:
				m[k] = 2
			case evidenceWork:
				m[k] = WorkIdentity{}
			default:
				m[k] = "wrong"
			}
		})
		if _, err := parseSourceResult(append(bad, '\n'), body, kind, w); err == nil {
			t.Fatalf("unbound %s accepted", key)
		}
	}
	position := sourceLagBytesKey
	invalid := []any{-1, true, "0", 1.5, json.Number("9223372036854775808")}
	if kind == sourceHeadKind {
		position = sourceHeadLSNKey
		invalid = []any{sourceZeroLSN, "invalid-lsn", 0, true}
	}
	for _, value := range invalid {
		bad := mutateWire(t, raw, []string{position}, func(m map[string]any, k string) { m[k] = value })
		if _, err := parseSourceResult(append(bad, '\n'), body, kind, w); err == nil {
			t.Fatal("invalid position accepted")
		}
	}
	for _, bad := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), raw...), append(bytes.Clone(raw), 'x'), []byte(strings.Repeat(" ", nativeWireLimit) + "\n")} {
		if _, err := parseSourceResult(bad, body, kind, w); err == nil {
			t.Fatal("bad framing accepted")
		}
	}
	if _, err := parseSourceResult(raw, append(bytes.Clone(body), ' '), kind, w); err == nil {
		t.Fatal("raw request bytes were reserialized")
	}
}

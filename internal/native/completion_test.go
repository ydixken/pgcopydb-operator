package native

import "testing"

func TestNativeCompletedCloneCounters(t *testing.T) {
	w := sourceWant().Work
	zero := int64(0)
	good := func() *Report {
		return &Report{ProtocolVersion: 1, EngineVersion: "1", Command: "clone", Capabilities: []Capability{CapCloneSingleDB}, Work: &w, Phase: "completed", Copy: &CopyProgress{Tables: &Counters{Total: &zero, Done: &zero}, Indexes: &Counters{Total: &zero, Done: &zero}, Bytes: &Counters{Total: &zero, Done: &zero}}}
	}
	if err := ValidateCompletedClone(good(), w); err != nil {
		t.Fatal("measured zero clone was refused")
	}
	for name, mutate := range map[string]func(*Report){"phase": func(r *Report) { r.Phase = sourceCopyingPhase }, "command": func(r *Report) { r.Command = nativeProgressCommand }, evidenceWork: func(r *Report) { r.Work = nil }, "copy": func(r *Report) { r.Copy = nil }, "tables": func(r *Report) { r.Copy.Tables = nil }, "indexes": func(r *Report) { r.Copy.Indexes = nil }, "bytes": func(r *Report) { r.Copy.Bytes = nil }, "missing_total": func(r *Report) { r.Copy.Tables.Total = nil }, "missing_done": func(r *Report) { r.Copy.Tables.Done = nil }, "incomplete": func(r *Report) { one := int64(1); r.Copy.Tables.Total = &one }, "negative": func(r *Report) { negative := int64(-1); r.Copy.Bytes.Total = &negative; r.Copy.Bytes.Done = &negative }, "capability": func(r *Report) { r.Capabilities = nil }, fixtureVersionKind: func(r *Report) { r.ProtocolVersion = 2 }} {
		t.Run(name, func(t *testing.T) {
			r := good()
			mutate(r)
			if ValidateCompletedClone(r, w) == nil {
				t.Fatal("incomplete or unbound clone accepted")
			}
		})
	}
	if ValidateCompletedClone(nil, w) == nil {
		t.Fatal("absent report completed")
	}
}

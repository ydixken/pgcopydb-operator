package controller

import (
	"encoding/base64"
	"testing"
)

// Throwaway: prints the O16 goldens as base64 so they can be recorded from CI.
func TestRecordO16Goldens(t *testing.T) {
	for name, got := range legacyRunnerGoldens(t) {
		t.Errorf("GOLDEN %s %s", name, base64.StdEncoding.EncodeToString(got))
	}
}

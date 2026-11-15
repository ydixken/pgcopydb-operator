package native

import (
	"encoding/json"
	"os"
	"testing"
)

// TestCatalogFixturesMatchQualifiedRunner keeps the receipt catalogs the operator checks on the engine source it qualifies.
func TestCatalogFixturesMatchQualifiedRunner(t *testing.T) {
	raw, err := os.ReadFile("testdata/evidence/native_m8_source_manifest.json")
	if err != nil {
		t.Fatal("cannot read the v7 catalog source manifest")
	}
	var manifest struct {
		SourceCommit *string `json:"source_commit"`
	}
	if json.Unmarshal(raw, &manifest) != nil || manifest.SourceCommit == nil {
		t.Fatal("the v7 catalog source manifest records no engine commit")
	}
	if *manifest.SourceCommit != qualifiedRunnerSourceCommit {
		t.Fatalf("v7 catalogs come from engine %s, the qualified runner from %s; run hack/sync-native-catalogs.py", *manifest.SourceCommit, qualifiedRunnerSourceCommit)
	}
}

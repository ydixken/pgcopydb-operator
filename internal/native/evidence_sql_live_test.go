package native

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestNativeEvidenceSQLLive executes the shipped inventory and receipt catalog
// statements on a real server, because PREPARE alone accepted the oid-as-string defect.
func TestNativeEvidenceSQLLive(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Fatal("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance for native evidence SQL regressions")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	if _, err := exec.LookPath("psql"); err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	path, err := filepath.Abs("evidence_sql_live_test.py")
	if err != nil {
		t.Fatal("cannot locate the live evidence SQL tests")
	}
	level, err := exec.Command("psql", uri, "-XAtc", "SHOW wal_level").Output()
	if err != nil {
		t.Fatal("cannot read the test server's wal_level")
	}
	for _, method := range []string{"test_InventoryPublicationVerdicts", "test_ReceiptCatalogVerdicts", "test_InventoryLogicalSlotAndPublication"} {
		t.Run(method, func(t *testing.T) {
			if method == "test_InventoryLogicalSlotAndPublication" && strings.TrimSpace(string(level)) != "logical" {
				t.Skip("the logical slot verdict needs a server with wal_level=logical")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			output, err := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", path, "EvidenceSQLLive."+method).CombinedOutput()
			if err != nil || !bytes.Contains(output, []byte("Ran 1 test")) || !bytes.Contains(output, []byte("\nOK\n")) || bytes.Contains(output, []byte("skipped")) {
				t.Fatalf("live evidence SQL case %s failed: %s", method, output)
			}
		})
	}
}

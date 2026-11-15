package native

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestNativeEvidenceSQLLive executes the shipped receipt catalog statements on a
// real server, because a fixture comparison alone accepted an unfilled engine placeholder.
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
	for _, method := range []string{"test_ReceiptCatalogVerdicts"} {
		t.Run(method, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			output, err := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", path, "EvidenceSQLLive."+method).CombinedOutput()
			if err != nil || !bytes.Contains(output, []byte("Ran 1 test")) || !bytes.Contains(output, []byte("\nOK\n")) || bytes.Contains(output, []byte("skipped")) {
				t.Fatalf("live evidence SQL case %s failed: %s", method, output)
			}
		})
	}
}

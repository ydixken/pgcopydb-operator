package native

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestNativeCompletionSessionsLive runs the cleaned and cleanup-rejoin observations on a real
// server, because the mocked inputs let one one-use source input serve three sessions.
func TestNativeCompletionSessionsLive(t *testing.T) {
	uri := os.Getenv("PGCOPYDB_TEST_PGURI")
	if uri == "" {
		t.Fatal("set PGCOPYDB_TEST_PGURI to a disposable PostgreSQL instance for native completion session regressions")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter is absent")
	}
	if _, err := exec.LookPath("psql"); err != nil {
		t.Fatal("PGCOPYDB_TEST_PGURI is set but psql is missing")
	}
	path, err := filepath.Abs("completion_sessions_live_test.py")
	if err != nil {
		t.Fatal("cannot locate the live completion session tests")
	}
	level, err := exec.Command("psql", uri, "-XAtc", "SHOW wal_level").Output()
	if err != nil {
		t.Fatal("cannot read the test server's wal_level")
	}
	const inventory = `import ast,json,sys
t=ast.parse(open(sys.argv[1]).read())
print(json.dumps(sorted(f.name for c in t.body if isinstance(c,ast.ClassDef) and c.name=='CompletionSessionsLive' for f in c.body if isinstance(f,ast.FunctionDef) and f.name.startswith('test_'))))
`
	output, err := exec.Command(python, "-I", "-B", "-c", inventory, path).Output()
	var methods []string
	want := []string{"test_CleanedRefusesPresentPublication", "test_CleanedVerdicts", "test_RejoinLogicalSlotAndPublicationVerdict", "test_RejoinLogicalSlotVerdicts", "test_RejoinPublicationVerdict", "test_RejoinRefusesRemovedLogicalSlot"}
	if err != nil || json.Unmarshal(output, &methods) != nil || !slices.Equal(methods, want) {
		t.Fatalf("live completion session inventory changed: %v %v", methods, err)
	}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			if strings.Contains(method, "LogicalSlot") && strings.TrimSpace(string(level)) != "logical" {
				if os.Getenv("PGCOPYDB_TEST_REQUIRE_LOGICAL") != "" {
					t.Fatalf("PGCOPYDB_TEST_REQUIRE_LOGICAL is set but the test server's wal_level is %q", strings.TrimSpace(string(level)))
				}
				t.Skip("the logical slot verdicts need a server with wal_level=logical")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			output, err := exec.CommandContext(ctx, python, "-I", "-B", "-W", "error", path, "CompletionSessionsLive."+method).CombinedOutput()
			if err != nil || !bytes.Contains(output, []byte("Ran 1 test")) || !bytes.Contains(output, []byte("\nOK\n")) || bytes.Contains(output, []byte("skipped")) {
				t.Fatalf("live completion session case %s failed: %s", method, output)
			}
		})
	}
}

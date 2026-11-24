/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package buildconfig

import (
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	runnerClientSelector = "../../images/runner/client-selector.sh"
	targetPGURI          = "PGCOPYDB_TARGET_PGURI"
	targetMajor          = "TARGET_MAJOR"
)

func TestRunnerClientSelector(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("no sh available: %v", err)
	}

	root := t.TempDir()
	selector := read(t, runnerClientSelector)
	toolsRoot := filepath.Join(root, "tools")
	commandDir := filepath.Join(root, "commands")
	fakeBin := filepath.Join(root, "bin")
	for _, dir := range []string{toolsRoot, commandDir, fakeBin} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	writeExecutable := func(path, contents string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, major := range []string{"16", "18"} {
		toolDir := filepath.Join(toolsRoot, major)
		if err := os.MkdirAll(toolDir, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, tool := range []string{"pg_dump", "pg_restore"} {
			writeExecutable(filepath.Join(toolDir, tool), "#!/bin/sh\nprintf '%s\\n' 'client"+major+"-"+tool+"'\n")
		}
	}
	writeExecutable(filepath.Join(fakeBin, "psql"), `#!/bin/sh
if [ "${FAIL_PSQL:-}" = 1 ]; then
  printf '%s\n' "$PGCOPYDB_TARGET_PGURI" >&2
  exit 1
fi
if [ -n "${PSQL_CALL_LOG:-}" ]; then printf 'called\n' >> "$PSQL_CALL_LOG"; fi
printf '%s\n' "$TARGET_MAJOR"
`)
	realPgcopydb := filepath.Join(root, "pgcopydb-real")
	writeExecutable(realPgcopydb, `#!/bin/sh
pg_dump --version
pg_restore --version
`)
	for _, tool := range []string{"pgcopydb", "pg_dump", "pg_restore"} {
		writeExecutable(filepath.Join(commandDir, tool), selector)
	}

	cache := filepath.Join(root, "client-major")
	callLog := filepath.Join(root, "psql-calls")
	path := strings.Join([]string{fakeBin, commandDir, os.Getenv("PATH")}, string(os.PathListSeparator))
	run := func(t *testing.T, tool string, env map[string]string, wantSuccess bool, want string) string {
		t.Helper()
		cmd := exec.Command(sh, "./"+tool, "--version")
		cmd.Dir = commandDir
		cmd.Env = runnerSelectorEnv(os.Environ(), map[string]string{
			"PATH":                  path,
			"PGCOPYDB_TOOLS_ROOT":   toolsRoot,
			"PGCOPYDB_CLIENT_CACHE": cache,
			"PGCOPYDB_REAL_BINARY":  realPgcopydb,
			targetPGURI:             "postgresql://user@target/database",
			targetMajor:             "16",
			"PSQL_CALL_LOG":         callLog,
			"PGCOPYDB_CLIENT_MAJOR": "",
			"FAIL_PSQL":             "",
		}, env)
		out, err := cmd.CombinedOutput()
		if wantSuccess && err != nil {
			t.Fatalf("%s failed: %v\n%s", tool, err, out)
		}
		if !wantSuccess && err == nil {
			t.Fatalf("%s unexpectedly succeeded:\n%s", tool, out)
		}
		if !strings.Contains(string(out), want) {
			t.Fatalf("%s output does not contain %q:\n%s", tool, want, out)
		}
		return string(out)
	}

	t.Run("target major selects and caches matching tools", func(t *testing.T) {
		out := run(t, "pgcopydb", map[string]string{targetMajor: "16"}, true, "client16-pg_dump")
		if !strings.Contains(out, "client16-pg_restore") {
			t.Fatalf("pgcopydb did not use PostgreSQL 16 pg_restore:\n%s", out)
		}
		if got, err := os.ReadFile(cache); err != nil || string(got) != "16\n" {
			t.Fatalf("cache=%q err=%v, want 16", got, err)
		}

		// Once selected, later commands in the same pod must use that choice
		// rather than querying the target again.
		run(t, "pg_restore", map[string]string{targetMajor: "18"}, true, "client16-pg_restore")
		if got, err := os.ReadFile(callLog); err != nil || string(got) != "called\n" {
			t.Fatalf("psql calls=%q err=%v, want one call before cache reuse", got, err)
		}
	})

	t.Run("default and explicit selection", func(t *testing.T) {
		if err := os.Remove(cache); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		run(t, "pg_dump", map[string]string{targetPGURI: ""}, true, "client18-pg_dump")

		if err := os.WriteFile(cache, []byte("16\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		run(t, "pg_dump", map[string]string{"PGCOPYDB_CLIENT_MAJOR": "18"}, true, "client18-pg_dump")
	})

	t.Run("invalid or unreadable target fails without leaking URI", func(t *testing.T) {
		if err := os.Remove(cache); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		run(t, "pg_dump", map[string]string{targetMajor: "unknown"}, false,
			"runner client selector: target PostgreSQL major is not numeric")

		const secretURI = "postgresql://alice:secret-password@target/database"
		out := run(t, "pg_dump", map[string]string{
			targetPGURI: secretURI,
			"FAIL_PSQL": "1",
		}, false, "runner client selector: cannot read the target PostgreSQL major version")
		if strings.Contains(out, secretURI) || strings.Contains(out, "secret-password") {
			t.Fatalf("selector leaked target credentials:\n%s", out)
		}
	})
}

func runnerSelectorEnv(base []string, defaults, overrides map[string]string) []string {
	values := make(map[string]string, len(defaults)+len(overrides))
	maps.Copy(values, defaults)
	maps.Copy(values, overrides)
	env := make([]string, 0, len(base)+len(values))
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if _, replace := values[key]; ok && replace {
			continue
		}
		env = append(env, entry)
	}
	for key, value := range values {
		env = append(env, key+"="+value)
	}
	return env
}

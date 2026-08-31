# Persistent e2e Live Writer Throughput Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the live writer's per-row `kubectl exec` bottleneck while preserving one committed transaction per ordered row, lifecycle failures, and the data-loss acceptance checks.

**Architecture:** `liveWriter` owns one persistent interactive psql child. Its three private function and duration fields default to `primaryPod`, `exec.Command`, and `liveWriteInterval` in `startLiveWriter`, then let package-local standard-library subprocess tests control the child without a reusable interface. After the stream closes and the child exits, the writer runs one separate marker query and uses its parsed result as `last`.

**Tech Stack:** Go standard library, `os/exec`, Ginkgo v2, Gomega, CNPG-backed e2e tests.

**Spec:** `docs/superpowers/specs/2026-08-31-live-writer-throughput-design.md`

## Global Constraints

- Do not add a dependency, a public test seam, a broad interface, or a reusable streaming helper. The private `primary`, `command`, and `interval` fields exist only to make this one e2e helper testable.
- Keep `sync.Once` cleanup and `defer GinkgoRecover()` in the writer goroutine.
- The persistent path resolves the source primary once, starts one `kubectl exec -i` child, and never reconnects or replaces that stream after a write failure.
- The persistent child command is exactly `kubectl exec -i -n <e2e namespace> <source primary> -c postgres -- psql -U postgres app -q -v ON_ERROR_STOP=1`.
- Every ticker event writes one ordered newline-terminated `INSERT`. psql autocommit makes each successful statement one committed transaction.
- If `cmd.Start` fails after `StdinPipe` succeeds, close stdin, record the Start error first, then record a close error only if it exists. Do not run the final query because no persistent child ran.
- After a started child ends, close stdin, wait for it, then run one separate final query. Do not start a second persistent writer.
- `liveMarkerQuery(marker string) string` must produce exactly `SELECT COALESCE(MAX((substring(note FROM char_length('marker') + 2))::int), 0) FROM orders WHERE note LIKE 'marker-%'` after marker substitution.
- `parseLiveMarker(out []byte) (int, error)` must trim psql output and reject empty or non-integer output.
- Resolve the primary once for the persistent stream and once for the separate final query. The latter is a read, not a stream replacement.
- Preserve the first error from pipe setup, Start, write, close, Wait, final command, or marker parse. Each must make `stop` return a non-nil error.
- Retain `last > 100`, source-target marker count equality, the target gap assertion, full data verification, and replication cleanup assertions.
- `test/e2e` is untagged, but `go test ./test/e2e -run '^TestLiveWriter' -count=1` runs only standard tests whose names start with `TestLiveWriter`. It does not run `TestE2E` or `BeforeSuite`.
- Run `task lint` and `task test` before completion. Run `task e2e` only after a human approves its current-context prompt. Never use `task --yes`.
- This repository is public. Do not commit or paste private endpoints, cluster names, node names, credentials, or secret values.
- No em dashes. No Claude session URLs. Keep comments within repository limits.

---

## File Map and Interfaces

- `test/e2e/e2e_suite_test.go:1434-1494`: owns the persistent writer, `liveMarkerQuery`, `parseLiveMarker`, and private test seams.
- `test/e2e/live_writer_test.go`: new standard-library subprocess tests. It has `TestLiveWriter...` tests and `TestLiveWriterHelper`, which acts as the controlled child only when its helper environment variable is set.
- `test/e2e/e2e_test.go:484-547`: keeps the real-cluster scenario and cross-checks `last` against an independent source query. Its stale retry comment becomes a source-marker comment.
- `tasks/todo.md`: records only completed review, verification, e2e-decision, and delivery facts.

## Test Strategy and TDD Order

Write the local tests first, run the exact named test command, and expect compilation to fail because the current writer has neither the injected fields nor the marker helpers. Implement only the private fields and functions required by those failures, then rerun the same command until it passes.

The helper subprocess is the test binary itself. `TestLiveWriterHelper` exits immediately unless `LIVE_WRITER_HELPER=1`; a parent test starts it with `os.Args[0]`, `-test.run=^TestLiveWriterHelper$`, and mode-specific environment. Stream mode records every input line and writes an EOF marker after stdin closes. Query mode prints the requested scalar. Failure modes either exit nonzero or let the injected command return a deliberately malformed `exec.Cmd`.

Do not add a synthetic writer solely to make `StdinPipe` or OS-pipe `Close` fail. `exec.Cmd{Stdin: ...}` provides the practical pipe-setup failure, and the successful helper proves the normal EOF close and Wait lifecycle. Production code still records a Close error if one occurs.

### Task 1: Add local live-writer tests before implementation

**Files:**

- Create: `test/e2e/live_writer_test.go`

**Interfaces:**

- Consumes after Task 2: `liveWriter{stopCh, done, primary, command, interval}`, `(*liveWriter).run(marker string)`, `(*liveWriter).stop() (int, error)`, `liveMarkerQuery(marker string) string`, and `parseLiveMarker(out []byte) (int, error)`.
- Produces: package-local tests named `TestLiveWriter...`, plus `TestLiveWriterHelper(t *testing.T)` for child-process modes.

- [ ] **Step 1: Add the subprocess helpers and a test writer constructor**

Use standard-library imports only: `bufio`, `errors`, `fmt`, `io`, `os`, `os/exec`, `path/filepath`, `strconv`, `strings`, `sync`, `testing`, and `time`, trimming imports after implementation.

Write `liveWriterForTest` as a test-only literal constructor. It creates `stopCh` and `done`, assigns an injected `primary`, `command`, and short `interval`, then starts `go w.run(marker)`.

Write `helperCommand(t, modes, env)` to return the exact production signature:

```go
func helperCommand(t *testing.T, modes []string, env []string) func(string, ...string) *exec.Cmd
```

Its returned closure records the command name and arguments, consumes one mode per call, and starts the current test binary with `-test.run=^TestLiveWriterHelper$`, `LIVE_WRITER_HELPER=1`, and that mode. After `w.stop()`, tests inspect the recorded calls. They must see two calls after normal shutdown or a post-Start stream failure: first persistent stream, then final query. They must see only the initial call when `StdinPipe` or `Start` fails before a child starts.

The closure starts each helper with this shape, then appends the supplied capture and signal paths to its environment.

```go
cmd := exec.Command(os.Args[0], "-test.run=^TestLiveWriterHelper$")
cmd.Env = append(os.Environ(), append(env, "LIVE_WRITER_HELPER=1", "LIVE_WRITER_MODE="+mode)...)
return cmd
```

Each parent test counts calls with an injected primary such as `func(string) string { primaryCalls++; return "source-0" }`. It reads `primaryCalls` and the recorded command slice only after `stop` returns, so the writer goroutine is finished before the assertions inspect them.

`TestLiveWriterHelper` must implement these modes:

- `stream`: read newline-delimited stdin into a capture file, write the first-line signal after receiving one line, write the EOF signal after `io.EOF`, then exit 0.
- `stream-exit`: write a readiness signal and exit nonzero before accepting an insert.
- `stream-wait-error`: read stdin through EOF, then exit nonzero.
- `query-3`: print `3\n` and exit 0.
- `query-error`: exit nonzero.
- `query-bad`: print `not-an-int\n` and exit 0.

Use a deadline-based `waitForFile` helper rather than a fixed sleep. It polls the signal file until its expected text appears or fails the test after one second.

- [ ] **Step 2: Write the failing pure marker tests**

Add table tests that require:

```go
if got := liveMarkerQuery("live-load"); got !=
	"SELECT COALESCE(MAX((substring(note FROM char_length('live-load') + 2))::int), 0) FROM orders WHERE note LIKE 'live-load-%'" {
	t.Fatalf("query = %q", got)
}
```

Also require `parseLiveMarker([]byte(" 3\n")) == 3` and non-nil errors for `[]byte{} ` and `[]byte("not-an-int\n")`.

- [ ] **Step 3: Write the failing lifecycle tests**

Write focused `t.Run` cases with these expectations:

- Success: modes `stream`, `query-3`; `stop` returns `(3, nil)`; the capture holds ordered, newline-terminated inserts beginning with `<marker>-1`; EOF signal exists; exactly two command calls ran; the persistent call contains `exec -i`; the final call contains `psql -tAc` and `liveMarkerQuery(marker)`; injected primary was called exactly twice.
- `StdinPipe`: the injected first command has its `Stdin` pre-set, so `StdinPipe` fails; `stop` returns that error; no final query call runs.
- Start: the injected first command names a non-existent executable; `stop` returns an error containing `start persistent psql`; no final query call runs. This exercises the Start-failure path that closes the already-created stdin pipe. Do not add a fake pipe to observe its OS-level Close.
- Write and no replacement: `stream-exit`, then `query-3`; wait for the helper readiness signal and then for `w.done`; `stop` returns a write error; exactly two commands ran, proving the second command is the required final query and no replacement stream started.
- Wait: `stream-wait-error`, then `query-3`; wait for the first input signal, call `stop`, and require a `wait for persistent psql` error while the final query still ran.
- Final command: `stream`, then `query-error`; require a `read final marker` error.
- Parse: `stream`, then `query-bad`; require a `parse final marker` error.
- First error: `stream-exit`, then `query-error`; require the write error, not the later final-query error.

Use these concrete injections for the two pre-start failures.

```go
stdinPipeFailure := func(string, ...string) *exec.Cmd {
	cmd := helperCommand(t, []string{"stream"}, nil)("kubectl")
	cmd.Stdin = strings.NewReader("")
	return cmd
}
missing := filepath.Join(t.TempDir(), "missing")
startFailure := func(string, ...string) *exec.Cmd {
	return exec.Command(missing)
}
```

- [ ] **Step 4: Prove the tests are red before Task 2**

Run:

```bash
go test ./test/e2e -run '^TestLiveWriter' -count=1
```

Expected: FAIL at compile time because the current `liveWriter` has no `primary`, `command`, or `interval` fields, and `liveMarkerQuery` plus `parseLiveMarker` do not exist.

- [ ] **Step 5: Do not commit the intentionally failing tests alone**

Keep the test file staged only after Task 2 turns the exact command green. A red standalone commit would violate the repository rule that every commit is lint-clean.

### Task 2: Implement the minimal persistent writer for the local tests

**Files:**

- Modify: `test/e2e/e2e_suite_test.go:1434-1494`
- Test: `test/e2e/live_writer_test.go`

**Interfaces:**

- Consumes: the Task 1 tests and their required fields and helpers.
- Produces: `startLiveWriter(marker string) *liveWriter`, `(*liveWriter).run(marker string)`, `(*liveWriter).stop() (int, error)`, `liveMarkerQuery(marker string) string`, and `parseLiveMarker(out []byte) (int, error)`.

- [ ] **Step 1: Add only the private injectable fields and defaults**

Extend `liveWriter` with:

```go
primary  func(string) string
command  func(string, ...string) *exec.Cmd
interval time.Duration
```

In `startLiveWriter`, initialize those fields with `primaryPod`, `exec.Command`, and `liveWriteInterval` beside the existing channels. Tests construct the same struct directly with their controlled functions. No field is exported and no interface is introduced.

Retain `recordErr(err error)`, which locks `mu` and preserves the first non-nil error.

- [ ] **Step 2: Add the marker helpers**

Add these helpers beside `liveWriter`.

```go
func liveMarkerQuery(marker string) string {
	return fmt.Sprintf(
		"SELECT COALESCE(MAX((substring(note FROM char_length('%s') + 2))::int), 0)"+
			" FROM orders WHERE note LIKE '%s-%%'", marker, marker)
}

func parseLiveMarker(out []byte) (int, error) {
	value := strings.TrimSpace(string(out))
	if value == "" {
		return 0, errors.New("final marker query returned no value")
	}
	return strconv.Atoi(value)
}
```

The query has one caller in production and one test oracle. Keeping it in a function prevents the source query from drifting from the final marker query.

- [ ] **Step 3: Start one child and close stdin on Start failure**

In `run`, defer `close(w.done)` and `GinkgoRecover()`, resolve `pod := w.primary(sourceCluster)` once, and construct the persistent command through `w.command`.

```go
cmd := w.command("kubectl", "exec", "-i", "-n", nsE2E, pod, "-c", "postgres", "--",
	"psql", "-U", "postgres", appDB, "-q", "-v", "ON_ERROR_STOP=1")
stdin, err := cmd.StdinPipe()
if err != nil {
	w.recordErr(fmt.Errorf("open persistent psql stdin: %w", err))
	return
}
if err := cmd.Start(); err != nil {
	w.recordErr(fmt.Errorf("start persistent psql: %w", err))
	if closeErr := stdin.Close(); closeErr != nil {
		w.recordErr(fmt.Errorf("close persistent psql stdin after start failure: %w", closeErr))
	}
	return
}
```

The Start error remains first. Closing stdin after Start failure avoids retaining the pipe writer even when no child was launched.

- [ ] **Step 4: Stream writes once, then always close, wait, and query after a successful Start**

Immediately after successful `Start`, defer this shutdown sequence:

```go
defer func() {
	if err := stdin.Close(); err != nil {
		w.recordErr(fmt.Errorf("close persistent psql stdin: %w", err))
	}
	if err := cmd.Wait(); err != nil {
		w.recordErr(fmt.Errorf("wait for persistent psql: %w", err))
	}

	pod := w.primary(sourceCluster)
	query := liveMarkerQuery(marker)
	out, err := w.command("kubectl", "exec", "-n", nsE2E, pod, "-c", "postgres", "--",
		"psql", "-U", "postgres", appDB, "-tAc", query).Output()
	if err != nil {
		w.recordErr(fmt.Errorf("read final marker: %w", err))
		return
	}
	last, err := parseLiveMarker(out)
	if err != nil {
		w.recordErr(fmt.Errorf("parse final marker: %w", err))
		return
	}
	w.mu.Lock()
	w.last = last
	w.mu.Unlock()
}()
```

Drive `time.NewTicker(w.interval)`. On each tick, write one statement and fail on either a returned write error or a short write:

```go
statement := fmt.Sprintf(
	"INSERT INTO orders (customer_id, amount, note) VALUES (1, 1.00, '%s-%d');\n", marker, n)
written, err := fmt.Fprint(stdin, statement)
if err != nil || written != len(statement) {
	if err == nil {
		err = io.ErrShortWrite
	}
	w.recordErr(fmt.Errorf("write live marker %d: %w", n, err))
	return
}
```

Do not call `psqlDBErr` in the ticker. After a stream error, return to the deferred close, Wait, and final read. The final read is not a replacement stream, and `recordErr` retains the stream error.

- [ ] **Step 5: Keep `stop` and comments accurate**

Keep the existing `sync.Once` close, wait for `done`, and locked `last, err` return.

Replace the current `run` and `stop` comments. State that one psql child receives ordered inserts, and that `stop` returns the source maximum after the child exits rather than an in-memory lower bound.

- [ ] **Step 6: Turn the local test suite green**

Run:

```bash
gofmt -w test/e2e/e2e_suite_test.go test/e2e/live_writer_test.go
go test ./test/e2e -run '^TestLiveWriter' -count=1
go vet ./test/e2e/...
```

Expected: PASS. The named test command runs the subprocess tests only, reports no `TestE2E` execution, and does not reach the Kubernetes API.

- [ ] **Step 7: Commit the tested writer and tests**

Run:

```bash
git add test/e2e/e2e_suite_test.go test/e2e/live_writer_test.go
git commit -m "test(e2e): keep live writes in one psql session"
```

Expected: one lint-clean conventional commit with the writer and its local tests together.

### Task 3: Keep the real-cluster scenario authoritative

**Files:**

- Modify: `test/e2e/e2e_test.go:484-547`

**Interfaces:**

- Consumes: `startLiveWriter(marker string) *liveWriter`, `(*liveWriter).stop() (int, error)`, and `liveMarkerQuery(marker string) string`.
- Produces: real-cluster acceptance coverage for the source maximum, target count and gap, full comparison, and cleanup.

- [ ] **Step 1: Cross-check `last` before cutover approval**

After the existing `last > 100` assertion and before `approveCutover(name)`, add:

```go
sourceLast := psql(sourceCluster, liveMarkerQuery(marker))
Expect(fmt.Sprint(last)).To(Equal(sourceLast),
	"writer returned marker %d but the source reports %s for %q", last, sourceLast, marker)
```

The writer remains stopped before approval, so this source read is stable.

- [ ] **Step 2: Replace the stale retry comment without weakening the assertions**

Replace the full old comment above `srcRows` at lines 523-526 with:

```go
// last is the source maximum after the psql child exits. Keep the count
// comparison to prove every row for this marker reached the target.
```

Keep the full section from line 484 through line 547 otherwise intact: the source-target `count(*)` equality, target `generate_series` gap check through `last`, `Verified` condition, source slot cleanup, target origin cleanup, and source plus target fixture cleanup.

- [ ] **Step 3: Format, re-run local tests, and commit**

Run:

```bash
gofmt -w test/e2e/e2e_test.go
go test ./test/e2e -run '^TestLiveWriter' -count=1
go vet ./test/e2e/...
git add test/e2e/e2e_test.go
git commit -m "test(e2e): check the live writer source marker"
```

Expected: the local command passes without starting `TestE2E`. The real scenario runs later through the guarded task.

### Task 4: Verify, review, and deliver in fact order

**Files:**

- Modify: `tasks/todo.md` after each observed result

**Interfaces:**

- Consumes: the Task 1 through Task 3 commits and their command output.
- Produces: a reviewable branch, a pull request, recorded verification facts, and CI evidence tied to the checked implementation SHA.

- [ ] **Step 1: Run the repository gates and inspect the implementation diff**

Run:

```bash
task lint
task test
git diff --check main...HEAD
git diff main...HEAD -- test/e2e/e2e_suite_test.go test/e2e/live_writer_test.go test/e2e/e2e_test.go
git status --short
```

Expected: lint and test exit 0. The diff has one persistent stream, no per-tick `psqlDBErr`, the private test seams only, exact marker query helpers, local subprocess tests, the corrected scenario comment, and no private infrastructure facts.

- [ ] **Step 2: Obtain independent implementation review**

Give the reviewer the approved spec, this plan, and the implementation diff.

The reviewer must check that Start closes stdin while preserving its first error, final marker command and parse failures use `recordErr`, one initial stream and one final query are the only command calls, tests cover the listed local failure modes, and the real scenario retains every assertion named in Task 3.

- [ ] **Step 3: Make the human-gated e2e decision**

Inspect the current context without copying its value to a committed file:

```bash
kubectl config current-context
```

If the human approves the displayed prompt, run:

```bash
task e2e
```

Expected: Task asks for confirmation. The live-load spec passes with a source maximum over 100, equal marker counts, gap `0`, data verification, and cleanup.

If approval is not given, do not run `go test ./test/e2e/...` as a substitute and do not use `task --yes`. Record a human-declined e2e decision and leave that verification pending.

- [ ] **Step 4: Push implementation, create or locate the pull request, and wait for checks**

Run:

```bash
git push -u origin fix/172-live-writer-throughput
gh pr view --repo ydixken/pgcopydb-operator --json url,state 2>/dev/null || \
	gh pr create --repo ydixken/pgcopydb-operator --base main --head fix/172-live-writer-throughput \
		--title "test(e2e): keep live writes in one psql session" \
		--body-file - <<'EOF'
## Summary

- replace per-row kubectl exec with one persistent e2e psql session
- use the post-shutdown source marker as the live-write bound
- add local subprocess tests and retain real-cluster acceptance coverage

## Verification

- `go test ./test/e2e -run '^TestLiveWriter' -count=1`
- `task lint`
- `task test`
- `task e2e` only after human approval
EOF
gh pr checks --repo ydixken/pgcopydb-operator --watch
```

Expected: the implementation SHA is on the pull request and its required checks have reached a terminal result before any CI outcome is written to the tracker.

- [ ] **Step 5: Record observed facts, then commit and push the tracker**

Only after Step 4 completes, update `tasks/todo.md` with the exact command outcomes, review result, e2e decision, implementation SHA, pull request URL, and the checks result for that SHA. Do not claim later checks for the tracker-only commit have passed before they run.

Run:

```bash
git add tasks/todo.md
git commit -m "docs: record live writer verification"
git push
```

Expected: the tracker records facts, not intentions. If the tracker commit starts a later check run, report its status separately rather than rewriting the recorded implementation-check result.

## Final Verification Checklist

- [ ] The named local subprocess test command passed without invoking `TestE2E` or `BeforeSuite`.
- [ ] `task lint` and `task test` exited 0.
- [ ] Start failure closes stdin and retains Start as the first returned error.
- [ ] The persistent path has one primary resolution and one psql child. Its final query is separate and does not resume writes.
- [ ] The final query and parse failures flow through `recordErr`, and its source maximum SQL exactly matches the approved spec.
- [ ] The scenario section at `test/e2e/e2e_test.go:484-547` still enforces the floor, count, gap, full compare, cleanup, and corrected source-marker comment.
- [ ] The e2e result is recorded as passed or human-declined without bypassing confirmation.
- [ ] The pull request checks were observed before tracker facts were committed and pushed.

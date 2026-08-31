# Persistent e2e Live Writer Throughput Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the per-row `kubectl exec` bottleneck from the e2e live writer while preserving one committed transaction per ordered row and its data-loss acceptance checks.

**Architecture:** `liveWriter` remains a specialized e2e helper. Its goroutine resolves the source primary once, starts one interactive `kubectl exec -i ... psql` child, and writes one `INSERT` statement per ticker event to that child's standard input. On shutdown, it closes and waits for that child, then runs a separate source query whose marker-scoped maximum is the authoritative `last` value used by the existing scenario.

**Tech Stack:** Go, `os/exec`, `sync`, Ginkgo v2, Gomega, the existing CNPG-backed `test/e2e` suite.

**Spec:** `docs/superpowers/specs/2026-08-31-live-writer-throughput-design.md`

## Global Constraints

- Do not add a dependency, a generic streaming helper, or a command abstraction. `liveWriter` has one caller and the standard library already supplies the process and pipe primitives.
- Keep the writer's `sync.Once` cleanup guard and `defer GinkgoRecover()` in its goroutine.
- Resolve the source primary once before starting the persistent child. Do not restart the child or re-resolve the primary to continue a failed write stream.
- The persistent child command is exactly `kubectl exec -i -n <e2e namespace> <source primary> -c postgres -- psql -U postgres app -q -v ON_ERROR_STOP=1`.
- Every ticker event writes one ordered `INSERT` ending in a newline. psql autocommit makes each successful statement one committed transaction.
- `stop` must close standard input, wait for psql, then execute a separate one-shot query for the source marker maximum. It returns the first lifecycle error and does not turn a failed persistent stream into a retry.
- For marker `marker`, the authoritative query is exactly `SELECT COALESCE(MAX((substring(note FROM char_length('marker') + 2))::int), 0) FROM orders WHERE note LIKE 'marker-%'`, rendered with the scenario marker in both places.
- A failure to resolve the initial primary, start the child, write a statement, close the stream, wait for psql, run the final query, or parse its scalar output must fail the spec. Preserve the first recorded error for `stop` to report.
- Retain `last > 100`, source-target marker count equality, the target gap query, full data verification, and source slot plus target origin cleanup assertions.
- Refresh the stale source-target-count comment in `test/e2e/e2e_test.go`. It must describe the authoritative source marker returned after psql shuts down, not retryable per-row exec behavior.
- The e2e suite is untagged and its `BeforeSuite` reaches the current Kubernetes context. `task test` intentionally excludes `/e2e`; do not run `go test ./test/e2e/...` directly because that bypasses the Taskfile confirmation.
- Run `task lint` and `task test` before completion. Run `task e2e` only after a human approves the displayed current-context prompt. Never use `task --yes` for e2e.
- This repository is public. Do not commit or paste private cluster, endpoint, node, credential, or inventory details. Do not read or expose secret values.
- No em dashes. No Claude session URLs. Keep comments within the repository limits.

---

## File Map and Interfaces

- `test/e2e/e2e_suite_test.go`: owns `liveWriter`, `startLiveWriter(marker string) *liveWriter`, `(*liveWriter).run(marker string)`, `(*liveWriter).stop() (int, error)`, and an unexported first-error recorder. This is the sole process-lifecycle implementation.
- `test/e2e/e2e_test.go`: owns the `e2e-follow-load` acceptance scenario. It cross-checks `stop`'s returned marker against a separate source query, keeps the count and gap checks, and carries the corrected comment.
- `tasks/todo.md`: records implementation, review, verification, e2e decision, and delivery results as the work happens.

## Test Strategy

There is no credible local unit-test target for this process lifecycle without adding a single-use command seam solely for tests. Every file under `test/e2e` has no build tag, belongs to the Ginkgo package whose `BeforeSuite` reads the active kubeconfig and creates real fixtures, and `task test` excludes it through `go list ./... | grep -v /e2e`.

The smallest automated behavioral seam is therefore the existing `e2e-follow-load` Ginkgo scenario. Task 1 makes its authoritative-marker expectation explicit before Task 2 changes the writer, and the guarded `task e2e` execution validates the persistent process, marker query, error propagation, target equality, gap check, full compare, and cleanup against the real fixture. `go vet ./test/e2e/...` is safe static compilation coverage and does not execute the suite.

The TDD order is deliberate: first make the existing scenario assert the source-authoritative marker, then replace the writer, then run the guarded acceptance test. There is no safe local red-test command for the process failure branches. A fake command seam would be production-only complexity for one e2e helper, and direct e2e execution would bypass the confirmation gate. The new assertion fails when `stop` returns an incorrect marker, while the real scenario remains the only credible process, stream, shutdown, query, and error-propagation test.

### Task 1: Make the scenario the authoritative-marker acceptance test

**Files:**

- Modify: `test/e2e/e2e_test.go:484-541`

**Interfaces:**

- Consumes: existing `startLiveWriter(marker string) *liveWriter`, `(*liveWriter).stop() (int, error)`, `psql(cluster, sql string) string`, and the `e2e-follow-load` constants `marker = "live-load"` and `last`.
- Produces: an acceptance expectation that `stop` returns the separate source query's exact maximum numeric suffix, before source-target count and target-gap assertions run.

- [ ] **Step 1: Write the acceptance assertion and replace the stale comment**

Immediately after `Expect(last).To(BeNumerically(">", 100), ...)`, query the source independently and assert that its scalar output equals `fmt.Sprint(last)`.

```go
sourceLast := psql(sourceCluster, fmt.Sprintf(
	"SELECT COALESCE(MAX((substring(note FROM char_length('%s') + 2))::int), 0)"+
		" FROM orders WHERE note LIKE '%s-%%'", marker, marker))
Expect(fmt.Sprint(last)).To(Equal(sourceLast),
	"writer returned marker %d but the source reports %s for %q", last, sourceLast, marker)
```

Replace the existing comment above `srcRows` with this two-line explanation.

```go
// last comes from the source after the psql child exits, so it is the
// authoritative marker bound. Compare counts separately to check the target.
```

Leave the existing source-target `count(*)` assertion, target gap query, full data verification, and cleanup assertions intact.

- [ ] **Step 2: Format and statically check the e2e package**

Run:

```bash
gofmt -w test/e2e/e2e_test.go
go vet ./test/e2e/...
```

Expected: `gofmt` makes no further change when rerun, and `go vet` exits 0 without executing the Ginkgo suite or contacting the cluster.

Do not substitute a direct `go test ./test/e2e/...` run to force an expected failure. This acceptance assertion may pass against the old writer when no retry occurred, because its old in-memory counter was then equal to the source maximum. Its purpose is to lock the source-authoritative contract before the implementation removes that counter.

- [ ] **Step 3: Commit the acceptance contract**

Run:

```bash
git add test/e2e/e2e_test.go
git commit -m "test(e2e): assert the live writer's source marker"
```

Expected: one small conventional commit containing only the scenario assertion and corrected comment.

### Task 2: Replace per-row exec with one persistent psql child

**Files:**

- Modify: `test/e2e/e2e_suite_test.go:1434-1500`

**Interfaces:**

- Consumes: `primaryPod(cluster string) string`, `sourceCluster`, `appDB`, `nsE2E`, `liveWriteInterval`, `GinkgoRecover`, `exec.Command`, `fmt`, `strconv`, `strings`, and `sync.Once`.
- Produces: unchanged suite API `startLiveWriter(marker string) *liveWriter` and `(*liveWriter).stop() (int, error)`. `stop` returns the parsed source marker maximum when the lifecycle succeeds, or the first recorded lifecycle error.

- [ ] **Step 1: Keep the first-error state minimal**

Retain `stopCh`, `done`, `stopOnce`, `mu`, `last`, and `err` on `liveWriter`.

Add only this private method to preserve the earliest failure across setup, stream, shutdown, and final query.

```go
func (w *liveWriter) recordErr(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = err
	}
}
```

Do not add an interface, a factory, or an `exec.Command` replacement variable. This helper has one writer and one contract: retain the first error that makes the spec fail.

- [ ] **Step 2: Replace the retrying per-tick call with one ordered stdin stream**

At the start of `run`, retain `defer close(w.done)` and `defer GinkgoRecover()`.

Resolve `primaryPod(sourceCluster)` once, construct this child, obtain its stdin pipe, and start it once.

```go
cmd := exec.Command("kubectl", "exec", "-i", "-n", nsE2E, pod, "-c", "postgres", "--",
	"psql", "-U", "postgres", appDB, "-q", "-v", "ON_ERROR_STOP=1")
stdin, err := cmd.StdinPipe()
if err != nil {
	w.recordErr(fmt.Errorf("open persistent psql stdin: %w", err))
	return
}
if err := cmd.Start(); err != nil {
	w.recordErr(fmt.Errorf("start persistent psql: %w", err))
	return
}
```

Run the ticker loop from `n := 1` upward. On each tick, render exactly one newline-terminated statement and write it to `stdin`.

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

Add the `io` import only for `io.ErrShortWrite`.

Do not call `psqlDBErr` inside the ticker and do not increment a success counter for `last`. A failed pipe is terminal. The goroutine stops producing rows and its deferred shutdown completes, rather than replacing the stream or resolving a new primary.

- [ ] **Step 3: Close, wait, and read the authoritative source marker in deferred shutdown**

Install the deferred shutdown immediately after a successful `cmd.Start`, so it runs on a normal stop and on a write failure.

```go
defer func() {
	if err := stdin.Close(); err != nil {
		w.recordErr(fmt.Errorf("close persistent psql stdin: %w", err))
	}
	if err := cmd.Wait(); err != nil {
		w.recordErr(fmt.Errorf("wait for persistent psql: %w", err))
	}

	query := fmt.Sprintf(
		"SELECT COALESCE(MAX((substring(note FROM char_length('%s') + 2))::int), 0)"+
			" FROM orders WHERE note LIKE '%s-%%'", marker, marker)
	pod := primaryPod(sourceCluster)
	out, err := exec.Command("kubectl", "exec", "-n", nsE2E, pod, "-c", "postgres", "--",
		"psql", "-U", "postgres", appDB, "-tAc", query).Output()
	if err != nil {
		w.recordErr(fmt.Errorf("read final marker: %w", err))
		return
	}
	last, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		w.recordErr(fmt.Errorf("parse final marker %q: %w", strings.TrimSpace(string(out)), err))
		return
	}
	w.mu.Lock()
	w.last = last
	w.mu.Unlock()
}()
```

The final query is intentionally a separate, one-shot read after the persistent child exits. It may resolve the current source primary for that read, but it must never start another persistent child or use that resolution to resume inserts. Run the query even after a stream failure so shutdown reaches a known source state, while `recordErr` still returns the earliest failure to the scenario.

- [ ] **Step 4: Keep `stop` as the only synchronization boundary**

Keep `stop`'s `sync.Once` close and wait for `<-w.done` before reading `last` and `err` under `w.mu`.

```go
func (w *liveWriter) stop() (int, error) {
	w.stopOnce.Do(func() { close(w.stopCh) })
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last, w.err
}
```

This permits the scenario and `DeferCleanup` to call `stop` safely. The cleanup call observes the same completed process and result rather than closing stdin, waiting, or querying a second time.

- [ ] **Step 5: Format and statically check the changed package**

Run:

```bash
gofmt -w test/e2e/e2e_suite_test.go
gofmt -d test/e2e/e2e_suite_test.go test/e2e/e2e_test.go
go vet ./test/e2e/...
```

Expected: the `gofmt -d` command prints no diff, and `go vet` exits 0 without executing the real-cluster suite.

The error branches are exercised by the helper's direct error recording and independently reviewed in Task 3. Do not induce a pipe, process, or primary failure on the shared cluster only to make this test red.

- [ ] **Step 6: Commit the persistent writer**

Run:

```bash
git add test/e2e/e2e_suite_test.go
git commit -m "test(e2e): keep live writes in one psql session"
```

Expected: a second small conventional commit. It changes only the writer implementation and its required standard-library import.

### Task 3: Verify, review, and deliver the implementation

**Files:**

- Modify: `tasks/todo.md` (record actual results only)

**Interfaces:**

- Consumes: the acceptance scenario from Task 1 and persistent writer from Task 2.
- Produces: recorded verification evidence, an implementation review result, the guarded e2e decision, and a pull request with required CI checks.

- [ ] **Step 1: Run the repository gates**

Run:

```bash
task lint
task test
```

Expected: both commands exit 0. `task test` validates all unit and envtest packages but deliberately excludes `test/e2e`, as verified from `Makefile`.

- [ ] **Step 2: Review the exact behavioral diff**

Run:

```bash
git diff --check main...HEAD
git diff -- test/e2e/e2e_suite_test.go test/e2e/e2e_test.go
git status --short
```

Expected: no whitespace errors; the diff contains one persistent `kubectl exec -i` process, no per-tick `psqlDBErr` call, the exact marker query in the writer and scenario, the corrected comment, and no private-infrastructure facts.

- [ ] **Step 3: Obtain an independent implementation review**

Give a reviewer the spec, this plan, and the Task 1 and Task 2 diff.

The reviewer must confirm all of these points before the result is recorded in `tasks/todo.md`:

- The process is started once against the startup primary and no write path retries or reconnects it.
- Each inserted statement is ordered, newline-terminated, and autocommitted by the requested psql command.
- `stop` closes stdin, waits, queries only the exact marker prefix, parses the scalar, and returns the first lifecycle error.
- The test preserves the floor, source-target equality, target gap check, full compare, and cleanup assertions.
- No new dependency or speculative abstraction was introduced.

- [ ] **Step 4: Make the guarded real-cluster e2e decision**

First inspect, but do not record or share the context value in a committed file:

```bash
kubectl config current-context
```

If the human approves running against that displayed context, run exactly:

```bash
task e2e
```

Expected: Task asks for confirmation and the human answers it. The `e2e-follow-load` scenario passes with `last > 100`, source and target marker counts equal, target gap `0`, `Verified` true, and both cleanup assertions satisfied.

If the human does not approve, do not run any direct `go test ./test/e2e/...` substitute and do not use `task --yes`. Record the declined decision in `tasks/todo.md` and leave e2e verification pending.

- [ ] **Step 5: Record results, push, create the pull request, and verify CI**

Update `tasks/todo.md` with command results, review result, e2e decision, commit hashes, pull request URL, and CI result. Do not insert any private infrastructure values.

Run:

```bash
git add tasks/todo.md
git commit -m "docs: record live writer verification"
git push -u origin fix/172-live-writer-throughput
gh pr view --repo ydixken/pgcopydb-operator --json url,state 2>/dev/null || \
	gh pr create --repo ydixken/pgcopydb-operator --base main --head fix/172-live-writer-throughput \
		--title "test(e2e): keep live writes in one psql session" \
		--body-file - <<'EOF'
## Summary

- replace per-row kubectl exec with one persistent e2e psql session
- query the source marker after shutdown for the authoritative live-write bound
- retain the live migration's count, gap, full compare, and cleanup checks

## Verification

- `task lint`
- `task test`
- `task e2e` only when human-approved for the displayed current context
EOF
gh pr checks --repo ydixken/pgcopydb-operator --watch
```

Expected: the pull request exists, required CI lint, test, and docs checks pass, and the e2e decision is transparent in the tracker and pull request.

## Final Verification Checklist

- [ ] `task lint` exited 0.
- [ ] `task test` exited 0.
- [ ] The implementation diff has no per-row `kubectl exec` or `psqlDBErr` call in `liveWriter`.
- [ ] The source maximum query matches the exact marker-scoped SQL in the approved spec.
- [ ] Every specified lifecycle failure reaches `stop` as the first error or fails the Ginkgo spec during initial primary resolution.
- [ ] The corrected scenario comment describes the post-shutdown source marker rather than the old retry behavior.
- [ ] The real-cluster e2e result is recorded as passed or human-declined, with no confirmation bypass.
- [ ] The branch is pushed, the pull request exists, and required CI results are recorded.

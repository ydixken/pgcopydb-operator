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

// Package buildconfig guards the image builds against silent slowdowns.
//
// Every assertion here stands for a change that costs minutes per release and
// reports no error when it is lost. kubebuilder owns Dockerfile and Makefile
// and AGENTS.md tells agents to regenerate rather than hand-edit them, so a
// scaffold refresh is a live way to lose these.
package buildconfig

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const (
	managerDockerfile = "../../Dockerfile"
	runnerDockerfile  = "../../images/runner/Dockerfile"
	releaseWorkflow   = "../../.github/workflows/release.yml"
	builderWorkflow   = "../../.github/workflows/pgcopydb-builder.yml"
	workflowDir       = "../../.github/workflows"
	ciWorkflow        = "../../.github/workflows/ci.yml"
	chartValues       = "../../charts/pgcopydb-operator/values.yaml"
	chartReadme       = "../../charts/pgcopydb-operator/README.md"
	runnerReadme      = "../../images/runner/README.md"
	mainGo            = "../../cmd/main.go"
	mainTest          = "../../cmd/main_test.go"
	pollerTest        = "../../internal/progress/poller_test.go"
	builderDockerfile = "../../images/pgcopydb-builder/Dockerfile"
	e2eSuite          = "../../test/e2e/e2e_suite_test.go"
)

const dependencyReviewAllowLicenses = "Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, MIT, " +
	"LicenseRef-scancode-google-patent-license-golang"

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// pin reads an ARG's default out of a Dockerfile. Empty is fatal, not a
// mismatch: a renamed ARG must not let every comparison agree trivially on
// "".
func pin(t *testing.T, path, arg string) string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^ARG\s+` + regexp.QuoteMeta(arg) + `=(\S+)\s*$`)
	m := re.FindStringSubmatch(read(t, path))
	if m == nil || m[1] == "" {
		t.Fatalf("%s declares no `ARG %s=<value>`", path, arg)
	}
	return m[1]
}

// Without --platform=$BUILDPLATFORM a FROM resolves to the stage's *target*
// platform, so buildx runs the whole Go toolchain under QEMU for the arm64
// half. Measured on release run 33242100882: 544.7s emulated against 55.6s
// native for the same `go build`.
func TestManagerBuilderPinsBuildPlatform(t *testing.T) {
	src := read(t, managerDockerfile)

	builder := regexp.MustCompile(`(?m)^FROM\s+(.*)\s+AS\s+builder\s*$`)
	m := builder.FindStringSubmatch(src)
	if m == nil {
		t.Fatal("no `FROM ... AS builder` stage in the manager Dockerfile")
	}
	if !strings.Contains(m[1], "--platform=$BUILDPLATFORM") {
		t.Errorf("builder stage does not pin --platform=$BUILDPLATFORM, so the arm64 "+
			"half compiles under QEMU (~10x slower):\n  FROM %s AS builder", m[1])
	}

	// The cross-compile is only safe because the build is pure Go: a cgo build
	// would need a cross C toolchain the golang image does not carry.
	if !strings.Contains(src, "CGO_ENABLED=0") {
		t.Error("the builder cross-compiles, so the build MUST stay CGO_ENABLED=0")
	}
	if !strings.Contains(src, "GOARCH=${TARGETARCH}") {
		t.Error("the builder runs on the host arch, so the build MUST set GOARCH=${TARGETARCH}")
	}
}

// The Makefile used to sed --platform into a throwaway Dockerfile.cross. The
// Dockerfile carries it now, and a second --platform on the same FROM is at
// best ignored, so the scaffold's workaround must not come back.
func TestDockerBuildxDoesNotReinjectPlatform(t *testing.T) {
	for line := range strings.SplitSeq(read(t, "../../Makefile"), "\n") {
		// Recipe comments may still name the workaround as provenance; only a
		// line that actually runs it is a regression.
		if strings.HasPrefix(strings.TrimLeft(line, " \t"), "#") {
			continue
		}
		if strings.Contains(line, "Dockerfile.cross") {
			t.Errorf("docker-buildx still builds via Dockerfile.cross; the Dockerfile pins "+
				"--platform=$BUILDPLATFORM itself, so the sed would duplicate the flag:\n %s",
				strings.TrimSpace(line))
		}
	}
}

// pgcopydb is ~67 translation units, of which the vendored 9 MB sqlite3.c is
// one and cannot be split. Upstream's own Dockerfile for this tree runs
// -j$(nproc); this matters on every SHA bump.
func TestBuilderCompilesInParallel(t *testing.T) {
	src := read(t, builderDockerfile)

	// Anchored past a leading "#" so a comment mentioning make/install cannot
	// stand in for the live command this test means to inspect.
	makeLine := regexp.MustCompile(`(?m)^\s*[^#\s].*\bmake\b.*\binstall\b.*$`)
	line := makeLine.FindString(src)
	if line == "" {
		t.Fatal("no live `make ... install` line in the builder Dockerfile")
	}
	if !regexp.MustCompile(`\s-j`).MatchString(line) {
		t.Errorf("pgcopydb compiles serially; pass -j:\n %s", strings.TrimSpace(line))
	}
}

// The split puts the compile in one file and the tag that consumes it in
// another. If they disagree and the old tag still exists in the registry, the
// build succeeds and the release ships the previous pgcopydb.
func TestRunnerPullsThePinnedBuilder(t *testing.T) {
	wantSHA := pin(t, builderDockerfile, "PGCOPYDB_SHA")

	re := regexp.MustCompile(`(?m)^FROM\s+\S*pgcopydb-builder:(\S+)\s+AS\s+pgcopydb\s*$`)
	m := re.FindStringSubmatch(read(t, runnerDockerfile))
	if m == nil {
		t.Fatal("no pgcopydb-builder FROM (tag@sha256:digest AS pgcopydb) in the runner Dockerfile")
	}

	// A bare tag is mutable: a debian bump inside the builder republishes it,
	// so only the digest stops that rebuild from reaching this image unreviewed.
	tag, digest, hasDigest := strings.Cut(m[1], "@")
	if !hasDigest || !strings.HasPrefix(digest, "sha256:") {
		t.Errorf("pgcopydb-builder FROM %q has no @sha256: digest pin", m[1])
	}
	if tag != wantSHA {
		t.Errorf("builder pins %s, runner's FROM pins %s", wantSHA, tag)
	}

	// PGCOPYDB_VERSION can drift independently of the SHA check above; if it
	// does, the runner's canary asserts a version the builder never produced.
	wantVersion := pin(t, builderDockerfile, "PGCOPYDB_VERSION")
	if !strings.Contains(read(t, runnerDockerfile), "grep -qF '"+wantVersion+"'") {
		t.Errorf("builder pins version %s, runner canary does not assert it", wantVersion)
	}
}

// A --platform here would copy an amd64 binary into the arm64 image, and the
// canary below cannot catch it: binfmt runs the amd64 ELF natively on the
// build host, so `pgcopydb --version` passes anyway.
func TestBuilderReferenceIsNotPlatformPinned(t *testing.T) {
	re := regexp.MustCompile(`(?m)^FROM\s+(.*pgcopydb-builder.*)$`)
	m := re.FindStringSubmatch(read(t, runnerDockerfile))
	if m == nil {
		t.Fatal("no FROM referencing pgcopydb-builder in the runner Dockerfile")
	}
	if strings.Contains(m[1], "--platform") {
		t.Errorf("the pgcopydb-builder FROM must not pin a platform:\n  FROM %s", m[1])
	}
}

// The whole point of the split. Restoring the compile here raises no error,
// because the image still builds correctly, just twenty minutes slower.
func TestRunnerDoesNotCompilePgcopydb(t *testing.T) {
	src := read(t, runnerDockerfile)
	for _, banned := range []string{"codeload.github.com", "postgresql-server-dev-18"} {
		if strings.Contains(src, banned) {
			t.Errorf("the runner Dockerfile still compiles pgcopydb: found %q", banned)
		}
	}
	// Same anchoring as TestBuilderCompilesInParallel: a comment mentioning
	// make/install must not read as the live command coming back.
	if regexp.MustCompile(`(?m)^\s*[^#\s].*\bmake\b.*\binstall\b.*$`).MatchString(src) {
		t.Error("the runner Dockerfile still runs `make ... install`")
	}
}

type workflowStep struct {
	Name string `json:"name"`
	Uses string `json:"uses"`
	If   string `json:"if"`
	Run  string `json:"run"`
	With struct {
		AllowLicenses  string `json:"allow-licenses"`
		Context        string `json:"context"`
		FailOnScopes   string `json:"fail-on-scopes"`
		FailOnSeverity string `json:"fail-on-severity"`
		Platforms      string `json:"platforms"`
		Outputs        string `json:"outputs"`
	} `json:"with"`
}

type workflowJob struct {
	Needs json.RawMessage `json:"needs"`
	If    string          `json:"if"`
	Uses  string          `json:"uses"`
	Steps []workflowStep  `json:"steps"`
}

type workflow struct {
	Jobs map[string]workflowJob `json:"jobs"`
}

func TestReleasePromotionWaitsForCandidateChecks(t *testing.T) {
	var wf workflow
	if err := yaml.Unmarshal([]byte(read(t, releaseWorkflow)), &wf); err != nil {
		t.Fatalf("parse release.yml: %v", err)
	}

	promote, ok := wf.Jobs["promote"]
	if !ok {
		t.Fatal("release.yml has no promote job")
	}
	if len(promote.Needs) == 0 {
		t.Fatal("release.yml promote job declares no prerequisites")
	}

	var needs []string
	if err := json.Unmarshal(promote.Needs, &needs); err != nil {
		t.Fatalf("parse release.yml promote needs: %v", err)
	}
	want := []string{"e2e", "release-notes"}
	if !slices.Equal(needs, want) {
		t.Errorf("release.yml promote needs %v, want exactly %v", needs, want)
	}
	if promote.If != "" {
		t.Errorf("release.yml promote has job-level if %q; default dependency handling must gate promotion", promote.If)
	}
}

func TestE2EDefaultRelease(t *testing.T) {
	re := regexp.MustCompile(`(?m)^var operatorTag = "([^"]+)"$`)
	match := re.FindStringSubmatch(read(t, e2eSuite))
	if match == nil {
		t.Fatal("e2e suite declares no operatorTag default")
	}
	if got, want := match[1], "v0.11.3"; got != want {
		t.Errorf("e2e operatorTag default is %s, want %s", got, want)
	}
}

func TestWorkflowActionInventory(t *testing.T) {
	approved := map[string]string{
		"actions/cache":                    "v6",
		"actions/checkout":                 "v7",
		"actions/configure-pages":          "v6",
		"actions/dependency-review-action": "v4",
		"actions/deploy-pages":             "v5",
		"actions/setup-go":                 "v7",
		"actions/setup-python":             "v7",
		"actions/upload-pages-artifact":    "v5",
		"azure/setup-helm":                 "v5",
		"azure/setup-kubectl":              "v5",
		"codecov/codecov-action":           "v7",
		"docker/build-push-action":         "v7",
		"docker/login-action":              "v4",
		"docker/setup-buildx-action":       "v4",
		"docker/setup-qemu-action":         "v4",
		"oras-project/setup-oras":          "v2",
	}

	entries, err := os.ReadDir(workflowDir)
	if err != nil {
		t.Fatalf("read workflow directory: %v", err)
	}

	files := 0
	references := 0
	localCalls := 0
	dependencyReviews := 0
	check := func(location, uses string) {
		if uses == "" {
			return
		}
		// A job may call another workflow in this repository instead of an
		// action. Only a local path: a third-party reusable workflow would run
		// on our runners with our secrets, which is the thing the pinned
		// version list above exists to prevent.
		if strings.HasPrefix(uses, "./") {
			if !strings.HasPrefix(uses, "./.github/workflows/") || !strings.HasSuffix(uses, ".yml") {
				t.Errorf("%s calls %q, which is not a workflow in this repository", location, uses)
			}
			localCalls++
			return
		}
		if strings.Count(uses, "/") >= 2 && strings.Contains(uses, ".yml@") {
			t.Errorf("%s calls a workflow from another repository: %q", location, uses)
			return
		}
		references++
		name, version, ok := strings.Cut(uses, "@")
		if !ok || name == "" || version == "" {
			t.Errorf("%s has malformed action reference %q", location, uses)
			return
		}
		want, ok := approved[name]
		if !ok {
			t.Errorf("%s uses unapproved action %q", location, name)
			return
		}
		if version != want {
			t.Errorf("%s uses %s@%s, want %s@%s", location, name, version, name, want)
		}
		if name == "actions/dependency-review-action" {
			dependencyReviews++
		}
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yml" {
			continue
		}
		files++
		path := filepath.Join(workflowDir, entry.Name())
		var wf workflow
		if err := yaml.Unmarshal([]byte(read(t, path)), &wf); err != nil {
			t.Errorf("parse %s: %v", path, err)
			continue
		}
		for jobName, job := range wf.Jobs {
			check(entry.Name()+"/jobs/"+jobName, job.Uses)
			for i, step := range job.Steps {
				check(fmt.Sprintf("%s/jobs/%s/steps/%d", entry.Name(), jobName, i), step.Uses)
			}
		}
	}

	if files != 10 {
		t.Errorf("workflow inventory contains %d YAML files, want 10", files)
	}
	if references != 51 {
		t.Errorf("workflow inventory contains %d action references, want 51", references)
	}
	// release.yml calling pgcopydb-builder.yml is the only one; a second would
	// mean the split got copied somewhere rather than reused.
	if localCalls != 1 {
		t.Errorf("workflow inventory makes %d local workflow calls, want 1", localCalls)
	}
	if dependencyReviews != 1 {
		t.Errorf("workflow inventory contains %d dependency review references, want 1", dependencyReviews)
	}
}

func TestDependencyReviewPolicy(t *testing.T) {
	var wf workflow
	if err := yaml.Unmarshal([]byte(read(t, ciWorkflow)), &wf); err != nil {
		t.Fatalf("parse ci.yml: %v", err)
	}

	lint, ok := wf.Jobs["lint"]
	if !ok {
		t.Fatal("ci.yml has no lint job")
	}

	reviews := 0
	for i, step := range lint.Steps {
		if step.Uses != "actions/dependency-review-action@v4" {
			continue
		}
		reviews++
		if i == 0 || lint.Steps[i-1].Uses != "actions/checkout@v7" {
			t.Error("dependency review must appear immediately after lint checkout")
		}
		if step.Name != "Review dependency changes" {
			t.Errorf("dependency review name is %q", step.Name)
		}
		if step.If != "github.event_name == 'pull_request'" {
			t.Errorf("dependency review if is %q", step.If)
		}
		if step.With.FailOnSeverity != "moderate" {
			t.Errorf("dependency review fail-on-severity is %q", step.With.FailOnSeverity)
		}
		if step.With.FailOnScopes != "runtime, development, unknown" {
			t.Errorf("dependency review fail-on-scopes is %q", step.With.FailOnScopes)
		}
		if step.With.AllowLicenses != dependencyReviewAllowLicenses {
			t.Errorf("dependency review allow-licenses is %q", step.With.AllowLicenses)
		}
	}
	if reviews != 1 {
		t.Errorf("lint job contains %d dependency review steps, want 1", reviews)
	}
	if strings.Contains(read(t, ciWorkflow), "allow-dependencies-licenses") {
		t.Error("dependency review must not use an allow-dependencies-licenses exemption")
	}
}

func usesQEMU(steps []workflowStep) bool {
	for _, s := range steps {
		if strings.HasPrefix(s.Uses, "docker/setup-qemu-action") {
			return true
		}
	}
	return false
}

// Emulation is now the runner image's business alone: its final stage runs apt
// and a canary on both platforms. The manager's final stage is COPY-only, so
// installing QEMU there is dead weight, and needing it again would mean the
// builder stage lost its --platform pin.
func TestOnlyTheRunnerImageInstallsQEMU(t *testing.T) {
	var wf workflow
	if err := yaml.Unmarshal([]byte(read(t, releaseWorkflow)), &wf); err != nil {
		t.Fatalf("parse release.yml: %v", err)
	}
	for _, name := range []string{"manager-image", "runner-image"} {
		if _, ok := wf.Jobs[name]; !ok {
			t.Fatalf("release.yml has no %s job", name)
		}
	}
	if usesQEMU(wf.Jobs["manager-image"].Steps) {
		t.Error("manager-image installs QEMU, which it no longer needs; if the arm64 " +
			"build broke without it, restore --platform=$BUILDPLATFORM instead")
	}
	if !usesQEMU(wf.Jobs["runner-image"].Steps) {
		t.Error("runner-image must keep docker/setup-qemu-action: it runs apt and the " +
			"version canary on the emulated arm64 platform")
	}
}

// Matches by context, not position: release.yml has three build-push-action
// steps and only one builds images/pgcopydb-builder. The union across every
// matching step, not the first one's list, because pgcopydb-builder.yml builds
// each architecture on a machine of that architecture and joins them after:
// one step says linux/amd64 and another linux/arm64, and the tag that results
// carries both. Fatal on a miss, so a missing step cannot read as an empty
// platform list to the caller.
func builderPlatforms(t *testing.T, path string) string {
	t.Helper()
	var wf workflow
	if err := yaml.Unmarshal([]byte(read(t, path)), &wf); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	seen := map[string]bool{}
	for _, job := range wf.Jobs {
		for _, s := range job.Steps {
			if strings.HasPrefix(s.Uses, "docker/build-push-action") && s.With.Context == "images/pgcopydb-builder" {
				for p := range strings.SplitSeq(s.With.Platforms, ",") {
					if p = strings.TrimSpace(p); p != "" {
						seen[p] = true
					}
				}
			}
		}
	}
	if len(seen) == 0 {
		t.Fatalf("%s has no docker/build-push-action step building images/pgcopydb-builder", path)
	}
	return strings.Join(slices.Sorted(maps.Keys(seen)), ",")
}

// Building the two architectures separately buys a new way to fail: both get
// pushed by digest and nothing ever writes the tag, so the existence check in
// release.yml finds nothing and the next release rebuilds under emulation
// without saying why. A split build has to join what it split.
func TestSplitBuilderJoinsItsArchitectures(t *testing.T) {
	var wf workflow
	if err := yaml.Unmarshal([]byte(read(t, builderWorkflow)), &wf); err != nil {
		t.Fatalf("parse %s: %v", builderWorkflow, err)
	}
	var byDigest, merges bool
	for _, job := range wf.Jobs {
		for _, s := range job.Steps {
			if strings.Contains(s.With.Outputs, "push-by-digest=true") {
				byDigest = true
			}
			if strings.Contains(s.Run, "imagetools create") {
				merges = true
			}
		}
	}
	if byDigest && !merges {
		t.Errorf("%s pushes by digest and never runs `docker buildx imagetools create`; "+
			"the tag would name no architecture at all", builderWorkflow)
	}
}

// The existence check in release.yml only proves the pre-built tag resolves,
// never that it is multi-arch, so a single-arch pre-build would read as
// "exists" and let an amd64 pgcopydb land inside the arm64 runner image.
func TestBuilderPlatformsAgreeAcrossWorkflows(t *testing.T) {
	// release.yml may build the builder itself or delegate to the workflow that
	// does. Delegating is what keeps the per-architecture split in one file, so
	// it satisfies the same requirement: what it ends up with is multi-arch.
	if delegatesBuilder(t, releaseWorkflow) {
		prebuild := builderPlatforms(t, builderWorkflow)
		requireMultiArch(t, builderWorkflow, prebuild)
		return
	}
	release := builderPlatforms(t, releaseWorkflow)
	if release == "" {
		t.Fatalf("%s: pgcopydb-builder build step declares no platforms", releaseWorkflow)
	}
	prebuild := builderPlatforms(t, builderWorkflow)
	if prebuild == "" {
		t.Fatalf("%s: pgcopydb-builder build step declares no platforms", builderWorkflow)
	}
	if release != prebuild {
		t.Errorf("release.yml builds pgcopydb-builder for %q, %s for %q; they must agree "+
			"or a single-arch pre-build passes the existence check unnoticed",
			release, builderWorkflow, prebuild)
	}
	requireMultiArch(t, releaseWorkflow, release)
	requireMultiArch(t, builderWorkflow, prebuild)
}

// A release that neither builds the builder nor calls the workflow that does
// would fall through both checks, so this answers only "does it call it".
func delegatesBuilder(t *testing.T, path string) bool {
	t.Helper()
	var wf workflow
	if err := yaml.Unmarshal([]byte(read(t, path)), &wf); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, job := range wf.Jobs {
		if strings.HasSuffix(job.Uses, "/pgcopydb-builder.yml") {
			return true
		}
	}
	return false
}

// Agreement alone is not enough: both files could agree on linux/amd64 only,
// which is exactly the tag the release check cannot tell from multi-arch.
func requireMultiArch(t *testing.T, path, platforms string) {
	t.Helper()
	list := strings.Split(platforms, ",")
	for _, want := range []string{"linux/amd64", "linux/arm64"} {
		if !slices.Contains(list, want) {
			t.Errorf("%s: pgcopydb-builder platforms are %q, missing %s", path, platforms, want)
		}
	}
}

// The pinned pgcopydb version is a runtime exact-match allowlist, not a
// hint: charts/values.yaml feeds gateScript()'s shell case, so an unlisted
// version matches nothing and fails closed with no error logged anywhere.
func TestPinnedVersionMatchesEveryAssertion(t *testing.T) {
	want := pin(t, builderDockerfile, "PGCOPYDB_VERSION")
	q := regexp.QuoteMeta(want)

	for _, c := range []struct {
		path string
		// pattern anchors to the specific construct why names, built from q
		// so a count can't be satisfied by an unrelated, coincidental hit.
		pattern *regexp.Regexp
		why     string
	}{
		{runnerDockerfile, regexp.MustCompile(`pgcopydb --version \| grep -qF '` + q + `'`),
			"the build canary that fails the image build on drift"},
		{releaseWorkflow, regexp.MustCompile(`pgcopydb --version \| grep -F '` + q + `'`),
			"the release smoke test that runs the pushed image"},
		{mainGo, regexp.MustCompile(`"progress-poll-versions", "` + q + `"`),
			"the --progress-poll-versions default"},
		{mainTest, regexp.MustCompile(`defaultPollVersions = "` + q + `"`),
			"the flag-default assertion"},
		{pollerTest, regexp.MustCompile(`const patchedVersion = "` + q + `"`),
			"the gate-script assertion's fixture constant"},
		// Anchored to the start of the line so a comment mentioning the same
		// pin and pipe elsewhere in the file cannot satisfy this, and to the
		// parenthesis the pattern list has to open with (see GateScript), so
		// the pin and that form are pinned by one expression.
		{pollerTest, regexp.MustCompile(`(?m)^\s*"\\n\(` + q + `\|`),
			"the gate-script assertion's case pattern"},
		// Anchored to the sentence that introduces the value, not just any
		// backticked mention, so a decoy mention elsewhere cannot satisfy this.
		{runnerReadme, regexp.MustCompile("The version string is `" + q + "`"),
			"the runner image's documented version string"},
		// Anchored to the table row (same line as the field name), not just
		// any backticked mention, so a decoy elsewhere cannot satisfy this.
		{chartReadme, regexp.MustCompile("progressPollVersions.*`[^`]*" + q + "[^`]*`"),
			"the documented default for runner.progressPollVersions"},
	} {
		if !c.pattern.MatchString(read(t, c.path)) {
			t.Errorf("%s has no construct matching pgcopydb %s (%s)", c.path, want, c.why)
		}
	}

	// The chart value is what actually ships, so assert the parsed list rather
	// than a substring: a commented-out line would satisfy Contains.
	var values struct {
		Runner struct {
			ProgressPollVersions []string `json:"progressPollVersions"`
		} `json:"runner"`
	}
	if err := yaml.Unmarshal([]byte(read(t, chartValues)), &values); err != nil {
		t.Fatalf("parse chart values: %v", err)
	}
	if !slices.Contains(values.Runner.ProgressPollVersions, want) {
		t.Errorf("runner.progressPollVersions is %v, which does not allow the pinned %s; "+
			"the in-pod progress poll would fail closed and status.progress would go dark",
			values.Runner.ProgressPollVersions, want)
	}
}

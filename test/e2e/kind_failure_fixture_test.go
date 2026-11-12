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

package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/types"
)

const kindBootstrapSpec = "Kind bootstrap migrates nonempty PostgreSQL 17 fixtures" +
	" with committed writer load and verified cutover"

const kindBootstrapLabel = "kind-bootstrap"
const kindSetupConnectStage = "kind-setup:connect"
const kindSetupFailureName = "suite-setup"

const kindReportWriter = "ginkgo-report-after-suite"
const kindFailureCanary = "inert-sensitive-failure-canary"

type kindSuiteFailure struct {
	NodeType string `json:"node_type"`
	TestName string `json:"test_name"`
	Stage    string `json:"stage"`
	File     string `json:"file,omitempty"`
	Line     int    `json:"line,omitempty"`
}

// kindSpecWitness requires exactly one executed, passed spec named want under label, and no other
// spec run; a native label also covers specs its run skipped, so only the bootstrap label refuses those.
func kindSpecWitness(report types.Report, label, want string) (executed, passed, skipped int, err error) {
	for _, spec := range report.SpecReports {
		if spec.LeafNodeType != types.NodeTypeIt {
			continue
		}
		labelled, labelErr := spec.MatchesLabelFilter(label)
		if labelErr != nil {
			return 0, 0, 0, labelErr
		}
		ran := spec.State != types.SpecStateSkipped && spec.State != types.SpecStatePending
		other := spec.FullText() != want
		if other && !ran && (!labelled || label != kindBootstrapLabel) {
			continue
		}
		if other || !labelled {
			return 0, 0, 0, errors.New("kind run selected an unexpected spec")
		}
		if spec.State == types.SpecStateSkipped || spec.State == types.SpecStatePending {
			skipped++
		} else {
			executed++
		}
		if spec.State == types.SpecStatePassed {
			passed++
		}
	}
	if !report.SuiteSucceeded || executed != 1 || passed != 1 || skipped != 0 {
		err = errors.New("kind run requires exactly one executed, named, passed spec")
	}
	return
}

func TestKindFailureProjectionOmitsSensitiveReportFields(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	spec := types.SpecReport{
		LeafNodeType: types.NodeTypeSynchronizedBeforeSuite,
		State:        types.SpecStateFailed,
		Failure: types.Failure{
			Message:        kindFailureCanary,
			ForwardedPanic: kindFailureCanary,
			Location:       types.CodeLocation{FileName: file, LineNumber: 123, FullStackTrace: kindFailureCanary},
		},
		LeafNodeText:               kindFailureCanary,
		ContainerHierarchyTexts:    []string{kindFailureCanary},
		CapturedGinkgoWriterOutput: kindFailureCanary,
		CapturedStdOutErr:          kindFailureCanary,
		SpecEvents: types.SpecEvents{
			{SpecEventType: types.SpecEventByStart, Message: kindSetupConnectStage},
			{SpecEventType: types.SpecEventByStart, Message: kindFailureCanary},
			{SpecEventType: types.SpecEventByStart, Message: "kind-setup:" + kindFailureCanary},
		},
	}
	got := kindSuiteFailures(types.Report{SpecReports: types.SpecReports{spec}})
	if len(got) != 1 || got[0].File != "test/e2e/kind_failure_fixture_test.go" || got[0].Line != 123 ||
		got[0].Stage != kindSetupConnectStage || got[0].NodeType != "SynchronizedBeforeSuite" ||
		got[0].TestName != kindSetupFailureName {
		t.Fatalf("safe failure projection missing expected identity: %+v", got)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal("marshal safe failure projection")
	}
	digest := sha256.Sum256([]byte(kindFailureCanary))
	for _, forbidden := range []string{
		kindFailureCanary, hex.EncodeToString(digest[:]), "Message", "Output", "StackTrace", filepath.Dir(file),
	} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("safe failure projection exposed sensitive report data")
		}
	}
	for _, location := range []types.CodeLocation{
		{FileName: "/outside/" + kindFailureCanary + ".go", LineNumber: 1},
		{FileName: "/outside/e2e_suite_test.go", LineNumber: 1},
		{FileName: filepath.Join(filepath.Dir(file), kindFailureCanary+".go"), LineNumber: 1},
		{FileName: file, LineNumber: 0},
	} {
		spec.Failure.Location = location
		got = kindSuiteFailures(types.Report{SpecReports: types.SpecReports{spec}})
		if len(got) != 1 || got[0].File != "" || got[0].Line != 0 {
			t.Fatal("unrecognized source location was persisted")
		}
	}
	spec.State = types.SpecStatePassed
	if len(kindSuiteFailures(types.Report{SuiteSucceeded: true, SpecReports: types.SpecReports{spec}})) != 0 {
		t.Fatal("passing node reported as a failure")
	}
}

func TestKindFailureBeforeSuiteFixture(t *testing.T) {
	if path := os.Getenv("RPGMIGRATE_KIND_FAILURE_FIXTURE"); path != "" {
		BeforeSuite(func() {
			By(kindSetupConnectStage)
			Fail(kindFailureCanary)
		})
		Describe("Kind bootstrap", Label(kindBootstrapLabel), func() {
			It(strings.TrimPrefix(kindBootstrapSpec, "Kind bootstrap "), func() { Fail("fixture spec must not execute") })
		})
		ReportAfterSuite("private failure fixture", func(report Report) {
			executed, passed, skipped, witnessErr := kindSpecWitness(report, kindBootstrapLabel, kindBootstrapSpec)
			receipt := struct {
				Reporter        string             `json:"reporter"`
				SuiteSucceeded  bool               `json:"suite_succeeded"`
				ExecutedSpecs   int                `json:"executed_specs"`
				PassedSpecs     int                `json:"passed_specs"`
				SkippedSpecs    int                `json:"skipped_specs"`
				WitnessAccepted bool               `json:"witness_accepted"`
				Failures        []kindSuiteFailure `json:"failures"`
			}{kindReportWriter, report.SuiteSucceeded, executed, passed, skipped, witnessErr == nil, kindSuiteFailures(report)}
			raw, err := json.Marshal(receipt)
			if err != nil || os.WriteFile(path, append(raw, '\n'), 0600) != nil {
				Fail("private fixture receipt write failed")
			}
		})
		RunSpecs(t, "private failure fixture")
		return
	}
	_, file, _, _ := runtime.Caller(0)
	path := filepath.Join(t.TempDir(), "receipt.json")
	command := exec.Command("go", "test", file, "-run", "^TestKindFailureBeforeSuiteFixture$", "-count=1", "-timeout=45s")
	command.Env = append(os.Environ(), "RPGMIGRATE_KIND_FAILURE_FIXTURE="+path)
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Run(); err == nil {
		t.Fatal("failed BeforeSuite fixture unexpectedly passed")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("failed BeforeSuite fixture did not retain a private receipt")
	}
	var receipt struct {
		Reporter        string             `json:"reporter"`
		SuiteSucceeded  bool               `json:"suite_succeeded"`
		ExecutedSpecs   int                `json:"executed_specs"`
		PassedSpecs     int                `json:"passed_specs"`
		WitnessAccepted bool               `json:"witness_accepted"`
		Failures        []kindSuiteFailure `json:"failures"`
	}
	if json.Unmarshal(raw, &receipt) != nil || receipt.Reporter != kindReportWriter || receipt.SuiteSucceeded ||
		receipt.ExecutedSpecs != 0 || receipt.PassedSpecs != 0 || receipt.WitnessAccepted || len(receipt.Failures) != 1 {
		t.Fatal("failed BeforeSuite fixture did not retain a nonpassing zero-execution witness")
	}
	failure := receipt.Failures[0]
	if failure.NodeType != "BeforeSuite" || failure.Stage != kindSetupConnectStage ||
		failure.TestName != kindSetupFailureName ||
		failure.File != "test/e2e/kind_failure_fixture_test.go" || failure.Line <= 0 {
		t.Fatal("failed BeforeSuite fixture lost structured failure identity")
	}
	digest := sha256.Sum256([]byte(kindFailureCanary))
	if strings.Contains(string(raw), kindFailureCanary) || strings.Contains(string(raw), hex.EncodeToString(digest[:])) {
		t.Fatal("failed BeforeSuite fixture persisted sensitive failure data")
	}
}

func kindSuiteFailures(report types.Report) []kindSuiteFailure {
	_, source, _, _ := runtime.Caller(0)
	failures := []kindSuiteFailure{}
	for _, spec := range report.SpecReports {
		if !spec.Failed() {
			continue
		}
		node := spec.Failure.FailureNodeType
		if node == types.NodeTypeInvalid {
			node = spec.LeafNodeType
		}
		failure := kindSuiteFailure{NodeType: "unclassified-node", TestName: "suite", Stage: "unclassified"}
		switch node {
		case types.NodeTypeBeforeSuite, types.NodeTypeSynchronizedBeforeSuite:
			failure.NodeType, failure.TestName = node.String(), kindSetupFailureName
		case types.NodeTypeAfterSuite, types.NodeTypeSynchronizedAfterSuite, types.NodeTypeCleanupAfterSuite:
			failure.NodeType, failure.TestName, failure.Stage = node.String(), "suite-cleanup", "suite-cleanup"
		case types.NodeTypeIt, types.NodeTypeBeforeEach, types.NodeTypeAfterEach, types.NodeTypeJustBeforeEach,
			types.NodeTypeJustAfterEach, types.NodeTypeReportBeforeEach, types.NodeTypeReportAfterEach:
			failure.NodeType = node.String()
			if spec.FullText() == kindBootstrapSpec {
				failure.TestName, failure.Stage = kindBootstrapSpec, kindBootstrapLabel
			}
		}
		for _, event := range spec.SpecEvents {
			if event.SpecEventType != types.SpecEventByStart {
				continue
			}
			switch event.Message {
			case kindSetupConnectStage, "kind-setup:crd-exists", "kind-setup:crd-schema", "kind-setup:storage",
				"kind-setup:namespaces", "kind-setup:release-ownership", "kind-setup:otel", "kind-setup:helm-values",
				"kind-setup:helm-install", "kind-setup:seed-config", "kind-setup:fixture-nodes",
				"kind-setup:fixture-clusters", "kind-setup:fixture-seed", "kind-setup:target-reset",
				"kind-setup:source-replication-reset", "kind-setup:target-replication-reset", "kind-setup:follow-privileges":
				failure.Stage = event.Message
			}
		}
		location := spec.Failure.Location
		relative, err := filepath.Rel(filepath.Dir(source), location.FileName)
		if err == nil && location.LineNumber > 0 {
			switch relative {
			case "e2e_suite_test.go", "kind_bootstrap_test.go", "kind_build_info_test.go", "kind_config_test.go",
				"kind_failure_fixture_test.go":
				failure.File, failure.Line = "test/e2e/"+relative, location.LineNumber
			}
		}
		failures = append(failures, failure)
	}
	return failures
}

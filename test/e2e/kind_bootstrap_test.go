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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/types"
	. "github.com/onsi/gomega"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const kindSequenceAspect = "sequence"
const kindAuditSchema = "audit"
const kindOrdersTable = "orders"
const kindPublicSchema = "public"
const kindSequenceFixture = "value=1 true"
const kindAcceptedWitness = "accepted witness"

type kindBootstrapEvidence struct {
	Phase                string `json:"phase"`
	Reason               string `json:"reason"`
	Complete             bool   `json:"complete"`
	CompleteReason       string `json:"complete_reason"`
	CutoverCompleted     bool   `json:"cutover_completed"`
	NonemptyDataVerified bool   `json:"nonempty_data_verified"`
	SequenceVerified     bool   `json:"sequence_verified"`
}

var kindEvidence kindBootstrapEvidence

func TestKindVerificationRejectsEmptyOrMismatchedFixtures(t *testing.T) {
	base := make([]fingerprintRow, 0, 13)
	for _, name := range []string{
		"customers", kindOrdersTable, "app_users", "events", "readings", "documents", "e2e_seed",
	} {
		base = append(base, fingerprintRow{Schema: kindPublicSchema, Aspect: compareDataCheck, Key: name, Value: "1 abc"})
	}
	base = append(base,
		fingerprintRow{Schema: kindAuditSchema, Aspect: compareDataCheck, Key: "events", Value: "1 abc"},
		fingerprintRow{
			Schema: kindPublicSchema, Aspect: kindSequenceAspect, Key: "customers_id_seq", Value: kindSequenceFixture,
		},
	)
	for _, seq := range []string{"orders_id_seq", "invoice_number_seq", "ticket_number_seq"} {
		base = append(base, fingerprintRow{
			Schema: kindPublicSchema, Aspect: kindSequenceAspect, Key: seq, Value: kindSequenceFixture,
		})
	}
	base = append(base, fingerprintRow{
		Schema: kindAuditSchema, Aspect: kindSequenceAspect, Key: "events_id_seq", Value: kindSequenceFixture,
	})
	for _, name := range []string{
		kindAcceptedWitness, "no fixtures", "missing table", "changed value", "sequence mismatch", "missing sequences",
	} {
		t.Run(name, func(t *testing.T) {
			source := append([]fingerprintRow{}, base...)
			target := append([]fingerprintRow{}, base...)
			switch name {
			case "no fixtures":
				source = nil
				target = nil
			case "missing table":
				source = source[1:]
				target = target[1:]
			case "changed value":
				target[0].Value = "1 def"
			case "sequence mismatch":
				target[len(target)-1].Value = "value=2 true"
			case "missing sequences":
				source = source[:len(source)-1]
				target = target[:len(target)-1]
			}
			if err := verifyKindFingerprints(source, target); (err == nil) != (name == kindAcceptedWitness) {
				t.Fatalf("verification = %v", err)
			}
		})
	}
}

func TestKindSpecWitnessRequiresExactExecutedPass(t *testing.T) {
	const otherSpecRan, anotherSpec = "other spec ran", "another spec"
	pass := types.SpecReport{
		ContainerHierarchyTexts: []string{"Kind bootstrap"},
		LeafNodeText:            strings.TrimPrefix(kindBootstrapSpec, "Kind bootstrap "),
		LeafNodeType:            types.NodeTypeIt, LeafNodeLabels: []string{kindBootstrapLabel}, State: types.SpecStatePassed,
	}
	for _, name := range []string{
		kindAcceptedWitness, "no selected specs", "skipped", "wrong name", "unlabelled", "two passes", "failed suite",
		"skipped foreign bootstrap", otherSpecRan,
	} {
		t.Run(name, func(t *testing.T) {
			report := types.Report{SuiteSucceeded: true, SpecReports: types.SpecReports{pass}}
			switch name {
			case "no selected specs":
				report.SpecReports = nil
			case "skipped":
				report.SpecReports[0].State = types.SpecStateSkipped
			case "wrong name":
				report.SpecReports[0].LeafNodeText = "another spec"
			case "unlabelled":
				report.SpecReports[0].LeafNodeLabels = nil
			case "two passes":
				report.SpecReports = append(report.SpecReports, pass)
			case "failed suite":
				report.SuiteSucceeded = false
			case "skipped foreign bootstrap":
				foreign := pass
				foreign.LeafNodeText, foreign.State = anotherSpec, types.SpecStateSkipped
				report.SpecReports = append(report.SpecReports, foreign)
			case otherSpecRan:
				other := pass
				other.LeafNodeText, other.LeafNodeLabels = anotherSpec, nil
				report.SpecReports = append(report.SpecReports, other)
			}
			_, _, _, err := kindSpecWitness(report, kindBootstrapLabel, kindBootstrapSpec)
			if (err == nil) != (name == kindAcceptedWitness) {
				t.Fatalf("witness = %v", err)
			}
		})
	}
}

const kindSchemaVersionField = "schema_version"
const kindSuspendSpec = "Native faults keeps its state while suspended and loses nothing after resume"

func kindNativeReport(label string, specs ...types.SpecReport) types.Report {
	return types.Report{SuiteSucceeded: true, SuiteConfig: types.SuiteConfig{LabelFilter: label}, SpecReports: specs}
}

func kindNativeSpec(leaf, label string, state types.SpecState) types.SpecReport {
	return types.SpecReport{ContainerHierarchyTexts: []string{"Native faults"}, LeafNodeText: leaf,
		ContainerHierarchyLabels: [][]string{{nativeFaultLabel}}, LeafNodeLabels: []string{label},
		LeafNodeType: types.NodeTypeIt, State: state}
}

func TestKindReceiptSelectsTheBoundEngineSpec(t *testing.T) {
	suspend := kindNativeSpec(strings.TrimPrefix(kindSuspendSpec, "Native faults "), "native-fault-suspend",
		types.SpecStatePassed)
	skippedDelete := kindNativeSpec("removes only its own source resources when deleted while streaming",
		"native-fault-delete", types.SpecStateSkipped)
	native := &kindSuite{input: kindSuiteInput{RunID: strings.Repeat("a", 32),
		Engine: string(v1beta1.MigrationEngineRPGMigrate), Pair: kindNativePairs[0], Spec: kindSuspendSpec},
		inputSHA256: "input"}
	receipt, err := kindReceipt(kindNativeReport(nativeFaultLabel, suspend, skippedDelete), native)
	if err != nil {
		t.Fatalf("native witness refused its bound spec: %v", err)
	}
	raw, _ := json.Marshal(receipt)
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, legacy := range []string{"phase", "reason", "complete", "complete_reason", "cutover_completed",
		"nonempty_data_verified", "sequence_verified"} {
		if _, ok := fields[legacy]; ok {
			t.Fatalf("native receipt carries the legacy claim %s", legacy)
		}
	}
	if receipt.SchemaVersion != 2 || receipt.SelectedSpec != kindSuspendSpec || receipt.Engine != native.input.Engine ||
		receipt.Pair != kindNativePairs[0] || receipt.ExecutedSpecs != 1 || receipt.PassedSpecs != 1 {
		t.Fatalf("native receipt = %s", raw)
	}
	// The guard's NATIVE_RECEIPT_KEYS in hack/e2e-kind.py admits exactly these fields.
	if got := slices.Sorted(maps.Keys(fields)); !slices.Equal(got, []string{
		"cleanup", "engine", "executed_specs", "failures",
		"input_sha256", "manager_runtime", "pair", "passed_specs", "reporter", "run_id", kindSchemaVersionField,
		"selected_spec", "skipped_specs", "suite_succeeded"}) {
		t.Fatalf("native receipt fields = %v", got)
	}
	for name, report := range map[string]types.Report{
		"another native spec ran": kindNativeReport(nativeFaultLabel, suspend,
			kindNativeSpec(skippedDelete.LeafNodeText, "native-fault-delete", types.SpecStatePassed)),
		"bound spec skipped": kindNativeReport(nativeFaultLabel,
			kindNativeSpec(suspend.LeafNodeText, "native-fault-suspend", types.SpecStateSkipped)),
		"bootstrap label":    kindNativeReport(kindBootstrapLabel, suspend),
		"compound label":     kindNativeReport(nativeFaultLabel+" || "+nativeLabel, suspend),
		"bound spec missing": kindNativeReport(nativeFaultLabel, skippedDelete),
	} {
		if _, err := kindReceipt(report, native); err == nil {
			t.Errorf("native receipt witnessed %s", name)
		}
	}
	legacy := &kindSuite{input: kindSuiteInput{Engine: string(v1beta1.MigrationEnginePGCopyDB), Pair: kindLegacyPair,
		Spec: kindBootstrapSpec}}
	pass := types.SpecReport{ContainerHierarchyTexts: []string{"Kind bootstrap"},
		LeafNodeText: strings.TrimPrefix(kindBootstrapSpec, "Kind bootstrap "), LeafNodeType: types.NodeTypeIt,
		LeafNodeLabels: []string{kindBootstrapLabel}, State: types.SpecStatePassed}
	receipt, err = kindReceipt(types.Report{SuiteSucceeded: true, SpecReports: types.SpecReports{pass}}, legacy)
	if err != nil || receipt.SchemaVersion != 2 || receipt.SelectedSpec != kindBootstrapSpec ||
		receipt.kindBootstrapEvidence == nil {
		t.Fatalf("legacy receipt = %+v, %v", receipt, err)
	}
	if _, err := kindReceipt(kindNativeReport(nativeFaultLabel, suspend), legacy); err == nil {
		t.Fatal("legacy receipt witnessed a native spec")
	}
}

func TestKindNativeFocusAnchorsTheBoundSpec(t *testing.T) {
	legacy := &kindSuite{input: kindSuiteInput{Engine: string(v1beta1.MigrationEnginePGCopyDB), Spec: kindBootstrapSpec}}
	if legacy.kindFocus() != nil {
		t.Fatal("legacy run gained a focus")
	}
	native := &kindSuite{input: kindSuiteInput{Engine: string(v1beta1.MigrationEngineRPGMigrate),
		Spec: "Native faults completes or fails closed when the runner is killed mid-drain (x)"}}
	focus := native.kindFocus()
	if len(focus) != 1 {
		t.Fatalf("native focus = %q", focus)
	}
	pattern := regexp.MustCompile(focus[0])
	exact := kindSuiteDescription + " " + native.input.Spec
	for text, want := range map[string]bool{exact: true, exact + " again": false, "x" + exact: false,
		kindSuiteDescription + " " + strings.ReplaceAll(native.input.Spec, "(x)", "x"): false} {
		if pattern.MatchString(text) != want {
			t.Errorf("focus %q matched %q = %t", focus[0], text, !want)
		}
	}
}

func verifyKindFingerprints(source, target []fingerprintRow) error {
	selectRows := func(rows []fingerprintRow) (map[string]string, map[string]string) {
		data, seq := map[string]string{}, map[string]string{}
		for _, row := range rows {
			key := row.Schema + "." + row.Key
			switch row.Aspect {
			case compareDataCheck:
				data[key] = row.Value
			case kindSequenceAspect:
				seq[key] = row.Value
			}
		}
		return data, seq
	}
	sourceData, sourceSeq := selectRows(source)
	targetData, targetSeq := selectRows(target)
	for _, name := range []string{
		otelTable, "public.orders", "public.app_users", "public.events",
		"public.readings", "public.documents", "public.e2e_seed", "audit.events",
	} {
		fields := strings.Fields(sourceData[name])
		if len(fields) != 2 {
			return errors.New("required kind fixture table is missing")
		}
		count, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil || count <= 0 {
			return errors.New("required kind fixture table is empty")
		}
	}
	if !reflect.DeepEqual(sourceData, targetData) {
		return errors.New("kind fixture table inventory or contents differ")
	}
	for _, name := range []string{
		"public.customers_id_seq", "public.orders_id_seq", "audit.events_id_seq",
		"public.invoice_number_seq", "public.ticket_number_seq",
	} {
		if sourceSeq[name] == "" {
			return errors.New("required kind fixture sequence is missing")
		}
	}
	if len(sourceSeq) == 0 || !reflect.DeepEqual(sourceSeq, targetSeq) {
		return errors.New("kind fixture sequence inventory or state differs")
	}
	return nil
}

var _ = Describe("Kind bootstrap", Label(kindBootstrapLabel), func() {
	It("migrates nonempty PostgreSQL 17 fixtures with committed writer load and verified cutover", func() {
		if activeKindSuite == nil {
			Skip("requires the private kind wrapper")
		}
		Expect(pgSource).To(Equal(17))
		Expect(pgTarget).To(Equal(17))
		resetMigrationPair()
		const name = "e2e-kind-bootstrap"
		const marker = "kind-bootstrap-load"
		migration := newFollowMigration(name, v1beta1.CutoverManual)
		migration.Spec.Engine = v1beta1.MigrationEnginePGCopyDB
		migration.Spec.Runner.Image = selectedRunnerImage()
		migration.Spec.Verification = &v1beta1.VerificationOptions{Schema: true, Data: true}
		writer := startLiveWriter(marker)
		DeferCleanup(func() { _, _ = writer.stop() })
		create(migration)
		waitFollowStreaming(name)
		last, err := writer.stop()
		Expect(publicationRetryProbeError(err)).NotTo(HaveOccurred(), "kind bootstrap writer did not finish successfully")
		Expect(last).To(BeNumerically(">", 0), "kind bootstrap writer committed no transactions")
		Expect(psql(sourceCluster, liveMarkerQuery(marker))).To(Equal(strconv.Itoa(last)))
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCutoverPending)
		approveCutover(name)
		terminal := waitPhase(name, nsE2E, followTimeout, v1beta1.PhaseCompleted)
		expectConditionTrue(terminal, v1beta1.ConditionComplete)
		expectConditionTrue(terminal, v1beta1.ConditionCutoverComplete)
		expectConditionTrue(terminal, v1beta1.ConditionVerified)
		expectCleanupSucceeded(name)
		complete := apimeta.FindStatusCondition(terminal.Status.Conditions, v1beta1.ConditionComplete)
		cutover := apimeta.FindStatusCondition(terminal.Status.Conditions, v1beta1.ConditionCutoverComplete)
		kindEvidence.Phase = string(terminal.Status.Phase)
		kindEvidence.Complete = complete.Status == metav1.ConditionTrue
		kindEvidence.CompleteReason = complete.Reason
		kindEvidence.CutoverCompleted = cutover.Status == metav1.ConditionTrue
		kindEvidence.Reason = cutover.Reason
		read := func(cluster string) []fingerprintRow {
			readCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			rows, readErr := readFingerprint(readCtx, clusterPSQL(cluster), []string{kindPublicSchema, kindAuditSchema})
			Expect(publicationRetryProbeError(readErr)).NotTo(HaveOccurred(), "independent kind fixture fingerprint failed")
			return rows
		}
		source, target := read(sourceCluster), read(targetCluster)
		err = verifyKindFingerprints(source, target)
		Expect(err).NotTo(HaveOccurred())
		kindEvidence.NonemptyDataVerified = err == nil
		kindEvidence.SequenceVerified = err == nil
		gap := psql(targetCluster, fmt.Sprintf(
			"SELECT coalesce(min(g),0) FROM generate_series(1,%d) g"+
				" WHERE NOT EXISTS (SELECT 1 FROM orders WHERE note='"+marker+"-' || g)", last))
		Expect(gap).To(Equal("0"), "kind bootstrap lost a committed transaction")
		Eventually(sourceSlotCount, 2*time.Minute, time.Second).Should(Equal("0"))
		Eventually(targetOriginCount, 2*time.Minute, time.Second).Should(Equal("0"))
	})
})

type kindSuiteReceipt struct {
	Reporter       string                     `json:"reporter"`
	SuiteSucceeded bool                       `json:"suite_succeeded"`
	Failures       []kindSuiteFailure         `json:"failures"`
	ManagerRuntime kindManagerRuntimeEvidence `json:"manager_runtime"`
	SchemaVersion  int                        `json:"schema_version"`
	RunID          string                     `json:"run_id"`
	Engine         string                     `json:"engine"`
	Pair           string                     `json:"pair"`
	InputSHA256    string                     `json:"input_sha256"`
	SelectedSpec   string                     `json:"selected_spec"`
	ExecutedSpecs  int                        `json:"executed_specs"`
	PassedSpecs    int                        `json:"passed_specs"`
	SkippedSpecs   int                        `json:"skipped_specs"`
	Cleanup        bool                       `json:"cleanup"`
	// Nil for a native run, which then carries no legacy bootstrap claims.
	*kindBootstrapEvidence
}

// kindReceipt selects the spec the run's own engine bound and witnesses it; schema 2 is what the guard validates.
func kindReceipt(report Report, run *kindSuite) (kindSuiteReceipt, error) {
	label, spec, evidence := kindBootstrapLabel, kindBootstrapSpec, &kindEvidence
	if run.input.Engine == string(v1beta1.MigrationEngineRPGMigrate) {
		label, spec, evidence = report.SuiteConfig.LabelFilter, run.input.Spec, nil
	}
	executed, passed, skipped, err := kindSpecWitness(report, label, spec)
	if err == nil && (!kindLabelFilterAllowed(v1beta1.MigrationEngine(run.input.Engine), label) ||
		!kindSelectionValid(run.input.Engine, run.input.Pair, spec)) {
		err = errors.New("kind receipt selection differs from its engine")
	}
	return kindSuiteReceipt{
		Reporter: kindReportWriter, SuiteSucceeded: report.SuiteSucceeded, Failures: kindSuiteFailures(report),
		SchemaVersion: 2, RunID: run.input.RunID, Engine: run.input.Engine,
		Pair: run.input.Pair, InputSHA256: run.inputSHA256, SelectedSpec: spec,
		ExecutedSpecs: executed, PassedSpecs: passed, SkippedSpecs: skipped,
		Cleanup: kindFixturesCleaned, kindBootstrapEvidence: evidence, ManagerRuntime: kindManagerRuntime,
	}, err
}

var _ = ReportAfterSuite("private kind receipt", func(report Report) {
	if activeKindSuite == nil {
		return
	}
	receipt, witnessErr := kindReceipt(report, activeKindSuite)
	data, err := json.Marshal(receipt)
	Expect(err).NotTo(HaveOccurred())
	path := filepath.Join(activeKindSuite.dir, "suite-receipt.json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	Expect(err).NotTo(HaveOccurred(), "kind receipt already exists or cannot be created")
	_, writeErr := file.Write(append(data, '\n'))
	syncErr := file.Sync()
	closeErr := file.Close()
	Expect(errors.Join(writeErr, syncErr, closeErr)).NotTo(HaveOccurred(), "writing private kind receipt")
	Expect(errors.Join(witnessErr, kindManagerRuntimeWitness(activeKindSuite, kindManagerRuntime))).NotTo(HaveOccurred())
	Expect(kindFixturesCleaned).To(BeTrue(), "kind fixtures were not observed cleaned")
})

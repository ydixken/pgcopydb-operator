package buildconfig

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func compareFeatureCRDs(base, head map[string]string, profile string) error {
	if profile != "" && profile != identicalSchemaProfile && profile != disposableSchemaProfile {
		return fmt.Errorf("unknown schema validation profile")
	}
	if maps.Equal(base, head) {
		return nil
	}
	if profile != disposableSchemaProfile || len(base) != len(head) {
		return fmt.Errorf("CRD inventory or schema differs")
	}
	for identity, original := range base {
		candidate, found := head[identity]
		if !found {
			return fmt.Errorf("CRD identity differs")
		}
		var before, after compatibilityDocument
		if json.Unmarshal([]byte(original), &before) != nil || json.Unmarshal([]byte(candidate), &after) != nil {
			return fmt.Errorf("CRD is not canonical JSON")
		}
		if err := compareFeatureCRDDocument(before, after); err != nil {
			return err
		}
	}
	return nil
}

func compareFeatureCRDDocument(base, head map[string]any) error {
	baseSpec, baseOK := base[specKey].(map[string]any)
	headSpec, headOK := head[specKey].(map[string]any)
	if !baseOK || !headOK {
		return fmt.Errorf("CRD specification is missing")
	}
	baseVersions, baseOK := baseSpec["versions"].([]any)
	headVersions, headOK := headSpec["versions"].([]any)
	if !baseOK || !headOK || len(baseVersions) != len(headVersions) || len(baseVersions) == 0 {
		return fmt.Errorf("CRD versions differ or are missing")
	}
	var servedAdditions map[string]string
	for i, value := range baseVersions {
		before, beforeOK := value.(map[string]any)
		after, afterOK := headVersions[i].(map[string]any)
		if !beforeOK || !afterOK {
			return fmt.Errorf("CRD version is malformed")
		}
		beforeSchema, beforeOK := before["schema"].(map[string]any)
		afterSchema, afterOK := after["schema"].(map[string]any)
		if !beforeOK || !afterOK {
			return fmt.Errorf("CRD schema is missing")
		}
		beforeRoot, beforeOK := beforeSchema["openAPIV3Schema"].(map[string]any)
		afterRoot, afterOK := afterSchema["openAPIV3Schema"].(map[string]any)
		if !beforeOK || !afterOK {
			return fmt.Errorf("CRD root schema is missing")
		}
		additions := map[string]string{}
		if err := validateFeatureSchemaAdditions(beforeRoot, afterRoot, nil, additions); err != nil {
			return err
		}
		if before["served"] == true {
			if servedAdditions != nil && !maps.Equal(servedAdditions, additions) {
				return fmt.Errorf("served versions have different schema additions")
			}
			servedAdditions = additions
		}
		delete(beforeSchema, "openAPIV3Schema")
		delete(afterSchema, "openAPIV3Schema")
	}
	if !reflect.DeepEqual(base, head) {
		return fmt.Errorf("existing CRD identity or schema metadata differs")
	}
	return nil
}

func validateFeatureSchemaAdditions(base, head map[string]any, path []string, additions map[string]string) error {
	if reflect.DeepEqual(base, head) {
		return nil
	}
	before, beforeOK := base[schemaPropertiesKey].(map[string]any)
	after, afterOK := head[schemaPropertiesKey].(map[string]any)
	if _, exists := base[schemaPropertiesKey]; exists && !beforeOK {
		return fmt.Errorf("invalid original schema properties")
	}
	if _, exists := head[schemaPropertiesKey]; exists && !afterOK {
		return fmt.Errorf("invalid candidate schema properties")
	}
	for key, child := range before {
		candidate, exists := after[key]
		if !exists {
			return fmt.Errorf("existing property removed")
		}
		baseChild, baseOK := child.(map[string]any)
		headChild, headOK := candidate.(map[string]any)
		if !baseOK || !headOK {
			return fmt.Errorf("schema property is not an object")
		}
		childPath := append(slices.Clone(path), key)
		if err := validateFeatureSchemaAdditions(baseChild, headChild, childPath, additions); err != nil {
			return err
		}
	}
	for key, child := range after {
		if _, exists := before[key]; exists {
			continue
		}
		if len(path) == 0 || (path[0] != specKey && path[0] != statusKey) {
			return fmt.Errorf("new property outside spec or status")
		}
		if _, valid := child.(map[string]any); !valid {
			return fmt.Errorf("new property is not a schema")
		}
		encoded, err := json.Marshal(child)
		if err != nil {
			return err
		}
		encodedPath, err := json.Marshal(append(slices.Clone(path), key))
		if err != nil {
			return err
		}
		additions["property:"+string(encodedPath)] = string(encoded)
	}
	baseKeywords, headKeywords := maps.Clone(base), maps.Clone(head)
	delete(baseKeywords, schemaPropertiesKey)
	delete(headKeywords, schemaPropertiesKey)
	if !reflect.DeepEqual(baseKeywords["x-kubernetes-validations"], headKeywords["x-kubernetes-validations"]) &&
		slices.Equal(path, []string{specKey, "clone"}) && approvedSplitRule(base, head) {
		encoded, err := json.Marshal(headKeywords["x-kubernetes-validations"])
		if err != nil {
			return err
		}
		additions["validation:spec/clone"] = string(encoded)
		delete(baseKeywords, "x-kubernetes-validations")
		delete(headKeywords, "x-kubernetes-validations")
	}
	if !reflect.DeepEqual(baseKeywords, headKeywords) {
		return fmt.Errorf("existing schema keyword differs")
	}
	return nil
}

func approvedSplitRule(base, head map[string]any) bool {
	baseProps, _ := base[schemaPropertiesKey].(map[string]any)
	headProps, _ := head[schemaPropertiesKey].(map[string]any)
	if _, exists := baseProps["splitTables"]; exists {
		return false
	}
	field, ok := headProps["splitTables"].(map[string]any)
	if !ok || field[schemaTypeKey] != schemaBooleanType || field[schemaDefaultKey] != true {
		return false
	}
	before, _ := base["x-kubernetes-validations"].([]any)
	after, ok := head["x-kubernetes-validations"].([]any)
	if !ok || len(after) != len(before)+1 || !slices.EqualFunc(before, after[:len(before)], reflect.DeepEqual) {
		return false
	}
	rule, ok := after[len(before)].(map[string]any)
	if !ok || len(rule) != 2 || rule["rule"] != splitTablesRule {
		return false
	}
	message, ok := rule["message"].(string)
	return ok && strings.TrimSpace(message) != ""
}

func compareFeatureRenderedPrivileges(base, head []string, profile string) error {
	baseCRDs, headCRDs := map[string]string{}, map[string]string{}
	other := make([][]string, 2)
	for i, documents := range [][]string{base, head} {
		crds := baseCRDs
		if i == 1 {
			crds = headCRDs
		}
		seen := map[string]bool{}
		for _, document := range documents {
			identity, body, ok := strings.Cut(document, "\x00")
			if !ok || seen[identity] {
				return fmt.Errorf("rendered identity is missing or duplicated")
			}
			seen[identity] = true
			if strings.Contains(identity, "/CustomResourceDefinition/") {
				crds[identity] = body
			} else {
				other[i] = append(other[i], document)
			}
		}
	}
	if !slices.Equal(other[0], other[1]) {
		return fmt.Errorf("rendered RBAC differs")
	}
	return compareFeatureCRDs(baseCRDs, headCRDs, profile)
}

const (
	schemaPropertiesKey       = "properties"
	schemaDefaultKey          = "default"
	schemaBooleanType         = "boolean"
	schemaRequiredKey         = "required"
	unknownValue              = "unknown"
	optionalSchemaMutation    = "optional"
	schemaListFixture         = "list"
	asymmetricSchemaMutation  = "asymmetric"
	unguardedSplitMutation    = "unguarded"
	oldRuleSplitMutation      = "old-rule"
	missingSplitMutation      = "no-split"
	falseSplitMutation        = "false-split"
	extraSplitKeyMutation     = "extra-key"
	emptySplitMessageMutation = "empty-message"
	featureClusterJob         = "cluster"
	identicalSchemaProfile    = "identical"
	disposableSchemaProfile   = "additive-disposable"
	splitTablesRule           = "!has(self.splitTables) || self.splitTables || " +
		"(!has(self.splitTablesLargerThan) && !has(self.splitMaxParts))"
)

func TestFeatureE2ESchemaProfileAdmission(t *testing.T) {
	t.Setenv("FEATURE_E2E_HELM", newCompatibilityHelmFixture(t))
	for _, tt := range []struct {
		name      string
		profile   string
		mutation  string
		wantError bool
	}{
		{"unset identical", "", "", false},
		{"explicit identical", identicalSchemaProfile, "", false},
		{"disposable smoke", disposableSchemaProfile, "", false},
		{"unknown profile", unknownValue, "", true},
		{"optional boolean strict", identicalSchemaProfile, optionalSchemaMutation, true},
		{"optional boolean both versions", disposableSchemaProfile, optionalSchemaMutation, false},
		{"optional status child requirements", disposableSchemaProfile, "status", false},
		{"changed existing default", disposableSchemaProfile, schemaDefaultKey, true},
		{"changed existing type", disposableSchemaProfile, schemaTypeKey, true},
		{"changed existing bounds", disposableSchemaProfile, "bounds", true},
		{"changed existing pruning", disposableSchemaProfile, "pruning", true},
		{"changed existing enum", disposableSchemaProfile, "enum", true},
		{"changed existing nullable", disposableSchemaProfile, "nullable", true},
		{"changed existing list semantics", disposableSchemaProfile, schemaListFixture, true},
		{"changed existing combinator", disposableSchemaProfile, "combinator", true},
		{"removed existing property", disposableSchemaProfile, "remove", true},
		{"new required property", disposableSchemaProfile, schemaRequiredKey, true},
		{"property outside spec and status", disposableSchemaProfile, "outside", true},
		{"asymmetric served versions", disposableSchemaProfile, asymmetricSchemaMutation, true},
		{"changed API group", disposableSchemaProfile, "group", true},
		{"changed conversion", disposableSchemaProfile, "conversion", true},
		{"changed storage version", disposableSchemaProfile, "storage", true},
		{"new guarded split rule", disposableSchemaProfile, "split", false},
		{"unguarded parent rule", disposableSchemaProfile, unguardedSplitMutation, true},
		{"changed old parent rule", disposableSchemaProfile, oldRuleSplitMutation, true},
		{"missing new split field", disposableSchemaProfile, missingSplitMutation, true},
		{"false split default", disposableSchemaProfile, falseSplitMutation, true},
		{"extra split rule key", disposableSchemaProfile, extraSplitKeyMutation, true},
		{"empty split message", disposableSchemaProfile, emptySplitMessageMutation, true},
		{"rendered-only drift", disposableSchemaProfile, "rendered", true},
		{"builder mode changed", disposableSchemaProfile, "builder-mode", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("FEATURE_E2E_SCHEMA_VALIDATION", tt.profile)
			base, head := newCompatibilityChartRoots(t)
			for _, root := range []string{base, head} {
				doc := featureSchemaFixture(t)
				if root == head {
					mutateFeatureSchema(doc, tt.mutation)
				}
				body, err := json.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				writeCompatibilityFixture(t, root, "config/crd/bases/migration.yaml", string(body)+"\n")
				if tt.mutation == "rendered" && root == head {
					doc[specKey].(map[string]any)["group"] = "changed.example"
					body, err = json.Marshal(doc)
					if err != nil {
						t.Fatal(err)
					}
				}
				writeCompatibilityFixture(t, root, "charts/pgcopydb-operator/templates/crd.yaml", string(body)+"\n")
			}
			if tt.mutation == "builder-mode" {
				if err := os.Chmod(head+"/images/pgcopydb-builder/Dockerfile", 0o755); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestFeatureE2ECandidateCompatibility$", "-test.count=1")
			cmd.Env = compatibilityFixtureEnv(base, head)
			output, err := cmd.CombinedOutput()
			if (err != nil) != tt.wantError {
				t.Fatalf("compatibility error = %v, want error %v:\n%s", err, tt.wantError, output)
			}
			if err != nil && !strings.Contains(string(output), "candidate changes") {
				t.Fatalf("fixture failed before candidate comparison: %v\n%s", err, output)
			}
		})
	}
}

func TestFeatureE2ESchemaLargeIntegerDefaults(t *testing.T) {
	t.Setenv("FEATURE_E2E_HELM", newCompatibilityHelmFixture(t))
	for _, tt := range []struct {
		name, profile, sourceDefault, renderedDefault string
		existing, sourceError, renderedError          bool
	}{
		{"equal additions", disposableSchemaProfile, "9007199254740993", "9007199254740993", false, false, false},
		{"asymmetric source", disposableSchemaProfile, "9007199254740992", "9007199254740993", false, true, false},
		{"asymmetric render", disposableSchemaProfile, "9007199254740993", "9007199254740992", false, false, true},
		{"asymmetric both", disposableSchemaProfile, "9007199254740992", "9007199254740992", false, true, true},
		{"strict existing change", identicalSchemaProfile, "9007199254740993", "9007199254740993", true, true, true},
		{"additive existing change", disposableSchemaProfile, "9007199254740993", "9007199254740993", true, true, true},
	} {
		for _, prefix := range []string{"", "---\n"} {
			t.Run(fmt.Sprintf("%s/yaml=%t", tt.name, prefix != ""), func(t *testing.T) {
				t.Setenv("FEATURE_E2E_SCHEMA_VALIDATION", tt.profile)
				base, head := newCompatibilityChartRoots(t)
				for _, root := range []string{base, head} {
					for path, firstDefault := range map[string]string{
						"config/crd/bases/migration.yaml":             tt.sourceDefault,
						"charts/pgcopydb-operator/templates/crd.yaml": tt.renderedDefault,
					} {
						doc := featureSchemaFixture(t)
						if root == head || tt.existing {
							versions := doc[specKey].(map[string]any)["versions"].([]any)
							for i, item := range versions {
								value := "9007199254740993"
								if root == base {
									value = "9007199254740992"
								} else if i == 0 {
									value = firstDefault
								}
								version := item.(map[string]any)
								schema := version["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)
								spec := schema[schemaPropertiesKey].(map[string]any)[specKey].(map[string]any)
								spec[schemaPropertiesKey].(map[string]any)["largeDefault"] = map[string]any{
									schemaTypeKey: "integer", schemaDefaultKey: json.Number(value),
								}
							}
						}
						body, err := json.Marshal(doc)
						if err != nil {
							t.Fatal(err)
						}
						writeCompatibilityFixture(t, root, path, prefix+string(body)+"\n")
					}
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestFeatureE2ECandidateCompatibility$", "-test.count=1")
				cmd.Env = compatibilityFixtureEnv(base, head)
				output, err := cmd.CombinedOutput()
				if (err != nil) != (tt.sourceError || tt.renderedError) ||
					strings.Contains(string(output), "candidate changes the CRD") != tt.sourceError ||
					strings.Contains(string(output), "candidate changes rendered") != tt.renderedError {
					t.Fatalf("numeric admission error = %v, want source=%t rendered=%t:\n%s",
						err, tt.sourceError, tt.renderedError, output)
				}
			})
		}
	}
}

func featureSchemaFixture(t *testing.T) map[string]any {
	t.Helper()
	const schema = `{"type":"object","properties":{
  "spec":{"type":"object","properties":{
    "clone":{"type":"object","x-kubernetes-validations":[{"rule":"true","message":"existing"}],
      "properties":{"splitTablesLargerThan":{"type":"integer","minimum":0},
        "splitMaxParts":{"type":"integer","minimum":0}}},
    "follow":{"type":"boolean","default":false}}},
  "status":{"type":"object","properties":{"phase":{"type":"string"}}}}}`
	var doc map[string]any
	body := `{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition",
  "metadata":{"name":"migrations.pgcopydb-operator.io"},
  "spec":{"group":"pgcopydb-operator.io","scope":"Namespaced",
    "names":{"kind":"Migration","plural":"migrations"},"versions":[
      {"name":"v1alpha1","served":true,"storage":false,"schema":{"openAPIV3Schema":` + schema + `}},
      {"name":"v1beta1","served":true,"storage":true,"schema":{"openAPIV3Schema":` + schema + `}}]}}`
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

//nolint:gocyclo // Each mutation targets one admission invariant.
func mutateFeatureSchema(doc map[string]any, mutation string) {
	spec := doc[specKey].(map[string]any)
	if mutation == "group" {
		spec["group"] = "changed.example"
	}
	if mutation == "conversion" {
		spec["conversion"] = map[string]any{"strategy": "None"}
	}
	for i, item := range spec["versions"].([]any) {
		version := item.(map[string]any)
		if mutation == "storage" {
			version["storage"] = i == 0
		}
		root := version["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)
		props := root[schemaPropertiesKey].(map[string]any)
		specSchema := props[specKey].(map[string]any)
		fields := specSchema[schemaPropertiesKey].(map[string]any)
		follow := fields["follow"].(map[string]any)
		clone := fields["clone"].(map[string]any)
		switch mutation {
		case optionalSchemaMutation, schemaRequiredKey, asymmetricSchemaMutation:
			if mutation != asymmetricSchemaMutation || i == 0 {
				fields["requireSameMajorVersion"] = map[string]any{schemaTypeKey: schemaBooleanType, schemaDefaultKey: false,
					"x-kubernetes-validations": []any{map[string]any{"rule": "self == oldSelf", "message": "immutable"}}}
			}
			if mutation == schemaRequiredKey {
				specSchema[schemaRequiredKey] = []any{"requireSameMajorVersion"}
			}
		case "status":
			props[statusKey].(map[string]any)[schemaPropertiesKey].(map[string]any)["sample"] = map[string]any{
				schemaTypeKey: "object", schemaRequiredKey: []any{"count"},
				schemaPropertiesKey: map[string]any{"count": map[string]any{schemaTypeKey: "integer"}},
			}
		case schemaDefaultKey:
			follow[schemaDefaultKey] = true
		case schemaTypeKey:
			follow[schemaTypeKey] = schemaStringType
		case "bounds":
			clone[schemaPropertiesKey].(map[string]any)["splitMaxParts"].(map[string]any)["minimum"] = 1
		case "pruning":
			specSchema["x-kubernetes-preserve-unknown-fields"] = true
		case "enum":
			follow["enum"] = []any{true}
		case "nullable":
			follow["nullable"] = true
		case schemaListFixture:
			clone["x-kubernetes-map-type"] = "atomic"
		case "combinator":
			specSchema["allOf"] = []any{map[string]any{schemaRequiredKey: []any{"follow"}}}
		case "remove":
			delete(fields, "follow")
		case "outside":
			props["extra"] = map[string]any{schemaTypeKey: schemaStringType}
		case "split", unguardedSplitMutation, oldRuleSplitMutation, missingSplitMutation,
			falseSplitMutation, extraSplitKeyMutation, emptySplitMessageMutation:
			newRule := map[string]any{
				"rule": "!has(self.splitTables) || self.splitTables || " +
					"(!has(self.splitTablesLargerThan) && !has(self.splitMaxParts))",
				"message": "split tuning requires splitting",
			}
			if mutation == unguardedSplitMutation {
				newRule["rule"] = "self.splitTables"
			}
			if mutation == extraSplitKeyMutation {
				newRule["reason"] = "FieldValueInvalid"
			}
			if mutation == emptySplitMessageMutation {
				newRule["message"] = ""
			}
			if mutation != missingSplitMutation {
				clone[schemaPropertiesKey].(map[string]any)["splitTables"] = map[string]any{
					schemaTypeKey: schemaBooleanType, schemaDefaultKey: mutation != falseSplitMutation,
				}
			}
			clone["x-kubernetes-validations"] = append(clone["x-kubernetes-validations"].([]any), newRule)
			if mutation == oldRuleSplitMutation {
				clone["x-kubernetes-validations"].([]any)[0].(map[string]any)["rule"] = "false"
			}
		}
	}
}

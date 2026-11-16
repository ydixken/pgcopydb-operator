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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// apiConstants returns the string values of the exported constants in the v1beta1 API types
// whose names start with prefix, so a new reason or condition type cannot skip the reference.
func apiConstants(t *testing.T, prefix string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "../../api/v1beta1/migration_types.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var values []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, prefix) || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, value)
			}
		}
	}
	if len(values) == 0 {
		t.Fatalf("no %s constants found in the API types", prefix)
	}
	return values
}

// section returns the Markdown between heading and the next heading of the same level.
func section(t *testing.T, doc, heading string) string {
	t.Helper()
	_, rest, ok := strings.Cut(doc, "\n"+heading+"\n")
	if !ok {
		t.Fatalf("conditions.md has no %q section", heading)
	}
	body, _, _ := strings.Cut(rest, "\n## ")
	return body
}

func TestConditionsReferenceListsEveryTypeAndReason(t *testing.T) {
	raw, err := os.ReadFile("../../docs/reference/conditions.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	types, reasons := section(t, doc, "## Condition types"), section(t, doc, "## Reasons")
	for _, value := range apiConstants(t, "Condition") {
		if !strings.Contains(types, "| `"+value+"` |") {
			t.Errorf("condition type %q is missing from the Condition types table of docs/reference/conditions.md", value)
		}
	}
	for _, value := range apiConstants(t, "Reason") {
		if !strings.Contains(reasons, "| `"+value+"` |") {
			t.Errorf("reason %q is missing from the Reasons table of docs/reference/conditions.md", value)
		}
	}
}

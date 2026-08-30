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

package fixtures

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const suiteFile = "../e2e/e2e_suite_test.go"

// seedJob is the part of the seed Job the fixtures depend on, read out of the
// suite's source. Parsed rather than called: package e2e cannot be imported
// here, because make test excludes it and its only entry point wants a cluster.
type seedJob struct {
	command    []string
	env        []string
	mountPaths []string
	embeds     []string
}

func readSeedJob(t *testing.T) seedJob {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), suiteFile, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing %s: %v", suiteFile, err)
	}

	var j seedJob
	for _, group := range f.Comments {
		for _, c := range group.List {
			if rest, ok := strings.CutPrefix(c.Text, "//go:embed "); ok {
				j.embeds = append(j.embeds, strings.Fields(rest)...)
			}
		}
	}

	var fn *ast.FuncDecl
	for _, d := range f.Decls {
		if decl, ok := d.(*ast.FuncDecl); ok && decl.Name.Name == "buildSeedJob" {
			fn = decl
			break
		}
	}
	if fn == nil {
		t.Fatalf("%s declares no buildSeedJob", suiteFile)
	}

	ast.Inspect(fn, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			return true
		}
		switch key.Name {
		case "Command":
			j.command = append(j.command, strLits(kv.Value)...)
		case "MountPath":
			j.mountPaths = append(j.mountPaths, strLits(kv.Value)...)
		case "Env":
			j.env = append(j.env, envNames(kv.Value)...)
		}
		return true
	})
	return j
}

// envNames reads the Name of each EnvVar, and only the top-level one: a
// secret-backed variable nests a LocalObjectReference carrying its own Name.
func envNames(e ast.Expr) []string {
	list, ok := e.(*ast.CompositeLit)
	if !ok {
		return nil
	}
	names := make([]string, 0, len(list.Elts))
	for _, elt := range list.Elts {
		v, ok := elt.(*ast.CompositeLit)
		if !ok {
			continue
		}
		for _, field := range v.Elts {
			kv, ok := field.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if id, ok := kv.Key.(*ast.Ident); ok && id.Name == "Name" {
				names = append(names, strLits(kv.Value)...)
			}
		}
	}
	return names
}

// strLits returns the string constants under an expression. Constants only:
// anything computed at run time is not something this can check.
func strLits(e ast.Expr) []string {
	var out []string
	ast.Inspect(e, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		if s, err := strconv.Unquote(lit.Value); err == nil {
			out = append(out, s)
		}
		return true
	})
	return out
}

// TestSeedJobRunsTheStagingScript ties the Job spec to the fixtures: it has to
// run a script that exists, from the path the fixtures are mounted at.
func TestSeedJobRunsTheStagingScript(t *testing.T) {
	j := readSeedJob(t)
	if len(j.command) == 0 {
		t.Fatal("the seed container declares no command")
	}
	if j.command[0] != "bash" {
		t.Errorf("the seed Job runs %q; run.sh reads PIPESTATUS to report the failing "+
			"stage, and sh does not set it", j.command[0])
	}

	var script string
	for _, arg := range j.command {
		if strings.HasSuffix(arg, ".sh") {
			script = arg
		}
	}
	if script == "" {
		t.Fatal("the seed Job command names no script; nothing stages the load")
	}
	if _, err := os.Stat(filepath.Join(dir, path.Base(script))); err != nil {
		t.Errorf("the seed Job runs %s, which is not a fixture", script)
	}
	if len(j.mountPaths) == 0 {
		t.Fatal("the seed container mounts nothing, so no fixture reaches it")
	}
	var mounted bool
	for _, m := range j.mountPaths {
		if path.Dir(script) == m {
			mounted = true
		}
	}
	if !mounted {
		t.Errorf("the seed Job runs %s but mounts the fixtures at %v", script, j.mountPaths)
	}
}

// TestSeedJobSetsWhatRunShReads is the contract that breaks latest: run.sh
// runs under set -u, so a variable the Job forgets aborts the pod after the
// fixtures are already bootstrapped.
func TestSeedJobSetsWhatRunShReads(t *testing.T) {
	run := read(t, "run.sh")
	set := map[string]bool{}
	for _, name := range readSeedJob(t).env {
		set[name] = true
	}
	want := map[string]bool{}
	for _, m := range regexp.MustCompile(`\$\{?(SEED_[A-Z_]+)`).FindAllStringSubmatch(run, -1) {
		want[m[1]] = true
	}
	if len(want) == 0 {
		t.Fatal("run.sh reads no SEED_ variable; this guard would pass vacuously")
	}
	for name := range want {
		if !set[name] {
			t.Errorf("run.sh reads $%s but the seed Job never sets it", name)
		}
	}
}

// TestRunShFailsTheJobOnAStageError pins the two shell options the failure
// path rests on. Without errexit a failed load still reaches finish.sql, which
// would stamp the seed marker onto data that is not all there.
func TestRunShFailsTheJobOnAStageError(t *testing.T) {
	run := read(t, "run.sh")
	opts := regexp.MustCompile(`(?m)^set -([a-z]+)`).FindStringSubmatch(run)
	if opts == nil {
		t.Fatal("run.sh sets no shell options: a failing stage would not fail the Job")
	}
	for opt, why := range map[string]string{
		"e": "a failing stage would still reach finish.sql and stamp the seed marker",
		"u": "an unset SEED_ variable would seed at an empty scale instead of failing",
	} {
		if !strings.Contains(opts[1], opt) {
			t.Errorf("run.sh does not set -%s: %s", opt, why)
		}
	}
}

// TestEveryFixtureIsEmbedded closes the gap between the directory and the
// ConfigMap. go:embed on a directory also skips names beginning with _ or .,
// so such a fixture would be staged by run.sh and never shipped.
func TestEveryFixtureIsEmbedded(t *testing.T) {
	j := readSeedJob(t)
	if len(j.embeds) == 0 {
		t.Fatal("the suite embeds nothing, so the ConfigMap would be empty")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	embedded := path.Base(dir)
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "_") || strings.HasPrefix(name, ".") {
			t.Errorf("fixture %s starts with %q: go:embed on a directory skips it and "+
				"it would never reach the seed Job", name, name[:1])
			continue
		}
		var covered bool
		for _, p := range j.embeds {
			p = strings.TrimPrefix(p, "all:")
			if p == embedded {
				covered = true
				break
			}
			if ok, _ := path.Match(p, embedded+"/"+name); ok {
				covered = true
				break
			}
		}
		if !covered {
			t.Errorf("fixture %s matches none of the go:embed patterns %v", name, j.embeds)
		}
	}
}

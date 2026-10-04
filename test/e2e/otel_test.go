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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// One small table: the spec checks export, not copy speed.
const otelTable = "public.customers"

var _ = Describe("OpenTelemetry export", func() {
	It("pushes the Migration metrics to an OTLP collector", func() {
		const name = "e2e-otel"
		mig := newMigration(name, nsE2E, v1beta1.CloneOptions{})
		mig.Spec.Clone.Filters = &v1beta1.Filters{IncludeOnlyTables: []string{otelTable}}
		DeferCleanup(func() { deleteMigration(name) })
		create(mig)
		waitPhase(name, nsE2E, migrationTimeout, v1beta1.PhaseCompleted)

		// The window is 60s and the export interval 5s, so a phase sample is always inside it.
		Eventually(otelCollectorLogs, 2*time.Minute, 5*time.Second).Should(And(
			ContainSubstring("pgcopydb_migration_phase"),
			ContainSubstring("service.name: Str(pgcopydb-operator)"),
			ContainSubstring("Str("+name+")"),
		), "the collector never logged this Migration's phase metric")
	})
})

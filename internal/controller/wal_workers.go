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

package controller

import (
	"fmt"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
)

// validateWalWorkers repeats the walWorkers admission rules for objects stored
// under an older CRD; the error becomes a terminal InvalidSpec.
func validateWalWorkers(m *v1beta1.Migration) error {
	f := m.Spec.Follow
	if f == nil {
		return nil
	}
	if f.WalWorkers < 0 || f.WalWorkers > 16 {
		return fmt.Errorf("walWorkers must be between 1 and 16")
	}
	if f.WalWorkers > 1 {
		if !f.Enabled || (f.Plugin != "" && f.Plugin != v1beta1.PluginPGOutput) || f.Publication != "" ||
			len(f.UnloggedTables) > 0 || len(f.InsertOnlyTables) > 0 {
			return fmt.Errorf("walWorkers above 1 requires follow.enabled, plugin pgoutput, no publication, and no unloggedTables or insertOnlyTables")
		}
		if len(f.SlotName) > 51 {
			return fmt.Errorf("walWorkers above 1 requires a slotName of at most 51 characters")
		}
	}
	if len(f.HighTrafficTables) == 0 {
		return nil
	}
	if f.WalWorkers <= 1 || len(f.HighTrafficTables) > int(f.WalWorkers) {
		return fmt.Errorf("highTrafficTables requires walWorkers above 1 and at most walWorkers entries")
	}
	// The list reaches pgcopydb comma-joined, so an unchecked entry could split or merge names.
	seen := map[string]bool{}
	for _, table := range f.HighTrafficTables {
		if !exactTable.MatchString(table) || seen[table] {
			return fmt.Errorf("highTrafficTables needs unique exact lowercase schema.table names")
		}
		seen[table] = true
	}
	return nil
}

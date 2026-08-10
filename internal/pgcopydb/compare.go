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

package pgcopydb

// CompareSchemaArgs renders the argv for `pgcopydb compare schema`. The
// connection URIs travel via the environment, like every other command.
func CompareSchemaArgs() []string {
	return []string{"compare", "schema", flagDir, WorkDir}
}

// CompareDataArgs renders the argv for `pgcopydb compare data`. --json makes
// the per-table result parseable from the Job logs.
func CompareDataArgs() []string {
	return []string{"compare", "data", flagDir, WorkDir, "--json"}
}

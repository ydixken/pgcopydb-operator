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

import "fmt"

// externalSide is one database of a supplied pair, named as its URIs name it.
type externalSide struct {
	Database, AppRole, AdminRole string
}

// externalConfig is the database pair the suite runs against in external mode.
type externalConfig struct {
	Source, Target externalSide
}

// external is set once before the suite starts; nil runs the CNPG fixtures.
var external *externalConfig

// side maps a fixture cluster name to the supplied database standing in for it.
// No other cluster has a stand-in, so reaching one is a bug in the suite.
func (c *externalConfig) side(cluster string) externalSide {
	switch cluster {
	case sourceCluster:
		return c.Source
	case targetCluster:
		return c.Target
	}
	panic(fmt.Sprintf("no external database stands in for cluster %q", cluster))
}

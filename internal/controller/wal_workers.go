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
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
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

// workerNameMatch is the SQL test that col is base plus "_<kind><digits>". LIKE
// would let each _ in base match any character, and so another migration's names.
func workerNameMatch(col, base, kind string) string {
	b := sqlLiteral(base)
	return "left(" + col + ", length(" + b + ")) = " + b +
		" AND substr(" + col + ", length(" + b + ") + 1) ~ '^_" + kind + "[0-9]+$'"
}

// walWorkersSweep drops the WAL worker slots, per-table publications and target
// origins that stream cleanup cannot find when the work-dir catalog is lost.
// A slot that is still active fails the drop, as the main slot does in pgcopydb.
func walWorkersSweep(m *v1beta1.Migration) string {
	if f := m.Spec.Follow; f == nil || f.WalWorkers <= 1 {
		return ""
	}
	base := effectiveSlotName(m)
	return `psql "$PGCOPYDB_SOURCE_PGURI" -Xq -v ON_ERROR_STOP=1 <<'WAL_WORKERS_SWEEP'
DO $wal_workers_sweep$
DECLARE
  n name;
BEGIN
  FOR n IN SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE ` + workerNameMatch("slot_name", base, "w") + ` LOOP
    RAISE NOTICE 'dropping WAL worker slot %', n;
    PERFORM pg_catalog.pg_drop_replication_slot(n);
  END LOOP;
  FOR n IN SELECT pubname FROM pg_catalog.pg_publication WHERE ` + workerNameMatch("pubname", base, "t") + ` LOOP
    RAISE NOTICE 'dropping WAL worker publication %', n;
    EXECUTE format('DROP PUBLICATION %I', n);
  END LOOP;
END;
$wal_workers_sweep$;
WAL_WORKERS_SWEEP
psql "$PGCOPYDB_TARGET_PGURI" -Xq -v ON_ERROR_STOP=1 <<'WAL_WORKERS_SWEEP'
DO $wal_workers_sweep$
DECLARE
  o text;
BEGIN
  FOR o IN SELECT roname FROM pg_catalog.pg_replication_origin WHERE ` + workerNameMatch("roname", base, "w") + ` LOOP
    RAISE NOTICE 'dropping WAL worker origin %', o;
    PERFORM pg_catalog.pg_replication_origin_drop(o);
  END LOOP;
END;
$wal_workers_sweep$;
WAL_WORKERS_SWEEP
`
}

// walWorkersPublicationGuard is the retry guard when pgcopydb publishes each table
// as slot_t<oid>: a kept slot needs one, orphans go, and worker slots without
// the main slot are an inconsistency for a human to resolve.
func walWorkersPublicationGuard(slot string) string {
	publications := workerNameMatch("pubname", slot, "t")
	return `psql "$PGCOPYDB_SOURCE_PGURI" -Xq -v ON_ERROR_STOP=1 <<'PUBLICATION_RETRY'
DO ` + publicationRetryDollarTag + `
DECLARE
  p name;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = '` + slot + `') THEN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE ` + publications + `) THEN
      RAISE EXCEPTION 'publication retry refused: source slot "` + slot + `" exists but no WAL worker publication "` + slot + `_t<table oid>" exists';
    END IF;
  ELSIF EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE ` + workerNameMatch("slot_name", slot, "w") + `) THEN
    RAISE EXCEPTION 'publication retry refused: WAL worker slots "` + slot + `_w<k>" exist but source slot "` + slot + `" is missing';
  ELSE
    FOR p IN SELECT pubname FROM pg_catalog.pg_publication WHERE ` + publications + ` LOOP
      EXECUTE format('DROP PUBLICATION %I', p);
    END LOOP;
  END IF;
END;
` + publicationRetryDollarTag + `;
PUBLICATION_RETRY`
}

// leftoverSlots names, for a CleanupFailed event, every source slot a failed
// cleanup can leave behind: the quoted base slot and any WAL worker slots.
func leftoverSlots(m *v1beta1.Migration) string {
	slot := effectiveSlotName(m)
	if f := m.Spec.Follow; f != nil && f.WalWorkers > 1 {
		workers := pgcopydb.WorkerSlots(slot, f.WalWorkers)[1:]
		return fmt.Sprintf("%q (with WAL worker slots %s)", slot, strings.Join(workers, ", "))
	}
	return fmt.Sprintf("%q", slot)
}

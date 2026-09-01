#!/usr/bin/env bash
# Stages the fixture load for the e2e seed Job (see runSeedJob in the suite).
#
# The two bulk tables are 92% of the seed and are bound by different resources,
# events per row and documents per byte, so they load at the same time instead
# of one after the other. The secondary indexes they feed are built once at the
# end by finish.sql rather than maintained per insert. Issue #146 has the
# measurement both follow from.
set -euo pipefail

# Everything below is named relative to this script, so the mount path lives
# in the Job spec alone and the stages can also be run by hand from here.
cd "$(dirname "$0")"

# stage runs one SQL file and prefixes its output, because the concurrent
# stages share this pod's log and psql cannot label its own lines. The return
# is psql's status, not sed's.
stage() {
    psql -v ON_ERROR_STOP=1 -v "scale=$SEED_SCALE" -v "profile=$SEED_PROFILE" \
        -v "extra_tables=${SEED_EXTRA_TABLES:-0}" -v "extra_mb=${SEED_EXTRA_MB:-0}" \
        -f "$1.sql" 2>&1 | sed "s|^|[$1] |"
    return "${PIPESTATUS[0]}"
}

stage schema

# The marker records the profile and scale a previous run finished, and it is
# written last, so a match means every stage below already succeeded. Checked
# here rather than inside the stages: \quit in an \ir-included file ends that
# file alone, so a guard in prelude.sql would not stop the stage that read it.
seeded=$(psql -tAqX -v ON_ERROR_STOP=1 -v "profile=$SEED_PROFILE" -v "scale=$SEED_SCALE" <<'SQL'
SELECT EXISTS (SELECT 1 FROM e2e_seed WHERE profile = :'profile' AND scale = :'scale'::numeric);
SQL
)
if [ "$seeded" = t ]; then
    echo "seed: marker matches profile $SEED_PROFILE at scale $SEED_SCALE, nothing to do"
    exit 0
fi

# seed_extra joins the concurrent group only when asked for. It is bound by
# bytes like seed_documents, so it overlaps the row-bound stages usefully.
stages=(seed_small seed_events seed_documents)
if [ "${SEED_EXTRA_TABLES:-0}" -gt 0 ]; then
    stages+=(seed_extra)
fi

pids=()
for s in "${stages[@]}"; do
    stage "$s" &
    pids+=("$!")
done
# Waited on one at a time so set -e reports the stage that actually failed.
for p in "${pids[@]}"; do
    wait "$p"
done

stage finish

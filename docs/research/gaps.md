# Verification gaps (research round 1, 2026-08-07)

Empirical facts still to confirm; each becomes an M1 spike task in MILESTONES.md.

1. Whether `--host/--port` (sentinel TCP coordinator) is actually accepted on `pgcopydb clone --follow` (not just `stream replay`); flagged as unverified in the CLI report and load-bearing for the controller's no-exec control plane. Confirm via a live run of the v0.18 image or the getopt table in `src/bin/pgcopydb/cli_common.c`.

2. Whether the TCP sentinel coordinator (port 5442) has any authentication or transport security: nothing in either pgcopydb report says so, and an open port that accepts SET_ENDPOS would dictate NetworkPolicy and controller design; lives in `ld_ipc.c`/`docs/sentinel-protocol.rst` and a live probe.

3. Whether `pg_replication_origin_create/_advance/_xact_setup` on the target work for a non-superuser (operator-managed app users are typically non-superuser); explicitly open in the CLI report and it determines the CRD's target-credential requirements; needs a live test against a PostgreSQL 17 target or the GRANT docs.

4. Whether the e2e target instances run `wal_level=logical` with sufficient `max_replication_slots`/`max_wal_senders`; check on a live cluster.

5. How to provision a source role with the REPLICATION attribute (and publication-creation rights) for the e2e fixtures (CNPG `managed.roles`/`enableSuperuserAccess`); lives in CNPG documentation.

6. Which pgcopydb image supports a PG17 target: the CLI report says the v0.18 image ships PG16 client tools while pg_dump must be ≥ target major, so whether a PGVERSION=17 build or custom image is needed is unresolved; lives in the upstream Dockerfile build args or a live restore test against PG17.

7. The exact JSON schemas of `summary.json` and `pgcopydb list progress --json` output (field names/types the controller will parse into status); flagged as uncaptured in the CLI report; capture from a test run of v0.18.

8. The exit code of `pgcopydb clone --follow` after a graceful SIGTERM mid-clone (0 vs nonzero) and empirical `--resume` safety: both reports describe "clean shutdown" but never the exit status, which drives Job failure interpretation and operator retry policy; needs an empirical test.

9. Live e2e-cluster facts (Kubernetes version, node architecture, storage capacity) gate CRD/CEL feature availability, image arch choice, and e2e PVC sizing; checked out-of-band and kept in private ops notes (this repository is public).

10. Where the operator's chart and images will be hosted (GitLab group path or ghcr.io) and whether an OCI Helm source actually works with the `platform` AppProject: the cluster report notes OCI is unproven and the recipe leaves `repoURL` as a placeholder; decide the registry and test one OCI-sourced Application, or default to the proven git-repo-with-path pattern.

No blocking gaps found for the filter INI format, sentinel/cutover semantics, work-dir/resume model, ArgoCD integration recipe, or CRD design conventions; those are sufficiently covered.

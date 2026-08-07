# Spike findings (live, recorded as verified)

Live dev cluster (context `cloudcats-ber-oidc`), 2026-08-07.

## S9: dev cluster facts (done)

- Kubernetes: **k3s v1.35.1** (server v1.35.1+k3s1), 3 control-plane+etcd nodes, Ubuntu 24.04, containerd 2.1.5.
- Architecture: **amd64** on all nodes; 20 CPU / ~32Gi memory each. So the runner image needs amd64 first; arm64 is optional (M4).
- Existing CNPG clusters (all PG17, healthy): aws-visualize-pg, keycloak-pg, netbox-pg, timesheet-pg.

## S4: CNPG replication defaults (done)

On `timesheet-pg` primary (PostgreSQL **17.9**):

- `wal_level = logical` (CNPG default; no per-cluster `postgresql.parameters` needed for follow).
- `max_replication_slots = 32`, `max_wal_senders = 10`.

Implication: follow mode (M2) works against CNPG clusters out of the box. e2e source cluster needs no special postgresql tuning for CDC.

## Still open (tracked in MILESTONES M1 spikes)

- S1/S2: sentinel TCP transport on `clone --follow` (verify in runner image, M2-relevant).
- S3: non-superuser replication origin on PG17 (M2).
- S5: CNPG `managed.roles` REPLICATION role for e2e source (M2).
- S6: runner image build with postgresql-client-17 + clone against PG17 (B9).
- S7: capture live JSON samples from `list progress --json` etc. (needs runner image, B9/B7).
- S8: SIGTERM mid-clone exit code + resume (needs runner image, B9).
- S10: OCI ArgoCD Application viability (deferred, git-path used).

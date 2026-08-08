# pgcopydb output samples (S7)

Verbatim outputs captured live on 2026-08-08 from pgcopydb 0.18 (`0.18-1.pgdg12+1`, the PGDG build in `ghcr.io/ydixken/pgcopydb-operator/runner:v0.1.0-alpha.9`), running by hand in a pod against the CNPG PostgreSQL 18 fixture clusters of the e2e suite ([test/e2e](../../../test/e2e)), plus a temporary 336 MB bulk table (`public.spike_bulk`) added to widen the copy window. Files are unedited; none of the outputs contained hostnames beyond the e2e namespace's service DNS.

| File | Invocation | Note |
|------|------------|------|
| `summary.json` | none (file `<workdir>/summary.json`) | Written at clone completion. Top-level keys: `setup`, `steps`, `tables[]` (per table: `oid`, `schema`, `name`, `duration`, `network`, `index`, `indexes[]`, `constraints[]`). |
| `list-progress-summary.json` | `pgcopydb list progress --summary --json` | Byte-identical to `summary.json`: the command just reads that file. Before completion it fails (`Summary JSON file ... does not exists`, exit 12). There is no working mid-run progress command in 0.18, see next row. |
| `list-progress-failure.txt` | `pgcopydb list progress --json` | **Broken in 0.18**, with and without `--json`, mid-run and post-run: the query references a `bytes` column the catalog schema does not have. Always exit 12. Upstream: [dimitri/pgcopydb#1036](https://github.com/dimitri/pgcopydb/issues/1036). |
| `sentinel-get-streaming.txt` / `.json` | `pgcopydb stream sentinel get [--json]` | During `clone --follow` streaming, before an endpos was set. |
| `sentinel-get-endpos-set.txt` / `.json` | same | After `stream sentinel set endpos --current` returned `0/2B33F780`. |
| `sentinel-get-selectors.txt` | `pgcopydb stream sentinel get --<selector>` | All six single-value selectors; transcript with one `$` line per invocation. |

Findings the samples prove:

- **`sentinel get --json` endpos bug still present in 0.18**: the JSON `endpos` field carries the startpos. Compare `sentinel-get-endpos-set.txt` (`endpos 0/2B33F780`) with `sentinel-get-endpos-set.json` (`"endpos": "0\/2B33CDC0"`, which is the startpos); the `--endpos` selector returns the true value. Automation reads the plain-text output or the selectors, never the JSON. Background: [docs/research/pgcopydb-follow.md](../pgcopydb-follow.md).
- JSON LSNs are slash-escaped (`"0\/2B33CDC0"`); `--apply` prints `enabled`/`disabled` while the JSON field is a boolean.
- Polling `list progress` in a tight loop while a clone runs locked pgcopydb's own SQLite catalog (`[SQLite 5: database is locked]`) and failed the clone with exit 12. Progress polling against a live workdir has to stay sparse (seconds apart, as the operator's poller does).

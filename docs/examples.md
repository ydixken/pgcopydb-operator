# Examples

Every example is a complete resource that applies as-is after you swap in your hosts and Secrets.

| Example | Shows | Start here when |
|---|---|---|
| [01-clone-minimal](examples/01-clone-minimal.yaml) | The shortest correct clone | You are migrating for the first time |
| [02-clone-dsn-secret](examples/02-clone-dsn-secret.yaml) | A Secret holding one complete connection URI | Your provider hands you a DSN (RDS, Neon, ...) |
| [03-clone-platform-secret](examples/03-clone-platform-secret.yaml) | One Secret with DB/PW/URL/URL_EXTERNAL/USER keys, remappable | Your platform provisions per-database credential Secrets |
| [04-clone-tuned](examples/04-clone-tuned.yaml) | Parallelism, filters, placement, lifecycle | The default clone is too slow or copies too much |
| [05-live-migration](examples/05-live-migration.yaml) | Follow mode with a Manual cutover | Downtime must stay in seconds, not hours |
| [06-live-superuser](examples/06-live-superuser.yaml) | Preflight applies the missing grants itself | You have superuser credentials and no grants applied yet |
| [07-verified](examples/07-verified.yaml) | Post-migration `pgcopydb compare` checks | You must prove the target matches the source |
| [08-reference](examples/08-reference.yaml) | Every spec field, annotated | You are looking for a specific field |
| [09-all-databases](examples/09-all-databases.yaml) | Clone-only whole-instance copy, roles implied | Both endpoints permit superuser access and target schemas are empty |

Alert rules ship in the Helm chart, not as an example here: set `metrics.prometheusRule.enabled=true` to install them.
The [monitoring guide](operations/monitoring.md) documents them alongside the metrics and dashboards.

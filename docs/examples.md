# Examples

Every example is a complete resource that applies as-is after you swap in your hosts and Secrets.
They build on each other in order; `08-reference.yaml` is the annotated map of the whole spec.

| Example | Shows | Start here when |
|---|---|---|
| [01-clone-minimal](examples/01-clone-minimal.yaml) | The shortest correct clone | You are migrating for the first time |
| [02-clone-dsn-secret](examples/02-clone-dsn-secret.yaml) | A Secret holding one complete connection URI | Your provider hands you a DSN (RDS, Neon, ...) |
| [03-clone-platform-secret](examples/03-clone-platform-secret.yaml) | One Secret with DB/PW/URL/URL_EXTERNAL/USER keys, remappable | Your platform provisions per-database credential Secrets |
| [04-clone-tuned](examples/04-clone-tuned.yaml) | Parallelism, filters, placement, lifecycle | The default clone is too slow or copies too much |
| [05-live-migration](examples/05-live-migration.yaml) | Follow mode with a Manual cutover | Downtime must stay in seconds, not hours |
| [06-live-superuser](examples/06-live-superuser.yaml) | Preflight applies the missing grants itself | You have superuser credentials but no DBA time |
| [07-verified](examples/07-verified.yaml) | Post-migration `pgcopydb compare` checks | The result must be proven, not assumed |
| [08-reference](examples/08-reference.yaml) | Every spec knob, annotated | You are looking for a specific field |
| [prometheusrule-migrations](examples/prometheusrule-migrations.yaml) | Alerts on the operator's metrics | Migrations run unattended |

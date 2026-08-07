# Runner image for the operator's migration Jobs: pgcopydb 0.18 plus the
# PostgreSQL 17 client tools, since pg_dump/pg_restore MUST be >= the target
# major (our targets run PostgreSQL 17).
#
# Built from debian:bookworm-slim with the PGDG apt repo instead of the
# upstream dimitri/pgcopydb:v0.18 image: upstream bundles postgresql-client-16
# and a passwordless-sudo user, so basing on it would carry two client majors
# plus sudo. PGDG bookworm ships pgcopydb 0.18 and postgresql-client-17 for
# amd64 and arm64, so one apt install yields exactly what the Jobs need.

# Pinned by tag and digest; Renovate's dockerfile manager bumps both together.
# renovate: datasource=docker depName=debian
FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241

# DL3008: apt versions deliberately float within their majors (PGDG rotates
# point releases); the version canary below fails the build on a major drift.
# hadolint ignore=DL3008
RUN set -eux; \
    apt-get update; \
    # curl fetches the PGDG key; ca-certificates stays for libpq TLS (sslmode=verify-*)
    apt-get install -y --no-install-recommends ca-certificates curl; \
    # /etc/apt/keyrings is the bookworm convention for third-party repo keys
    install -d -m 0755 /etc/apt/keyrings; \
    curl -fsSL https://apt.postgresql.org/pub/repos/apt/ACCC4CF8.asc -o /etc/apt/keyrings/pgdg.asc; \
    echo "deb [signed-by=/etc/apt/keyrings/pgdg.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    # postgresql-client-17 Provides postgresql-client, which satisfies pgcopydb's
    # metapackage dependency without pulling in the current default major (18)
    apt-get install -y --no-install-recommends pgcopydb postgresql-client-17; \
    # curl was build-time only; drop it to shrink the attack surface
    apt-get purge -y --auto-remove curl; \
    rm -rf /var/lib/apt/lists/*

# Version-17 binaries ahead of Debian's pg_wrapper, so pg_dump and pg_restore
# resolve to 17.x deterministically even if another client major lands later.
ENV PATH=/usr/lib/postgresql/17/bin:$PATH

# Version canary: fail the build if PGDG drifts off pgcopydb 0.18 or client 17.
RUN set -eux; \
    pgcopydb --version; \
    pg_dump --version; \
    pgcopydb --version | grep -qF '0.18'; \
    pg_dump --version | grep -qE ' 17\.'

# Uid 65532 follows the distroless nonroot convention; a numeric uid lets
# Kubernetes verify runAsNonRoot without a user lookup in the image.
RUN useradd --uid 65532 --user-group --create-home --shell /usr/sbin/nologin runner \
    # /workdir is where Jobs mount the migration work volume; owned by the
    # runner so the image also works without a mount
    && install -d -o runner -g runner /workdir

USER 65532:65532
WORKDIR /workdir

# No baked credentials: pgcopydb reads PGCOPYDB_SOURCE_PGURI,
# PGCOPYDB_TARGET_PGURI, and PGPASSFILE from the Job's environment at runtime.
# Empty entrypoint so the Job supplies the full command (pgcopydb clone, ...);
# a bare run prints help instead of touching any database.
ENTRYPOINT []
CMD ["pgcopydb", "--help"]

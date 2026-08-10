# Runner image for the operator's migration Jobs: pgcopydb 0.18 plus the
# PostgreSQL 18 client tools: pg_dump/pg_restore MUST be >= the newest server
# major on either side, and 18 is the current release major.
#
# Built from debian:trixie-slim with the PGDG apt repo instead of the
# upstream dimitri/pgcopydb:v0.18 image: upstream bundles postgresql-client-16
# and a passwordless-sudo user, so basing on it would carry two client majors
# plus sudo. PGDG trixie ships pgcopydb 0.18 and postgresql-client-18 for
# amd64 and arm64, so one apt install yields exactly what the Jobs need.
#
# Trixie rather than bookworm because bookworm is oldstable and loses regular
# security support first. This is hygiene, not a vulnerability fix: scanned
# side by side the two bases report the same total, and trixie clears exactly
# one critical (zlib1g CVE-2023-45853).
#
# Most of what a scanner reports here is perl, which Debian ships unfixed in
# both releases. perl arrives through postgresql-client-common and is never
# executed: PATH below puts /usr/lib/postgresql/18/bin first, so pg_dump,
# pg_restore and psql resolve to the real binaries, not the pg_wrapper perl
# scripts in /usr/bin. Removing perl would need dpkg --force-depends and leave
# a broken package state, which is not worth a lower scanner count.

# Pinned by tag and digest; Renovate's dockerfile manager bumps both together.
# renovate: datasource=docker depName=debian
FROM debian:trixie-slim@sha256:3a39a0592364683e6bab97937b72cad5a8fa6dcbbee90edb3bb48c7f8e94f258

# DL3008: apt versions deliberately float within their majors (PGDG rotates
# point releases); the version canary below fails the build on a major drift.
# hadolint ignore=DL3008
RUN set -eux; \
    apt-get update; \
    # curl fetches the PGDG key; ca-certificates stays for libpq TLS (sslmode=verify-*)
    apt-get install -y --no-install-recommends ca-certificates curl; \
    # /etc/apt/keyrings is the Debian convention for third-party repo keys
    install -d -m 0755 /etc/apt/keyrings; \
    curl -fsSL https://apt.postgresql.org/pub/repos/apt/ACCC4CF8.asc -o /etc/apt/keyrings/pgdg.asc; \
    echo "deb [signed-by=/etc/apt/keyrings/pgdg.asc] https://apt.postgresql.org/pub/repos/apt trixie-pgdg main" > /etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    # Client major 18 covers every supported server major on either side:
    # pg_dump/pg_restore must be >= the newest server they talk to, and 18
    # reads servers back to 9.2.
    apt-get install -y --no-install-recommends pgcopydb postgresql-client-18; \
    # curl was build-time only; drop it to shrink the attack surface
    apt-get purge -y --auto-remove curl; \
    rm -rf /var/lib/apt/lists/*

# Version-18 binaries ahead of Debian's pg_wrapper, so pg_dump and pg_restore
# resolve to 18.x deterministically even if another client major lands later.
ENV PATH=/usr/lib/postgresql/18/bin:$PATH

# Version canary: fail the build if PGDG drifts off pgcopydb 0.18 or client 18.
RUN set -eux; \
    pgcopydb --version; \
    pg_dump --version; \
    pgcopydb --version | grep -qF '0.18'; \
    pg_dump --version | grep -qE ' 18\.'

# Uid 65532 follows the distroless nonroot convention; a numeric uid lets
# Kubernetes verify runAsNonRoot without a user lookup in the image.
RUN useradd --uid 65532 --user-group --create-home --shell /usr/sbin/nologin runner \
    # /work is where Jobs mount the migration work volume; owned by the
    # runner so the image also works without a mount
    && install -d -o runner -g runner /work

USER 65532:65532
WORKDIR /work

# No baked credentials: pgcopydb reads PGCOPYDB_SOURCE_PGURI,
# PGCOPYDB_TARGET_PGURI, and PGPASSFILE from the Job's environment at runtime.
# Empty entrypoint so the Job supplies the full command (pgcopydb clone, ...);
# a bare run prints help instead of touching any database.
ENTRYPOINT []
CMD ["pgcopydb", "--help"]

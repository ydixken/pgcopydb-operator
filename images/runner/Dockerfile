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
# The image then deletes what it does not run. Debian ships no fixed version
# for most of what a scanner finds here, so patching cannot help; removal can.
# perl arrives as a dependency of postgresql-client-common and accounted for
# every critical in the image, and util-linux, login and gzip are never
# executed by a migration. Dropping them takes the report from 241 findings
# with 16 criticals to 92 with none.
#
# Alpine and Wolfi were both tried and rejected. Alpine is musl, and pgcopydb's
# CLI needs GNU getopt_long to permute argv so that options can follow the
# subcommand; every worker Job died with "unrecognized option: dir" before
# touching a database. Wolfi is glibc and worked, but its images live behind
# Chainguard's catalog tiers, which is not a dependency this project should
# take. Staying on Debian keeps glibc, a free base, and an intact package
# database, so what a scanner reports is the truth rather than an artifact of
# having nothing left to enumerate.

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
    # Everything below runs in this layer on purpose: files deleted in a later
    # layer still occupy the earlier one, so the image would not shrink.
    #
    # perl is matched rather than named because the package names carry the
    # version (perl-modules-5.40, libperl5.40) and would silently stop matching
    # on the next perl major. postgresql-client-common is what pulls it in; its
    # only contribution is the pg_wrapper perl scripts in /usr/bin, which the
    # PATH below already bypasses.
    perl_pkgs="$(dpkg-query -Wf '${Package}\n' \
      | grep -E '^(perl|perl-base|libperl[0-9.]+|perl-modules-[0-9.]+)$')"; \
    # libacl1 is NOT in this list: GNU sed links it, and the prelude escapes
    # passwords with sed. ncurses-base and libtinfo6 stay for psql's readline.
    # login needs --force-remove-protected: modern dpkg protects it beyond
    # Essential, and nothing in a migration Job logs anyone in.
    # shellcheck disable=SC2086
    dpkg --purge --force-depends --force-remove-essential --force-remove-protected \
      $perl_pkgs postgresql-client-common \
      bsdutils gzip libblkid1 liblastlog2-2 libmount1 libsmartcols1 \
      libuuid1 login mount ncurses-bin util-linux; \
    rm -rf /var/lib/apt/lists/*

# Version-18 binaries ahead of Debian's pg_wrapper, so pg_dump and pg_restore
# resolve to 18.x deterministically even if another client major lands later.
# With postgresql-client-common purged the wrappers are gone entirely, which
# makes this the only path to the client tools.
ENV PATH=/usr/lib/postgresql/18/bin:$PATH

# Canary: every assertion here stands for a way the image has actually broken.
# Version drift (PGDG moving off 0.18 or client 18); a purge that took GNU sed
# with it (removing libacl1 did exactly that, and the prelude escapes passwords
# with sed); a libc whose getopt does not permute, which is how Alpine failed
# with "unrecognized option: dir" only once Jobs ran on a cluster; and perl
# surviving the purge, which would mean the report never improved.
RUN set -eux; \
    pgcopydb --version | grep -qF '0.18'; \
    pg_dump --version | grep -qE ' 18\.'; \
    pg_restore --version >/dev/null; \
    psql --version >/dev/null; \
    sed --version | grep -qF 'GNU sed'; \
    printf 'a:b' | sed -e 's/:/\\:/g' | grep -qF 'a\:b'; \
    pgcopydb clone --dir /tmp/canary --not-consistent 2>&1 \
      | grep -qF 'Options --source and --target are mandatory'; \
    ! command -v perl

# Uid 65532 follows the distroless nonroot convention; a numeric uid lets
# Kubernetes verify runAsNonRoot without a user lookup in the image. useradd
# came from the purged login package, so the entries are written directly.
RUN set -eux; \
    echo 'runner:x:65532:65532::/home/runner:/usr/sbin/nologin' >> /etc/passwd; \
    echo 'runner:x:65532:' >> /etc/group; \
    # /work is where Jobs mount the migration work volume; owned by the
    # runner so the image also works without a mount
    install -d -o 65532 -g 65532 /work /home/runner

USER 65532:65532
WORKDIR /work

# No baked credentials: pgcopydb reads PGCOPYDB_SOURCE_PGURI,
# PGCOPYDB_TARGET_PGURI, and PGPASSFILE from the Job's environment at runtime.
# Empty entrypoint so the Job supplies the full command (pgcopydb clone, ...);
# a bare run prints help instead of touching any database.
ENTRYPOINT []
CMD ["pgcopydb", "--help"]

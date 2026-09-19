# Runner image for the operator's migration Jobs: the forked pgcopydb 0.18
# plus the PostgreSQL 18 client tools, on debian:trixie-slim with the PGDG apt
# repo. pg_dump/pg_restore MUST be >= the newest server major on either side.
# The binary comes from the pgcopydb-builder image rather than from PGDG, and
# the install purges what a migration never runs. See images/runner/README.md
# for the pinned patch list, the removals, and why not Alpine or Wolfi.

# Pinned by tag and digest, not just tag: a debian bump inside the
# builder republishes this same tag, so only the digest stops that
# rebuild from reaching this image before Renovate can raise a PR.
# No --platform here. It is right for the manager's Go builder and wrong here:
# it would copy an amd64 ELF into the arm64 image, and the canary cannot catch
# that because binfmt runs the amd64 binary natively on the build host.
# renovate: datasource=docker depName=ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder
FROM ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:ea2dc96a47c2f7676d71a4967d044a1e469e4110@sha256:1145d382fc74bb35c1b8a19a42ed9469639b66d405b560ad9777a7111c9d3b33 AS pgcopydb

# Pinned by tag and digest; Renovate's dockerfile manager bumps this line.
# renovate: datasource=docker depName=debian
FROM debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132

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
    # reads servers back to 9.2. libgc1 is the one library the fork-built
    # pgcopydb links beyond postgresql-client-18's libpq5.
    apt-get install -y --no-install-recommends postgresql-client-18 libgc1; \
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

# The fork-built binary lands next to the version-18 client tools, so the
# PATH below covers it too.
COPY --from=pgcopydb /pgcopydb /usr/lib/postgresql/18/bin/pgcopydb

# Version-18 binaries ahead of Debian's pg_wrapper, so pg_dump and pg_restore
# resolve to 18.x deterministically even if another client major lands later.
# With postgresql-client-common purged the wrappers are gone entirely, which
# makes this the only path to the client tools.
ENV PATH=/usr/lib/postgresql/18/bin:$PATH

# Purging packages can break the remaining tools through shared libraries.
# Run the worker, credential escaping, and sampler timeout after purging so
# a missing runtime capability fails the image build.
RUN set -eux; \
    pgcopydb --version | grep -qF '0.18.15.gea2dc96'; \
    pg_dump --version | grep -qE ' 18\.'; \
    pg_restore --version >/dev/null; \
    psql --version >/dev/null; \
    timeout --version | grep -qF 'GNU coreutils'; \
    timeout --signal=TERM --kill-after=1s 6s true; \
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

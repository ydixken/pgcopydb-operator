# Compiles pgcopydb from the pinned fork commit and ships nothing but the
# binary. Its only input, PGCOPYDB_SHA, has changed once across 55
# releases, unlike the runner image's apt layers, which float on purpose
# and rebuild every release; a registry cache cannot tell the two apart.
# A debian digest bump here still rebuilds and republishes this tag, so
# it tracks the pinned pgcopydb commit but not the base it compiled against.

# Pinned by tag and digest; Renovate's dockerfile manager bumps this line.
# renovate: datasource=docker depName=debian
FROM debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132 AS build

# These two move together: the version is what `git describe` prints for the
# sha (v0.18-5-ge37d2bd, dashes become dots). GIT-VERSION-GEN reads VERSION
# from the environment because a tarball has no .git to describe.
ARG PGCOPYDB_SHA=e37d2bd4dd10b7ed7b415555ce3318202d9633cf
ARG PGCOPYDB_VERSION=0.18.5.ge37d2bd

# DL3008: apt versions deliberately float within their majors; the version
# canary in the final stage fails the build on a major drift.
# hadolint ignore=DL3008
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl; \
    install -d -m 0755 /etc/apt/keyrings; \
    curl -fsSL https://apt.postgresql.org/pub/repos/apt/ACCC4CF8.asc -o /etc/apt/keyrings/pgdg.asc; \
    echo "deb [signed-by=/etc/apt/keyrings/pgdg.asc] https://apt.postgresql.org/pub/repos/apt trixie-pgdg main" > /etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    # debian/control Build-Depends minus packaging and docs; SQLite is
    # vendored in-tree. server-dev-18 pins which pg_config the build reads.
    # numa and uring are PG18 additions that pg_config --libs puts on the
    # link line; control predates them.
    apt-get install -y --no-install-recommends \
      build-essential postgresql-server-dev-18 libpq-dev \
      libedit-dev libgc-dev libkrb5-dev liblz4-dev libncurses-dev \
      libnuma-dev libpam0g-dev libreadline-dev libselinux1-dev libssl-dev \
      liburing-dev libxslt1-dev libzstd-dev zlib1g-dev; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN set -eux; \
    curl -fsSL -o pgcopydb.tar.gz "https://codeload.github.com/ydixken/pgcopydb/tar.gz/${PGCOPYDB_SHA}"; \
    tar -xzf pgcopydb.tar.gz --strip-components=1; \
    # install lands the binary in pg_config --bindir, the same path the
    # final stage's PATH prefers. -j$(nproc) is what upstream's own Dockerfile
    # runs against this tree; the vendored 9 MB sqlite3.c is one translation
    # unit, so it stays the floor no matter how many cores are free.
    VERSION="${PGCOPYDB_VERSION}" make -s -j"$(nproc)" install PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config; \
    # ldd documents what the final stage has to install (libgc1; the rest
    # rides in with libpq5)
    ldd /usr/lib/postgresql/18/bin/pgcopydb; \
    /usr/lib/postgresql/18/bin/pgcopydb --version | grep -qF "${PGCOPYDB_VERSION}"

# scratch, not the build environment: every release pulls this once per
# platform onto an ephemeral runner with no warm snapshot store, so a few MB
# beats several hundred. The checks above run in the stage that can run them.
FROM scratch
LABEL org.opencontainers.image.source=https://github.com/ydixken/pgcopydb-operator
COPY --from=build /usr/lib/postgresql/18/bin/pgcopydb /pgcopydb

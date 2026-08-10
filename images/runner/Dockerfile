# Runner image for the operator's migration Jobs: pgcopydb 0.18 plus the
# PostgreSQL 18 client tools: pg_dump/pg_restore MUST be >= the newest server
# major on either side, and 18 is the current release major.
#
# Built on Alpine, with pgcopydb compiled from source, because Debian has no
# answer for what a scanner finds here. On bookworm the runner reported 241
# vulnerabilities, 16 critical, and not one of them had a fixed version
# available; trixie changed the total by zero and cleared a single critical.
# The bulk was perl, pulled in by postgresql-client-common and never executed.
# The same image on Alpine reports zero and is a fifth of the size, with the
# apk database intact so a scanner still enumerates every package.
#
# The cost is that we build pgcopydb ourselves instead of installing PGDG's
# package, and that musl is not glibc. `task e2e` is the gate for the second
# part: it runs clone, follow, cutover and verification against real servers,
# which is where a musl difference would surface.

# Pinned by tag and digest; Renovate's dockerfile manager bumps both together.
# renovate: datasource=docker depName=alpine
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b AS build

# renovate: datasource=github-tags depName=dimitri/pgcopydb
ARG PGCOPYDB_VERSION=v0.18

# postgresql18-dev supplies pg_config and the server headers; the rest are
# pgcopydb's link-time dependencies. libxml2-dev is easy to miss: it is not
# used directly, it comes in through libpq's link line.
# hadolint ignore=DL3018
RUN apk add --no-cache \
      bison build-base flex git \
      gc-dev icu-dev krb5-dev libxml2-dev lz4-dev ncurses-dev openldap-dev \
      openssl-dev postgresql18-dev readline-dev sqlite-dev xxhash-dev \
      zlib-dev zstd-dev

RUN git clone --depth 1 --branch "$PGCOPYDB_VERSION" \
      https://github.com/dimitri/pgcopydb.git /src
WORKDIR /src
# -j4 rather than -j$(nproc) on purpose. The release builds arm64 under QEMU,
# where each cc1 costs far more memory than it does natively, and one core's
# worth of parallelism per core is enough to OOM the builder: -j$(nproc) on a
# 9-core machine dies with "cc: fatal error: Killed signal terminated program
# cc1". Four keeps a project this size quick without betting on the runner's
# memory. Raise it only with a measurement.
RUN make -j4 USE_PGXS=1 && make USE_PGXS=1 install

FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

# GNU sed rather than Alpine's busybox one: conn.PreludeScript escapes
# passwords into .pgpass with sed, and a mis-escaped password fails
# authentication in a way that is thankless to debug. ca-certificates stays
# for libpq TLS (sslmode=verify-*).
# DL3018: apk versions deliberately float within the Alpine release; the
# version canary below fails the build on a pgcopydb or client major drift.
# hadolint ignore=DL3018
RUN apk add --no-cache \
      ca-certificates gc icu-libs krb5-libs libldap libpq libxml2 libxxhash \
      lz4-libs ncurses-libs postgresql18-client readline sed zstd-libs

COPY --from=build /usr/libexec/postgresql18/pgcopydb /usr/bin/pgcopydb

# Version canary: fail the build if pgcopydb or the client drifts off 18/0.18.
RUN set -eux; \
    pgcopydb --version; \
    pg_dump --version; \
    pgcopydb --version | grep -qF '0.18'; \
    pg_dump --version | grep -qE ' 18\.'; \
    # GNU sed, not busybox: the prelude depends on it.
    sed --version | grep -qF 'GNU sed'

# Uid 65532 follows the distroless nonroot convention; a numeric uid lets
# Kubernetes verify runAsNonRoot without a user lookup in the image.
RUN adduser -D -u 65532 -s /sbin/nologin runner \
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

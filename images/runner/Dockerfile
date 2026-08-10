# Runner image for the operator's migration Jobs: pgcopydb 0.18 plus the
# PostgreSQL 18 client tools: pg_dump/pg_restore MUST be >= the newest server
# major on either side, and 18 is the current release major.
#
# Built on Wolfi, with pgcopydb compiled from source, because Debian has no
# answer for what a scanner finds here. On bookworm the runner reported 241
# vulnerabilities, 16 critical, and not one of them had a fixed version
# available; trixie changed the total by zero and cleared a single critical.
# The bulk was perl, pulled in by postgresql-client-common and never executed.
#
# Wolfi rather than Alpine, and this is the whole reason: pgcopydb's CLI relies
# on GNU getopt_long permuting argv, so that `pgcopydb clone --dir ...` parses
# options that follow the subcommand. musl's getopt does not permute, and on
# Alpine every worker died with "pgcopydb: unrecognized option: dir" before it
# touched a database. Wolfi is glibc, so argument handling is identical to the
# Debian image it replaces, while still being apk-based with a near-zero CVE
# baseline. Alpine was tried, e2e caught it, see git history.
#
# The apk database survives into the final image, so a scanner still
# enumerates every package. That is deliberate: copying binaries onto scratch
# would also report near zero, but only by leaving openssl, krb5 and readline
# present and uncounted.

# Pinned by digest; Chainguard rebuilds the floating tag constantly, so the
# digest is the real pin and Renovate bumps it.
# renovate: datasource=docker depName=cgr.dev/chainguard/wolfi-base
FROM cgr.dev/chainguard/wolfi-base@sha256:30f03343947c7ae3581fda727a6e2aa7b8ce7009b7bfc2ab8d5c9483ace5812f AS build

# renovate: datasource=github-tags depName=dimitri/pgcopydb
ARG PGCOPYDB_VERSION=v0.18

# postgresql-18-dev supplies pg_config and the server headers; the rest are
# pgcopydb's link-time dependencies. libxml2-dev is easy to miss: it is not
# used directly, it arrives through libpq's link line, as do libxslt and PAM.
RUN apk add --no-cache \
      bison build-base flex git \
      gc-dev icu-dev krb5-dev libxml2-dev libxslt-dev linux-pam-dev lz4-dev \
      ncurses-dev openldap-dev openssl-dev postgresql-18-dev readline-dev \
      sqlite-dev xxhash-dev zlib-dev zstd-dev

RUN git clone --depth 1 --branch "$PGCOPYDB_VERSION" \
      https://github.com/dimitri/pgcopydb.git /src
WORKDIR /src
# -j4 rather than -j$(nproc) on purpose. The release builds arm64 under QEMU,
# where each cc1 costs far more memory than it does natively, and one job per
# core is enough to OOM the builder: -j$(nproc) on a 9-core machine dies with
# "cc: fatal error: Killed signal terminated program cc1". Raise it only with
# a measurement.
RUN make -j4 USE_PGXS=1

FROM cgr.dev/chainguard/wolfi-base@sha256:30f03343947c7ae3581fda727a6e2aa7b8ce7009b7bfc2ab8d5c9483ace5812f

# GNU sed rather than the busybox applet: conn.PreludeScript escapes passwords
# into .pgpass with sed, and a mis-escaped password fails authentication in a
# way that is thankless to debug. ca-certificates stays for libpq TLS
# (sslmode=verify-*).
RUN apk add --no-cache \
      ca-certificates gc icu krb5 libpq-18 libxml2 libxslt linux-pam lz4 \
      ncurses openldap postgresql-18-client readline sed xxhash zstd

# Copied from the build tree rather than `make install`, so the path does not
# depend on what pg_config reports as its bindir.
COPY --from=build /src/src/bin/pgcopydb/pgcopydb /usr/bin/pgcopydb

# Version canary: fail the build if pgcopydb or the client drifts off 0.18/18,
# if sed is not GNU, or if option parsing stops permuting. That last check is
# the one that would have caught Alpine: it asserts that an option written
# after the subcommand is understood, so the failure mode is a red build
# rather than every migration Job dying on the cluster.
RUN set -eux; \
    pgcopydb --version; \
    pg_dump --version; \
    pgcopydb --version | grep -qF '0.18'; \
    pg_dump --version | grep -qE ' 18\.'; \
    sed --version | grep -qF 'GNU sed'; \
    pgcopydb clone --dir /tmp/canary --not-consistent 2>&1 \
      | grep -qF 'Options --source and --target are mandatory'

# Uid 65532 follows the distroless nonroot convention; a numeric uid lets
# Kubernetes verify runAsNonRoot without a user lookup in the image. Wolfi
# already ships that uid as `nonroot`, so there is no user to add here, only
# /work to create: it is where Jobs mount the migration work volume, owned by
# the running uid so the image also works without a mount.
RUN install -d -o 65532 -g 65532 /work

USER 65532:65532
WORKDIR /work

# No baked credentials: pgcopydb reads PGCOPYDB_SOURCE_PGURI,
# PGCOPYDB_TARGET_PGURI, and PGPASSFILE from the Job's environment at runtime.
# Empty entrypoint so the Job supplies the full command (pgcopydb clone, ...);
# a bare run prints help instead of touching any database.
ENTRYPOINT []
CMD ["pgcopydb", "--help"]

# pgcopydb builder image

Compiles [pgcopydb](https://github.com/dimitri/pgcopydb) from the pinned fork commit and publishes nothing but the binary, on `scratch`.
The [runner image](../runner/README.md) `COPY --from`s the result instead of recompiling pgcopydb on every release.

## Why a separate image

The compile takes about 20 minutes under QEMU for the arm64 half, and its only real input is `PGCOPYDB_SHA`.
Across 55 releases, that pin has changed once.
The runner image's apt layers float on purpose and must keep rebuilding every release, so a registry build cache is the wrong tool: it cannot tell the two halves apart and would freeze both.
Publishing the compile as its own image, tagged by the commit it builds, reuses the half that almost never changes and rebuilds only the half that must.

## What it pins

`PGCOPYDB_SHA` and `PGCOPYDB_VERSION` sit at the top of the Dockerfile as `ARG` defaults and move together.
`PGCOPYDB_VERSION` is what `git describe` prints for that commit, with dashes turned to dots.
The [runner Dockerfile](../runner/Dockerfile) pulls this image pinned by tag and digest: `ghcr.io/ydixken/pgcopydb-operator/pgcopydb-builder:<PGCOPYDB_SHA>@sha256:<digest>`, not by an interpolated ARG, so a debian bump here cannot reach it through a republished tag.
A test in `test/buildconfig` fails the build if the runner's pin disagrees with either value here.

## Building it locally

```sh
podman build -t pgcopydb-builder images/pgcopydb-builder
```

The result is a `scratch` image holding a single file, `/pgcopydb`.
It has no shell and cannot run its own checks: the `ldd` and `--version` checks that would otherwise catch a broken build run in the build stage, before the binary is copied out.

> [!caution]
> The published tag MUST cover both `linux/amd64` and `linux/arm64`.
> A single-arch tag published as a bare manifest does not fail the runner build.
> BuildKit only warns (`InvalidBaseImagePlatform`) and proceeds, and binfmt runs the wrong-arch binary natively on the build host, so the canary in the runner Dockerfile passes too.
> The break only surfaces later, as `exec format error` on a real node of the missing architecture.

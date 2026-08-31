# Build the manager binary
# Pinned by tag AND digest; Renovate bumps both together.
# --platform=$BUILDPLATFORM keeps the toolchain on the build host's own
# architecture. Without it buildx resolves the builder stage to the *target*
# platform, so the arm64 half runs the Go compiler under QEMU: measured 544s
# against 55s native for the same build.
FROM --platform=$BUILDPLATFORM golang:1.27.0@sha256:4013ae0f9e7994f8535c58c811f8f863fbed38b72e0d51e6592156f758d66146 AS builder
ARG TARGETOS
ARG TARGETARCH
# VERSION lands in pgcopydb_operator_build_info; CI passes the release tag.
ARG VERSION=dev

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the Go source (relies on .dockerignore to filter)
COPY . .

# Build
# buildx sets TARGETARCH per requested platform, so one native toolchain
# cross-compiles every one of them. CGO_ENABLED=0 is what makes that safe:
# a pure-Go build needs no cross C toolchain.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -ldflags "-X main.version=${VERSION}" -o manager cmd/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
# distroless nonroot: the tag floats, so the digest is the real pin.
FROM gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]

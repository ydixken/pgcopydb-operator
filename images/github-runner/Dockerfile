# The runner image both scale sets boot for this repository's workflows.
#
# The stock actions-runner image carries git, curl, jq, python3 and a Docker
# client, where a GitHub-hosted runner ships hundreds of tools. Every job made
# up the difference at runtime: apt for make, gh and psql, a setup- action for
# Go, Helm and kubectl. That is about 20 seconds a job against the 60 a warm
# lint run costs, and it puts a network fetch in front of every build.
#
# Every version here is pinned so the image is reproducible and Renovate can
# see it. Merging one of its bumps rebuilds this image, because the workflow
# watches this file. Go and promtool are the exception: they are read out of
# go.mod and hack/ensure-promtool.sh at build time, so this file cannot drift
# from the versions the Makefile and the test suite actually use.
FROM ghcr.io/actions/actions-runner:2.337.0

USER root

# renovate: datasource=github-releases depName=helm/helm
ARG HELM_VERSION=v4.2.4
# renovate: datasource=github-releases depName=kubernetes/kubernetes
ARG KUBECTL_VERSION=v1.37.0
# renovate: datasource=github-releases depName=oras-project/oras
ARG ORAS_VERSION=v1.3.4
# renovate: datasource=pypi depName=yamllint
ARG YAMLLINT_VERSION=1.38.0
# renovate: datasource=github-releases depName=rhysd/actionlint
ARG ACTIONLINT_VERSION=v1.7.7

# gh and postgresql-client are what the release and test jobs shell out to, and
# make drives the Makefile. gcc and libc6-dev are here because cgo is on by
# default and a package that needs it fails confusingly without them;
# build-essential would pull about 200MB for the one compiler we want.
#
# No buildah, though this image is what builds the next one. That build goes
# through the pod's dind sidecar instead, because the runner container runs
# under cri-containerd.apparmor.d and that profile denies mount as an LSM rule,
# which layer extraction needs and no capability overrides.
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
        make gcc libc6-dev gh postgresql-client python3-pip pipx \
    && rm -rf /var/lib/apt/lists/*

# System-wide rather than through pipx: pipx installs into ~/.local/bin, which
# is not on PATH in this image, and finding that out cost a red lint job.
# mkdocs-material carries the same pin as ci.yml and docs.yml, and Renovate
# bumps all three together, because a docs build that disagrees with the
# staleness gate fails on a rendering diff rather than on anything real.
RUN pip install --no-cache-dir --break-system-packages \
        "yamllint==${YAMLLINT_VERSION}" \
        mkdocs-material==9.7.7

WORKDIR /tmp/build
# The two versions this file deliberately does not own. Copying the files in
# keeps the Dockerfile from becoming a second place to bump either.
COPY go.mod hack/ensure-promtool.sh ./

RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    go_version="$(sed -n 's/^go \([0-9.]*\)$/\1/p' go.mod)"; \
    [ -n "$go_version" ] || { echo "go.mod has no go directive" >&2; exit 1; }; \
    curl -fsSL "https://go.dev/dl/go${go_version}.linux-${arch}.tar.gz" | tar -C /usr/local -xz; \
    prom_version="$(sed -n 's/^version=//p' ensure-promtool.sh | head -1)"; \
    [ -n "$prom_version" ] || { echo "ensure-promtool.sh has no version=" >&2; exit 1; }; \
    curl -fsSL "https://github.com/prometheus/prometheus/releases/download/v${prom_version}/prometheus-${prom_version}.linux-${arch}.tar.gz" \
      | tar -xz --strip-components=1 -C /usr/local/bin "prometheus-${prom_version}.linux-${arch}/promtool"; \
    curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-${arch}.tar.gz" \
      | tar -xz --strip-components=1 -C /usr/local/bin "linux-${arch}/helm"; \
    curl -fsSLo /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${arch}/kubectl"; \
    chmod +x /usr/local/bin/kubectl; \
    curl -fsSL "https://github.com/oras-project/oras/releases/download/${ORAS_VERSION}/oras_${ORAS_VERSION#v}_linux_${arch}.tar.gz" \
      | tar -xz -C /usr/local/bin oras; \
    curl -fsSL "https://github.com/rhysd/actionlint/releases/download/${ACTIONLINT_VERSION}/actionlint_${ACTIONLINT_VERSION#v}_linux_${arch}.tar.gz" \
      | tar -xz -C /usr/local/bin actionlint

WORKDIR /home/runner
RUN rm -rf /tmp/build

# Go on PATH for every step, not just login shells.
#
# GOTOOLCHAIN is left at its default, auto, on purpose. Pinning it to local
# made the go command fail on a version mismatch rather than fetch what go.mod
# asks for, and that failure has nowhere to go: the image rebuilds only once a
# go.mod bump is on main, so the pull request making the bump would be red with
# no way to merge through it, and a push to main races its own rebuild. auto
# means the toolchain arrives on its own in that window, into the module cache
# on the node, and no job needs actions/setup-go to cover the gap.
ENV PATH="/usr/local/go/bin:${PATH}"


# Everything `make test` and `make lint` would otherwise fetch or link on every
# job. golangci-lint is the expensive one: .custom-gcl.yml makes the Makefile
# link a custom binary with plugins, which took two minutes of every lint job.
# controller-gen, setup-envtest and the envtest control-plane binaries were
# downloaded per job for the same reason, that LOCALBIN defaults to ./bin
# inside a workspace an ephemeral runner creates fresh every time.
#
# LOCALBIN is baked to a path outside the workspace and the whole tree is
# handed to the runner user, so the Makefile's rules find their targets already
# present and skip. Nothing writes here during a job, which matters: the rules
# take the directory as their prerequisite, so a write would make make consider
# every tool in it stale.
# go build ./... leaves the whole dependency tree in the build cache. This
# repository's own packages go stale with the next commit, which is what the
# node cache under /cache is for; their dependencies do not.
ENV LOCALBIN=/opt/toolchain/bin \
    GOMODCACHE=/home/runner/go/pkg/mod \
    GOCACHE=/home/runner/.cache/go-build

# The module download is its own layer, keyed on go.mod and go.sum alone, so a
# change anywhere else in the tree does not make the image re-fetch every
# dependency. That ordering is worth real time: the download is the largest
# single step in this build.
COPY go.mod go.sum /tmp/src/
RUN cd /tmp/src && go mod download

COPY . /tmp/src
RUN set -eux; \
    cd /tmp/src; \
    mkdir -p "$LOCALBIN"; \
    make controller-gen setup-envtest golangci-lint kustomize; \
    crd_ref="$(sed -n 's|.*go install \(github.com/elastic/crd-ref-docs@v[0-9.]*\).*|\1|p' Taskfile.yml | head -1)"; \
    [ -n "$crd_ref" ] || { echo "Taskfile.yml has no crd-ref-docs pin" >&2; exit 1; }; \
    GOBIN="$LOCALBIN" go install "$crd_ref"; \
    go clean -cache; \
    cd /; rm -rf /tmp/src /home/runner/.cache/go-build; \
    chown -R runner:runner "$LOCALBIN" /home/runner/go /home/runner/.cache

# The module cache is baked and stays writable, so a go.sum that has moved
# since the last rebuild fetches only the difference. Modules are worth carrying
# because they come off the network.
#
# The build cache is not, and `go clean -cache` above drops what building the
# tools left behind. Compiling the dependency tree here cost 970MB of image, and
# the jobs only read it on a node whose /cache is still empty: every node in the
# fleet is warm, so it was pulled weekly and never read. Recomputing it costs
# about 100 seconds, once, the first time a fresh node runs a job.

USER runner

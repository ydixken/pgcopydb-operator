# Recon report: `/Users/ydixken/development/cloudcats/app-of-apps` (dev cluster GitOps repo)

Remote: `git@gitlab.com:cloudcats/app-of-apps.git` (referenced everywhere as `https://gitlab.com/cloudcats/app-of-apps.git`, branch `main`).

## 1. Overall structure: ArgoCD app-of-apps via a single Helm chart

- Yes, classic app-of-apps. The whole repo is one Helm chart: `platform/Chart.yaml` (chart name `app-of-apps`, "Bootstrapping all Kubernetes resources for the cluster").
- The **root Application is NOT in this repo**. `platform/values.yaml` line 2 says values are "Passed via Ansible"; the root app (named `app-of-apps`, per `tasks/lessons.md` "Root-app sync-wave health deadlock") is created by the provisioning/Ansible repo. This repo only contains what it renders.
- Child apps are **plain ArgoCD `Application` CRs rendered as Helm templates**, one directory per app: `platform/templates/<app>/application-<app>.yaml`. No ApplicationSet, no Helm chart dependencies.
- Every app is feature-gated with `{{ if .Values.<camelCaseApp>.enabled }}` and gets its chart values from `.Values.<app>.values` passed through `tpl (toYaml ...)` into `spec.source.helm.valuesObject`.
- `AppProject`: `platform/templates/platform-appproject.yaml`: project `platform`, with an explicit `sourceRepos` whitelist (all Helm repos and git repos used). **A new chart repo MUST be added to this list** or the Application will be rejected. (Note: `n8n`'s Application uses `project: default`; everything else uses `platform`.)
- Sync policy conventions (uniform): `automated: {prune: true, selfHeal: true}`, `syncOptions: [CreateNamespace=true]` (operators add `ServerSideApply=true`), finalizer `resources-finalizer.argocd.argoproj.io/background`, and app-level `retry: {limit: 20, backoff: {duration: 1m, factor: 2, maxDuration: 15m}}` on most apps.
- **Sync-wave convention** (annotations `argocd.argoproj.io/sync-wave`): operators/infra `-3` (e.g. cloudnative-pg, infisical global-config), CRD/custom-resource apps and Namespaces `-2`, InfisicalSecrets and CNPG `Cluster` CRs `-1`, app Application `0`, HTTPRoute `1`, PrometheusRule `2`. CNPG Clusters also carry `argocd.argoproj.io/sync-options: SkipDryRunOnMissingResource=true` (CRD may not exist yet).
- **Critical caveat** from `tasks/lessons.md` (2026-07-22): the root app has no retry/timeout, so any health-gated resource in `platform/templates/` (CNPG Clusters at wave -1 especially) that can't become healthy wedges ALL later root-app syncs cluster-wide. Ensure secrets/dependencies exist before pushing, or use `argocd app terminate-op app-of-apps --core`. Never patch `.operation` via kubectl.

## 2. PostgreSQL operator: CloudNativePG

- `platform/templates/cloudnative-pg/application-cloudnative-pg.yaml`: chart `cloudnative-pg` **version 0.28.2** from `https://cloudnative-pg.github.io/charts`, namespace `cnpg-system`, sync-wave `-3`, ServerSideApply. `platform/values.yaml` enables `monitoring.podMonitorEnabled: true` (PodMonitor `cnpg-system/cloudnative-pg`).
- Existing CNPG `Cluster` CRs (all `imageName: ghcr.io/cloudnative-pg/postgresql:17`, `storage: {size: 4Gi, storageClass: longhorn}`, `primaryUpdateStrategy: unsupervised`, resources `requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 512Mi}`, preferred pod anti-affinity on `kubernetes.io/hostname`):
  - `platform/templates/keycloak/cluster-keycloak.yaml`: `keycloak-pg`, ns `keycloak`, 3 instances, initdb secret `keycloak-pg-credentials`.
  - `platform/templates/timesheet/cluster-timesheet.yaml`: `timesheet-pg`, ns `timesheet`, 3 instances, secret `timesheet-pg-credentials`.
  - `platform/templates/netbox/cluster-netbox.yaml`: `netbox-pg`, ns `netbox`, 2 instances, secret `netbox-pg-credentials`.
  - `platform/templates/aws-visualize/cluster-aws-visualize.yaml`: `aws-visualize-pg`, ns `aws-visualize`, 3 instances, **no initdb secret**: CNPG auto-generates `aws-visualize-pg-app` (keys `uri`, `username`, `password`, ...) consumed by the app.
- Apps read/write hosts follow CNPG naming: `<cluster>-rw` services (`keycloak-pg-rw`, `netbox-pg-rw`, `timesheet-pg-rw`).
- Also present (not CNPG): Bitnami postgresql subcharts inside nextcloud and n8n charts, and a hand-rolled postgres StatefulSet in `platform/applications/auth/templates/postgres-statefulset.yaml`. These are legacy/bundled; the convention for new Postgres is CNPG.
- CNPG alerts: `platform/templates/cloudnative-pg/prometheusrule-cnpg.yaml` (operator-down, instance-down, replication lag, backup staleness).

## 3. Secrets: Infisical operator (external secrets store)

- Operator: `platform/templates/infisical-operator/application-infisical-operator.yaml` (chart from `https://dl.cloudsmith.io/public/infisical/helm-charts/helm/charts`), global config `platform/templates/infisical-operator/global-config.yaml`, a ConfigMap `infisical-config` in `infisical-operator-system` with `hostAPI: https://secrets.cloudcats.io/api` (the `/api` suffix is load-bearing; see lessons.md).
- Convention (concrete example `platform/templates/timesheet/infisicalsecret-timesheet.yaml`): per app, an `InfisicalSecret` CR at sync-wave `-1`, guarded by `{{ ... .Capabilities.APIVersions.Has "secrets.infisical.com/v1alpha1" }}`:
  - `projectSlug: infrastructure-secrets`, `envSlug: {{ .Values.environmentType }}` (dev), `secretsPath: /<app-name>` (e.g. `/timesheet-postgresql`, `/timesheet-registry`).
  - `credentialsRef: {secretName: infisical-universal-auth-credentials, secretNamespace: infisical-operator-system}`, `resyncInterval: 300`.
  - Templated managed secrets, e.g. CNPG bootstrap credentials rendered as `secretType: kubernetes.io/basic-auth` with `username`/`password`, and app `DATABASE_URL` composed from the Infisical `password` value.
  - Image pull secrets: `<app>-registry` secrets of type `kubernetes.io/dockerconfigjson` synced from Infisical (GitLab registry `registry.gitlab.com`).
- Human step: secrets must be created in Infisical (env `dev`) before ArgoCD sync, otherwise the root-app sync deadlocks (see 2026-07-22 lesson). CNPG's auto-generated `<cluster>-app` secret (aws-visualize pattern) avoids the external store entirely; a good option for ephemeral e2e credentials.
- Managed secrets use creationPolicy Orphan semantics (existing secrets survive operator outage).

## 4. Networking / storage / scheduling relevant to migration jobs

- **CNI: Cilium** (k3s, kubeProxyReplacement, BGP to a UCG router; LB pool `10.31.0.80/28`). Per-app `CiliumClusterwideNetworkPolicy` exist (e.g. `platform/templates/argocd/ciliumnetworkpolicy-argocd-internal.yaml`); not every namespace has one, but if you add one, whitelist ns `traefik` and entities `host`/`remote-node` (Prometheus scrapes with hostNetwork=true).
- **Ingress**: Gateway API only. Shared `traefik/traefik-gateway` (Traefik chart pinned at **39.0.7 / v3.6, do NOT bump to 40/v3.7**, TLS regression, lessons.md). Apps own an `HTTPRoute` in their namespace with `parentRefs` to `traefik-gateway` listener `websecure`, hostname `<app>.{{ .Values.baseClusterDomain }}` (example: `platform/templates/timesheet/httproute-timesheet.yaml`). external-dns auto-creates DNS. A migration operator likely needs no ingress at all.
- **Storage**: Longhorn (`platform/templates/longhorn/templates/application-longhorn.yaml`; values in `platform/values.yaml`: v2 data engine, default class replica count 2, data path `/mnt/longhorn`). StorageClass name used everywhere: `longhorn`. RWX is available (Nextcloud/NetBox use ReadWriteMany). Capacity caution: Longhorn's 25% minimal-available floor on 2TB NVMe nodes already blocks large PVC growth (see nextcloud comment in `platform/values.yaml` ~line 635), so keep e2e/migration PVCs modest.
- **Nodes**: 3-node k3s cluster, all control-plane (cat01/02/03 = 10.31.0.218/.161/.132, per `tasks/todo.md`). No taints/nodeSelectors used by apps; anti-affinity/topologySpreadConstraints on `kubernetes.io/hostname` are the norm.
- **Resources convention**: always set `requests` (cpu+memory) and a `memory` limit; cpu limits generally omitted. Typical small-app footprint: 100m/256Mi requests, 512Mi limit. No LimitRange/ResourceQuota manifests found.

## 5. How to add the pgcopydb-operator app (exact recipe)

1. Create `platform/templates/pgcopydb-operator/application-pgcopydb-operator.yaml`. Best template to copy is the operator-style app `platform/templates/cloudnative-pg/application-cloudnative-pg.yaml`:

```yaml
{{- if .Values.pgcopydbOperator.enabled }}
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: pgcopydb-operator
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: "-3"
  finalizers:
    - resources-finalizer.argocd.argoproj.io/background
spec:
  project: platform
  source:
    # Helm-repo variant (CNPG style):
    #   chart: pgcopydb-operator
    #   repoURL: https://<helm-repo-url>
    #   targetRevision: <chart-version>
    # Git-repo variant (timesheet style, platform/templates/timesheet/application-timesheet.yaml):
    repoURL: https://gitlab.com/<group>/pgcopydb-operator.git
    targetRevision: main
    path: charts/pgcopydb-operator
{{- if .Values.pgcopydbOperator.values }}
    helm:
      valuesObject:
        {{- tpl (toYaml .Values.pgcopydbOperator.values) . | nindent 8 }}
{{- end }}
  destination:
    server: https://kubernetes.default.svc
    namespace: pgcopydb-system
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
      - ServerSideApply=true
{{- end }}
```

2. Add a values block to `platform/values.yaml` (`pgcopydbOperator: {enabled: true, values: {...}}`); key names are camelCase of the app name.
3. Add the chart source (git URL or Helm repo URL) to `sourceRepos` in `platform/templates/platform-appproject.yaml` (mandatory, it's a whitelist). OCI is permitted in principle (`registry-1.docker.io/bitnamicharts` is whitelisted) but **no existing Application sources a chart from OCI**; all use HTTPS Helm repos or git repos with `path:`. Git-repo-with-path (timesheet/aws-visualize/studenthousing pattern) is the proven route for in-house charts.
4. Optional per conventions: `prometheusrule-pgcopydb.yaml` (wave 2, guarded by `.Capabilities.APIVersions.Has "monitoring.coreos.com/v1"`), `infisicalsecret-*.yaml` (wave -1) if the operator/e2e needs secrets or a pull secret, plus a Namespace manifest at wave -2 if secrets must land before the Application (aws-visualize/timesheet pattern).
5. If images live in a private GitLab registry: pull secret `<app>-registry` via InfisicalSecret (dockerconfigjson), path `/<app>-registry` in project `infrastructure-secrets`.

## 6. Cluster access

- Kubeconfig/context referenced in `tasks/todo.md`: **`cloudcats-ber-oidc`**, API server `https://kube.dev.ber1.cloudcats.io:6443` resolving to BGP VIP **10.31.0.80** (private; WireGuard access to `10.31.0.0/24` required). Auth is Keycloak OIDC (`kubelogin`/`kubectl oidc-login`); group `cluster-admins` maps to admin.
- Environment: `platform/values-ber1.yaml` sets `environmentName: ber1`, `environmentType: dev`; base domain is `dev.ber1.cloudcats.io` (inferred from `kube.dev.ber1.cloudcats.io`; `values.yaml` default `example.com` is overridden by Ansible). k3s, 3 nodes.
- Emergency access: `/etc/rancher/k3s/k3s.yaml` on any server node with `127.0.0.1` rewritten to `10.31.0.80` (lessons.md).

## 7. Monitoring stack

- kube-prometheus-stack: `platform/templates/kube-prometheus-stack/application-kube-prometheus-stack.yaml`, ns `monitoring`. Prometheus is configured with `serviceMonitorSelectorNilUsesHelmValues: false`, `podMonitorSelectorNilUsesHelmValues: false`, `ruleSelectorNilUsesHelmValues: false`: **any ServiceMonitor/PodMonitor/PrometheusRule in any namespace is discovered automatically**, no special labels needed.
- Convention: enable the operator chart's `metrics.serviceMonitor` (or PodMonitor, like CNPG) and ship a `prometheusrule-<app>.yaml` in the app's template dir at sync-wave 2, guarded by the `monitoring.coreos.com/v1` capability check (see `platform/templates/cloudnative-pg/prometheusrule-cnpg.yaml`).
- Prometheus runs `hostNetwork: true`; factor this into any network policy (allow `host`/`remote-node` entities).
- Grafana dashboards: ConfigMaps labeled `grafana_dashboard` in ns `monitoring` (sidecar; see `platform/templates/kube-prometheus-stack/configmap-dashboards.yaml` + `platform/dashboards/`). Alerts route to Discord via AlertmanagerConfig (`alertmanagerconfig-discord.yaml`); alert labels use `severity: critical|warning`; Alertmanager templates are plain Go text/template (no Sprig).

## 8. Other constraints an operator author must respect

- **k3s specifics**: no separate etcd/scheduler/controller-manager pods; metrics-server runs hostNetwork with `--kubelet-insecure-tls`.
- **No Pod Security Admission / PSS labels, no ResourceQuota/LimitRange found** in the repo: nothing enforced, but the house style is explicit requests + memory limits.
- **Image registries**: private images come from `registry.gitlab.com` with per-namespace dockerconfigjson pull secrets synced from Infisical (`<app>-registry`); argocd-image-updater is configured for the GitLab registry (`platform/templates/argocd-image-updater/`) with per-app `imageupdater-*.yaml` manifests if `latest`-tag auto-updating is wanted. Public images (ghcr.io, quay.io, docker.io) are pulled directly.
- **Helm templating gotchas**: values blocks are run through `tpl`, so `{{ .Values.baseClusterDomain }}` works inside app values; literal Go templates destined for other systems must be escaped with backticks (see infisicalsecret/prometheusrule files).
- **RFC-style rules from `tasks/lessons.md`** to honor: never kubectl-patch Application `.operation`; external-dns handles DNS automatically; Keycloak client config lives in the provisioning repo (Ansible playbook, not GitOps); Traefik pinned to chart 39.0.7; secrets must exist in Infisical before a health-gated resource lands or the root app deadlocks.
- Namespaces are created either via `CreateNamespace=true` or explicit Namespace manifests at wave -2 when secrets must precede the app.


## Open questions from this research pass

- Where exactly the ArgoCD root Application manifest lives. values.yaml says it is passed via Ansible from a separate 'provisioning' repo (referenced as /Users/ydixken/development/cloudcats/provisioning in tasks files), which was not part of the explored repo.
- The actual value of baseClusterDomain for the ber1 dev environment. values-ber1.yaml only sets environmentName/environmentType; 'dev.ber1.cloudcats.io' is inferred from kube.dev.ber1.cloudcats.io in tasks/todo.md, not stated in a values file.
- The exact kubectl context name as it appears in the user's local kubeconfig. tasks/todo.md calls the kubeconfig 'cloudcats-ber-oidc', but the literal context name could differ.
- Whether OCI-registry chart sources actually work with the AppProject as-is. registry-1.docker.io/bitnamicharts is whitelisted but no Application uses an OCI source, so the pattern is unproven in this cluster.
- Current live cluster state (node capacity, Longhorn free space, k3s/Kubernetes version): repo is manifests only; no version pin for Kubernetes itself was found.

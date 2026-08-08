# Argo CD health checks

Argo CD does not know what a healthy `Migration` looks like; without help it reports every custom resource as `Healthy` the moment it applies. This Lua health check maps the [conditions](../reference/conditions.md) to Argo CD's health states, so a failed migration shows up as `Degraded` in the UI and in notifications instead of sitting green.

Add it to the `argocd-cm` ConfigMap (or the equivalent `resource.customizations` block of your Argo CD Helm values):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argocd-cm
  namespace: argocd
data:
  resource.customizations.health.pgcopydb-operator.io_Migration: |
    local hs = {}
    hs.status = "Progressing"
    hs.message = "no status yet"
    if obj.status == nil then
      return hs
    end
    if obj.status.phase == "Suspended" then
      hs.status = "Suspended"
      hs.message = "spec.suspend is set; work volume kept"
      return hs
    end
    if obj.status.conditions ~= nil then
      for _, c in ipairs(obj.status.conditions) do
        if c.type == "Failed" and c.status == "True" then
          hs.status = "Degraded"
          hs.message = c.reason .. ": " .. c.message
          return hs
        end
        if c.type == "Complete" and c.status == "True" then
          hs.status = "Healthy"
          hs.message = c.message
          return hs
        end
      end
    end
    if obj.status.phase ~= nil then
      hs.message = "phase: " .. obj.status.phase
    end
    return hs
```

The mapping: `Failed=True` is `Degraded`, `Complete=True` is `Healthy`, phase `Suspended` is `Suspended`, and everything else is `Progressing` with the current phase in the message. Both terminal conditions are absorbing, so the health state settles once and stays.

This is a starting point, not a policy. A live migration parked at `CutoverPending` counts as `Progressing` here indefinitely, which is correct (it is waiting for your approval) but may deserve its own mapping if you alert on stuck progress. Extend the Lua with the [condition reasons](../reference/conditions.md) as needed.

One caveat for GitOps-managed migrations: `spec.cutover.approved: true` is a deliberate, timed action (writes to the source must already be stopped). Flipping it through a Git commit works, but mind your sync latency; `kubectl patch` at the moment of cutover is the sharper tool.

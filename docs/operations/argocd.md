# Argo CD health checks

Without a custom health check, Argo CD reports every `Migration` as `Healthy` as soon as Argo CD applies it.
This Lua health check maps the [conditions](../reference/conditions.md) to the health states of Argo CD.
A failed migration then shows as `Degraded` in the UI and in notifications.

Add it to the `argocd-cm` ConfigMap, or to the equivalent `resource.customizations` block in your Argo CD Helm values:

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

The mapping:

- `Failed=True` is `Degraded`.
- `Complete=True` is `Healthy`.
- Phase `Suspended` is `Suspended`.
- Everything else is `Progressing`, with the current phase in the message.

Both terminal conditions are absorbing, so the health state settles one time.
A [dry run](lifecycle.md#dry-run) that passed sets `Complete=True` with reason `DryRunSucceeded`, so it also shows `Healthy`.
If you need to distinguish the two, match that reason in the Lua.
A verification mismatch does not set `Failed`, so a migration with `Verified=False` still reports `Healthy`.
Alert on `Verified` separately.

A live migration at `CutoverPending` stays `Progressing` in this mapping until you approve the cutover.
If you alert on stuck progress, give that phase its own mapping.
Extend the Lua with the [condition reasons](../reference/conditions.md).

One caveat applies to GitOps-managed migrations.
`spec.cutover.approved: true` is a timed action.
You must stop the writes to the source before that value lands.
A change through a Git commit works, but pay attention to your sync latency.
`kubectl patch` at the moment of cutover gives you exact timing.

# OpenTelemetry

The operator can send its metrics over OTLP (the OpenTelemetry protocol) to an OpenTelemetry Collector or a compatible backend.
This release exports metrics only.
The export does not change the Prometheus scrape endpoint.

> [!warning]
> Send the metrics to a Prometheus by one path only.
> When the collector sends the OTLP metrics to the Prometheus that scrapes the operator, each series occurs two times.
> Then the rule of the `PgcopydbMigrationCutoverStalled` alert fails with an error, and that alert does not fire.
> The other alerts fire two times, and the dashboards show each Migration two times.
> For this configuration, stop the scrape: set `metrics.serviceMonitor.enabled=false`.
> The "Scrape Targets Up" panel of the Operator Health dashboard then shows no data, because only a scrape makes the `up` series.

## Turn on the export

1. Run `helm upgrade` with the collector endpoint.

   ```sh
   helm upgrade pgcopydb-operator oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator \
     --namespace pgcopydb-system --reset-then-reuse-values \
     --set otel.enabled=true \
     --set otel.endpoint=http://otel-collector.observability:4317
   ```

   The `--reset-then-reuse-values` flag needs Helm 3.14 or later.
   It applies the defaults of the new chart, then the values of your release.
   With `--reuse-values`, Helm does not apply the new defaults of the `otel` block.
   As an alternative, replace `--reset-then-reuse-values` with `-f values.yaml`, a file that holds all the values of your release.
   The `http://` endpoint sends without TLS.
   For a collector with TLS, use `https://`.

2. Wait for the operator Pod to restart.

   ```sh
   kubectl -n pgcopydb-system rollout status deployment/pgcopydb-operator
   ```

3. Look at your backend for metrics from the service `pgcopydb-operator`.

The chart fails to render when `otel.enabled` is true and `otel.endpoint` is empty.

## Settings

The chart sets the standard `OTEL_*` environment variables on the manager container.
The operator reads them at start.
A change to an `otel` value changes the Deployment, and Kubernetes then replaces the Pod.

`otel.endpoint` is the OTLP endpoint.
Set it whenever `otel.enabled` is true.
`otel.protocol` is `grpc` (the default) or `http/protobuf`.
A `grpc` endpoint usually uses port 4317.
An `http/protobuf` endpoint usually uses port 4318 and must have a scheme.

The scheme of the endpoint sets TLS:

- `http://` sends without TLS, for example `http://otel-collector.observability:4317`.
- `https://` sends with TLS.
- A `grpc` endpoint without a scheme (`host:port`) uses TLS, unless `otel.tls.insecure` is true.

An endpoint with an IP address as the host must have a scheme, for example `http://10.0.0.5:4317`.

`otel.signals.metrics` (default `true`) turns the metrics export on or off.
`otel.metricsIntervalSeconds` (default 30) sets how often the operator pushes metrics.
The chart pins the trace and log exporters to `none`, because the operator sends neither in this release.

The operator exports only when `OTEL_METRICS_EXPORTER` is `otlp`.
An unset `OTEL_METRICS_EXPORTER` means no export.
This differs from the OpenTelemetry SDK default, which exports over OTLP.
`OTEL_SDK_DISABLED=true` turns the export off.
The operator stops at start, with an error that names the variable, when one of these settings is not valid:

- `OTEL_METRICS_EXPORTER` or the protocol has an unsupported value.
- The exporter cannot use the endpoint, for example an IP address and port without a scheme.
- A headers variable, such as `OTEL_EXPORTER_OTLP_HEADERS`, is not a list of `key=value` pairs.
  The error does not show the value, because the value can contain a token.
- `OTEL_RESOURCE_ATTRIBUTES` is not a list of `key=value` pairs.

`otel.resourceAttributes` adds resource attributes in the form `key=value,key2=value2`.
The chart passes the string as `OTEL_RESOURCE_ATTRIBUTES`.

`otel.tls.insecure` turns TLS off.
Use it only for a `grpc` endpoint that has no scheme, because it also turns TLS off for an `https://` endpoint.
`otel.tls.caSecret` names a Secret that holds the CA bundle for the collector certificate, in the key `ca.crt` by default.
`otel.tls.clientCertSecret` names a `kubernetes.io/tls` Secret for mutual TLS.
The chart mounts each Secret in the Pod and sets the matching `OTEL_EXPORTER_OTLP_*` path variables.

`otel.headersSecret` names a Secret key that holds the OTLP headers, in the key `headers` by default.
The chart passes the value as `OTEL_EXPORTER_OTLP_HEADERS` from the Secret, so a token does not appear in the Deployment.

The operator reads the content of the `headersSecret`, `caSecret`, and `clientCertSecret` Secrets one time, at start.
A change to the content of one of these Secrets does not replace the Pod.
This includes a certificate that cert-manager renews.
After you change the content of a Secret, restart the operator:

```sh
kubectl -n pgcopydb-system rollout restart deployment/pgcopydb-operator
```

`extraEnv` adds raw environment variables to the manager container.
Use it for an `OTEL_*` setting that the chart has no value for.
The chart renders `extraEnv` after the `otel` block.

## Examples

Check out these **examples**:

- A collector in the cluster, over `http/protobuf`:

  ```yaml
  otel:
    enabled: true
    endpoint: http://otel-collector.observability:4318
    protocol: http/protobuf
  ```

- A SaaS backend that needs an authorization header.
  Create the Secret first.

  ```sh
  kubectl -n pgcopydb-system create secret generic otlp-auth \
    --from-literal=headers='authorization=Bearer <token>'
  ```

  Then reference it in the values.

  ```yaml
  otel:
    enabled: true
    endpoint: https://otlp.example.com
    protocol: http/protobuf
    headersSecret:
      name: otlp-auth
  ```

- A collector with a private CA and mutual TLS.
  The CA Secret holds `ca.crt`.
  The client Secret is a `kubernetes.io/tls` Secret.

  ```yaml
  otel:
    enabled: true
    endpoint: https://otel-collector.observability:4318
    protocol: http/protobuf
    tls:
      caSecret:
        name: otel-collector-ca
      clientCertSecret:
        name: pgcopydb-operator-otlp-client
  ```

## What the operator sends

Every metric carries these resource attributes:

- `service.name` is `pgcopydb-operator`.
- `service.version` is the operator version.
- `service.instance.id` is the name of the operator Pod.
  When `POD_NAME` is not set, it is the host name.
- `k8s.namespace.name` is the namespace of the operator Pod.
- `k8s.pod.name` is the name of the operator Pod.

`OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` override these values.

Every metric keeps its Prometheus name and labels.
This includes the controller-runtime metrics.
The [metric reference](monitoring.md#metric-reference) lists the Migration metrics.

Every replica exports, not only the leader.
Tell the replicas apart with `service.instance.id` or `k8s.pod.name`.

The operator flushes metrics at shutdown and waits at most 5 seconds.

When you delete a Migration, its series stop.
They do not go to zero.
Treat a missing series as no data.

## Troubleshooting

The operator Pod does not start:

1. Find the Pod that restarts.

   ```sh
   kubectl -n pgcopydb-system get pods -l app.kubernetes.io/name=pgcopydb-operator
   ```

2. Read the error in the log of its previous container.
   The error names the variable that is not valid.

   ```sh
   kubectl -n pgcopydb-system logs <pod> --previous
   ```

No data arrives:

1. Search the operator log for `OpenTelemetry export failed`.

   ```sh
   kubectl -n pgcopydb-system logs deployment/pgcopydb-operator | grep "OpenTelemetry export failed"
   ```

2. Make sure the endpoint scheme matches the TLS setting of the collector.
   Use `http://` for a collector without TLS, and `https://` for a collector with TLS.
   A `grpc` endpoint without a scheme uses TLS, unless `otel.tls.insecure` is true.
3. Make sure a NetworkPolicy allows egress from the operator Pod to the collector port.
4. If you changed the content of a referenced Secret, restart the operator, as [Settings](#settings) shows.
5. Check the receiver in the collector configuration.
   It must listen on the same protocol and port as `otel.protocol` and `otel.endpoint`.

> [!note]
> The operator logs an export error at most once per minute.
> The log line says "OpenTelemetry export failed", followed by a note that the operator suppresses further errors for a minute.

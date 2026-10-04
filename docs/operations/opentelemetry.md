# OpenTelemetry

The operator can send its metrics over OTLP (the OpenTelemetry protocol) to an OpenTelemetry Collector or a compatible backend.
This release exports metrics only.
Prometheus scraping does not change, and both paths can run at the same time.

## Turn on the export

1. Run `helm upgrade` with the collector endpoint.

   ```sh
   helm upgrade pgcopydb-operator oci://ghcr.io/ydixken/pgcopydb-operator/charts/pgcopydb-operator \
     --namespace pgcopydb-system --reuse-values \
     --set otel.enabled=true \
     --set otel.endpoint=otel-collector.observability:4317
   ```

2. Wait for the operator Pod to restart.

   ```sh
   kubectl -n pgcopydb-system rollout status deployment/pgcopydb-operator
   ```

3. Look at your backend for metrics from the service `pgcopydb-operator`.

The chart fails to render when `otel.enabled` is true and `otel.endpoint` is empty.

## Settings

The chart sets the standard `OTEL_*` environment variables on the manager container.
The operator reads them at start, so a change to a value restarts the Pod.

`otel.endpoint` is the OTLP endpoint.
Set it whenever `otel.enabled` is true.
`otel.protocol` is `grpc` (the default) or `http/protobuf`.
A `grpc` endpoint has the form `host:port`, usually port 4317.
An `http/protobuf` endpoint has a scheme, usually `http://host:4318`.

`otel.signals.metrics` (default `true`) turns the metrics export on or off.
`otel.metricsIntervalSeconds` (default 30) sets how often the operator pushes metrics.
The chart pins the trace and log exporters to `none`, because the operator sends neither in this release.

The operator exports only when `OTEL_METRICS_EXPORTER` is `otlp`.
An unset `OTEL_METRICS_EXPORTER` means no export.
This differs from the OpenTelemetry SDK default, which exports over OTLP.
`OTEL_SDK_DISABLED=true` turns the export off.
The operator stops at start, with an error that names the variable, when `OTEL_METRICS_EXPORTER` or the protocol has an unsupported value.

`otel.resourceAttributes` adds resource attributes in the form `key=value,key2=value2`.
The chart passes the string as `OTEL_RESOURCE_ATTRIBUTES`.

`otel.tls.insecure` turns TLS off for a `grpc` endpoint that has no scheme.
`otel.tls.caSecret` names a Secret that holds the CA bundle for the collector certificate, in the key `ca.crt` by default.
`otel.tls.clientCertSecret` names a `kubernetes.io/tls` Secret for mutual TLS.
The chart mounts each Secret in the Pod and sets the matching `OTEL_EXPORTER_OTLP_*` path variables.

`otel.headersSecret` names a Secret key that holds the OTLP headers, in the key `headers` by default.
The chart passes the value as `OTEL_EXPORTER_OTLP_HEADERS` from the Secret, so a token does not appear in the Deployment.

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
- `k8s.namespace.name` is the namespace of the operator Pod.
- `k8s.pod.name` is the name of the operator Pod.

`OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` override these values.

Every metric keeps its Prometheus name and labels.
This includes the controller-runtime metrics.
The [metric reference](monitoring.md#metric-reference) lists the Migration metrics.

Every replica exports, not only the leader.
Tell the replicas apart with `k8s.pod.name`.

The operator flushes metrics at shutdown and waits at most 5 seconds.

When you delete a Migration, its series stop.
They do not go to zero.
Treat a missing series as no data.

## Troubleshooting

No data arrives:

1. Search the operator log for `OpenTelemetry export failed`.

   ```sh
   kubectl -n pgcopydb-system logs deployment/pgcopydb-operator | grep "OpenTelemetry export failed"
   ```

2. Make sure the endpoint scheme matches the protocol.
   A `grpc` endpoint has no scheme.
   An `http/protobuf` endpoint starts with `http://` or `https://`.
3. Make sure a NetworkPolicy allows egress from the operator Pod to the collector port.
4. Check the receiver in the collector configuration.
   It must listen on the same protocol and port as `otel.protocol` and `otel.endpoint`.

> [!note]
> The operator logs an export error at most once per minute.
> The log line says "OpenTelemetry export failed", followed by a note that the operator suppresses further errors for a minute.

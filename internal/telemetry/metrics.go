/*
Copyright 2026 pgcopydb-operator contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package telemetry

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	otelprom "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	serviceName = "pgcopydb-operator"
	// shutdownTimeout bounds the final flush so a hanging collector cannot
	// hold up a rollout past the pod's termination grace period.
	shutdownTimeout = 5 * time.Second
)

type metricsExporter struct{ provider *sdkmetric.MeterProvider }

// NewMetrics returns nil when export is off, so the caller adds nothing.
func NewMetrics(ctx context.Context, cfg Config, version string, gatherer prometheus.Gatherer) (manager.Runnable, error) {
	if !cfg.Metrics {
		return nil, nil
	}
	setErrorHandler()
	res, err := newResource(ctx, version)
	if err != nil {
		return nil, fmt.Errorf("build the OpenTelemetry resource: %w", err)
	}
	var exp sdkmetric.Exporter
	if cfg.Protocol == protocolHTTP {
		exp, err = otlpmetrichttp.New(ctx)
	} else {
		exp, err = otlpmetricgrpc.New(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("create the OTLP metrics exporter: %w", err)
	}
	// The interval comes from OTEL_METRIC_EXPORT_INTERVAL, read by the SDK.
	reader := sdkmetric.NewPeriodicReader(exp,
		sdkmetric.WithProducer(otelprom.NewMetricProducer(otelprom.WithGatherer(gatherer))))
	return &metricsExporter{provider: sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res), sdkmetric.WithReader(reader))}, nil
}

func (e *metricsExporter) Start(ctx context.Context) error {
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := e.provider.Shutdown(sctx); err != nil {
		logf.Log.WithName("telemetry").Error(err, "OTLP metrics exporter did not flush on shutdown")
	}
	return nil
}

// NeedLeaderElection is false: standbys export controller-runtime's metrics too,
// as they are scraped today.
func (e *metricsExporter) NeedLeaderElection() bool { return false }

// newResource lets OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME override the
// defaults, because WithFromEnv comes after WithAttributes.
func newResource(ctx context.Context, version string) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		attribute.String("service.name", serviceName),
		attribute.String("service.version", version),
	}
	if v := os.Getenv("POD_NAMESPACE"); v != "" {
		attrs = append(attrs, attribute.String("k8s.namespace.name", v))
	}
	if v := os.Getenv("POD_NAME"); v != "" {
		attrs = append(attrs, attribute.String("k8s.pod.name", v))
	}
	return resource.New(ctx, resource.WithAttributes(attrs...), resource.WithFromEnv(), resource.WithTelemetrySDK())
}

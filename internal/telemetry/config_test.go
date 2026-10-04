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
	"strings"
	"testing"
)

const (
	httpProtobuf = "http/protobuf"
	grpcProto    = "grpc"
)

func TestConfigFromEnv(t *testing.T) {
	const (
		otelMetricsExporter          = "OTEL_METRICS_EXPORTER"
		otelExporterOTLPProtocol     = "OTEL_EXPORTER_OTLP_PROTOCOL"
		otelExporterOTLPMetricsProto = "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"
		otelSDKDisabled              = "OTEL_SDK_DISABLED"
		otelEndpoint                 = "OTEL_EXPORTER_OTLP_ENDPOINT"
		otelMetricsEndpoint          = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
		otelHeaders                  = "OTEL_EXPORTER_OTLP_HEADERS"
		otelMetricsHeaders           = "OTEL_EXPORTER_OTLP_METRICS_HEADERS"
		otlp                         = "otlp"
		secret                       = "s3cr3t-token"
	)
	grpcOn := Config{Metrics: true, Protocol: grpcProto}
	httpOn := Config{Metrics: true, Protocol: httpProtobuf}
	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr string
	}{
		{name: "unset is off", env: map[string]string{}, want: Config{}},
		{name: "none is off", env: map[string]string{otelMetricsExporter: "none"}, want: Config{}},
		{name: "otlp defaults to grpc", env: map[string]string{otelMetricsExporter: otlp},
			want: Config{Metrics: true, Protocol: grpcProto}},
		{name: "general protocol", env: map[string]string{otelMetricsExporter: otlp,
			otelExporterOTLPProtocol: httpProtobuf}, want: Config{Metrics: true, Protocol: httpProtobuf}},
		{name: "signal protocol wins", env: map[string]string{otelMetricsExporter: otlp,
			otelExporterOTLPProtocol: grpcProto, otelExporterOTLPMetricsProto: httpProtobuf},
			want: Config{Metrics: true, Protocol: httpProtobuf}},
		{name: "sdk disabled wins", env: map[string]string{otelSDKDisabled: "true",
			otelMetricsExporter: otlp}, want: Config{}},
		{name: "unsupported exporter", env: map[string]string{otelMetricsExporter: "prometheus"},
			wantErr: "OTEL_METRICS_EXPORTER"},
		{name: "unsupported protocol", env: map[string]string{otelMetricsExporter: otlp,
			otelExporterOTLPProtocol: "http/json"}, wantErr: "OTEL_EXPORTER_OTLP_PROTOCOL"},
		{name: "grpc host:port", env: map[string]string{otelMetricsExporter: otlp,
			otelEndpoint: "collector:4317"}, want: grpcOn},
		{name: "grpc IP with scheme", env: map[string]string{otelMetricsExporter: otlp,
			otelEndpoint: "http://10.0.0.5:4317"}, want: grpcOn},
		{name: "grpc IP without scheme", env: map[string]string{otelMetricsExporter: otlp,
			otelEndpoint: "10.0.0.5:4317"}, wantErr: otelEndpoint},
		{name: "signal endpoint wins", env: map[string]string{otelMetricsExporter: otlp,
			otelEndpoint: "http://10.0.0.5:4317", otelMetricsEndpoint: "10.0.0.6:4317"},
			wantErr: otelMetricsEndpoint},
		{name: "http IP with scheme", env: map[string]string{otelMetricsExporter: otlp,
			otelExporterOTLPProtocol: httpProtobuf, otelEndpoint: "http://10.0.0.5:4318"}, want: httpOn},
		{name: "http without scheme", env: map[string]string{otelMetricsExporter: otlp,
			otelExporterOTLPProtocol: httpProtobuf, otelEndpoint: "collector:4318"}, wantErr: otelEndpoint},
		{name: "headers", env: map[string]string{otelMetricsExporter: otlp,
			otelHeaders: "authorization=Bearer%20" + secret + ", x-team = dba"}, want: grpcOn},
		{name: "header without =", env: map[string]string{otelMetricsExporter: otlp,
			otelHeaders: "Authorization: Bearer " + secret}, wantErr: otelHeaders},
		{name: "header with bad escape", env: map[string]string{otelMetricsExporter: otlp,
			otelMetricsHeaders: "authorization=" + secret + "%zz"}, wantErr: otelMetricsHeaders},
		{name: "header with bad key", env: map[string]string{otelMetricsExporter: otlp,
			otelHeaders: "auth orization=" + secret}, wantErr: otelHeaders},
		{name: "general headers checked too", env: map[string]string{otelMetricsExporter: otlp,
			otelHeaders: secret, otelMetricsHeaders: "a=b"}, wantErr: otelHeaders},
		{name: "export off skips checks", env: map[string]string{otelEndpoint: "10.0.0.5:4317",
			otelHeaders: secret}, want: Config{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range []string{otelSDKDisabled, otelMetricsExporter,
				otelExporterOTLPProtocol, otelExporterOTLPMetricsProto, otelEndpoint, otelMetricsEndpoint,
				otelHeaders, otelMetricsHeaders} {
				t.Setenv(k, tc.env[k])
			}
			got, err := ConfigFromEnv()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tc.wantErr)
				}
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("err = %v leaks the header value", err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("ConfigFromEnv() = %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

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
		otlp                         = "otlp"
	)
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range []string{otelSDKDisabled, otelMetricsExporter,
				otelExporterOTLPProtocol, otelExporterOTLPMetricsProto} {
				t.Setenv(k, tc.env[k])
			}
			got, err := ConfigFromEnv()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("ConfigFromEnv() = %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

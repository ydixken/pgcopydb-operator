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

// Package telemetry exports the operator's signals over OTLP when the
// standard OTEL_* environment asks for it. Nothing runs otherwise.
package telemetry

import (
	"fmt"
	"os"
	"strings"
)

// Config is the part of the OTEL_* environment the operator decides on; the
// exporters read endpoint, headers, TLS and timeouts from it themselves.
type Config struct {
	Metrics  bool
	Protocol string
}

// ConfigFromEnv treats an unset OTEL_METRICS_EXPORTER as none, unlike the SDK
// default of otlp: export is opt-in, so an upgrade never starts pushing data.
func ConfigFromEnv() (Config, error) {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true") {
		return Config{}, nil
	}
	switch exp := strings.TrimSpace(os.Getenv("OTEL_METRICS_EXPORTER")); exp {
	case "", "none":
		return Config{}, nil
	case "otlp":
	default:
		return Config{}, fmt.Errorf("OTEL_METRICS_EXPORTER=%q is not supported: use otlp or none", exp)
	}
	name, proto := "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"))
	if proto == "" {
		name, proto = "OTEL_EXPORTER_OTLP_PROTOCOL", strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"))
	}
	switch proto {
	case "":
		proto = "grpc"
	case "grpc", "http/protobuf":
	default:
		return Config{}, fmt.Errorf("%s=%q is not supported: use grpc or http/protobuf", name, proto)
	}
	return Config{Metrics: true, Protocol: proto}, nil
}

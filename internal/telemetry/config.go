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
	"net/url"
	"os"
	"strings"
)

const protocolHTTP = "http/protobuf"

// Config is the part of the OTEL_* environment the operator decides on; the
// exporters read endpoint, headers, TLS and timeouts from it themselves, after
// ConfigFromEnv has rejected the endpoint and header values they would drop.
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
	case "grpc", protocolHTTP:
	default:
		return Config{}, fmt.Errorf("%s=%q is not supported: use grpc or http/protobuf", name, proto)
	}
	if err := checkEndpoint(proto); err != nil {
		return Config{}, err
	}
	// The SDK parses both variables, so a bad general value leaks even when
	// the signal one is set.
	for _, name := range []string{"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_METRICS_HEADERS"} {
		if err := checkHeaders(name, strings.TrimSpace(os.Getenv(name))); err != nil {
			return Config{}, err
		}
	}
	return Config{Metrics: true, Protocol: proto}, nil
}

// checkEndpoint rejects what the SDK would drop with only a stderr line: an
// unparsable URL (grpc "10.0.0.5:4317" falls back to localhost) or one with no
// host where the exporter needs it (http/protobuf "collector:4318").
func checkEndpoint(proto string) error {
	name := "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		name, v = "OTEL_EXPORTER_OTLP_ENDPOINT", strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	}
	if v == "" {
		return nil
	}
	u, err := url.Parse(v)
	if err == nil && u.Host == "" && (proto == protocolHTTP || u.Scheme == "http" || u.Scheme == "https") {
		err = fmt.Errorf("%q has no host", v)
	}
	if err != nil {
		return fmt.Errorf("%s is not a valid endpoint, write it as http://host:port or https://host:port: %w", name, err)
	}
	return nil
}

// checkHeaders mirrors the SDK's stringToHeader, which logs a bad pair in full,
// token included. The error names the variable and never echoes the value.
func checkHeaders(name, v string) error {
	if v == "" {
		return nil
	}
	for pair := range strings.SplitSeq(v, ",") {
		k, val, ok := strings.Cut(pair, "=")
		if !ok || !isToken(strings.TrimSpace(k)) {
			return fmt.Errorf("%s is not valid: write each header as key=value, separated by commas", name)
		}
		if _, err := url.PathUnescape(val); err != nil {
			return fmt.Errorf("%s is not valid: a header value has a malformed %%-escape", name)
		}
	}
	return nil
}

// tokenChars is the RFC 9110 token set the SDK accepts in a header key.
const tokenChars = "!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func isToken(s string) bool { return s != "" && strings.Trim(s, tokenChars) == "" }

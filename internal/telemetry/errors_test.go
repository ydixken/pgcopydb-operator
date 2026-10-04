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
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func TestRateLimitedHandlerLogsOncePerWindow(t *testing.T) {
	now := time.Unix(0, 0)
	var logged int
	h := &rateLimitedHandler{every: time.Minute, now: func() time.Time { return now },
		log: func(error) { logged++ }}
	h.Handle(errors.New("a"))
	h.Handle(errors.New("b"))
	if logged != 1 {
		t.Fatalf("logged %d times inside one window, want 1", logged)
	}
	now = now.Add(61 * time.Second)
	h.Handle(errors.New("c"))
	if logged != 2 {
		t.Fatalf("logged %d times after the window, want 2", logged)
	}
}

// syncBuffer guards the log buffer the SDK and the test touch concurrently.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestSDKDiagnosticsReachTheOperatorLog(t *testing.T) {
	out := &syncBuffer{}
	logf.SetLogger(crzap.New(crzap.WriteTo(out)))
	setErrorHandler()
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT", "30s") // the SDK wants milliseconds and logs the rest
	if _, err := otlpmetrichttp.New(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "parse duration") || !strings.Contains(got, `"logger":"telemetry"`) {
		t.Fatalf("SDK diagnostic did not reach the operator log, got %q", got)
	}
}

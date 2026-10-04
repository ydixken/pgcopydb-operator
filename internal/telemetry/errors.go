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
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// rateLimitedHandler keeps a down collector from flooding the operator log:
// every export interval would otherwise log the same connection error.
type rateLimitedHandler struct {
	mu    sync.Mutex
	every time.Duration
	last  time.Time
	now   func() time.Time
	log   func(error)
}

func (h *rateLimitedHandler) Handle(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if t := h.now(); h.last.IsZero() || t.Sub(h.last) >= h.every {
		h.last = t
		h.log(err)
	}
}

var installErrorHandler sync.Once

func setErrorHandler() {
	installErrorHandler.Do(func() {
		log := logf.Log.WithName("telemetry")
		otel.SetErrorHandler(&rateLimitedHandler{every: time.Minute, now: time.Now,
			log: func(err error) {
				log.Error(err, "OpenTelemetry export failed; further errors are suppressed for a minute")
			}})
	})
}

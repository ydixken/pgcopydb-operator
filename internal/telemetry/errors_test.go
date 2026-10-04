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
	"errors"
	"testing"
	"time"
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

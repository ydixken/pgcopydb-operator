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

package e2e

import "testing"

type execErrorCase struct {
	name   string
	stderr string
}

var safeExecErrorCases = []execErrorCase{
	{
		name:   "front-end TLS timeout",
		stderr: "Unable to connect to the server: net/http: TLS handshake timeout",
	},
	{
		name:   "front-end refusal",
		stderr: "The connection to the server 192.0.2.4:6443 was refused - did you specify the right host or port?",
	},
	{
		name:   "API-server backend dial",
		stderr: "error: Internal error occurred: error dialing backend: dial tcp 192.0.2.5:10250: i/o timeout",
	},
	{
		name: "SPDY TLS timeout",
		stderr: `error: error sending request: Post "https://192.0.2.4:6443/api/v1/namespaces/ns/pods/p/exec": ` +
			`net/http: TLS handshake timeout`,
	},
	{
		name: "SPDY refusal",
		stderr: `error: error sending request: Post "https://192.0.2.4:6443/api/v1/namespaces/ns/pods/p/exec": ` +
			`dial tcp 192.0.2.4:6443: connect: connection refused`,
	},
}

var terminalExecErrorCases = []execErrorCase{
	{name: "empty", stderr: ""},
	{name: "sending request EOF", stderr: "error: error sending request: EOF"},
	{name: "front-end EOF", stderr: "Unable to connect to the server: EOF"},
	{name: "bare TLS fragment", stderr: "TLS handshake timeout"},
	{name: "bare refusal fragment", stderr: "connection refused"},
	{name: "remote psql stderr", stderr: "psql: error: connection to server failed: connection refused"},
	{
		name:   "multiline after safe line",
		stderr: "Unable to connect to the server: net/http: TLS handshake timeout\nremote output",
	},
	{
		name:   "multiline before safe line",
		stderr: "remote output\nUnable to connect to the server: net/http: TLS handshake timeout",
	},
	{
		name:   "carriage return without LF",
		stderr: "Unable to connect to the server: net/http: TLS handshake timeout\r",
	},
	{
		name:   "empty refusal endpoint",
		stderr: "The connection to the server  was refused - did you specify the right host or port?",
	},
	{name: "empty backend detail", stderr: "error: Internal error occurred: error dialing backend: "},
	{name: "blank backend detail", stderr: "error: Internal error occurred: error dialing backend:   "},
	{
		name:   "empty SPDY URL",
		stderr: `error: error sending request: Post "": net/http: TLS handshake timeout`,
	},
	{
		name:   "empty SPDY address",
		stderr: `error: error sending request: Post "https://192.0.2.4/exec": dial tcp : connect: connection refused`,
	},
	{
		name:   "two final line feeds",
		stderr: "Unable to connect to the server: net/http: TLS handshake timeout\n\n",
	},
}

func TestTransientExecError(t *testing.T) {
	tests := append([]execErrorCase(nil), safeExecErrorCases...)
	tests = append(tests,
		execErrorCase{
			name:   "one final LF",
			stderr: safeExecErrorCases[0].stderr + "\n",
		},
		execErrorCase{
			name:   "one final CRLF",
			stderr: safeExecErrorCases[0].stderr + "\r\n",
		},
	)
	for _, tt := range tests {
		t.Run("accepts "+tt.name, func(t *testing.T) {
			if !transientExecError(tt.stderr) {
				t.Fatalf("transientExecError(%q) = false, want true", tt.stderr)
			}
		})
	}

	for _, tt := range terminalExecErrorCases {
		t.Run("rejects "+tt.name, func(t *testing.T) {
			if transientExecError(tt.stderr) {
				t.Fatalf("transientExecError(%q) = true, want false", tt.stderr)
			}
		})
	}
}

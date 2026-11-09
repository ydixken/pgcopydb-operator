package podexec

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestNativeComparisonOutput(t *testing.T) {
	if (ComparisonOutput{}).Payload() != nil {
		t.Fatal("zero output carries bytes")
	}
	for _, test := range []struct {
		name string
		edit func(*nativeAPI, *NativeContainerBinding)
		ok   bool
	}{
		{"closed original", func(*nativeAPI, *NativeContainerBinding) {}, true},
		{"classification overflow", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.stdout = []byte(strings.Repeat("x", nativePayloadLimit+1))
		}, false},
		{"failed main", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 3
		}, false},
		{"read failure", func(s *nativeAPI, _ *NativeContainerBinding) { s.logReadError = true }, false},
		{"replaced after read", func(s *nativeAPI, _ *NativeContainerBinding) {
			s.after = func(s *nativeAPI) { s.pod.UID = nativeFixtureReplacement }
		}, false},
		{"wrong execution", func(_ *nativeAPI, w *NativeContainerBinding) { w.ExecutionSHA256[0] ^= 1 }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, want := newNativeOutputAPI(t)
			s.stdout = []byte("{\"engine_exit\":3}\n")
			test.edit(s, &want)
			out, err := newExec(t, s.URL).JobComparisonNative(context.Background(), "ns", nativeFixtureJobUID, want)
			if (err == nil) != test.ok || (out.Payload() != nil) != test.ok {
				t.Fatalf("checked=%v error=%v", out.Payload() != nil, err)
			}
			if test.ok {
				raw := out.Payload()
				if !bytes.Equal(raw, s.stdout) {
					t.Fatal("capture changed classification")
				}
				raw[0] = 'x'
				if bytes.Equal(raw, out.Payload()) {
					t.Fatal("payload aliases capture")
				}
			}
		})
	}
}

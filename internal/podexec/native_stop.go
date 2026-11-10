package podexec

import (
	"context"
	"reflect"
)

// The prelude can still be PID1 when exec starts; caught TERM proves handler installation.
const nativeTermProgram = `import os, signal, sys, time
deadline = time.monotonic() + 25.0
bit = 1 << (signal.SIGTERM - 1)
try:
    while time.monotonic() < deadline:
        ready = False
        if os.readlink("/proc/1/exe") == "/usr/local/bin/rpgmigrate":
            with open("/proc/1/status", encoding="ascii") as status:
                caught = [line.split(":", 1)[1].strip() for line in status if line.startswith("SigCgt:")]
            ready = len(caught) == 1 and bool(int(caught[0], 16) & bit)
        if ready:
            if os.readlink("/proc/1/exe") != "/usr/local/bin/rpgmigrate":
                sys.exit(1)
            os.kill(1, signal.SIGTERM)
            print("term-requested", flush=True)
            sys.exit(0)
        time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
except (OSError, ValueError, UnicodeError):
    pass
sys.exit(1)
`

// RequestNativeTerm returns request data only; an independent observer must prove quiescence.
func (e *Exec) RequestNativeTerm(ctx context.Context, namespace, pod string, want NativeContainerBinding) error {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
	before, err := e.nativeIncarnation(ctx, namespace, pod, want)
	if err != nil || want.ExecutionSHA256 == ([32]byte{}) || NativeExecutionSHA256(before.job.Spec.Template) != want.ExecutionSHA256 ||
		!reflect.DeepEqual(before.job.Spec.Template.Spec.Containers, before.pod.Spec.Containers) {
		return errNativeTransport
	}
	out, err := e.InPodNative(ctx, namespace, pod, want, []string{"/usr/bin/python3", "-I", "-B", "-c", nativeTermProgram}, nil)
	if err != nil || !out.Successful() || string(out.Payload()) != "term-requested\n" {
		return errNativeTransport
	}
	return nil
}

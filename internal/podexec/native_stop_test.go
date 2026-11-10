package podexec

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNativeTermRequest(t *testing.T) {
	t.Run("fixed isolated argv and no input", func(t *testing.T) {
		s, binding := newNativeAPI(t, false)
		binding.ExecutionSHA256 = NativeExecutionSHA256(s.job.Spec.Template)
		s.stdout = []byte("term-requested\n")
		var argv []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/exec") {
				argv = append([]string(nil), r.URL.Query()["command"]...)
			}
			s.serve(w, r)
		}))
		t.Cleanup(server.Close)
		if err := newExec(t, server.URL).RequestNativeTerm(context.Background(), "ns", nativeFixturePodName, binding); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(argv, []string{"/usr/bin/python3", "-I", "-B", "-c", nativeTermProgram}) || len(s.input) != 0 || s.effects != 1 {
			t.Fatal("TERM request changed its fixed command or sent input")
		}
	})
	for _, fault := range []string{"lost reply", "timeout", nativeFixtureReplacement, "unexpected reply"} {
		t.Run(fault, func(t *testing.T) {
			s, binding := newNativeAPI(t, false)
			binding.ExecutionSHA256 = NativeExecutionSHA256(s.job.Spec.Template)
			s.stdout = []byte("term-requested\n")
			e := newExec(t, s.URL)
			switch fault {
			case "lost reply":
				s.closeCode = websocket.CloseAbnormalClosure
			case "timeout":
				s.hold = true
				e.timeout = 30 * time.Millisecond
			case nativeFixtureReplacement:
				s.after = func(s *nativeAPI) { s.pod.UID = nativeFixtureReplacement }
			case "unexpected reply":
				s.stdout = []byte("stopped\n")
			}
			if err := e.RequestNativeTerm(context.Background(), "ns", nativeFixturePodName, binding); err == nil {
				t.Fatal("unknown request effect returned a successful request response")
			}
		})
	}
	for _, fault := range []string{"missing TERM execution hash", "changed TERM command", "pod image"} {
		t.Run(fault, func(t *testing.T) {
			s, binding := newNativeAPI(t, false)
			binding.ExecutionSHA256 = NativeExecutionSHA256(s.job.Spec.Template)
			s.stdout = []byte("term-requested\n")
			switch fault {
			case "missing TERM execution hash":
				binding.ExecutionSHA256 = [32]byte{}
			case "changed TERM command":
				s.job.Spec.Template.Spec.Containers[0].Command = []string{"other"}
			case "pod image":
				s.pod.Spec.Containers[0].Image = nativeFixtureWrong
			}
			if err := newExec(t, s.URL).RequestNativeTerm(context.Background(), "ns", nativeFixturePodName, binding); err == nil || s.effects != 0 {
				t.Fatal("changed execution reached TERM request")
			}
		})
	}
}

func TestNativeStopStartupRace(t *testing.T) {
	for _, scenario := range []string{"ready", "shell then native", "handler late", "shell", "wrong native", "no handler", "identity changes", "proc failure"} {
		t.Run(scenario, func(t *testing.T) {
			program, err := json.Marshal(nativeTermProgram)
			if err != nil {
				t.Fatal(err)
			}
			harness := `import builtins, contextlib, io, json, os, signal, sys, time
scenario = sys.argv[1]
now = [0.0]
reads = [0]
kills = []
def monotonic():
    return now[0]
def sleep(seconds):
    assert 0 < seconds <= 0.1
    now[0] += seconds
def readlink(path):
    assert path == "/proc/1/exe"
    reads[0] += 1
    if scenario == "proc failure":
        raise OSError("private diagnostic")
    if scenario == "shell" or scenario == "shell then native" and now[0] < 0.3:
        return "/bin/sh"
    if scenario == "wrong native" or scenario == "identity changes" and reads[0] > 1:
        return "/usr/local/bin/other"
    return "/usr/local/bin/rpgmigrate"
def read_status(path, *args, **kwargs):
    assert path == "/proc/1/status"
    caught = scenario != "no handler" and (scenario != "handler late" or now[0] >= 0.3)
    return io.StringIO("SigCgt:\t" + ("0000000000004000" if caught else "0000000000000000") + "\n")
def kill(pid, sig):
    assert pid == 1 and sig == signal.SIGTERM
    kills.append((pid, sig))
os.readlink = readlink
os.kill = kill
time.monotonic = monotonic
time.sleep = sleep
builtins.open = read_status
out, diag = io.StringIO(), io.StringIO()
refused = False
with contextlib.redirect_stdout(out), contextlib.redirect_stderr(diag):
    try:
        exec(json.loads(sys.argv[2]), {})
    except SystemExit as error:
        refused = error.code != 0
assert not diag.getvalue(), "script leaked diagnostics"
allowed = scenario in ("ready", "shell then native", "handler late")
assert len(kills) == int(allowed), (scenario, kills)
assert refused != allowed, (scenario, refused)
assert out.getvalue() == ("term-requested\n" if allowed else "")
assert now[0] <= 25.1
if scenario in ("shell then native", "handler late"):
    assert now[0] >= 0.3
`
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, "python3", "-I", "-B", "-c", harness, scenario, string(program)).CombinedOutput()
			if err != nil {
				t.Fatalf("fixed TERM program failed %s: %v: %s", scenario, err, out)
			}
		})
	}
}

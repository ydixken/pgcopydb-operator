package native

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Loads every routed writer once and checks each request and result against each of them.
const routingDriver = `import sys,json,base64
cases=json.load(sys.stdin);writers=[]
for c in cases:
 before=set(sys.modules);g={'__name__':'_routing_loader'};sys.argv=['-']
 try:exec(base64.b64decode(c['program']),g)
 except SystemExit as e:assert e.code==1
 new=[sys.modules[n] for n in set(sys.modules)-before if n.startswith('_rpgmigrate_native')]
 assert len(new)<=1;writers.append(new[0].__dict__ if new else g)
def verdict(f,*a):
 try:return repr(f(*a) is not False)
 except ValueError:return 'refused'
out=[]
for c in cases:
 b,r=base64.b64decode(c['body']),base64.b64decode(c['result'])
 out.append([[verdict(w['strict_body'],b,c['op']),verdict(w['result_envelope'],r,b,c['op'])] for w in writers])
print(json.dumps(out))
`

type routedRequest struct {
	Kind    string `json:"kind"`
	Program []byte `json:"program"`
	Body    []byte `json:"body"`
	Result  []byte `json:"result"`
	Op      string `json:"op"`
}

// TestNativeRequestWriterRouting drives every relayed read through its Reader and checks the writer it was sent
// through: each request and result is admitted by its own writer and refused by every other one.
func TestNativeRequestWriterRouting(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter absent")
	}
	ctx := context.Background()
	reads := []struct {
		kind string
		read func(t *testing.T) (*readerAPI, string, error)
	}{
		{completedEvidenceKind, func(t *testing.T) (*readerAPI, string, error) {
			api, r := newReaderAPI(t)
			_, err := r.ReadCompleted(ctx, api.fixture.want, fixtureEndLSN)
			return api, api.fixture.want.OperationID, err
		}},
		{targetRetryKind, func(t *testing.T) (*readerAPI, string, error) {
			in := RetryInputs{CommitTest: true}
			api, r, w := newRetryReaderAPI(t, in)
			_, err := r.ReadRetryReadiness(ctx, w, in)
			return api, w.OperationID, err
		}},
		{ownedInventoryKind, func(t *testing.T) (*readerAPI, string, error) {
			api, r, w := newOwnedReaderAPI(t)
			_, err := r.ReadOwnedState(ctx, w)
			return api, w.OperationID, err
		}},
		{cleanedCompletionKind, func(t *testing.T) (*readerAPI, string, error) {
			api, r, w := newCleanedReaderAPI(t)
			_, err := r.ReadCleanedCompleted(ctx, w, fixtureEndLSN)
			return api, w.OperationID, err
		}},
		{cleanupRejoinCompletionKind, func(t *testing.T) (*readerAPI, string, error) {
			api, r, w := newCleanupRejoinReaderAPI(t)
			_, err := r.ReadCleanupRejoinCompleted(ctx, w, fixtureEndLSN)
			return api, w.OperationID, err
		}},
	}
	routed := make([]routedRequest, 0, len(reads))
	for _, read := range reads {
		api, op, err := read.read(t)
		if err != nil || api.writerCalls != 1 {
			t.Fatalf("%s: checked read failed: %v", read.kind, err)
		}
		argv := api.relayArgv
		if len(argv) != 11 || argv[8] != "-c" || argv[10] != op || !bytes.Contains(api.input, []byte(`"kind":"`+read.kind+`"`)) {
			t.Fatalf("%s: relay argv or request kind changed: %d arguments", read.kind, len(argv))
		}
		routed = append(routed, routedRequest{read.kind, []byte(argv[9]), api.input, api.result, op})
	}
	input, err := json.Marshal(routed)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(runCtx, python, "-I", "-B", "-c", routingDriver)
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("routing driver failed: %v", err)
	}
	var verdicts [][][2]string
	if err := json.Unmarshal(output, &verdicts); err != nil || len(verdicts) != len(routed) {
		t.Fatalf("routing driver output unreadable: %s", output)
	}
	for i, request := range routed {
		for j, writer := range routed {
			want := [2]string{"refused", "refused"}
			if i == j {
				want = [2]string{"True", "True"}
			}
			if len(verdicts[i]) != len(routed) || verdicts[i][j] != want {
				t.Errorf("%s request through the %s writer: got %v, want %v", request.Kind, writer.Kind, verdicts[i], want)
			}
		}
	}
}

func TestNativeRetryRelayPolicy(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("required Python interpreter absent")
	}
	w, in := retryWant(), RetryInputs{CommitTest: true, ExpectLocal: true}
	body, err := retryRequest(w, in)
	if err != nil {
		t.Fatal(err)
	}
	script := `import sys,base64
sys.argv=['-']
try:exec(base64.b64decode(RETRY_PROGRAM_B64),{'__name__':'_retry_loader'})
except SystemExit as e:assert e.code==1
m=sys.modules['_rpgmigrate_native_retry_writer']
b=base64.b64decode(RETRY_REQUEST_B64);r=base64.b64decode(RETRY_RESULT_B64);o=RETRY_OPERATION_ID
assert m.strict_body(b,o)['kind']=='target_retry'
assert m.result_envelope(r,b,o) is True
for value in (b.replace(b'"retry_gate"',b'"worker"'),b.replace(b'"version":1',b'"version":true'),b.replace(b'"version":1',b'"version":1,"version":1'),b.replace(b'"version":1',b'"version":1,"saved_e":"0/20"'),b.replace(b'"commit_test":true',b'"commit_test":1')):
 assert value!=b
 try:m.strict_body(value,o)
 except ValueError:pass
 else:raise AssertionError('retry request policy admitted invalid bytes')
for value in (r.replace(b'"census_known":true',b'"census_known":false'),r.replace(b'"target_closed":true',b'"target_closed":false'),r.replace(b'"total":0',b'"total":-1'),r.replace(b'"total":0',b'"total":false'),r.replace(b'"version":1',b'"version":1,"checkpoint":{}'),r.replace(b'"local"',b'"maybe"'),r+r):
 assert value!=r
 try:m.result_envelope(value,b,o)
 except ValueError:pass
 else:raise AssertionError('retry result policy admitted invalid bytes')
`
	for key, value := range map[string]string{"RETRY_PROGRAM_B64": base64.StdEncoding.EncodeToString(RetryRequestWriterProgram()), "RETRY_REQUEST_B64": base64.StdEncoding.EncodeToString(body), "RETRY_RESULT_B64": base64.StdEncoding.EncodeToString(retryResult(t, w, body, in)), "RETRY_OPERATION_ID": w.OperationID} {
		script = strings.ReplaceAll(script, key, strconv.Quote(value))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, python, "-I", "-B", "-c", script).CombinedOutput(); err != nil || len(output) != 0 {
		t.Fatalf("retry relay policy failed: %s", output)
	}
}

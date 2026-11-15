package controller

// The seam harness's cluster: a fake API store the reconciler reads and writes, an HTTP
// endpoint that answers podexec, and podman standing in for kubelet. Every Pod runs the
// Job's own container spec, env and volumes in the qualified runner image.

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/podexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8slabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	seamsOwnerKey     = "io.rpgmigrate.owner"
	seamsPodKey       = "io.rpgmigrate.seams.pod"
	seamsNode         = "seams-node"
	seamsNamespace    = "seams"
	seamsGroup        = 65532
	seamsDNSDomain    = ".dns.podman"
	seamsSecretPrefix = "..2026_11_15_00_00_00.000000001"
	seamsExitError    = "Error"
	seamsListVersion  = "seams-snapshot"
	seamsRunning      = "running"
	// nativeCloneCommandArg marks the worker, which gets the larger cap.
	nativeCloneCommandArg = "clone"
	seamsLabelFlag        = "--label"
	seamsNetworkFlag      = "--network"
	seamsRunWord          = "run"
	seamsExecWord         = "exec"
	seamsPodsWord         = "pods"
	seamsContainerWord    = "container"
	seamsBackoffReason    = "BackoffLimitExceeded"
)

// seamsCluster owns every podman object it creates under one owner label.
type seamsCluster struct {
	t        *testing.T
	owner    string
	run      string
	network  string
	initRef  string // a root-capable image for volume setup
	scheme   *runtime.Scheme
	store    client.WithWatch
	api      client.Client
	reader   client.Reader
	exec     *podexec.Exec
	private  string
	mu       sync.Mutex
	pods     map[types.UID]*seamsPod
	claims   map[string]string
	execs    []seamsExecRecord
	failures []string
	redact   []string
	volumes  []string
	stopSync chan struct{}
	synced   sync.WaitGroup
}

type seamsPod struct {
	key       client.ObjectKey
	uid       types.UID
	job       client.ObjectKey
	container string
	id        string
	volumes   []string
	finished  bool
}

// seamsExecRecord is one exec through the podexec endpoint, kept for verdict diagnostics.
type seamsExecRecord struct {
	Pod      string `json:"pod"`
	Program  string `json:"program"`
	Stdin    int    `json:"stdin_bytes"`
	Exit     int    `json:"exit"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	Duration string `json:"duration"`
}

func seamsRandom(n int) string {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return hex.EncodeToString(raw)
}

func seamsUID() types.UID {
	h := seamsRandom(16)
	return types.UID(h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:])
}

func seamsPodman(ctx context.Context, stdin []byte, args ...string) ([]byte, []byte, error) {
	command := exec.CommandContext(ctx, "podman", args...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if stdin != nil {
		command.Stdin = bytes.NewReader(stdin)
	}
	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func newSeamsCluster(t *testing.T, owner, initRef string) *seamsCluster {
	t.Helper()
	c := &seamsCluster{t: t, owner: owner, run: seamsRandom(4), initRef: initRef, pods: map[types.UID]*seamsPod{}, claims: map[string]string{}, stopSync: make(chan struct{})}
	c.private = t.TempDir()
	c.scheme = runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(c.scheme); err != nil {
			t.Fatal(err)
		}
	}
	c.store = clientfake.NewClientBuilder().WithScheme(c.scheme).WithStatusSubresource(&v1beta1.Migration{}, &batchv1.Job{}, &corev1.Pod{}, &corev1.PersistentVolumeClaim{}).Build()
	c.api = interceptor.NewClient(c.store, interceptor.Funcs{Create: c.create, Delete: c.delete})
	c.reader = seamsReader{c.store}
	c.network = "seams-" + c.run
	ctx := context.Background()
	if _, stderr, err := seamsPodman(ctx, nil, "network", "create", seamsLabelFlag, seamsOwnerKey+"="+owner, c.network); err != nil {
		t.Fatalf("podman network: %v %s", err, stderr)
	}
	t.Cleanup(c.close)
	server := httptest.NewServer(http.HandlerFunc(c.serve))
	t.Cleanup(server.Close)
	transport, err := podexec.New(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	c.exec = transport
	c.synced.Add(1)
	go c.syncLoop()
	return c
}

// seamsReader stamps a list snapshot version, which the fake store leaves empty and a real API server never does.
type seamsReader struct{ client.Reader }

func (r seamsReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	if accessor, ok := list.(metav1.ListInterface); ok && accessor.GetResourceVersion() == "" {
		accessor.SetResourceVersion(seamsListVersion)
	}
	return nil
}

func (c *seamsCluster) close() {
	close(c.stopSync)
	c.synced.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c.mu.Lock()
	pods := make([]*seamsPod, 0, len(c.pods))
	for _, p := range c.pods {
		pods = append(pods, p)
	}
	volumes := append([]string(nil), c.volumes...)
	c.mu.Unlock()
	for _, p := range pods {
		c.removeContainer(ctx, p)
	}
	for _, v := range volumes {
		_, _, _ = seamsPodman(ctx, nil, "volume", "rm", "-f", v)
	}
	if _, stderr, err := seamsPodman(ctx, nil, "network", "rm", "-f", c.network); err != nil {
		c.t.Logf("podman network removal: %v %s", err, stderr)
	}
}

func (c *seamsCluster) removeContainer(ctx context.Context, p *seamsPod) {
	if p.id != "" {
		_, _, _ = seamsPodman(ctx, nil, "rm", "-f", "-t", "10", p.id)
	}
	for _, v := range p.volumes {
		_, _, _ = seamsPodman(ctx, nil, "volume", "rm", "-f", v)
	}
	c.mu.Lock()
	c.volumes = slices.DeleteFunc(c.volumes, func(v string) bool { return slices.Contains(p.volumes, v) })
	c.mu.Unlock()
}

// seamsTrace logs each API write and podexec request when NATIVE_SEAMS_TRACE is set.
var seamsTrace = os.Getenv("NATIVE_SEAMS_TRACE") != ""

func (c *seamsCluster) tracef(format string, args ...any) {
	if seamsTrace {
		c.t.Logf("trace "+format, args...)
	}
}

func (c *seamsCluster) create(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	c.tracef("create %T %s", obj, obj.GetName())
	if obj.GetUID() == "" {
		obj.SetUID(seamsUID())
	}
	obj.SetGeneration(1)
	obj.SetCreationTimestamp(metav1.Now())
	switch o := obj.(type) {
	case *batchv1.Job:
		seamsJobDefaults(o)
		if err := w.Create(ctx, o, opts...); err != nil {
			return err
		}
		return c.startJob(ctx, o)
	case *corev1.PersistentVolumeClaim:
		if err := w.Create(ctx, o, opts...); err != nil {
			return err
		}
		return c.bindClaim(ctx, o)
	}
	return w.Create(ctx, obj, opts...)
}

// seamsJobDefaults is what the API server adds to a created Job: the fields nativeWorkerDefaults
// normalizes, the generated selector and the controller labels. It leaves hostUsers unset, as kind does.
func seamsJobDefaults(job *batchv1.Job) {
	defaultProbeJob(job)
	job.Spec.Template.Spec.HostUsers = nil
	if job.Spec.Parallelism == nil {
		job.Spec.Parallelism = ptr.To(int32(1))
	}
	if job.Spec.Completions == nil {
		job.Spec.Completions = ptr.To(int32(1))
	}
}

// delete applies the API server's UID precondition and the garbage collector's Pod removal.
func (c *seamsCluster) delete(ctx context.Context, w client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	options := &client.DeleteOptions{}
	options.ApplyOptions(opts)
	c.tracef("delete %T %s", obj, obj.GetName())
	if options.Preconditions != nil && options.Preconditions.UID != nil {
		live := obj.DeepCopyObject().(client.Object)
		if err := w.Get(ctx, client.ObjectKeyFromObject(obj), live); err != nil {
			return err
		}
		if live.GetUID() != *options.Preconditions.UID {
			return apierrors.NewConflict(schema.GroupResource{Resource: fmt.Sprintf("%T", obj)}, obj.GetName(), errors.New("uid precondition"))
		}
	}
	if err := w.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return nil
	}
	c.mu.Lock()
	var owned []*seamsPod
	for uid, p := range c.pods {
		if p.job == client.ObjectKeyFromObject(job) {
			owned = append(owned, p)
			delete(c.pods, uid)
		}
	}
	c.mu.Unlock()
	for _, p := range owned {
		// A last sync records an exit the periodic one has not seen, with its log, before the container goes.
		if !p.finished && p.id != "" {
			if err := c.syncPods(ctx, []*seamsPod{p}); err != nil {
				c.t.Logf("seam kubelet sync before removal: %v", err)
			}
		}
		c.removeContainer(ctx, p)
		pod := &corev1.Pod{}
		if err := c.store.Get(ctx, p.key, pod); err == nil {
			if err := c.store.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

// bindClaim binds the work claim to a private volume shaped like the kind suite's work
// directory: owned by uid 65532 with mode 0700 (docs/operations/e2e-kind.md).
func (c *seamsCluster) bindClaim(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	volume := fmt.Sprintf("seams-%s-claim-%s", c.run, seamsRandom(3))
	if err := c.createVolumes(ctx, []seamsVolume{{name: volume, root: seamsEntry{uid: seamsGroup, gid: seamsGroup, mode: 0o700}}}); err != nil {
		return err
	}
	c.mu.Lock()
	c.claims[pvc.Name] = volume
	c.mu.Unlock()
	live := &corev1.PersistentVolumeClaim{}
	if err := c.store.Get(ctx, client.ObjectKeyFromObject(pvc), live); err != nil {
		return err
	}
	live.Spec.VolumeName = "pv-" + string(pvc.UID)
	if err := c.store.Update(ctx, live); err != nil {
		return err
	}
	live.Status.Phase = corev1.ClaimBound
	live.Status.AccessModes = live.Spec.AccessModes
	live.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: live.Spec.Resources.Requests[corev1.ResourceStorage]}
	if err := c.store.Status().Update(ctx, live); err != nil {
		return err
	}
	live.DeepCopyInto(pvc)
	return nil
}

type seamsEntry struct {
	uid, gid int
	mode     int64
}

type seamsFile struct {
	path, link string
	data       []byte
	seamsEntry
}

type seamsVolume struct {
	name  string
	root  seamsEntry
	files []seamsFile
}

// createVolumes makes labelled volumes and fills them through one root init container,
// so file content travels on stdin and never in an argv.
func (c *seamsCluster) createVolumes(ctx context.Context, volumes []seamsVolume) error {
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	args := []string{seamsRunWord, "--rm", "-i", seamsNetworkFlag, "none", seamsLabelFlag, seamsOwnerKey + "=" + c.owner, "--user", "0:0", "--entrypoint", "tar"}
	for i, v := range volumes {
		if _, stderr, err := seamsPodman(ctx, nil, "volume", "create", seamsLabelFlag, seamsOwnerKey+"="+c.owner, v.name); err != nil {
			return fmt.Errorf("volume %s: %v %s", v.name, err, stderr)
		}
		c.mu.Lock()
		c.volumes = append(c.volumes, v.name)
		c.mu.Unlock()
		base := fmt.Sprintf("v%d", i)
		args = append(args, "-v", v.name+":/seams/"+base)
		entries := append([]seamsFile{{path: "", seamsEntry: v.root}}, v.files...)
		for _, f := range entries {
			name := strings.TrimSuffix(base+"/"+f.path, "/")
			h := &tar.Header{Name: name, Uid: f.uid, Gid: f.gid, Mode: f.mode, ModTime: time.Unix(0, 0)}
			switch {
			case f.link != "":
				h.Typeflag, h.Linkname, h.Mode = tar.TypeSymlink, f.link, 0o777
			case f.data == nil:
				h.Typeflag = tar.TypeDir
			default:
				h.Typeflag, h.Size = tar.TypeReg, int64(len(f.data))
			}
			if err := tw.WriteHeader(h); err != nil {
				return err
			}
			if h.Typeflag == tar.TypeReg {
				if _, err := tw.Write(f.data); err != nil {
					return err
				}
			}
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	args = append(args, c.initRef, "-x", "-p", "--same-owner", "--numeric-owner", "-C", "/seams", "-f", "-")
	if _, stderr, err := seamsPodman(ctx, archive.Bytes(), args...); err != nil {
		return fmt.Errorf("volume setup: %v %s", err, c.scrub(string(stderr)))
	}
	return nil
}

func (c *seamsCluster) scrub(text string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, secret := range c.redact {
		text = strings.ReplaceAll(text, secret, "[redacted]")
	}
	return text
}

// startJob is the Job controller and kubelet in one: it creates the Job's single Pod and runs it.
func (c *seamsCluster) startJob(ctx context.Context, job *batchv1.Job) error {
	pod := probePod(job)
	pod.Name, pod.GenerateName, pod.UID = job.Name+"-"+seamsRandom(3)[:5], job.Name+"-", seamsUID()
	pod.Spec.NodeName = seamsNode
	pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
	if err := c.store.Create(ctx, pod); err != nil {
		return err
	}
	args, volumes, err := c.podmanArgs(ctx, pod)
	if err != nil {
		return err
	}
	p := &seamsPod{key: client.ObjectKeyFromObject(pod), uid: pod.UID, job: client.ObjectKeyFromObject(job), container: args[slices.Index(args, "--name")+1], volumes: volumes}
	c.mu.Lock()
	c.pods[pod.UID] = p
	c.mu.Unlock()
	stdout, stderr, err := seamsPodman(ctx, nil, args...)
	if err != nil {
		return fmt.Errorf("podman run %s: %v %s", pod.Name, err, c.scrub(string(stderr)))
	}
	c.mu.Lock()
	p.id = strings.TrimSpace(string(stdout))
	c.mu.Unlock()
	return c.syncPods(ctx, []*seamsPod{p})
}

// podmanArgs renders the Pod the way kubelet runs it: securityContext, $(VAR) expansion, the
// kubernetes service variables, PVC, emptyDir and atomic-writer Secret volumes, and caps.
func (c *seamsCluster) podmanArgs(ctx context.Context, pod *corev1.Pod) ([]string, []string, error) {
	spec := &pod.Spec
	if len(spec.Containers) != 1 || len(spec.InitContainers) != 0 {
		return nil, nil, fmt.Errorf("seam kubelet runs one container, got %d", len(spec.Containers))
	}
	ct := &spec.Containers[0]
	env, err := c.podEnv(ctx, pod, ct)
	if err != nil {
		return nil, nil, err
	}
	name := fmt.Sprintf("seams-%s-%s", c.run, pod.Name)
	if len(name) > 120 {
		name = name[:120]
	}
	args := []string{seamsRunWord, "-d", "--name", name, seamsLabelFlag, seamsOwnerKey + "=" + c.owner, seamsLabelFlag, seamsPodKey + "=" + string(pod.UID),
		seamsNetworkFlag, c.network, "--hostname", pod.Name[:min(len(pod.Name), 63)], "--log-driver", "k8s-file", "--pids-limit", "512",
		"--unsetenv", seamsContainerWord, "--stop-timeout", "30"}
	for _, search := range []string{pod.Namespace + ".svc.cluster.local", "svc.cluster.local", "cluster.local"} {
		args = append(args, "--dns-search", search)
	}
	cpus, memory := "1", "1g"
	if len(ct.Args) > 0 && ct.Args[0] == nativeCloneCommandArg {
		cpus, memory = "2", "3g"
	}
	args = append(args, "--cpus", cpus, "--memory", memory, "--memory-swap", memory)
	psc, sc := spec.SecurityContext, ct.SecurityContext
	if psc != nil && psc.RunAsUser != nil {
		group := *psc.RunAsUser
		if psc.RunAsGroup != nil {
			group = *psc.RunAsGroup
		}
		args = append(args, "--user", fmt.Sprintf("%d:%d", *psc.RunAsUser, group))
	}
	fsGroup := -1
	if psc != nil && psc.FSGroup != nil {
		fsGroup = int(*psc.FSGroup)
		args = append(args, "--group-add", strconv.Itoa(fsGroup))
	}
	if sc != nil {
		if sc.ReadOnlyRootFilesystem != nil && *sc.ReadOnlyRootFilesystem {
			args = append(args, "--read-only", "--read-only-tmpfs=false")
		}
		if sc.Capabilities != nil {
			for _, drop := range sc.Capabilities.Drop {
				args = append(args, "--cap-drop", string(drop))
			}
		}
		if sc.AllowPrivilegeEscalation != nil && !*sc.AllowPrivilegeEscalation {
			args = append(args, "--security-opt", "no-new-privileges")
		}
	}
	mounts, volumes, err := c.podVolumes(ctx, pod, ct, fsGroup)
	if err != nil {
		return nil, nil, err
	}
	args = append(args, mounts...)
	envFile := filepath.Join(c.private, pod.Name+".env")
	var lines strings.Builder
	for _, e := range env {
		if strings.ContainsAny(e[1], "\n\r\x00") {
			return nil, nil, fmt.Errorf("env %s cannot travel in an env file", e[0])
		}
		lines.WriteString(e[0] + "=" + e[1] + "\n")
	}
	if err := os.WriteFile(envFile, []byte(lines.String()), 0o600); err != nil {
		return nil, nil, err
	}
	args = append(args, "--env-file", envFile)
	if ct.WorkingDir != "" {
		args = append(args, "--workdir", ct.WorkingDir)
	}
	values := map[string]string{}
	for _, e := range env {
		values[e[0]] = e[1]
	}
	expand := func(in []string) []string {
		out := make([]string, len(in))
		for i, s := range in {
			out[i] = seamsExpand(s, values)
		}
		return out
	}
	command := expand(ct.Command)
	if len(command) > 0 {
		raw, err := json.Marshal(command)
		if err != nil {
			return nil, nil, err
		}
		args = append(args, "--entrypoint", string(raw))
	}
	args = append(args, ct.Image)
	return append(args, expand(ct.Args)...), volumes, nil
}

// podEnv is kubelet's environment: the default kubernetes service variables, then the
// container's own variables in order, each expanded against what precedes it.
func (c *seamsCluster) podEnv(ctx context.Context, pod *corev1.Pod, ct *corev1.Container) ([][2]string, error) {
	env := [][2]string{{"KUBERNETES_SERVICE_HOST", "10.96.0.1"}, {"KUBERNETES_SERVICE_PORT", "443"}, {"KUBERNETES_SERVICE_PORT_HTTPS", "443"},
		{"KUBERNETES_PORT", "tcp://10.96.0.1:443"}, {"KUBERNETES_PORT_443_TCP", "tcp://10.96.0.1:443"}, {"KUBERNETES_PORT_443_TCP_PROTO", "tcp"},
		{"KUBERNETES_PORT_443_TCP_PORT", "443"}, {"KUBERNETES_PORT_443_TCP_ADDR", "10.96.0.1"}}
	values := map[string]string{}
	for _, e := range env {
		values[e[0]] = e[1]
	}
	if len(ct.EnvFrom) != 0 {
		return nil, fmt.Errorf("seam kubelet has no envFrom")
	}
	for _, e := range ct.Env {
		value := seamsExpand(e.Value, values)
		if e.ValueFrom != nil {
			ref := e.ValueFrom.SecretKeyRef
			if ref == nil {
				return nil, fmt.Errorf("seam kubelet resolves only secretKeyRef, not %s", e.Name)
			}
			secret := &corev1.Secret{}
			if err := c.store.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: ref.Name}, secret); err != nil {
				return nil, fmt.Errorf("env %s: secret %s: %w", e.Name, ref.Name, err)
			}
			raw, ok := secret.Data[ref.Key]
			if !ok {
				return nil, fmt.Errorf("env %s: key %s absent", e.Name, ref.Key)
			}
			value = string(raw)
		}
		values[e.Name] = value
		if i := slices.IndexFunc(env, func(x [2]string) bool { return x[0] == e.Name }); i >= 0 {
			env[i][1] = value
			continue
		}
		env = append(env, [2]string{e.Name, value})
	}
	return env, nil
}

// seamsExpand is kubelet's $(VAR) expansion, ported from
// k8s.io/kubernetes third_party/forked/golang/expansion (v1.34): $$ escapes, unknown references stay.
func seamsExpand(input string, values map[string]string) string {
	var buf strings.Builder
	checkpoint := 0
	for cursor := 0; cursor < len(input); cursor++ {
		if input[cursor] != '$' || cursor+1 >= len(input) {
			continue
		}
		buf.WriteString(input[checkpoint:cursor])
		tail := input[cursor+1:]
		advance := 1
		switch tail[0] {
		case '$':
			buf.WriteByte('$')
		case '(':
			if end := strings.IndexByte(tail, ')'); end > 0 {
				name := tail[1:end]
				if value, ok := values[name]; ok {
					buf.WriteString(value)
				} else {
					buf.WriteString("$(" + name + ")")
				}
				advance = end + 1
			} else {
				buf.WriteString("$(")
			}
		default:
			buf.WriteString("$" + tail[:1])
		}
		cursor += advance
		checkpoint = cursor + 1
	}
	return buf.String() + input[checkpoint:]
}

// podVolumes mounts each volume the way kubelet lays it out under fsGroup: a PVC is the bound
// work volume, an emptyDir is a root-owned setgid 0777 directory, and a Secret is atomic-writer
// symlinks into a timestamped directory with group read added.
func (c *seamsCluster) podVolumes(ctx context.Context, pod *corev1.Pod, ct *corev1.Container, fsGroup int) ([]string, []string, error) {
	gid := max(fsGroup, 0)
	var mounts []string
	var fresh []seamsVolume
	byName := map[string]corev1.Volume{}
	for _, v := range pod.Spec.Volumes {
		byName[v.Name] = v
	}
	for _, m := range ct.VolumeMounts {
		v, ok := byName[m.Name]
		if !ok || m.SubPath != "" || m.SubPathExpr != "" {
			return nil, nil, fmt.Errorf("seam kubelet cannot mount %s", m.Name)
		}
		suffix := ""
		if m.ReadOnly {
			suffix = ":ro"
		}
		switch {
		case v.PersistentVolumeClaim != nil:
			c.mu.Lock()
			volume, bound := c.claims[v.PersistentVolumeClaim.ClaimName]
			c.mu.Unlock()
			if !bound {
				return nil, nil, fmt.Errorf("claim %s is not bound", v.PersistentVolumeClaim.ClaimName)
			}
			mounts = append(mounts, "-v", volume+":"+m.MountPath+suffix)
		case v.EmptyDir != nil && v.EmptyDir.Medium == corev1.StorageMediumMemory:
			// kubelet backs a memory emptyDir with tmpfs; podman cannot set its group, so it stays root.
			option := "type=tmpfs,dst=" + m.MountPath + ",tmpfs-mode=2777"
			if limit := v.EmptyDir.SizeLimit; limit != nil {
				option += ",tmpfs-size=" + strconv.FormatInt(limit.Value(), 10)
			}
			mounts = append(mounts, "--mount", option)
		case v.EmptyDir != nil:
			name := fmt.Sprintf("seams-%s-empty-%s", c.run, seamsRandom(4))
			fresh = append(fresh, seamsVolume{name: name, root: seamsEntry{uid: 0, gid: gid, mode: 0o2777}})
			mounts = append(mounts, "-v", name+":"+m.MountPath+suffix)
		case v.Secret != nil:
			volume, err := c.secretVolume(ctx, pod.Namespace, v.Secret, gid)
			if err != nil {
				return nil, nil, err
			}
			fresh = append(fresh, volume)
			mounts = append(mounts, "-v", volume.name+":"+m.MountPath+":ro")
		default:
			return nil, nil, fmt.Errorf("seam kubelet has no %s volume type", m.Name)
		}
	}
	if len(fresh) == 0 {
		return mounts, nil, nil
	}
	if err := c.createVolumes(ctx, fresh); err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(fresh))
	for _, v := range fresh {
		names = append(names, v.name)
	}
	return mounts, names, nil
}

func (c *seamsCluster) secretVolume(ctx context.Context, namespace string, source *corev1.SecretVolumeSource, gid int) (seamsVolume, error) {
	secret := &corev1.Secret{}
	if err := c.store.Get(ctx, client.ObjectKey{Namespace: namespace, Name: source.SecretName}, secret); err != nil {
		return seamsVolume{}, err
	}
	mode := int64(0o644)
	if source.DefaultMode != nil {
		mode = int64(*source.DefaultMode)
	}
	items := source.Items
	if len(items) == 0 {
		for key := range secret.Data {
			items = append(items, corev1.KeyToPath{Key: key, Path: key})
		}
		slices.SortFunc(items, func(a, b corev1.KeyToPath) int { return strings.Compare(a.Key, b.Key) })
	}
	readable := int64(0o440)
	volume := seamsVolume{name: fmt.Sprintf("seams-%s-secret-%s", c.run, seamsRandom(4)), root: seamsEntry{uid: 0, gid: gid, mode: 0o3777}}
	volume.files = append(volume.files, seamsFile{path: seamsSecretPrefix, seamsEntry: seamsEntry{uid: 0, gid: gid, mode: 0o2755}})
	for _, item := range items {
		raw, ok := secret.Data[item.Key]
		if !ok {
			return seamsVolume{}, fmt.Errorf("secret %s key %s absent", source.SecretName, item.Key)
		}
		fileMode := mode
		if item.Mode != nil {
			fileMode = int64(*item.Mode)
		}
		volume.files = append(volume.files,
			seamsFile{path: seamsSecretPrefix + "/" + item.Path, data: raw, seamsEntry: seamsEntry{uid: 0, gid: gid, mode: fileMode | readable}},
			seamsFile{path: item.Path, link: "..data/" + item.Path})
	}
	volume.files = append(volume.files, seamsFile{path: "..data", link: seamsSecretPrefix})
	return volume, nil
}

func (c *seamsCluster) syncLoop() {
	defer c.synced.Done()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopSync:
			return
		case <-ticker.C:
			c.mu.Lock()
			var live []*seamsPod
			for _, p := range c.pods {
				if !p.finished && p.id != "" {
					live = append(live, p)
				}
			}
			c.mu.Unlock()
			if len(live) > 0 {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				if err := c.syncPods(ctx, live); err != nil {
					c.t.Logf("seam kubelet sync: %v", err)
				}
				cancel()
			}
		}
	}
}

type seamsState struct {
	ID    string `json:"Id"`
	State struct {
		Status     string    `json:"Status"`
		ExitCode   int32     `json:"ExitCode"`
		StartedAt  time.Time `json:"StartedAt"`
		FinishedAt time.Time `json:"FinishedAt"`
	} `json:"State"`
	message string
}

// syncPods writes kubelet's Pod status and the Job controller's Job status from podman's view.
func (c *seamsCluster) syncPods(ctx context.Context, pods []*seamsPod) error {
	args := make([]string, 0, 3+len(pods))
	args = append(args, "inspect", "--type", seamsContainerWord)
	for _, p := range pods {
		args = append(args, p.id)
	}
	stdout, stderr, err := seamsPodman(ctx, nil, args...)
	if err != nil {
		if strings.Contains(string(stderr), "no such") {
			return nil
		}
		return fmt.Errorf("inspect: %v %s", err, stderr)
	}
	var states []seamsState
	if err := json.Unmarshal(stdout, &states); err != nil {
		return err
	}
	for _, s := range states {
		for _, p := range pods {
			if p.id == s.ID {
				if err := c.applyState(ctx, p, s); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *seamsCluster) applyState(ctx context.Context, p *seamsPod, s seamsState) error {
	finished := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod := &corev1.Pod{}
		if err := c.store.Get(ctx, p.key, pod); err != nil {
			return client.IgnoreNotFound(err)
		}
		job := &batchv1.Job{}
		if err := c.store.Get(ctx, p.job, job); err != nil {
			return client.IgnoreNotFound(err)
		}
		if s.State.ExitCode != 0 && s.message == "" && pod.Spec.Containers[0].TerminationMessagePolicy == corev1.TerminationMessageFallbackToLogsOnError {
			s.message = c.terminationTail(ctx, p.id)
		}
		var known bool
		finished, known = seamsStatus(pod, job, s)
		if !known {
			return nil
		}
		return c.writeStatus(ctx, pod, job)
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	newly := finished && !p.finished
	p.finished = finished
	c.mu.Unlock()
	if newly && s.State.ExitCode != 0 {
		tail := c.scrub(c.terminationTail(ctx, p.id))
		c.mu.Lock()
		c.failures = append(c.failures, fmt.Sprintf("%s exited %d: %s", p.key.Name, s.State.ExitCode, strings.TrimSpace(tail[max(0, len(tail)-600):])))
		c.mu.Unlock()
	}
	return nil
}

func (c *seamsCluster) failuresSince(n int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.failures[min(n, len(c.failures)):]...)
}

func (c *seamsCluster) failureCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.failures)
}

// terminationTail is kubelet's FallbackToLogsOnError message: the last 80 log lines, at most 2048 bytes.
func (c *seamsCluster) terminationTail(ctx context.Context, id string) string {
	command := exec.CommandContext(ctx, "podman", "logs", "--tail", "80", id)
	var out bytes.Buffer
	command.Stdout, command.Stderr = &out, &out
	if command.Run() != nil {
		return ""
	}
	raw := out.Bytes()
	return string(raw[max(0, len(raw)-2048):])
}

// writeStatus stores only a changed status, so readers comparing two reads see no churn.
func (c *seamsCluster) writeStatus(ctx context.Context, pod *corev1.Pod, job *batchv1.Job) error {
	for _, obj := range []client.Object{pod, job} {
		stored := obj.DeepCopyObject().(client.Object)
		if err := c.store.Get(ctx, client.ObjectKeyFromObject(obj), stored); err != nil {
			return client.IgnoreNotFound(err)
		}
		if equalJSON(stored, obj) {
			continue
		}
		if err := c.store.Status().Update(ctx, obj); err != nil {
			return err
		}
	}
	return nil
}

// seamsStatus is kubelet's Pod status and the Job controller's Job status for one podman state.
func seamsStatus(pod *corev1.Pod, job *batchv1.Job, s seamsState) (finished, known bool) {
	id := "containerd://" + s.ID
	started := metav1.NewTime(s.State.StartedAt.Truncate(time.Second))
	status := corev1.ContainerStatus{Name: workerContainer, ContainerID: id, Image: pod.Spec.Containers[0].Image, ImageID: pod.Spec.Containers[0].Image}
	start := job.Status.StartTime
	if start == nil {
		start = &started
	}
	switch s.State.Status {
	case seamsRunning:
		status.Ready, status.Started = true, ptr.To(true)
		status.State.Running = &corev1.ContainerStateRunning{StartedAt: started}
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, HostIP: "10.89.0.1", StartTime: &started, ContainerStatuses: []corev1.ContainerStatus{status}}
		job.Status = batchv1.JobStatus{Active: 1, Ready: ptr.To(int32(1)), Terminating: ptr.To(int32(0)), StartTime: start, UncountedTerminatedPods: &batchv1.UncountedTerminatedPods{}}
		return false, true
	case "exited", "stopped":
	default:
		return false, false
	}
	ended := metav1.NewTime(s.State.FinishedAt.Truncate(time.Second))
	reason, phase := "Completed", corev1.PodSucceeded
	if s.State.ExitCode != 0 {
		reason, phase = seamsExitError, corev1.PodFailed
	}
	status.Started = ptr.To(false)
	status.State.Terminated = &corev1.ContainerStateTerminated{ExitCode: s.State.ExitCode, Reason: reason, Message: s.message, StartedAt: started, FinishedAt: ended, ContainerID: id}
	pod.Status = corev1.PodStatus{Phase: phase, HostIP: "10.89.0.1", StartTime: &started, ContainerStatuses: []corev1.ContainerStatus{status}}
	job.Status = batchv1.JobStatus{Ready: ptr.To(int32(0)), Terminating: ptr.To(int32(0)), StartTime: start, UncountedTerminatedPods: &batchv1.UncountedTerminatedPods{}}
	if phase == corev1.PodSucceeded {
		job.Status.Succeeded, job.Status.CompletionTime = 1, &ended
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, Reason: "CompletionsReached"}, {Type: batchv1.JobComplete, Status: corev1.ConditionTrue, Reason: "CompletionsReached"}}
	} else {
		job.Status.Failed = 1
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: seamsBackoffReason}, {Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: seamsBackoffReason}}
	}
	return true, true
}

func equalJSON(a, b any) bool {
	x, errA := json.Marshal(a)
	y, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(x, y)
}

// waitFinished blocks until kubelet saw the Pod's container exit, so a reader never races the sync.
func (c *seamsCluster) podFor(jobName string) *seamsPod {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pods {
		if p.job.Name == jobName {
			return p
		}
	}
	return nil
}

func (c *seamsCluster) podByName(name string) *seamsPod {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pods {
		if p.key.Name == name {
			return p
		}
	}
	return nil
}

// serve answers the podexec calls: Pod and Job reads, Pod logs and the v5 exec channel.
func (c *seamsCluster) serve(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	c.tracef("podexec %s %s?%s", r.Method, r.URL.Path, r.URL.Query().Get("labelSelector"))
	switch {
	case len(parts) == 7 && parts[4] == seamsPodsWord && parts[6] == seamsExecWord:
		c.serveExec(w, r, parts[5])
	case len(parts) == 7 && parts[4] == seamsPodsWord && parts[6] == "log":
		c.serveLog(w, r, parts[5])
	case len(parts) == 5 && parts[4] == seamsPodsWord:
		pods := &corev1.PodList{}
		opts := []client.ListOption{client.InNamespace(parts[3])}
		if selector := r.URL.Query().Get("labelSelector"); selector != "" {
			parsed, err := k8slabels.Parse(selector)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			opts = append(opts, client.MatchingLabelsSelector{Selector: parsed})
		}
		if err := c.reader.List(r.Context(), pods, opts...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		c.writeJSON(w, pods)
	case len(parts) == 6 && parts[4] == seamsPodsWord:
		c.serveObject(w, r, client.ObjectKey{Namespace: parts[3], Name: parts[5]}, &corev1.Pod{})
	case len(parts) == 7 && parts[0] == "apis" && parts[5] == "jobs":
		c.serveObject(w, r, client.ObjectKey{Namespace: parts[4], Name: parts[6]}, &batchv1.Job{})
	default:
		http.NotFound(w, r)
	}
}

func (c *seamsCluster) writeJSON(w http.ResponseWriter, obj runtime.Object) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(obj)
}

func (c *seamsCluster) serveObject(w http.ResponseWriter, r *http.Request, key client.ObjectKey, obj client.Object) {
	if err := c.store.Get(r.Context(), key, obj); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound})
		return
	}
	c.writeJSON(w, obj)
}

func (c *seamsCluster) serveLog(w http.ResponseWriter, r *http.Request, name string) {
	p := c.podByName(name)
	if p == nil || p.id == "" {
		http.NotFound(w, r)
		return
	}
	command := exec.CommandContext(r.Context(), "podman", "logs", p.id)
	var out bytes.Buffer
	command.Stdout, command.Stderr = &out, &out
	if err := command.Run(); err != nil {
		http.Error(w, "logs unavailable", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(out.Bytes())
}

// serveExec runs the exec in the Pod's container: it reads the whole stdin first, as every native
// relay does, then returns stdout, stderr and the API server's v1 Status on channel 3.
func (c *seamsCluster) serveExec(w http.ResponseWriter, r *http.Request, name string) {
	argv := r.URL.Query()["command"]
	upgrade := websocket.Upgrader{Subprotocols: []string{nativeCleanupTestProtocol}}
	ws, err := upgrade.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = ws.Close() }()
	var input []byte
	for {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if bytes.Equal(raw, []byte{255, 0}) {
			break
		}
		if len(raw) == 0 || raw[0] != 0 {
			return
		}
		input = append(input, raw[1:]...)
	}
	p := c.podByName(name)
	c.tracef("exec %s stdin %s", name, c.scrub(string(input)))
	started := time.Now()
	record := seamsExecRecord{Pod: name, Program: seamsProgramName(argv), Stdin: len(input), Exit: -1}
	var stdout, stderr []byte
	if p == nil || p.id == "" {
		stderr = []byte("seam kubelet: pod not running")
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		command := exec.CommandContext(ctx, "podman", append([]string{seamsExecWord, "-i", p.id}, argv...)...)
		var out, errOut bytes.Buffer
		command.Stdin, command.Stdout, command.Stderr = bytes.NewReader(input), &out, &errOut
		runErr := command.Run()
		cancel()
		stdout, stderr = out.Bytes(), errOut.Bytes()
		record.Exit = 0
		if runErr != nil {
			record.Exit = 1
			if exit, ok := runErr.(*exec.ExitError); ok {
				record.Exit = exit.ExitCode()
			}
		}
	}
	record.Stdout, record.Stderr, record.Duration = c.scrub(seamsClip(stdout)), c.scrub(seamsClip(stderr)), time.Since(started).Round(time.Millisecond).String()
	c.mu.Lock()
	c.execs = append(c.execs, record)
	c.mu.Unlock()
	if len(stdout) > 0 {
		_ = ws.WriteMessage(websocket.BinaryMessage, append([]byte{1}, stdout...))
	}
	if len(stderr) > 0 {
		_ = ws.WriteMessage(websocket.BinaryMessage, append([]byte{2}, stderr...))
	}
	status := `{"metadata":{},"status":"Success"}`
	if record.Exit != 0 {
		status = fmt.Sprintf(`{"metadata":{},"status":"Failure","message":"command terminated with non-zero exit code","reason":"NonZeroExitCode","details":{"causes":[{"reason":"ExitCode","message":"%d"}]}}`, record.Exit)
	}
	_ = ws.WriteMessage(websocket.BinaryMessage, append([]byte{3}, status...))
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
}

func seamsClip(raw []byte) string {
	if len(raw) > 2048 {
		return string(raw[:2048]) + "...(clipped)"
	}
	return string(raw)
}

// seamsProgramName names an exec by its first line of program text, never its arguments.
func seamsProgramName(argv []string) string {
	for i, a := range argv {
		if a == "-c" && i+1 < len(argv) {
			first, _, _ := strings.Cut(argv[i+1], "\n")
			return fmt.Sprintf("%s -c %q", argv[max(0, i-3)], first[:min(len(first), 60)])
		}
	}
	if len(argv) == 0 {
		return ""
	}
	return argv[0]
}

func (c *seamsCluster) execsSince(n int) []seamsExecRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]seamsExecRecord(nil), c.execs[min(n, len(c.execs)):]...)
}

func (c *seamsCluster) execCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.execs)
}

// kill sends a signal to a Pod's container the way a node-side kill does.
func (c *seamsCluster) kill(ctx context.Context, jobName, signal string) error {
	p := c.podFor(jobName)
	if p == nil || p.id == "" {
		return fmt.Errorf("no running pod for %s", jobName)
	}
	_, stderr, err := seamsPodman(ctx, nil, "kill", "-s", signal, p.id)
	if err != nil {
		return fmt.Errorf("kill: %v %s", err, stderr)
	}
	return nil
}

// waitJob polls the store until the Job's status satisfies done.
func (c *seamsCluster) waitJob(ctx context.Context, name string, done func(*batchv1.Job) bool) (*batchv1.Job, error) {
	for {
		job := &batchv1.Job{}
		if err := c.store.Get(ctx, client.ObjectKey{Namespace: seamsNamespace, Name: name}, job); err == nil && done(job) {
			return job, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("job %s: %w", name, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// podLogs returns a Pod's complete log for diagnostics.
func (c *seamsCluster) podLogs(ctx context.Context, jobName string) string {
	p := c.podFor(jobName)
	if p == nil || p.id == "" {
		return ""
	}
	command := exec.CommandContext(ctx, "podman", "logs", p.id)
	var out bytes.Buffer
	command.Stdout, command.Stderr = &out, &out
	_ = command.Run()
	return c.scrub(out.String())
}

// The cases follow kubelet's own expansion tests (third_party/forked/golang/expansion).
func TestSeamsExpandMatchesKubelet(t *testing.T) {
	values := map[string]string{"VAR_A": "A", "VAR_EMPTY": ""}
	for in, want := range map[string]string{
		"$(VAR_A)":           "A",
		"___$(VAR_A)___":     "___A___",
		"$(VAR_EMPTY)":       "",
		"$(UNKNOWN)":         "$(UNKNOWN)",
		"$$(VAR_A)":          "$(VAR_A)",
		"$$$(VAR_A)":         "$A",
		"$$":                 "$",
		"$1 and $2":          "$1 and $2",
		"$(VAR_A":            "$(VAR_A",
		"trailing $":         "trailing $",
		"$(sed -e 's/x/y/')": "$(sed -e 's/x/y/')",
	} {
		if got := seamsExpand(in, values); got != want {
			t.Errorf("seamsExpand(%q) = %q, want %q", in, got, want)
		}
	}
}

package podexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"time"

	"github.com/gorilla/websocket"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	wsclient "k8s.io/client-go/transport/websocket"
)

const nativePayloadLimit = 65536

var errNativeTransport = errors.New("native transport refused")

// NativeContainerBinding correlates a caller's independently admitted incarnation.
type NativeContainerBinding struct {
	PodUID          types.UID
	ContainerName   string
	ContainerID     string
	RestartCount    int32
	ImageID         string
	TemplateSHA256  [32]byte
	ExecutionSHA256 [32]byte
}

type nativeTemplateEnv struct {
	Name      string
	ValueFrom *corev1.EnvVarSource
}

type nativeTemplateContainer struct {
	Name  string
	Image string
	Env   []nativeTemplateEnv
}

// NativeTemplateSHA256 correlates structural references; exact admitted content needs independent checks.
func NativeTemplateSHA256(template corev1.PodTemplateSpec) [32]byte {
	project := func(name, image string, env []corev1.EnvVar) nativeTemplateContainer {
		out := nativeTemplateContainer{Name: name, Image: image, Env: make([]nativeTemplateEnv, len(env))}
		for i, value := range env {
			out.Env[i] = nativeTemplateEnv{Name: value.Name, ValueFrom: value.ValueFrom.DeepCopy()}
		}
		return out
	}
	shape := struct {
		RestartPolicy                                   corev1.RestartPolicy
		ServiceAccountName                              string
		AutomountServiceAccountToken                    *bool
		ImagePullSecrets                                []string
		Containers, InitContainers, EphemeralContainers []nativeTemplateContainer
	}{RestartPolicy: template.Spec.RestartPolicy, ServiceAccountName: template.Spec.ServiceAccountName, AutomountServiceAccountToken: template.Spec.AutomountServiceAccountToken}
	for _, ref := range template.Spec.ImagePullSecrets {
		shape.ImagePullSecrets = append(shape.ImagePullSecrets, ref.Name)
	}
	for _, c := range template.Spec.Containers {
		shape.Containers = append(shape.Containers, project(c.Name, c.Image, c.Env))
	}
	for _, c := range template.Spec.InitContainers {
		shape.InitContainers = append(shape.InitContainers, project(c.Name, c.Image, c.Env))
	}
	for _, c := range template.Spec.EphemeralContainers {
		shape.EphemeralContainers = append(shape.EphemeralContainers, project(c.Name, c.Image, c.Env))
	}
	raw, err := json.Marshal(shape)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(raw)
}

// NativeOutcome cannot establish native completion authority on its own.
type NativeOutcome struct {
	payload    []byte
	successful bool
}

func (o NativeOutcome) Successful() bool { return o.successful }

func (o NativeOutcome) Payload() []byte {
	if !o.successful {
		return nil
	}
	return bytes.Clone(o.payload)
}

type nativeIncarnation struct {
	pod *corev1.Pod
	job *batchv1.Job
}

// InPodNative checks the relay's status, streams and original live incarnation.
func (e *Exec) InPodNative(ctx context.Context, namespace, pod string, want NativeContainerBinding, argv []string, input []byte) (NativeOutcome, error) {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
	if len(argv) == 0 || len(input) > nativePayloadLimit {
		return NativeOutcome{}, errNativeTransport
	}
	before, err := e.nativeIncarnation(ctx, namespace, pod, want)
	if err != nil || !before.execState(false) {
		return NativeOutcome{}, errNativeTransport
	}
	payload, err := e.nativeExec(ctx, namespace, pod, want.ContainerName, argv, bytes.Clone(input))
	if err != nil {
		return NativeOutcome{}, errNativeTransport
	}
	after, err := e.nativeIncarnation(ctx, namespace, pod, want)
	if err != nil || !before.same(after) || !after.execState(true) || ctx.Err() != nil {
		return NativeOutcome{}, errNativeTransport
	}
	return NativeOutcome{payload: bytes.Clone(payload), successful: true}, nil
}

// JobCompletedNative requires the original successful main process and quiet complete logs.
func (e *Exec) JobCompletedNative(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding) (NativeOutcome, error) {
	ctx, cancel := e.bounded(ctx)
	defer cancel()
	if jobUID == "" {
		return NativeOutcome{}, errNativeTransport
	}
	before, err := e.nativeJobPod(ctx, namespace, jobUID, want)
	if err != nil {
		return NativeOutcome{}, errNativeTransport
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := before.completed()
		if err != nil {
			return NativeOutcome{}, errNativeTransport
		}
		if ready {
			break
		}
		select {
		case <-ctx.Done():
			return NativeOutcome{}, errNativeTransport
		case <-ticker.C:
		}
		next, err := e.nativeJobPod(ctx, namespace, jobUID, want)
		if err != nil || !before.same(next) {
			return NativeOutcome{}, errNativeTransport
		}
		before = next
	}
	if err := e.nativeQuietLogs(ctx, namespace, before.pod.Name, want.ContainerName); err != nil {
		return NativeOutcome{}, errNativeTransport
	}
	after, err := e.nativeJobPod(ctx, namespace, jobUID, want)
	if err != nil || !before.same(after) || !before.sameTerminal(after) || ctx.Err() != nil {
		return NativeOutcome{}, errNativeTransport
	}
	if ready, err := after.completed(); err != nil || !ready {
		return NativeOutcome{}, errNativeTransport
	}
	return NativeOutcome{successful: true}, nil
}

func (e *Exec) nativeIncarnation(ctx context.Context, namespace, pod string, want NativeContainerBinding) (nativeIncarnation, error) {
	if namespace == "" || pod == "" || want.PodUID == "" || want.ContainerName == "" || want.ContainerID == "" || want.ImageID == "" || want.RestartCount < 0 || want.TemplateSHA256 == ([32]byte{}) {
		return nativeIncarnation{}, errNativeTransport
	}
	p, err := e.clientset.CoreV1().Pods(namespace).Get(ctx, pod, metav1.GetOptions{})
	if err != nil || !nativePodMatchesBinding(p, namespace, pod, want) {
		return nativeIncarnation{}, errNativeTransport
	}
	owner := metav1.GetControllerOf(p)
	if owner == nil || owner.APIVersion != "batch/v1" || owner.Kind != "Job" || owner.UID == "" || owner.Name == "" {
		return nativeIncarnation{}, errNativeTransport
	}
	controllers := 0
	for _, reference := range p.OwnerReferences {
		if reference.Controller != nil && *reference.Controller {
			controllers++
		}
	}
	if controllers != 1 {
		return nativeIncarnation{}, errNativeTransport
	}
	j, err := e.clientset.BatchV1().Jobs(namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil || j.UID != owner.UID || j.Name != owner.Name || j.Namespace != namespace || j.DeletionTimestamp != nil || NativeTemplateSHA256(j.Spec.Template) != want.TemplateSHA256 {
		return nativeIncarnation{}, errNativeTransport
	}
	return nativeIncarnation{pod: p, job: j}, nil
}

func nativePodMatchesBinding(p *corev1.Pod, namespace, pod string, want NativeContainerBinding) bool {
	if p.UID != want.PodUID || p.Name != pod || p.Namespace != namespace || p.DeletionTimestamp != nil || len(p.Spec.Containers) != 1 || len(p.Spec.InitContainers) != 0 || len(p.Spec.EphemeralContainers) != 0 || len(p.Status.ContainerStatuses) != 1 || len(p.Status.InitContainerStatuses) != 0 || len(p.Status.EphemeralContainerStatuses) != 0 {
		return false
	}
	status := p.Status.ContainerStatuses[0]
	if p.Spec.Containers[0].Name != want.ContainerName || status.Name != want.ContainerName || status.ContainerID != want.ContainerID || status.RestartCount != want.RestartCount || status.ImageID != want.ImageID || status.State.Waiting != nil || (status.State.Running == nil && status.State.Terminated == nil) || (status.State.Running != nil && status.State.Terminated != nil) {
		return false
	}
	return true
}

func (e *Exec) nativeJobPod(ctx context.Context, namespace string, jobUID types.UID, want NativeContainerBinding) (nativeIncarnation, error) {
	pods, err := e.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: batchv1.ControllerUidLabel + "=" + string(jobUID)})
	if err != nil || len(pods.Items) != 1 {
		return nativeIncarnation{}, errNativeTransport
	}
	n, err := e.nativeIncarnation(ctx, namespace, pods.Items[0].Name, want)
	if err != nil || n.job.UID != jobUID || pods.Items[0].UID != n.pod.UID || n.pod.Labels[batchv1.ControllerUidLabel] != string(jobUID) {
		return nativeIncarnation{}, errNativeTransport
	}
	return n, nil
}

func (n nativeIncarnation) same(other nativeIncarnation) bool {
	return n.pod.UID == other.pod.UID && n.job.UID == other.job.UID && n.pod.Name == other.pod.Name && n.job.Name == other.job.Name && n.pod.Generation == other.pod.Generation && n.job.Generation == other.job.Generation && reflect.DeepEqual(n.pod.Spec, other.pod.Spec) && reflect.DeepEqual(n.job.Spec, other.job.Spec) && reflect.DeepEqual(n.pod.OwnerReferences, other.pod.OwnerReferences) && reflect.DeepEqual(n.job.OwnerReferences, other.job.OwnerReferences) && reflect.DeepEqual(n.pod.Labels, other.pod.Labels) && reflect.DeepEqual(n.pod.Annotations, other.pod.Annotations) && reflect.DeepEqual(n.job.Labels, other.job.Labels) && reflect.DeepEqual(n.job.Annotations, other.job.Annotations)
}

// sameTerminal compares only the container state: the kubelet still rewrites other status fields, such as status.resources, after the pod Succeeded.
func (n nativeIncarnation) sameTerminal(other nativeIncarnation) bool {
	return reflect.DeepEqual(n.pod.Status.ContainerStatuses[0].State, other.pod.Status.ContainerStatuses[0].State)
}

func (n nativeIncarnation) execState(allowTerminated bool) bool {
	s := n.pod.Status.ContainerStatuses[0]
	if s.RestartCount != 0 || s.LastTerminationState != (corev1.ContainerState{}) || n.job.Status.Failed != 0 || n.pod.Status.Phase == corev1.PodFailed || s.State.Waiting != nil {
		return false
	}
	for _, condition := range n.job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return false
		}
	}
	if s.State.Running != nil {
		return s.State.Terminated == nil
	}
	return allowTerminated && s.State.Terminated != nil && s.State.Terminated.ExitCode == 0 && s.State.Terminated.Signal == 0 && s.State.Terminated.ContainerID == s.ContainerID
}

func (n nativeIncarnation) completed() (bool, error) {
	s := n.pod.Status.ContainerStatuses[0]
	if n.pod.Spec.RestartPolicy != corev1.RestartPolicyNever || !n.execState(true) || n.job.Status.Succeeded > 1 {
		return false, errNativeTransport
	}
	if s.State.Terminated == nil {
		return false, nil
	}
	for _, condition := range n.job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue && n.job.Status.Succeeded == 1 && n.job.Status.Active == 0 && n.pod.Status.Phase == corev1.PodSucceeded {
			return true, nil
		}
	}
	return false, nil
}

func (e *Exec) nativeQuietLogs(ctx context.Context, namespace, pod, container string) error {
	r, err := e.clientset.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{Container: container}).Stream(ctx)
	if err != nil {
		return errNativeTransport
	}
	payload, readErr := io.ReadAll(io.LimitReader(r, 1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(payload) != 0 || ctx.Err() != nil {
		return errNativeTransport
	}
	return nil
}

func (e *Exec) nativeExec(ctx context.Context, namespace, pod, container string, argv []string, input []byte) ([]byte, error) {
	req := e.clientset.CoreV1().RESTClient().Post().Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: container, Command: argv, Stdin: true, Stdout: true, Stderr: true}, scheme.ParameterCodec)
	transport, holder, err := wsclient.RoundTripperFor(e.config)
	if err != nil {
		return nil, errNativeTransport
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL().String(), nil)
	if err != nil {
		return nil, errNativeTransport
	}
	c, err := wsclient.Negotiate(transport, holder, request, "v5.channel.k8s.io")
	if err != nil {
		return nil, errNativeTransport
	}
	defer c.Close() //nolint:errcheck
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	deadline, ok := ctx.Deadline()
	if !ok || c.SetReadDeadline(deadline) != nil || c.SetWriteDeadline(deadline) != nil {
		return nil, errNativeTransport
	}
	if len(input) > 0 {
		if err := c.WriteMessage(websocket.BinaryMessage, append([]byte{0}, input...)); err != nil {
			return nil, errNativeTransport
		}
	}
	if err := c.WriteMessage(websocket.BinaryMessage, []byte{255, 0}); err != nil {
		return nil, errNativeTransport
	}
	var streams [3][]byte
	limits := [3]int{nativePayloadLimit, nativePayloadLimit, nativePayloadLimit}
	for {
		kind, r, err := c.NextReader()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) && ctx.Err() == nil && len(streams[0]) > 0 && len(streams[1]) == 0 && nativeStatusSuccess(streams[2]) {
				return streams[0], nil
			}
			return nil, errNativeTransport
		}
		if kind != websocket.BinaryMessage {
			return nil, errNativeTransport
		}
		var channel [1]byte
		if _, err := io.ReadFull(r, channel[:]); err != nil || channel[0] < 1 || channel[0] > 3 {
			return nil, errNativeTransport
		}
		i := int(channel[0]) - 1
		payload, err := io.ReadAll(io.LimitReader(r, int64(limits[i]-len(streams[i])+1)))
		if err != nil || len(payload) > limits[i]-len(streams[i]) {
			return nil, errNativeTransport
		}
		streams[i] = append(streams[i], payload...)
	}
}

func nativeStatusSuccess(raw []byte) bool {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return false
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return false
		}
		seen[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return false
		}
		if !nativeStatusFieldSuccess(key, value) {
			return false
		}
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') {
		return false
	}
	var extra json.RawMessage
	return seen["status"] && d.Decode(&extra) == io.EOF
}

func nativeStatusFieldSuccess(key string, value json.RawMessage) bool {
	switch key {
	case "status", "kind", "apiVersion", "message", "reason":
		var text string
		if len(value) == 0 || value[0] != '"' || json.Unmarshal(value, &text) != nil {
			return false
		}
		if (key == "status" && text != "Success") || (key == "kind" && text != "Status") || (key == "apiVersion" && text != "v1") || ((key == "message" || key == "reason") && text != "") {
			return false
		}
	case "metadata":
		var metadata map[string]json.RawMessage
		if json.Unmarshal(value, &metadata) != nil || metadata == nil || len(metadata) != 0 {
			return false
		}
	case "code":
		return bytes.Equal(value, []byte("0"))
	case "details":
		return bytes.Equal(value, []byte("null"))
	default:
		return false
	}
	return true
}

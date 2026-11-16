package controller

import (
	"encoding/json"
	"path"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/ydixken/pgcopydb-operator/internal/conn"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// uriFileRef matches a composed-URI file path a prelude names; program sources only hold the base name.
var uriFileRef = regexp.MustCompile(`(/[A-Za-z0-9_.-]+)+/pgm-(source|target)(-super)?-uri`)

// passfileBacking returns the pod's PGPASSFILE and the volume holding it, or nil when the root file system does.
func passfileBacking(spec *corev1.PodSpec) (string, *corev1.Volume) {
	c := spec.Containers[0]
	file := ""
	for _, e := range c.Env {
		if e.Name == "PGPASSFILE" {
			file = e.Value
		}
	}
	if file == "" {
		return "", nil
	}
	var best *corev1.VolumeMount
	for i, m := range c.VolumeMounts {
		if strings.HasPrefix(file, strings.TrimSuffix(m.MountPath, "/")+"/") && (best == nil || len(m.MountPath) > len(best.MountPath)) {
			best = &c.VolumeMounts[i]
		}
	}
	if best == nil {
		return file, nil
	}
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == best.Name {
			return file, &spec.Volumes[i]
		}
	}
	return file, nil
}

// The passfile and the composed URIs carry plaintext passwords, so no native pod keeps them on node disk.
func TestNativePassfilesOnMemoryVolumes(t *testing.T) {
	// The cleanup and recovery read fixtures connect without a password, so their secretRef rows below cover them.
	credentialFree := map[string]bool{"buildNativeVersionJob": true, "buildNativeCleanupJob": true, "buildNativeRecoveryReadJob": true}
	rows := nativeBuilderRows(t)
	rows["buildNativeCleanupJob/secretRef"] = func(t *testing.T) (*batchv1.Job, error) {
		f := newCleanupInventoryFixture(t)
		owned, err := f.read(t)
		if err != nil {
			t.Fatal(err)
		}
		f.migration.Spec.Target = secretRefTarget()
		return buildNativeCleanupJob(f.migration, f.pvc, f.want.ImageRef, nativeCleanupTestName, owned)
	}
	rows["buildNativeRecoveryReadJob/secretRef"] = func(t *testing.T) (*batchv1.Job, error) {
		m, w := cleanedJobInputs(t)
		m.Spec.Target = secretRefTarget()
		w.Role, w.JobName, w.ProgramSHA256 = native.JobRoleRetryGate, "rpgmigrate-retry-"+w.OperationID, native.EvidenceProgramSHA256()
		return buildNativeRecoveryReadJob(m, nativeEvidenceTestImage, w)
	}
	rows["buildNativeWorkerJob/secretRef"] = func(*testing.T) (*batchv1.Job, error) {
		m, pvc := nativeEndposJobInputs()
		m.Spec.Target = secretRefTarget()
		return buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
	}
	rows["buildNativeEvidenceJob/secretRef"] = func(t *testing.T) (*batchv1.Job, error) {
		m, want := nativeEvidenceJobInputs(t)
		m.Spec.Source, m.Spec.Target = secretRefTarget(), secretRefTarget()
		return buildNativeEvidenceJob(m, nativeEvidenceTestImage, want)
	}
	for name, build := range rows {
		t.Run(name, func(t *testing.T) {
			job, err := build(t)
			if err != nil || job == nil {
				t.Fatalf("builder refused its fixture: %v", err)
			}
			spec := &job.Spec.Template.Spec
			file, volume := passfileBacking(spec)
			if credentialFree[name] {
				if file != "" {
					t.Fatalf("credential-free builder sets PGPASSFILE %s", file)
				}
				return
			}
			if file == "" {
				t.Fatal("native builder sets no PGPASSFILE")
			}
			if volume == nil || volume.EmptyDir == nil || volume.EmptyDir.Medium != corev1.StorageMediumMemory || volume.EmptyDir.SizeLimit == nil {
				t.Fatalf("passfile %s is on volume %+v, not a size-limited memory emptyDir", file, volume)
			}
			c := spec.Containers[0]
			for _, ref := range uriFileRef.FindAllString(strings.Join(append(append([]string{}, c.Command...), c.Args...), "\n"), -1) {
				if path.Dir(ref) != path.Dir(file) {
					t.Errorf("credential file %s is outside the passfile directory %s", ref, path.Dir(file))
				}
			}
		})
	}
}

// A Job and Pod that agree on a disk-backed credential volume still do not match: agreement is not the guard.
func TestNativePodMatchRequiresMemoryCredentials(t *testing.T) {
	for name, mutate := range map[string]func(*corev1.PodSpec){
		"memory":    func(*corev1.PodSpec) {},
		"disk":      func(s *corev1.PodSpec) { credentialVolume(t, s).EmptyDir.Medium = corev1.StorageMediumDefault },
		"unlimited": func(s *corev1.PodSpec) { credentialVolume(t, s).EmptyDir.SizeLimit = nil },
		"tmp":       func(s *corev1.PodSpec) { setPassfile(s, conn.PgpassPath) },
	} {
		t.Run(name, func(t *testing.T) {
			m, pvc := nativeEndposJobInputs()
			job, err := buildNativeWorkerJob(m, pvc, nativeEvidenceTestImage, 1)
			if err != nil {
				t.Fatal(err)
			}
			job.UID = "worker-job"
			mutate(&job.Spec.Template.Spec)
			if got := nativePreflightPodMatches(job, probePod(job)); got != (name == "memory") {
				t.Fatalf("worker Pod with %s credentials: matched=%v", name, got)
			}
		})
	}
}

func credentialVolume(t *testing.T, spec *corev1.PodSpec) *corev1.Volume {
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == "credentials" {
			return &spec.Volumes[i]
		}
	}
	t.Fatal("worker has no credential volume")
	return nil
}

func setPassfile(spec *corev1.PodSpec, file string) {
	for i, e := range spec.Containers[0].Env {
		if e.Name == "PGPASSFILE" {
			spec.Containers[0].Env[i].Value = file
		}
	}
}

// Native admission compares volumes with reflect.DeepEqual, so every native size limit must survive API decoding.
func TestNativeVolumeLimitsSurviveAPIDecoding(t *testing.T) {
	for name, build := range nativeBuilderRows(t) {
		t.Run(name, func(t *testing.T) {
			job, err := build(t)
			if err != nil || job == nil {
				t.Fatalf("builder refused its fixture: %v", err)
			}
			raw, err := json.Marshal(job.Spec.Template.Spec.Volumes)
			if err != nil {
				t.Fatal(err)
			}
			var decoded []corev1.Volume
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			limits := 0
			for i, v := range job.Spec.Template.Spec.Volumes {
				if v.EmptyDir == nil || v.EmptyDir.SizeLimit == nil {
					continue
				}
				limits++
				if !reflect.DeepEqual(v, decoded[i]) {
					t.Errorf("volume %s size limit %#v decodes as %#v", v.Name, *v.EmptyDir.SizeLimit, *decoded[i].EmptyDir.SizeLimit)
				}
			}
			if limits == 0 && len(job.Spec.Template.Spec.Volumes) != 0 {
				t.Fatal("native builder mounts volumes but no size-limited emptyDir")
			}
		})
	}
}

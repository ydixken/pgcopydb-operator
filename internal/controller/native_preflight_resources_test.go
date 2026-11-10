package controller

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/native"
	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	probeTestVersionCommand     = "version"
	probeTestTimeoutEnvironment = "PGCONNECT_TIMEOUT"
	probeTestPreflightRole      = "preflight-probe"
	probeTestOperation          = "0123456789abcdef0123456789abcdef"
	probeTestVersionName        = "rpgmigrate-version-" + probeTestOperation
	probeTestPreflightName      = "rpgmigrate-preflight-" + probeTestOperation
	probeTestSecretCanary       = "probe-private-secret-canary"
)

func probeJobMigration() *v1beta1.Migration {
	m, _ := nativeWorkerJobInputs()
	return m
}

func TestNativeVersionJob(t *testing.T) {
	m := probeJobMigration()
	m.Spec.Source = v1beta1.PostgresConnection{
		Host: probeTestSecretCanary, Username: probeTestSecretCanary,
		SynchronousCommit:  "invalid-on-source",
		PasswordSecretRef:  &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: probeTestSecretCanary}, Key: "password"},
		URISecretRef:       &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: probeTestSecretCanary}, Key: "probe-uri"},
		SecretRef:          &v1beta1.ConnectionSecret{Name: probeTestSecretCanary},
		SuperuserSecretRef: &v1beta1.ConnectionSecret{Name: probeTestSecretCanary},
		TLS:                &v1beta1.TLSSecretRefs{Key: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: probeTestSecretCanary}, Key: "tls.key"}},
	}
	m.Spec.Target = m.Spec.Source
	before := m.DeepCopy()
	job, err := buildNativeVersionJob(m, nativeEvidenceTestImage, probeTestVersionName)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m, before) {
		t.Fatal("discovery construction changed the Migration")
	}
	assertProbeEnvelope(t, m, job, probeTestVersionName)
	c := job.Spec.Template.Spec.Containers[0]
	if !slices.Equal(c.Command, []string{nativeWorkerTestExecutable}) || !slices.Equal(c.Args, []string{probeTestVersionCommand, "--json"}) || len(c.Env) != 0 || len(c.EnvFrom) != 0 || len(c.VolumeMounts) != 0 || len(job.Spec.Template.Spec.Volumes) != 0 {
		t.Fatal("discovery materialized credentials, identity or work instead of direct discovery")
	}
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(probeTestSecretCanary)) || bytes.Contains(raw, []byte("RPGMIGRATE_MIGRATION_UID")) || bytes.Contains(raw, []byte(pgcopydb.WorkDir)) {
		t.Fatal("discovery serialized a private connection, identity or work path")
	}
}

func assertProbeEnvelope(t *testing.T, m *v1beta1.Migration, job *batchv1.Job, name string) {
	t.Helper()
	if job.Name != name || job.Namespace != m.Namespace || job.UID != "" || len(job.OwnerReferences) != 0 || !job.CreationTimestamp.IsZero() {
		t.Fatal("probe claimed allocated ownership or changed its name")
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || job.Spec.Completions == nil || *job.Spec.Completions != 1 || job.Spec.Parallelism == nil || *job.Spec.Parallelism != 1 || job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 90 {
		t.Fatal("probe does not enforce one bounded execution")
	}
	pod := job.Spec.Template.Spec
	assertNativeEvidencePodIsolation(t, pod)
	assertNativeEvidencePodSecurity(t, pod)
	assertNativeEvidenceContainerIsolation(t, pod.Containers[0], nativeEvidenceTestImage)
	if !reflect.DeepEqual(job.Labels, labels(m)) || !reflect.DeepEqual(job.Spec.Template.Labels, labels(m)) {
		t.Fatal("probe lost existing valid labels")
	}
	if !reflect.DeepEqual(pod.Containers[0].Resources, pgcopydb.EffectiveRunnerResources(m.Spec.Runner.Resources)) || !reflect.DeepEqual(pod.Affinity, runnerAffinity(m)) || !reflect.DeepEqual(pod.NodeSelector, m.Spec.Runner.NodeSelector) || !reflect.DeepEqual(pod.Tolerations, m.Spec.Runner.Tolerations) || job.Spec.TTLSecondsAfterFinished != nil {
		t.Fatal("probe lost resource, scheduling or evidence retention")
	}
}

func TestNativePreflightJob(t *testing.T) {
	forms := map[string]v1beta1.PostgresConnection{
		nativeEvidenceInlineForm:  passwordMigration().Spec.Source,
		nativeWorkerSecretForm:    secretRefTarget(),
		nativeWorkerURISecretForm: uriSecretRefTarget(),
	}
	for sourceName, source := range forms {
		for targetName, target := range forms {
			for _, follow := range []bool{false, true} {
				name := sourceName + "/" + targetName
				if follow {
					name += "/follow"
				}
				t.Run(name, func(t *testing.T) {
					m := probeJobMigration()
					m.Spec.Source, m.Spec.Target = source, target
					m.Spec.Follow = &v1beta1.FollowOptions{Enabled: follow}
					for _, connection := range []*v1beta1.PostgresConnection{&m.Spec.Source, &m.Spec.Target} {
						if connection.URISecretRef == nil {
							connection.TLS = &v1beta1.TLSSecretRefs{RootCA: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "probe-tls"}, Key: "probe-ca.pem"}}
						}
					}
					before := m.DeepCopy()
					job, err := buildNativePreflightJob(m, nativeEvidenceTestImage, probeTestPreflightName, probeTestOperation)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(before, m) {
						t.Fatal("preflight construction changed its source")
					}
					assertProbeEnvelope(t, m, job, probeTestPreflightName)
					command, args, env, volumes, mounts, err := native.PreflightMaterialization(m, probeTestOperation)
					if err != nil {
						t.Fatal(err)
					}
					c, pod := job.Spec.Template.Spec.Containers[0], job.Spec.Template.Spec
					if !reflect.DeepEqual(c.Command, command) || !reflect.DeepEqual(c.Args, args) || !reflect.DeepEqual(c.Env, env) || !reflect.DeepEqual(pod.Volumes, volumes) || !reflect.DeepEqual(c.VolumeMounts, mounts) {
						t.Fatal("preflight diverged from canonical private materialization")
					}
					assertPreflightProgram(t, c, follow)
					for _, v := range pod.Volumes {
						if v.PersistentVolumeClaim != nil {
							t.Fatal("preflight mounted a work PVC")
						}
					}
					if len(pod.Volumes) == 0 || pod.Volumes[0].EmptyDir == nil || pod.Volumes[0].EmptyDir.Medium != corev1.StorageMediumMemory || pod.Volumes[0].EmptyDir.SizeLimit == nil || pod.Volumes[0].EmptyDir.SizeLimit.Cmp(resource.MustParse("64Mi")) != 0 {
						t.Fatal("preflight private scratch is not bounded memory storage")
					}
				})
			}
		}
	}
}

func assertPreflightProgram(t *testing.T, c corev1.Container, follow bool) {
	t.Helper()
	if len(c.Command) != 4 || c.Command[0] != shellPath || c.Command[1] != "-c" || c.Command[3] != shellPath || len(c.Args) != 2 || c.Args[0] != "-c" {
		t.Fatal("preflight shell envelope changed")
	}
	hash := native.EvidenceProgramSHA256()
	prefix := "exec /usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B - native_preflight " + probeTestOperation + " " + hex.EncodeToString(hash[:]) + " <<'RPGMIGRATE_NATIVE_EVIDENCE_PY'\n"
	if c.Args[1] != prefix+string(native.EvidenceProgram())+"RPGMIGRATE_NATIVE_EVIDENCE_PY\n" || len(c.Args[1])+1 > native.EvidenceMainArgumentLimit || len(c.Command[2])+1 > native.EvidencePreludeArgumentLimit || strings.IndexByte(c.Args[1], 0) >= 0 {
		t.Fatal("preflight source, role, operation, hash or argument bounds changed")
	}
	wantFollow := "0"
	if follow {
		wantFollow = "1"
	}
	for _, expected := range []corev1.EnvVar{{Name: "LC_ALL", Value: "C"}, {Name: "LANG", Value: "C"}, {Name: probeTestTimeoutEnvironment, Value: "10"}, {Name: "RPGMIGRATE_NATIVE_PREFLIGHT_FOLLOW", Value: wantFollow}} {
		if !slices.Contains(c.Env, expected) {
			t.Fatalf("preflight lost %s binding", expected.Name)
		}
	}
	for _, e := range c.Env {
		if strings.HasPrefix(e.Name, "RPGMIGRATE_") && e.Name != "RPGMIGRATE_NATIVE_PREFLIGHT_FOLLOW" {
			t.Fatal("preflight inherited operator work identity")
		}
	}
}

func TestNativeProbeJobInputs(t *testing.T) {
	for _, role := range []string{"version-probe", probeTestPreflightRole} {
		for _, bad := range []string{"absent probe migration", "legacy selection", "unrecognized probe engine", "invalid Migration name", "absent native default", "mutable native image", "wrong role name", "uppercase operation", "short operation", "nonhex operation", "invalid namespace"} {
			t.Run(role+"/"+bad, func(t *testing.T) {
				m := probeJobMigration()
				image, name, op := nativeEvidenceTestImage, probeTestVersionName, probeTestOperation
				if role == probeTestPreflightRole {
					name = probeTestPreflightName
				}
				switch bad {
				case "absent probe migration":
					m = nil
				case "legacy selection":
					m.Spec.Engine = ""
				case "unrecognized probe engine":
					m.Spec.Engine = testUnrecognizedEngine
				case "invalid Migration name":
					m.Name = "bad/migration"
				case "absent native default":
					image = ""
				case "mutable native image":
					image = "registry.example/probe:private-image-canary"
				case "wrong role name":
					name = "unexpected-probe"
				case "uppercase operation":
					op = strings.ToUpper(op)
					name = strings.Replace(name, probeTestOperation, op, 1)
				case "short operation":
					op = op[:31]
					name = strings.Replace(name, probeTestOperation, op, 1)
				case "nonhex operation":
					op = "$(probe-private-canary)"
					name = strings.Replace(name, probeTestOperation, op, 1)
				case "invalid namespace":
					m.Namespace = "probe namespace"
				}
				var job *batchv1.Job
				var err error
				if role == probeTestPreflightRole {
					job, err = buildNativePreflightJob(m, image, name, op)
				} else {
					job, err = buildNativeVersionJob(m, image, name)
				}
				if err == nil || job != nil {
					t.Fatal("invalid probe returned a usable Job")
				}
				if strings.Contains(err.Error(), "canary") {
					t.Fatal("probe error leaked invalid input")
				}
			})
		}
	}
	t.Run("preflight operation mismatch", func(t *testing.T) {
		m := probeJobMigration()
		job, err := buildNativePreflightJob(m, nativeEvidenceTestImage, probeTestPreflightName, strings.Repeat("f", 32))
		if err == nil || job != nil {
			t.Fatal("preflight name and operation were not bound")
		}
	})
	t.Run("preflight materialization failure", func(t *testing.T) {
		m := probeJobMigration()
		m.Spec.Source.Host = ""
		job, err := buildNativePreflightJob(m, nativeEvidenceTestImage, probeTestPreflightName, probeTestOperation)
		if err == nil || job != nil {
			t.Fatal("invalid private materialization returned a usable Job")
		}
	})
	t.Run("oversized private materialization", func(t *testing.T) {
		m := probeJobMigration()
		m.Spec.Source.Host = strings.Repeat("private-probe-cap", 8192)
		job, err := buildNativePreflightJob(m, nativeEvidenceTestImage, probeTestPreflightName, probeTestOperation)
		if err == nil || job != nil || strings.Contains(err.Error(), "private-probe-cap") {
			t.Fatal("oversized materialization was accepted or disclosed")
		}
	})
	t.Run("preflight missing UID", func(t *testing.T) {
		m := probeJobMigration()
		m.UID = ""
		job, err := buildNativePreflightJob(m, nativeEvidenceTestImage, probeTestPreflightName, probeTestOperation)
		if err == nil || job != nil {
			t.Fatal("preflight rendered absent consumed UID")
		}
	})
}

func TestNativeProbeJobEnvelope(t *testing.T) {
	m := probeJobMigration()
	m.Spec.Runner.Image = nativeEvidenceTestImage
	ttl := int32(75)
	m.Spec.TTLSecondsAfterFinished = &ttl
	m.Spec.Runner.NodeSelector = map[string]string{"probe-pool": "private"}
	m.Spec.Runner.Tolerations = []corev1.Toleration{{Key: "probe", Operator: corev1.TolerationOpExists}}
	m.Spec.Runner.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")}
	m.Annotations = map[string]string{annotationWorkNode: "probe-node"}
	for _, build := range []func() (*batchv1.Job, error){func() (*batchv1.Job, error) {
		return buildNativeVersionJob(m, "unused-probe-image", probeTestVersionName)
	}, func() (*batchv1.Job, error) {
		return buildNativePreflightJob(m, "unused-probe-image", probeTestPreflightName, probeTestOperation)
	}} {
		job, err := build()
		if err != nil {
			t.Fatal(err)
		}
		assertProbeEnvelope(t, m, job, job.Name)
		raw, err := json.Marshal(job)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > native.EvidenceJobJSONLimit {
			t.Fatal("synthetic probe exceeds bounded Job representation")
		}
	}
}

func TestNativeProbeJobGolden(t *testing.T) {
	m := probeJobMigration()
	for name, build := range map[string]func() (*batchv1.Job, error){
		"native-version-job.json": func() (*batchv1.Job, error) {
			return buildNativeVersionJob(m, nativeEvidenceTestImage, probeTestVersionName)
		},
		"native-preflight-job.json": func() (*batchv1.Job, error) {
			return buildNativePreflightJob(m, nativeEvidenceTestImage, probeTestPreflightName, probeTestOperation)
		},
	} {
		t.Run(name, func(t *testing.T) {
			job, err := build()
			if err != nil {
				t.Fatal(err)
			}
			got, err := json.MarshalIndent(job, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			file := filepath.Join("testdata", name)
			if os.Getenv("UPDATE_GOLDEN") != "" {
				if err := os.WriteFile(file, got, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("native probe golden changed; inspect before updating")
			}
		})
	}
}

package native

import (
	"strings"
	"testing"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	corev1 "k8s.io/api/core/v1"
)

// credentialSpec is the evidence pod for secretRef connections, whose preludes write the passfile and URIs.
func credentialSpec(t *testing.T) *corev1.PodSpec {
	t.Helper()
	f := newAdmissionFixture(t)
	f.m.Spec.Source = v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: evidenceSource}}
	f.m.Spec.Target = v1beta1.PostgresConnection{SecretRef: &v1beta1.ConnectionSecret{Name: "target"}}
	command, args, env, volumes, mounts, err := EvidenceMaterialization(f.m, f.want)
	if err != nil {
		t.Fatal(err)
	}
	spec := f.job.Spec.Template.Spec.DeepCopy()
	c := &spec.Containers[0]
	spec.Volumes, c.Command, c.Args, c.Env, c.VolumeMounts = volumes, command, args, env, mounts
	return spec
}

func credentialMount(spec *corev1.PodSpec) *corev1.VolumeMount {
	_, want := conn.MemoryCredentialsVolume()
	for i, m := range spec.Containers[0].VolumeMounts {
		if m.Name == want.Name {
			return &spec.Containers[0].VolumeMounts[i]
		}
	}
	return nil
}

func credentialPassfile(spec *corev1.PodSpec) *corev1.EnvVar {
	for i, e := range spec.Containers[0].Env {
		if e.Name == nativePreflightPassfileEnv {
			return &spec.Containers[0].Env[i]
		}
	}
	return nil
}

func TestNativeCredentialsInMemory(t *testing.T) {
	base := credentialSpec(t)
	if credentialMount(base) == nil || credentialPassfile(base) == nil || credentialPassfile(base).Value != conn.MemoryCredentials.Pgpass() {
		t.Fatal("evidence fixture lost its memory credential volume")
	}
	if !CredentialsInMemory(base) || !admittedPodSpec(base, base.Containers[0].Image) {
		t.Fatal("memory-backed evidence pod refused")
	}
	for name, mutate := range map[string]func(*corev1.PodSpec){
		"disk_medium": func(s *corev1.PodSpec) {
			for i := range s.Volumes {
				if s.Volumes[i].Name == credentialMount(s).Name {
					s.Volumes[i].EmptyDir.Medium = corev1.StorageMediumDefault
				}
			}
		},
		"no_size_limit": func(s *corev1.PodSpec) {
			for i := range s.Volumes {
				if s.Volumes[i].Name == credentialMount(s).Name {
					s.Volumes[i].EmptyDir.SizeLimit = nil
				}
			}
		},
		"secret_volume": func(s *corev1.PodSpec) {
			for i := range s.Volumes {
				if s.Volumes[i].Name == credentialMount(s).Name {
					s.Volumes[i].VolumeSource = corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "x"}}
				}
			}
		},
		"volume_missing": func(s *corev1.PodSpec) { credentialMount(s).Name = "not-a-volume" },
		"read_only":      func(s *corev1.PodSpec) { credentialMount(s).ReadOnly = true },
		"sub_path":       func(s *corev1.PodSpec) { credentialMount(s).SubPath = "x" },
		"mount_moved":    func(s *corev1.PodSpec) { credentialMount(s).MountPath = "/run/other" },
		"disk_tmp":       func(s *corev1.PodSpec) { credentialPassfile(s).Value = conn.PgpassPath },
		"relative":       func(s *corev1.PodSpec) { credentialPassfile(s).Value = "run/rpgmigrate-credentials/pgpass" },
		"unclean":        func(s *corev1.PodSpec) { credentialPassfile(s).Value = "/run/x/../rpgmigrate-credentials/pgpass" },
		"empty_value":    func(s *corev1.PodSpec) { credentialPassfile(s).Value = "" },
		"value_from": func(s *corev1.PodSpec) {
			credentialPassfile(s).ValueFrom = &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}
		},
		"shadow_below": func(s *corev1.PodSpec) {
			s.Containers[0].VolumeMounts = append(s.Containers[0].VolumeMounts, corev1.VolumeMount{Name: nativePreflightTmp, MountPath: conn.MemoryCredentials.Pgpass()})
		},
		"inside_other_mount": func(s *corev1.PodSpec) {
			s.Containers[0].VolumeMounts = append(s.Containers[0].VolumeMounts, corev1.VolumeMount{Name: nativePreflightTmp, MountPath: "/run"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			spec := credentialSpec(t)
			mutate(spec)
			if CredentialsInMemory(spec) || admittedPodSpec(spec, spec.Containers[0].Image) {
				t.Fatal("credential volume off memory admitted")
			}
		})
	}
	t.Run("no_passfile", func(t *testing.T) {
		spec := credentialSpec(t)
		env := spec.Containers[0].Env[:0]
		for _, e := range spec.Containers[0].Env {
			if e.Name != nativePreflightPassfileEnv {
				env = append(env, e)
			}
		}
		spec.Containers[0].Env = env
		if !CredentialsInMemory(spec) {
			t.Fatal("pod without a passfile refused")
		}
	})
}

// The fixed readers in the worker and evidence pods name the same directory the builders mount.
func TestNativeCredentialReadersMatchVolume(t *testing.T) {
	if _, mount := conn.MemoryCredentialsVolume(); mount.MountPath != string(conn.MemoryCredentials) {
		t.Fatal("memory credential mount moved")
	}
	if !strings.Contains(string(EvidenceProgram()), "\nMEMORY_CREDENTIALS='"+string(conn.MemoryCredentials)+"'\n") {
		t.Fatal("evidence program names another credential directory")
	}
	if !strings.Contains(controlRecovery, "path='"+string(conn.MemoryCredentials)+"/pgm-'+role+'-uri'") || strings.Contains(controlRecovery, "/tmp/") {
		t.Fatal("worker recovery reads URIs outside the memory credential volume")
	}
	for _, call := range []string{"Materialization(os.environ,credentials=MEMORY_CREDENTIALS)", "Materialization(os.environ,source_only=True,credentials=MEMORY_CREDENTIALS)"} {
		if strings.Count(string(EvidenceProgram()), call) != 1 {
			t.Fatalf("worker or evidence reader %s missing", call)
		}
	}
}

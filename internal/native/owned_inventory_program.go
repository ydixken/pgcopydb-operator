package native

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	"github.com/ydixken/pgcopydb-operator/internal/conn"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

//go:embed owned_inventory_program.py
var ownedInventoryProgramSource string

//go:embed owned_inventory_request_writer.py
var ownedInventoryWriterAdapterSource string

const (
	OwnedInventoryHelperSourceLimit = 16384
	nativeOwnedWorkMount            = "/work"
	ownedInventorySeparator         = "\n# RPGMIGRATE_NATIVE_OWNED_INVENTORY_V1\n"
	ownedInventoryBootstrap         = `import sys,os,base64,zlib,hashlib,types
try:
 o,h,c,p=sys.argv[1:]
 if len(o)!=32 or any(x not in '0123456789abcdef' for x in o) or len(h)!=64 or any(x not in '0123456789abcdef' for x in h): raise ValueError()
 def source(value,limit):
  b=base64.b64decode(value,validate=True)
  if base64.b64encode(b).decode('ascii')!=value: raise ValueError()
  d=zlib.decompressobj();s=d.decompress(b,limit+1)
  if not s or len(s)>limit or not d.eof or d.unused_data or d.unconsumed_tail or b'\0' in s or not s.endswith(b'\n'): raise ValueError()
  return s
 c=source(c,122880);p=source(p,16384);s=b'\n# RPGMIGRATE_NATIVE_OWNED_INVENTORY_V1\n'
 if s in c or s in p or hashlib.sha256(c+s+p).hexdigest()!=h: raise ValueError()
 m=types.ModuleType('_rpgmigrate_native_owned_inventory');sys.modules[m.__name__]=m
 exec(compile(c,'<native-canonical>','exec'),m.__dict__)
 exec(compile(p,'<native-owned-inventory>','exec'),m.__dict__)
 os._exit(m.owned_inventory_main(['-','owned_inventory',o,h]))
except BaseException:
 os._exit(1)
`
)

func OwnedInventoryProgram() []byte {
	return []byte(string(EvidenceProgram()) + ownedInventorySeparator + ownedInventoryProgramSource)
}

func OwnedInventoryProgramSHA256() [32]byte { return sha256.Sum256(OwnedInventoryProgram()) }

func OwnedInventoryRequestWriterProgram() []byte {
	adapter := []byte(ownedInventoryWriterAdapterSource)
	writer := EvidenceRequestWriterProgram()
	if len(adapter) == 0 || len(adapter) > 3072 || adapter[len(adapter)-1] != '\n' || bytes.IndexByte(adapter, 0) >= 0 || len(writer) == 0 || len(writer) > 16384 || writer[len(writer)-1] != '\n' || bytes.IndexByte(writer, 0) >= 0 {
		return nil
	}
	program := "import sys,types,base64\nm=types.ModuleType('_rpgmigrate_native_owned_inventory_writer')\nsys.modules[m.__name__]=m\nexec(base64.b64decode('" + base64.StdEncoding.EncodeToString(writer) + "'),m.__dict__)\nexec(" + strconv.Quote(string(adapter)) + ",m.__dict__)\nsys.exit(m.main(sys.argv))\n"
	if len(program) > 16384 {
		return nil
	}
	return []byte(program)
}

func OwnedInventoryRequestWriterSHA256() [32]byte {
	return sha256.Sum256(OwnedInventoryRequestWriterProgram())
}

// OwnedInventoryMaterialization is a projection and admits no live execution by itself.
func OwnedInventoryMaterialization(m *v1beta1.Migration, w ExecutionData) ([]string, []string, []corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount, error) {
	if m == nil || m.UID == "" || m.Spec.Engine != v1beta1.MigrationEngineRPGMigrate || m.Spec.Clone.AllDatabases || m.Spec.Follow == nil || !m.Spec.Follow.Enabled || w.Role != JobRoleCleanup || !validIdentity(w.Work) || w.MigrationUID != m.UID || w.Work.MigrationUID != string(m.UID) || w.Work.WorkdirID != string(w.PVCUID) || len(validation.IsDNS1123Subdomain(w.PVCName)) != 0 || w.PVCName != m.Name+"-work" || !lowerHex(w.OperationID, 32) || w.ProgramSHA256 != OwnedInventoryProgramSHA256() {
		return nil, nil, nil, nil, nil, ErrNativeOwnedStateUnavailable
	}
	src, err := conn.Materialize(conn.Source, &m.Spec.Source)
	if err != nil {
		return nil, nil, nil, nil, nil, ErrNativeOwnedStateUnavailable
	}
	tgt, err := conn.Materialize(conn.Target, &m.Spec.Target)
	if err != nil {
		return nil, nil, nil, nil, nil, ErrNativeOwnedStateUnavailable
	}
	env := []corev1.EnvVar{}
	volumes := []corev1.Volume{{Name: resourceKindWorkdir, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: w.PVCName, ReadOnly: true}}}, {Name: nativePreflightTmp, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: resource.NewQuantity(64*1024*1024, resource.BinarySI)}}}}
	mounts := []corev1.VolumeMount{{Name: resourceKindWorkdir, MountPath: nativeOwnedWorkMount, ReadOnly: true}, {Name: nativePreflightTmp, MountPath: nativeRetryTmpPath}}
	var preludes []string
	var passfiles []conn.Passfile
	for _, material := range []*conn.Materialized{src, tgt} {
		env = append(env, material.Env...)
		volumes = append(volumes, material.Volumes...)
		mounts = append(mounts, material.Mounts...)
		if material.Prelude != "" {
			preludes = append(preludes, material.Prelude)
		}
		if material.Passfile != nil {
			passfiles = append(passfiles, *material.Passfile)
		}
	}
	if len(preludes) > 0 || len(passfiles) > 0 {
		env = append(env, corev1.EnvVar{Name: nativePreflightPassfileEnv, Value: conn.PgpassPath})
	}
	env = append(env, corev1.EnvVar{Name: nativePreflightTimeoutEnv, Value: "10"})
	env = append(env, identityEnv(w.Work)...)
	env = append(env, corev1.EnvVar{Name: nativePreflightLocale, Value: "C"}, corev1.EnvVar{Name: nativePreflightLang, Value: "C"})
	command, args, err := ownedInventoryScripts(EvidenceProgram(), []byte(ownedInventoryProgramSource), w, conn.PreludeScript(preludes, passfiles, ""))
	if err != nil {
		return nil, nil, nil, nil, nil, ErrNativeOwnedStateUnavailable
	}
	return command, args, env, volumes, mounts, nil
}

func OwnedInventoryConnectionReferenceSHA256(m *v1beta1.Migration, w ExecutionData) ([32]byte, error) {
	_, _, env, volumes, mounts, err := OwnedInventoryMaterialization(m, w)
	if err != nil {
		return [32]byte{}, ErrNativeOwnedStateUnavailable
	}
	return StructuralConnectionReferenceSHA256(env, volumes, mounts)
}

func ownedInventoryScripts(core, helper []byte, w ExecutionData, prelude string) ([]string, []string, error) {
	composite := append(append(slices.Clone(core), []byte(ownedInventorySeparator)...), helper...)
	if !lowerHex(w.OperationID, 32) || sha256.Sum256(composite) != w.ProgramSHA256 || len(ownedInventoryBootstrap) > 2048 || len(prelude)+1 > EvidencePreludeArgumentLimit || strings.IndexByte(prelude, 0) >= 0 {
		return nil, nil, ErrNativeOwnedStateUnavailable
	}
	compressed := make([]string, 0, 2)
	for i, source := range [][]byte{core, helper} {
		limit := EvidenceProgramSourceLimit
		if i == 1 {
			limit = OwnedInventoryHelperSourceLimit
		}
		if len(source) == 0 || len(source) > limit || source[len(source)-1] != '\n' || bytes.IndexByte(source, 0) >= 0 || bytes.Contains(source, []byte(ownedInventorySeparator)) {
			return nil, nil, ErrNativeOwnedStateUnavailable
		}
		var buffer bytes.Buffer
		writer, err := zlib.NewWriterLevel(&buffer, zlib.BestCompression)
		if err != nil {
			return nil, nil, ErrNativeOwnedStateUnavailable
		}
		_, writeErr := writer.Write(source)
		closeErr := writer.Close()
		if writeErr != nil || closeErr != nil {
			return nil, nil, ErrNativeOwnedStateUnavailable
		}
		compressed = append(compressed, base64.StdEncoding.EncodeToString(buffer.Bytes()))
	}
	command := []string{nativeShellPath, "-c", prelude, nativeEnvExecutable}
	args := []string{"-u", nativePreloadEnv, "-u", nativeLibraryPathEnv, nativePythonExecutable, "-I", "-B", "-c", ownedInventoryBootstrap, w.OperationID, hex.EncodeToString(w.ProgramSHA256[:]), compressed[0], compressed[1]}
	total := 0
	for _, arg := range append(slices.Clone(command), args...) {
		if strings.IndexByte(arg, 0) >= 0 || len(arg)+1 > EvidenceMainArgumentLimit {
			return nil, nil, ErrNativeOwnedStateUnavailable
		}
		total += len(arg) + 1
	}
	if total > EvidenceJobJSONLimit {
		return nil, nil, ErrNativeOwnedStateUnavailable
	}
	return command, args, nil
}

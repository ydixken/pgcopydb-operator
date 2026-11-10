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
	corev1 "k8s.io/api/core/v1"
)

//go:embed cleaned_completion_program.py
var cleanedCompletionProgramSource string

//go:embed cleaned_completion_request_writer.py
var cleanedCompletionWriterSource string

const (
	CleanedCompletionHelperSourceLimit = 16384
	cleanedCompletionSeparator         = "\n# RPGMIGRATE_NATIVE_CLEANED_COMPLETION_V1\n"
	cleanedCompletionBootstrap         = `import sys,os,base64,zlib,hashlib,types
try:
 o,h,c,i,p=sys.argv[1:]
 if len(o)!=32 or any(x not in '0123456789abcdef' for x in o) or len(h)!=64 or any(x not in '0123456789abcdef' for x in h): raise ValueError()
 def source(value,limit):
  b=base64.b64decode(value,validate=True)
  if base64.b64encode(b).decode('ascii')!=value: raise ValueError()
  d=zlib.decompressobj();s=d.decompress(b,limit+1)
  if not s or len(s)>limit or not d.eof or d.unused_data or d.unconsumed_tail or b'\0' in s or not s.endswith(b'\n'): raise ValueError()
  return s
 c=source(c,122880);i=source(i,16384);p=source(p,16384)
 a=b'\n# RPGMIGRATE_NATIVE_OWNED_INVENTORY_V1\n';b=b'\n# RPGMIGRATE_NATIVE_CLEANED_COMPLETION_V1\n'
 if any(s in v for s in (a,b) for v in (c,i,p)) or hashlib.sha256(c+a+i+b+p).hexdigest()!=h: raise ValueError()
 m=types.ModuleType('_rpgmigrate_native_cleaned_completion');sys.modules[m.__name__]=m
 for s,n in ((c,'canonical'),(i,'inventory'),(p,'cleaned')): exec(compile(s,'<native-'+n+'>','exec'),m.__dict__)
 os._exit(m.cleaned_completion_main(['-','cleaned_completed_evidence',o,h]))
except BaseException:
 os._exit(1)
`
)

func CleanedCompletionProgram() []byte {
	return append(append(OwnedInventoryProgram(), []byte(cleanedCompletionSeparator)...), []byte(cleanedCompletionProgramSource)...)
}
func CleanedCompletionProgramSHA256() [32]byte { return sha256.Sum256(CleanedCompletionProgram()) }

func cleanedCompressed(source []byte) (string, error) {
	var buffer bytes.Buffer
	writer, err := zlib.NewWriterLevel(&buffer, zlib.BestCompression)
	if err != nil {
		return "", ErrNativeCleanedCompletionUnavailable
	}
	_, writeErr := writer.Write(source)
	closeErr := writer.Close()
	if writeErr != nil || closeErr != nil {
		return "", ErrNativeCleanedCompletionUnavailable
	}
	return base64.StdEncoding.EncodeToString(buffer.Bytes()), nil
}

func CleanedCompletionRequestWriterProgram() []byte {
	writer, adapter := EvidenceRequestWriterProgram(), []byte(cleanedCompletionWriterSource)
	if len(writer) == 0 || len(writer) > 16384 || writer[len(writer)-1] != '\n' || bytes.IndexByte(writer, 0) >= 0 || len(adapter) == 0 || len(adapter) > 16384 || adapter[len(adapter)-1] != '\n' || bytes.IndexByte(adapter, 0) >= 0 {
		return nil
	}
	compressed, err := cleanedCompressed(writer)
	if err != nil {
		return nil
	}
	writerHash := sha256.Sum256(writer)
	program := "import sys,types,base64,zlib,hashlib\nm=types.ModuleType('_rpgmigrate_native_cleaned_writer');sys.modules[m.__name__]=m\nd=zlib.decompressobj();s=d.decompress(base64.b64decode('" + compressed + "',validate=True),16385)\nif not s or len(s)>16384 or not d.eof or d.unused_data or d.unconsumed_tail or hashlib.sha256(s).hexdigest()!='" + hex.EncodeToString(writerHash[:]) + "':raise ValueError()\nexec(s,m.__dict__)\nexec(" + strconv.Quote(string(adapter)) + ",m.__dict__)\nsys.exit(m.main(sys.argv))\n"
	if len(program) > 16384 {
		return nil
	}
	return []byte(program)
}

// Materialization projects expectations only; independent admission binds live execution.
func CleanedCompletionMaterialization(m *v1beta1.Migration, w ExecutionData) ([]string, []string, []corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount, error) {
	if w.Role != JobRoleEvidence || w.ProgramSHA256 != CleanedCompletionProgramSHA256() {
		return nil, nil, nil, nil, nil, ErrNativeCleanedCompletionUnavailable
	}
	projection := w
	projection.Role, projection.ProgramSHA256 = JobRoleCleanup, OwnedInventoryProgramSHA256()
	command, _, env, volumes, mounts, err := OwnedInventoryMaterialization(m, projection)
	if err != nil {
		return nil, nil, nil, nil, nil, ErrNativeCleanedCompletionUnavailable
	}
	command, args, err := cleanedCompletionScripts(w, command[2])
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	return command, args, env, volumes, mounts, nil
}

func CleanedCompletionConnectionReferenceSHA256(m *v1beta1.Migration, w ExecutionData) ([32]byte, error) {
	_, _, env, volumes, mounts, err := CleanedCompletionMaterialization(m, w)
	if err != nil {
		return [32]byte{}, ErrNativeCleanedCompletionUnavailable
	}
	return StructuralConnectionReferenceSHA256(env, volumes, mounts)
}

func cleanedCompletionScripts(w ExecutionData, prelude string) ([]string, []string, error) {
	if !lowerHex(w.OperationID, 32) || w.ProgramSHA256 != CleanedCompletionProgramSHA256() || len(cleanedCompletionBootstrap) > 2048 || len(prelude)+1 > EvidencePreludeArgumentLimit || strings.IndexByte(prelude, 0) >= 0 {
		return nil, nil, ErrNativeCleanedCompletionUnavailable
	}
	var compressed []string
	for i, source := range [][]byte{EvidenceProgram(), []byte(ownedInventoryProgramSource), []byte(cleanedCompletionProgramSource)} {
		limit := CleanedCompletionHelperSourceLimit
		if i == 0 {
			limit = EvidenceProgramSourceLimit
		}
		if len(source) == 0 || len(source) > limit || source[len(source)-1] != '\n' || bytes.IndexByte(source, 0) >= 0 || bytes.Contains(source, []byte(ownedInventorySeparator)) || bytes.Contains(source, []byte(cleanedCompletionSeparator)) {
			return nil, nil, ErrNativeCleanedCompletionUnavailable
		}
		value, err := cleanedCompressed(source)
		if err != nil {
			return nil, nil, err
		}
		compressed = append(compressed, value)
	}
	command := []string{nativeShellPath, "-c", prelude, nativeEnvExecutable}
	args := append([]string{"-u", nativePreloadEnv, "-u", nativeLibraryPathEnv, nativePythonExecutable, "-I", "-B", "-c", cleanedCompletionBootstrap, w.OperationID, hex.EncodeToString(w.ProgramSHA256[:])}, compressed...)
	total := 0
	for _, arg := range append(slices.Clone(command), args...) {
		if len(arg)+1 > EvidenceMainArgumentLimit || strings.IndexByte(arg, 0) >= 0 {
			return nil, nil, ErrNativeCleanedCompletionUnavailable
		}
		total += len(arg) + 1
	}
	if total > EvidenceJobJSONLimit {
		return nil, nil, ErrNativeCleanedCompletionUnavailable
	}
	return command, args, nil
}

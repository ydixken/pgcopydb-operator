package native

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"strings"

	v1beta1 "github.com/ydixken/pgcopydb-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
)

//go:embed comparison_program.py
var comparisonProgramSource string

const (
	// The separate helper budget preserves the canonical library and process argument limits.
	ComparisonHelperSourceLimit = 16384
	comparisonSourceSeparator   = "\n# RPGMIGRATE_NATIVE_COMPARISON_V1\n"
	comparisonBootstrap         = `import sys,os,base64,zlib,hashlib,types
try:
 k,o,h,c,p=sys.argv[1:]
 if k not in ('schema','data') or len(o)!=32 or any(x not in '0123456789abcdef' for x in o) or len(h)!=64 or any(x not in '0123456789abcdef' for x in h): raise ValueError()
 def source(value,limit):
  b=base64.b64decode(value,validate=True)
  if base64.b64encode(b).decode('ascii')!=value: raise ValueError()
  d=zlib.decompressobj();s=d.decompress(b,limit+1)
  if not s or len(s)>limit or not d.eof or d.unused_data or d.unconsumed_tail or b'\0' in s or not s.endswith(b'\n'): raise ValueError()
  return s
 c=source(c,122880);p=source(p,16384);s=b'\n# RPGMIGRATE_NATIVE_COMPARISON_V1\n'
 if s in c or s in p or hashlib.sha256(c+s+p).hexdigest()!=h: raise ValueError()
 m=types.ModuleType('_rpgmigrate_native_comparison');sys.modules[m.__name__]=m
 exec(compile(c,'<native-canonical>','exec'),m.__dict__)
 exec(compile(p,'<native-comparison>','exec'),m.__dict__)
 os._exit(m.comparison_main(['-','native_comparison',k,o,h]))
except BaseException:
 os._exit(1)
`
)

// ComparisonProgram binds the canonical library and private comparison mechanism.
func ComparisonProgram() []byte {
	return []byte(string(EvidenceProgram()) + comparisonSourceSeparator + comparisonProgramSource)
}

func ComparisonProgramSHA256() [32]byte { return sha256.Sum256(ComparisonProgram()) }

// ComparisonMaterialization reuses the canonical projections without admitting an execution.
func ComparisonMaterialization(m *v1beta1.Migration, operationID, check string) ([]string, []string, []corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount, error) {
	if m == nil || m.Spec.Engine != v1beta1.MigrationEngineRPGMigrate {
		return nil, nil, nil, nil, nil, ErrNativeComparisonUnavailable
	}
	command, _, env, volumes, mounts, err := PreflightMaterialization(m, operationID)
	if err != nil {
		return nil, nil, nil, nil, nil, ErrNativeComparisonUnavailable
	}
	command, args, err := comparisonScripts(check, operationID, command[2])
	if err != nil {
		return nil, nil, nil, nil, nil, ErrNativeComparisonUnavailable
	}
	return command, args, env, volumes, mounts, nil
}

func comparisonScripts(check, operationID, prelude string) ([]string, []string, error) {
	if check != comparisonCheckSchema && check != comparisonCheckData || !lowerHex(operationID, 32) || len(comparisonBootstrap) > 2048 || len(prelude)+1 > EvidencePreludeArgumentLimit || strings.IndexByte(prelude, 0) >= 0 {
		return nil, nil, ErrNativeComparisonUnavailable
	}
	compressed := make([]string, 0, 2)
	for i, source := range [][]byte{EvidenceProgram(), []byte(comparisonProgramSource)} {
		limit := EvidenceProgramSourceLimit
		if i == 1 {
			limit = ComparisonHelperSourceLimit
		}
		if len(source) == 0 || len(source) > limit || source[len(source)-1] != '\n' || bytes.IndexByte(source, 0) >= 0 || bytes.Contains(source, []byte(comparisonSourceSeparator)) {
			return nil, nil, ErrNativeComparisonUnavailable
		}
		var buffer bytes.Buffer
		writer, err := zlib.NewWriterLevel(&buffer, zlib.BestCompression)
		if err != nil {
			return nil, nil, ErrNativeComparisonUnavailable
		}
		_, writeErr := writer.Write(source)
		closeErr := writer.Close()
		if writeErr != nil || closeErr != nil {
			return nil, nil, ErrNativeComparisonUnavailable
		}
		compressed = append(compressed, base64.StdEncoding.EncodeToString(buffer.Bytes()))
	}
	hash := ComparisonProgramSHA256()
	command := []string{nativeShellPath, "-c", prelude, nativeEnvExecutable}
	args := []string{"-u", nativePreloadEnv, "-u", nativeLibraryPathEnv, nativePythonExecutable, "-I", "-B", "-c", comparisonBootstrap, check, operationID, hex.EncodeToString(hash[:]), compressed[0], compressed[1]}
	total := 0
	for _, arg := range append(append([]string{}, command...), args...) {
		if strings.IndexByte(arg, 0) >= 0 || len(arg)+1 > EvidenceMainArgumentLimit {
			return nil, nil, ErrNativeComparisonUnavailable
		}
		total += len(arg) + 1
	}
	if total > EvidenceJobJSONLimit {
		return nil, nil, ErrNativeComparisonUnavailable
	}
	return command, args, nil
}

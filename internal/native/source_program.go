package native

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	nativeEnvExecutable    = "/usr/bin/env"
	nativePythonExecutable = "/usr/bin/python3"
	nativeLibraryPathEnv   = "LD_LIBRARY_PATH"
	nativePreloadEnv       = "LD_PRELOAD"
	sourceHeadKind         = "source_head"
	activeLagKind          = "active_lag"
	sourceBootstrap        = `import sys,os,base64,zlib,hashlib,types
try:
 k,o,h,c=sys.argv[1:]
 if k not in ('source_head','active_lag') or len(o)!=32 or any(x not in '0123456789abcdef' for x in o) or len(h)!=64 or any(x not in '0123456789abcdef' for x in h): raise ValueError()
 b=base64.b64decode(c,validate=True)
 if base64.b64encode(b).decode('ascii')!=c: raise ValueError()
 d=zlib.decompressobj();s=d.decompress(b,122881)
 if not s or len(s)>122880 or not d.eof or d.unused_data or d.unconsumed_tail or hashlib.sha256(s).hexdigest()!=h: raise ValueError()
 m=types.ModuleType('_rpgmigrate_native_source');sys.modules[m.__name__]=m
 exec(compile(s,'<native-source>','exec'),m.__dict__)
 os._exit(m.run_source_observation(k,o,h))
except BaseException:
 os._exit(1)
`
)

func sourceExecArgv(kind string, w ExecutionData) ([]string, error) {
	if !activeRoleWorker(w) || (kind != sourceHeadKind && kind != activeLagKind) || len(sourceBootstrap) > 2048 {
		return nil, errors.New("native source delivery unavailable")
	}
	source := EvidenceProgram()
	if len(source) == 0 || len(source) > EvidenceProgramSourceLimit || source[len(source)-1] != '\n' || bytes.IndexByte(source, 0) >= 0 || sha256.Sum256(source) != w.ProgramSHA256 {
		return nil, errors.New("native source delivery unavailable")
	}
	var compressed bytes.Buffer
	writer, err := zlib.NewWriterLevel(&compressed, zlib.BestCompression)
	if err != nil {
		return nil, errors.New("native source delivery unavailable")
	}
	_, writeErr := writer.Write(source)
	closeErr := writer.Close()
	if writeErr != nil || closeErr != nil {
		return nil, errors.New("native source delivery unavailable")
	}
	argv := []string{nativeEnvExecutable, "-u", nativePreloadEnv, "-u", nativeLibraryPathEnv, nativePythonExecutable, "-I", "-B", "-c", sourceBootstrap, kind, w.OperationID, hex.EncodeToString(w.ProgramSHA256[:]), base64.StdEncoding.EncodeToString(compressed.Bytes())}
	if !sourceExecFits(w, argv) {
		return nil, errors.New("native source delivery unavailable")
	}
	return argv, nil
}

func sourceExecFits(w ExecutionData, argv []string) bool {
	query, err := scheme.ParameterCodec.EncodeParameters(&corev1.PodExecOptions{Container: w.Container.ContainerName, Command: argv, Stdin: true, Stdout: true, Stderr: true}, corev1.SchemeGroupVersion)
	if err != nil {
		return false
	}
	size := 0
	for _, argument := range argv {
		if strings.IndexByte(argument, 0) >= 0 {
			return false
		}
		size += len(argument) + 1
		if size > nativeWireLimit {
			return false
		}
	}
	target := url.URL{Path: "/api/v1/namespaces/" + w.Namespace + "/pods/" + w.PodName + "/exec", RawQuery: query.Encode()}
	return len(target.RequestURI()) <= nativeWireLimit
}

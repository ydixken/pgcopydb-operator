package native

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"strconv"
	"strings"
)

const (
	EvidenceProgramSourceLimit   = 122880
	EvidenceMainArgumentLimit    = 126976
	EvidencePreludeArgumentLimit = 65536
	EvidenceJobJSONLimit         = 1048576
)

//go:embed evidence_program.py
var evidenceProgramSource string

//go:embed evidence_request_writer.py
var evidenceRequestWriterSource string

// EvidenceProgram returns a copy of the reviewed private Job program.
func EvidenceProgram() []byte { return []byte(evidenceProgramSource) }

// EvidenceProgramSHA256 binds the exact source bytes, including the final LF.
func EvidenceProgramSHA256() [32]byte { return sha256.Sum256([]byte(evidenceProgramSource)) }

// EvidenceRequestWriterProgram returns a copy of the one-use FIFO writer.
func EvidenceRequestWriterProgram() []byte { return []byte(evidenceRequestWriterSource) }

// EvidenceRequestWriterSHA256 binds the exact checked-exec writer source.
func EvidenceRequestWriterSHA256() [32]byte {
	return sha256.Sum256([]byte(evidenceRequestWriterSource))
}

// adaptedRequestWriterProgram overrides the writer's request and result policy with one kind's adapter.
func adaptedRequestWriterProgram(module, adapter string) []byte {
	writer := EvidenceRequestWriterProgram()
	if len(adapter) == 0 || len(adapter) > 3072 || adapter[len(adapter)-1] != '\n' || strings.IndexByte(adapter, 0) >= 0 || len(writer) == 0 || len(writer) > 16384 || writer[len(writer)-1] != '\n' || bytes.IndexByte(writer, 0) >= 0 {
		return nil
	}
	program := "import sys,types,base64\nm=types.ModuleType('" + module + "')\nsys.modules[m.__name__]=m\nexec(base64.b64decode('" + base64.StdEncoding.EncodeToString(writer) + "'),m.__dict__)\nexec(" + strconv.Quote(adapter) + ",m.__dict__)\nsys.exit(m.main(sys.argv))\n"
	if len(program) > 16384 {
		return nil
	}
	return []byte(program)
}

package native

import (
	"crypto/sha256"
	_ "embed"
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

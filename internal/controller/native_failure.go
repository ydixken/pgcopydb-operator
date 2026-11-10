package controller

import (
	"bytes"
	"strings"
	"unicode/utf8"

	"github.com/ydixken/pgcopydb-operator/internal/pgcopydb"
)

const (
	nativeFailureBadArgs   = "NativeBadArgs"
	nativeFailureBadConfig = "NativeBadConfig"
	nativeFailureBadState  = "NativeBadState"
)

func nativeWorkerTerminalFailure(raw []byte) (string, bool) {
	if len(raw) == 0 || len(raw) > 65536 || !utf8.Valid(raw) || raw[len(raw)-1] != '\n' {
		return "", false
	}
	// Only the complete final diagnostic can classify a failed original execution.
	last := bytes.TrimSpace(raw)
	last = last[bytes.LastIndexByte(last, '\n')+1:]
	message := pgcopydb.LastErrorLine(last)
	switch {
	case strings.HasPrefix(message, "native:BadArgs:"):
		return nativeFailureBadArgs, true
	case strings.HasPrefix(message, "native:BadConfig:"):
		return nativeFailureBadConfig, true
	case strings.HasPrefix(message, "native:BadState:"):
		return nativeFailureBadState, true
	default:
		return "", false
	}
}

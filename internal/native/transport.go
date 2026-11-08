package native

import (
	"bytes"
	"errors"
)

// ParseReportLine requires a terminated record so a truncated observation cannot prove completion.
func ParseReportLine(raw []byte, want Binding) (*Report, error) {
	var payload []byte
	for len(raw) > 0 {
		line, rest, terminated := bytes.Cut(raw, []byte("\n"))
		raw = rest
		if !bytes.HasPrefix(line, []byte("native-report:")) {
			continue
		}
		record, ok := bytes.CutPrefix(line, []byte("native-report: "))
		if !ok || !terminated {
			return nil, errors.New("invalid native report framing")
		}
		if payload != nil {
			return nil, errors.New("multiple native report records")
		}
		payload = record
	}
	if payload == nil {
		return nil, errors.New("native report record is absent")
	}
	return ParseReport(payload, want)
}

// ParseVersionLine excludes log selection because discovery stdout is its own document transport.
func ParseVersionLine(raw []byte) (*Version, error) {
	line, rest, terminated := bytes.Cut(raw, []byte("\n"))
	if !terminated || len(rest) != 0 {
		return nil, errors.New("invalid native version framing")
	}
	return ParseVersion(line)
}

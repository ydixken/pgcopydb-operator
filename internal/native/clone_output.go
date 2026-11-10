package native

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"unicode/utf8"
)

const cloneOutputCompletedPhase = "completed"

var errCloneOutput = errors.New("native clone output unavailable")

// ParseCloneOutput validates complete combined Job logs; returned data cannot certify follow completion.
func ParseCloneOutput(raw []byte, work WorkIdentity, follow bool) (*Report, error) {
	if len(raw) == 0 || len(raw) > nativeWireLimit || !validIdentity(work) || !utf8.Valid(raw) || bytes.ContainsRune(raw, '\r') {
		return nil, errCloneOutput
	}
	var limits map[string]uint64
	var backpressure, streaming bool
	for frame := 0; len(raw) > 0 && frame < 4; frame++ {
		line, rest, terminated := bytes.Cut(raw, []byte("\n"))
		if !terminated {
			return nil, errCloneOutput
		}
		raw = rest
		if record, ok := bytes.CutPrefix(line, []byte("native-report: ")); ok {
			if cloneOutputObject(record, "protocol_version", "engine_version", "command", "capabilities", "work", "phase", "copy") == nil {
				return nil, errCloneOutput
			}
			report, err := ParseReport(record, Binding{Command: nativeCloneCommand, Work: work})
			if err != nil {
				return nil, errCloneOutput
			}
			switch report.Phase {
			case cloneOutputCompletedPhase:
				if len(raw) != 0 || ValidateCompletedClone(report, work) != nil {
					return nil, errCloneOutput
				}
				return report, nil
			case "streaming":
				if !follow || limits == nil || streaming {
					return nil, errCloneOutput
				}
				streaming = true
			default:
				return nil, errCloneOutput
			}
		} else if follow && frame == 0 {
			limits = cloneOutputStartup(line)
			if limits == nil {
				return nil, errCloneOutput
			}
		} else if follow && limits != nil && !backpressure && cloneOutputBackpressure(line, limits) {
			backpressure = true
		} else {
			return nil, errCloneOutput
		}
	}
	return nil, errCloneOutput
}

func cloneOutputStartup(raw []byte) map[string]uint64 {
	object := cloneOutputObject(raw, "level", "error_severity", "event", "message", "limits", "apply_group_txns", "wal_storage_free_bytes")
	if !cloneOutputLog(object, "follow.startup", "native follow startup") || !cloneOutputText(object["wal_storage_free_bytes"], "unknown") || cloneOutputUint(object["apply_group_txns"]) == 0 {
		return nil
	}
	limits := cloneOutputObject(object["limits"], "channel_bytes", "channel_messages", "max_message_bytes", "group_rows", "group_bytes")
	if limits == nil {
		return nil
	}
	values := make(map[string]uint64, len(limits))
	for name, maximum := range map[string]uint64{"channel_bytes": 8388608, "channel_messages": 1024, "max_message_bytes": 16777216, "group_rows": 10000, "group_bytes": 8388608} {
		value := cloneOutputUint(limits[name])
		if value == 0 || value > maximum {
			return nil
		}
		values[name] = value
	}
	return values
}

func cloneOutputBackpressure(raw []byte, limits map[string]uint64) bool {
	object := cloneOutputObject(raw, "level", "error_severity", "event", "message", "queue")
	if !cloneOutputLog(object, "follow.backpressure", "native follow backpressure") {
		return false
	}
	queue := cloneOutputObject(object["queue"], "messages", "owned_bytes", "limit_messages", "limit_bytes")
	messages, owned := cloneOutputUint(queue["messages"]), cloneOutputUint(queue["owned_bytes"])
	if messages == 0 || messages > limits["channel_messages"] || owned == 0 || cloneOutputUint(queue["limit_messages"]) != limits["channel_messages"] || cloneOutputUint(queue["limit_bytes"]) != limits["channel_bytes"] {
		return false
	}
	maximum := limits["channel_bytes"]
	// An empty FIFO admits one oversized message within the separate message budget.
	if messages == 1 {
		maximum = limits["max_message_bytes"]
	}
	return owned <= maximum
}

func cloneOutputLog(object map[string]json.RawMessage, event, message string) bool {
	return object != nil && cloneOutputText(object["level"], "info") && cloneOutputText(object["error_severity"], "INFO") && cloneOutputText(object["event"], event) && cloneOutputText(object["message"], message)
}

func cloneOutputText(raw []byte, want string) bool {
	var got string
	return json.Unmarshal(raw, &got) == nil && got == want
}

func cloneOutputUint(raw []byte) uint64 {
	var got uint64
	if json.Unmarshal(raw, &got) != nil {
		return 0
	}
	return got
}

// Exact keys and duplicate rejection avoid encoding/json's case folding and last-member selection.
func cloneOutputObject(raw []byte, fields ...string) map[string]json.RawMessage {
	d := json.NewDecoder(bytes.NewReader(raw))
	if token, err := d.Token(); err != nil || token != json.Delim('{') {
		return nil
	}
	object := make(map[string]json.RawMessage, len(fields))
	for d.More() {
		token, err := d.Token()
		name, ok := token.(string)
		if err != nil || !ok || !slices.Contains(fields, name) || object[name] != nil {
			return nil
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return nil
		}
		object[name] = value
	}
	if token, err := d.Token(); err != nil || token != json.Delim('}') || len(object) != len(fields) {
		return nil
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return nil
	}
	return object
}

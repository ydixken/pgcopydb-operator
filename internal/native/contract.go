// Package native validates rpgmigrate contracts and runs checked commands while keeping execution data separate from independent completion proof.
package native

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/ydixken/pgcopydb-operator/internal/sentinel"
)

type Capability string

const (
	CapCloneSingleDB    Capability = "clone.single_db"
	CapLargeObjects     Capability = "copy.large_objects"
	CapBinaryFallback   Capability = "copy.binary_fallback"
	CapSplit            Capability = "copy.split"
	CapPGOutput         Capability = "cdc.pgoutput"
	CapProgress         Capability = "progress.v1"
	CapCutover          Capability = "cutover.v1"
	CapVerify           Capability = "verify.v1"
	CapCleanup          Capability = "cleanup.v1"
	commandSetEndpos               = "stream sentinel set endpos"
	resourceKindWorkdir            = "workdir"
)

type WorkIdentity struct {
	MigrationUID         string `json:"migration_uid"`
	RunID                string `json:"run_id"`
	WorkdirID            string `json:"workdir_id"`
	CheckpointGeneration int64  `json:"checkpoint_generation"`
}

// Binding comes from the caller's owned state, never from the report being checked.
type Binding struct {
	Command    string
	Work       WorkIdentity
	EndLSN     string
	Checkpoint *CheckpointEvidence
	Resources  []ResourceObligation
}

type CheckpointEvidence struct {
	CheckpointID     string
	CertifiedLSN     string
	DurableReplayLSN string
	DrainEndLSN      string
	DrainReplayLSN   string
	DrainKind        string
	BarrierConfirmed bool
}

type ResourceObligation struct {
	Kind   string
	Name   string
	Owned  bool
	Retain bool
}

type Version struct {
	ProtocolVersion int64        `json:"protocol_version"`
	EngineVersion   string       `json:"engine_version"`
	Capabilities    []Capability `json:"capabilities"`
}

type Report struct {
	ProtocolVersion int64         `json:"protocol_version"`
	EngineVersion   string        `json:"engine_version"`
	Command         string        `json:"command"`
	Capabilities    []Capability  `json:"capabilities"`
	Work            *WorkIdentity `json:"work"`
	Phase           string        `json:"phase"`
	Copy            *CopyProgress `json:"copy,omitempty"`
	Replication     *Replication  `json:"replication,omitempty"`
	Cleanup         *Cleanup      `json:"cleanup,omitempty"`
}

type Counters struct {
	Total *int64 `json:"total,omitempty"`
	Done  *int64 `json:"done,omitempty"`
}

type CopyProgress struct {
	Tables  *Counters `json:"tables,omitempty"`
	Indexes *Counters `json:"indexes,omitempty"`
	Bytes   *Counters `json:"bytes,omitempty"`
}

type Replication struct {
	SourceDurableHeadLSN *string     `json:"source_durable_head_lsn,omitempty"`
	ReceivedLSN          *string     `json:"received_lsn,omitempty"`
	CertifiedLSN         *string     `json:"certified_lsn"`
	DurableReplayLSN     *string     `json:"durable_replay_lsn,omitempty"`
	EndLSN               *string     `json:"end_lsn,omitempty"`
	EndPersisted         *bool       `json:"end_persisted,omitempty"`
	Drained              *bool       `json:"drained,omitempty"`
	CheckpointID         string      `json:"checkpoint_id,omitempty"`
	DrainProof           *DrainProof `json:"drain_proof,omitempty"`
}

type DrainProof struct {
	Kind         string        `json:"kind"`
	Work         *WorkIdentity `json:"work"`
	CheckpointID string        `json:"checkpoint_id"`
	Committed    *bool         `json:"committed"`
	EndLSN       string        `json:"end_lsn"`
	ReplayLSN    string        `json:"replay_lsn"`
}

type Cleanup struct {
	Complete  *bool           `json:"complete"`
	Resources []OwnedResource `json:"resources"`
}

type OwnedResource struct {
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Owned   *bool  `json:"owned"`
	Removed *bool  `json:"removed"`
}

func RequireCapabilities(got []Capability, required ...Capability) error {
	seen := make(map[Capability]bool, len(got))
	for _, c := range got {
		switch c {
		case CapCloneSingleDB, CapLargeObjects, CapBinaryFallback, CapSplit, CapPGOutput, CapProgress, CapCutover, CapVerify, CapCleanup:
		default:
			return errors.New("unknown native capability")
		}
		if seen[c] {
			return errors.New("duplicate native capability")
		}
		seen[c] = true
	}
	for _, c := range required {
		if !seen[c] {
			return errors.New("required native capability is absent")
		}
	}
	return nil
}

func commandCapability(command string) Capability {
	switch command {
	case "clone", "list tables":
		return CapCloneSingleDB
	case "list progress":
		return CapProgress
	case "stream sentinel get", commandSetEndpos:
		return CapCutover
	case "stream cleanup":
		return CapCleanup
	default:
		return ""
	}
}

func validLabel(s string) bool {
	return s != "" && len(s) <= 256 && strings.TrimSpace(s) == s && !strings.ContainsFunc(s, unicode.IsControl)
}

func validIdentity(w WorkIdentity) bool {
	if len(w.MigrationUID) != 36 || w.MigrationUID[8] != '-' || w.MigrationUID[13] != '-' ||
		w.MigrationUID[18] != '-' || w.MigrationUID[23] != '-' || w.CheckpointGeneration != 1 {
		return false
	}
	run := strings.ReplaceAll(w.MigrationUID, "-", "")
	return len(run) == 32 && !strings.ContainsFunc(run, func(c rune) bool {
		return (c < '0' || c > '9') && (c < 'a' || c > 'f')
	}) && w.RunID == run && validLabel(w.WorkdirID) && !strings.ContainsAny(w.WorkdirID, `/\`)
}

func ParseVersion(raw []byte) (*Version, error) {
	if !strictJSON(raw) {
		return nil, errors.New("invalid native JSON document")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var v Version
	if err := d.Decode(&v); err != nil {
		return nil, errors.New("invalid native version fields")
	}
	if v.ProtocolVersion != 1 || !validLabel(v.EngineVersion) || v.Capabilities == nil {
		return nil, errors.New("unsupported native version")
	}
	if err := RequireCapabilities(v.Capabilities); err != nil {
		return nil, err
	}
	return &v, nil
}

// ParseReport does not infer success from exit status or from absent measurements.
func ParseReport(raw []byte, want Binding) (*Report, error) {
	capability := commandCapability(want.Command)
	if capability == "" || !validIdentity(want.Work) {
		return nil, errors.New("invalid native report binding")
	}
	if want.EndLSN != "" {
		if lsn, err := sentinel.ParseLSN(want.EndLSN); err != nil || lsn == 0 {
			return nil, errors.New("invalid expected native end LSN")
		}
	}
	if !strictJSON(raw) {
		return nil, errors.New("invalid native JSON document")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var r Report
	if err := d.Decode(&r); err != nil {
		return nil, errors.New("invalid native report fields")
	}
	if r.ProtocolVersion != 1 || !validLabel(r.EngineVersion) || r.Command != want.Command || r.Capabilities == nil ||
		r.Work == nil || !validIdentity(*r.Work) || *r.Work != want.Work {
		return nil, errors.New("unsupported or unbound native report")
	}
	if err := RequireCapabilities(r.Capabilities, capability); err != nil {
		return nil, err
	}
	if !slices.Contains([]string{"planning", "copying", "finalizing", "streaming", "draining", "completed", "failed", "suspended"}, r.Phase) {
		return nil, errors.New("unknown native phase")
	}
	if err := validatePayload(&r, want); err != nil {
		return nil, err
	}
	return &r, nil
}

func validatePayload(r *Report, want Binding) error {
	switch r.Command {
	case "clone", "list tables", "list progress":
		if r.Copy == nil {
			return errors.New("native copy report is absent")
		}
	case "stream sentinel get", commandSetEndpos:
		if r.Replication == nil {
			return errors.New("native replication report is absent")
		}
	case "stream cleanup":
		if r.Cleanup == nil {
			return errors.New("native cleanup report is absent")
		}
	}
	if r.Copy != nil {
		for _, c := range []*Counters{r.Copy.Tables, r.Copy.Indexes, r.Copy.Bytes} {
			if c != nil && ((c.Total != nil && *c.Total < 0) || (c.Done != nil && *c.Done < 0) ||
				(c.Total != nil && c.Done != nil && *c.Done > *c.Total)) {
				return errors.New("invalid native copy counters")
			}
		}
	}
	if r.Replication != nil {
		if err := validateReplication(r.Replication, want); err != nil {
			return err
		}
	}
	if r.Cleanup != nil {
		return validateCleanup(r.Cleanup, want.Resources)
	}
	return nil
}

func validateReplication(r *Replication, want Binding) error {
	if r.CertifiedLSN == nil {
		return errors.New("native certified LSN measurement is absent")
	}
	for _, lsn := range []*string{r.SourceDurableHeadLSN, r.ReceivedLSN, r.CertifiedLSN, r.DurableReplayLSN, r.EndLSN} {
		if lsn != nil {
			if _, err := sentinel.ParseLSN(*lsn); err != nil {
				return errors.New("invalid native replication LSN")
			}
		}
	}
	if r.DurableReplayLSN != nil {
		certified, _ := sentinel.ParseLSN(*r.CertifiedLSN)
		durable, _ := sentinel.ParseLSN(*r.DurableReplayLSN)
		if certified > durable {
			return errors.New("native certified LSN exceeds durable replay")
		}
	}
	if r.CheckpointID != "" && !validCheckpointID(r.CheckpointID, want.Work.CheckpointGeneration) {
		return errors.New("invalid native checkpoint token")
	}
	if want.EndLSN == "" && (want.Command == commandSetEndpos || (r.Drained != nil && *r.Drained)) {
		return errors.New("native safety acknowledgement requires the caller's end LSN")
	}
	if want.EndLSN != "" && (r.EndLSN == nil || !sameLSN(*r.EndLSN, want.EndLSN)) {
		return errors.New("native cutover end LSN changed")
	}
	if want.Command == commandSetEndpos || (r.Drained != nil && *r.Drained) || (r.EndPersisted != nil && *r.EndPersisted) {
		if r.EndLSN == nil || sameLSN(*r.EndLSN, sentinel.ZeroLSN) || r.EndPersisted == nil || !*r.EndPersisted {
			return errors.New("native cutover end LSN is not durably acknowledged")
		}
	}
	if r.Drained == nil || !*r.Drained {
		if r.DrainProof != nil {
			return errors.New("native drain proof lacks a drained assertion")
		}
		return nil
	}
	return validateDrainProof(r, want)
}

func validateDrainProof(r *Replication, want Binding) error {
	p := r.DrainProof
	c := want.Checkpoint
	if p == nil || p.Work == nil || *p.Work != want.Work || !validCheckpointID(r.CheckpointID, want.Work.CheckpointGeneration) ||
		p.CheckpointID != r.CheckpointID || p.Committed == nil || !*p.Committed || r.DurableReplayLSN == nil ||
		c == nil || !c.BarrierConfirmed || c.CheckpointID != r.CheckpointID || c.DrainKind != p.Kind ||
		!sameLSN(c.CertifiedLSN, *r.CertifiedLSN) || !sameLSN(c.DurableReplayLSN, *r.DurableReplayLSN) ||
		!sameLSN(c.DrainEndLSN, p.EndLSN) || !sameLSN(c.DrainReplayLSN, p.ReplayLSN) || !sameLSN(p.EndLSN, *r.EndLSN) {
		return errors.New("native drain lacks a bound committed checkpoint")
	}
	end, err := sentinel.ParseLSN(p.EndLSN)
	if err != nil {
		return errors.New("invalid native drain end LSN")
	}
	replay, err := sentinel.ParseLSN(p.ReplayLSN)
	if err != nil {
		return errors.New("invalid native drain witness LSN")
	}
	certified, _ := sentinel.ParseLSN(*r.CertifiedLSN)
	if (p.Kind != "keepalive" && p.Kind != "later_begin") ||
		(p.Kind == "keepalive" && (replay < end || replay > certified)) ||
		(p.Kind == "later_begin" && replay <= end) {
		return errors.New("native committed checkpoint does not cover cutover")
	}
	return nil
}

func validCheckpointID(id string, generation int64) bool {
	parts := strings.SplitN(id, ":", 4)
	if len(parts) != 3 {
		return false
	}
	g, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || g <= 0 || g != generation || strconv.FormatInt(g, 10) != parts[0] {
		return false
	}
	for i, part := range parts[1:] {
		value, err := strconv.ParseUint(part, 10, 64)
		if err != nil || (i == 1 && value == 0) || strconv.FormatUint(value, 10) != part {
			return false
		}
	}
	return true
}

func sameLSN(a, b string) bool {
	x, err1 := sentinel.ParseLSN(a)
	y, err2 := sentinel.ParseLSN(b)
	return err1 == nil && err2 == nil && x == y
}

func validateCleanup(c *Cleanup, obligations []ResourceObligation) error {
	if c.Complete == nil || c.Resources == nil || len(c.Resources) != len(obligations) || (*c.Complete && len(obligations) == 0) {
		return errors.New("native cleanup outcome or inventory is absent")
	}
	want := make(map[[2]string]ResourceObligation, len(obligations))
	for _, r := range obligations {
		key := [2]string{r.Kind, r.Name}
		if !slices.Contains([]string{"replication_slot", "publication", "replication_origin", resourceKindWorkdir}, r.Kind) || !validLabel(r.Name) ||
			(r.Kind == "replication_origin" && r.Owned) || (r.Kind == resourceKindWorkdir && r.Owned && !r.Retain) ||
			(r.Owned && r.Retain && r.Kind != resourceKindWorkdir) {
			return errors.New("invalid native cleanup obligations")
		}
		if _, exists := want[key]; exists {
			return errors.New("duplicate native cleanup obligation")
		}
		want[key] = r
	}
	for _, r := range c.Resources {
		key := [2]string{r.Kind, r.Name}
		expected, exists := want[key]
		if !exists || r.Owned == nil || r.Removed == nil || *r.Owned != expected.Owned ||
			((!expected.Owned || expected.Retain) && *r.Removed) ||
			(*c.Complete && expected.Owned && !expected.Retain && !*r.Removed) {
			return errors.New("unsafe or incomplete native cleanup ownership")
		}
		delete(want, key)
	}
	return nil
}

// Decoder normally accepts duplicate members, which could select a different proof in another consumer.
func strictJSON(raw []byte) bool {
	if !utf8.Valid(raw) || !bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{")) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if !uniqueValue(d, 0) {
		return false
	}
	_, err := d.Token()
	return errors.Is(err, io.EOF)
}

func uniqueValue(d *json.Decoder, depth int) bool {
	if depth > 32 {
		return false
	}
	token, err := d.Token()
	if err != nil {
		return false
	}
	delim, container := token.(json.Delim)
	if !container {
		return true
	}
	seen := map[string]bool{}
	for d.More() {
		if delim == '{' {
			key, keyErr := d.Token()
			name, ok := key.(string)
			if keyErr != nil || !ok || !canonicalMember(name) || seen[name] {
				return false
			}
			seen[name] = true
		}
		if !uniqueValue(d, depth+1) {
			return false
		}
	}
	_, err = d.Token()
	return err == nil
}

// encoding/json also matches case variants, so enforce the v1 spellings before decoding.
func canonicalMember(name string) bool {
	switch name {
	case "protocol_version", "engine_version", "command", "capabilities", "work", "phase", "copy", "replication", "cleanup",
		"migration_uid", "run_id", "workdir_id", "checkpoint_generation", "tables", "indexes", "bytes", "total", "done",
		"source_durable_head_lsn", "received_lsn", "certified_lsn", "durable_replay_lsn", "end_lsn", "end_persisted", "drained", "checkpoint_id", "drain_proof",
		"committed", "replay_lsn", "complete", "resources", "kind", "name", "owned", "removed":
		return true
	default:
		return false
	}
}

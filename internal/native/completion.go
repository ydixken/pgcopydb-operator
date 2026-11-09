package native

import "errors"

// Clone counters gate base-copy completion; they supply no follow or target authority.
func ValidateCompletedClone(report *Report, work WorkIdentity) error {
	if report == nil || report.ProtocolVersion != 1 || !validLabel(report.EngineVersion) || report.Command != nativeCloneCommand || report.Phase != "completed" || report.Work == nil || !validIdentity(work) || *report.Work != work || report.Copy == nil || RequireCapabilities(report.Capabilities, CapCloneSingleDB) != nil {
		return errors.New("native clone completion unavailable")
	}
	if validatePayload(report, Binding{Command: nativeCloneCommand, Work: work}) != nil {
		return errors.New("native clone completion unavailable")
	}
	for _, counter := range []*Counters{report.Copy.Tables, report.Copy.Indexes, report.Copy.Bytes} {
		if counter == nil || counter.Total == nil || counter.Done == nil || *counter.Total < 0 || *counter.Done != *counter.Total {
			return errors.New("native clone completion unavailable")
		}
	}
	return nil
}

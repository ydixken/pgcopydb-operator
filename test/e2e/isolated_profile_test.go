package e2e

import "testing"

func TestIsolatedAlertProofProfile(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    bool
		wantErr bool
	}{
		{value: ""},
		{value: "identical"},
		{value: "additive-disposable", want: true},
		{value: "yes", wantErr: true},
		{value: "additive", wantErr: true},
		{value: " additive-disposable", wantErr: true},
		{value: "ADDITIVE-DISPOSABLE", wantErr: true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("FEATURE_E2E_SCHEMA_VALIDATION", tc.value)
			got, err := loadIsolatedAlertProof()
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("profile %q: got (%t, %v), want (%t, error=%t)", tc.value, got, err, tc.want, tc.wantErr)
			}
		})
	}
}

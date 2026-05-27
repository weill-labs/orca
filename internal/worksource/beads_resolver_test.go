package worksource

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestBeadsSourceResolveID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		id       string
		results  []fakeBeadsResult
		want     IDPair
		wantErr  error
		wantText string
	}{
		{
			name: "beads id resolves external Linear ref",
			id:   "orca-y5r.1",
			results: []fakeBeadsResult{{
				args:   []string{"show", "orca-y5r.1", "--json"},
				stdout: `[{"id":"orca-y5r.1","title":"Implement source","description":"Adapt bd","priority":2,"labels":["orca"],"issue_type":"task","external_ref":"LAB-1925"}]`,
			}},
			want: IDPair{BeadsID: "orca-y5r.1", LinearID: "LAB-1925"},
		},
		{
			name: "Linear id resolves matching beads id",
			id:   "LAB-1925",
			results: []fakeBeadsResult{{
				args:   []string{"search", "--external-contains", "LAB-1925", "--json"},
				stdout: `[{"id":"orca-y5r.10","external_ref":"LAB-19250"},{"id":"orca-y5r.1","external_ref":"LAB-1925"}]`,
			}},
			want: IDPair{BeadsID: "orca-y5r.1", LinearID: "LAB-1925"},
		},
		{
			name: "missing Linear mapping keeps Linear id",
			id:   "LAB-404",
			results: []fakeBeadsResult{{
				args:   []string{"search", "--external-contains", "LAB-404", "--json"},
				stdout: `[]`,
			}},
			want:    IDPair{LinearID: "LAB-404"},
			wantErr: ErrNotFound,
		},
		{
			name: "search failure includes stderr",
			id:   "LAB-1925",
			results: []fakeBeadsResult{{
				args:   []string{"search", "--external-contains", "LAB-1925", "--json"},
				stderr: "database locked\n",
				err:    errors.New("exit status 1"),
			}},
			want:     IDPair{LinearID: "LAB-1925"},
			wantText: "database locked",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeBeadsRunner{t: t, results: tt.results}
			source := NewBeadsSource("bd", runner)

			got, err := source.ResolveID(context.Background(), tt.id)
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("ResolveID() error = %v, want errors.Is %v", err, tt.wantErr)
			}
			if tt.wantText != "" && (err == nil || !strings.Contains(err.Error(), tt.wantText)) {
				t.Fatalf("ResolveID() error = %v, want substring %q", err, tt.wantText)
			}
			if tt.wantErr == nil && tt.wantText == "" && err != nil {
				t.Fatalf("ResolveID() error = %v, want nil", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ResolveID() = %#v, want %#v", got, tt.want)
			}
			runner.assertDone()
		})
	}
}

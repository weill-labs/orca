package worksource

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type fakeBeadsRunner struct {
	t       *testing.T
	results []fakeBeadsResult
	calls   []fakeBeadsCall
}

type fakeBeadsResult struct {
	bin    string
	args   []string
	stdout string
	stderr string
	err    error
}

type fakeBeadsCall struct {
	bin  string
	args []string
}

func (f *fakeBeadsRunner) run(ctx context.Context, bin string, args ...string) ([]byte, []byte, error) {
	f.t.Helper()

	if len(f.results) == 0 {
		f.t.Fatalf("unexpected run(%q, %v)", bin, args)
	}

	result := f.results[0]
	f.results = f.results[1:]
	f.calls = append(f.calls, fakeBeadsCall{bin: bin, args: append([]string(nil), args...)})

	if result.bin != "" && result.bin != bin {
		f.t.Fatalf("run() bin = %q, want %q", bin, result.bin)
	}
	if !reflect.DeepEqual(args, result.args) {
		f.t.Fatalf("run() args = %#v, want %#v", args, result.args)
	}

	return []byte(result.stdout), []byte(result.stderr), result.err
}

func (f *fakeBeadsRunner) assertDone() {
	f.t.Helper()

	if len(f.results) != 0 {
		f.t.Fatalf("unused fake results = %#v", f.results)
	}
}

func TestBeadsSourceReadyParsesWorkItemsAndSkipsEpics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "ready maps fields and filters epics",
			run: func(t *testing.T) {
				runner := &fakeBeadsRunner{
					t: t,
					results: []fakeBeadsResult{
						{
							bin:  "bd",
							args: []string{"ready", "--json", "--limit", "3"},
							stdout: `[
								{"id":"orca-y5r.1","title":"Implement source","description":"Adapt bd","priority":2,"labels":["orca","worksource"],"issue_type":"task"},
								{"id":"orca-y5r","title":"Epic","description":"Parent","priority":1,"labels":["epic"],"issue_type":"epic"},
								{"id":"orca-y5r.2","title":"Wire source","description":"Phase 3","priority":3,"labels":[],"issue_type":"feature"}
							]`,
							stderr: "warning: Linear data has never been pulled\n",
						},
					},
				}
				source := NewBeadsSource("", runner)

				items, err := source.Ready(context.Background(), 3)
				if err != nil {
					t.Fatalf("Ready() error = %v, want nil", err)
				}

				want := []WorkItem{
					{ID: "orca-y5r.1", Title: "Implement source", Body: "Adapt bd", Priority: 2, Labels: []string{"orca", "worksource"}},
					{ID: "orca-y5r.2", Title: "Wire source", Body: "Phase 3", Priority: 3, Labels: []string{}},
				}
				if !reflect.DeepEqual(items, want) {
					t.Fatalf("Ready() items = %#v, want %#v", items, want)
				}
				runner.assertDone()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}

func TestBeadsSourceGetParsesFirstResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "get unwraps array",
			run: func(t *testing.T) {
				runner := &fakeBeadsRunner{
					t: t,
					results: []fakeBeadsResult{
						{
							args:   []string{"show", "orca-y5r.1", "--json"},
							stdout: `[{"id":"orca-y5r.1","title":"Implement source","description":"Adapt bd","priority":2,"labels":["orca"],"issue_type":"task"}]`,
							stderr: "warning: Linear data has never been pulled\n",
						},
					},
				}
				source := NewBeadsSource("bd-test", runner)

				item, err := source.Get(context.Background(), "orca-y5r.1")
				if err != nil {
					t.Fatalf("Get() error = %v, want nil", err)
				}

				want := WorkItem{ID: "orca-y5r.1", Title: "Implement source", Body: "Adapt bd", Priority: 2, Labels: []string{"orca"}}
				if !reflect.DeepEqual(item, want) {
					t.Fatalf("Get() item = %#v, want %#v", item, want)
				}
				runner.assertDone()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}

func TestBeadsSourceMutations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, source *BeadsSource) error
		want []string
		out  string
		err  error
	}{
		{
			name: "claim success",
			run: func(t *testing.T, source *BeadsSource) error {
				return source.Claim(context.Background(), "orca-y5r.1", "worker-1")
			},
			want: []string{"update", "orca-y5r.1", "--claim", "--actor", "worker-1", "--json"},
			out:  `[{"id":"orca-y5r.1","status":"in_progress"}]`,
		},
		{
			name: "claim already claimed",
			run: func(t *testing.T, source *BeadsSource) error {
				err := source.Claim(context.Background(), "orca-y5r.1", "worker-1")
				if !errors.Is(err, ErrAlreadyClaimed) {
					t.Fatalf("Claim() error = %v, want ErrAlreadyClaimed", err)
				}
				return nil
			},
			want: []string{"update", "orca-y5r.1", "--claim", "--actor", "worker-1", "--json"},
			err:  errors.New("exit status 1"),
		},
		{
			name: "release clears assignee",
			run: func(t *testing.T, source *BeadsSource) error {
				return source.Release(context.Background(), "orca-y5r.1", "no worker available")
			},
			want: []string{"update", "orca-y5r.1", "--status", "open", "--assignee", "", "--json"},
			out:  `[{"id":"orca-y5r.1","status":"open"}]`,
		},
		{
			name: "complete merged closes with suggest next object",
			run: func(t *testing.T, source *BeadsSource) error {
				return source.Complete(context.Background(), "orca-y5r.1", OutcomeMerged)
			},
			want: []string{"close", "orca-y5r.1", "--suggest-next", "--json"},
			out:  `{"closed":["orca-y5r.1"],"unblocked":["orca-y5r.2"]}`,
		},
		{
			name: "complete abandoned releases",
			run: func(t *testing.T, source *BeadsSource) error {
				return source.Complete(context.Background(), "orca-y5r.1", OutcomeAbandoned)
			},
			want: []string{"update", "orca-y5r.1", "--status", "open", "--assignee", "", "--json"},
			out:  `[{"id":"orca-y5r.1","status":"open"}]`,
		},
		{
			name: "complete failed blocks",
			run: func(t *testing.T, source *BeadsSource) error {
				return source.Complete(context.Background(), "orca-y5r.1", OutcomeFailed)
			},
			want: []string{"update", "orca-y5r.1", "--status", "blocked", "--json"},
			out:  `[{"id":"orca-y5r.1","status":"blocked"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stderr := ""
			if tt.name == "claim already claimed" {
				stderr = "Error claiming orca-y5r.1: issue already claimed by worker-2\n"
			}
			runner := &fakeBeadsRunner{
				t: t,
				results: []fakeBeadsResult{
					{args: tt.want, stdout: tt.out, stderr: stderr, err: tt.err},
				},
			}
			source := NewBeadsSource("bd", runner)

			if err := tt.run(t, source); err != nil {
				t.Fatalf("%s error = %v, want nil", tt.name, err)
			}
			runner.assertDone()
		})
	}
}

func TestBeadsSourceVerify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		results   []fakeBeadsResult
		wantError string
	}{
		{
			name: "pass",
			results: []fakeBeadsResult{
				{args: []string{"version", "--json"}, stdout: `{"version":"1.0.4"}`},
				{args: []string{"dolt", "--help"}, stdout: "usage: bd dolt\n"},
			},
		},
		{
			name: "fail br style version string",
			results: []fakeBeadsResult{
				{args: []string{"version", "--json"}, stdout: "br 0.20.0\n"},
			},
			wantError: "bd version --json",
		},
		{
			name: "fail missing dolt subcommand",
			results: []fakeBeadsResult{
				{args: []string{"version", "--json"}, stdout: `{"version":"1.0.4"}`},
				{args: []string{"dolt", "--help"}, stderr: "unknown command: dolt\n", err: errors.New("exit status 1")},
			},
			wantError: "bd dolt --help",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeBeadsRunner{t: t, results: tt.results}
			source := NewBeadsSource("bd", runner)

			err := source.Verify(context.Background())
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("Verify() error = %v, want nil", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Verify() error = %v, want substring %q", err, tt.wantError)
			}
			runner.assertDone()
		})
	}
}

package ci

import (
	"strings"
	"testing"
	"time"
)

func TestParseRegistry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    Registry
		wantErr string
	}{
		{
			name: "valid registry",
			input: `
[[flake]]
package = "github.com/weill-labs/orca/internal/daemon"
test = "TestKnownDaemonFlake"
last_seen = "2026-04-01"
issue = "LAB-701"

[[flake]]
package = "github.com/weill-labs/orca/internal/cli"
test = "TestKnownCLIFlake"
last_seen = "2026-03-15"
issue = "LAB-702"
`,
			want: Registry{
				Entries: map[TestKey]RegistryEntry{
					{Package: "github.com/weill-labs/orca/internal/daemon", Test: "TestKnownDaemonFlake"}: {
						Package:  "github.com/weill-labs/orca/internal/daemon",
						Test:     "TestKnownDaemonFlake",
						LastSeen: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
						Issue:    "LAB-701",
					},
					{Package: "github.com/weill-labs/orca/internal/cli", Test: "TestKnownCLIFlake"}: {
						Package:  "github.com/weill-labs/orca/internal/cli",
						Test:     "TestKnownCLIFlake",
						LastSeen: time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC),
						Issue:    "LAB-702",
					},
				},
			},
		},
		{
			name: "duplicate package and test rejected",
			input: `
[[flake]]
package = "github.com/weill-labs/orca/internal/daemon"
test = "TestKnownDaemonFlake"
last_seen = "2026-04-01"
issue = "LAB-701"

[[flake]]
package = "github.com/weill-labs/orca/internal/daemon"
test = "TestKnownDaemonFlake"
last_seen = "2026-04-02"
issue = "LAB-999"
`,
			wantErr: "duplicate flake entry",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseRegistry(strings.NewReader(tt.input))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseRegistry() error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRegistry() error = %v", err)
			}

			if len(got.Entries) != len(tt.want.Entries) {
				t.Fatalf("ParseRegistry() entry count = %d, want %d", len(got.Entries), len(tt.want.Entries))
			}

			for key, wantEntry := range tt.want.Entries {
				gotEntry, ok := got.Entries[key]
				if !ok {
					t.Fatalf("ParseRegistry() missing key %+v", key)
				}
				if gotEntry != wantEntry {
					t.Fatalf("ParseRegistry() entry %+v = %+v, want %+v", key, gotEntry, wantEntry)
				}
			}
		})
	}
}

func TestAnalyzeTestOutput(t *testing.T) {
	t.Parallel()

	registry := Registry{
		Entries: map[TestKey]RegistryEntry{
			{
				Package: "github.com/weill-labs/orca/internal/daemon",
				Test:    "TestKnownDaemonFlake",
			}: {
				Package:  "github.com/weill-labs/orca/internal/daemon",
				Test:     "TestKnownDaemonFlake",
				LastSeen: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
				Issue:    "LAB-701",
			},
			{
				Package: "github.com/weill-labs/orca/internal/daemon",
				Test:    "TestKnownDaemonFlake/subcase",
			}: {
				Package:  "github.com/weill-labs/orca/internal/daemon",
				Test:     "TestKnownDaemonFlake/subcase",
				LastSeen: time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC),
				Issue:    "LAB-702",
			},
		},
	}

	tests := []struct {
		name     string
		input    string
		want     Report
		wantErr  string
		summary  []string
		reject   []string
		attempt  int
		maxTries int
		rerun    bool
	}{
		{
			name: "known failures only are rerunnable",
			input: strings.Join([]string{
				testEventJSON("fail", "github.com/weill-labs/orca/internal/daemon", "TestKnownDaemonFlake"),
				testEventJSON("fail", "github.com/weill-labs/orca/internal/daemon", "TestKnownDaemonFlake"),
			}, "\n"),
			want: Report{
				KnownFailures: []KnownFailure{
					{
						Failure: Failure{
							Package:     "github.com/weill-labs/orca/internal/daemon",
							Test:        "TestKnownDaemonFlake",
							Occurrences: 2,
						},
						LastSeen: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
						Issue:    "LAB-701",
					},
				},
				ShouldRerun: true,
			},
			summary:  []string{"Known flaky failures detected", "LAB-701", "auto-rerun"},
			reject:   []string{"new failure — investigate"},
			attempt:  1,
			maxTries: 2,
			rerun:    true,
		},
		{
			name: "failing subtest suppresses parent test failure",
			input: strings.Join([]string{
				testEventJSON("fail", "github.com/weill-labs/orca/internal/daemon", "TestKnownDaemonFlake/subcase"),
				testEventJSON("fail", "github.com/weill-labs/orca/internal/daemon", "TestKnownDaemonFlake"),
			}, "\n"),
			want: Report{
				KnownFailures: []KnownFailure{
					{
						Failure: Failure{
							Package:     "github.com/weill-labs/orca/internal/daemon",
							Test:        "TestKnownDaemonFlake/subcase",
							Occurrences: 1,
						},
						LastSeen: time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC),
						Issue:    "LAB-702",
					},
				},
				ShouldRerun: true,
			},
			attempt:  1,
			maxTries: 2,
			rerun:    true,
		},
		{
			name: "unknown test and package failures block rerun",
			input: strings.Join([]string{
				testEventJSON("fail", "github.com/weill-labs/orca/internal/daemon", "TestKnownDaemonFlake"),
				testEventJSON("fail", "github.com/weill-labs/orca/internal/daemon", "TestBrandNewFailure"),
				testEventJSON("fail", "github.com/weill-labs/orca/internal/daemon", ""),
			}, "\n"),
			want: Report{
				KnownFailures: []KnownFailure{
					{
						Failure: Failure{
							Package:     "github.com/weill-labs/orca/internal/daemon",
							Test:        "TestKnownDaemonFlake",
							Occurrences: 1,
						},
						LastSeen: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
						Issue:    "LAB-701",
					},
				},
				UnknownFailures: []Failure{
					{
						Package:     "github.com/weill-labs/orca/internal/daemon",
						Test:        "TestBrandNewFailure",
						Occurrences: 1,
					},
				},
				PackageFailures: []PackageFailure{
					{
						Package:     "github.com/weill-labs/orca/internal/daemon",
						Occurrences: 1,
					},
				},
				ShouldRerun: false,
			},
			summary:  []string{"new failure — investigate", "TestBrandNewFailure", "package-level failure"},
			attempt:  1,
			maxTries: 2,
			rerun:    false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := AnalyzeTestOutput(strings.NewReader(tt.input), registry)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("AnalyzeTestOutput() error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("AnalyzeTestOutput() error = %v", err)
			}

			assertKnownFailures(t, got.KnownFailures, tt.want.KnownFailures)
			assertFailures(t, got.UnknownFailures, tt.want.UnknownFailures)
			assertPackageFailures(t, got.PackageFailures, tt.want.PackageFailures)
			if got.ShouldRerun != tt.want.ShouldRerun {
				t.Fatalf("AnalyzeTestOutput() ShouldRerun = %v, want %v", got.ShouldRerun, tt.want.ShouldRerun)
			}

			summary := RenderSummary(got)
			for _, want := range tt.summary {
				if !strings.Contains(summary, want) {
					t.Fatalf("RenderSummary() = %q, want substring %q", summary, want)
				}
			}
			for _, reject := range tt.reject {
				if strings.Contains(summary, reject) {
					t.Fatalf("RenderSummary() = %q, did not want substring %q", summary, reject)
				}
			}

			if rerun := ShouldAutoRerun(got, tt.attempt, tt.maxTries); rerun != tt.rerun {
				t.Fatalf("ShouldAutoRerun() = %v, want %v", rerun, tt.rerun)
			}
		})
	}
}

func assertKnownFailures(t *testing.T, got, want []KnownFailure) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("known failure count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("known failure[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func assertFailures(t *testing.T, got, want []Failure) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("failure count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("failure[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func assertPackageFailures(t *testing.T, got, want []PackageFailure) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("package failure count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("package failure[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func testEventJSON(action, pkg, test string) string {
	if test == "" {
		return `{"Action":"` + action + `","Package":"` + pkg + `"}`
	}
	return `{"Action":"` + action + `","Package":"` + pkg + `","Test":"` + test + `"}`
}

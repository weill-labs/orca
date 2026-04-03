package ci

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseRegistryValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "invalid toml",
			input:   `[[flake]`,
			wantErr: "parse registry",
		},
		{
			name: "missing package",
			input: `
[[flake]]
test = "TestKnownDaemonFlake"
last_seen = "2026-04-03"
issue = "LAB-693"
`,
			wantErr: "missing package",
		},
		{
			name: "missing test",
			input: `
[[flake]]
package = "github.com/weill-labs/orca/internal/daemon"
last_seen = "2026-04-03"
issue = "LAB-693"
`,
			wantErr: "missing test",
		},
		{
			name: "missing last seen",
			input: `
[[flake]]
package = "github.com/weill-labs/orca/internal/daemon"
test = "TestKnownDaemonFlake"
issue = "LAB-693"
`,
			wantErr: "missing last_seen",
		},
		{
			name: "missing issue",
			input: `
[[flake]]
package = "github.com/weill-labs/orca/internal/daemon"
test = "TestKnownDaemonFlake"
last_seen = "2026-04-03"
`,
			wantErr: "missing issue",
		},
		{
			name: "invalid date",
			input: `
[[flake]]
package = "github.com/weill-labs/orca/internal/daemon"
test = "TestKnownDaemonFlake"
last_seen = "soon"
issue = "LAB-693"
`,
			wantErr: "parse registry date",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseRegistry(strings.NewReader(tt.input))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ParseRegistry() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadRegistry(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "registry.toml")
	if err := os.WriteFile(path, []byte(`
[[flake]]
package = "github.com/weill-labs/orca/internal/daemon"
test = "TestKnownDaemonFlake"
last_seen = "2026-04-03"
issue = "LAB-693"
`), 0o644); err != nil {
		t.Fatalf("WriteFile(registry) error = %v", err)
	}

	registry, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("LoadRegistry() error = %v", err)
	}

	entry, ok := registry.Entries[TestKey{
		Package: "github.com/weill-labs/orca/internal/daemon",
		Test:    "TestKnownDaemonFlake",
	}]
	if !ok {
		t.Fatal("LoadRegistry() missing expected entry")
	}
	if entry.Issue != "LAB-693" {
		t.Fatalf("LoadRegistry() issue = %q, want %q", entry.Issue, "LAB-693")
	}
}

func TestLoadRegistryReturnsOpenError(t *testing.T) {
	t.Parallel()

	_, err := LoadRegistry(filepath.Join(t.TempDir(), "missing.toml"))
	if err == nil || !strings.Contains(err.Error(), "open registry") {
		t.Fatalf("LoadRegistry() error = %v, want open error", err)
	}
}

func TestAnalyzeTestOutputEmptyInput(t *testing.T) {
	t.Parallel()

	report, err := AnalyzeTestOutput(strings.NewReader(""), Registry{})
	if err != nil {
		t.Fatalf("AnalyzeTestOutput() error = %v", err)
	}

	if len(report.KnownFailures) != 0 || len(report.UnknownFailures) != 0 || len(report.PackageFailures) != 0 {
		t.Fatalf("AnalyzeTestOutput() = %+v, want empty report", report)
	}
	if report.ShouldRerun {
		t.Fatal("AnalyzeTestOutput() ShouldRerun = true, want false")
	}

	summary := RenderSummary(report)
	if !strings.Contains(summary, "No flaky failures detected.") {
		t.Fatalf("RenderSummary() = %q, want empty summary text", summary)
	}
}

func TestAnalyzeTestOutputSortsFailures(t *testing.T) {
	t.Parallel()

	registry := Registry{
		Entries: map[TestKey]RegistryEntry{
			{Package: "github.com/weill-labs/orca/internal/a", Test: "TestB"}: {
				Package:  "github.com/weill-labs/orca/internal/a",
				Test:     "TestB",
				LastSeen: time.Date(2026, 4, 3, 0, 0, 0, 0, time.UTC),
				Issue:    "LAB-100",
			},
			{Package: "github.com/weill-labs/orca/internal/a", Test: "TestA"}: {
				Package:  "github.com/weill-labs/orca/internal/a",
				Test:     "TestA",
				LastSeen: time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC),
				Issue:    "LAB-101",
			},
			{Package: "github.com/weill-labs/orca/internal/b", Test: "TestC"}: {
				Package:  "github.com/weill-labs/orca/internal/b",
				Test:     "TestC",
				LastSeen: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
				Issue:    "LAB-102",
			},
		},
	}

	input := strings.Join([]string{
		`{"Action":"fail","Package":"github.com/weill-labs/orca/internal/b","Test":"TestC"}`,
		`{"Action":"fail","Package":"github.com/weill-labs/orca/internal/a","Test":"TestB"}`,
		`{"Action":"fail","Package":"github.com/weill-labs/orca/internal/a","Test":"TestA"}`,
		`{"Action":"fail","Package":"github.com/weill-labs/orca/internal/y","Test":"TestD"}`,
		`{"Action":"fail","Package":"github.com/weill-labs/orca/internal/y","Test":"TestC"}`,
		`{"Action":"fail","Package":"github.com/weill-labs/orca/internal/w","Test":"TestB"}`,
		`{"Action":"fail","Package":"github.com/weill-labs/orca/internal/z"}`,
		`{"Action":"fail","Package":"github.com/weill-labs/orca/internal/x"}`,
	}, "\n")

	report, err := AnalyzeTestOutput(strings.NewReader(input), registry)
	if err != nil {
		t.Fatalf("AnalyzeTestOutput() error = %v", err)
	}

	if got := report.KnownFailures[0].Failure; got.Package != "github.com/weill-labs/orca/internal/a" || got.Test != "TestA" {
		t.Fatalf("known[0] = %+v, want package a test A", got)
	}
	if got := report.KnownFailures[1].Failure; got.Package != "github.com/weill-labs/orca/internal/a" || got.Test != "TestB" {
		t.Fatalf("known[1] = %+v, want package a test B", got)
	}
	if got := report.KnownFailures[2].Failure; got.Package != "github.com/weill-labs/orca/internal/b" || got.Test != "TestC" {
		t.Fatalf("known[2] = %+v, want package b test C", got)
	}
	if got := report.UnknownFailures[0]; got.Package != "github.com/weill-labs/orca/internal/w" || got.Test != "TestB" {
		t.Fatalf("unknown[0] = %+v, want package w test B", got)
	}
	if got := report.UnknownFailures[1]; got.Package != "github.com/weill-labs/orca/internal/y" || got.Test != "TestC" {
		t.Fatalf("unknown[1] = %+v, want package y test C", got)
	}
	if got := report.UnknownFailures[2]; got.Package != "github.com/weill-labs/orca/internal/y" || got.Test != "TestD" {
		t.Fatalf("unknown[2] = %+v, want package y test D", got)
	}
	if got := report.PackageFailures[0]; got.Package != "github.com/weill-labs/orca/internal/x" {
		t.Fatalf("package[0] = %+v, want package x", got)
	}
	if got := report.PackageFailures[1]; got.Package != "github.com/weill-labs/orca/internal/z" {
		t.Fatalf("package[1] = %+v, want package z", got)
	}
}

func TestShouldAutoRerunBoundaries(t *testing.T) {
	t.Parallel()

	report := Report{ShouldRerun: true}

	tests := []struct {
		name        string
		runAttempt  int
		maxAttempts int
		want        bool
	}{
		{name: "before limit", runAttempt: 1, maxAttempts: 2, want: true},
		{name: "at limit", runAttempt: 2, maxAttempts: 2, want: false},
		{name: "zero attempt", runAttempt: 0, maxAttempts: 2, want: false},
		{name: "zero max attempts", runAttempt: 1, maxAttempts: 0, want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := ShouldAutoRerun(report, tt.runAttempt, tt.maxAttempts); got != tt.want {
				t.Fatalf("ShouldAutoRerun() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReportRoundTrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	want := Report{
		ShouldRerun: true,
	}
	if err := WriteReport(&buf, want); err != nil {
		t.Fatalf("WriteReport() error = %v", err)
	}

	got, err := ReadReport(&buf)
	if err != nil {
		t.Fatalf("ReadReport() error = %v", err)
	}

	if !got.ShouldRerun {
		t.Fatal("ReadReport() ShouldRerun = false, want true")
	}
	if got.KnownFailures == nil || got.UnknownFailures == nil || got.PackageFailures == nil {
		t.Fatalf("ReadReport() slices were not normalized: %+v", got)
	}
}

func TestWriteReportEncodeError(t *testing.T) {
	t.Parallel()

	err := WriteReport(errWriter{}, Report{})
	if err == nil || !strings.Contains(err.Error(), "encode report") {
		t.Fatalf("WriteReport() error = %v, want encode error", err)
	}
}

func TestReadReportDecodeError(t *testing.T) {
	t.Parallel()

	_, err := ReadReport(strings.NewReader("{"))
	if err == nil || !strings.Contains(err.Error(), "decode report") {
		t.Fatalf("ReadReport() error = %v, want decode error", err)
	}
}

type errWriter struct{}

func (errWriter) Write(p []byte) (int, error) {
	return 0, bytes.ErrTooLarge
}

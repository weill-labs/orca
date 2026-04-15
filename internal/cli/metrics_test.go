package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestParseMetricsSince(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr string
	}{
		{name: "default empty", value: "", want: 7 * 24 * time.Hour},
		{name: "days suffix", value: "1.5d", want: 36 * time.Hour},
		{name: "time parse duration", value: "12h", want: 12 * time.Hour},
		{name: "zero duration rejected", value: "0h", wantErr: "greater than zero"},
		{name: "invalid duration rejected", value: "bad", wantErr: "parse --since"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseMetricsSince(tt.value)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseMetricsSince(%q) error = %v, want substring %q", tt.value, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMetricsSince(%q) error = %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("parseMetricsSince(%q) = %s, want %s", tt.value, got, tt.want)
			}
		})
	}
}

func TestFormatLatencyDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{name: "zero rounds to zero seconds", duration: 0, want: "0s"},
		{name: "seconds only", duration: 30 * time.Second, want: "30s"},
		{name: "minutes and seconds", duration: 90 * time.Second, want: "1m30s"},
		{name: "hours only", duration: 2 * time.Hour, want: "2h"},
		{name: "hours and seconds without empty minutes", duration: time.Hour + 30*time.Second, want: "1h30s"},
		{name: "hours minutes and seconds", duration: 2*time.Hour + 15*time.Minute + 4*time.Second, want: "2h15m4s"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got, want := formatLatencyDuration(tt.duration), tt.want; got != want {
				t.Fatalf("formatLatencyDuration(%s) = %q, want %q", tt.duration, got, want)
			}
		})
	}
}

func TestQueryLatencyMetricsAndWriteOutput(t *testing.T) {
	t.Parallel()

	store := newMetricsStore(t)
	projectPath := newRepoRoot(t)
	now := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventTaskAssigned, Issue: "LAB-1282-A", Message: "assigned", CreatedAt: time.Date(2026, 4, 9, 10, 0, 0, 0, time.UTC)})
	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventPRDetected, Issue: "LAB-1282-A", Message: "pr detected", CreatedAt: time.Date(2026, 4, 9, 13, 30, 0, 0, time.UTC)})
	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventReviewApproved, Issue: "LAB-1282-A", Message: "approved", CreatedAt: time.Date(2026, 4, 9, 14, 30, 0, 0, time.UTC)})
	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventPRMerged, Issue: "LAB-1282-A", Message: "merged", CreatedAt: time.Date(2026, 4, 9, 16, 0, 0, 0, time.UTC)})

	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventTaskAssigned, Issue: "LAB-1282-B", Message: "assigned", CreatedAt: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC)})
	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventPRDetected, Issue: "LAB-1282-B", Message: "pr detected", CreatedAt: time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)})
	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventReviewApproved, Issue: "LAB-1282-B", Message: "approved", CreatedAt: time.Date(2026, 4, 10, 15, 0, 0, 0, time.UTC)})
	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventPRMerged, Issue: "LAB-1282-B", Message: "merged", CreatedAt: time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC)})

	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventTaskAssigned, Issue: "LAB-1282-C", Message: "assigned", CreatedAt: time.Date(2026, 4, 12, 9, 0, 0, 0, time.UTC)})

	report, err := queryLatencyMetrics(context.Background(), store, projectPath, "30d", since, now)
	if err != nil {
		t.Fatalf("queryLatencyMetrics() error = %v", err)
	}

	if got, want := report.Project, projectPath; got != want {
		t.Fatalf("report.Project = %q, want %q", got, want)
	}
	if got, want := report.Window, "30d"; got != want {
		t.Fatalf("report.Window = %q, want %q", got, want)
	}
	if got, want := report.Since, since; !got.Equal(want) {
		t.Fatalf("report.Since = %v, want %v", got, want)
	}
	if got, want := report.GeneratedAt, now; !got.Equal(want) {
		t.Fatalf("report.GeneratedAt = %v, want %v", got, want)
	}

	wantMetrics := []latencyMetric{
		{Name: "assign->pr", Count: 2, P50: "2h45m", P90: "3h21m", Mean: "2h45m", Min: "2h", Max: "3h30m"},
		{Name: "pr->approved", Count: 2, P50: "2h", P90: "2h48m", Mean: "2h", Min: "1h", Max: "3h"},
		{Name: "pr->merged", Count: 2, P50: "12h15m", P90: "20h3m", Mean: "12h15m", Min: "2h30m", Max: "22h"},
		{Name: "assign->merged", Count: 2, P50: "15h", P90: "22h12m", Mean: "15h", Min: "6h", Max: "24h"},
	}
	if got, want := report.Metrics, wantMetrics; len(got) != len(want) {
		t.Fatalf("len(report.Metrics) = %d, want %d", len(got), len(want))
	}
	for i := range wantMetrics {
		if got, want := report.Metrics[i], wantMetrics[i]; got != want {
			t.Fatalf("report.Metrics[%d] = %#v, want %#v", i, got, want)
		}
	}

	var buf bytes.Buffer
	if err := writeLatencyMetrics(&buf, report); err != nil {
		t.Fatalf("writeLatencyMetrics() error = %v", err)
	}

	output := buf.String()
	for _, want := range []string{
		"project: " + projectPath,
		"window: last 30d",
		"since: 2026-04-01T00:00:00Z",
		"assign->pr      2      2h45m   3h21m",
		"pr->approved    2      2h      2h48m",
		"pr->merged      2      12h15m  20h3m",
		"assign->merged  2      15h     22h12m",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("writeLatencyMetrics() output = %q, want substring %q", output, want)
		}
	}
}

func TestAppRunMetricsJSON(t *testing.T) {
	t.Parallel()

	store := newMetricsStore(t)
	projectPath := newRepoRoot(t)
	cwdPath := filepath.Join(projectPath, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	now := time.Now().UTC()
	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventTaskAssigned, Issue: "LAB-1282-JSON", Message: "assigned", CreatedAt: now.Add(-2 * time.Hour)})
	appendMetricsEvent(t, store, state.Event{Project: projectPath, Kind: daemon.EventPRDetected, Issue: "LAB-1282-JSON", Message: "pr detected", CreatedAt: now.Add(-time.Hour)})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  &fakeDaemon{},
		State:   store,
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	if err := app.Run(context.Background(), []string{"metrics", "--project", projectPath, "--since", "7d", "--json"}); err != nil {
		t.Fatalf("Run(metrics --json) error = %v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}

	var report latencyMetricsReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("json.Unmarshal(stdout) error = %v", err)
	}
	if got, want := report.Project, projectPath; got != want {
		t.Fatalf("report.Project = %q, want %q", got, want)
	}
	if got, want := report.Window, "7d"; got != want {
		t.Fatalf("report.Window = %q, want %q", got, want)
	}
	if len(report.Metrics) != len(latencyMetricDefinitions) {
		t.Fatalf("len(report.Metrics) = %d, want %d", len(report.Metrics), len(latencyMetricDefinitions))
	}
	if got, want := report.Metrics[0].Name, "assign->pr"; got != want {
		t.Fatalf("report.Metrics[0].Name = %q, want %q", got, want)
	}
	if got, want := report.Metrics[0].Count, 1; got != want {
		t.Fatalf("report.Metrics[0].Count = %d, want %d", got, want)
	}
	if got, want := report.Metrics[0].Min, "1h"; got != want {
		t.Fatalf("report.Metrics[0].Min = %q, want %q", got, want)
	}
}

func TestAppRunMetricsResolvesProjectFlag(t *testing.T) {
	t.Parallel()

	store := newMetricsStore(t)
	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	targetRepo := newRepoRoot(t)
	targetSubdir := filepath.Join(targetRepo, "cmd", "orca")
	if err := os.MkdirAll(targetSubdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", targetSubdir, err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  &fakeDaemon{},
		State:   store,
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	if err := app.Run(context.Background(), []string{"metrics", "--project", targetSubdir, "--json"}); err != nil {
		t.Fatalf("Run(metrics --project) error = %v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	if !strings.Contains(stdout.String(), "\"project\":\""+targetRepo+"\"") {
		t.Fatalf("stdout = %q, want project %q", stdout.String(), targetRepo)
	}
}

func TestIssueLatencyTimelineRecord(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		events []struct {
			kind      string
			createdAt time.Time
		}
		want issueLatencyTimeline
	}{
		{
			name: "keeps first assignment and first valid downstream events",
			events: []struct {
				kind      string
				createdAt time.Time
			}{
				{kind: daemon.EventTaskAssigned, createdAt: base},
				{kind: daemon.EventTaskAssigned, createdAt: base.Add(time.Minute)},
				{kind: daemon.EventPRDetected, createdAt: base.Add(2 * time.Hour)},
				{kind: daemon.EventPRDetected, createdAt: base.Add(3 * time.Hour)},
				{kind: daemon.EventReviewApproved, createdAt: base.Add(4 * time.Hour)},
				{kind: daemon.EventPRMerged, createdAt: base.Add(5 * time.Hour)},
			},
			want: issueLatencyTimeline{
				assignedAt:       base,
				prDetectedAt:     base.Add(2 * time.Hour),
				reviewApprovedAt: base.Add(4 * time.Hour),
				prMergedAt:       base.Add(5 * time.Hour),
			},
		},
		{
			name: "replaces out of order pr detection once assignment is known",
			events: []struct {
				kind      string
				createdAt time.Time
			}{
				{kind: daemon.EventPRDetected, createdAt: base.Add(-time.Hour)},
				{kind: daemon.EventTaskAssigned, createdAt: base},
				{kind: daemon.EventPRDetected, createdAt: base.Add(2 * time.Hour)},
			},
			want: issueLatencyTimeline{
				assignedAt:   base,
				prDetectedAt: base.Add(2 * time.Hour),
			},
		},
		{
			name: "ignores approval and merge before pr detection",
			events: []struct {
				kind      string
				createdAt time.Time
			}{
				{kind: daemon.EventTaskAssigned, createdAt: base},
				{kind: daemon.EventReviewApproved, createdAt: base.Add(time.Hour)},
				{kind: daemon.EventPRMerged, createdAt: base.Add(2 * time.Hour)},
				{kind: daemon.EventPRDetected, createdAt: base.Add(3 * time.Hour)},
				{kind: daemon.EventReviewApproved, createdAt: base.Add(4 * time.Hour)},
			},
			want: issueLatencyTimeline{
				assignedAt:       base,
				prDetectedAt:     base.Add(3 * time.Hour),
				reviewApprovedAt: base.Add(4 * time.Hour),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var timeline issueLatencyTimeline
			for _, event := range tt.events {
				timeline.record(event.kind, event.createdAt)
			}

			if got, want := timeline, tt.want; got != want {
				t.Fatalf("timeline = %#v, want %#v", got, want)
			}
		})
	}
}

func newMetricsStore(t *testing.T) *state.SQLiteStore {
	t.Helper()

	store, err := state.OpenSQLite(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return store
}

func appendMetricsEvent(t *testing.T, store *state.SQLiteStore, event state.Event) {
	t.Helper()

	if _, err := store.AppendEvent(context.Background(), event); err != nil {
		t.Fatalf("AppendEvent(%s %s) error = %v", event.Issue, event.Kind, err)
	}
}

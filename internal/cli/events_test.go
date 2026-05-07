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

func TestEventsPostmortemFilterIsolatesPostmortemView(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	events := []state.Event{
		{ID: 1, Kind: daemon.EventTaskAssigned, Issue: "LAB-1396", WorkerID: "worker-05", Message: "assigned", CreatedAt: now},
		{
			ID:        2,
			Kind:      daemon.EventWorkerPostmortem,
			Issue:     "LAB-1396",
			WorkerID:  "worker-05",
			Message:   "postmortem sent: postmortem command sent",
			Payload:   eventPayload(t, daemon.EventWorkerPostmortem, "LAB-1396", "worker-05", "pane-5"),
			CreatedAt: now.Add(time.Second),
		},
		{
			ID:        3,
			Kind:      daemon.EventTaskCompleted,
			Issue:     "LAB-1396",
			WorkerID:  "worker-05",
			Message:   "task finished",
			Payload:   eventPayload(t, daemon.EventTaskCompleted, "LAB-1396", "worker-05", "pane-5"),
			CreatedAt: now.Add(2 * time.Second),
		},
		{ID: 4, Kind: daemon.EventWorkerNudged, Issue: "LAB-2000", WorkerID: "worker-09", Message: "nudge", CreatedAt: now.Add(3 * time.Second)},
	}

	got := runEventsForTest(t, []string{"--filter", "postmortem"}, events)
	if gotKinds(got) != "worker.postmortem,task.completed" {
		t.Fatalf("postmortem filter kinds = %s, want worker.postmortem,task.completed", gotKinds(got))
	}
}

func TestEventsPostmortemFilterFlagsSentWithoutFollowOn(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	events := []state.Event{
		{
			ID:        1,
			Kind:      daemon.EventWorkerPostmortem,
			Issue:     "LAB-1396",
			WorkerID:  "worker-05",
			Message:   "postmortem sent: postmortem command sent",
			Payload:   eventPayload(t, daemon.EventWorkerPostmortem, "LAB-1396", "worker-05", "pane-5"),
			CreatedAt: now.Add(-10 * time.Minute),
		},
	}

	got := runEventsForTest(t, []string{"--filter", "postmortem"}, events)
	if gotKinds(got) != "worker.postmortem,worker.postmortem_suspicious" {
		t.Fatalf("postmortem filter kinds = %s, want worker.postmortem,worker.postmortem_suspicious", gotKinds(got))
	}
	warning := got[1]
	for _, want := range []string{"LAB-1396", "worker-05", "pane-5", daemon.EventTaskCompleted, daemon.EventTaskCancelled} {
		if !strings.Contains(warning.Message, want) {
			t.Fatalf("suspicious postmortem message = %q, want %q", warning.Message, want)
		}
	}
}

func TestEventsPostmortemFilterDoesNotFlagNormalCleanup(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Add(-10 * time.Minute)
	events := []state.Event{
		{
			ID:        1,
			Kind:      daemon.EventWorkerPostmortem,
			Issue:     "LAB-1396",
			WorkerID:  "worker-05",
			Message:   "postmortem sent: postmortem command sent",
			Payload:   eventPayload(t, daemon.EventWorkerPostmortem, "LAB-1396", "worker-05", "pane-5"),
			CreatedAt: now,
		},
		{
			ID:        2,
			Kind:      daemon.EventTaskCompleted,
			Issue:     "LAB-1396",
			WorkerID:  "worker-05",
			Message:   "task finished",
			Payload:   eventPayload(t, daemon.EventTaskCompleted, "LAB-1396", "worker-05", "pane-5"),
			CreatedAt: now.Add(time.Minute),
		},
	}

	got := runEventsForTest(t, []string{"--filter", "postmortem"}, events)
	if gotKinds(got) != "worker.postmortem,task.completed" {
		t.Fatalf("postmortem filter kinds = %s, want worker.postmortem,task.completed", gotKinds(got))
	}
}

func eventPayload(t *testing.T, kind, issue, workerID, paneID string) json.RawMessage {
	t.Helper()

	payload, err := json.Marshal(daemon.Event{
		Type:     kind,
		Issue:    issue,
		WorkerID: workerID,
		PaneID:   paneID,
		PaneName: "Worker 5",
	})
	if err != nil {
		t.Fatalf("Marshal(payload): %v", err)
	}
	return payload
}

func runEventsForTest(t *testing.T, args []string, events []state.Event) []state.Event {
	t.Helper()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  &fakeDaemon{},
		State:   &fakeState{events: events},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	command := append([]string{"events"}, args...)
	if err := app.Run(context.Background(), command); err != nil {
		t.Fatalf("Run(%v) error = %v, stderr = %q", command, err, stderr.String())
	}
	return decodeEventLines(t, stdout.String())
}

func decodeEventLines(t *testing.T, output string) []state.Event {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(output), "\n")
	events := make([]state.Event, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event state.Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("Unmarshal(%q): %v", line, err)
		}
		events = append(events, event)
	}
	return events
}

func gotKinds(events []state.Event) string {
	kinds := make([]string, 0, len(events))
	for _, event := range events {
		kinds = append(kinds, event.Kind)
	}
	return strings.Join(kinds, ",")
}

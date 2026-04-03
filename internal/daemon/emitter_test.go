package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNDJSONEmitterWritesLineDelimitedJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	emitter := NewNDJSONEmitter(&buf)
	event := Event{
		Time:    time.Date(2026, 4, 2, 10, 11, 12, 0, time.UTC),
		Type:    EventTaskAssigned,
		Project: "/tmp/project",
		Issue:   "LAB-689",
		Branch:  "LAB-689",
		Message: "assigned",
	}

	if err := emitter.Emit(context.Background(), event); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if got, want := len(lines), 1; got != want {
		t.Fatalf("line count = %d, want %d", got, want)
	}

	var decoded Event
	if err := json.Unmarshal([]byte(lines[0]), &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got, want := decoded.Type, EventTaskAssigned; got != want {
		t.Fatalf("decoded.Type = %q, want %q", got, want)
	}
	if got, want := decoded.Issue, "LAB-689"; got != want {
		t.Fatalf("decoded.Issue = %q, want %q", got, want)
	}
}

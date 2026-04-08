package daemon

import (
	"context"
	"strings"
	"testing"
)

func TestAssignLogsFallbackWindowWhenSpawnUsesNewWindow(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.spawnPane = Pane{ID: "pane-1", Name: "worker-1", Window: "window-2"}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := d.Assign(ctx, "LAB-976", "Implement split-space fallback", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	event, ok := deps.events.lastEventOfType(EventTaskAssigned)
	if !ok {
		t.Fatal("missing task assigned event")
	}
	if !strings.Contains(event.Message, "window-2") {
		t.Fatalf("task assigned event message = %q, want fallback window context", event.Message)
	}
}

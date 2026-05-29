package daemon

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestAssignWaitsForSpawnedPaneBeforeStartupMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())

	var (
		mu  sync.Mutex
		ops []string
	)
	record := func(op string) {
		mu.Lock()
		defer mu.Unlock()
		if len(ops) < 2 {
			ops = append(ops, op)
		}
	}
	deps.amux.waitIdleFunc = func(_ context.Context, paneID string, timeout, settle time.Duration) error {
		if paneID == "pane-1" && timeout == defaultAgentHandshakeTimeout && settle == 0 {
			record("wait_idle")
		}
		if timeout == codexPromptRetryIdleProbeTime && len(snapshotSentKeys(deps.amux)[paneID]) > 0 {
			return errors.New("idle timeout")
		}
		return nil
	}
	deps.amux.setMetadataHook = func(paneID string, metadata map[string]string) {
		if paneID == "pane-1" && metadata["agent_profile"] == "codex" && metadata["branch"] == "LAB-1919" {
			record("set_startup_metadata")
		}
	}

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1919", "Wait for spawned pane readiness", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	mu.Lock()
	got := append([]string(nil), ops...)
	mu.Unlock()
	want := []string{"wait_idle", "set_startup_metadata"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("startup operations = %#v, want %#v", got, want)
	}
}

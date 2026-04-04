package amux

import (
	"context"
	"reflect"
	"testing"
)

func TestCLIClientCapturePane(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{
		output: []byte(`{"id":9,"name":"worker-1","content":["line one","line two"],"cwd":"/tmp/clone-01","current_command":"bash","child_pids":[],"exited":true}`),
	}
	client := newTestClient(Config{Session: "orca-dev"}, runner)

	got, err := client.CapturePane(context.Background(), "pane-9")
	if err != nil {
		t.Fatalf("CapturePane() error = %v", err)
	}

	wantCmd := recordedCommand{
		name: "amux",
		args: []string{
			"-s", "orca-dev",
			"capture",
			"--format", "json",
			"pane-9",
		},
	}
	if !reflect.DeepEqual(runner.calls, []recordedCommand{wantCmd}) {
		t.Fatalf("CapturePane() commands = %#v, want %#v", runner.calls, []recordedCommand{wantCmd})
	}

	if got.Output() != "line one\nline two" {
		t.Fatalf("CapturePane() output = %q, want %q", got.Output(), "line one\nline two")
	}
	if got.CWD != "/tmp/clone-01" {
		t.Fatalf("CapturePane() cwd = %q, want %q", got.CWD, "/tmp/clone-01")
	}
	if got.CurrentCommand != "bash" {
		t.Fatalf("CapturePane() current command = %q, want %q", got.CurrentCommand, "bash")
	}
	if len(got.ChildPIDs) != 0 {
		t.Fatalf("CapturePane() child pids = %#v, want empty", got.ChildPIDs)
	}
	if !got.Exited {
		t.Fatal("CapturePane() exited = false, want true")
	}
}

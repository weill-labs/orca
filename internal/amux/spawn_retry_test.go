package amux

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCLIClientSpawnFallsBackToNewWindowWhenTargetWindowHasNoSplitSpace(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{
		queue: []runnerResult{
			{
				output: []byte("amux split: not enough space to split (4 < 5)\n"),
				err:    errors.New("exit status 1"),
			},
			{output: []byte("Created window-2\n")},
			{output: []byte("Split horizontal: new pane w-LAB-976\n")},
		},
	}
	client := newTestClient(Config{Session: "main"}, runner)

	gotPane, err := client.Spawn(context.Background(), SpawnRequest{
		AtPane: "lead-pane",
		Name:   "w-LAB-976",
		CWD:    "/tmp/clone-01",
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}

	wantCmds := []recordedCommand{
		{name: "amux", args: []string{"-s", "main", "spawn", "--auto", "--name", "w-LAB-976"}},
		{name: "amux", args: []string{"-s", "main", "new-window"}},
		{name: "amux", args: []string{"-s", "main", "spawn", "--auto", "--name", "w-LAB-976"}},
		{name: "amux", args: []string{"-s", "main", "send-keys", "w-LAB-976", "--delay-final", "250ms", "cd '/tmp/clone-01'"}},
		{name: "amux", args: []string{"-s", "main", "send-keys", "w-LAB-976", "--delay-final", "250ms", "Enter"}},
		{name: "amux", args: []string{"-s", "main", "wait", "idle", "w-LAB-976", "--timeout", "5s"}},
	}
	if !reflect.DeepEqual(runner.calls, wantCmds) {
		t.Fatalf("Spawn() commands = %#v, want %#v", runner.calls, wantCmds)
	}

	wantPane := Pane{ID: "w-LAB-976", Name: "w-LAB-976", Window: "window-2"}
	if gotPane != wantPane {
		t.Fatalf("Spawn() pane = %#v, want %#v", gotPane, wantPane)
	}
}

func TestCLIClientSpawnCreatesMissingTargetWindowBeforeRetry(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{
		queue: []runnerResult{
			{
				output: []byte(`window "alphaosZ" not found`),
				err:    errors.New("exit status 1"),
			},
			{output: []byte("Created alphaosZ\n")},
			{output: []byte("Split horizontal: new pane w-LAB-977\n")},
		},
	}
	client := newTestClient(Config{Session: "main"}, runner)

	gotPane, err := client.Spawn(context.Background(), SpawnRequest{
		Window: "alphaosZ",
		Name:   "w-LAB-977",
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}

	wantCmds := []recordedCommand{
		{name: "amux", args: []string{"-s", "main", "spawn", "--auto", "--window", "alphaosZ", "--name", "w-LAB-977"}},
		{name: "amux", args: []string{"-s", "main", "new-window", "--name", "alphaosZ"}},
		{name: "amux", args: []string{"-s", "main", "spawn", "--auto", "--window", "alphaosZ", "--name", "w-LAB-977"}},
	}
	if !reflect.DeepEqual(runner.calls, wantCmds) {
		t.Fatalf("Spawn() commands = %#v, want %#v", runner.calls, wantCmds)
	}

	wantPane := Pane{ID: "w-LAB-977", Name: "w-LAB-977"}
	if gotPane != wantPane {
		t.Fatalf("Spawn() pane = %#v, want %#v", gotPane, wantPane)
	}
}

func TestCLIClientSpawnRetriesWhenMissingTargetWindowAlreadyExists(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{
		queue: []runnerResult{
			{
				output: []byte(`window "alphaosZ" not found`),
				err:    errors.New("exit status 1"),
			},
			{
				output: []byte(`window "alphaosZ" already exists`),
				err:    errors.New("exit status 1"),
			},
			{output: []byte("Split horizontal: new pane w-LAB-978\n")},
		},
	}
	client := newTestClient(Config{Session: "main"}, runner)

	gotPane, err := client.Spawn(context.Background(), SpawnRequest{
		Window: "alphaosZ",
		Name:   "w-LAB-978",
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}

	wantCmds := []recordedCommand{
		{name: "amux", args: []string{"-s", "main", "spawn", "--auto", "--window", "alphaosZ", "--name", "w-LAB-978"}},
		{name: "amux", args: []string{"-s", "main", "new-window", "--name", "alphaosZ"}},
		{name: "amux", args: []string{"-s", "main", "spawn", "--auto", "--window", "alphaosZ", "--name", "w-LAB-978"}},
	}
	if !reflect.DeepEqual(runner.calls, wantCmds) {
		t.Fatalf("Spawn() commands = %#v, want %#v", runner.calls, wantCmds)
	}

	wantPane := Pane{ID: "w-LAB-978", Name: "w-LAB-978"}
	if gotPane != wantPane {
		t.Fatalf("Spawn() pane = %#v, want %#v", gotPane, wantPane)
	}
}

package daemon

import (
	"context"
	"testing"
)

func TestWrapAssignmentPrompt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		profile AgentProfile
		prompt  string
		want    string
	}{
		{
			name:    "appends codex pr reminder",
			profile: AgentProfile{Name: "codex"},
			prompt:  "Implement daemon core",
			want:    "Implement daemon core\n\nWhen tests pass, commit, push, and open a PR with gh pr create.",
		},
		{
			name:    "does not duplicate codex pr reminder",
			profile: AgentProfile{Name: "codex"},
			prompt:  "Implement daemon core\n\nWhen tests pass, commit, push, and open a PR with gh pr create.",
			want:    "Implement daemon core\n\nWhen tests pass, commit, push, and open a PR with gh pr create.",
		},
		{
			name:    "leaves claude prompts unchanged",
			profile: AgentProfile{Name: "claude"},
			prompt:  "Implement daemon core",
			want:    "Implement daemon core",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := wrapAssignmentPrompt(tt.profile, tt.prompt); got != tt.want {
				t.Fatalf("wrapAssignmentPrompt(%q) = %q, want %q", tt.prompt, got, tt.want)
			}
		})
	}
}

func TestAssignWrapsCodexPromptWithPROpeningInstructions(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-892", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-892")
		return ok && task.Status == TaskStatusActive
	})

	wantPrompt := "Implement daemon core\n\nWhen tests pass, commit, push, and open a PR with gh pr create."
	task, ok := deps.state.task("LAB-892")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got := task.Prompt; got != wantPrompt {
		t.Fatalf("task.Prompt = %q, want %q", got, wantPrompt)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{wantPrompt + "\n"})
}

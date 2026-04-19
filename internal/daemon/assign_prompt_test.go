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
		issue   string
		prompt  string
		want    string
	}{
		{
			name:    "appends codex pr reminder and title convention",
			profile: AgentProfile{Name: "codex"},
			issue:   "LAB-892",
			prompt:  "Implement daemon core",
			want:    wantedCodexAssignmentPrompt("LAB-892", "Implement daemon core"),
		},
		{
			name:    "appends only missing title convention reminder",
			profile: AgentProfile{Name: "codex"},
			issue:   "LAB-893",
			prompt:  "Implement daemon core\n\nWhen tests pass, commit, push, and open a PR with gh pr create.",
			want:    "Implement daemon core\n\n" + wantedCodexAssignmentReminder("LAB-893"),
		},
		{
			name:    "does not duplicate full codex reminder",
			profile: AgentProfile{Name: "codex"},
			issue:   "LAB-894",
			prompt:  wantedCodexAssignmentPrompt("LAB-894", "Implement daemon core"),
			want:    wantedCodexAssignmentPrompt("LAB-894", "Implement daemon core"),
		},
		{
			name:    "omits title convention when issue is empty",
			profile: AgentProfile{Name: "codex"},
			prompt:  "Implement daemon core",
			want:    "Implement daemon core\n\n" + codexAssignmentPromptSuffix,
		},
		{
			name:    "returns reminder block for empty codex prompt",
			profile: AgentProfile{Name: "codex"},
			issue:   "LAB-895",
			want:    wantedCodexAssignmentReminder("LAB-895"),
		},
		{
			name:    "leaves claude prompts unchanged",
			profile: AgentProfile{Name: "claude"},
			issue:   "LAB-892",
			prompt:  "Implement daemon core",
			want:    "Implement daemon core",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := wrapAssignmentPrompt(tt.profile, tt.issue, tt.prompt); got != tt.want {
				t.Fatalf("wrapAssignmentPrompt(%q, %q) = %q, want %q", tt.issue, tt.prompt, got, tt.want)
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

	wantPrompt := wantedCodexAssignmentPrompt("LAB-892", "Implement daemon core")
	task, ok := deps.state.task("LAB-892")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got := task.Prompt; got != wantPrompt {
		t.Fatalf("task.Prompt = %q, want %q", got, wantPrompt)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{wrappedCodexPrompt("LAB-892", "Implement daemon core") + "\n"})
}

func TestAssignAppendsOnlyMissingCodexPRTitleInstructions(t *testing.T) {
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

	prompt := "Implement daemon core\n\nWhen tests pass, commit, push, and open a PR with gh pr create."
	if err := d.Assign(ctx, "LAB-893", prompt, "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-893")
		return ok && task.Status == TaskStatusActive
	})

	wantPrompt := prompt + "\n" + codexAssignmentPRTitlePrompt("LAB-893")
	task, ok := deps.state.task("LAB-893")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got := task.Prompt; got != wantPrompt {
		t.Fatalf("task.Prompt = %q, want %q", got, wantPrompt)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core " + codexAssignmentPromptSuffix + " " + codexAssignmentPRTitlePrompt("LAB-893") + "\n",
	})
}

func wantedCodexAssignmentReminder(issue string) string {
	return codexAssignmentPromptSuffix + "\n" + codexAssignmentPRTitlePrompt(issue)
}

func wantedCodexAssignmentPrompt(issue, prompt string) string {
	return prompt + "\n\n" + wantedCodexAssignmentReminder(issue)
}

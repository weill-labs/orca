package daemon

import (
	"context"
	"strings"
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
			prompt:  "Implement daemon core\n\nWhen tests pass, commit, push, and open a PR with gh pr create --base main.",
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

func TestAppendNotifyConvention(t *testing.T) {
	t.Parallel()

	convention := strings.Join([]string{
		"To notify or ask the lieutenant, run `amux send-keys pane-13 \"<one-line message>\"`.",
		"Use it for: a blocking question before you guess, or a milestone (PR opened).",
		"Keep messages to one line; do not spam.",
	}, "\n")

	tests := []struct {
		name   string
		prompt string
		pane   string
		want   string
	}{
		{
			name:   "appends convention when pane resolves",
			prompt: "Implement daemon core",
			pane:   "pane-13",
			want:   "Implement daemon core\n\n" + convention,
		},
		{
			name:   "omits convention when pane is empty",
			prompt: "Implement daemon core",
			pane:   "",
			want:   "Implement daemon core",
		},
		{
			name:   "does not duplicate existing convention",
			prompt: "Implement daemon core\n\n" + convention,
			pane:   "pane-13",
			want:   "Implement daemon core\n\n" + convention,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := appendNotifyConvention(tt.prompt, tt.pane)
			got = appendNotifyConvention(got, tt.pane)
			if got != tt.want {
				t.Fatalf("appendNotifyConvention() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveNotifyPane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		notifyPane string
		callerPane string
		configPane string
		want       string
	}{
		{
			name:       "notify pane wins",
			notifyPane: "  pane-flag  ",
			callerPane: "pane-caller",
			configPane: "pane-config",
			want:       "pane-flag",
		},
		{
			name:       "falls back to caller pane",
			callerPane: "  pane-caller  ",
			configPane: "pane-config",
			want:       "pane-caller",
		},
		{
			name:       "falls back to config pane",
			configPane: "  pane-config  ",
			want:       "pane-config",
		},
		{
			name: "omits when no pane resolves",
			want: "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := resolveNotifyPane(tt.notifyPane, tt.callerPane, tt.configPane); got != tt.want {
				t.Fatalf("resolveNotifyPane() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAssignAppendsNotifyConventionFromCallerPane(t *testing.T) {
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

	if err := d.AssignWithCallerPane(ctx, "LAB-1946", "Implement worker comms", "codex", "pane-13"); err != nil {
		t.Fatalf("AssignWithCallerPane() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-1946")
		return ok && task.Status == TaskStatusActive
	})

	wantPrompt := appendNotifyConvention(wantedCodexAssignmentPrompt("LAB-1946", "Implement worker comms"), "pane-13")
	task, ok := deps.state.task("LAB-1946")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got := task.Prompt; got != wantPrompt {
		t.Fatalf("task.Prompt = %q, want %q", got, wantPrompt)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{normalizedPromptForTest(t, wantPrompt) + "\n"})
}

func TestAssignAppendsNotifyConventionFromConfigPane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.NotificationPane = "pane-config"
	})
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1947", "Implement config comms", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-1947")
		return ok && task.Status == TaskStatusActive
	})

	wantPrompt := appendNotifyConvention(wantedCodexAssignmentPrompt("LAB-1947", "Implement config comms"), "pane-config")
	task, ok := deps.state.task("LAB-1947")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got := task.Prompt; got != wantPrompt {
		t.Fatalf("task.Prompt = %q, want %q", got, wantPrompt)
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

	prompt := "Implement daemon core\n\nWhen tests pass, commit, push, and open a PR with gh pr create --base main."
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

func normalizedPromptForTest(t *testing.T, prompt string) string {
	t.Helper()

	normalized, err := normalizePromptForDelivery(prompt)
	if err != nil {
		t.Fatalf("normalizePromptForDelivery() error = %v", err)
	}
	return normalized
}

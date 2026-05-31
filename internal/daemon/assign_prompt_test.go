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
		landing LandingConfig
		want    string
	}{
		{
			name:    "appends codex direct landing reminder by default",
			profile: AgentProfile{Name: "codex"},
			issue:   "LAB-892",
			prompt:  "Implement daemon core",
			want:    "Implement daemon core\n\nWhen tests pass, commit, push the branch, and queue direct landing with `orca enqueue LAB-892`. Do not open a GitHub PR.",
		},
		{
			name:    "appends only missing title convention reminder",
			profile: AgentProfile{Name: "codex"},
			issue:   "LAB-893",
			prompt:  "Implement daemon core\n\nWhen tests pass, commit, push, and open a PR with gh pr create --base main.",
			landing: LandingConfig{Mode: LandingModePR, BaseBranch: "main"},
			want:    "Implement daemon core\n\n" + wantedCodexAssignmentReminder("LAB-893"),
		},
		{
			name:    "does not duplicate full codex reminder",
			profile: AgentProfile{Name: "codex"},
			issue:   "LAB-894",
			prompt:  wantedCodexAssignmentPrompt("LAB-894", "Implement daemon core"),
			landing: LandingConfig{Mode: LandingModePR, BaseBranch: "main"},
			want:    wantedCodexAssignmentPrompt("LAB-894", "Implement daemon core"),
		},
		{
			name:    "omits title convention when issue is empty in pr mode",
			profile: AgentProfile{Name: "codex"},
			prompt:  "Implement daemon core",
			landing: LandingConfig{Mode: LandingModePR, BaseBranch: "main"},
			want:    "Implement daemon core\n\n" + codexAssignmentPromptSuffix,
		},
		{
			name:    "returns reminder block for empty codex prompt in pr mode",
			profile: AgentProfile{Name: "codex"},
			issue:   "LAB-895",
			landing: LandingConfig{Mode: LandingModePR, BaseBranch: "main"},
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

			landing := tt.landing
			if !landingConfigExplicit(landing) {
				landing = defaultLandingConfig()
			}
			if got := wrapAssignmentPromptForLanding(tt.profile, tt.issue, tt.prompt, landing); got != tt.want {
				t.Fatalf("wrapAssignmentPrompt(%q, %q) = %q, want %q", tt.issue, tt.prompt, got, tt.want)
			}
		})
	}
}

func TestAppendNotifyConvention(t *testing.T) {
	t.Parallel()

	convention := notifyConventionForTest(`amux msg send --from "$AMUX_PANE" --to 'pane-13' --subject 'LAB-1946' --body "<one-line message>"`)
	spaceConvention := notifyConventionForTest(`amux msg send --from "$AMUX_PANE" --to 'my pane' --subject 'LAB-1946' --body "<one-line message>"`)
	quoteConvention := notifyConventionForTest(`amux msg send --from "$AMUX_PANE" --to 'it'\''s-a-pane' --subject 'LAB-1946' --body "<one-line message>"`)
	subjectConvention := notifyConventionForTest(`amux msg send --from "$AMUX_PANE" --to 'pane-13' --subject 'orca-hmq' --body "<one-line message>"`)

	tests := []struct {
		name   string
		prompt string
		issue  string
		pane   string
		want   string
	}{
		{
			name:   "appends convention when pane resolves",
			prompt: "Implement daemon core",
			issue:  "LAB-1946",
			pane:   "pane-13",
			want:   "Implement daemon core\n\n" + convention,
		},
		{
			name:   "returns convention for empty prompt",
			prompt: "",
			issue:  "LAB-1946",
			pane:   "pane-13",
			want:   convention,
		},
		{
			name:   "quotes pane names with spaces",
			prompt: "Implement daemon core",
			issue:  "LAB-1946",
			pane:   "my pane",
			want:   "Implement daemon core\n\n" + spaceConvention,
		},
		{
			name:   "quotes pane names with single quotes",
			prompt: "Implement daemon core",
			issue:  "LAB-1946",
			pane:   "it's-a-pane",
			want:   "Implement daemon core\n\n" + quoteConvention,
		},
		{
			name:   "subject carries issue id",
			prompt: "Implement daemon core",
			issue:  "orca-hmq",
			pane:   "pane-13",
			want:   "Implement daemon core\n\n" + subjectConvention,
		},
		{
			name:   "omits convention when pane is empty",
			prompt: "Implement daemon core",
			issue:  "LAB-1946",
			pane:   "",
			want:   "Implement daemon core",
		},
		{
			name:   "does not duplicate existing convention",
			prompt: "Implement daemon core\n\n" + convention,
			issue:  "LAB-1946",
			pane:   "pane-13",
			want:   "Implement daemon core\n\n" + convention,
		},
		{
			name:   "does not duplicate existing convention for different pane",
			prompt: "Implement daemon core\n\n" + convention,
			issue:  "LAB-1946",
			pane:   "pane-99",
			want:   "Implement daemon core\n\n" + convention,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := appendNotifyConvention(tt.prompt, tt.issue, tt.pane)
			got = appendNotifyConvention(got, tt.issue, tt.pane)
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

	wantPrompt := appendNotifyConvention(wantedCodexAssignmentPrompt("LAB-1946", "Implement worker comms"), "LAB-1946", "pane-13")
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

	wantPrompt := appendNotifyConvention(wantedCodexAssignmentPrompt("LAB-1947", "Implement config comms"), "LAB-1947", "pane-config")
	task, ok := deps.state.task("LAB-1947")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got := task.Prompt; got != wantPrompt {
		t.Fatalf("task.Prompt = %q, want %q", got, wantPrompt)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{normalizedPromptForTest(t, wantPrompt) + "\n"})
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

func TestAssignWrapsCodexPromptWithDirectLandingInstructions(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.LandingConfig = LandingConfig{Mode: LandingModeDirect, BaseBranch: "main"}
	})
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1969", "Implement direct landing", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-1969")
		return ok && task.Status == TaskStatusActive
	})

	task, ok := deps.state.task("LAB-1969")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if strings.Contains(task.Prompt, "gh pr create") || strings.Contains(task.Prompt, "open a PR") {
		t.Fatalf("direct-mode prompt contains PR-opening instructions: %q", task.Prompt)
	}
	for _, want := range []string{"commit", "push", "orca enqueue LAB-1969"} {
		if !strings.Contains(task.Prompt, want) {
			t.Fatalf("direct-mode prompt = %q, want substring %q", task.Prompt, want)
		}
	}
}

func TestAssignDefaultsToDirectLandingWithoutExternalIntegrations(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemonWithProductDefaults(t, nil)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1969", "Implement local direct landing", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-1969")
		return ok && task.Status == TaskStatusActive
	})

	task, ok := deps.state.task("LAB-1969")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if strings.Contains(task.Prompt, "gh pr create") || strings.Contains(task.Prompt, "open a PR") {
		t.Fatalf("default prompt contains PR-opening instructions: %q", task.Prompt)
	}
	if !strings.Contains(task.Prompt, "orca enqueue LAB-1969") {
		t.Fatalf("default prompt = %q, want direct enqueue instructions", task.Prompt)
	}
	if ghCalls := deps.commands.callsByName("gh"); len(ghCalls) != 0 {
		t.Fatalf("default assignment made GitHub calls: %#v", ghCalls)
	}
	if got := deps.issueTracker.statuses(); len(got) != 0 {
		t.Fatalf("default assignment updated Linear statuses: %#v", got)
	}
	deps.commands.reset()
	result, err := d.EnqueueTarget(ctx, "LAB-1969")
	if err != nil {
		t.Fatalf("EnqueueTarget() error = %v", err)
	}
	if got, want := result.Mode, LandingModeDirect; got != want {
		t.Fatalf("result.Mode = %q, want %q", got, want)
	}
	if got, want := result.BaseBranch, "main"; got != want {
		t.Fatalf("result.BaseBranch = %q, want %q", got, want)
	}
	if ghCalls := deps.commands.callsByName("gh"); len(ghCalls) != 0 {
		t.Fatalf("default direct enqueue made GitHub calls: %#v", ghCalls)
	}
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

func notifyConventionForTest(command string) string {
	return strings.Join([]string{
		"To notify or ask the lead, run `" + command + "`.",
		"Use it for: a blocking question before you guess, or a milestone.",
		"Keep messages to one line; do not spam.",
	}, "\n")
}

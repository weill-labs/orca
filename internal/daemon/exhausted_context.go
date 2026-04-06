package daemon

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const autoReassignContextThreshold = 15

var contextPercentPattern = regexp.MustCompile(`\b(\d{1,3})\s*%`)

func (d *Daemon) exhaustedContextAutoReassignReason(active ActiveAssignment, profile AgentProfile, snapshot PaneCapture) (string, bool) {
	if !strings.EqualFold(profile.Name, "codex") {
		return "", false
	}
	if active.Worker.Health != WorkerHealthEscalated {
		return "", false
	}
	if !looksLikeIdleAgentPrompt(snapshot) {
		return "", false
	}

	remaining, ok := contextRemainingPercent(snapshot)
	if !ok || remaining >= autoReassignContextThreshold {
		return "", false
	}

	return fmt.Sprintf("idle codex prompt with %d%% context remaining", remaining), true
}

func looksLikeIdleAgentPrompt(snapshot PaneCapture) bool {
	lines := captureLines(snapshot)
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		return strings.HasPrefix(line, "›")
	}
	return false
}

func captureLines(snapshot PaneCapture) []string {
	if len(snapshot.Content) > 0 {
		return snapshot.Content
	}
	return strings.Split(snapshot.Output(), "\n")
}

func contextRemainingPercent(snapshot PaneCapture) (int, bool) {
	for _, line := range captureLines(snapshot) {
		line := strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		if !strings.Contains(lower, "context") {
			continue
		}
		matches := contextPercentPattern.FindStringSubmatch(line)
		if len(matches) != 2 {
			continue
		}
		value, err := strconv.Atoi(matches[1])
		if err != nil || value < 0 || value > 100 {
			continue
		}
		return value, true
	}

	return 0, false
}

func (d *Daemon) autoReassignEscalatedWorker(ctx context.Context, active ActiveAssignment, reason string) {
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}

	prompt := d.exhaustedContextReassignPrompt(ctx, active)
	message := fmt.Sprintf("task cancelled for exhausted-context auto-reassign: %s", reason)
	// CLAUDE.md documents exhausted-context recovery as the one automatic cancel/kill exception.
	if err := d.finishAssignmentWithMessage(ctx, active, TaskStatusCancelled, EventTaskCancelled, false, message); err != nil {
		d.emit(ctx, d.assignmentEvent(active, profile, EventTaskCompletionFailed, fmt.Sprintf("auto reassign cancellation failed: %v", err)))
		return
	}

	if err := d.Assign(ctx, active.Task.Issue, prompt, active.Task.AgentProfile); err != nil {
		d.emit(ctx, Event{
			Time:         d.now(),
			Type:         EventTaskAssignFailed,
			Project:      d.project,
			Issue:        active.Task.Issue,
			Branch:       active.Task.Branch,
			AgentProfile: profile.Name,
			PRNumber:     active.Task.PRNumber,
			Message:      fmt.Sprintf("auto reassign failed: %v", err),
		})
	}
}

func (d *Daemon) exhaustedContextReassignPrompt(ctx context.Context, active ActiveAssignment) string {
	commitCount := "unavailable"
	if count, err := d.branchCommitCount(ctx, active.Task.ClonePath); err == nil {
		commitCount = strconv.Itoa(count)
	}

	summary := exhaustedContextProgressSummary(active.Task.Branch, active.Task.PRNumber, commitCount)
	original := strings.TrimRight(active.Task.Prompt, "\r\n")
	if original == "" {
		return summary
	}
	return original + "\n\n" + summary
}

func exhaustedContextProgressSummary(branch string, prNumber int, commitCount string) string {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		branch = "unknown"
	}

	commitCount = strings.TrimSpace(commitCount)
	if commitCount == "" {
		commitCount = "unavailable"
	}

	pr := "none"
	if prNumber > 0 {
		pr = fmt.Sprintf("#%d", prNumber)
	}

	var builder strings.Builder
	builder.WriteString("Previous worker exhausted its Codex context and was auto-reassigned after postmortem. Continue from the existing progress instead of starting over.\n\n")
	builder.WriteString("Previous worker summary:\n")
	fmt.Fprintf(&builder, "PR: %s\n", pr)
	fmt.Fprintf(&builder, "Branch: %s\n", branch)
	fmt.Fprintf(&builder, "Commit count: %s\n", commitCount)
	return builder.String()
}

func (d *Daemon) branchCommitCount(ctx context.Context, clonePath string) (int, error) {
	if strings.TrimSpace(clonePath) == "" {
		return 0, fmt.Errorf("task clone path missing")
	}

	output, err := d.commands.Run(ctx, clonePath, "git", "rev-list", "--count", "origin/main..HEAD")
	if err != nil {
		return 0, err
	}

	trimmed := strings.TrimSpace(string(output))
	if trimmed == "" {
		return 0, nil
	}

	count, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, fmt.Errorf("parse commit count %q: %w", trimmed, err)
	}
	return count, nil
}

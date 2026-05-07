package daemon

import (
	"fmt"
	"strings"
)

const codexAssignmentPromptSuffix = "When tests pass, commit, push, and open a PR with gh pr create."
const assignPreflightSkippedPrompt = "Orca skipped its open PR preflight because GitHub is rate limited. Before editing, check whether an open PR already exists for %s. If one exists, fetch and resume that branch before making changes."

func wrapAssignmentPrompt(profile AgentProfile, issue, prompt string) string {
	prompt = strings.TrimSpace(prompt)
	if !strings.EqualFold(profile.Name, "codex") {
		return prompt
	}

	titlePrompt := codexAssignmentPRTitlePrompt(issue)
	hasPROpenReminder := strings.Contains(prompt, codexAssignmentPromptSuffix)
	hasTitleReminder := titlePrompt != "" && strings.Contains(prompt, titlePrompt)

	var reminders []string
	if !hasPROpenReminder {
		reminders = append(reminders, codexAssignmentPromptSuffix)
	}
	if titlePrompt != "" && !hasTitleReminder {
		reminders = append(reminders, titlePrompt)
	}
	if len(reminders) == 0 {
		return prompt
	}
	if prompt == "" {
		return strings.Join(reminders, "\n")
	}
	separator := "\n\n"
	if hasPROpenReminder || hasTitleReminder {
		separator = "\n"
	}
	return prompt + separator + strings.Join(reminders, "\n")
}

func codexAssignmentPRTitlePrompt(issue string) string {
	issue = normalizeIssueIdentifier(issue)
	if issue == "" {
		return ""
	}
	return fmt.Sprintf("Before opening the PR, verify the title follows %q per CLAUDE.md.", issue+": Imperative summary")
}

func withAssignPreflightSkippedPrompt(prompt, issue string) string {
	prompt = strings.TrimSpace(prompt)
	note := fmt.Sprintf(assignPreflightSkippedPrompt, normalizeIssueIdentifier(issue))
	if prompt == "" {
		return note
	}
	if strings.Contains(prompt, note) {
		return prompt
	}
	return prompt + "\n\n" + note
}

package daemon

import (
	"fmt"
	"strings"
)

const workerPRCreateCommand = "gh pr create --base main"
const codexAssignmentPromptSuffix = "When tests pass, commit, push, and open a PR with " + workerPRCreateCommand + "."
const codexDirectLandingPromptSuffix = "When tests pass, commit, push the branch, and queue direct landing with `orca enqueue %s`. Do not open a GitHub PR."
const assignPreflightSkippedPrompt = "Orca skipped its open PR preflight because GitHub is rate limited. Before editing, check whether an open PR already exists for %s. If one exists, fetch and resume that branch before making changes."
const notifyConventionPrefix = "To notify or ask the lead, run `amux msg send --from \"$AMUX_PANE\" --to "

func wrapAssignmentPrompt(profile AgentProfile, issue, prompt string) string {
	return wrapAssignmentPromptForLanding(profile, issue, prompt, defaultLandingConfig())
}

func wrapAssignmentPromptForLanding(profile AgentProfile, issue, prompt string, landing LandingConfig) string {
	prompt = strings.TrimSpace(prompt)
	if !strings.EqualFold(profile.Name, "codex") {
		return prompt
	}
	if landing.directMode() {
		return wrapDirectAssignmentPrompt(issue, prompt)
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

func wrapDirectAssignmentPrompt(issue, prompt string) string {
	target := normalizeIssueIdentifier(issue)
	if target == "" {
		target = "<issue-or-branch>"
	}
	reminder := fmt.Sprintf(codexDirectLandingPromptSuffix, target)
	if strings.Contains(prompt, "queue direct landing") || strings.Contains(prompt, "orca enqueue "+target) {
		return prompt
	}
	if prompt == "" {
		return reminder
	}
	return prompt + "\n\n" + reminder
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

func appendNotifyConvention(prompt, issue, pane string) string {
	prompt = strings.TrimSpace(prompt)
	pane = strings.TrimSpace(pane)
	if pane == "" {
		return prompt
	}

	convention := notifyConvention(issue, pane)
	if strings.Contains(prompt, notifyConventionPrefix) {
		return prompt
	}
	if prompt == "" {
		return convention
	}
	return prompt + "\n\n" + convention
}

func notifyConvention(issue, pane string) string {
	subject := normalizeIssueIdentifier(issue)
	if subject == "" {
		subject = "<issue>"
	}
	return strings.Join([]string{
		fmt.Sprintf("To notify or ask the lead, run `amux msg send --from \"$AMUX_PANE\" --to %s --subject %s --body \"<one-line message>\"`.", shellQuote(pane), shellQuote(subject)),
		"Use it for: a blocking question before you guess, or a milestone.",
		"Keep messages to one line; do not spam.",
	}, "\n")
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

package daemon

import "strings"

const codexAssignmentPromptSuffix = "When tests pass, commit, push, and open a PR with gh pr create."

func wrapAssignmentPrompt(profile AgentProfile, prompt string) string {
	prompt = strings.TrimSpace(prompt)
	if !strings.EqualFold(profile.Name, "codex") {
		return prompt
	}
	if prompt == "" {
		return codexAssignmentPromptSuffix
	}
	if strings.Contains(prompt, codexAssignmentPromptSuffix) {
		return prompt
	}
	return prompt + "\n\n" + codexAssignmentPromptSuffix
}

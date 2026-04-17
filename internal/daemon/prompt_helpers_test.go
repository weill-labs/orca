package daemon

func wrappedCodexPrompt(issue, prompt string) string {
	return wrapAssignmentPrompt(AgentProfile{Name: "codex"}, issue, prompt)
}

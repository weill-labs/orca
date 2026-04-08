package daemon

func wrappedCodexPrompt(prompt string) string {
	return wrapAssignmentPrompt(AgentProfile{Name: "codex"}, prompt)
}

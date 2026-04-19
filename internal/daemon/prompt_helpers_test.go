package daemon

func wrappedCodexPrompt(issue, prompt string) string {
	normalized, err := normalizePromptForDelivery(wrapAssignmentPrompt(AgentProfile{Name: "codex"}, issue, prompt))
	if err != nil {
		panic(err)
	}
	return normalized
}

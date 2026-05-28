package daemon

func wrappedCodexPrompt(issue, prompt string) string {
	normalized, err := normalizePromptForDelivery(wrapAssignmentPromptForLanding(AgentProfile{Name: "codex"}, issue, prompt, LandingConfig{
		Mode:       LandingModePR,
		BaseBranch: "main",
	}))
	if err != nil {
		panic(err)
	}
	return normalized
}

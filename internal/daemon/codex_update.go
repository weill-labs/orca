package daemon

import (
	"errors"
	"fmt"
	"strings"
)

const CodexRefreshCommand = "sudo npm install -g @openai/codex"

var ErrCodexUpdateRequired = errors.New("codex update required")

type CodexUpdateRequiredError struct {
	Cause    error
	Evidence string
}

func (e *CodexUpdateRequiredError) Error() string {
	message := fmt.Sprintf("%s: codex self-update failed because global npm install needs elevated permissions; run: %s", ErrCodexUpdateRequired, CodexRefreshCommand)
	if e == nil {
		return message
	}
	if evidence := codexUpdateEvidence(e.Evidence); evidence != "" {
		message += "; evidence: " + evidence
	}
	if e.Cause != nil {
		message += fmt.Sprintf("; underlying error: %v", e.Cause)
	}
	return message
}

func (e *CodexUpdateRequiredError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *CodexUpdateRequiredError) Is(target error) bool {
	return target == ErrCodexUpdateRequired
}

func codexUpdateRequiredError(cause error) error {
	return codexUpdateRequiredErrorWithEvidence(cause, "")
}

func codexUpdateRequiredErrorWithEvidence(cause error, evidence string) error {
	if errors.Is(cause, ErrCodexUpdateRequired) {
		return cause
	}
	return &CodexUpdateRequiredError{Cause: cause, Evidence: evidence}
}

func wrapCodexUpdateRequiredFromOutput(profile AgentProfile, cause error, output string) error {
	if !strings.EqualFold(profile.Name, "codex") {
		return cause
	}
	if !codexOutputMatchesUpdatePermissionError(output) {
		return cause
	}
	return codexUpdateRequiredErrorWithEvidence(cause, output)
}

func wrapCodexUpdateRequiredFromScrollback(profile AgentProfile, cause error, scrollback []string) error {
	if len(scrollback) == 0 {
		return cause
	}
	return wrapCodexUpdateRequiredFromOutput(profile, cause, strings.Join(scrollback, "\n"))
}

func codexOutputMatchesUpdatePermissionError(output string) bool {
	output = strings.ToLower(output)
	if strings.TrimSpace(output) == "" {
		return false
	}

	globalInstallFailed := strings.Contains(output, "global npm install") &&
		(strings.Contains(output, "fail") || strings.Contains(output, "eacces") || strings.Contains(output, "permission"))
	// The /usr/lib/node_modules path matches the production Linux failure from
	// LAB-1553; other global install paths are covered by the generic Codex
	// "global npm install" failure text above.
	nodeModulesPermissionDenied := strings.Contains(output, "/usr/lib/node_modules") &&
		(strings.Contains(output, "eacces") || strings.Contains(output, "permission denied") || strings.Contains(output, "operation was rejected"))

	return globalInstallFailed || nodeModulesPermissionDenied
}

func codexUpdateEvidence(output string) string {
	output = strings.TrimSpace(output)
	if output == "" {
		return ""
	}
	const maxEvidenceLen = 600
	evidence := strings.Join(strings.Fields(output), " ")
	if len(evidence) <= maxEvidenceLen {
		return evidence
	}
	return evidence[:maxEvidenceLen] + "..."
}

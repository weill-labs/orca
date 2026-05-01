package daemon

import (
	"errors"
	"fmt"
	"strings"
)

const CodexRefreshCommand = "sudo npm install -g @openai/codex"

var ErrCodexUpdateRequired = errors.New("codex update required")

type CodexUpdateRequiredError struct {
	Cause error
}

func (e *CodexUpdateRequiredError) Error() string {
	if e == nil || e.Cause == nil {
		return fmt.Sprintf("%s: codex self-update failed because global npm install needs elevated permissions; run: %s", ErrCodexUpdateRequired, CodexRefreshCommand)
	}
	return fmt.Sprintf("%s: codex self-update failed because global npm install needs elevated permissions; run: %s; underlying error: %v", ErrCodexUpdateRequired, CodexRefreshCommand, e.Cause)
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
	if errors.Is(cause, ErrCodexUpdateRequired) {
		return cause
	}
	return &CodexUpdateRequiredError{Cause: cause}
}

func wrapCodexUpdateRequiredFromOutput(profile AgentProfile, cause error, output string) error {
	if !strings.EqualFold(profile.Name, "codex") {
		return cause
	}
	if !codexOutputMatchesUpdatePermissionError(output) {
		return cause
	}
	return codexUpdateRequiredError(cause)
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

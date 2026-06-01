package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

func (a *App) waitAndWriteDirectLandingSummary(ctx context.Context, projectPath string, result daemon.MergeQueueActionResult) error {
	eventsCh, errCh := a.state.Events(ctx, projectPath, 0)
	for eventsCh != nil || errCh != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-eventsCh:
			if !ok {
				eventsCh = nil
				continue
			}
			payload := directLandingEventPayload(event)
			if !matchesDirectLandingResult(event, payload, result) {
				continue
			}
			switch event.Kind {
			case daemon.EventDirectLanded:
				return a.writeDirectLandingSummary(ctx, projectPath, result, payload)
			case daemon.EventDirectLandingFailed, daemon.EventDirectLandingConflict:
				message := firstNonEmptyString(payload.Message, event.Message, "direct landing failed")
				return fmt.Errorf("%s", message)
			}
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func directLandingEventPayload(event state.Event) daemon.Event {
	var payload daemon.Event
	if len(event.Payload) > 0 {
		_ = json.Unmarshal(event.Payload, &payload)
	}
	if payload.Type == "" {
		payload.Type = event.Kind
	}
	if payload.Project == "" {
		payload.Project = event.Project
	}
	if payload.Issue == "" {
		payload.Issue = event.Issue
	}
	if payload.Message == "" {
		payload.Message = event.Message
	}
	if payload.Time.IsZero() {
		payload.Time = event.CreatedAt
	}
	return payload
}

func matchesDirectLandingResult(event state.Event, payload daemon.Event, result daemon.MergeQueueActionResult) bool {
	switch event.Kind {
	case daemon.EventDirectLanded, daemon.EventDirectLandingFailed, daemon.EventDirectLandingConflict:
	default:
		return false
	}
	if !event.CreatedAt.IsZero() && event.CreatedAt.Before(result.UpdatedAt) {
		return false
	}

	resultIssue := strings.TrimSpace(result.Issue)
	eventIssue := strings.TrimSpace(firstNonEmptyString(payload.Issue, event.Issue))
	if resultIssue != "" && eventIssue != "" && !strings.EqualFold(resultIssue, eventIssue) {
		return false
	}

	resultBranch := strings.TrimSpace(result.Branch)
	eventBranch := strings.TrimSpace(payload.Branch)
	if resultBranch != "" && eventBranch != "" && resultBranch != eventBranch {
		return false
	}

	target := strings.TrimSpace(result.Target)
	if target == "" || strings.EqualFold(target, eventIssue) || target == eventBranch {
		return true
	}
	return resultIssue != "" || resultBranch != ""
}

func (a *App) writeDirectLandingSummary(ctx context.Context, projectPath string, result daemon.MergeQueueActionResult, event daemon.Event) error {
	target := firstNonEmptyString(event.Issue, result.Issue, event.Branch, result.Branch, result.Target)
	branch := firstNonEmptyString(event.Branch, result.Branch, target)
	baseBranch := firstNonEmptyString(event.BaseBranch, result.BaseBranch, "main")
	landedSHA := firstNonEmptyString(event.OriginMainAfterSHA, event.FeatureBranchAfterSHA)

	if _, err := fmt.Fprintf(a.stdout, "landed %s (branch %s)\n", target, branch); err != nil {
		return err
	}
	if err := writeRefDelta(a.stdout, fmt.Sprintf("remote %s", baseBranch), "origin/"+baseBranch, event.OriginMainBeforeSHA, event.OriginMainAfterSHA); err != nil {
		return err
	}
	if event.FeatureBranchBeforeSHA != "" && event.FeatureBranchAfterSHA != "" && event.FeatureBranchBeforeSHA != event.FeatureBranchAfterSHA {
		if _, err := fmt.Fprintf(a.stdout, "feature branch updated: %s %s -> %s\n", branch, event.FeatureBranchBeforeSHA, event.FeatureBranchAfterSHA); err != nil {
			return err
		}
	}
	return a.writeDirectLandingLocalState(ctx, projectPath, baseBranch, landedSHA)
}

func writeRefDelta(w io.Writer, subject, ref, before, after string) error {
	before = strings.TrimSpace(before)
	after = strings.TrimSpace(after)
	if before == "" || after == "" {
		return nil
	}
	if before == after {
		_, err := fmt.Fprintf(w, "%s unchanged (%s): %s\n", subject, ref, after)
		return err
	}
	_, err := fmt.Fprintf(w, "%s advanced (%s): %s -> %s\n", subject, ref, before, after)
	return err
}

func (a *App) writeDirectLandingLocalState(ctx context.Context, projectPath, baseBranch, landedSHA string) error {
	if landedSHA == "" {
		return nil
	}

	branch, headSHA, err := localCheckoutState(ctx, projectPath)
	if err != nil {
		if _, writeErr := fmt.Fprintf(a.stdout, "local checkout unknown: %v\n", err); writeErr != nil {
			return writeErr
		}
		_, writeErr := fmt.Fprintf(a.stdout, "next: %s\n", directLandingSyncCommand(baseBranch))
		return writeErr
	}

	if branch == baseBranch && headSHA == landedSHA {
		_, err := fmt.Fprintf(a.stdout, "local checkout synced: %s at %s\n", baseBranch, landedSHA)
		return err
	}

	if _, err := fmt.Fprintf(a.stdout, "local checkout not synced: %s at %s; expected %s at %s\n", branch, headSHA, baseBranch, landedSHA); err != nil {
		return err
	}
	_, err = fmt.Fprintf(a.stdout, "next: %s\n", directLandingSyncCommand(baseBranch))
	return err
}

func localCheckoutState(ctx context.Context, projectPath string) (string, string, error) {
	branch, err := gitOutput(ctx, projectPath, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return "", "", err
	}
	branch = strings.TrimSpace(branch)
	if branch == "HEAD" || branch == "" {
		branch = "detached HEAD"
	}
	head, err := gitOutput(ctx, projectPath, "rev-parse", "HEAD")
	if err != nil {
		return "", "", err
	}
	return branch, strings.TrimSpace(head), nil
}

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func directLandingSyncCommand(baseBranch string) string {
	baseBranch = strings.TrimSpace(baseBranch)
	if baseBranch == "" {
		baseBranch = "main"
	}
	return fmt.Sprintf("git fetch origin %s && git checkout %s && git reset --hard origin/%s", baseBranch, baseBranch, baseBranch)
}

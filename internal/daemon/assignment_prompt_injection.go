package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const (
	assignmentPromptInjectionTokenKey = "orca.assignment_prompt.token"
	assignmentPromptInjectionStageKey = "orca.assignment_prompt.stage"

	assignmentPromptInjectionStagePromptSent  = "prompt_sent"
	assignmentPromptInjectionStageCommandSent = "command_sent"
)

func (d *Daemon) sendAssignmentPromptAndEnter(ctx context.Context, paneID string, task Task, prompt string) error {
	deliveryPrompt, err := normalizePromptForDelivery(prompt)
	if err != nil {
		return err
	}
	return d.sendIdempotentAssignmentPromptCommand(ctx, paneID, assignmentPromptInjectionToken(task, deliveryPrompt), deliveryPrompt, "Enter")
}

func (d *Daemon) sendIdempotentAssignmentPromptCommand(ctx context.Context, paneID, token, prompt, command string) error {
	trimmed := strings.TrimRight(prompt, "\r\n")
	if trimmed == "" {
		return fmt.Errorf("prompt is empty")
	}
	if strings.TrimSpace(command) == "" {
		return fmt.Errorf("command is empty")
	}

	metadata, err := d.amuxClient(ctx).Metadata(ctx, paneID)
	if err != nil {
		return fmt.Errorf("read assignment prompt metadata: %w", err)
	}
	stage, promptAlreadySent := assignmentPromptInjectionStage(metadata, token)
	if stage == assignmentPromptInjectionStageCommandSent {
		return nil
	}

	if !promptAlreadySent {
		if err := d.sendKeysToLivePane(ctx, paneID, trimmed); err != nil {
			return err
		}
		d.recordAssignmentPromptInjectionStage(ctx, paneID, token, assignmentPromptInjectionStagePromptSent)
		if err := d.waitIdleSettleForLivePane(ctx, paneID, defaultAgentHandshakeTimeout, defaultPromptSettleDuration); err != nil {
			d.logAssignmentPromptSettleError(err)
		}
	}

	if err := d.sendKeysToLivePane(ctx, paneID, command); err != nil {
		return err
	}
	d.recordAssignmentPromptInjectionStage(ctx, paneID, token, assignmentPromptInjectionStageCommandSent)
	return nil
}

func (d *Daemon) recordAssignmentPromptInjectionStage(ctx context.Context, paneID, token, stage string) {
	err := d.amuxClient(ctx).SetMetadata(ctx, paneID, map[string]string{
		assignmentPromptInjectionTokenKey: token,
		assignmentPromptInjectionStageKey: stage,
	})
	// Once prompt text has been sent, metadata errors must not prevent the tail
	// Enter. Idempotency becomes best-effort if amux cannot store the stage.
	if err != nil && d.logf != nil {
		d.logf("record assignment prompt injection stage failed: %v", err)
	}
}

func (d *Daemon) clearAssignmentPromptInjection(ctx context.Context, paneID string) {
	err := d.amuxClient(ctx).RemoveMetadata(ctx, paneID, assignmentPromptInjectionTokenKey, assignmentPromptInjectionStageKey)
	if err != nil && d.logf != nil {
		d.logf("clear assignment prompt injection metadata failed: %v", err)
	}
}

func (d *Daemon) logAssignmentPromptSettleError(err error) {
	if err != nil && d.logf != nil {
		d.logf("assignment prompt settle failed before Enter; sending Enter anyway: %v", err)
	}
}

func assignmentPromptInjectionStage(metadata map[string]string, token string) (string, bool) {
	if metadata[assignmentPromptInjectionTokenKey] != token {
		return "", false
	}
	return metadata[assignmentPromptInjectionStageKey], true
}

func assignmentPromptInjectionToken(task Task, prompt string) string {
	createdAt := task.CreatedAt.UTC().Format(time.RFC3339Nano)
	sum := sha256.Sum256([]byte(strings.Join([]string{
		task.Project,
		task.Issue,
		task.WorkerID,
		task.ClonePath,
		createdAt,
		prompt,
	}, "\x00")))
	return hex.EncodeToString(sum[:])
}

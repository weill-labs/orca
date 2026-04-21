package daemon

import (
	"context"
	"errors"
	"testing"
)

func TestSendAndConfirmWorkingFlattensMultilinePromptBeforeSubmit(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitIdleErr = errors.New("idle timeout")
	d := deps.newDaemon(t)

	prompt := "Wrap up the merged PR.\n\nRun postmortem and leave the clone clean."
	if err := d.sendAndConfirmWorking(context.Background(), "pane-1", prompt); err != nil {
		t.Fatalf("sendAndConfirmWorking() error = %v", err)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Wrap up the merged PR. Run postmortem and leave the clone clean.\n",
	})
}

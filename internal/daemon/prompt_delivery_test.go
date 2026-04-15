package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

func TestConfirmPromptDeliverySkipsNonCodexProfiles(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	if err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "claude"}); err != nil {
		t.Fatalf("confirmPromptDelivery() error = %v", err)
	}
	if got, want := len(deps.amux.waitContentCalls), 0; got != want {
		t.Fatalf("waitContent calls = %d, want %d", got, want)
	}
}

func TestConfirmPromptDeliveryReturnsWaitContentError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitContentResults = []error{errors.New("wait failed")}
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil || !strings.Contains(err.Error(), `wait for "Working" after prompt: wait failed`) {
		t.Fatalf("confirmPromptDelivery() error = %v, want wrapped wait-content failure", err)
	}
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: defaultAgentHandshakeTimeout},
	}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
}

func TestConfirmPromptDeliveryReturnsSendKeysErrorOnRetry(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitContentResults = []error{amuxapi.ErrWaitContentTimeout}
	deps.amux.sendKeysErr = errors.New("send failed")
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil || !strings.Contains(err.Error(), "retry prompt delivery: send failed") {
		t.Fatalf("confirmPromptDelivery() error = %v, want wrapped send failure", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
}

func TestConfirmPromptDeliveryRetriesAfterWaitIdleTimeout(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		nil,
	}
	deps.amux.waitIdleErr = errors.New("wait idle timed out")
	d := deps.newDaemon(t)

	if err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"}); err != nil {
		t.Fatalf("confirmPromptDelivery() error = %v", err)
	}
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{"Enter"})
}

func TestConfirmPromptDeliverySucceedsWhenWorkingAppearsAfterRetryIdleError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		nil,
	}
	deps.amux.waitIdleErr = errors.New("idle failed")
	d := deps.newDaemon(t)

	if err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"}); err != nil {
		t.Fatalf("confirmPromptDelivery() error = %v", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{"Enter"})
}

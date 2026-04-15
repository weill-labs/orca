package daemon

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/amux"
)

func TestCircuitBreakerOpensAfterThreeFailuresAndClosesAfterCooldown(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)}
	breaker := NewCircuitBreaker(clock.Now, 3, 60*time.Second)
	fail := errors.New("amux unavailable")

	for i := 0; i < 2; i++ {
		if err := breaker.Allow(); err != nil {
			t.Fatalf("Allow() before threshold error = %v", err)
		}
		breaker.RecordFailure(fail)
	}

	if err := breaker.Allow(); err != nil {
		t.Fatalf("Allow() at threshold error = %v", err)
	}
	breaker.RecordFailure(fail)

	if err := breaker.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("Allow() after threshold error = %v, want %v", err, ErrCircuitBreakerOpen)
	}

	clock.Advance(59 * time.Second)
	if err := breaker.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("Allow() before cooldown expiry error = %v, want %v", err, ErrCircuitBreakerOpen)
	}

	clock.Advance(1 * time.Second)
	if err := breaker.Allow(); err != nil {
		t.Fatalf("Allow() after cooldown error = %v", err)
	}
}

func TestCircuitBreakerBacksOffRepeatedReopeningsUntilSuccess(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)}
	breaker := NewCircuitBreaker(clock.Now, 3, 60*time.Second)
	fail := errors.New("amux unavailable")

	for i := 0; i < 3; i++ {
		if err := breaker.Allow(); err != nil {
			t.Fatalf("Allow() before first open error = %v", err)
		}
		breaker.RecordFailure(fail)
	}

	clock.Advance(60 * time.Second)
	if err := breaker.Allow(); err != nil {
		t.Fatalf("Allow() after first cooldown error = %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := breaker.Allow(); err != nil {
			t.Fatalf("Allow() before second open error = %v", err)
		}
		breaker.RecordFailure(fail)
	}

	clock.Advance(119 * time.Second)
	if err := breaker.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("Allow() before second cooldown expiry error = %v, want %v", err, ErrCircuitBreakerOpen)
	}

	clock.Advance(1 * time.Second)
	if err := breaker.Allow(); err != nil {
		t.Fatalf("Allow() after second cooldown error = %v", err)
	}

	breaker.RecordSuccess()
	for i := 0; i < 3; i++ {
		if err := breaker.Allow(); err != nil {
			t.Fatalf("Allow() before reset open error = %v", err)
		}
		breaker.RecordFailure(fail)
	}

	clock.Advance(59 * time.Second)
	if err := breaker.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("Allow() before reset cooldown expiry error = %v, want %v", err, ErrCircuitBreakerOpen)
	}

	clock.Advance(1 * time.Second)
	if err := breaker.Allow(); err != nil {
		t.Fatalf("Allow() after reset cooldown error = %v", err)
	}
}

func TestCircuitBreakerHooksEmitOnOpenAndCloseTransitions(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)}
	var transitions []string
	fail := errors.New("amux capture pane-1: exit status 1: connection refused")
	breaker := NewCircuitBreakerWithHooks(clock.Now, 3, 60*time.Second, CircuitBreakerHooks{
		OnOpen: func(info CircuitBreakerTransition) {
			transitions = append(transitions, "open")
			if got, want := info.FailureCount, 3; got != want {
				t.Fatalf("OnOpen failure count = %d, want %d", got, want)
			}
			if got, want := info.Cooldown, 60*time.Second; got != want {
				t.Fatalf("OnOpen cooldown = %s, want %s", got, want)
			}
			if !errors.Is(info.Err, fail) {
				t.Fatalf("OnOpen err = %v, want %v", info.Err, fail)
			}
		},
		OnClose: func(info CircuitBreakerTransition) {
			transitions = append(transitions, "close")
			if got, want := info.Cooldown, 60*time.Second; got != want {
				t.Fatalf("OnClose cooldown = %s, want %s", got, want)
			}
		},
	})

	for i := 0; i < 3; i++ {
		if err := breaker.Allow(); err != nil {
			t.Fatalf("Allow() before open error = %v", err)
		}
		breaker.RecordFailure(fail)
	}

	clock.Advance(60 * time.Second)
	if err := breaker.Allow(); err != nil {
		t.Fatalf("Allow() after cooldown error = %v", err)
	}

	if got, want := transitions, []string{"open", "close"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("transitions = %#v, want %#v", got, want)
	}
}

func TestWithCircuitDoesNotRecordCanceledContextAsFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			breaker := NewCircuitBreaker(time.Now, 3, time.Minute)
			for i := 0; i < 3; i++ {
				_, err := withCircuit(breaker, func() (int, error) {
					return 0, tt.err
				})
				if !errors.Is(err, tt.err) {
					t.Fatalf("withCircuit() error = %v, want %v", err, tt.err)
				}
			}

			if err := breaker.Allow(); err != nil {
				t.Fatalf("Allow() after canceled errors = %v, want nil", err)
			}
		})
	}
}

func TestCircuitGitHubClientLookupPRReviewsSharesBreakerState(t *testing.T) {
	t.Parallel()

	breaker := NewCircuitBreaker(time.Now, 3, time.Minute)
	client := newCircuitGitHubClient(circuitGitHubClientStub{
		err: errors.New("github unavailable"),
	}, breaker)

	for i := 0; i < 3; i++ {
		_, _, err := client.lookupPRReviews(context.Background(), 42)
		if err == nil {
			t.Fatalf("lookupPRReviews() error on attempt %d = nil, want failure", i+1)
		}
	}

	if _, _, err := client.lookupPRReviews(context.Background(), 42); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("lookupPRReviews() after threshold error = %v, want %v", err, ErrCircuitBreakerOpen)
	}
}

func TestCircuitAmuxClientWrapsBaseMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(t *testing.T, client AmuxClient, base *fakeAmux)
	}{
		{
			name: "spawn",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				req := SpawnRequest{Name: "worker-1", Command: "orca"}
				got, err := client.Spawn(context.Background(), req)
				if err != nil {
					t.Fatalf("Spawn() error = %v", err)
				}
				if got != base.spawnPane {
					t.Fatalf("Spawn() pane = %#v, want %#v", got, base.spawnPane)
				}
				if got := len(base.spawnRequests); got != 1 {
					t.Fatalf("len(spawnRequests) = %d, want 1", got)
				}
			},
		},
		{
			name: "pane exists",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				got, err := client.PaneExists(context.Background(), base.spawnPane.ID)
				if err != nil {
					t.Fatalf("PaneExists() error = %v", err)
				}
				if !got {
					t.Fatalf("PaneExists() = false, want true")
				}
			},
		},
		{
			name: "list panes",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				got, err := client.ListPanes(context.Background())
				if err != nil {
					t.Fatalf("ListPanes() error = %v", err)
				}
				if want := []Pane{base.spawnPane}; !reflect.DeepEqual(got, want) {
					t.Fatalf("ListPanes() = %#v, want %#v", got, want)
				}
			},
		},
		{
			name: "metadata",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				got, err := client.Metadata(context.Background(), base.spawnPane.ID)
				if err != nil {
					t.Fatalf("Metadata() error = %v", err)
				}
				if want := map[string]string{"role": "worker", "status": "active"}; !reflect.DeepEqual(got, want) {
					t.Fatalf("Metadata() = %#v, want %#v", got, want)
				}
			},
		},
		{
			name: "events",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				event := amux.Event{Type: "exited", PaneName: base.spawnPane.ID}
				base.eventSequences = []fakeAmuxEventSequence{{events: []amux.Event{event}}}

				eventsCh, errCh := client.Events(context.Background(), amux.EventsRequest{Filter: []string{"exited"}})

				select {
				case got, ok := <-eventsCh:
					if !ok {
						t.Fatal("Events() closed events channel before delivering event")
					}
					if got != event {
						t.Fatalf("Events() event = %#v, want %#v", got, event)
					}
				case <-time.After(time.Second):
					t.Fatal("timed out waiting for event stream value")
				}

				select {
				case err, ok := <-errCh:
					if ok && err != nil {
						t.Fatalf("Events() error = %v, want nil", err)
					}
				case <-time.After(10 * time.Millisecond):
				}

				if got, want := base.eventsCalls, 1; got != want {
					t.Fatalf("eventsCalls = %d, want %d", got, want)
				}
			},
		},
		{
			name: "set metadata",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				if err := client.SetMetadata(context.Background(), base.spawnPane.ID, map[string]string{"owner": "orca"}); err != nil {
					t.Fatalf("SetMetadata() error = %v", err)
				}
				base.requireMetadata(t, base.spawnPane.ID, map[string]string{
					"owner":  "orca",
					"role":   "worker",
					"status": "active",
				})
			},
		},
		{
			name: "remove metadata",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				if err := client.RemoveMetadata(context.Background(), base.spawnPane.ID, "status"); err != nil {
					t.Fatalf("RemoveMetadata() error = %v", err)
				}
				base.requireMetadata(t, base.spawnPane.ID, map[string]string{"role": "worker"})
			},
		},
		{
			name: "send keys",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				if err := client.SendKeys(context.Background(), base.spawnPane.ID, "status", "Enter"); err != nil {
					t.Fatalf("SendKeys() error = %v", err)
				}
				base.requireSentKeys(t, base.spawnPane.ID, []string{"status", "Enter"})
			},
		},
		{
			name: "kill pane",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				if err := client.KillPane(context.Background(), base.spawnPane.ID); err != nil {
					t.Fatalf("KillPane() error = %v", err)
				}
				if got := len(base.killCalls); got != 1 {
					t.Fatalf("len(killCalls) = %d, want 1", got)
				}
			},
		},
		{
			name: "wait idle",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				timeout := 5 * time.Second
				if err := client.WaitIdle(context.Background(), base.spawnPane.ID, timeout); err != nil {
					t.Fatalf("WaitIdle() error = %v", err)
				}
				if got, want := base.waitIdleCalls, []waitIdleCall{{PaneID: base.spawnPane.ID, Timeout: timeout}}; !reflect.DeepEqual(got, want) {
					t.Fatalf("waitIdleCalls = %#v, want %#v", got, want)
				}
			},
		},
		{
			name: "wait idle settle",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				timeout := 5 * time.Second
				settle := 2 * time.Second
				if err := client.WaitIdleSettle(context.Background(), base.spawnPane.ID, timeout, settle); err != nil {
					t.Fatalf("WaitIdleSettle() error = %v", err)
				}
				if got, want := base.waitIdleCalls, []waitIdleCall{{PaneID: base.spawnPane.ID, Timeout: timeout, Settle: settle}}; !reflect.DeepEqual(got, want) {
					t.Fatalf("waitIdleCalls = %#v, want %#v", got, want)
				}
			},
		},
		{
			name: "wait content",
			call: func(t *testing.T, client AmuxClient, base *fakeAmux) {
				t.Helper()

				base.waitContentResults = []error{nil}
				timeout := 5 * time.Second
				if err := client.WaitContent(context.Background(), base.spawnPane.ID, "ready", timeout); err != nil {
					t.Fatalf("WaitContent() error = %v", err)
				}
				if got, want := base.waitContentCalls, []waitContentCall{{PaneID: base.spawnPane.ID, Substring: "ready", Timeout: timeout}}; !reflect.DeepEqual(got, want) {
					t.Fatalf("waitContentCalls = %#v, want %#v", got, want)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pane := Pane{ID: "pane-1", Name: "worker-1"}
			base := &fakeAmux{
				spawnPane:  pane,
				paneExists: map[string]bool{pane.ID: true},
				listPanes:  []Pane{pane},
				metadata: map[string]map[string]string{
					pane.ID: {"role": "worker", "status": "active"},
				},
			}
			client := newCircuitAmuxClient(base, NewCircuitBreaker(time.Now, 3, time.Minute))
			tt.call(t, client, base)
		})
	}
}

func TestCircuitAmuxClientDoesNotTripBreakerOnPaneNotFound(t *testing.T) {
	t.Parallel()

	breaker := NewCircuitBreaker(time.Now, 1, time.Minute)
	base := &fakeAmux{
		capturePaneErr: errors.New(`amux -s main capture --format json w-LAB-1033: exit status 1: amux capture: pane "w-LAB-1033" not found`),
	}
	client := newCircuitAmuxClient(base, breaker)

	if _, err := client.CapturePane(context.Background(), "w-LAB-1033"); err == nil {
		t.Fatal("CapturePane() error = nil, want pane not found")
	}
	if err := breaker.Allow(); err != nil {
		t.Fatalf("Allow() after pane-not-found error = %v, want nil", err)
	}
}

func TestCircuitAmuxClientTripsBreakerOnConnectionRefusedWhenThresholdIsOne(t *testing.T) {
	t.Parallel()

	breaker := NewCircuitBreaker(time.Now, 1, time.Minute)
	base := &fakeAmux{
		capturePaneErr: errors.New("amux -s main capture --format json pane-1: exit status 1: connection refused"),
	}
	client := newCircuitAmuxClient(base, breaker)

	if _, err := client.CapturePane(context.Background(), "pane-1"); err == nil {
		t.Fatal("CapturePane() error = nil, want connection refused")
	}
	if err := breaker.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("Allow() after connection-refused error = %v, want %v", err, ErrCircuitBreakerOpen)
	}
}

func TestCircuitAmuxClientOpensAfterThreeConnectionRefusedFailures(t *testing.T) {
	t.Parallel()

	breaker := NewCircuitBreaker(time.Now, 3, time.Minute)
	base := &fakeAmux{
		capturePaneErr: errors.New("amux -s main capture --format json pane-1: exit status 1: connection refused"),
	}
	client := newCircuitAmuxClient(base, breaker)

	for i := 0; i < 3; i++ {
		if _, err := client.CapturePane(context.Background(), "pane-1"); err == nil {
			t.Fatalf("CapturePane() error on attempt %d = nil, want connection refused", i+1)
		}
		if i < 2 {
			if err := breaker.Allow(); err != nil {
				t.Fatalf("Allow() after %d failure(s) = %v, want nil", i+1, err)
			}
		}
	}

	if err := breaker.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("Allow() after three connection-refused errors = %v, want %v", err, ErrCircuitBreakerOpen)
	}
}

func TestCircuitCommandRunnerBypassesNonGitHubCommandsWhenCircuitIsOpen(t *testing.T) {
	t.Parallel()

	breaker := NewCircuitBreaker(time.Now, 3, time.Minute)
	for i := 0; i < 3; i++ {
		breaker.RecordFailure(errors.New("github unavailable"))
	}

	commands := newFakeCommands()
	commands.queue("git", []string{"status"}, "working tree clean", nil)

	runner := newCircuitCommandRunner(commands, breaker)
	got, err := runner.Run(context.Background(), "/tmp/repo", "git", "status")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if string(got) != "working tree clean" {
		t.Fatalf("Run() output = %q, want %q", got, "working tree clean")
	}
	if got := commands.countCall("git", "status"); got != 1 {
		t.Fatalf("countCall(git status) = %d, want 1", got)
	}
}

func TestCircuitGitHubClientWrapsLookupMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(t *testing.T, client gitHubClient)
	}{
		{
			name: "lookup pr number",
			call: func(t *testing.T, client gitHubClient) {
				t.Helper()

				got, err := client.lookupPRNumber(context.Background(), "LAB-924")
				if err != nil {
					t.Fatalf("lookupPRNumber() error = %v", err)
				}
				if got != 42 {
					t.Fatalf("lookupPRNumber() = %d, want 42", got)
				}
			},
		},
		{
			name: "lookup open pr number",
			call: func(t *testing.T, client gitHubClient) {
				t.Helper()

				got, err := client.lookupOpenPRNumber(context.Background(), "LAB-924")
				if err != nil {
					t.Fatalf("lookupOpenPRNumber() error = %v", err)
				}
				if got != 43 {
					t.Fatalf("lookupOpenPRNumber() = %d, want 43", got)
				}
			},
		},
		{
			name: "is pr merged",
			call: func(t *testing.T, client gitHubClient) {
				t.Helper()

				got, err := client.isPRMerged(context.Background(), 42)
				if err != nil {
					t.Fatalf("isPRMerged() error = %v", err)
				}
				if !got {
					t.Fatalf("isPRMerged() = false, want true")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newCircuitGitHubClient(&circuitGitHubClientStub{
				prNumber:     42,
				openPRNumber: 43,
				merged:       true,
			}, NewCircuitBreaker(time.Now, 3, time.Minute))
			tt.call(t, client)
		})
	}
}

func TestMonitorCircuitDependencyAccessorsFallbackToBaseDependencies(t *testing.T) {
	t.Parallel()

	baseAmux := &fakeAmux{}
	baseCommands := newFakeCommands()
	baseGitHub := &circuitGitHubClientStub{prNumber: 42}

	daemon := &Daemon{
		project:  "orca",
		amux:     baseAmux,
		commands: baseCommands,
		github:   baseGitHub,
	}

	if got := daemon.amuxClient(context.Background()); got != baseAmux {
		t.Fatalf("amuxClient() = %#v, want base amux %#v", got, baseAmux)
	}
	if got := daemon.commandRunner(context.Background()); got != baseCommands {
		t.Fatalf("commandRunner() = %#v, want base commands %#v", got, baseCommands)
	}
	if got := daemon.gitHubClientForContext(context.Background(), daemon.project); got != baseGitHub {
		t.Fatalf("gitHubClientForContext() = %#v, want base github %#v", got, baseGitHub)
	}
}

type circuitGitHubClientStub struct {
	err          error
	prNumber     int
	openPRNumber int
	allPRNumber  int
	mergedPR     bool
	merged       bool
}

func (c circuitGitHubClientStub) lookupPRNumber(context.Context, string) (int, error) {
	return c.prNumber, c.err
}

func (c circuitGitHubClientStub) lookupOpenPRNumber(context.Context, string) (int, error) {
	return c.openPRNumber, c.err
}

func (c circuitGitHubClientStub) lookupOpenOrMergedPRNumber(context.Context, string) (int, bool, error) {
	return c.allPRNumber, c.mergedPR, c.err
}

func (c circuitGitHubClientStub) isPRMerged(context.Context, int) (bool, error) {
	return c.merged, c.err
}

func (c circuitGitHubClientStub) lookupPRReviews(context.Context, int) (prReviewPayload, bool, error) {
	return prReviewPayload{}, false, c.err
}

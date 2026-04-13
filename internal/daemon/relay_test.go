package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDaemonRelayEventPullRequestReviewTriggersImmediateReviewPoll(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 42)
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found")
	}
	worker.LastActivityAt = deps.clock.Now().Add(-time.Minute)
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("reviewer", "CHANGES_REQUESTED", "Please add tests."),
	}, nil), nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})

	d.handleRelayEvent(context.Background(), relayEventMessage{
		ID:       "evt-1",
		Type:     "pull_request_review",
		Repo:     "weill-labs/orca",
		PRNumber: 42,
	})

	worker, ok = deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after relay review poll")
	}
	if got, want := worker.LastReviewCount, 1; got != want {
		t.Fatalf("worker.LastReviewCount = %d, want %d", got, want)
	}
	if got, want := worker.ReviewNudgeCount, 1; got != want {
		t.Fatalf("worker.ReviewNudgeCount = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedReview), 1; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func TestDaemonRelayEventCheckRunTriggersImmediateCIPoll(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "git@github.com:weill-labs/orca.git", nil
		}
	})

	d.handleRelayEvent(context.Background(), relayEventMessage{
		ID:       "evt-2",
		Type:     "check_run",
		Repo:     "github.com/weill-labs/orca",
		PRNumber: 42,
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after relay ci poll")
	}
	if got, want := worker.LastCIState, ciStatePending; got != want {
		t.Fatalf("worker.LastCIState = %q, want %q", got, want)
	}
}

func TestDaemonRelayEventPullRequestMergedTriggersImmediateMergeDetection(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-13T12:00:00Z"}`, nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})

	d.handleRelayEvent(context.Background(), relayEventMessage{
		ID:       "evt-3",
		Type:     "pull_request",
		Repo:     "weill-labs/orca",
		PRNumber: 42,
		Merged:   true,
	})

	task, ok := deps.state.task("LAB-1166")
	if !ok {
		t.Fatal("task not found after relay merge poll")
	}
	if got, want := task.Status, TaskStatusDone; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := deps.events.countType(EventPRMerged), 1; got != want {
		t.Fatalf("merged event count = %d, want %d", got, want)
	}
}

func TestDaemonRelayConnectsAndReconnectsWithLastEventID(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)

	server := newRelayTestServer(t)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.RelayURL = server.wsURL()
		opts.RelayToken = "relay-token"
		opts.Hostname = "devbox-01"
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	first := server.nextConnection(t)
	if got, want := first.auth, "Bearer relay-token"; got != want {
		t.Fatalf("Authorization header = %q, want %q", got, want)
	}
	if got, want := first.identify.Type, "identify"; got != want {
		t.Fatalf("identify.Type = %q, want %q", got, want)
	}
	if got, want := first.identify.Hostname, "devbox-01"; got != want {
		t.Fatalf("identify.Hostname = %q, want %q", got, want)
	}
	if got := first.identify.LastEventID; got != "" {
		t.Fatalf("identify.LastEventID = %q, want empty", got)
	}
	if got, want := len(first.identify.Projects), 1; got != want {
		t.Fatalf("len(identify.Projects) = %d, want %d", got, want)
	}
	if got, want := first.identify.Projects[0].Path, "/tmp/project"; got != want {
		t.Fatalf("identify.Projects[0].Path = %q, want %q", got, want)
	}
	if got, want := first.identify.Projects[0].Repo, "weill-labs/orca"; got != want {
		t.Fatalf("identify.Projects[0].Repo = %q, want %q", got, want)
	}

	if err := first.conn.WriteJSON(relayEventMessage{
		ID:       "evt-9",
		Type:     "check_run",
		Repo:     "weill-labs/orca",
		PRNumber: 42,
	}); err != nil {
		t.Fatalf("WriteJSON(first event) error = %v", err)
	}

	waitFor(t, "relay event ci poll", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCIState == ciStatePending
	})

	_ = first.conn.Close()

	second := server.nextConnection(t)
	if got, want := second.identify.LastEventID, "evt-9"; got != want {
		t.Fatalf("identify.LastEventID after reconnect = %q, want %q", got, want)
	}
}

func TestDaemonRelayHealthEnqueuesPollIntervalUpdates(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	d.pollIntervalCh = make(chan time.Duration, 1)

	d.setRelayHealthy(true)
	select {
	case got := <-d.pollIntervalCh:
		if got != relayHealthyPollInterval {
			t.Fatalf("healthy poll interval = %v, want %v", got, relayHealthyPollInterval)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for healthy poll interval update")
	}

	d.setRelayHealthy(false)
	select {
	case got := <-d.pollIntervalCh:
		if got != d.pollInterval {
			t.Fatalf("fallback poll interval = %v, want %v", got, d.pollInterval)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for fallback poll interval update")
	}
}

type relayTestServer struct {
	server      *httptest.Server
	connections chan relayServerConnection
}

type relayServerConnection struct {
	auth     string
	identify relayIdentifyMessage
	conn     *websocket.Conn
}

func newRelayTestServer(t *testing.T) *relayTestServer {
	t.Helper()

	server := &relayTestServer{
		connections: make(chan relayServerConnection, 8),
	}
	upgrader := websocket.Upgrader{}
	server.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade() error = %v", err)
			return
		}

		var identify relayIdentifyMessage
		if err := conn.ReadJSON(&identify); err != nil {
			_ = conn.Close()
			t.Errorf("ReadJSON(identify) error = %v", err)
			return
		}

		select {
		case server.connections <- relayServerConnection{
			auth:     r.Header.Get("Authorization"),
			identify: identify,
			conn:     conn,
		}:
		case <-time.After(time.Second):
			_ = conn.Close()
			t.Errorf("timed out recording relay connection")
		}
	}))
	t.Cleanup(server.server.Close)

	return server
}

func (s *relayTestServer) wsURL() string {
	return "ws" + strings.TrimPrefix(s.server.URL, "http")
}

func (s *relayTestServer) nextConnection(t *testing.T) relayServerConnection {
	t.Helper()

	select {
	case conn := <-s.connections:
		return conn
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for relay connection")
		return relayServerConnection{}
	}
}

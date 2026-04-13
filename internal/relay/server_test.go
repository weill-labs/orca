package relay

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebhookPublishesToWebsocket(t *testing.T) {
	t.Parallel()

	server := newTestServer(t)
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	conn := mustDialWebsocket(t, httpServer.URL, url.Values{
		"token": []string{"relay-token"},
	})
	defer conn.Close()

	status := postWebhook(t, httpServer.URL, "webhook-secret", "pull_request_review", map[string]any{
		"action": "submitted",
		"repository": map[string]any{
			"full_name": "weill-labs/orca",
		},
		"pull_request": map[string]any{
			"number": 77,
		},
		"review": map[string]any{
			"state":        "approved",
			"submitted_at": "2026-04-13T12:00:00Z",
			"user": map[string]any{
				"login": "octocat",
			},
		},
	})
	if got, want := status, http.StatusAccepted; got != want {
		t.Fatalf("POST /webhook status = %d, want %d", got, want)
	}

	got := mustReadEnvelope(t, conn)
	if got.ID != 1 {
		t.Fatalf("Envelope.ID = %d, want 1", got.ID)
	}
	if got.EventType != "pull_request_review" {
		t.Fatalf("Envelope.EventType = %q, want pull_request_review", got.EventType)
	}
	if got.Repo != "weill-labs/orca" {
		t.Fatalf("Envelope.Repo = %q, want weill-labs/orca", got.Repo)
	}
	if got.PRNumber != 77 {
		t.Fatalf("Envelope.PRNumber = %d, want 77", got.PRNumber)
	}
	if got.PayloadSummary["review_state"] != "approved" {
		t.Fatalf("Envelope.PayloadSummary[review_state] = %q, want approved", got.PayloadSummary["review_state"])
	}
}

func TestWebsocketReplayFromLastEventID(t *testing.T) {
	t.Parallel()

	server := newTestServer(t)
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	firstConn := mustDialWebsocket(t, httpServer.URL, url.Values{
		"token": []string{"relay-token"},
	})

	postWebhook(t, httpServer.URL, "webhook-secret", "pull_request", map[string]any{
		"action": "opened",
		"repository": map[string]any{
			"full_name": "weill-labs/orca",
		},
		"pull_request": map[string]any{
			"number":     10,
			"state":      "open",
			"merged":     false,
			"updated_at": "2026-04-13T12:00:00Z",
		},
	})
	first := mustReadEnvelope(t, firstConn)
	if first.ID != 1 {
		t.Fatalf("first event ID = %d, want 1", first.ID)
	}

	postWebhook(t, httpServer.URL, "webhook-secret", "pull_request", map[string]any{
		"action": "synchronize",
		"repository": map[string]any{
			"full_name": "weill-labs/orca",
		},
		"pull_request": map[string]any{
			"number":     10,
			"state":      "open",
			"merged":     false,
			"updated_at": "2026-04-13T12:01:00Z",
		},
	})
	second := mustReadEnvelope(t, firstConn)
	if second.ID != 2 {
		t.Fatalf("second event ID = %d, want 2", second.ID)
	}
	if err := firstConn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reconnected := mustDialWebsocket(t, httpServer.URL, url.Values{
		"token":         []string{"relay-token"},
		"last_event_id": []string{"1"},
	})
	defer reconnected.Close()

	replayed := mustReadEnvelope(t, reconnected)
	if replayed.ID != 2 {
		t.Fatalf("replayed event ID = %d, want 2", replayed.ID)
	}
	if replayed.PayloadSummary["action"] != "synchronize" {
		t.Fatalf("replayed action = %q, want synchronize", replayed.PayloadSummary["action"])
	}

	postWebhook(t, httpServer.URL, "webhook-secret", "pull_request", map[string]any{
		"action": "closed",
		"repository": map[string]any{
			"full_name": "weill-labs/orca",
		},
		"pull_request": map[string]any{
			"number":     10,
			"state":      "closed",
			"merged":     true,
			"updated_at": "2026-04-13T12:02:00Z",
		},
	})
	live := mustReadEnvelope(t, reconnected)
	if live.ID != 3 {
		t.Fatalf("live event ID = %d, want 3", live.ID)
	}
	if live.PayloadSummary["merged"] != "true" {
		t.Fatalf("live merged = %q, want true", live.PayloadSummary["merged"])
	}
}

func TestWebhookRejectsInvalidSignature(t *testing.T) {
	t.Parallel()

	server := newTestServer(t)
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	body := []byte(`{"repository":{"full_name":"weill-labs/orca"}}`)
	req, err := http.NewRequest(http.MethodPost, httpServer.URL+"/webhook", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("http.NewRequest() error = %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-Hub-Signature-256", "sha256=deadbeef")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()

	if got, want := resp.StatusCode, http.StatusUnauthorized; got != want {
		t.Fatalf("POST /webhook status = %d, want %d", got, want)
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()

	cfg := Config{
		WebhookSecret: "webhook-secret",
		RelayToken:    "relay-token",
		Port:          "8080",
	}
	return NewServer(cfg, ServerOptions{
		Now: func() time.Time {
			return time.Date(2026, 4, 13, 12, 30, 0, 0, time.UTC)
		},
	})
}

func mustDialWebsocket(t *testing.T, httpURL string, query url.Values) *websocket.Conn {
	t.Helper()

	wsURL := "ws" + httpURL[len("http"):] + "/ws?" + query.Encode()
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", wsURL, err)
	}
	return conn
}

func postWebhook(t *testing.T, baseURL, secret, eventType string, payload any) int {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/webhook", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("http.NewRequest() error = %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", eventType)
	req.Header.Set("X-Hub-Signature-256", signGitHubPayload(secret, body))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()

	return resp.StatusCode
}

func mustReadEnvelope(t *testing.T, conn *websocket.Conn) Envelope {
	t.Helper()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	var env Envelope
	if err := conn.ReadJSON(&env); err != nil {
		t.Fatalf("ReadJSON() error = %v", err)
	}
	return env
}

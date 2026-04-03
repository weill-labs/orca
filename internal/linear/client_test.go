package linear

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestClientSetIssueStatusUsesGraphQLLifecycle(t *testing.T) {
	t.Parallel()

	var requests []graphQLRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		if got, want := r.Header.Get("Authorization"), "test-key"; got != want {
			t.Fatalf("Authorization header = %q, want %q", got, want)
		}

		var request graphQLRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests = append(requests, request)

		switch len(requests) {
		case 1:
			writeJSON(t, w, map[string]any{
				"data": map[string]any{
					"issue": map[string]any{
						"team":  map[string]any{"key": "LAB"},
						"state": map[string]any{"id": "backlog-state", "name": "Backlog"},
					},
				},
			})
		case 2:
			writeJSON(t, w, map[string]any{
				"data": map[string]any{
					"workflowStates": map[string]any{
						"nodes": []map[string]any{
							{"id": "progress-state", "name": "In Progress"},
							{"id": "done-state", "name": "Done"},
						},
					},
				},
			})
		case 3:
			writeJSON(t, w, map[string]any{
				"data": map[string]any{
					"issueUpdate": map[string]any{"success": true},
				},
			})
		default:
			t.Fatalf("unexpected request count = %d", len(requests))
		}
	}))
	t.Cleanup(server.Close)

	client, err := New(Options{
		APIKey:   "test-key",
		Endpoint: server.URL,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := client.SetIssueStatus(context.Background(), "LAB-713", "In Progress"); err != nil {
		t.Fatalf("SetIssueStatus() error = %v", err)
	}

	if got, want := len(requests), 3; got != want {
		t.Fatalf("request count = %d, want %d", got, want)
	}
	if got := requests[0].Variables.(map[string]any)["id"]; got != "LAB-713" {
		t.Fatalf("issue lookup id = %#v, want %q", got, "LAB-713")
	}
	if got := requests[1].Variables.(map[string]any)["teamKey"]; got != "LAB" {
		t.Fatalf("workflow state lookup teamKey = %#v, want %q", got, "LAB")
	}
	if got, want := requests[2].Variables.(map[string]any)["stateId"], "progress-state"; got != want {
		t.Fatalf("issue update stateId = %#v, want %q", got, want)
	}
	if !strings.Contains(requests[2].Query, "issueUpdate") {
		t.Fatalf("mutation query = %q, want issueUpdate", requests[2].Query)
	}
}

func TestAPIKeyFromEnvFallsBackToDotEnv(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("LINEAR_API_KEY", "")

	if err := os.WriteFile(filepath.Join(homeDir, ".env"), []byte("export LINEAR_API_KEY=from-dot-env\n"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	key, err := APIKeyFromEnv()
	if err != nil {
		t.Fatalf("APIKeyFromEnv() error = %v", err)
	}
	if got, want := key, "from-dot-env"; got != want {
		t.Fatalf("APIKeyFromEnv() = %q, want %q", got, want)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, payload map[string]any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
}

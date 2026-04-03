package linear

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestClientSetIssueStatusUsesGraphQLLifecycleAcrossPages(t *testing.T) {
	t.Parallel()

	var requests []graphQLRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		if got, want := r.Header.Get("Authorization"), "test-key"; got != want {
			t.Fatalf("Authorization header = %q, want %q", got, want)
		}

		request := decodeGraphQLRequest(t, r)
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
							{"id": "todo-state", "name": "Todo"},
						},
						"pageInfo": map[string]any{
							"hasNextPage": true,
							"endCursor":   "cursor-2",
						},
					},
				},
			})
		case 3:
			writeJSON(t, w, map[string]any{
				"data": map[string]any{
					"workflowStates": map[string]any{
						"nodes": []map[string]any{
							{"id": "progress-state", "name": "In Progress"},
						},
						"pageInfo": map[string]any{
							"hasNextPage": false,
							"endCursor":   "",
						},
					},
				},
			})
		case 4:
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

	client := newTestClient(t, server.URL)
	if err := client.SetIssueStatus(context.Background(), "LAB-713", "In Progress"); err != nil {
		t.Fatalf("SetIssueStatus() error = %v", err)
	}

	if got, want := len(requests), 4; got != want {
		t.Fatalf("request count = %d, want %d", got, want)
	}
	if got, want := requests[1].Variables.(map[string]any)["first"], float64(workflowStatesPageSize); got != want {
		t.Fatalf("workflow state page size = %#v, want %#v", got, want)
	}
	if got := requests[1].Variables.(map[string]any)["after"]; got != nil {
		t.Fatalf("first workflow page after = %#v, want nil", got)
	}
	if got, want := requests[2].Variables.(map[string]any)["after"], "cursor-2"; got != want {
		t.Fatalf("second workflow page after = %#v, want %q", got, want)
	}
	if got, want := requests[3].Variables.(map[string]any)["stateId"], "progress-state"; got != want {
		t.Fatalf("issue update stateId = %#v, want %q", got, want)
	}
}

func TestClientSetIssueStatusSkipsMutationWhenAlreadyInTargetState(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		requestCount++
		_ = decodeGraphQLRequest(t, r)
		writeJSON(t, w, map[string]any{
			"data": map[string]any{
				"issue": map[string]any{
					"team":  map[string]any{"key": "LAB"},
					"state": map[string]any{"id": "progress-state", "name": "In Progress"},
				},
			},
		})
	}))
	t.Cleanup(server.Close)

	client := newTestClient(t, server.URL)
	if err := client.SetIssueStatus(context.Background(), "LAB-713", "In Progress"); err != nil {
		t.Fatalf("SetIssueStatus() error = %v", err)
	}
	if got, want := requestCount, 1; got != want {
		t.Fatalf("request count = %d, want %d", got, want)
	}
}

func TestClientSetIssueStatusSurfacesGraphQLError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		_ = decodeGraphQLRequest(t, r)
		writeJSON(t, w, map[string]any{
			"errors": []map[string]any{
				{
					"message": "Entity not found",
					"extensions": map[string]any{
						"code": "ENTITY_NOT_FOUND",
					},
				},
			},
		})
	}))
	t.Cleanup(server.Close)

	client := newTestClient(t, server.URL)
	err := client.SetIssueStatus(context.Background(), "LAB-713", "In Progress")
	if err == nil {
		t.Fatal("SetIssueStatus() = nil error, want non-nil")
	}
	if !strings.Contains(err.Error(), "Entity not found (ENTITY_NOT_FOUND)") {
		t.Fatalf("SetIssueStatus() error = %v, want GraphQL error details", err)
	}
}

func TestClientSetIssueStatusSurfacesHTTPStatusError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		_ = decodeGraphQLRequest(t, r)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	client := newTestClient(t, server.URL)
	err := client.SetIssueStatus(context.Background(), "LAB-713", "In Progress")
	if err == nil {
		t.Fatal("SetIssueStatus() = nil error, want non-nil")
	}
	if !strings.Contains(err.Error(), "unexpected status 502") {
		t.Fatalf("SetIssueStatus() error = %v, want HTTP status details", err)
	}
}

func TestNewReturnsMissingAPIKeyError(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("LINEAR_API_KEY", "")

	_, err := New(Options{})
	if !errors.Is(err, ErrMissingAPIKey) {
		t.Fatalf("New() error = %v, want ErrMissingAPIKey", err)
	}
}

func TestAPIKeyFromEnvPrefersEnvironmentVariable(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("LINEAR_API_KEY", "from-env")

	if err := os.WriteFile(filepath.Join(homeDir, ".env"), []byte("LINEAR_API_KEY=from-dot-env\n"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	key, err := APIKeyFromEnv()
	if err != nil {
		t.Fatalf("APIKeyFromEnv() error = %v", err)
	}
	if got, want := key, "from-env"; got != want {
		t.Fatalf("APIKeyFromEnv() = %q, want %q", got, want)
	}
}

func TestAPIKeyFromEnvParsesDotEnvFormats(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("LINEAR_API_KEY", "")

	if err := os.WriteFile(filepath.Join(homeDir, ".env"), []byte(strings.Join([]string{
		"# comment",
		`LINEAR_API_KEY="quoted-bare"`,
		`export LINEAR_API_KEY='quoted-export'`,
	}, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	key, err := APIKeyFromEnv()
	if err != nil {
		t.Fatalf("APIKeyFromEnv() error = %v", err)
	}
	if got, want := key, "quoted-bare"; got != want {
		t.Fatalf("APIKeyFromEnv() = %q, want %q", got, want)
	}
}

func TestParseAPIKeyLine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		line   string
		want   string
		wantOK bool
	}{
		{name: "bare", line: "LINEAR_API_KEY=value", want: "value", wantOK: true},
		{name: "exported", line: "export LINEAR_API_KEY=value", want: "value", wantOK: true},
		{name: "quoted", line: `LINEAR_API_KEY="value"`, want: "value", wantOK: true},
		{name: "comment", line: "# LINEAR_API_KEY=value", wantOK: false},
		{name: "other key", line: "OTHER_KEY=value", wantOK: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := parseAPIKeyLine(tc.line)
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("parseAPIKeyLine(%q) = (%q, %t), want (%q, %t)", tc.line, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func newTestClient(t *testing.T, endpoint string) *Client {
	t.Helper()

	client, err := New(Options{
		APIKey:   "test-key",
		Endpoint: endpoint,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return client
}

func decodeGraphQLRequest(t *testing.T, r *http.Request) graphQLRequest {
	t.Helper()

	var request graphQLRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	return request
}

func writeJSON(t *testing.T, w http.ResponseWriter, payload map[string]any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
}

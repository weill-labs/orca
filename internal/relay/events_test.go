package relay

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"
)

func TestVerifyGitHubSignature(t *testing.T) {
	t.Parallel()

	secret := "top-secret"
	body := []byte(`{"zen":"keep it logically awesome"}`)
	valid := signGitHubPayload(secret, body)

	tests := []struct {
		name      string
		signature string
		want      bool
	}{
		{name: "valid signature", signature: valid, want: true},
		{name: "wrong digest", signature: "sha256=deadbeef", want: false},
		{name: "wrong prefix", signature: "sha1=deadbeef", want: false},
		{name: "missing signature", signature: "", want: false},
		{name: "malformed hex", signature: "sha256=not-hex", want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := VerifyGitHubSignature(secret, body, tt.signature); got != tt.want {
				t.Fatalf("VerifyGitHubSignature() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeWebhookEvent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 13, 12, 30, 0, 0, time.UTC)

	tests := []struct {
		name        string
		eventType   string
		payload     any
		wantOK      bool
		wantEvent   string
		wantRepo    string
		wantPR      int
		wantTime    time.Time
		wantSummary map[string]string
	}{
		{
			name:      "pull request review",
			eventType: "pull_request_review",
			payload: map[string]any{
				"action": "submitted",
				"repository": map[string]any{
					"full_name": "weill-labs/orca",
				},
				"pull_request": map[string]any{
					"number": 42,
				},
				"review": map[string]any{
					"state":        "approved",
					"submitted_at": "2026-04-13T11:00:00Z",
					"user": map[string]any{
						"login": "octocat",
					},
				},
			},
			wantOK:    true,
			wantEvent: "pull_request_review",
			wantRepo:  "weill-labs/orca",
			wantPR:    42,
			wantTime:  time.Date(2026, 4, 13, 11, 0, 0, 0, time.UTC),
			wantSummary: map[string]string{
				"action":       "submitted",
				"author":       "octocat",
				"review_state": "approved",
			},
		},
		{
			name:      "issue comment on pull request",
			eventType: "issue_comment",
			payload: map[string]any{
				"action": "created",
				"repository": map[string]any{
					"full_name": "weill-labs/orca",
				},
				"issue": map[string]any{
					"number":       24,
					"pull_request": map[string]any{"url": "https://api.github.com/repos/weill-labs/orca/pulls/24"},
				},
				"comment": map[string]any{
					"html_url":   "https://github.com/weill-labs/orca/pull/24#issuecomment-1",
					"created_at": "2026-04-13T11:05:00Z",
					"user": map[string]any{
						"login": "reviewer",
					},
				},
			},
			wantOK:    true,
			wantEvent: "issue_comment",
			wantRepo:  "weill-labs/orca",
			wantPR:    24,
			wantTime:  time.Date(2026, 4, 13, 11, 5, 0, 0, time.UTC),
			wantSummary: map[string]string{
				"action":      "created",
				"author":      "reviewer",
				"comment_url": "https://github.com/weill-labs/orca/pull/24#issuecomment-1",
			},
		},
		{
			name:      "check suite",
			eventType: "check_suite",
			payload: map[string]any{
				"action": "completed",
				"repository": map[string]any{
					"full_name": "weill-labs/orca",
				},
				"check_suite": map[string]any{
					"status":      "completed",
					"conclusion":  "success",
					"head_branch": "LAB-1165",
					"updated_at":  "2026-04-13T11:10:00Z",
					"pull_requests": []map[string]any{
						{"number": 11},
					},
				},
			},
			wantOK:    true,
			wantEvent: "check_suite",
			wantRepo:  "weill-labs/orca",
			wantPR:    11,
			wantTime:  time.Date(2026, 4, 13, 11, 10, 0, 0, time.UTC),
			wantSummary: map[string]string{
				"action":      "completed",
				"conclusion":  "success",
				"head_branch": "LAB-1165",
				"status":      "completed",
			},
		},
		{
			name:      "check run",
			eventType: "check_run",
			payload: map[string]any{
				"action": "completed",
				"repository": map[string]any{
					"full_name": "weill-labs/orca",
				},
				"check_run": map[string]any{
					"name":       "ci",
					"status":     "completed",
					"conclusion": "failure",
					"completed_at": "2026-04-13T11:15:00Z",
					"pull_requests": []map[string]any{
						{"number": 17},
					},
				},
			},
			wantOK:    true,
			wantEvent: "check_run",
			wantRepo:  "weill-labs/orca",
			wantPR:    17,
			wantTime:  time.Date(2026, 4, 13, 11, 15, 0, 0, time.UTC),
			wantSummary: map[string]string{
				"action":     "completed",
				"check_name": "ci",
				"conclusion": "failure",
				"status":     "completed",
			},
		},
		{
			name:      "pull request",
			eventType: "pull_request",
			payload: map[string]any{
				"action": "synchronize",
				"repository": map[string]any{
					"full_name": "weill-labs/orca",
				},
				"pull_request": map[string]any{
					"number":     99,
					"state":      "open",
					"merged":     false,
					"updated_at": "2026-04-13T11:20:00Z",
				},
			},
			wantOK:    true,
			wantEvent: "pull_request",
			wantRepo:  "weill-labs/orca",
			wantPR:    99,
			wantTime:  time.Date(2026, 4, 13, 11, 20, 0, 0, time.UTC),
			wantSummary: map[string]string{
				"action": "synchronize",
				"merged": "false",
				"state":  "open",
			},
		},
		{
			name:      "non pull request issue comment is ignored",
			eventType: "issue_comment",
			payload: map[string]any{
				"action": "created",
				"repository": map[string]any{
					"full_name": "weill-labs/orca",
				},
				"issue": map[string]any{
					"number": 7,
				},
				"comment": map[string]any{
					"created_at": "2026-04-13T11:25:00Z",
				},
			},
			wantOK: false,
		},
		{
			name:      "check run falls back to receive time when timestamp missing",
			eventType: "check_run",
			payload: map[string]any{
				"action": "requested_action",
				"repository": map[string]any{
					"full_name": "weill-labs/orca",
				},
				"check_run": map[string]any{
					"name":       "ci",
					"status":     "in_progress",
					"conclusion": "",
					"pull_requests": []map[string]any{
						{"number": 18},
					},
				},
			},
			wantOK:    true,
			wantEvent: "check_run",
			wantRepo:  "weill-labs/orca",
			wantPR:    18,
			wantTime:  now,
			wantSummary: map[string]string{
				"action":     "requested_action",
				"check_name": "ci",
				"status":     "in_progress",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body, err := json.Marshal(tt.payload)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}

			got, ok, err := NormalizeWebhookEvent(tt.eventType, body, now)
			if err != nil {
				t.Fatalf("NormalizeWebhookEvent() error = %v", err)
			}
			if ok != tt.wantOK {
				t.Fatalf("NormalizeWebhookEvent() ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if got.EventType != tt.wantEvent {
				t.Fatalf("EventType = %q, want %q", got.EventType, tt.wantEvent)
			}
			if got.Repo != tt.wantRepo {
				t.Fatalf("Repo = %q, want %q", got.Repo, tt.wantRepo)
			}
			if got.PRNumber != tt.wantPR {
				t.Fatalf("PRNumber = %d, want %d", got.PRNumber, tt.wantPR)
			}
			if !got.Timestamp.Equal(tt.wantTime) {
				t.Fatalf("Timestamp = %s, want %s", got.Timestamp.Format(time.RFC3339), tt.wantTime.Format(time.RFC3339))
			}
			if len(got.PayloadSummary) != len(tt.wantSummary) {
				t.Fatalf("len(PayloadSummary) = %d, want %d", len(got.PayloadSummary), len(tt.wantSummary))
			}
			for key, want := range tt.wantSummary {
				if got.PayloadSummary[key] != want {
					t.Fatalf("PayloadSummary[%q] = %q, want %q", key, got.PayloadSummary[key], want)
				}
			}
		})
	}
}

func signGitHubPayload(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNewGitHubCLIClientAppliesDefaults(t *testing.T) {
	t.Parallel()

	client := newGitHubCLIClient(gitHubCLIClientConfig{})

	if client.now == nil {
		t.Fatal("client.now is nil")
	}
	if client.sleep == nil {
		t.Fatal("client.sleep is nil")
	}
	if got, want := client.maxAttempts, 1; got != want {
		t.Fatalf("client.maxAttempts = %d, want %d", got, want)
	}
}

func TestGitHubCLIClientAppliesMinimumIntervalBetweenCalls(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	clock := &fakeClock{now: time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)}
	var sleeps []time.Duration
	client := newGitHubCLIClient(gitHubCLIClientConfig{
		project:     "/tmp/project",
		commands:    commands,
		now:         clock.Now,
		sleep:       recordSleep(&sleeps, clock),
		minInterval: 500 * time.Millisecond,
		maxAttempts: 1,
	})

	commands.queue("gh", []string{"pr", "list", "--head", "LAB-697", "--json", "number"}, `[{"number":42}]`, nil)
	commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	ctx := context.Background()
	prNumber, err := client.lookupPRNumber(ctx, "LAB-697")
	if err != nil {
		t.Fatalf("lookupPRNumber() error = %v", err)
	}
	if got, want := prNumber, 42; got != want {
		t.Fatalf("lookupPRNumber() = %d, want %d", got, want)
	}

	merged, err := client.isPRMerged(ctx, 42)
	if err != nil {
		t.Fatalf("isPRMerged() error = %v", err)
	}
	if !merged {
		t.Fatal("isPRMerged() = false, want true")
	}

	if got, want := sleeps, []time.Duration{500 * time.Millisecond}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep durations = %#v, want %#v", got, want)
	}
}

func TestDaemonGitHubForProjectCachesClientsPerProject(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	globalClient := newGitHubCLIClient(gitHubCLIClientConfig{
		project:     "",
		commands:    commands,
		sleep:       noSleep,
		maxAttempts: 1,
	})
	d := &Daemon{
		project:  "",
		commands: commands,
		github:   globalClient,
	}

	first := d.githubForProject("/tmp/project-a")
	second := d.githubForProject("/tmp/project-a")
	if first != second {
		t.Fatal("githubForProject() returned different clients for the same project")
	}

	third := d.githubForProject("/tmp/project-b")
	if first == third {
		t.Fatal("githubForProject() reused a client for a different project")
	}

	if got := d.githubForProject(""); got != globalClient {
		t.Fatal("githubForProject(\"\") did not reuse the daemon global client")
	}
}

func TestGitHubCLIClientRetriesRateLimitedRequestsWithBackoff(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	clock := &fakeClock{now: time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)}
	var sleeps []time.Duration
	client := newGitHubCLIClient(gitHubCLIClientConfig{
		project:        "/tmp/project",
		commands:       commands,
		now:            clock.Now,
		sleep:          recordSleep(&sleeps, clock),
		initialBackoff: time.Second,
		maxBackoff:     4 * time.Second,
		maxAttempts:    3,
	})

	args := []string{"pr", "view", "42", "--json", "mergedAt"}
	commands.queue("gh", args, ``, errors.New("gh: API rate limit exceeded for 203.0.113.42"))
	commands.queue("gh", args, ``, errors.New("gh: secondary rate limit: wait a few minutes before trying again"))
	commands.queue("gh", args, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	merged, err := client.isPRMerged(context.Background(), 42)
	if err != nil {
		t.Fatalf("isPRMerged() error = %v", err)
	}
	if !merged {
		t.Fatal("isPRMerged() = false, want true")
	}

	if got, want := sleeps, []time.Duration{time.Second, 2 * time.Second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep durations = %#v, want %#v", got, want)
	}
	if got, want := commands.countCalls("gh", args), 3; got != want {
		t.Fatalf("gh call count = %d, want %d", got, want)
	}
}

func TestGitHubCLIClientDoesNotRetryNonRateLimitErrors(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	clock := &fakeClock{now: time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)}
	var sleeps []time.Duration
	client := newGitHubCLIClient(gitHubCLIClientConfig{
		project:        "/tmp/project",
		commands:       commands,
		now:            clock.Now,
		sleep:          recordSleep(&sleeps, clock),
		initialBackoff: time.Second,
		maxBackoff:     4 * time.Second,
		maxAttempts:    3,
	})

	args := []string{"pr", "list", "--head", "LAB-697", "--json", "number"}
	commands.queue("gh", args, ``, errors.New("gh: authentication failed"))

	_, err := client.lookupPRNumber(context.Background(), "LAB-697")
	if err == nil {
		t.Fatal("lookupPRNumber() succeeded, want error")
	}

	if got, want := sleeps, []time.Duration(nil); !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep durations = %#v, want %#v", got, want)
	}
	if got, want := commands.countCalls("gh", args), 1; got != want {
		t.Fatalf("gh call count = %d, want %d", got, want)
	}
}

func TestGitHubCLIClientLookupOpenPRNumber(t *testing.T) {
	t.Parallel()

	t.Run("returns first open pr number", func(t *testing.T) {
		t.Parallel()

		commands := newFakeCommands()
		client := newGitHubCLIClient(gitHubCLIClientConfig{
			project:     "/tmp/project",
			commands:    commands,
			sleep:       noSleep,
			maxAttempts: 1,
		})
		args := []string{"pr", "list", "--head", "LAB-697", "--state", "open", "--json", "number"}
		commands.queue("gh", args, `[{"number":77}]`, nil)

		got, err := client.lookupOpenPRNumber(context.Background(), "LAB-697")
		if err != nil {
			t.Fatalf("lookupOpenPRNumber() error = %v", err)
		}
		if want := 77; got != want {
			t.Fatalf("lookupOpenPRNumber() = %d, want %d", got, want)
		}
	})

	t.Run("returns command error", func(t *testing.T) {
		t.Parallel()

		commands := newFakeCommands()
		client := newGitHubCLIClient(gitHubCLIClientConfig{
			project:     "/tmp/project",
			commands:    commands,
			sleep:       noSleep,
			maxAttempts: 1,
		})
		args := []string{"pr", "list", "--head", "LAB-697", "--state", "open", "--json", "number"}
		wantErr := errors.New("gh failed")
		commands.queue("gh", args, ``, wantErr)

		_, err := client.lookupOpenPRNumber(context.Background(), "LAB-697")
		if !errors.Is(err, wantErr) {
			t.Fatalf("lookupOpenPRNumber() error = %v, want %v", err, wantErr)
		}
	})
}

func TestGitHubCLIClientLookupOpenOrMergedPRNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		output     string
		err        error
		wantNumber int
		wantMerged bool
		wantErr    bool
	}{
		{
			name:       "returns merged pr number",
			output:     `[{"number":77,"state":"MERGED"}]`,
			wantNumber: 77,
			wantMerged: true,
		},
		{
			name:       "returns open pr number",
			output:     `[{"number":88,"state":"OPEN"}]`,
			wantNumber: 88,
		},
		{
			name:   "skips closed pr number",
			output: `[{"number":66,"state":"CLOSED"}]`,
		},
		{
			name:    "returns command error",
			err:     errors.New("gh failed"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			commands := newFakeCommands()
			client := newGitHubCLIClient(gitHubCLIClientConfig{
				project:     "/tmp/project",
				commands:    commands,
				sleep:       noSleep,
				maxAttempts: 1,
			})
			args := []string{"pr", "list", "--head", "LAB-697", "--state", "all", "--json", "number,state"}
			commands.queue("gh", args, tt.output, tt.err)

			gotNumber, gotMerged, err := client.lookupOpenOrMergedPRNumber(context.Background(), "LAB-697")
			if (err != nil) != tt.wantErr {
				t.Fatalf("lookupOpenOrMergedPRNumber() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if gotNumber != tt.wantNumber {
				t.Fatalf("lookupOpenOrMergedPRNumber() number = %d, want %d", gotNumber, tt.wantNumber)
			}
			if gotMerged != tt.wantMerged {
				t.Fatalf("lookupOpenOrMergedPRNumber() merged = %v, want %v", gotMerged, tt.wantMerged)
			}
		})
	}
}

func TestGitHubCLIClientFindPRByIssueID(t *testing.T) {
	t.Parallel()

	t.Run("returns no match for empty issue id without calling github", func(t *testing.T) {
		t.Parallel()

		commands := newFakeCommands()
		client := newGitHubCLIClient(gitHubCLIClientConfig{
			project:     "/tmp/project",
			commands:    commands,
			sleep:       noSleep,
			maxAttempts: 1,
		})

		number, branch, err := client.findPRByIssueID(context.Background(), "   ")
		if err != nil {
			t.Fatalf("findPRByIssueID() error = %v", err)
		}
		if got := number; got != 0 {
			t.Fatalf("findPRByIssueID() number = %d, want 0", got)
		}
		if got := branch; got != "" {
			t.Fatalf("findPRByIssueID() branch = %q, want empty", got)
		}
		if got := len(commands.callsByName("gh")); got != 0 {
			t.Fatalf("gh call count = %d, want 0", got)
		}
	})

	t.Run("returns pr number and observed branch for unique match", func(t *testing.T) {
		t.Parallel()

		commands := newFakeCommands()
		client := newGitHubCLIClient(gitHubCLIClientConfig{
			project:     "/tmp/project",
			commands:    commands,
			sleep:       noSleep,
			maxAttempts: 1,
		})
		args := []string{"pr", "list", "--search", "LAB-1322 in:title", "--state", "all", "--json", "number,state,headRefName,title", "--limit", "5"}
		commands.queue("gh", args, `[{"number":456,"state":"MERGED","headRefName":"lab-1322-renamed","title":"LAB-1322: recover renamed branch"}]`, nil)

		number, branch, err := client.findPRByIssueID(context.Background(), "LAB-1322")
		if err != nil {
			t.Fatalf("findPRByIssueID() error = %v", err)
		}
		if got, want := number, 456; got != want {
			t.Fatalf("findPRByIssueID() number = %d, want %d", got, want)
		}
		if got, want := branch, "lab-1322-renamed"; got != want {
			t.Fatalf("findPRByIssueID() branch = %q, want %q", got, want)
		}
	})

	t.Run("returns no match and logs when search is ambiguous", func(t *testing.T) {
		t.Parallel()

		commands := newFakeCommands()
		var logs []string
		client := newGitHubCLIClient(gitHubCLIClientConfig{
			project:     "/tmp/project",
			commands:    commands,
			sleep:       noSleep,
			maxAttempts: 1,
			logf: func(format string, args ...any) {
				logs = append(logs, fmt.Sprintf(format, args...))
			},
		})
		args := []string{"pr", "list", "--search", "LAB-1322 in:title", "--state", "all", "--json", "number,state,headRefName,title", "--limit", "5"}
		commands.queue("gh", args, `[
			{"number":456,"state":"OPEN","headRefName":"lab-1322-renamed","title":"LAB-1322: current task"},
			{"number":457,"state":"MERGED","headRefName":"lab-1322-old","title":"follow-up for LAB-1322"}
		]`, nil)

		number, branch, err := client.findPRByIssueID(context.Background(), "LAB-1322")
		if err != nil {
			t.Fatalf("findPRByIssueID() error = %v", err)
		}
		if got := number; got != 0 {
			t.Fatalf("findPRByIssueID() number = %d, want 0", got)
		}
		if got := branch; got != "" {
			t.Fatalf("findPRByIssueID() branch = %q, want empty", got)
		}
		if got := len(logs); got != 1 {
			t.Fatalf("log count = %d, want 1", got)
		}
		if got := logs[0]; !strings.Contains(got, "LAB-1322") || !strings.Contains(got, "multiple pull requests") {
			t.Fatalf("log message = %q, want ambiguous search context", got)
		}
	})

	t.Run("filters out single false positive search results", func(t *testing.T) {
		t.Parallel()

		commands := newFakeCommands()
		client := newGitHubCLIClient(gitHubCLIClientConfig{
			project:     "/tmp/project",
			commands:    commands,
			sleep:       noSleep,
			maxAttempts: 1,
		})
		args := []string{"pr", "list", "--search", "LAB-1322 in:title", "--state", "all", "--json", "number,state,headRefName,title", "--limit", "5"}
		commands.queue("gh", args, `[{"number":456,"state":"MERGED","headRefName":"lab-13220-renamed","title":"LAB-13220: different task"}]`, nil)

		number, branch, err := client.findPRByIssueID(context.Background(), "LAB-1322")
		if err != nil {
			t.Fatalf("findPRByIssueID() error = %v", err)
		}
		if got := number; got != 0 {
			t.Fatalf("findPRByIssueID() number = %d, want 0", got)
		}
		if got := branch; got != "" {
			t.Fatalf("findPRByIssueID() branch = %q, want empty", got)
		}
	})

	t.Run("keeps exact issue match when siblings are present", func(t *testing.T) {
		t.Parallel()

		commands := newFakeCommands()
		client := newGitHubCLIClient(gitHubCLIClientConfig{
			project:     "/tmp/project",
			commands:    commands,
			sleep:       noSleep,
			maxAttempts: 1,
		})
		args := []string{"pr", "list", "--search", "LAB-1322 in:title", "--state", "all", "--json", "number,state,headRefName,title", "--limit", "5"}
		commands.queue("gh", args, `[
			{"number":455,"state":"MERGED","headRefName":"lab-13220-renamed","title":"LAB-13220: sibling"},
			{"number":456,"state":"MERGED","headRefName":"lab-1322-renamed","title":"LAB-1322: exact match"}
		]`, nil)

		number, branch, err := client.findPRByIssueID(context.Background(), "LAB-1322")
		if err != nil {
			t.Fatalf("findPRByIssueID() error = %v", err)
		}
		if got, want := number, 456; got != want {
			t.Fatalf("findPRByIssueID() number = %d, want %d", got, want)
		}
		if got, want := branch, "lab-1322-renamed"; got != want {
			t.Fatalf("findPRByIssueID() branch = %q, want %q", got, want)
		}
	})
}

func TestParsePRNumberList(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		output []byte
		want   int
		hasErr bool
	}{
		{name: "empty output"},
		{name: "empty list", output: []byte(`[]`)},
		{name: "single entry", output: []byte(`[{"number":19}]`), want: 19},
		{name: "invalid json", output: []byte(`{`), hasErr: true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parsePRNumberList(tc.output)
			if (err != nil) != tc.hasErr {
				t.Fatalf("parsePRNumberList() error = %v, hasErr = %v", err, tc.hasErr)
			}
			if got != tc.want {
				t.Fatalf("parsePRNumberList() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestParseIssueIDPRSearchResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		issueID    string
		output     []byte
		wantNumber int
		wantBranch string
		wantErr    bool
	}{
		{name: "empty output", issueID: "LAB-1322"},
		{name: "empty list", issueID: "LAB-1322", output: []byte(`[]`)},
		{name: "single match", issueID: "LAB-1322", output: []byte(`[{"number":456,"state":"MERGED","headRefName":"lab-1322-renamed","title":"LAB-1322: recover renamed branch"}]`), wantNumber: 456, wantBranch: "lab-1322-renamed"},
		{name: "single false positive", issueID: "LAB-1322", output: []byte(`[{"number":456,"state":"MERGED","headRefName":"lab-13220-renamed","title":"LAB-13220: sibling"}]`)},
		{name: "filters siblings to one exact match", issueID: "LAB-1322", output: []byte(`[{"number":455,"state":"MERGED","headRefName":"lab-13220-renamed","title":"LAB-13220: sibling"},{"number":456,"state":"MERGED","headRefName":"lab-1322-renamed","title":"LAB-1322: exact match"}]`), wantNumber: 456, wantBranch: "lab-1322-renamed"},
		{name: "multiple exact matches", issueID: "LAB-1322", output: []byte(`[{"number":456,"state":"OPEN","headRefName":"lab-1322-renamed","title":"LAB-1322: current task"},{"number":457,"state":"MERGED","headRefName":"follow-up-lab-1322","title":"follow-up for LAB-1322"}]`)},
		{name: "invalid json", issueID: "LAB-1322", output: []byte(`{`), wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotNumber, gotBranch, gotMultiple, err := parseIssueIDPRSearchResults(tt.output, tt.issueID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseIssueIDPRSearchResults() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if gotNumber != tt.wantNumber {
				t.Fatalf("parseIssueIDPRSearchResults() number = %d, want %d", gotNumber, tt.wantNumber)
			}
			if gotBranch != tt.wantBranch {
				t.Fatalf("parseIssueIDPRSearchResults() branch = %q, want %q", gotBranch, tt.wantBranch)
			}
			if got, want := gotMultiple, tt.name == "multiple exact matches"; got != want {
				t.Fatalf("parseIssueIDPRSearchResults() multiple = %v, want %v", got, want)
			}
		})
	}
}

func TestContainsIssueIDToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		issueID string
		want    bool
	}{
		{name: "empty value", issueID: "LAB-1322"},
		{name: "empty issue id", value: "LAB-1322: title"},
		{name: "title prefix", value: "LAB-1322: title", issueID: "LAB-1322", want: true},
		{name: "branch prefix", value: "lab-1322-renamed", issueID: "LAB-1322", want: true},
		{name: "embedded suffix digit", value: "LAB-13220: title", issueID: "LAB-1322"},
		{name: "embedded inside token", value: "featureLAB-1322", issueID: "LAB-1322"},
		{name: "surrounded by separators", value: "fix/LAB-1322.branch", issueID: "LAB-1322", want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := containsIssueIDToken(tt.value, tt.issueID); got != tt.want {
				t.Fatalf("containsIssueIDToken(%q, %q) = %v, want %v", tt.value, tt.issueID, got, tt.want)
			}
		})
	}
}

func TestParseOpenOrMergedPRNumberList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		output     []byte
		wantNumber int
		wantMerged bool
		wantErr    bool
	}{
		{name: "empty output"},
		{name: "empty list", output: []byte(`[]`)},
		{name: "merged entry", output: []byte(`[{"number":19,"state":"MERGED"}]`), wantNumber: 19, wantMerged: true},
		{name: "open entry", output: []byte(`[{"number":20,"state":"OPEN"}]`), wantNumber: 20},
		{name: "closed entry", output: []byte(`[{"number":21,"state":"CLOSED"}]`)},
		{name: "closed entry skipped for later open pr", output: []byte(`[{"number":21,"state":"CLOSED"},{"number":22,"state":"OPEN"}]`), wantNumber: 22},
		{name: "invalid json", output: []byte(`{`), wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotNumber, gotMerged, err := parseOpenOrMergedPRNumberList(tt.output)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseOpenOrMergedPRNumberList() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if gotNumber != tt.wantNumber {
				t.Fatalf("parseOpenOrMergedPRNumberList() number = %d, want %d", gotNumber, tt.wantNumber)
			}
			if gotMerged != tt.wantMerged {
				t.Fatalf("parseOpenOrMergedPRNumberList() merged = %v, want %v", gotMerged, tt.wantMerged)
			}
		})
	}
}

func TestGitHubCLIClientIsPRMergedEdgeCases(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		output  string
		err     error
		want    bool
		wantErr bool
	}{
		{name: "empty output"},
		{name: "empty mergedAt", output: `{"mergedAt":""}`},
		{name: "invalid json", output: `{`, wantErr: true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			commands := newFakeCommands()
			client := newGitHubCLIClient(gitHubCLIClientConfig{
				project:     "/tmp/project",
				commands:    commands,
				sleep:       noSleep,
				maxAttempts: 1,
			})
			args := []string{"pr", "view", "42", "--json", "mergedAt"}
			commands.queue("gh", args, tc.output, tc.err)

			got, err := client.isPRMerged(context.Background(), 42)
			if (err != nil) != tc.wantErr {
				t.Fatalf("isPRMerged() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("isPRMerged() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGitHubCLIClientPRTerminalState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		output    string
		err       error
		wantState prTerminalState
		wantErr   bool
	}{
		{name: "empty output defaults open", wantState: prTerminalState{}},
		{name: "open pr", output: `{"state":"OPEN","mergedAt":null,"closedAt":null}`, wantState: prTerminalState{}},
		{name: "merged pr by state", output: `{"state":"MERGED","mergedAt":null,"closedAt":null}`, wantState: prTerminalState{merged: true}},
		{name: "merged pr by mergedAt", output: `{"state":"CLOSED","mergedAt":"2026-04-02T12:00:00Z","closedAt":"2026-04-02T12:00:00Z"}`, wantState: prTerminalState{merged: true}},
		{name: "closed without merge by state", output: `{"state":"CLOSED","mergedAt":null,"closedAt":null}`, wantState: prTerminalState{closedWithoutMerge: true}},
		{name: "closed without merge by closedAt", output: `{"state":"OPEN","mergedAt":null,"closedAt":"2026-04-02T12:00:00Z"}`, wantState: prTerminalState{closedWithoutMerge: true}},
		{name: "command error", err: errors.New("gh failed"), wantErr: true},
		{name: "invalid json", output: `{`, wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			commands := newFakeCommands()
			client := newGitHubCLIClient(gitHubCLIClientConfig{
				project:     "/tmp/project",
				commands:    commands,
				sleep:       noSleep,
				maxAttempts: 1,
			})
			args := []string{"pr", "view", "42", "--json", prSnapshotJSONFields}
			commands.queue("gh", args, tt.output, tt.err)

			got, err := client.lookupPRTerminalState(context.Background(), 42)
			if (err != nil) != tt.wantErr {
				t.Fatalf("lookupPRTerminalState() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got != tt.wantState {
				t.Fatalf("lookupPRTerminalState() = %#v, want %#v", got, tt.wantState)
			}
		})
	}
}

func TestGitHubCLIClientLookupPRReviewsEdgeCases(t *testing.T) {
	t.Parallel()

	apiArgs := []string{"api", "repos/{owner}/{repo}/pulls/42/comments?per_page=100"}
	testCases := []struct {
		name         string
		output       string
		wantOK       bool
		wantBody     string
		wantErr      bool
		wantAPICalls int
	}{
		{name: "empty output"},
		{
			name:         "valid payload",
			output:       `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`,
			wantOK:       true,
			wantBody:     "Please add tests.",
			wantAPICalls: 0,
		},
		{name: "invalid json", output: `{`, wantErr: true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			commands := newFakeCommands()
			client := newGitHubCLIClient(gitHubCLIClientConfig{
				project:     "/tmp/project",
				commands:    commands,
				sleep:       noSleep,
				maxAttempts: 1,
			})
			args := []string{"pr", "view", "42", "--json", prReviewJSONFields}
			commands.queue("gh", args, tc.output, nil)
			if tc.wantAPICalls > 0 {
				commands.queue("gh", apiArgs, `[]`, nil)
			}

			payload, ok, err := client.lookupPRReviews(context.Background(), 42)
			if (err != nil) != tc.wantErr {
				t.Fatalf("lookupPRReviews() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if ok != tc.wantOK {
				t.Fatalf("lookupPRReviews() ok = %v, want %v", ok, tc.wantOK)
			}
			if tc.wantBody != "" && payload.Reviews[0].Body != tc.wantBody {
				t.Fatalf("first review body = %q, want %q", payload.Reviews[0].Body, tc.wantBody)
			}
			if got := commands.countCalls("gh", apiArgs); got != tc.wantAPICalls {
				t.Fatalf("inline comments api call count = %d, want %d", got, tc.wantAPICalls)
			}
		})
	}
}

func TestGitHubCLIClientLookupPRReviewsIncludesIssueComments(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	client := newGitHubCLIClient(gitHubCLIClientConfig{
		project:     "/tmp/project",
		commands:    commands,
		sleep:       noSleep,
		maxAttempts: 1,
	})
	viewArgs := []string{"pr", "view", "42", "--json", prReviewJSONFields}
	apiArgs := []string{"api", "repos/{owner}/{repo}/pulls/42/comments?per_page=100"}
	commands.queue("gh", viewArgs, `{"reviewDecision":"APPROVED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"### Blocking Issues\n\n**1. Add regression coverage**"}]}`, nil)

	payload, ok, err := client.lookupPRReviews(context.Background(), 42)
	if err != nil {
		t.Fatalf("lookupPRReviews() error = %v", err)
	}
	if !ok {
		t.Fatal("lookupPRReviews() ok = false, want true")
	}
	if got, want := len(payload.Comments), 1; got != want {
		t.Fatalf("len(payload.Comments) = %d, want %d", got, want)
	}
	if got, want := payload.Comments[0].Author.Login, "github-actions"; got != want {
		t.Fatalf("first comment author = %q, want %q", got, want)
	}
	if got, want := payload.Comments[0].Body, "### Blocking Issues\n\n**1. Add regression coverage**"; got != want {
		t.Fatalf("first comment body = %q, want %q", got, want)
	}
	if got, want := commands.countCalls("gh", apiArgs), 0; got != want {
		t.Fatalf("inline comments api call count = %d, want %d", got, want)
	}
}

func TestGitHubCLIClientLookupPRReviewCommentsIncludesInlineReviewComments(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	client := newGitHubCLIClient(gitHubCLIClientConfig{
		project:     "/tmp/project",
		commands:    commands,
		sleep:       noSleep,
		maxAttempts: 1,
	})
	apiArgs := []string{"api", "repos/{owner}/{repo}/pulls/42/comments?per_page=100"}
	commands.queue("gh", apiArgs, `[
		{
			"user":{"login":"alice"},
			"path":"internal/daemon/review.go",
			"line":174,
			"body":"Include reviewer details in the worker nudge.",
			"created_at":"2026-04-02T09:05:00Z"
		}
	]`, nil)

	payload, err := client.lookupPRReviewComments(context.Background(), 42)
	if err != nil {
		t.Fatalf("lookupPRReviewComments() error = %v", err)
	}
	if got, want := len(payload), 1; got != want {
		t.Fatalf("len(payload.ReviewComments) = %d, want %d", got, want)
	}
	if got, want := payload[0].User.Login, "alice"; got != want {
		t.Fatalf("first review comment author = %q, want %q", got, want)
	}
	if got, want := payload[0].Path, "internal/daemon/review.go"; got != want {
		t.Fatalf("first review comment path = %q, want %q", got, want)
	}
	if got, want := payload[0].Line, 174; got != want {
		t.Fatalf("first review comment line = %d, want %d", got, want)
	}
}

func TestGitHubCLIClientStopsWhenBackoffSleepFails(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	client := newGitHubCLIClient(gitHubCLIClientConfig{
		project:        "/tmp/project",
		commands:       commands,
		sleep:          func(context.Context, time.Duration) error { return context.Canceled },
		initialBackoff: time.Second,
		maxAttempts:    3,
	})
	args := []string{"pr", "view", "42", "--json", "mergedAt"}
	commands.queue("gh", args, ``, errors.New("gh: API rate limit exceeded"))

	_, err := client.isPRMerged(context.Background(), 42)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("isPRMerged() error = %v, want %v", err, context.Canceled)
	}
	if got, want := commands.countCalls("gh", args), 1; got != want {
		t.Fatalf("gh call count = %d, want %d", got, want)
	}
}

func TestGitHubCLIClientReturnsRateLimitErrorAfterMaxAttempts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		output    string
		wantUntil time.Time
	}{
		{
			name:      "retry after header",
			output:    "HTTP 429: API rate limit exceeded\nRetry-After: 120\n",
			wantUntil: time.Date(2026, 4, 2, 9, 2, 0, 0, time.UTC),
		},
		{
			name:      "reset header",
			output:    "HTTP 403: API rate limit exceeded\nX-RateLimit-Reset: 1775725560\n",
			wantUntil: time.Unix(1775725560, 0).UTC(),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			commands := newFakeCommands()
			clock := &fakeClock{now: time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)}
			client := newGitHubCLIClient(gitHubCLIClientConfig{
				project:        "/tmp/project",
				commands:       commands,
				now:            clock.Now,
				sleep:          noSleep,
				initialBackoff: time.Second,
				maxAttempts:    2,
			})
			args := []string{"pr", "view", "42", "--json", "mergedAt"}
			wantErr := errors.New("gh: HTTP 429")
			commands.queue("gh", args, tt.output, wantErr)
			commands.queue("gh", args, tt.output, wantErr)

			_, err := client.isPRMerged(context.Background(), 42)
			if !errors.Is(err, wantErr) {
				t.Fatalf("isPRMerged() error = %v, want %v", err, wantErr)
			}
			var rateLimited interface{ RateLimitedUntil() time.Time }
			if !errors.As(err, &rateLimited) {
				t.Fatalf("isPRMerged() error = %v, want rate limit metadata", err)
			}
			if got := rateLimited.RateLimitedUntil(); !got.Equal(tt.wantUntil) {
				t.Fatalf("rate limited until = %v, want %v", got, tt.wantUntil)
			}
			if got, want := commands.countCalls("gh", args), 2; got != want {
				t.Fatalf("gh call count = %d, want %d", got, want)
			}
		})
	}
}

func TestNextBackoff(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		current time.Duration
		max     time.Duration
		want    time.Duration
	}{
		{name: "non-positive current", current: 0, max: time.Second},
		{name: "doubles within max", current: time.Second, max: 3 * time.Second, want: 2 * time.Second},
		{name: "clamps to max", current: 2 * time.Second, max: 3 * time.Second, want: 3 * time.Second},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := nextBackoff(tc.current, tc.max); got != tc.want {
				t.Fatalf("nextBackoff() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsGitHubRateLimitError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		err    error
		output []byte
		want   bool
	}{
		{name: "nil error"},
		{name: "auth error", err: errors.New("authentication failed")},
		{name: "secondary rate limit in error", err: errors.New("Secondary Rate Limit"), want: true},
		{name: "rate limit in output", err: errors.New("gh failed"), output: []byte("API rate limit exceeded"), want: true},
		{name: "http 403 rate limit", err: errors.New("gh: HTTP 403"), output: []byte("rate limit exceeded"), want: true},
		{name: "http 429 retry after", err: errors.New("gh: HTTP 429"), output: []byte("Retry-After: 120"), want: true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isGitHubRateLimitError(tc.err, tc.output); got != tc.want {
				t.Fatalf("isGitHubRateLimitError() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGitHubRateLimitDeadline(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		err      error
		output   []byte
		fallback time.Duration
		want     time.Time
	}{
		{
			name:     "invalid retry-after falls back to explicit backoff",
			err:      errors.New("gh: HTTP 429"),
			output:   []byte("Retry-After: 0"),
			fallback: 8 * time.Second,
			want:     now.Add(8 * time.Second),
		},
		{
			name:   "invalid reset header falls back to default minute",
			err:    errors.New("gh: HTTP 403"),
			output: []byte("X-RateLimit-Reset: 0"),
			want:   now.Add(time.Minute),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := gitHubRateLimitDeadline(tt.err, tt.output, now, tt.fallback); !got.Equal(tt.want) {
				t.Fatalf("gitHubRateLimitDeadline() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSleepContext(t *testing.T) {
	t.Parallel()

	t.Run("returns immediately for zero delay", func(t *testing.T) {
		t.Parallel()
		if err := sleepContext(context.Background(), 0); err != nil {
			t.Fatalf("sleepContext() error = %v", err)
		}
	})

	t.Run("returns context error when canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := sleepContext(ctx, time.Second); !errors.Is(err, context.Canceled) {
			t.Fatalf("sleepContext() error = %v, want %v", err, context.Canceled)
		}
	})

	t.Run("waits for timer", func(t *testing.T) {
		t.Parallel()
		if err := sleepContext(context.Background(), time.Millisecond); err != nil {
			t.Fatalf("sleepContext() error = %v", err)
		}
	})
}

func recordSleep(sleeps *[]time.Duration, clock *fakeClock) func(context.Context, time.Duration) error {
	return func(_ context.Context, delay time.Duration) error {
		*sleeps = append(*sleeps, delay)
		clock.Advance(delay)
		return nil
	}
}

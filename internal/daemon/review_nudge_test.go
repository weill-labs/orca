package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPRReviewPollingSkipsNudgesForApprovalOrLGTM(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name                  string
		payload               string
		wantReviewCount       int
		wantIssueCommentCount int
	}

	tests := []testCase{
		{
			name: "approved review decision skips github-actions comments without lgtm",
			payload: marshalReviewPayload(t,
				"APPROVED",
				nil,
				[]prComment{
					testIssueComment("github-actions", "Potential bug: add regression coverage."),
				},
			),
			wantIssueCommentCount: 1,
		},
		{
			name: "github-actions lgtm skips issue comments with blocking headings",
			payload: marshalReviewPayload(t,
				"CHANGES_REQUESTED",
				nil,
				[]prComment{
					testIssueComment("github-actions", "### PR Review\n\n### Blocking Issues\n\n**1. Add regression coverage**\n\nLGTM"),
				},
			),
			wantIssueCommentCount: 1,
		},
		{
			name: "github-actions review progress comment does not nudge worker",
			payload: marshalReviewPayload(t,
				"CHANGES_REQUESTED",
				nil,
				[]prComment{
					testIssueComment("github-actions", "### PR Review: LAB-985\n\nReviewing...\n\n- [x] Gather context\n- [ ] Read changed files\n- [ ] Post final review\n\n[View job run](https://github.com/weill-labs/orca/actions/runs/24174001351)"),
				},
			),
			wantIssueCommentCount: 1,
		},
		{
			name: "latest review lgtm skips older blocking reviews",
			payload: marshalReviewPayload(t,
				"CHANGES_REQUESTED",
				[]prReview{
					testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
					testReview("bob", "COMMENTED", "Looks good to me.\nlGtM\nShip it."),
				},
				nil,
			),
			wantReviewCount: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			captureTicker := newFakeTicker()
			prTicker := newFakeTicker()
			deps.tickers.enqueue(captureTicker, prTicker)
			deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
			deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
			deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, tt.payload, nil)

			d := deps.newDaemon(t)
			ctx := context.Background()
			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})

			if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
				t.Fatalf("Assign() error = %v", err)
			}

			prTicker.tick(deps.clock.Now())
			waitFor(t, "review poll processed", func() bool {
				worker, ok := deps.state.worker("pane-1")
				return ok &&
					deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 1 &&
					worker.LastReviewCount == tt.wantReviewCount &&
					worker.LastIssueCommentCount == tt.wantIssueCommentCount
			})

			if got := deps.events.countType(EventWorkerNudgedReview); got != 0 {
				t.Fatalf("review nudge event count = %d, want 0", got)
			}
			deps.amux.requireSentKeys(t, "pane-1", []string{wrappedCodexPrompt("Implement daemon core") + "\n"})
		})
	}
}

func TestPRReviewPollingEscalatesAfterThreeNudgesAndResetsAfterApprovalCycle(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
	}, nil), nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
		testReview("bob", "CHANGES_REQUESTED", "Handle the nil case too."),
	}, nil), nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
		testReview("bob", "CHANGES_REQUESTED", "Handle the nil case too."),
		testReview("carol", "CHANGES_REQUESTED", "Cover the restart flow."),
	}, nil), nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
		testReview("bob", "CHANGES_REQUESTED", "Handle the nil case too."),
		testReview("carol", "CHANGES_REQUESTED", "Cover the restart flow."),
		testReview("dave", "CHANGES_REQUESTED", "Persist the nudge counter."),
	}, nil), nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "APPROVED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
		testReview("bob", "CHANGES_REQUESTED", "Handle the nil case too."),
		testReview("carol", "CHANGES_REQUESTED", "Cover the restart flow."),
		testReview("dave", "CHANGES_REQUESTED", "Persist the nudge counter."),
	}, nil), nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
		testReview("bob", "CHANGES_REQUESTED", "Handle the nil case too."),
		testReview("carol", "CHANGES_REQUESTED", "Cover the restart flow."),
		testReview("dave", "CHANGES_REQUESTED", "Persist the nudge counter."),
		testReview("erin", "CHANGES_REQUESTED", "Add reset coverage."),
	}, nil), nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	makeWorkerIdleForReviewNudge(deps)

	aliceNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update.\n"
	bobNudge := "New blocking PR review feedback on #42:\n- bob: Handle the nil case too.\n\nAddress the feedback in the PR review and push an update.\n"
	carolNudge := "New blocking PR review feedback on #42:\n- carol: Cover the restart flow.\n\nAddress the feedback in the PR review and push an update.\n"
	daveNudge := "New blocking PR review feedback on #42:\n- dave: Persist the nudge counter.\n\nAddress the feedback in the PR review and push an update.\n"
	erinNudge := "New blocking PR review feedback on #42:\n- erin: Add reset coverage.\n\nAddress the feedback in the PR review and push an update.\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "first review nudge", func() bool {
		return deps.amux.countKey("pane-1", aliceNudge) == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second review nudge", func() bool {
		return deps.amux.countKey("pane-1", bobNudge) == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "third review nudge", func() bool {
		return deps.amux.countKey("pane-1", carolNudge) == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "review escalation", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastReviewCount == 4 &&
			deps.events.countType(EventWorkerReviewEscalated) == 1
	})
	if got := deps.amux.countKey("pane-1", daveNudge); got != 0 {
		t.Fatalf("fourth review nudge count = %d, want 0", got)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "approval poll processed", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 5
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "review nudge after reset", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastReviewCount == 5 &&
			deps.amux.countKey("pane-1", erinNudge) == 1
	})

	if got, want := deps.events.countType(EventWorkerNudgedReview), 4; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerReviewEscalated), 1; got != want {
		t.Fatalf("review escalation event count = %d, want %d", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{
		wrappedCodexPrompt("Implement daemon core") + "\n",
		aliceNudge,
		bobNudge,
		carolNudge,
		erinNudge,
	})
}

func TestPRReviewPollingNotifiesCallerPaneWhenReviewNudgesAreExhausted(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
	}, nil), nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
		testReview("bob", "CHANGES_REQUESTED", "Handle the nil case too."),
	}, nil), nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
		testReview("bob", "CHANGES_REQUESTED", "Handle the nil case too."),
		testReview("carol", "CHANGES_REQUESTED", "Cover the restart flow."),
	}, nil), nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
		testReview("bob", "CHANGES_REQUESTED", "Handle the nil case too."),
		testReview("carol", "CHANGES_REQUESTED", "Cover the restart flow."),
		testReview("dave", "CHANGES_REQUESTED", "Persist the nudge counter."),
	}, nil), nil)

	d := deps.newDaemon(t)
	d.leadPane = "fallback-lead-pane"
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.AssignWithCallerPane(ctx, "LAB-689", "Implement daemon core", "codex", "caller-pane-13"); err != nil {
		t.Fatalf("AssignWithCallerPane() error = %v", err)
	}
	makeWorkerIdleForReviewNudge(deps)

	aliceNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update.\n"
	bobNudge := "New blocking PR review feedback on #42:\n- bob: Handle the nil case too.\n\nAddress the feedback in the PR review and push an update.\n"
	carolNudge := "New blocking PR review feedback on #42:\n- carol: Cover the restart flow.\n\nAddress the feedback in the PR review and push an update.\n"
	leadNotification := "Review nudges exhausted for LAB-689 in w-LAB-689 on PR #42.\nUnresolved review feedback:\n- alice: Please add tests.\n- bob: Handle the nil case too.\n- carol: Cover the restart flow.\n- dave: Persist the nudge counter.\n\nIntervene in w-LAB-689 to address the feedback or reassign the task.\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "first review nudge", func() bool {
		return deps.amux.countKey("pane-1", aliceNudge) == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second review nudge", func() bool {
		return deps.amux.countKey("pane-1", bobNudge) == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "third review nudge", func() bool {
		return deps.amux.countKey("pane-1", carolNudge) == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "lead review escalation notification", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastReviewCount == 4 &&
			deps.amux.countKey("caller-pane-13", leadNotification) == 1 &&
			deps.events.countType(EventWorkerReviewEscalated) == 1
	})

	deps.amux.requireSentKeys(t, "caller-pane-13", []string{leadNotification})
}

func TestPRReviewPollingDefersReviewNudgeUntilWorkerAppearsIdle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		prepareWorker func(deps *testDeps)
		assertWorker  func(t *testing.T, worker Worker)
	}{
		{
			name: "recent worker output defers without capture",
			prepareWorker: func(deps *testDeps) {
				worker, ok := deps.state.worker("pane-1")
				if !ok {
					t.Fatal("worker not found")
				}
				worker.LastActivityAt = deps.clock.Now()
				if err := deps.state.PutWorker(context.Background(), worker); err != nil {
					t.Fatalf("PutWorker() error = %v", err)
				}
			},
			assertWorker: func(t *testing.T, worker Worker) {
				t.Helper()
				if worker.LastActivityAt.IsZero() {
					t.Fatal("LastActivityAt should remain set")
				}
				if worker.LastCapture != "" {
					t.Fatalf("LastCapture = %q, want unchanged empty capture", worker.LastCapture)
				}
			},
		},
		{
			name: "fresh capture output defers nudge",
			prepareWorker: func(deps *testDeps) {
				makeWorkerIdleForReviewNudge(deps)
				deps.amux.capturePaneSequence("pane-1", []PaneCapture{paneCaptureFromOutput("worker is still typing")})
			},
			assertWorker: func(t *testing.T, worker Worker) {
				t.Helper()
				if got, want := worker.LastCapture, "worker is still typing"; got != want {
					t.Fatalf("LastCapture = %q, want %q", got, want)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			captureTicker := newFakeTicker()
			prTicker := newFakeTicker()
			deps.tickers.enqueue(captureTicker, prTicker)
			deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
			deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
			deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
				testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
			}, nil), nil)

			d := deps.newDaemon(t)
			ctx := context.Background()
			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})

			if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
				t.Fatalf("Assign() error = %v", err)
			}

			tt.prepareWorker(deps)

			prTicker.tick(deps.clock.Now())
			waitFor(t, "review poll processed", func() bool {
				return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 1
			})

			if got := deps.events.countType(EventWorkerNudgedReview); got != 0 {
				t.Fatalf("review nudge event count = %d, want 0", got)
			}

			worker, ok := deps.state.worker("pane-1")
			if !ok {
				t.Fatal("worker not found after deferred nudge")
			}
			if got, want := worker.LastReviewCount, 0; got != want {
				t.Fatalf("worker.LastReviewCount = %d, want %d", got, want)
			}
			tt.assertWorker(t, worker)
		})
	}
}

func TestWorkerAppearsIdleForReviewNudgeHandlesCaptureEdges(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 6, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		captureSetup func(amux *fakeAmux)
		worker       Worker
		wantIdle     bool
		wantCapture  int
	}{
		{
			name: "capture errors are treated as idle",
			captureSetup: func(amux *fakeAmux) {
				amux.capturePaneErr = errors.New("capture failed")
			},
			worker: Worker{
				PaneID:         "pane-1",
				LastActivityAt: now.Add(-11 * time.Second),
			},
			wantIdle:    true,
			wantCapture: 1,
		},
		{
			name: "exited panes are not idle for nudging",
			captureSetup: func(amux *fakeAmux) {
				amux.capturePaneSequence("pane-1", []PaneCapture{{Exited: true}})
			},
			worker: Worker{
				PaneID:         "pane-1",
				LastActivityAt: now.Add(-11 * time.Second),
			},
			wantIdle:    false,
			wantCapture: 1,
		},
		{
			name: "unchanged output is idle",
			captureSetup: func(amux *fakeAmux) {
				amux.capturePaneSequence("pane-1", []PaneCapture{paneCaptureFromOutput("same output")})
			},
			worker: Worker{
				PaneID:         "pane-1",
				LastCapture:    "same output",
				LastActivityAt: now.Add(-11 * time.Second),
			},
			wantIdle:    true,
			wantCapture: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.clock.now = now
			tt.captureSetup(deps.amux)

			d := deps.newDaemon(t)
			update := &TaskStateUpdate{Active: ActiveAssignment{
				Task: Task{
					Project:   "/tmp/project",
					Issue:     "LAB-804",
					PaneID:    "pane-1",
					ClonePath: "/tmp/clone-01",
				},
				Worker: tt.worker,
			}}

			got := d.workerAppearsIdleForReviewNudge(context.Background(), update, AgentProfile{Name: "codex"}, now)
			if got != tt.wantIdle {
				t.Fatalf("workerAppearsIdleForReviewNudge() = %t, want %t", got, tt.wantIdle)
			}
			if got, want := deps.amux.captureCount("pane-1"), tt.wantCapture; got != want {
				t.Fatalf("capture count = %d, want %d", got, want)
			}
		})
	}
}

func TestReviewNudgeIdleThresholdAndRecentOutput(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 6, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name            string
		captureInterval time.Duration
		lastActivityAt  time.Time
		wantThreshold   time.Duration
		wantRecent      bool
	}{
		{
			name:            "disabled capture interval",
			captureInterval: 0,
			lastActivityAt:  now,
			wantThreshold:   0,
			wantRecent:      false,
		},
		{
			name:            "zero last activity",
			captureInterval: 5 * time.Second,
			lastActivityAt:  time.Time{},
			wantThreshold:   10 * time.Second,
			wantRecent:      false,
		},
		{
			name:            "recent activity within threshold",
			captureInterval: 5 * time.Second,
			lastActivityAt:  now.Add(-9 * time.Second),
			wantThreshold:   10 * time.Second,
			wantRecent:      true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.clock.now = now
			d := deps.newDaemon(t)
			d.captureInterval = tt.captureInterval

			if got, want := d.reviewNudgeIdleThreshold(), tt.wantThreshold; got != want {
				t.Fatalf("reviewNudgeIdleThreshold() = %v, want %v", got, want)
			}
			if got, want := d.workerHadRecentOutput(Worker{LastActivityAt: tt.lastActivityAt}, now), tt.wantRecent; got != want {
				t.Fatalf("workerHadRecentOutput() = %t, want %t", got, want)
			}
		})
	}
}

func TestReviewFormattingHelpers(t *testing.T) {
	t.Parallel()

	t.Run("normalize review body", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			body string
			want string
		}{
			{
				name: "empty body",
				body: " \n\t ",
				want: "requested changes without a review body.",
			},
			{
				name: "collapses whitespace",
				body: "Please   add\n\nregression\tcoverage.",
				want: "Please add regression coverage.",
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if got := normalizeReviewBody(tt.body); got != tt.want {
					t.Fatalf("normalizeReviewBody(%q) = %q, want %q", tt.body, got, tt.want)
				}
			})
		}
	})

	t.Run("trim leading issue number", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name  string
			title string
			want  string
		}{
			{name: "strips numbered prefix", title: "1. Add coverage", want: "Add coverage"},
			{name: "preserves non-numbered title", title: "Add coverage", want: "Add coverage"},
			{name: "preserves numeric suffix without dot", title: "123", want: "123"},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if got := trimLeadingIssueNumber(tt.title); got != tt.want {
					t.Fatalf("trimLeadingIssueNumber(%q) = %q, want %q", tt.title, got, tt.want)
				}
			})
		}
	})

	t.Run("extract blocking issue titles", func(t *testing.T) {
		t.Parallel()

		section := strings.Join([]string{
			"**1. Add regression coverage**",
			"### Ignored heading",
			"#### 2. Handle nil pane state",
			"plain text",
		}, "\n")
		got := extractBlockingIssueTitles(section)
		want := []string{"Add regression coverage", "Handle nil pane state"}
		if len(got) != len(want) {
			t.Fatalf("len(extractBlockingIssueTitles()) = %d, want %d (%#v)", len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("extractBlockingIssueTitles()[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	})
}

func marshalReviewPayload(t *testing.T, reviewDecision string, reviews []prReview, comments []prComment) string {
	t.Helper()

	payload := prReviewPayload{
		ReviewDecision: reviewDecision,
		Reviews:        reviews,
		Comments:       comments,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v", err)
	}
	return string(encoded)
}

func testReview(author, state, body string) prReview {
	review := prReview{
		State: state,
		Body:  body,
	}
	review.Author.Login = author
	return review
}

func testIssueComment(author, body string) prComment {
	comment := prComment{Body: body}
	comment.Author.Login = author
	return comment
}

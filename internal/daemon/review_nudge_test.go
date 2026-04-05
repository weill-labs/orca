package daemon

import (
	"context"
	"encoding/json"
	"testing"
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
			name: "approved review decision skips blocking issue comments",
			payload: marshalReviewPayload(t,
				"APPROVED",
				nil,
				[]prComment{
					testIssueComment("github-actions", "### PR Review\n\n### Blocking Issues\n\n**1. Add regression coverage**"),
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
			deps.amux.requireSentKeys(t, "pane-1", []string{"Implement daemon core\n"})
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
		"Implement daemon core\n",
		aliceNudge,
		bobNudge,
		carolNudge,
		erinNudge,
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

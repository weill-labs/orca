package daemon

import (
	"context"
	"strings"
)

const (
	githubAPIKindGraphQL = "graphql"
	githubAPIKindREST    = "rest"

	githubAPIConsumerTerminalState = "terminal_state"
	githubAPIConsumerReview        = "review"
	githubAPIConsumerOther         = "other"
)

type githubAPIConsumerContextKey struct{}

type githubAPICallMetrics struct {
	kind            string
	consumer        string
	estimatedPoints int
}

type githubAPICounters struct {
	GraphQLCalls              int
	GraphQLEstimatedPoints    int
	GraphQLTerminalStateCalls int
	GraphQLReviewCalls        int
	GraphQLOtherCalls         int
	RESTCalls                 int
	RESTReviewCalls           int
	RESTOtherCalls            int
}

type githubAPIStatsSnapshot struct {
	GraphQL githubGraphQLStatsSnapshot `json:"graphql"`
	REST    githubRESTStatsSnapshot    `json:"rest"`
}

type githubGraphQLStatsSnapshot struct {
	LastPollCalls              int `json:"last_poll_calls"`
	LastPollEstimatedPoints    int `json:"last_poll_estimated_points"`
	LastPollTerminalStateCalls int `json:"last_poll_terminal_state_calls"`
	LastPollReviewCalls        int `json:"last_poll_review_calls"`
	LastPollOtherCalls         int `json:"last_poll_other_calls"`
	TotalCalls                 int `json:"total_calls"`
	TotalEstimatedPoints       int `json:"total_estimated_points"`
}

type githubRESTStatsSnapshot struct {
	LastPollCalls       int `json:"last_poll_calls"`
	LastPollReviewCalls int `json:"last_poll_review_calls"`
	LastPollOtherCalls  int `json:"last_poll_other_calls"`
	TotalCalls          int `json:"total_calls"`
}

func withGitHubAPIConsumer(ctx context.Context, consumer string) context.Context {
	consumer = strings.TrimSpace(consumer)
	if consumer == "" {
		return ctx
	}
	return context.WithValue(ctx, githubAPIConsumerContextKey{}, consumer)
}

func gitHubAPIConsumerFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	consumer, _ := ctx.Value(githubAPIConsumerContextKey{}).(string)
	return strings.TrimSpace(consumer)
}

func classifyGitHubAPICall(ctx context.Context, args []string) githubAPICallMetrics {
	kind := gitHubAPIKind(args)
	if kind == "" {
		return githubAPICallMetrics{}
	}
	consumer := gitHubAPIConsumerFromContext(ctx)
	if consumer == "" {
		consumer = inferGitHubAPIConsumer(args)
	}
	if consumer == "" {
		consumer = githubAPIConsumerOther
	}

	estimatedPoints := 0
	if kind == githubAPIKindGraphQL {
		estimatedPoints = 1
	}
	return githubAPICallMetrics{
		kind:            kind,
		consumer:        consumer,
		estimatedPoints: estimatedPoints,
	}
}

func gitHubAPIKind(args []string) string {
	if len(args) == 0 {
		return ""
	}
	if args[0] == "api" {
		if len(args) > 1 && args[1] == "graphql" {
			return githubAPIKindGraphQL
		}
		return githubAPIKindREST
	}
	switch args[0] {
	case "issue", "pr":
		return githubAPIKindGraphQL
	default:
		return ""
	}
}

func inferGitHubAPIConsumer(args []string) string {
	joined := strings.Join(args, "\x00")
	switch {
	case strings.Contains(joined, prReviewJSONFields),
		strings.Contains(joined, prReviewThreadsGraphQLQuery):
		return githubAPIConsumerReview
	case strings.Contains(joined, prSnapshotJSONFields),
		strings.Contains(joined, "mergedAt merged state"):
		return githubAPIConsumerTerminalState
	default:
		return githubAPIConsumerOther
	}
}

func (c *githubAPICounters) add(call githubAPICallMetrics) {
	switch call.kind {
	case githubAPIKindGraphQL:
		c.GraphQLCalls++
		c.GraphQLEstimatedPoints += call.estimatedPoints
		switch call.consumer {
		case githubAPIConsumerTerminalState:
			c.GraphQLTerminalStateCalls++
		case githubAPIConsumerReview:
			c.GraphQLReviewCalls++
		default:
			c.GraphQLOtherCalls++
		}
	case githubAPIKindREST:
		c.RESTCalls++
		switch call.consumer {
		case githubAPIConsumerReview:
			c.RESTReviewCalls++
		default:
			c.RESTOtherCalls++
		}
	}
}

func (c *githubAPICounters) addCounters(other githubAPICounters) {
	c.GraphQLCalls += other.GraphQLCalls
	c.GraphQLEstimatedPoints += other.GraphQLEstimatedPoints
	c.GraphQLTerminalStateCalls += other.GraphQLTerminalStateCalls
	c.GraphQLReviewCalls += other.GraphQLReviewCalls
	c.GraphQLOtherCalls += other.GraphQLOtherCalls
	c.RESTCalls += other.RESTCalls
	c.RESTReviewCalls += other.RESTReviewCalls
	c.RESTOtherCalls += other.RESTOtherCalls
}

func (d *Daemon) recordPollGitHubAPIStats(counters githubAPICounters) {
	d.githubMetricsMu.Lock()
	defer d.githubMetricsMu.Unlock()
	d.lastPollGitHubAPI = counters
	d.totalGitHubAPI.addCounters(counters)
}

func (d *Daemon) githubAPIStatsSnapshot() githubAPIStatsSnapshot {
	d.githubMetricsMu.Lock()
	defer d.githubMetricsMu.Unlock()

	last := d.lastPollGitHubAPI
	total := d.totalGitHubAPI
	return githubAPIStatsSnapshot{
		GraphQL: githubGraphQLStatsSnapshot{
			LastPollCalls:              last.GraphQLCalls,
			LastPollEstimatedPoints:    last.GraphQLEstimatedPoints,
			LastPollTerminalStateCalls: last.GraphQLTerminalStateCalls,
			LastPollReviewCalls:        last.GraphQLReviewCalls,
			LastPollOtherCalls:         last.GraphQLOtherCalls,
			TotalCalls:                 total.GraphQLCalls,
			TotalEstimatedPoints:       total.GraphQLEstimatedPoints,
		},
		REST: githubRESTStatsSnapshot{
			LastPollCalls:       last.RESTCalls,
			LastPollReviewCalls: last.RESTReviewCalls,
			LastPollOtherCalls:  last.RESTOtherCalls,
			TotalCalls:          total.RESTCalls,
		},
	}
}

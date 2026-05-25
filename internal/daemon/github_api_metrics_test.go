package daemon

import (
	"context"
	"reflect"
	"testing"
)

func TestClassifyGitHubAPICall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ctx  context.Context
		args []string
		want githubAPICallMetrics
	}{
		{
			name: "empty args are not github API calls",
			ctx:  context.Background(),
			args: nil,
			want: githubAPICallMetrics{},
		},
		{
			name: "non gh api command is not github API call",
			ctx:  context.Background(),
			args: []string{"status", "--short"},
			want: githubAPICallMetrics{},
		},
		{
			name: "pr command infers graphql review consumer",
			ctx:  context.Background(),
			args: []string{"pr", "view", "42", "--json", prReviewJSONFields},
			want: githubAPICallMetrics{kind: githubAPIKindGraphQL, consumer: githubAPIConsumerReview, estimatedPoints: 1},
		},
		{
			name: "review thread query infers graphql review consumer",
			ctx:  context.Background(),
			args: []string{"api", "graphql", "-f", "query=" + prReviewThreadsGraphQLQuery},
			want: githubAPICallMetrics{kind: githubAPIKindGraphQL, consumer: githubAPIConsumerReview, estimatedPoints: 1},
		},
		{
			name: "snapshot fields infer graphql terminal state consumer",
			ctx:  context.Background(),
			args: []string{"pr", "view", "42", "--json", prSnapshotJSONFields},
			want: githubAPICallMetrics{kind: githubAPIKindGraphQL, consumer: githubAPIConsumerTerminalState, estimatedPoints: 1},
		},
		{
			name: "batch query fragment infers graphql terminal state consumer",
			ctx:  context.Background(),
			args: []string{"api", "graphql", "-f", "query=query{repository{pullRequest{number mergedAt merged state}}}"},
			want: githubAPICallMetrics{kind: githubAPIKindGraphQL, consumer: githubAPIConsumerTerminalState, estimatedPoints: 1},
		},
		{
			name: "api graphql command is graphql",
			ctx:  context.Background(),
			args: []string{"api", "graphql", "-f", "query=query{viewer{login}}"},
			want: githubAPICallMetrics{kind: githubAPIKindGraphQL, consumer: githubAPIConsumerOther, estimatedPoints: 1},
		},
		{
			name: "api endpoint command is rest",
			ctx:  context.Background(),
			args: []string{"api", "repos/{owner}/{repo}/pulls/42"},
			want: githubAPICallMetrics{kind: githubAPIKindREST, consumer: githubAPIConsumerOther},
		},
		{
			name: "context consumer overrides inference",
			ctx:  withGitHubAPIConsumer(context.Background(), githubAPIConsumerTerminalState),
			args: []string{"pr", "view", "42", "--json", prReviewJSONFields},
			want: githubAPICallMetrics{kind: githubAPIKindGraphQL, consumer: githubAPIConsumerTerminalState, estimatedPoints: 1},
		},
		{
			name: "blank context consumer is ignored",
			ctx:  withGitHubAPIConsumer(context.Background(), "   "),
			args: []string{"api", "repos/{owner}/{repo}/pulls/42/comments"},
			want: githubAPICallMetrics{kind: githubAPIKindREST, consumer: githubAPIConsumerOther},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := classifyGitHubAPICall(tt.ctx, tt.args); got != tt.want {
				t.Fatalf("classifyGitHubAPICall() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestGitHubAPICountersAndSnapshot(t *testing.T) {
	t.Parallel()

	var counters githubAPICounters
	counters.add(githubAPICallMetrics{kind: githubAPIKindGraphQL, consumer: githubAPIConsumerTerminalState, estimatedPoints: 1})
	counters.add(githubAPICallMetrics{kind: githubAPIKindGraphQL, consumer: githubAPIConsumerReview, estimatedPoints: 1})
	counters.add(githubAPICallMetrics{kind: githubAPIKindGraphQL, consumer: githubAPIConsumerOther, estimatedPoints: 1})
	counters.add(githubAPICallMetrics{kind: githubAPIKindREST, consumer: githubAPIConsumerReview})
	counters.add(githubAPICallMetrics{kind: githubAPIKindREST, consumer: githubAPIConsumerOther})
	counters.add(githubAPICallMetrics{kind: "unknown", consumer: githubAPIConsumerOther})

	wantCounters := githubAPICounters{
		GraphQLCalls:              3,
		GraphQLEstimatedPoints:    3,
		GraphQLTerminalStateCalls: 1,
		GraphQLReviewCalls:        1,
		GraphQLOtherCalls:         1,
		RESTCalls:                 2,
		RESTReviewCalls:           1,
		RESTOtherCalls:            1,
	}
	if !reflect.DeepEqual(counters, wantCounters) {
		t.Fatalf("githubAPICounters after add = %#v, want %#v", counters, wantCounters)
	}

	d := &Daemon{}
	d.recordPollGitHubAPIStats(counters)
	d.recordPollGitHubAPIStats(githubAPICounters{
		GraphQLCalls:           1,
		GraphQLEstimatedPoints: 1,
		GraphQLOtherCalls:      1,
		RESTCalls:              1,
		RESTOtherCalls:         1,
	})
	snapshot := d.githubAPIStatsSnapshot()

	if got, want := snapshot.GraphQL.LastPollCalls, 1; got != want {
		t.Fatalf("last graphql calls = %d, want %d", got, want)
	}
	if got, want := snapshot.GraphQL.LastPollOtherCalls, 1; got != want {
		t.Fatalf("last graphql other calls = %d, want %d", got, want)
	}
	if got, want := snapshot.GraphQL.TotalCalls, 4; got != want {
		t.Fatalf("total graphql calls = %d, want %d", got, want)
	}
	if got, want := snapshot.GraphQL.TotalEstimatedPoints, 4; got != want {
		t.Fatalf("total graphql estimated points = %d, want %d", got, want)
	}
	if got, want := snapshot.REST.LastPollCalls, 1; got != want {
		t.Fatalf("last rest calls = %d, want %d", got, want)
	}
	if got, want := snapshot.REST.LastPollOtherCalls, 1; got != want {
		t.Fatalf("last rest other calls = %d, want %d", got, want)
	}
	if got, want := snapshot.REST.TotalCalls, 3; got != want {
		t.Fatalf("total rest calls = %d, want %d", got, want)
	}
}

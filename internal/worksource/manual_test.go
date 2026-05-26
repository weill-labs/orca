package worksource

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestManualSourceReadyReturnsNoWork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		limit int
	}{
		{name: "zero limit", limit: 0},
		{name: "positive limit", limit: 5},
		{name: "negative limit", limit: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := ManualSource{}
			items, err := source.Ready(context.Background(), tt.limit)
			if err != nil {
				t.Fatalf("Ready() error = %v, want nil", err)
			}
			if items != nil {
				t.Fatalf("Ready() items = %#v, want nil", items)
			}
		})
	}
}

func TestManualSourceGetReturnsNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "empty id"},
		{name: "specific id", id: "LAB-1925"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := ManualSource{}
			item, err := source.Get(context.Background(), tt.id)
			if err == nil {
				t.Fatal("Get() error = nil, want not-found error")
			}
			if !reflect.DeepEqual(item, WorkItem{}) {
				t.Fatalf("Get() item = %#v, want zero WorkItem", item)
			}
		})
	}
}

func TestManualSourceMutationsAreNoOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(context.Context, ManualSource) error
	}{
		{
			name: "claim",
			run: func(ctx context.Context, source ManualSource) error {
				return source.Claim(ctx, "LAB-1925", "worker-1")
			},
		},
		{
			name: "release",
			run: func(ctx context.Context, source ManualSource) error {
				return source.Release(ctx, "LAB-1925", "operator reassigned")
			},
		},
		{
			name: "complete merged",
			run: func(ctx context.Context, source ManualSource) error {
				return source.Complete(ctx, "LAB-1925", OutcomeMerged)
			},
		},
		{
			name: "complete abandoned",
			run: func(ctx context.Context, source ManualSource) error {
				return source.Complete(ctx, "LAB-1925", OutcomeAbandoned)
			},
		},
		{
			name: "complete failed",
			run: func(ctx context.Context, source ManualSource) error {
				return source.Complete(ctx, "LAB-1925", OutcomeFailed)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := ManualSource{}
			if err := tt.run(context.Background(), source); err != nil {
				t.Fatalf("%s error = %v, want nil", tt.name, err)
			}
		})
	}
}

func TestManualSourcePublicAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "manual source implements source",
			run: func(t *testing.T) {
				var _ Source = ManualSource{}
			},
		},
		{
			name: "work item preserves all fields",
			run: func(t *testing.T) {
				item := WorkItem{
					ID:        "LAB-1925",
					Title:     "Phase 1 worksource package",
					Body:      "Define the shared worksource interface.",
					Priority:  2,
					Score:     9.5,
					Rationale: "Unblocks source adapters.",
					Labels:    []string{"orca", "worksource"},
					AgentHint: "codex",
					Meta:      map[string]string{"linear_id": "LAB-1925"},
				}

				if item.ID != "LAB-1925" ||
					item.Title != "Phase 1 worksource package" ||
					item.Body != "Define the shared worksource interface." ||
					item.Priority != 2 ||
					item.Score != 9.5 ||
					item.Rationale != "Unblocks source adapters." ||
					!reflect.DeepEqual(item.Labels, []string{"orca", "worksource"}) ||
					item.AgentHint != "codex" ||
					!reflect.DeepEqual(item.Meta, map[string]string{"linear_id": "LAB-1925"}) {
					t.Fatalf("WorkItem fields were not preserved: %#v", item)
				}
			},
		},
		{
			name: "already claimed sentinel",
			run: func(t *testing.T) {
				if !errors.Is(ErrAlreadyClaimed, ErrAlreadyClaimed) {
					t.Fatal("ErrAlreadyClaimed should match itself")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}

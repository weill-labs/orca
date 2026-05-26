package worksource

import (
	"context"
	"errors"
	"fmt"
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
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get() error = %v, want ErrNotFound", err)
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
			name: "complete unknown",
			run: func(ctx context.Context, source ManualSource) error {
				return source.Complete(ctx, "LAB-1925", OutcomeUnknown)
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
			name: "already claimed sentinel",
			run: func(t *testing.T) {
				if ErrAlreadyClaimed == nil {
					t.Fatal("ErrAlreadyClaimed = nil, want sentinel error")
				}

				err := fmt.Errorf("wrapped: %w", ErrAlreadyClaimed)
				if !errors.Is(err, ErrAlreadyClaimed) {
					t.Fatal("wrapped ErrAlreadyClaimed did not match sentinel")
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

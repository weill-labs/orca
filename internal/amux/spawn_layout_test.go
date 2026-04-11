package amux

import (
	"reflect"
	"testing"
)

func TestSpawnPlacementArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		leadPane string
		want     []string
	}{
		{
			name:     "targets explicit lead pane with auto layout",
			leadPane: "lead-pane",
			want:     []string{"--auto", "--at", "lead-pane"},
		},
		{
			name:     "trims lead pane before targeting it",
			leadPane: "  lead-pane  ",
			want:     []string{"--auto", "--at", "lead-pane"},
		},
		{
			name:     "falls back to root with auto layout",
			leadPane: " \t ",
			want:     []string{"--auto", "--root"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := spawnPlacementArgs(tt.leadPane)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("spawnPlacementArgs(%q) = %#v, want %#v", tt.leadPane, got, tt.want)
			}
		})
	}
}

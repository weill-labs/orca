package amux

import (
	"reflect"
	"testing"
)

func TestSpawnPlacementArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		window string
		want   []string
	}{
		{
			name:   "auto with window target",
			window: "alphaos",
			want:   []string{"--auto", "--window", "alphaos"},
		},
		{
			name:   "trims window name",
			window: "  alphaos  ",
			want:   []string{"--auto", "--window", "alphaos"},
		},
		{
			name:   "auto without window",
			window: "",
			want:   []string{"--auto"},
		},
		{
			name:   "whitespace-only window falls back to auto",
			window: " \t ",
			want:   []string{"--auto"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := spawnPlacementArgs(tt.window)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("spawnPlacementArgs(%q) = %#v, want %#v", tt.window, got, tt.want)
			}
		})
	}
}

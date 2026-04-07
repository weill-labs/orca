package amux

import "testing"

func TestPaneRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pane Pane
		want string
	}{
		{name: "prefers pane name", pane: Pane{ID: "7", Name: "worker-LAB-854"}, want: "worker-LAB-854"},
		{name: "falls back to pane id", pane: Pane{ID: "7", Name: "   "}, want: "7"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.pane.Ref(); got != tt.want {
				t.Fatalf("Pane.Ref() = %q, want %q", got, tt.want)
			}
		})
	}
}

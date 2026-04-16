package daemon

import (
	"context"
	"errors"
	"testing"
)

func TestSpawnPaneTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		task      Task
		listPanes []Pane
		listErr   error
		want      string
	}{
		{
			name: "returns empty string when caller pane is empty",
			want: "",
		},
		{
			name: "keeps caller pane when it is not a lead pane",
			task: Task{CallerPane: "worker-07"},
			listPanes: []Pane{
				{ID: "155", Name: "worker-07", Window: "orca"},
				{ID: "2", Name: "pane-2", Window: "orca", Lead: true},
			},
			want: "worker-07",
		},
		{
			name: "uses a non-lead pane in the caller window when caller is lead",
			task: Task{CallerPane: "pane-2"},
			listPanes: []Pane{
				{ID: "2", Name: "pane-2", Window: "orca", Lead: true},
				{ID: "155", Name: "worker-07", Window: "orca"},
				{ID: "13", Name: "pane-13", Window: "alphaos", Lead: true},
			},
			want: "worker-07",
		},
		{
			name:    "keeps caller pane when amux list fails",
			task:    Task{CallerPane: "pane-2"},
			listErr: errors.New("amux unavailable"),
			want:    "pane-2",
		},
		{
			name: "keeps caller pane when no non-lead pane shares the window",
			task: Task{CallerPane: "pane-2"},
			listPanes: []Pane{
				{ID: "2", Name: "pane-2", Window: "orca", Lead: true},
				{ID: "13", Name: "pane-13", Window: "alphaos", Lead: true},
			},
			want: "pane-2",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.amux.listPanes = append([]Pane(nil), tt.listPanes...)
			deps.amux.listPanesErr = tt.listErr

			d := deps.newDaemon(t)

			if got := d.spawnPaneTarget(context.Background(), tt.task); got != tt.want {
				t.Fatalf("spawnPaneTarget() = %q, want %q", got, tt.want)
			}
		})
	}
}

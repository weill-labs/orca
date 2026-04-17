package daemon

import (
	"context"
	"errors"
	"strings"
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

func TestSpawnWindowTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		task      Task
		listPanes []Pane
		listErr   error
		want      string
		wantLog   string
	}{
		{
			name: "uses caller pane window when caller pane is valid",
			task: Task{
				Project:    "/home/cweill/github/weill-labs/amux",
				CallerPane: "pane-13",
			},
			listPanes: []Pane{
				{ID: "13", Name: "pane-13", Window: "orca"},
			},
			want: "orca",
		},
		{
			name: "uses project window when caller pane is empty",
			task: Task{
				Project: "/home/cweill/github/weill-labs/amux",
			},
			listPanes: []Pane{
				{ID: "13", Name: "pane-13", Window: "amux"},
			},
			want: "amux",
		},
		{
			name: "uses project window when caller pane is invalid",
			task: Task{
				Project:    "/home/cweill/github/weill-labs/amux",
				CallerPane: "missing-pane",
			},
			listPanes: []Pane{
				{ID: "13", Name: "pane-13", Window: "amux"},
			},
			want: "amux",
		},
		{
			name: "logs warning and falls back when project window is missing",
			task: Task{
				Project: "/home/cweill/github/weill-labs/amux",
			},
			listPanes: []Pane{
				{ID: "13", Name: "pane-13", Window: "orca"},
			},
			want:    "",
			wantLog: `worker spawn project window "amux"`,
		},
		{
			name: "falls back silently when amux listing fails",
			task: Task{
				Project: "/home/cweill/github/weill-labs/amux",
			},
			listErr: errors.New("amux unavailable"),
			want:    "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.amux.listPanes = append([]Pane(nil), tt.listPanes...)
			deps.amux.listPanesErr = tt.listErr
			logs := &fakeLogSink{}

			d := deps.newDaemonWithOptions(t, func(opts *Options) {
				opts.Logf = logs.Printf
			})

			if got := d.spawnWindowTarget(context.Background(), tt.task); got != tt.want {
				t.Fatalf("spawnWindowTarget() = %q, want %q", got, tt.want)
			}

			messages := logs.messages()
			if tt.wantLog == "" {
				if len(messages) != 0 {
					t.Fatalf("warning logs = %#v, want none", messages)
				}
				return
			}

			if got := len(messages); got != 1 {
				t.Fatalf("warning logs = %#v, want 1 entry", messages)
			}
			if !strings.Contains(messages[0], tt.wantLog) {
				t.Fatalf("warning log = %q, want substring %q", messages[0], tt.wantLog)
			}
		})
	}
}

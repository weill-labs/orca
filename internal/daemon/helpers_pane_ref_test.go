package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestWorkerPaneRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		task   Task
		worker Worker
		want   string
	}{
		{
			name: "prefers task pane name",
			task: Task{PaneName: "worker-LAB-854", PaneID: "7", Issue: "LAB-854"},
			want: "worker-LAB-854",
		},
		{
			name:   "falls back to worker pane name",
			task:   Task{PaneID: "7", Issue: "LAB-854"},
			worker: Worker{PaneName: "worker-LAB-854", PaneID: "8"},
			want:   "worker-LAB-854",
		},
		{
			name: "falls back to stable task pane id",
			task: Task{PaneID: "worker-LAB-854", Issue: "LAB-854"},
			want: "worker-LAB-854",
		},
		{
			name:   "falls back to stable worker pane id",
			task:   Task{Issue: "LAB-854"},
			worker: Worker{PaneID: "worker-LAB-854"},
			want:   "worker-LAB-854",
		},
		{
			name:   "guesses worker name from issue when refs are numeric",
			task:   Task{PaneID: "7", Issue: "LAB-854"},
			worker: Worker{PaneID: "8"},
			want:   "worker-LAB-854",
		},
		{
			name:   "falls back to numeric task id when issue is empty",
			task:   Task{PaneID: "7"},
			worker: Worker{PaneID: "8"},
			want:   "7",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := workerPaneRef(tt.task, tt.worker); got != tt.want {
				t.Fatalf("workerPaneRef() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeStoredPaneRef(t *testing.T) {
	t.Run("migrates legacy refs and deletes duplicates", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		d := deps.newDaemon(t)
		ctx := context.Background()
		now := deps.clock.Now()

		task := Task{
			Project:   "/tmp/project",
			Issue:     "LAB-854",
			PaneID:    "7",
			PaneName:  "7",
			UpdatedAt: now,
		}
		worker := Worker{
			Project:   "/tmp/project",
			Issue:     "LAB-854",
			PaneID:    "8",
			PaneName:  "worker-LAB-854",
			UpdatedAt: now,
		}
		deps.state.putTaskForTest(task)
		if err := deps.state.PutWorker(ctx, worker); err != nil {
			t.Fatalf("PutWorker() error = %v", err)
		}
		if err := deps.state.PutWorker(ctx, Worker{
			Project:   "/tmp/project",
			Issue:     "LAB-854",
			PaneID:    "7",
			PaneName:  "7",
			UpdatedAt: now,
		}); err != nil {
			t.Fatalf("PutWorker() stale error = %v", err)
		}

		if err := d.normalizeStoredPaneRef(ctx, &task, &worker); err != nil {
			t.Fatalf("normalizeStoredPaneRef() error = %v", err)
		}

		if got, want := task.PaneID, "worker-LAB-854"; got != want {
			t.Fatalf("task.PaneID = %q, want %q", got, want)
		}
		if got, want := worker.PaneID, "worker-LAB-854"; got != want {
			t.Fatalf("worker.PaneID = %q, want %q", got, want)
		}

		storedTask, ok := deps.state.task("LAB-854")
		if !ok {
			t.Fatal("task missing after normalization")
		}
		if got, want := storedTask.PaneID, "worker-LAB-854"; got != want {
			t.Fatalf("storedTask.PaneID = %q, want %q", got, want)
		}
		if _, ok := deps.state.worker("7"); ok {
			t.Fatal("stale task pane worker ref retained")
		}
		if _, ok := deps.state.worker("8"); ok {
			t.Fatal("stale worker pane ref retained")
		}
		if _, ok := deps.state.worker("worker-LAB-854"); !ok {
			t.Fatal("stable worker ref missing after normalization")
		}
	})

	t.Run("skips stable non numeric refs", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		d := deps.newDaemon(t)
		now := deps.clock.Now()
		task := Task{
			Project:   "/tmp/project",
			Issue:     "LAB-854",
			PaneID:    "worker-LAB-854",
			PaneName:  "",
			UpdatedAt: now,
		}
		worker := Worker{
			Project:   "/tmp/project",
			Issue:     "LAB-854",
			PaneID:    "worker-LAB-854",
			PaneName:  "",
			UpdatedAt: now,
		}

		if err := d.normalizeStoredPaneRef(context.Background(), &task, &worker); err != nil {
			t.Fatalf("normalizeStoredPaneRef() error = %v", err)
		}
		if got, want := task.PaneID, "worker-LAB-854"; got != want {
			t.Fatalf("task.PaneID = %q, want %q", got, want)
		}
		if got, want := task.PaneName, ""; got != want {
			t.Fatalf("task.PaneName = %q, want %q", got, want)
		}
	})

	t.Run("returns store errors", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			state  *resumeStateStub
			task   Task
			worker *Worker
			want   string
		}{
			{
				name:  "task update failure",
				state: &resumeStateStub{fakeState: newFakeState(), putTaskErr: errors.New("put task failed")},
				task:  Task{Project: "/tmp/project", Issue: "LAB-854", PaneID: "7", PaneName: "7"},
				want:  "normalize task pane ref: put task failed",
			},
			{
				name:  "worker update failure",
				state: &resumeStateStub{fakeState: newFakeState(), putWorkerErr: errors.New("put worker failed")},
				task:  Task{Project: "/tmp/project", Issue: "LAB-854", PaneID: "7", PaneName: "7"},
				worker: &Worker{
					Project:  "/tmp/project",
					Issue:    "LAB-854",
					PaneID:   "8",
					PaneName: "worker-LAB-854",
				},
				want: "normalize worker pane ref: put worker failed",
			},
			{
				name:  "legacy worker delete failure",
				state: &resumeStateStub{fakeState: newFakeState(), deleteWorkerErr: errors.New("delete worker failed")},
				task:  Task{Project: "/tmp/project", Issue: "LAB-854", PaneID: "7", PaneName: "7"},
				worker: &Worker{
					Project:  "/tmp/project",
					Issue:    "LAB-854",
					PaneID:   "8",
					PaneName: "worker-LAB-854",
				},
				want: "delete legacy worker pane ref 8: delete worker failed",
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				deps := newTestDeps(t)
				d := newResumeCoverageDaemon(t, deps, tt.state, deps.amux)

				task := tt.task
				worker := tt.worker
				err := d.normalizeStoredPaneRef(context.Background(), &task, worker)
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("normalizeStoredPaneRef() error = %v, want substring %q", err, tt.want)
				}
			})
		}
	})
}

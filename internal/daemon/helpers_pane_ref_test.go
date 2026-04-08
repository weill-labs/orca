package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestStableWorkerRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		task   Task
		worker Worker
		want   string
	}{
		{
			name: "prefers task worker id",
			task: Task{Issue: "LAB-854", WorkerID: "worker-01", PaneName: "w-LAB-854"},
			want: "worker-01",
		},
		{
			name:   "falls back to worker worker id",
			task:   Task{Issue: "LAB-854", PaneName: "w-LAB-854"},
			worker: Worker{Issue: "LAB-854", WorkerID: "worker-01", PaneName: "w-LAB-854"},
			want:   "worker-01",
		},
		{
			name: "ignores canonical issue pane name without stable id",
			task: Task{Issue: "LAB-854", PaneName: "w-LAB-854"},
			want: "",
		},
		{
			name: "ignores legacy issue pane name without stable id",
			task: Task{Issue: "LAB-854", PaneName: "worker-LAB-854"},
			want: "",
		},
		{
			name: "keeps stable pane style fallback when it is not an issue pane name",
			task: Task{Issue: "LAB-854", PaneName: "worker-01"},
			want: "worker-01",
		},
		{
			name:   "falls back to stable worker pane name",
			task:   Task{Issue: "LAB-854"},
			worker: Worker{PaneName: "worker-01"},
			want:   "worker-01",
		},
		{
			name:   "returns empty when only numeric pane refs remain",
			task:   Task{PaneID: "7", Issue: "LAB-854"},
			worker: Worker{PaneID: "8"},
			want:   "",
		},
		{
			name:   "returns empty when no stable worker ref exists",
			task:   Task{PaneID: "7"},
			worker: Worker{PaneID: "8"},
			want:   "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := stableWorkerRef(tt.task, tt.worker); got != tt.want {
				t.Fatalf("stableWorkerRef() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeStoredPaneRef(t *testing.T) {
	t.Run("copies stable worker id and canonical pane name onto task and worker refs", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		d := deps.newDaemon(t)
		ctx := context.Background()
		now := deps.clock.Now()

		task := Task{
			Project:   "/tmp/project",
			Issue:     "LAB-854",
			WorkerID:  "",
			PaneID:    "7",
			PaneName:  "7",
			UpdatedAt: now,
		}
		worker := Worker{
			Project:   "/tmp/project",
			WorkerID:  "worker-01",
			Issue:     "LAB-854",
			PaneID:    "8",
			PaneName:  "",
			UpdatedAt: now,
		}
		deps.state.putTaskForTest(task)
		if err := deps.state.PutWorker(ctx, worker); err != nil {
			t.Fatalf("PutWorker() error = %v", err)
		}

		if err := d.normalizeStoredPaneRef(ctx, &task, &worker); err != nil {
			t.Fatalf("normalizeStoredPaneRef() error = %v", err)
		}

		if got, want := task.WorkerID, "worker-01"; got != want {
			t.Fatalf("task.WorkerID = %q, want %q", got, want)
		}
		if got, want := task.PaneName, "w-LAB-854"; got != want {
			t.Fatalf("task.PaneName = %q, want %q", got, want)
		}
		if got, want := worker.WorkerID, "worker-01"; got != want {
			t.Fatalf("worker.WorkerID = %q, want %q", got, want)
		}
		if got, want := worker.PaneName, "w-LAB-854"; got != want {
			t.Fatalf("worker.PaneName = %q, want %q", got, want)
		}

		storedTask, ok := deps.state.task("LAB-854")
		if !ok {
			t.Fatal("task missing after normalization")
		}
		if got, want := storedTask.WorkerID, "worker-01"; got != want {
			t.Fatalf("storedTask.WorkerID = %q, want %q", got, want)
		}
		if got, want := storedTask.PaneName, "w-LAB-854"; got != want {
			t.Fatalf("storedTask.PaneName = %q, want %q", got, want)
		}
		if _, ok := deps.state.worker("worker-01"); !ok {
			t.Fatal("stable worker ref missing after normalization")
		}
	})

	t.Run("fills missing pane names from issue and stable worker id", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		d := deps.newDaemon(t)
		now := deps.clock.Now()
		task := Task{
			Project:   "/tmp/project",
			Issue:     "LAB-854",
			WorkerID:  "worker-01",
			PaneID:    "pane-1",
			PaneName:  "",
			UpdatedAt: now,
		}
		worker := Worker{
			Project:   "/tmp/project",
			WorkerID:  "worker-01",
			Issue:     "LAB-854",
			PaneID:    "pane-1",
			PaneName:  "",
			UpdatedAt: now,
		}

		if err := d.normalizeStoredPaneRef(context.Background(), &task, &worker); err != nil {
			t.Fatalf("normalizeStoredPaneRef() error = %v", err)
		}
		if got, want := task.PaneID, "pane-1"; got != want {
			t.Fatalf("task.PaneID = %q, want %q", got, want)
		}
		if got, want := task.PaneName, "w-LAB-854"; got != want {
			t.Fatalf("task.PaneName = %q, want %q", got, want)
		}
		if got, want := worker.PaneName, "w-LAB-854"; got != want {
			t.Fatalf("worker.PaneName = %q, want %q", got, want)
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
				task:  Task{Project: "/tmp/project", Issue: "LAB-854", WorkerID: "worker-01", PaneID: "7", PaneName: ""},
				want:  "normalize task pane ref: put task failed",
			},
			{
				name:  "worker update failure",
				state: &resumeStateStub{fakeState: newFakeState(), putWorkerErr: errors.New("put worker failed")},
				task:  Task{Project: "/tmp/project", Issue: "LAB-854", WorkerID: "worker-01", PaneID: "7", PaneName: "worker-01"},
				worker: &Worker{
					Project:  "/tmp/project",
					WorkerID: "worker-01",
					Issue:    "LAB-854",
					PaneID:   "8",
					PaneName: "",
				},
				want: "normalize worker pane ref: put worker failed",
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

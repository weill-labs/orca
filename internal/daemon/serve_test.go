package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStartDaemonLifecycleReportsRunningAfterStart(t *testing.T) {
	t.Parallel()

	calls := make([]string, 0, 2)
	instance := fakeDaemonLifecycle{
		start: func(context.Context) error {
			calls = append(calls, "start")
			return nil
		},
		stop: func(context.Context) error {
			calls = append(calls, "stop")
			return nil
		},
	}
	statusWriter := fakeOrderedDaemonStatusWriter{
		update: func(_ context.Context, status string, heartbeatAt time.Time) error {
			calls = append(calls, "status:"+status)
			if heartbeatAt.IsZero() {
				t.Fatal("heartbeatAt = zero, want startup timestamp")
			}
			return nil
		},
	}

	err := startDaemonLifecycle(context.Background(), instance, statusWriter, time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC), func(context.Context, time.Time) error {
		calls = append(calls, "mark-stopped")
		return nil
	})
	if err != nil {
		t.Fatalf("startDaemonLifecycle() error = %v", err)
	}

	if got, want := calls, []string{"start", "status:running"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("call order = %#v, want %#v", got, want)
	}
}

func TestStartDaemonLifecycleMarksStoppedWhenStartFails(t *testing.T) {
	t.Parallel()

	calls := make([]string, 0, 2)
	wantErr := errors.New("start failed")
	instance := fakeDaemonLifecycle{
		start: func(context.Context) error {
			calls = append(calls, "start")
			return wantErr
		},
		stop: func(context.Context) error {
			calls = append(calls, "stop")
			return nil
		},
	}
	statusWriter := fakeOrderedDaemonStatusWriter{
		update: func(context.Context, string, time.Time) error {
			calls = append(calls, "status")
			return nil
		},
	}

	err := startDaemonLifecycle(context.Background(), instance, statusWriter, time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC), func(context.Context, time.Time) error {
		calls = append(calls, "mark-stopped")
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("startDaemonLifecycle() error = %v, want %v", err, wantErr)
	}

	if got, want := calls, []string{"start", "mark-stopped"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("call order = %#v, want %#v", got, want)
	}
}

func TestStartDaemonLifecycleStopsAndMarksStoppedWhenStatusUpdateFails(t *testing.T) {
	t.Parallel()

	calls := make([]string, 0, 4)
	wantErr := errors.New("write status")
	instance := fakeDaemonLifecycle{
		start: func(context.Context) error {
			calls = append(calls, "start")
			return nil
		},
		stop: func(context.Context) error {
			calls = append(calls, "stop")
			return nil
		},
	}
	statusWriter := fakeOrderedDaemonStatusWriter{
		update: func(context.Context, string, time.Time) error {
			calls = append(calls, "status")
			return wantErr
		},
	}

	err := startDaemonLifecycle(context.Background(), instance, statusWriter, time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC), func(context.Context, time.Time) error {
		calls = append(calls, "mark-stopped")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("startDaemonLifecycle() error = %v, want message containing %q", err, wantErr)
	}

	if got, want := calls, []string{"start", "status", "stop", "mark-stopped"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("call order = %#v, want %#v", got, want)
	}
}

type fakeDaemonLifecycle struct {
	start func(context.Context) error
	stop  func(context.Context) error
}

func (f fakeDaemonLifecycle) Start(ctx context.Context) error {
	if f.start == nil {
		return nil
	}
	return f.start(ctx)
}

func (f fakeDaemonLifecycle) Stop(ctx context.Context) error {
	if f.stop == nil {
		return nil
	}
	return f.stop(ctx)
}

type fakeOrderedDaemonStatusWriter struct {
	update func(context.Context, string, time.Time) error
}

func (f fakeOrderedDaemonStatusWriter) Update(ctx context.Context, status string, heartbeatAt time.Time) error {
	if f.update == nil {
		return nil
	}
	return f.update(ctx, status, heartbeatAt)
}

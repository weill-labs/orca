package amux

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type fakeEventStarter struct {
	processes []*fakeEventProcess
	calls     []recordedCommand
	startErr  error
}

type fakeEventProcess struct {
	output  string
	waitErr error
	reader  io.ReadCloser
}

func (s *fakeEventStarter) Start(_ context.Context, name string, args []string) (eventProcess, error) {
	s.calls = append(s.calls, recordedCommand{
		name: name,
		args: append([]string(nil), args...),
	})
	if s.startErr != nil {
		return nil, s.startErr
	}
	if len(s.processes) == 0 {
		return &fakeEventProcess{}, nil
	}
	process := s.processes[0]
	s.processes = s.processes[1:]
	return process, nil
}

func (p *fakeEventProcess) Reader() io.ReadCloser {
	if p.reader == nil {
		p.reader = io.NopCloser(strings.NewReader(p.output))
	}
	return p.reader
}

func (p *fakeEventProcess) Wait() error {
	return p.waitErr
}

func TestCLIClientEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		processes []*fakeEventProcess
		wantCmd   recordedCommand
		wantEvent Event
		wantErr   string
	}{
		{
			name: "streams exited events as ndjson",
			processes: []*fakeEventProcess{
				{
					output: strings.Join([]string{
						`{"type":"exited","ts":"2026-04-09T12:00:00Z","pane_id":7,"pane_name":"pane-7"}`,
						`{"type":"exited","ts":"2026-04-09T12:00:01Z","pane_id":8,"pane_name":"pane-8"}`,
						"",
					}, "\n"),
				},
			},
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{"-s", "orca-dev", "events", "--filter", "exited", "--no-reconnect"},
			},
			wantEvent: Event{
				Type:      "exited",
				Timestamp: "2026-04-09T12:00:00Z",
				PaneID:    7,
				PaneName:  "pane-7",
			},
		},
		{
			name: "surfaces process exit errors",
			processes: []*fakeEventProcess{
				{
					waitErr: errors.New("stream dropped"),
				},
			},
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{"-s", "orca-dev", "events", "--filter", "exited", "--no-reconnect"},
			},
			wantErr: "stream dropped",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			starter := &fakeEventStarter{processes: tt.processes}
			client := &CLIClient{
				binary:       "amux",
				session:      "orca-dev",
				eventStarter: starter,
			}

			eventsCh, errCh := client.Events(ctx, EventsRequest{
				Filter:      []string{"exited"},
				NoReconnect: true,
			})

			if tt.wantErr == "" {
				select {
				case event, ok := <-eventsCh:
					if !ok {
						t.Fatal("eventsCh closed before first event")
					}
					if event != tt.wantEvent {
						t.Fatalf("first event = %#v, want %#v", event, tt.wantEvent)
					}
					cancel()
					for range eventsCh {
					}
				case err := <-errCh:
					t.Fatalf("unexpected err = %v", err)
				case <-time.After(2 * time.Second):
					t.Fatal("timed out waiting for first event")
				}
			} else {
				select {
				case err := <-errCh:
					if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
						t.Fatalf("err = %v, want substring %q", err, tt.wantErr)
					}
				case event := <-eventsCh:
					t.Fatalf("unexpected event = %#v", event)
				case <-time.After(2 * time.Second):
					t.Fatal("timed out waiting for stream error")
				}
			}

			if len(starter.calls) != 1 {
				t.Fatalf("event starter calls = %d, want 1", len(starter.calls))
			}
			if got := starter.calls[0]; got.name != tt.wantCmd.name || !strings.EqualFold(strings.Join(got.args, "\x00"), strings.Join(tt.wantCmd.args, "\x00")) {
				t.Fatalf("Events() command = %#v, want %#v", got, tt.wantCmd)
			}
		})
	}
}

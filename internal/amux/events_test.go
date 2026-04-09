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

type errReadCloser struct {
	err error
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

func (r errReadCloser) Read(_ []byte) (int, error) {
	return 0, r.err
}

func (r errReadCloser) Close() error {
	return nil
}

func TestCLIClientEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		processes []*fakeEventProcess
		startErr  error
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
		{
			name:     "surfaces process start errors",
			startErr: errors.New("start failed"),
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{"-s", "orca-dev", "events", "--filter", "exited", "--no-reconnect"},
			},
			wantErr: "start failed",
		},
		{
			name: "surfaces json parse errors",
			processes: []*fakeEventProcess{
				{
					output: "{not-json}\n",
				},
			},
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{"-s", "orca-dev", "events", "--filter", "exited", "--no-reconnect"},
			},
			wantErr: "parse amux event",
		},
		{
			name: "surfaces reader errors",
			processes: []*fakeEventProcess{
				{
					reader: errReadCloser{err: errors.New("read failed")},
				},
			},
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{"-s", "orca-dev", "events", "--filter", "exited", "--no-reconnect"},
			},
			wantErr: "read amux events: read failed",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			starter := &fakeEventStarter{processes: tt.processes, startErr: tt.startErr}
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
				deadline := time.After(2 * time.Second)
				for {
					select {
					case err, ok := <-errCh:
						if !ok {
							t.Fatal("errCh closed before surfacing stream error")
						}
						if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
							t.Fatalf("err = %v, want substring %q", err, tt.wantErr)
						}
						goto assertions
					case event, ok := <-eventsCh:
						if !ok {
							continue
						}
						t.Fatalf("unexpected event = %#v", event)
					case <-deadline:
						t.Fatal("timed out waiting for stream error")
					}
				}
			}

		assertions:
			if len(starter.calls) != 1 {
				t.Fatalf("event starter calls = %d, want 1", len(starter.calls))
			}
			if got := starter.calls[0]; got.name != tt.wantCmd.name || !strings.EqualFold(strings.Join(got.args, "\x00"), strings.Join(tt.wantCmd.args, "\x00")) {
				t.Fatalf("Events() command = %#v, want %#v", got, tt.wantCmd)
			}
		})
	}
}

func TestEventPaneRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		event Event
		want  string
	}{
		{
			name:  "prefers pane name",
			event: Event{PaneID: 7, PaneName: " worker-7 "},
			want:  "worker-7",
		},
		{
			name:  "falls back to pane id",
			event: Event{PaneID: 7},
			want:  "7",
		},
		{
			name:  "returns empty when event has no pane reference",
			event: Event{},
			want:  "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.event.PaneRef(); got != tt.want {
				t.Fatalf("PaneRef() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEventArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  EventsRequest
		want []string
	}{
		{
			name: "builds pane filter and reconnect flags",
			req: EventsRequest{
				Pane:        " pane-1 ",
				Filter:      []string{"exited", "focus"},
				NoReconnect: true,
			},
			want: []string{"--pane", "pane-1", "--filter", "exited,focus", "--no-reconnect"},
		},
		{
			name: "omits empty options",
			req:  EventsRequest{},
			want: []string{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := eventArgs(tt.req)
			if strings.Join(got, "\x00") != strings.Join(tt.want, "\x00") {
				t.Fatalf("eventArgs() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExecEventStarterStartsProcess(t *testing.T) {
	t.Parallel()

	process, err := (execEventStarter{}).Start(context.Background(), "/bin/sh", []string{"-lc", "printf 'hello\\n'"})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	output, err := io.ReadAll(process.Reader())
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got, want := string(output), "hello\n"; got != want {
		t.Fatalf("process output = %q, want %q", got, want)
	}
	if err := process.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}

func TestExecEventProcessWaitReportsCommandError(t *testing.T) {
	t.Parallel()

	process, err := (execEventStarter{}).Start(context.Background(), "/bin/sh", []string{"-lc", "echo boom >&2; exit 7"})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if _, err := io.ReadAll(process.Reader()); err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	err = process.Wait()
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Wait() error = %v, want stderr in command error", err)
	}
}

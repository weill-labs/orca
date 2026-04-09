package amux

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
)

type Event struct {
	Type       string `json:"type"`
	Timestamp  string `json:"ts"`
	Generation uint64 `json:"generation,omitempty"`
	PaneID     uint32 `json:"pane_id,omitempty"`
	PaneName   string `json:"pane_name,omitempty"`
	Host       string `json:"host,omitempty"`
	ActivePane string `json:"active_pane,omitempty"`
	ClientID   string `json:"client_id,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

func (e Event) PaneRef() string {
	if name := strings.TrimSpace(e.PaneName); name != "" {
		return name
	}
	if e.PaneID == 0 {
		return ""
	}
	return strconv.FormatUint(uint64(e.PaneID), 10)
}

type EventsRequest struct {
	Pane        string
	Filter      []string
	NoReconnect bool
}

type eventProcess interface {
	Reader() io.ReadCloser
	Wait() error
}

type eventStarter interface {
	Start(ctx context.Context, name string, args []string) (eventProcess, error)
}

type execEventStarter struct{}

type execEventProcess struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	stderr bytes.Buffer
	name   string
	args   []string
}

func (execEventStarter) Start(ctx context.Context, name string, args []string) (eventProcess, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	process := &execEventProcess{
		cmd:    cmd,
		stdout: stdout,
		name:   name,
		args:   append([]string(nil), args...),
	}
	cmd.Stderr = &process.stderr
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		return nil, err
	}
	return process, nil
}

func (p *execEventProcess) Reader() io.ReadCloser {
	return p.stdout
}

func (p *execEventProcess) Wait() error {
	if err := p.cmd.Wait(); err != nil {
		return commandError(p.name, p.args, p.stderr.Bytes(), err)
	}
	return nil
}

func (c *CLIClient) Events(ctx context.Context, req EventsRequest) (<-chan Event, <-chan error) {
	eventsCh := make(chan Event)
	errCh := make(chan error, 1)

	args := c.commandArgs(c.session, "events", eventArgs(req)...)
	starter := c.eventStarter
	if starter == nil {
		starter = execEventStarter{}
	}

	streamCtx, cancel := context.WithCancel(ctx)
	process, err := starter.Start(streamCtx, c.binary, args)
	if err != nil {
		cancel()
		errCh <- commandError(c.binary, args, nil, err)
		close(eventsCh)
		close(errCh)
		return eventsCh, errCh
	}

	go func() {
		defer cancel()
		defer close(eventsCh)
		defer close(errCh)
		reader := process.Reader()
		defer reader.Close()

		scanner := bufio.NewScanner(reader)
		var streamErr error
		for scanner.Scan() {
			var event Event
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				streamErr = fmt.Errorf("parse amux event: %w", err)
				cancel()
				break
			}

			select {
			case <-ctx.Done():
				cancel()
				_ = process.Wait()
				return
			case eventsCh <- event:
			}
		}

		if streamErr == nil {
			if err := scanner.Err(); err != nil && ctx.Err() == nil {
				streamErr = fmt.Errorf("read amux events: %w", err)
				cancel()
			}
		}

		waitErr := process.Wait()
		if ctx.Err() != nil {
			return
		}
		if streamErr != nil {
			errCh <- streamErr
			return
		}
		if waitErr != nil {
			errCh <- waitErr
		}
	}()

	return eventsCh, errCh
}

func eventArgs(req EventsRequest) []string {
	args := make([]string, 0, 5)
	if pane := strings.TrimSpace(req.Pane); pane != "" {
		args = append(args, "--pane", pane)
	}
	if len(req.Filter) > 0 {
		args = append(args, "--filter", strings.Join(req.Filter, ","))
	}
	if req.NoReconnect {
		args = append(args, "--no-reconnect")
	}
	return args
}

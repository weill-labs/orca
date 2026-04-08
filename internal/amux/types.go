package amux

import (
	"strings"
	"time"
)

type Pane struct {
	ID     string `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	CWD    string `json:"cwd,omitempty"`
	Window string `json:"window,omitempty"`
	Lead   bool   `json:"lead,omitempty"`
}

func (p Pane) Ref() string {
	if name := strings.TrimSpace(p.Name); name != "" {
		return name
	}
	return strings.TrimSpace(p.ID)
}

type SpawnRequest struct {
	Session string `json:"session,omitempty"`
	AtPane  string `json:"at_pane,omitempty"`
	Name    string `json:"name,omitempty"`
	CWD     string `json:"cwd,omitempty"`
	Command string `json:"command,omitempty"`
}

type PaneCapture struct {
	Content        []string `json:"content,omitempty"`
	CWD            string   `json:"cwd,omitempty"`
	CurrentCommand string   `json:"current_command,omitempty"`
	ChildPIDs      []int    `json:"child_pids,omitempty"`
	Exited         bool     `json:"exited,omitempty"`
	ExitedSince    string   `json:"exited_since,omitempty"`
}

func (c PaneCapture) Output() string {
	return strings.Join(c.Content, "\n")
}

func (c PaneCapture) ExitedAt() (time.Time, bool) {
	if !c.Exited || strings.TrimSpace(c.ExitedSince) == "" {
		return time.Time{}, false
	}

	exitedAt, err := time.Parse(time.RFC3339, c.ExitedSince)
	if err != nil {
		return time.Time{}, false
	}
	return exitedAt, true
}

package amux

import "strings"

type Pane struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
	CWD  string `json:"cwd,omitempty"`
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
}

func (c PaneCapture) Output() string {
	return strings.Join(c.Content, "\n")
}

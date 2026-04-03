package amux

type Pane struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type SpawnRequest struct {
	Session string `json:"session,omitempty"`
	CWD     string `json:"cwd,omitempty"`
	Command string `json:"command,omitempty"`
}

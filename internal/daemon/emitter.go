package daemon

import (
	"context"
	"encoding/json"
	"io"
	"sync"
)

type NDJSONEmitter struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

func NewNDJSONEmitter(w io.Writer) *NDJSONEmitter {
	return &NDJSONEmitter{encoder: json.NewEncoder(w)}
}

func (e *NDJSONEmitter) Emit(_ context.Context, event Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.encoder.Encode(event)
}

package amux

import (
	"context"
	"strings"
	"sync"
	"time"
)

// SendKeysCall records a SendKeys invocation.
type SendKeysCall struct {
	PaneID string
	Text   string
}

// MetadataCall records a SetMetadata invocation.
type MetadataCall struct {
	PaneID   string
	Metadata map[string]string
}

// MockClient is a test double for daemon tests.
type MockClient struct {
	mu sync.Mutex

	SpawnFunc       func(ctx context.Context, req SpawnRequest) (Pane, error)
	SendKeysFunc    func(ctx context.Context, paneID string, keys ...string) error
	CaptureFunc     func(ctx context.Context, paneID string) (string, error)
	SetMetadataFunc func(ctx context.Context, paneID string, metadata map[string]string) error
	KillPaneFunc    func(ctx context.Context, paneID string) error
	WaitIdleFunc    func(ctx context.Context, paneID string, timeout time.Duration) error

	SpawnCalls       []SpawnRequest
	SendKeysCalls    []SendKeysCall
	CaptureCalls     []string
	SetMetadataCalls []MetadataCall
	KillPaneCalls    []string
	WaitIdleCalls    []struct {
		PaneID  string
		Timeout time.Duration
	}
}

var _ Client = (*MockClient)(nil)

func (m *MockClient) Spawn(ctx context.Context, req SpawnRequest) (Pane, error) {
	m.mu.Lock()
	m.SpawnCalls = append(m.SpawnCalls, req)
	fn := m.SpawnFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, req)
	}
	return Pane{}, nil
}

func (m *MockClient) SendKeys(ctx context.Context, paneID string, keys ...string) error {
	m.mu.Lock()
	m.SendKeysCalls = append(m.SendKeysCalls, SendKeysCall{
		PaneID: paneID,
		Text:   strings.Join(keys, " "),
	})
	fn := m.SendKeysFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, paneID, keys...)
	}
	return nil
}

func (m *MockClient) Capture(ctx context.Context, paneID string) (string, error) {
	m.mu.Lock()
	m.CaptureCalls = append(m.CaptureCalls, paneID)
	fn := m.CaptureFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, paneID)
	}
	return "", nil
}

func (m *MockClient) SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	m.mu.Lock()
	copied := make(map[string]string, len(metadata))
	for key, value := range metadata {
		copied[key] = value
	}
	m.SetMetadataCalls = append(m.SetMetadataCalls, MetadataCall{
		PaneID:   paneID,
		Metadata: copied,
	})
	fn := m.SetMetadataFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, paneID, copied)
	}
	return nil
}

func (m *MockClient) KillPane(ctx context.Context, paneID string) error {
	m.mu.Lock()
	m.KillPaneCalls = append(m.KillPaneCalls, paneID)
	fn := m.KillPaneFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, paneID)
	}
	return nil
}

func (m *MockClient) WaitIdle(ctx context.Context, paneID string, timeout time.Duration) error {
	m.mu.Lock()
	m.WaitIdleCalls = append(m.WaitIdleCalls, struct {
		PaneID  string
		Timeout time.Duration
	}{
		PaneID:  paneID,
		Timeout: timeout,
	})
	fn := m.WaitIdleFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, paneID, timeout)
	}
	return nil
}

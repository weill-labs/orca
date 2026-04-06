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

	SpawnFunc          func(ctx context.Context, req SpawnRequest) (Pane, error)
	PaneExistsFunc     func(ctx context.Context, paneID string) (bool, error)
	ListPanesFunc      func(ctx context.Context) ([]Pane, error)
	SendKeysFunc       func(ctx context.Context, paneID string, keys ...string) error
	CaptureFunc        func(ctx context.Context, paneID string) (string, error)
	CapturePaneFunc    func(ctx context.Context, paneID string) (PaneCapture, error)
	CaptureHistoryFunc func(ctx context.Context, paneID string) (PaneCapture, error)
	SetMetadataFunc    func(ctx context.Context, paneID string, metadata map[string]string) error
	KillPaneFunc       func(ctx context.Context, paneID string) error
	WaitIdleFunc       func(ctx context.Context, paneID string, timeout time.Duration) error
	WaitIdleSettleFunc func(ctx context.Context, paneID string, timeout, settle time.Duration) error
	WaitContentFunc    func(ctx context.Context, paneID, substring string, timeout time.Duration) error

	SpawnCalls          []SpawnRequest
	PaneExistsCalls     []string
	ListPanesCalls      int
	SendKeysCalls       []SendKeysCall
	CaptureCalls        []string
	CapturePaneCalls    []string
	CaptureHistoryCalls []string
	SetMetadataCalls    []MetadataCall
	KillPaneCalls       []string
	WaitIdleCalls       []struct {
		PaneID  string
		Timeout time.Duration
		Settle  time.Duration
	}
	WaitContentCalls []struct {
		PaneID    string
		Substring string
		Timeout   time.Duration
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

func (m *MockClient) PaneExists(ctx context.Context, paneID string) (bool, error) {
	m.mu.Lock()
	m.PaneExistsCalls = append(m.PaneExistsCalls, paneID)
	fn := m.PaneExistsFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, paneID)
	}
	return false, nil
}

func (m *MockClient) ListPanes(ctx context.Context) ([]Pane, error) {
	m.mu.Lock()
	m.ListPanesCalls++
	fn := m.ListPanesFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx)
	}
	return nil, nil
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

func (m *MockClient) CapturePane(ctx context.Context, paneID string) (PaneCapture, error) {
	m.mu.Lock()
	m.CapturePaneCalls = append(m.CapturePaneCalls, paneID)
	fn := m.CapturePaneFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, paneID)
	}
	return PaneCapture{}, nil
}

func (m *MockClient) CaptureHistory(ctx context.Context, paneID string) (PaneCapture, error) {
	m.mu.Lock()
	m.CaptureHistoryCalls = append(m.CaptureHistoryCalls, paneID)
	fn := m.CaptureHistoryFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, paneID)
	}
	return PaneCapture{}, nil
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
		Settle  time.Duration
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

func (m *MockClient) WaitIdleSettle(ctx context.Context, paneID string, timeout, settle time.Duration) error {
	m.mu.Lock()
	m.WaitIdleCalls = append(m.WaitIdleCalls, struct {
		PaneID  string
		Timeout time.Duration
		Settle  time.Duration
	}{
		PaneID:  paneID,
		Timeout: timeout,
		Settle:  settle,
	})
	fn := m.WaitIdleSettleFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, paneID, timeout, settle)
	}
	return nil
}

func (m *MockClient) WaitContent(ctx context.Context, paneID, substring string, timeout time.Duration) error {
	m.mu.Lock()
	m.WaitContentCalls = append(m.WaitContentCalls, struct {
		PaneID    string
		Substring string
		Timeout   time.Duration
	}{
		PaneID:    paneID,
		Substring: substring,
		Timeout:   timeout,
	})
	fn := m.WaitContentFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, paneID, substring, timeout)
	}
	return nil
}

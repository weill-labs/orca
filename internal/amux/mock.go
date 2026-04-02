package amux

import "sync"

// SendKeysCall records a SendKeys invocation.
type SendKeysCall struct {
	PaneID string
	Text   string
}

// MetaCall records a Meta invocation.
type MetaCall struct {
	PaneID string
	Key    string
	Value  string
}

// MockClient is a test double for daemon tests.
type MockClient struct {
	mu sync.Mutex

	SpawnFunc    func(opts SpawnOptions) (string, error)
	SendKeysFunc func(paneID, text string) error
	CaptureFunc  func(paneID string) (string, error)
	MetaFunc     func(paneID, key, value string) error
	KillFunc     func(paneID string) error

	SpawnCalls    []SpawnOptions
	SendKeysCalls []SendKeysCall
	CaptureCalls  []string
	MetaCalls     []MetaCall
	KillCalls     []string
}

var _ Client = (*MockClient)(nil)

func (m *MockClient) Spawn(opts SpawnOptions) (string, error) {
	m.mu.Lock()
	m.SpawnCalls = append(m.SpawnCalls, opts)
	fn := m.SpawnFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(opts)
	}
	return "", nil
}

func (m *MockClient) SendKeys(paneID, text string) error {
	m.mu.Lock()
	m.SendKeysCalls = append(m.SendKeysCalls, SendKeysCall{
		PaneID: paneID,
		Text:   text,
	})
	fn := m.SendKeysFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(paneID, text)
	}
	return nil
}

func (m *MockClient) Capture(paneID string) (string, error) {
	m.mu.Lock()
	m.CaptureCalls = append(m.CaptureCalls, paneID)
	fn := m.CaptureFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(paneID)
	}
	return "", nil
}

func (m *MockClient) Meta(paneID, key, value string) error {
	m.mu.Lock()
	m.MetaCalls = append(m.MetaCalls, MetaCall{
		PaneID: paneID,
		Key:    key,
		Value:  value,
	})
	fn := m.MetaFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(paneID, key, value)
	}
	return nil
}

func (m *MockClient) Kill(paneID string) error {
	m.mu.Lock()
	m.KillCalls = append(m.KillCalls, paneID)
	fn := m.KillFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(paneID)
	}
	return nil
}

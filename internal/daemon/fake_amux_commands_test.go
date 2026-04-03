package daemon

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeAmux struct {
	mu                    sync.Mutex
	spawnPane             Pane
	sendKeysErr           error
	sendKeysResults       []error
	sendKeysHook          func(paneID string, keys []string)
	waitIdleErr           error
	rejectCanceledContext bool
	spawnRequests         []SpawnRequest
	metadata              map[string]map[string]string
	sentKeys              map[string][]string
	captures              map[string][]string
	captureCalls          map[string]int
	killCalls             []string
	waitIdleCalls         []waitIdleCall
}

type waitIdleCall struct {
	PaneID  string
	Timeout time.Duration
}

func (a *fakeAmux) Spawn(ctx context.Context, req SpawnRequest) (Pane, error) {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return Pane{}, ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.spawnRequests = append(a.spawnRequests, req)
	if a.metadata == nil {
		a.metadata = make(map[string]map[string]string)
	}
	if a.sentKeys == nil {
		a.sentKeys = make(map[string][]string)
	}
	return a.spawnPane, nil
}

func (a *fakeAmux) SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.metadata == nil {
		a.metadata = make(map[string]map[string]string)
	}
	copied := make(map[string]string, len(a.metadata[paneID])+len(metadata))
	for key, value := range a.metadata[paneID] {
		copied[key] = value
	}
	for key, value := range metadata {
		copied[key] = value
	}
	a.metadata[paneID] = copied
	return nil
}

func (a *fakeAmux) SendKeys(ctx context.Context, paneID string, keys ...string) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	if a.sendKeysHook != nil {
		a.sendKeysHook(paneID, append([]string(nil), keys...))
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.sendKeysResults) > 0 {
		err := a.sendKeysResults[0]
		a.sendKeysResults = a.sendKeysResults[1:]
		if err != nil {
			return err
		}
	} else if a.sendKeysErr != nil {
		return a.sendKeysErr
	}

	if a.sentKeys == nil {
		a.sentKeys = make(map[string][]string)
	}
	a.sentKeys[paneID] = append(a.sentKeys[paneID], normalizeSentKeys(keys...)...)
	return nil
}

func (a *fakeAmux) Capture(ctx context.Context, paneID string) (string, error) {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return "", ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.captureCalls == nil {
		a.captureCalls = make(map[string]int)
	}
	a.captureCalls[paneID]++
	sequence := a.captures[paneID]
	if len(sequence) == 0 {
		return "", nil
	}
	if len(sequence) == 1 {
		return sequence[0], nil
	}
	value := sequence[0]
	a.captures[paneID] = sequence[1:]
	return value, nil
}

func (a *fakeAmux) KillPane(ctx context.Context, paneID string) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.killCalls = append(a.killCalls, paneID)
	return nil
}

func (a *fakeAmux) WaitIdle(ctx context.Context, paneID string, timeout time.Duration) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.waitIdleCalls = append(a.waitIdleCalls, waitIdleCall{PaneID: paneID, Timeout: timeout})
	return a.waitIdleErr
}

func (a *fakeAmux) captureSequence(paneID string, sequence []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	copied := make([]string, len(sequence))
	copy(copied, sequence)
	a.captures[paneID] = copied
}

func (a *fakeAmux) countKey(paneID, key string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	count := 0
	for _, entry := range a.sentKeys[paneID] {
		if entry == key {
			count++
		}
	}
	return count
}

func (a *fakeAmux) captureCount(paneID string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.captureCalls[paneID]
}

func (a *fakeAmux) requireMetadata(t *testing.T, paneID string, want map[string]string) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if got := a.metadata[paneID]; !reflect.DeepEqual(got, want) {
		t.Fatalf("metadata[%q] = %#v, want %#v", paneID, got, want)
	}
}

func (a *fakeAmux) requireSentKeys(t *testing.T, paneID string, want []string) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	got := append([]string(nil), a.sentKeys[paneID]...)
	want = normalizeSentKeys(want...)
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sentKeys[%q] = %#v, want %#v", paneID, got, want)
	}
}

func normalizeSentKeys(keys ...string) []string {
	normalized := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "Enter" {
			if len(normalized) == 0 {
				normalized = append(normalized, "\n")
				continue
			}
			normalized[len(normalized)-1] += "\n"
			continue
		}
		normalized = append(normalized, key)
	}
	return normalized
}

type fakeCommands struct {
	mu      sync.Mutex
	calls   []commandCall
	queued  map[string][]commandResult
	blocked map[string]commandBlock
}

type commandCall struct {
	Dir  string
	Name string
	Args []string
}

type commandResult struct {
	output string
	err    error
}

type commandBlock struct {
	started chan struct{}
	release chan struct{}
}

func newFakeCommands() *fakeCommands {
	return &fakeCommands{
		queued:  make(map[string][]commandResult),
		blocked: make(map[string]commandBlock),
	}
}

func (c *fakeCommands) Run(_ context.Context, dir, name string, args ...string) ([]byte, error) {
	c.mu.Lock()

	call := commandCall{
		Dir:  dir,
		Name: name,
		Args: append([]string(nil), args...),
	}
	c.calls = append(c.calls, call)

	key := c.key(name, args)
	block, blocked := c.blocked[key]
	if blocked {
		delete(c.blocked, key)
	}
	queue := c.queued[key]
	var result commandResult
	if len(queue) == 0 {
		c.mu.Unlock()
		if blocked {
			select {
			case block.started <- struct{}{}:
			default:
			}
			<-block.release
		}
		return nil, nil
	}
	result = queue[0]
	c.queued[key] = queue[1:]
	c.mu.Unlock()

	if blocked {
		select {
		case block.started <- struct{}{}:
		default:
		}
		<-block.release
	}
	return []byte(result.output), result.err
}

func (c *fakeCommands) queue(name string, args []string, output string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.key(name, args)
	c.queued[key] = append(c.queued[key], commandResult{output: output, err: err})
}

func (c *fakeCommands) block(name string, args []string) commandBlock {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.key(name, args)
	block := commandBlock{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	c.blocked[key] = block
	return block
}

func (c *fakeCommands) callsByName(name string) []commandCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []commandCall
	for _, call := range c.calls {
		if call.Name == name {
			out = append(out, call)
		}
	}
	return out
}

func (c *fakeCommands) countCall(name string, args ...string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := 0
	for _, call := range c.calls {
		if call.Name == name && reflect.DeepEqual(call.Args, args) {
			count++
		}
	}
	return count
}

func (c *fakeCommands) tailGitCalls(count int) []commandCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	var gitCalls []commandCall
	for _, call := range c.calls {
		if call.Name == "git" {
			gitCalls = append(gitCalls, call)
		}
	}
	if len(gitCalls) < count {
		return gitCalls
	}
	return gitCalls[len(gitCalls)-count:]
}

func (c *fakeCommands) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = nil
}

func (c *fakeCommands) countCalls(name string, args []string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, call := range c.calls {
		if call.Name != name {
			continue
		}
		if reflect.DeepEqual(call.Args, args) {
			count++
		}
	}
	return count
}

func (c *fakeCommands) key(name string, args []string) string {
	return name + "\x00" + strings.Join(args, "\x00")
}

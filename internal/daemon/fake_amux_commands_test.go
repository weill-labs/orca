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
	paneExists            map[string]bool
	paneExistsErr         error
	listPanes             []Pane
	listPanesErr          error
	sendKeysErr           error
	sendKeysResults       []error
	sendKeysHook          func(paneID string, keys []string)
	setMetadataHook       func(paneID string, metadata map[string]string)
	killHook              func(paneID string)
	waitIdleErr           error
	waitIdleHook          func(paneID string, timeout time.Duration)
	capturePaneErr        error
	rejectCanceledContext bool
	spawnRequests         []SpawnRequest
	metadata              map[string]map[string]string
	sentKeys              map[string][]string
	captures              map[string][]string
	paneCaptures          map[string][]PaneCapture
	paneExistsCalls       []string
	captureCalls          map[string]int
	historyCaptures       map[string][]PaneCapture
	historyCaptureCalls   map[string]int
	historyCaptureErrors  map[string][]error
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

func (a *fakeAmux) ListPanes(ctx context.Context) ([]Pane, error) {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.listPanesErr != nil {
		return nil, a.listPanesErr
	}
	out := make([]Pane, len(a.listPanes))
	copy(out, a.listPanes)
	return out, nil
}

func (a *fakeAmux) PaneExists(ctx context.Context, paneID string) (bool, error) {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return false, ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.paneExistsCalls = append(a.paneExistsCalls, paneID)
	if a.paneExistsErr != nil {
		return false, a.paneExistsErr
	}
	if a.paneExists != nil {
		if exists, ok := a.paneExists[paneID]; ok {
			return exists, nil
		}
	}
	return true, nil
}

func (a *fakeAmux) SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	if a.setMetadataHook != nil {
		copied := make(map[string]string, len(metadata))
		for key, value := range metadata {
			copied[key] = value
		}
		a.setMetadataHook(paneID, copied)
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
	a.sentKeys[paneID] = appendNormalizedSentKeys(a.sentKeys[paneID], normalizeSentKeys(keys...))
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
	value, remaining, ok := nextStringCapture(a.captures[paneID])
	if !ok {
		return "", nil
	}
	a.captures[paneID] = remaining
	return value, nil
}

func (a *fakeAmux) CapturePane(ctx context.Context, paneID string) (PaneCapture, error) {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return PaneCapture{}, ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.captureCalls == nil {
		a.captureCalls = make(map[string]int)
	}
	a.captureCalls[paneID]++
	if a.capturePaneErr != nil {
		return PaneCapture{}, a.capturePaneErr
	}

	if sequence := a.paneCaptures[paneID]; len(sequence) > 0 {
		value, remaining, _ := nextPaneCapture(sequence)
		a.paneCaptures[paneID] = remaining
		return value, nil
	}

	output, remaining, ok := nextStringCapture(a.captures[paneID])
	if !ok {
		return PaneCapture{}, nil
	}
	a.captures[paneID] = remaining
	return paneCaptureFromOutput(output), nil
}

func (a *fakeAmux) CaptureHistory(ctx context.Context, paneID string) (PaneCapture, error) {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return PaneCapture{}, ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.historyCaptureCalls == nil {
		a.historyCaptureCalls = make(map[string]int)
	}
	a.historyCaptureCalls[paneID]++
	if errors := a.historyCaptureErrors[paneID]; len(errors) > 0 {
		err := errors[0]
		if len(errors) == 1 {
			delete(a.historyCaptureErrors, paneID)
		} else {
			a.historyCaptureErrors[paneID] = errors[1:]
		}
		if err != nil {
			return PaneCapture{}, err
		}
	}
	value, remaining, ok := nextPaneCapture(a.historyCaptures[paneID])
	if !ok {
		return PaneCapture{}, nil
	}
	a.historyCaptures[paneID] = remaining
	return value, nil
}

func (a *fakeAmux) KillPane(ctx context.Context, paneID string) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	if a.killHook != nil {
		a.killHook(paneID)
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
	if a.waitIdleHook != nil {
		a.waitIdleHook(paneID, timeout)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.waitIdleCalls = append(a.waitIdleCalls, waitIdleCall{PaneID: paneID, Timeout: timeout})
	return a.waitIdleErr
}

func (a *fakeAmux) WaitIdleSettle(ctx context.Context, paneID string, _, timeout time.Duration) error {
	return a.WaitIdle(ctx, paneID, timeout)
}

func (a *fakeAmux) WaitContent(_ context.Context, _, _ string, _ time.Duration) error {
	return nil
}

func (a *fakeAmux) captureSequence(paneID string, sequence []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	copied := make([]string, len(sequence))
	copy(copied, sequence)
	a.captures[paneID] = copied
}

func (a *fakeAmux) capturePaneSequence(paneID string, sequence []PaneCapture) {
	a.mu.Lock()
	defer a.mu.Unlock()
	copied := make([]PaneCapture, len(sequence))
	for i, capture := range sequence {
		copied[i] = clonePaneCapture(capture)
	}
	if a.paneCaptures == nil {
		a.paneCaptures = make(map[string][]PaneCapture)
	}
	a.paneCaptures[paneID] = copied
}

func (a *fakeAmux) captureHistorySequence(paneID string, sequence []PaneCapture) {
	a.mu.Lock()
	defer a.mu.Unlock()
	copied := make([]PaneCapture, len(sequence))
	for i, capture := range sequence {
		copied[i] = clonePaneCapture(capture)
	}
	if a.historyCaptures == nil {
		a.historyCaptures = make(map[string][]PaneCapture)
	}
	a.historyCaptures[paneID] = copied
}

func (a *fakeAmux) captureHistoryErrors(paneID string, errs []error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	copied := append([]error(nil), errs...)
	if a.historyCaptureErrors == nil {
		a.historyCaptureErrors = make(map[string][]error)
	}
	a.historyCaptureErrors[paneID] = copied
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

func (a *fakeAmux) captureHistoryCount(paneID string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.historyCaptureCalls[paneID]
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

func appendNormalizedSentKeys(existing, keys []string) []string {
	if len(keys) != 1 || keys[0] != "\n" || len(existing) == 0 || strings.HasSuffix(existing[len(existing)-1], "\n") {
		return append(existing, keys...)
	}

	merged := append([]string(nil), existing...)
	merged[len(merged)-1] += "\n"
	return merged
}

func clonePaneCapture(capture PaneCapture) PaneCapture {
	return PaneCapture{
		Content:        append([]string(nil), capture.Content...),
		CWD:            capture.CWD,
		CurrentCommand: capture.CurrentCommand,
		ChildPIDs:      append([]int(nil), capture.ChildPIDs...),
		Exited:         capture.Exited,
	}
}

func paneCaptureFromOutput(output string) PaneCapture {
	if output == "" {
		return PaneCapture{}
	}
	return PaneCapture{Content: strings.Split(output, "\n")}
}

func nextStringCapture(sequence []string) (string, []string, bool) {
	if len(sequence) == 0 {
		return "", nil, false
	}
	if len(sequence) == 1 {
		return sequence[0], sequence, true
	}
	return sequence[0], append([]string(nil), sequence[1:]...), true
}

func nextPaneCapture(sequence []PaneCapture) (PaneCapture, []PaneCapture, bool) {
	if len(sequence) == 0 {
		return PaneCapture{}, nil, false
	}
	if len(sequence) == 1 {
		return clonePaneCapture(sequence[0]), sequence, true
	}
	return clonePaneCapture(sequence[0]), append([]PaneCapture(nil), sequence[1:]...), true
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

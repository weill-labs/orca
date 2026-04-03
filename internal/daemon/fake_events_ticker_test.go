package daemon

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeEvents struct {
	mu     sync.Mutex
	events []Event
}

func newFakeEvents() *fakeEvents {
	return &fakeEvents{}
}

func (e *fakeEvents) Emit(_ context.Context, event Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
	return nil
}

func (e *fakeEvents) countType(eventType string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	count := 0
	for _, event := range e.events {
		if event.Type == eventType {
			count++
		}
	}
	return count
}

func (e *fakeEvents) lastEventOfType(eventType string) (Event, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := len(e.events) - 1; i >= 0; i-- {
		if e.events[i].Type == eventType {
			return e.events[i], true
		}
	}
	return Event{}, false
}

func (e *fakeEvents) requireTypes(t *testing.T, want ...string) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	have := make(map[string]bool, len(e.events))
	for _, event := range e.events {
		have[event.Type] = true
	}
	for _, eventType := range want {
		if !have[eventType] {
			t.Fatalf("event %q missing from %#v", eventType, e.events)
		}
	}
}

type fakeTickerFactory struct {
	mu      sync.Mutex
	tickers []*fakeTicker
}

func (f *fakeTickerFactory) enqueue(tickers ...*fakeTicker) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tickers = append(f.tickers, tickers...)
}

func (f *fakeTickerFactory) NewTicker(_ time.Duration) Ticker {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.tickers) == 0 {
		return newFakeTicker()
	}
	ticker := f.tickers[0]
	f.tickers = f.tickers[1:]
	return ticker
}

type fakeTicker struct {
	ch chan time.Time
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{ch: make(chan time.Time, 16)}
}

func (t *fakeTicker) C() <-chan time.Time {
	return t.ch
}

func (t *fakeTicker) Stop() {}

func (t *fakeTicker) tick(now time.Time) {
	t.ch <- now
}

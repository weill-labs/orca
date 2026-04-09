package daemon

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/weill-labs/orca/internal/amux"
)

const (
	defaultCircuitBreakerFailureThreshold = 3
	defaultCircuitBreakerCooldown         = 60 * time.Second
)

var ErrCircuitBreakerOpen = errors.New("circuit breaker open")

type CircuitBreakerHooks struct {
	OnOpen  func()
	OnClose func()
}

type CircuitBreaker struct {
	now       func() time.Time
	threshold int
	cooldown  time.Duration
	hooks     CircuitBreakerHooks

	mu                  sync.Mutex
	consecutiveFailures int
	openedAt            time.Time
}

func NewCircuitBreaker(now func() time.Time, threshold int, cooldown time.Duration) *CircuitBreaker {
	return NewCircuitBreakerWithHooks(now, threshold, cooldown, CircuitBreakerHooks{})
}

func NewCircuitBreakerWithHooks(now func() time.Time, threshold int, cooldown time.Duration, hooks CircuitBreakerHooks) *CircuitBreaker {
	if now == nil {
		now = time.Now
	}
	if threshold <= 0 {
		threshold = 1
	}

	return &CircuitBreaker{
		now:       now,
		threshold: threshold,
		cooldown:  cooldown,
		hooks:     hooks,
	}
}

func (c *CircuitBreaker) Allow() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	shouldClose := c.shouldCloseLocked()
	if shouldClose {
		c.closeLocked()
	}
	isOpen := c.isOpenLocked()
	onClose := c.hooks.OnClose
	c.mu.Unlock()

	if shouldClose && onClose != nil {
		onClose()
	}
	if isOpen {
		return ErrCircuitBreakerOpen
	}
	return nil
}

func (c *CircuitBreaker) RecordSuccess() {
	if c == nil {
		return
	}

	c.mu.Lock()
	c.closeLocked()
	c.mu.Unlock()
}

func (c *CircuitBreaker) RecordFailure() {
	if c == nil {
		return
	}

	c.mu.Lock()
	shouldClose := c.shouldCloseLocked()
	if shouldClose {
		c.closeLocked()
	}
	if c.isOpenLocked() {
		c.mu.Unlock()
		return
	}

	c.consecutiveFailures++
	shouldOpen := c.consecutiveFailures >= c.threshold
	if shouldOpen {
		c.openedAt = c.now()
	}
	onOpen := c.hooks.OnOpen
	onClose := c.hooks.OnClose
	c.mu.Unlock()

	if shouldClose && onClose != nil {
		onClose()
	}
	if shouldOpen && onOpen != nil {
		onOpen()
	}
}

func (c *CircuitBreaker) shouldCloseLocked() bool {
	return c.isOpenLocked() && !c.now().Before(c.openedAt.Add(c.cooldown))
}

func (c *CircuitBreaker) isOpenLocked() bool {
	return !c.openedAt.IsZero()
}

func (c *CircuitBreaker) closeLocked() {
	c.consecutiveFailures = 0
	c.openedAt = time.Time{}
}

type amuxClientContextKey struct{}
type commandRunnerContextKey struct{}
type gitHubClientFactoryContextKey struct{}

func (d *Daemon) withMonitorCircuits(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, amuxClientContextKey{}, newCircuitAmuxClient(d.amux, d.monitorAmuxCircuit))
	ctx = context.WithValue(ctx, commandRunnerContextKey{}, newCircuitCommandRunner(d.commands, d.monitorGitHubCircuit))
	ctx = context.WithValue(ctx, gitHubClientFactoryContextKey{}, func(projectPath string) gitHubClient {
		return newCircuitGitHubClient(d.githubForProject(projectPath), d.monitorGitHubCircuit)
	})
	return ctx
}

func (d *Daemon) amuxClient(ctx context.Context) AmuxClient {
	if client, ok := ctx.Value(amuxClientContextKey{}).(AmuxClient); ok && client != nil {
		return client
	}
	return d.amux
}

func (d *Daemon) commandRunner(ctx context.Context) CommandRunner {
	if runner, ok := ctx.Value(commandRunnerContextKey{}).(CommandRunner); ok && runner != nil {
		return runner
	}
	return d.commands
}

func (d *Daemon) gitHubClientForContext(ctx context.Context, projectPath string) gitHubClient {
	if factory, ok := ctx.Value(gitHubClientFactoryContextKey{}).(func(string) gitHubClient); ok && factory != nil {
		return factory(projectPath)
	}
	return d.githubForProject(projectPath)
}

type circuitAmuxClient struct {
	base    AmuxClient
	breaker *CircuitBreaker
}

func newCircuitAmuxClient(base AmuxClient, breaker *CircuitBreaker) AmuxClient {
	if base == nil || breaker == nil {
		return base
	}
	return &circuitAmuxClient{base: base, breaker: breaker}
}

func (c *circuitAmuxClient) Spawn(ctx context.Context, req SpawnRequest) (Pane, error) {
	return withCircuit(c.breaker, func() (Pane, error) {
		return c.base.Spawn(ctx, req)
	})
}

func (c *circuitAmuxClient) PaneExists(ctx context.Context, paneID string) (bool, error) {
	return withCircuit(c.breaker, func() (bool, error) {
		return c.base.PaneExists(ctx, paneID)
	})
}

func (c *circuitAmuxClient) ListPanes(ctx context.Context) ([]Pane, error) {
	return withCircuit(c.breaker, func() ([]Pane, error) {
		return c.base.ListPanes(ctx)
	})
}

func (c *circuitAmuxClient) Events(ctx context.Context, req amux.EventsRequest) (<-chan amux.Event, <-chan error) {
	return c.base.Events(ctx, req)
}

func (c *circuitAmuxClient) Metadata(ctx context.Context, paneID string) (map[string]string, error) {
	return withCircuit(c.breaker, func() (map[string]string, error) {
		return c.base.Metadata(ctx, paneID)
	})
}

func (c *circuitAmuxClient) SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	return withCircuitErr(c.breaker, func() error {
		return c.base.SetMetadata(ctx, paneID, metadata)
	})
}

func (c *circuitAmuxClient) RemoveMetadata(ctx context.Context, paneID string, keys ...string) error {
	return withCircuitErr(c.breaker, func() error {
		return c.base.RemoveMetadata(ctx, paneID, keys...)
	})
}

func (c *circuitAmuxClient) SendKeys(ctx context.Context, paneID string, keys ...string) error {
	return withCircuitErr(c.breaker, func() error {
		return c.base.SendKeys(ctx, paneID, keys...)
	})
}

func (c *circuitAmuxClient) Capture(ctx context.Context, paneID string) (string, error) {
	return withCircuit(c.breaker, func() (string, error) {
		return c.base.Capture(ctx, paneID)
	})
}

func (c *circuitAmuxClient) CapturePane(ctx context.Context, paneID string) (PaneCapture, error) {
	return withCircuit(c.breaker, func() (PaneCapture, error) {
		return c.base.CapturePane(ctx, paneID)
	})
}

func (c *circuitAmuxClient) CaptureHistory(ctx context.Context, paneID string) (PaneCapture, error) {
	return withCircuit(c.breaker, func() (PaneCapture, error) {
		return c.base.CaptureHistory(ctx, paneID)
	})
}

func (c *circuitAmuxClient) KillPane(ctx context.Context, paneID string) error {
	return withCircuitErr(c.breaker, func() error {
		return c.base.KillPane(ctx, paneID)
	})
}

func (c *circuitAmuxClient) WaitIdle(ctx context.Context, paneID string, timeout time.Duration) error {
	return withCircuitErr(c.breaker, func() error {
		return c.base.WaitIdle(ctx, paneID, timeout)
	})
}

func (c *circuitAmuxClient) WaitIdleSettle(ctx context.Context, paneID string, timeout, settle time.Duration) error {
	return withCircuitErr(c.breaker, func() error {
		return c.base.WaitIdleSettle(ctx, paneID, timeout, settle)
	})
}

func (c *circuitAmuxClient) WaitContent(ctx context.Context, paneID, substring string, timeout time.Duration) error {
	return withCircuitErr(c.breaker, func() error {
		return c.base.WaitContent(ctx, paneID, substring, timeout)
	})
}

type circuitCommandRunner struct {
	base    CommandRunner
	breaker *CircuitBreaker
}

func newCircuitCommandRunner(base CommandRunner, breaker *CircuitBreaker) CommandRunner {
	if base == nil || breaker == nil {
		return base
	}
	return &circuitCommandRunner{base: base, breaker: breaker}
}

func (c *circuitCommandRunner) Run(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
	if name != "gh" {
		return c.base.Run(ctx, dir, name, args...)
	}
	return withCircuit(c.breaker, func() ([]byte, error) {
		return c.base.Run(ctx, dir, name, args...)
	})
}

type circuitGitHubClient struct {
	base    gitHubClient
	breaker *CircuitBreaker
}

func newCircuitGitHubClient(base gitHubClient, breaker *CircuitBreaker) gitHubClient {
	if base == nil || breaker == nil {
		return base
	}
	return &circuitGitHubClient{base: base, breaker: breaker}
}

func (c *circuitGitHubClient) lookupPRNumber(ctx context.Context, branch string) (int, error) {
	return withCircuit(c.breaker, func() (int, error) {
		return c.base.lookupPRNumber(ctx, branch)
	})
}

func (c *circuitGitHubClient) lookupOpenPRNumber(ctx context.Context, branch string) (int, error) {
	return withCircuit(c.breaker, func() (int, error) {
		return c.base.lookupOpenPRNumber(ctx, branch)
	})
}

func (c *circuitGitHubClient) isPRMerged(ctx context.Context, prNumber int) (bool, error) {
	return withCircuit(c.breaker, func() (bool, error) {
		return c.base.isPRMerged(ctx, prNumber)
	})
}

func (c *circuitGitHubClient) lookupPRReviews(ctx context.Context, prNumber int) (prReviewPayload, bool, error) {
	return withCircuit3(c.breaker, func() (prReviewPayload, bool, error) {
		return c.base.lookupPRReviews(ctx, prNumber)
	})
}

func withCircuit[T any](breaker *CircuitBreaker, call func() (T, error)) (T, error) {
	var zero T
	if err := breaker.Allow(); err != nil {
		return zero, err
	}

	value, err := call()
	if err != nil {
		if shouldRecordCircuitFailure(err) {
			breaker.RecordFailure()
		}
		return zero, err
	}

	breaker.RecordSuccess()
	return value, nil
}

func withCircuit3[T1 any, T2 any](breaker *CircuitBreaker, call func() (T1, T2, error)) (T1, T2, error) {
	var zero1 T1
	var zero2 T2
	if err := breaker.Allow(); err != nil {
		return zero1, zero2, err
	}

	value1, value2, err := call()
	if err != nil {
		if shouldRecordCircuitFailure(err) {
			breaker.RecordFailure()
		}
		return zero1, zero2, err
	}

	breaker.RecordSuccess()
	return value1, value2, nil
}

func withCircuitErr(breaker *CircuitBreaker, call func() error) error {
	if err := breaker.Allow(); err != nil {
		return err
	}

	if err := call(); err != nil {
		if shouldRecordCircuitFailure(err) {
			breaker.RecordFailure()
		}
		return err
	}

	breaker.RecordSuccess()
	return nil
}

func shouldRecordCircuitFailure(err error) bool {
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

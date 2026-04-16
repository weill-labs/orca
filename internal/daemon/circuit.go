package daemon

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/weill-labs/orca/internal/amux"
)

const (
	defaultCircuitBreakerFailureThreshold = 3
	defaultCircuitBreakerCooldown         = 60 * time.Second
)

var ErrCircuitBreakerOpen = errors.New("circuit breaker open")

type CircuitBreakerTransition struct {
	FailureCount int
	Cooldown     time.Duration
	Err          error
}

type CircuitBreakerHooks struct {
	OnOpen  func(CircuitBreakerTransition)
	OnClose func(CircuitBreakerTransition)
}

type CircuitBreaker struct {
	now       func() time.Time
	threshold int
	cooldown  time.Duration
	hooks     CircuitBreakerHooks

	mu                  sync.Mutex
	consecutiveFailures int
	openedAt            time.Time
	activeCooldown      time.Duration
	reopenStreak        int
	lastFailure         error
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
	if cooldown < 0 {
		cooldown = 0
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
	closeInfo, shouldClose := c.expireOpenLocked()
	if shouldClose {
	}
	isOpen := c.isOpenLocked()
	onClose := c.hooks.OnClose
	c.mu.Unlock()

	if shouldClose && onClose != nil {
		onClose(closeInfo)
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
	c.resetLocked()
	c.mu.Unlock()
}

func (c *CircuitBreaker) RecordFailure(err error) {
	if c == nil {
		return
	}

	c.mu.Lock()
	closeInfo, shouldClose := c.expireOpenLocked()
	if shouldClose {
	}
	if c.isOpenLocked() {
		c.mu.Unlock()
		if shouldClose && c.hooks.OnClose != nil {
			c.hooks.OnClose(closeInfo)
		}
		return
	}

	c.consecutiveFailures++
	c.lastFailure = err
	shouldOpen := c.consecutiveFailures >= c.threshold
	var openInfo CircuitBreakerTransition
	if shouldOpen {
		openInfo = c.openLocked()
	}
	onOpen := c.hooks.OnOpen
	onClose := c.hooks.OnClose
	c.mu.Unlock()

	if shouldClose && onClose != nil {
		onClose(closeInfo)
	}
	if shouldOpen && onOpen != nil {
		onOpen(openInfo)
	}
}

func (c *CircuitBreaker) shouldCloseLocked() bool {
	return c.isOpenLocked() && !c.now().Before(c.openedAt.Add(c.activeCooldown))
}

func (c *CircuitBreaker) isOpenLocked() bool {
	return !c.openedAt.IsZero()
}

func (c *CircuitBreaker) openLocked() CircuitBreakerTransition {
	c.openedAt = c.now()
	c.activeCooldown = exponentialCircuitCooldown(c.cooldown, c.reopenStreak)
	c.reopenStreak++
	return c.transitionLocked()
}

func (c *CircuitBreaker) expireOpenLocked() (CircuitBreakerTransition, bool) {
	if !c.shouldCloseLocked() {
		return CircuitBreakerTransition{}, false
	}
	info := c.transitionLocked()
	c.closeLocked()
	return info, true
}

func (c *CircuitBreaker) transitionLocked() CircuitBreakerTransition {
	return CircuitBreakerTransition{
		FailureCount: c.consecutiveFailures,
		Cooldown:     c.activeCooldown,
		Err:          c.lastFailure,
	}
}

func (c *CircuitBreaker) closeLocked() {
	c.consecutiveFailures = 0
	c.openedAt = time.Time{}
	c.activeCooldown = 0
	c.lastFailure = nil
}

func (c *CircuitBreaker) resetLocked() {
	c.closeLocked()
	c.reopenStreak = 0
}

func exponentialCircuitCooldown(base time.Duration, reopenStreak int) time.Duration {
	if base <= 0 {
		return 0
	}

	cooldown := base
	for i := 0; i < reopenStreak; i++ {
		if cooldown > time.Duration(math.MaxInt64/2) {
			return time.Duration(math.MaxInt64)
		}
		cooldown *= 2
	}
	return cooldown
}

type amuxClientContextKey struct{}
type commandRunnerContextKey struct{}
type gitHubClientFactoryContextKey struct{}

func (d *Daemon) withMonitorCircuits(ctx context.Context) context.Context {
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

var _ AmuxClient = (*circuitAmuxClient)(nil)

func newCircuitAmuxClient(base AmuxClient, breaker *CircuitBreaker) AmuxClient {
	if base == nil || breaker == nil {
		return base
	}
	return &circuitAmuxClient{base: base, breaker: breaker}
}

func (c *circuitAmuxClient) Spawn(ctx context.Context, req SpawnRequest) (Pane, error) {
	return withAmuxCircuit(c.breaker, func() (Pane, error) {
		return c.base.Spawn(ctx, req)
	})
}

func (c *circuitAmuxClient) PaneExists(ctx context.Context, paneID string) (bool, error) {
	return withAmuxCircuit(c.breaker, func() (bool, error) {
		return c.base.PaneExists(ctx, paneID)
	})
}

func (c *circuitAmuxClient) ListPanes(ctx context.Context) ([]Pane, error) {
	return withAmuxCircuit(c.breaker, func() ([]Pane, error) {
		return c.base.ListPanes(ctx)
	})
}

func (c *circuitAmuxClient) Metadata(ctx context.Context, paneID string) (map[string]string, error) {
	return withAmuxCircuit(c.breaker, func() (map[string]string, error) {
		return c.base.Metadata(ctx, paneID)
	})
}

func (c *circuitAmuxClient) SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	return withAmuxCircuitErr(c.breaker, func() error {
		return c.base.SetMetadata(ctx, paneID, metadata)
	})
}

func (c *circuitAmuxClient) RemoveMetadata(ctx context.Context, paneID string, keys ...string) error {
	return withAmuxCircuitErr(c.breaker, func() error {
		return c.base.RemoveMetadata(ctx, paneID, keys...)
	})
}

func (c *circuitAmuxClient) SendKeys(ctx context.Context, paneID string, keys ...string) error {
	return withAmuxCircuitErr(c.breaker, func() error {
		return c.base.SendKeys(ctx, paneID, keys...)
	})
}

func (c *circuitAmuxClient) Capture(ctx context.Context, paneID string) (string, error) {
	return withAmuxCircuit(c.breaker, func() (string, error) {
		return c.base.Capture(ctx, paneID)
	})
}

func (c *circuitAmuxClient) CapturePane(ctx context.Context, paneID string) (PaneCapture, error) {
	return withAmuxCircuit(c.breaker, func() (PaneCapture, error) {
		return c.base.CapturePane(ctx, paneID)
	})
}

func (c *circuitAmuxClient) CaptureHistory(ctx context.Context, paneID string) (PaneCapture, error) {
	return withAmuxCircuit(c.breaker, func() (PaneCapture, error) {
		return c.base.CaptureHistory(ctx, paneID)
	})
}

func (c *circuitAmuxClient) KillPane(ctx context.Context, paneID string) error {
	return withAmuxCircuitErr(c.breaker, func() error {
		return c.base.KillPane(ctx, paneID)
	})
}

func (c *circuitAmuxClient) WaitIdle(ctx context.Context, paneID string, timeout time.Duration) error {
	return withAmuxCircuitErr(c.breaker, func() error {
		return c.base.WaitIdle(ctx, paneID, timeout)
	})
}

func (c *circuitAmuxClient) WaitIdleSettle(ctx context.Context, paneID string, timeout, settle time.Duration) error {
	return withAmuxCircuitErr(c.breaker, func() error {
		return c.base.WaitIdleSettle(ctx, paneID, timeout, settle)
	})
}

func (c *circuitAmuxClient) WaitContent(ctx context.Context, paneID, substring string, timeout time.Duration) error {
	return withAmuxCircuitErr(c.breaker, func() error {
		return c.base.WaitContent(ctx, paneID, substring, timeout)
	})
}

func (c *circuitAmuxClient) Events(ctx context.Context, req amux.EventsRequest) (<-chan amux.Event, <-chan error) {
	if err := c.breaker.Allow(); err != nil {
		eventsCh := make(chan amux.Event)
		errCh := make(chan error, 1)
		close(eventsCh)
		errCh <- err
		close(errCh)
		return eventsCh, errCh
	}

	eventsCh, errCh := c.base.Events(ctx, req)
	outEvents := make(chan amux.Event)
	outErr := make(chan error, 1)

	go func() {
		defer close(outEvents)
		defer close(outErr)

		for eventsCh != nil || errCh != nil {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-eventsCh:
				if !ok {
					eventsCh = nil
					continue
				}
				outEvents <- event
			case err, ok := <-errCh:
				if !ok {
					errCh = nil
					continue
				}
				if err != nil {
					if shouldRecordAmuxCircuitFailure(err) {
						c.breaker.RecordFailure(err)
					}
					outErr <- err
					return
				}
			}
		}

		c.breaker.RecordSuccess()
	}()

	return outEvents, outErr
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

func (c *circuitGitHubClient) findPRByIssueID(ctx context.Context, issueID string) (int, string, error) {
	return withCircuit3(c.breaker, func() (int, string, error) {
		return c.base.findPRByIssueID(ctx, issueID)
	})
}

func (c *circuitGitHubClient) lookupOpenPRNumber(ctx context.Context, branch string) (int, error) {
	return withCircuit(c.breaker, func() (int, error) {
		return c.base.lookupOpenPRNumber(ctx, branch)
	})
}

func (c *circuitGitHubClient) lookupOpenOrMergedPRNumber(ctx context.Context, branch string) (int, bool, error) {
	return withCircuit3(c.breaker, func() (int, bool, error) {
		return c.base.lookupOpenOrMergedPRNumber(ctx, branch)
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

func withAmuxCircuit[T any](breaker *CircuitBreaker, call func() (T, error)) (T, error) {
	return withCircuitPredicate(breaker, shouldRecordAmuxCircuitFailure, call)
}

func withAmuxCircuitErr(breaker *CircuitBreaker, call func() error) error {
	return withCircuitErrPredicate(breaker, shouldRecordAmuxCircuitFailure, call)
}

func withCircuit[T any](breaker *CircuitBreaker, call func() (T, error)) (T, error) {
	return withCircuitPredicate(breaker, shouldRecordCircuitFailure, call)
}

func withCircuitPredicate[T any](breaker *CircuitBreaker, shouldRecord func(error) bool, call func() (T, error)) (T, error) {
	var zero T
	if err := breaker.Allow(); err != nil {
		return zero, err
	}

	value, err := call()
	if err != nil {
		if shouldRecord(err) {
			breaker.RecordFailure(err)
		}
		return zero, err
	}

	breaker.RecordSuccess()
	return value, nil
}

func withCircuit3[T1 any, T2 any](breaker *CircuitBreaker, call func() (T1, T2, error)) (T1, T2, error) {
	return withCircuit3Predicate(breaker, shouldRecordCircuitFailure, call)
}

func withCircuit3Predicate[T1 any, T2 any](breaker *CircuitBreaker, shouldRecord func(error) bool, call func() (T1, T2, error)) (T1, T2, error) {
	var zero1 T1
	var zero2 T2
	if err := breaker.Allow(); err != nil {
		return zero1, zero2, err
	}

	value1, value2, err := call()
	if err != nil {
		if shouldRecord(err) {
			breaker.RecordFailure(err)
		}
		return zero1, zero2, err
	}

	breaker.RecordSuccess()
	return value1, value2, nil
}

func withCircuitErr(breaker *CircuitBreaker, call func() error) error {
	return withCircuitErrPredicate(breaker, shouldRecordCircuitFailure, call)
}

func withCircuitErrPredicate(breaker *CircuitBreaker, shouldRecord func(error) bool, call func() error) error {
	if err := breaker.Allow(); err != nil {
		return err
	}

	if err := call(); err != nil {
		if shouldRecord(err) {
			breaker.RecordFailure(err)
		}
		return err
	}

	breaker.RecordSuccess()
	return nil
}

func shouldRecordCircuitFailure(err error) bool {
	return !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!isPaneGoneError(err)
}

func shouldRecordAmuxCircuitFailure(err error) bool {
	if !shouldRecordCircuitFailure(err) {
		return false
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if amuxPaneNotFoundError(message) {
		return false
	}
	return amuxInfrastructureError(message)
}

func amuxPaneNotFoundError(message string) bool {
	switch {
	case strings.Contains(message, `pane "`):
		return strings.Contains(message, `" not found`)
	case strings.Contains(message, "pane not found"):
		return true
	default:
		return false
	}
}

func amuxInfrastructureError(message string) bool {
	switch {
	case strings.Contains(message, "connection refused"):
		return true
	case strings.Contains(message, "socket not found"):
		return true
	case strings.Contains(message, "dial unix"):
		return true
	case strings.Contains(message, "i/o timeout"):
		return true
	case strings.Contains(message, "connect: no such file or directory"):
		return true
	case strings.Contains(message, "timed out") && (strings.Contains(message, "connect") || strings.Contains(message, "dial")):
		return true
	case strings.Contains(message, "deadline exceeded") && (strings.Contains(message, "connect") || strings.Contains(message, "dial")):
		return true
	default:
		return false
	}
}

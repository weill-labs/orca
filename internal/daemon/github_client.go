package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	defaultGitHubAPIMinInterval    = 500 * time.Millisecond
	defaultGitHubAPIInitialBackoff = 1 * time.Second
	defaultGitHubAPIMaxBackoff     = 8 * time.Second
	defaultGitHubAPIMaxAttempts    = 3
)

type gitHubClient interface {
	lookupPRNumber(ctx context.Context, branch string) (int, error)
	lookupOpenPRNumber(ctx context.Context, branch string) (int, error)
	isPRMerged(ctx context.Context, prNumber int) (bool, error)
	lookupPRReviews(ctx context.Context, prNumber int) (prReviewPayload, bool, error)
}

type gitHubCLIClientConfig struct {
	project        string
	commands       CommandRunner
	now            func() time.Time
	sleep          func(context.Context, time.Duration) error
	minInterval    time.Duration
	initialBackoff time.Duration
	maxBackoff     time.Duration
	maxAttempts    int
}

type gitHubCLIClient struct {
	project        string
	commands       CommandRunner
	now            func() time.Time
	sleep          func(context.Context, time.Duration) error
	minInterval    time.Duration
	initialBackoff time.Duration
	maxBackoff     time.Duration
	maxAttempts    int

	mu          sync.Mutex
	nextAllowed time.Time
}

func newGitHubCLIClient(cfg gitHubCLIClientConfig) *gitHubCLIClient {
	if cfg.now == nil {
		cfg.now = time.Now
	}
	if cfg.sleep == nil {
		cfg.sleep = sleepContext
	}
	if cfg.maxAttempts < 1 {
		cfg.maxAttempts = 1
	}

	return &gitHubCLIClient{
		project:        cfg.project,
		commands:       cfg.commands,
		now:            cfg.now,
		sleep:          cfg.sleep,
		minInterval:    cfg.minInterval,
		initialBackoff: cfg.initialBackoff,
		maxBackoff:     cfg.maxBackoff,
		maxAttempts:    cfg.maxAttempts,
	}
}

func newDefaultGitHubClient(project string, commands CommandRunner) gitHubClient {
	return newGitHubCLIClient(gitHubCLIClientConfig{
		project:        project,
		commands:       commands,
		now:            time.Now,
		sleep:          sleepContext,
		minInterval:    defaultGitHubAPIMinInterval,
		initialBackoff: defaultGitHubAPIInitialBackoff,
		maxBackoff:     defaultGitHubAPIMaxBackoff,
		maxAttempts:    defaultGitHubAPIMaxAttempts,
	})
}

func (d *Daemon) githubForProject(projectPath string) gitHubClient {
	if d.github != nil && d.project == projectPath {
		return d.github
	}
	return newDefaultGitHubClient(projectPath, d.commands)
}

func (c *gitHubCLIClient) lookupPRNumber(ctx context.Context, branch string) (int, error) {
	output, err := c.run(ctx, "pr", "list", "--head", branch, "--json", "number")
	if err != nil {
		return 0, err
	}
	return parsePRNumberList(output)
}

func (c *gitHubCLIClient) lookupOpenPRNumber(ctx context.Context, branch string) (int, error) {
	output, err := c.run(ctx, "pr", "list", "--head", branch, "--state", "open", "--json", "number")
	if err != nil {
		return 0, err
	}
	return parsePRNumberList(output)
}

func parsePRNumberList(output []byte) (int, error) {
	if len(output) == 0 {
		return 0, nil
	}

	var prs []struct {
		Number int `json:"number"`
	}
	if err := json.Unmarshal(output, &prs); err != nil {
		return 0, err
	}
	if len(prs) == 0 {
		return 0, nil
	}
	return prs[0].Number, nil
}

func (c *gitHubCLIClient) isPRMerged(ctx context.Context, prNumber int) (bool, error) {
	output, err := c.run(ctx, "pr", "view", fmt.Sprintf("%d", prNumber), "--json", "mergedAt")
	if err != nil {
		return false, err
	}
	if len(output) == 0 {
		return false, nil
	}

	var payload struct {
		MergedAt *string `json:"mergedAt"`
	}
	if err := json.Unmarshal(output, &payload); err != nil {
		return false, err
	}
	return payload.MergedAt != nil && *payload.MergedAt != "", nil
}

func (c *gitHubCLIClient) lookupPRReviews(ctx context.Context, prNumber int) (prReviewPayload, bool, error) {
	output, err := c.run(ctx, "pr", "view", fmt.Sprintf("%d", prNumber), "--json", "reviews,reviewDecision,comments")
	if err != nil {
		return prReviewPayload{}, false, err
	}
	if len(output) == 0 {
		return prReviewPayload{}, false, nil
	}

	var payload prReviewPayload
	if err := json.Unmarshal(output, &payload); err != nil {
		return prReviewPayload{}, false, err
	}
	return payload, true, nil
}

func (c *gitHubCLIClient) run(ctx context.Context, args ...string) ([]byte, error) {
	backoff := c.initialBackoff
	var (
		output []byte
		err    error
	)

	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		if err := c.waitTurn(ctx); err != nil {
			return nil, err
		}

		output, err = c.commands.Run(ctx, c.project, "gh", args...)
		if err == nil {
			return output, nil
		}
		if !isGitHubRateLimitError(err, output) || attempt == c.maxAttempts {
			return output, err
		}
		if sleepErr := c.sleep(ctx, backoff); sleepErr != nil {
			return nil, sleepErr
		}
		backoff = nextBackoff(backoff, c.maxBackoff)
	}

	return output, err
}

func (c *gitHubCLIClient) waitTurn(ctx context.Context) error {
	if c.minInterval <= 0 {
		return nil
	}

	c.mu.Lock()
	now := c.now()
	wait := time.Duration(0)
	if !c.nextAllowed.IsZero() && now.Before(c.nextAllowed) {
		wait = c.nextAllowed.Sub(now)
		now = c.nextAllowed
	}
	c.nextAllowed = now.Add(c.minInterval)
	c.mu.Unlock()

	if wait <= 0 {
		return nil
	}
	return c.sleep(ctx, wait)
}

func nextBackoff(current, max time.Duration) time.Duration {
	if current <= 0 {
		return 0
	}

	next := current * 2
	if max > 0 && next > max {
		return max
	}
	return next
}

func isGitHubRateLimitError(err error, output []byte) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error() + " " + string(output))
	return strings.Contains(message, "secondary rate limit") ||
		strings.Contains(message, "api rate limit exceeded") ||
		strings.Contains(message, "rate limit exceeded")
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

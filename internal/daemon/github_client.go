package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultGitHubAPIMinInterval    = 500 * time.Millisecond
	defaultGitHubAPIInitialBackoff = 1 * time.Second
	defaultGitHubAPIMaxBackoff     = 8 * time.Second
	defaultGitHubAPIMaxAttempts    = 3
	issueIDPRSearchJSONFields      = "number,state,headRefName,title"
)

var (
	retryAfterPattern     = regexp.MustCompile(`(?im)retry-after:\s*(\d+)`)
	rateLimitResetPattern = regexp.MustCompile(`(?im)x-ratelimit-reset:\s*(\d+)`)
)

type gitHubClient interface {
	lookupPRNumber(ctx context.Context, branch string) (int, error)
	findPRByIssueID(ctx context.Context, issueID string) (int, string, error)
	lookupOpenPRNumber(ctx context.Context, branch string) (int, error)
	lookupOpenOrMergedPRNumber(ctx context.Context, branch string) (int, bool, error)
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
	logf           func(string, ...any)
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
	logf           func(string, ...any)

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
		logf:           cfg.logf,
	}
}

func newDefaultGitHubClient(project string, commands CommandRunner, logf func(string, ...any)) gitHubClient {
	return newGitHubCLIClient(gitHubCLIClientConfig{
		project:        project,
		commands:       commands,
		now:            time.Now,
		sleep:          sleepContext,
		minInterval:    defaultGitHubAPIMinInterval,
		initialBackoff: defaultGitHubAPIInitialBackoff,
		maxBackoff:     defaultGitHubAPIMaxBackoff,
		maxAttempts:    defaultGitHubAPIMaxAttempts,
		logf:           logf,
	})
}

func (d *Daemon) githubForProject(projectPath string) gitHubClient {
	if d.github != nil && d.project == projectPath {
		return d.github
	}

	d.githubMu.Lock()
	defer d.githubMu.Unlock()

	if d.githubClients == nil {
		d.githubClients = make(map[string]gitHubClient)
	}
	if client, ok := d.githubClients[projectPath]; ok {
		return client
	}

	client := d.newGitHubClient(projectPath)
	d.githubClients[projectPath] = client
	return client
}

func (d *Daemon) newGitHubClient(projectPath string) gitHubClient {
	base, ok := d.github.(*gitHubCLIClient)
	if !ok {
		return newDefaultGitHubClient(projectPath, d.commands, d.logf)
	}

	return newGitHubCLIClient(gitHubCLIClientConfig{
		project:        projectPath,
		commands:       d.commands,
		now:            base.now,
		sleep:          base.sleep,
		minInterval:    base.minInterval,
		initialBackoff: base.initialBackoff,
		maxBackoff:     base.maxBackoff,
		maxAttempts:    base.maxAttempts,
		logf:           d.logf,
	})
}

func (c *gitHubCLIClient) lookupPRNumber(ctx context.Context, branch string) (int, error) {
	output, err := c.run(ctx, "pr", "list", "--head", branch, "--json", "number")
	if err != nil {
		return 0, err
	}
	return parsePRNumberList(output)
}

func (c *gitHubCLIClient) findPRByIssueID(ctx context.Context, issueID string) (int, string, error) {
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return 0, "", nil
	}

	output, err := c.run(ctx, issueIDPRSearchArgs(issueID)...)
	if err != nil {
		return 0, "", err
	}

	number, branch, multiple, err := parseIssueIDPRSearchResults(output, issueID)
	if err != nil {
		return 0, "", err
	}
	if multiple {
		if c.logf != nil {
			c.logf("github issue-id PR search for %s returned multiple pull requests; leaving task unbound", issueID)
		}
		return 0, "", nil
	}
	return number, branch, nil
}

func issueIDPRSearchArgs(issueID string) []string {
	return []string{
		"pr",
		"list",
		"--search", issueID + " in:title",
		"--state", "all",
		"--json", issueIDPRSearchJSONFields,
		"--limit", "5",
	}
}

func (c *gitHubCLIClient) lookupOpenPRNumber(ctx context.Context, branch string) (int, error) {
	output, err := c.run(ctx, "pr", "list", "--head", branch, "--state", "open", "--json", "number")
	if err != nil {
		return 0, err
	}
	return parsePRNumberList(output)
}

func (c *gitHubCLIClient) lookupOpenOrMergedPRNumber(ctx context.Context, branch string) (int, bool, error) {
	output, err := c.run(ctx, "pr", "list", "--head", branch, "--state", "all", "--json", "number,state")
	if err != nil {
		return 0, false, err
	}
	return parseOpenOrMergedPRNumberList(output)
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

func parseOpenOrMergedPRNumberList(output []byte) (int, bool, error) {
	if len(output) == 0 {
		return 0, false, nil
	}

	var prs []struct {
		Number int    `json:"number"`
		State  string `json:"state"`
	}
	if err := json.Unmarshal(output, &prs); err != nil {
		return 0, false, err
	}
	for _, pr := range prs {
		switch {
		case strings.EqualFold(pr.State, "merged"):
			return pr.Number, true, nil
		case strings.EqualFold(pr.State, "open"):
			return pr.Number, false, nil
		}
	}
	return 0, false, nil
}

func parseIssueIDPRSearchResults(output []byte, issueID string) (int, string, bool, error) {
	if len(output) == 0 {
		return 0, "", false, nil
	}

	var prs []struct {
		Number      int    `json:"number"`
		State       string `json:"state"`
		HeadRefName string `json:"headRefName"`
		Title       string `json:"title"`
	}
	if err := json.Unmarshal(output, &prs); err != nil {
		return 0, "", false, err
	}

	candidates := prs[:0]
	for _, pr := range prs {
		if matchesIssueIDSearchResult(pr.Title, pr.HeadRefName, issueID) {
			candidates = append(candidates, pr)
		}
	}

	switch len(candidates) {
	case 0:
		return 0, "", false, nil
	case 1:
		return candidates[0].Number, candidates[0].HeadRefName, false, nil
	default:
		return 0, "", true, nil
	}
}

func matchesIssueIDSearchResult(title, headRefName, issueID string) bool {
	return containsIssueIDToken(title, issueID) || containsIssueIDToken(headRefName, issueID)
}

func containsIssueIDToken(value, issueID string) bool {
	value = strings.ToUpper(strings.TrimSpace(value))
	issueID = strings.ToUpper(strings.TrimSpace(issueID))
	if value == "" || issueID == "" {
		return false
	}

	for start := 0; start < len(value); {
		idx := strings.Index(value[start:], issueID)
		if idx == -1 {
			return false
		}
		idx += start
		end := idx + len(issueID)
		if issueIDTokenBoundary(value, idx-1) && issueIDTokenBoundary(value, end) {
			return true
		}
		start = idx + 1
	}
	return false
}

func issueIDTokenBoundary(value string, idx int) bool {
	if idx < 0 || idx >= len(value) {
		return true
	}

	ch := value[idx]
	return !(ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9')
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
	reviewComments, err := c.lookupPRReviewComments(ctx, prNumber)
	if err != nil {
		return prReviewPayload{}, false, err
	}
	payload.ReviewComments = reviewComments
	return payload, true, nil
}

func (c *gitHubCLIClient) lookupPRReviewComments(ctx context.Context, prNumber int) ([]prReviewComment, error) {
	output, err := c.run(ctx, "api", fmt.Sprintf("repos/{owner}/{repo}/pulls/%d/comments?per_page=100", prNumber))
	if err != nil {
		return nil, err
	}
	if len(output) == 0 {
		return nil, nil
	}

	var comments []prReviewComment
	if err := json.Unmarshal(output, &comments); err != nil {
		return nil, err
	}
	return comments, nil
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
		if !isGitHubRateLimitError(err, output) {
			return output, err
		}
		if attempt == c.maxAttempts {
			return output, &gitHubRateLimitError{
				err:   err,
				until: gitHubRateLimitDeadline(err, output, c.now(), backoff),
			}
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
		strings.Contains(message, "rate limit exceeded") ||
		strings.Contains(message, "http 429") ||
		(strings.Contains(message, "http 403") && (strings.Contains(message, "rate limit") || strings.Contains(message, "retry-after")))
}

func gitHubRateLimitDeadline(err error, output []byte, now time.Time, fallback time.Duration) time.Time {
	message := err.Error() + "\n" + string(output)
	if match := retryAfterPattern.FindStringSubmatch(message); len(match) == 2 {
		if seconds, convErr := strconv.Atoi(match[1]); convErr == nil && seconds > 0 {
			return now.Add(time.Duration(seconds) * time.Second)
		}
	}
	if match := rateLimitResetPattern.FindStringSubmatch(message); len(match) == 2 {
		if unixSeconds, convErr := strconv.ParseInt(match[1], 10, 64); convErr == nil && unixSeconds > 0 {
			return time.Unix(unixSeconds, 0).UTC()
		}
	}
	if fallback <= 0 {
		fallback = time.Minute
	}
	return now.Add(fallback)
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

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
	defaultPRSnapshotTTL           = time.Second
	issueIDPRSearchJSONFields      = "number,state,headRefName,title"
	prSnapshotJSONFields           = "mergedAt,state,closedAt,mergeable,mergeStateStatus,updatedAt,reviews,reviewDecision,comments"
	prReviewJSONFields             = "reviews,reviewDecision,comments,updatedAt"
	prReviewThreadsGraphQLQuery    = `query($owner:String!,$repo:String!,$number:Int!){repository(owner:$owner,name:$repo){pullRequest(number:$number){reviewThreads(first:100){nodes{id isResolved path line comments(first:50){nodes{id body createdAt author{login}}}}}}}}`
)

var (
	retryAfterPattern     = regexp.MustCompile(`(?im)retry-after:\s*(\d+)`)
	rateLimitResetPattern = regexp.MustCompile(`(?im)x-ratelimit-reset:\s*(\d+)`)
)

type gitHubClient interface {
	lookupIssue(ctx context.Context, number int) (gitHubIssue, error)
	lookupPRNumber(ctx context.Context, branch string) (int, error)
	findPRByIssueID(ctx context.Context, issueID string) (int, string, error)
	lookupOpenPRNumber(ctx context.Context, branch string) (int, error)
	lookupOpenOrMergedPRNumber(ctx context.Context, branch string) (int, bool, error)
	lookupPRTerminalState(ctx context.Context, prNumber int) (prTerminalState, error)
	lookupPRMergeability(ctx context.Context, prNumber int) (prMergeabilityPayload, bool, error)
	isPRMerged(ctx context.Context, prNumber int) (bool, error)
	lookupPRReviews(ctx context.Context, prNumber int) (prReviewPayload, bool, error)
	lookupPRReviewComments(ctx context.Context, prNumber int) ([]prReviewComment, error)
	lookupPRReviewThreads(ctx context.Context, prNumber int) ([]prReviewThread, error)
}

type prTerminalState struct {
	merged             bool
	closedWithoutMerge bool
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
	prSnapshotTTL  time.Duration
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
	prSnapshotTTL  time.Duration
	logf           func(string, ...any)

	mu              sync.Mutex
	nextAllowed     time.Time
	prSnapshotCache map[int]cachedPRSnapshot
}

type gitHubIssue struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

type cachedPRSnapshot struct {
	payload   prSnapshotPayload
	expiresAt time.Time
}

type prSnapshotPayload struct {
	MergedAt         *string      `json:"mergedAt"`
	State            *string      `json:"state"`
	ClosedAt         *string      `json:"closedAt"`
	Mergeable        *string      `json:"mergeable"`
	MergeStateStatus *string      `json:"mergeStateStatus"`
	UpdatedAt        *time.Time   `json:"updatedAt"`
	ReviewDecision   *string      `json:"reviewDecision"`
	Reviews          *[]prReview  `json:"reviews"`
	Comments         *[]prComment `json:"comments"`
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
	if cfg.prSnapshotTTL <= 0 {
		cfg.prSnapshotTTL = defaultPRSnapshotTTL
	}

	return &gitHubCLIClient{
		project:         cfg.project,
		commands:        cfg.commands,
		now:             cfg.now,
		sleep:           cfg.sleep,
		minInterval:     cfg.minInterval,
		initialBackoff:  cfg.initialBackoff,
		maxBackoff:      cfg.maxBackoff,
		maxAttempts:     cfg.maxAttempts,
		prSnapshotTTL:   cfg.prSnapshotTTL,
		logf:            cfg.logf,
		prSnapshotCache: make(map[int]cachedPRSnapshot),
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
		prSnapshotTTL:  defaultPRSnapshotTTL,
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
		prSnapshotTTL:  base.prSnapshotTTL,
		logf:           d.logf,
	})
}

func (c *gitHubCLIClient) lookupIssue(ctx context.Context, number int) (gitHubIssue, error) {
	output, err := c.run(ctx, "issue", "view", fmt.Sprintf("%d", number), "--json", "title,body")
	if err != nil {
		return gitHubIssue{}, err
	}
	if len(output) == 0 {
		return gitHubIssue{}, nil
	}

	var issue gitHubIssue
	if err := json.Unmarshal(output, &issue); err != nil {
		return gitHubIssue{}, err
	}
	issue.Title = strings.TrimSpace(issue.Title)
	issue.Body = strings.TrimSpace(issue.Body)
	return issue, nil
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
	if payload, ok := c.cachedPRSnapshot(prNumber); ok {
		return terminalStateFromSnapshot(payload).merged, nil
	}

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

func (c *gitHubCLIClient) lookupPRTerminalState(ctx context.Context, prNumber int) (prTerminalState, error) {
	payload, ok, err := c.lookupPRSnapshot(ctx, prNumber)
	if err != nil {
		return prTerminalState{}, err
	}
	if !ok {
		return prTerminalState{}, nil
	}
	return terminalStateFromSnapshot(payload), nil
}

func (c *gitHubCLIClient) lookupPRMergeability(ctx context.Context, prNumber int) (prMergeabilityPayload, bool, error) {
	if payload, ok := c.cachedPRSnapshot(prNumber); ok {
		if mergeability, available := mergeabilityFromSnapshot(payload); available {
			return mergeability, true, nil
		}
	}

	output, err := c.run(ctx, "pr", "view", fmt.Sprintf("%d", prNumber), "--json", prMergeableJSONFields)
	if err != nil {
		return prMergeabilityPayload{}, false, err
	}
	if len(output) == 0 {
		return prMergeabilityPayload{}, false, nil
	}

	var payload prMergeabilityPayload
	if err := json.Unmarshal(output, &payload); err != nil {
		return prMergeabilityPayload{}, false, err
	}

	payload.Mergeable = strings.TrimSpace(payload.Mergeable)
	payload.MergeStateStatus = strings.TrimSpace(payload.MergeStateStatus)
	if payload.Mergeable == "" && payload.MergeStateStatus == "" {
		return prMergeabilityPayload{}, false, nil
	}
	return payload, true, nil
}

func (c *gitHubCLIClient) lookupPRReviews(ctx context.Context, prNumber int) (prReviewPayload, bool, error) {
	if payload, ok := c.cachedPRSnapshot(prNumber); ok {
		if reviews, available := reviewPayloadFromSnapshot(payload); available {
			return reviews, true, nil
		}
	}

	output, err := c.run(ctx, "pr", "view", fmt.Sprintf("%d", prNumber), "--json", prReviewJSONFields)
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
	payload.ReviewDecision = strings.TrimSpace(payload.ReviewDecision)
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

func (c *gitHubCLIClient) lookupPRReviewThreads(ctx context.Context, prNumber int) ([]prReviewThread, error) {
	output, err := c.run(ctx, prReviewThreadsGraphQLArgs(prNumber)...)
	if err != nil {
		return nil, err
	}
	if len(output) == 0 {
		return nil, nil
	}

	var payload prReviewThreadsGraphQLPayload
	if err := json.Unmarshal(output, &payload); err != nil {
		return nil, err
	}
	if payload.Data.Repository.PullRequest == nil {
		return nil, nil
	}

	nodes := payload.Data.Repository.PullRequest.ReviewThreads.Nodes
	threads := make([]prReviewThread, 0, len(nodes))
	for _, node := range nodes {
		thread := prReviewThread{
			ID:         strings.TrimSpace(node.ID),
			IsResolved: node.IsResolved,
			Path:       strings.TrimSpace(node.Path),
			Line:       node.Line,
			Comments:   make([]prReviewThreadComment, 0, len(node.Comments.Nodes)),
		}
		for _, comment := range node.Comments.Nodes {
			thread.Comments = append(thread.Comments, prReviewThreadComment{
				ID:        strings.TrimSpace(comment.ID),
				Body:      comment.Body,
				Author:    strings.TrimSpace(comment.Author.Login),
				CreatedAt: comment.CreatedAt,
			})
		}
		threads = append(threads, thread)
	}
	return threads, nil
}

func prReviewThreadsGraphQLArgs(prNumber int) []string {
	return []string{
		"api",
		"graphql",
		"-F", "owner={owner}",
		"-F", "repo={repo}",
		"-F", fmt.Sprintf("number=%d", prNumber),
		"-f", "query=" + prReviewThreadsGraphQLQuery,
	}
}

func (c *gitHubCLIClient) lookupPRSnapshot(ctx context.Context, prNumber int) (prSnapshotPayload, bool, error) {
	if payload, ok := c.cachedPRSnapshot(prNumber); ok {
		return payload, true, nil
	}

	output, err := c.run(ctx, "pr", "view", fmt.Sprintf("%d", prNumber), "--json", prSnapshotJSONFields)
	if err != nil {
		return prSnapshotPayload{}, false, err
	}
	if len(output) == 0 {
		return prSnapshotPayload{}, false, nil
	}

	var payload prSnapshotPayload
	if err := json.Unmarshal(output, &payload); err != nil {
		return prSnapshotPayload{}, false, err
	}
	normalizePRSnapshot(&payload)
	c.storePRSnapshot(prNumber, payload)
	return payload, true, nil
}

func (c *gitHubCLIClient) cachedPRSnapshot(prNumber int) (prSnapshotPayload, bool) {
	if prNumber <= 0 {
		return prSnapshotPayload{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.prSnapshotCache[prNumber]
	if !ok {
		return prSnapshotPayload{}, false
	}
	if now := c.now(); !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
		delete(c.prSnapshotCache, prNumber)
		return prSnapshotPayload{}, false
	}
	return entry.payload, true
}

func (c *gitHubCLIClient) storePRSnapshot(prNumber int, payload prSnapshotPayload) {
	if prNumber <= 0 || c.prSnapshotTTL <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.prSnapshotCache == nil {
		c.prSnapshotCache = make(map[int]cachedPRSnapshot)
	}
	c.prSnapshotCache[prNumber] = cachedPRSnapshot{
		payload:   payload,
		expiresAt: c.now().Add(c.prSnapshotTTL),
	}
}

func normalizePRSnapshot(payload *prSnapshotPayload) {
	if payload == nil {
		return
	}

	payload.State = trimOptionalString(payload.State)
	payload.Mergeable = trimOptionalString(payload.Mergeable)
	payload.MergeStateStatus = trimOptionalString(payload.MergeStateStatus)
	payload.ReviewDecision = trimOptionalString(payload.ReviewDecision)
}

func trimOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	return &trimmed
}

func terminalStateFromSnapshot(payload prSnapshotPayload) prTerminalState {
	state := ""
	if payload.State != nil {
		state = *payload.State
	}

	merged := strings.EqualFold(state, "merged") || (payload.MergedAt != nil && strings.TrimSpace(*payload.MergedAt) != "")
	closed := strings.EqualFold(state, "closed") || (payload.ClosedAt != nil && strings.TrimSpace(*payload.ClosedAt) != "")
	return prTerminalState{
		merged:             merged,
		closedWithoutMerge: closed && !merged,
	}
}

func mergeabilityFromSnapshot(payload prSnapshotPayload) (prMergeabilityPayload, bool) {
	mergeable := ""
	if payload.Mergeable != nil {
		mergeable = *payload.Mergeable
	}
	stateStatus := ""
	if payload.MergeStateStatus != nil {
		stateStatus = *payload.MergeStateStatus
	}
	if mergeable == "" && stateStatus == "" {
		return prMergeabilityPayload{}, false
	}
	return prMergeabilityPayload{
		Mergeable:        mergeable,
		MergeStateStatus: stateStatus,
	}, true
}

func reviewPayloadFromSnapshot(payload prSnapshotPayload) (prReviewPayload, bool) {
	if payload.ReviewDecision == nil && payload.Reviews == nil && payload.Comments == nil && payload.UpdatedAt == nil {
		return prReviewPayload{}, false
	}

	reviewPayload := prReviewPayload{}
	if payload.ReviewDecision != nil {
		reviewPayload.ReviewDecision = *payload.ReviewDecision
	}
	if payload.Reviews != nil {
		reviewPayload.Reviews = append([]prReview(nil), (*payload.Reviews)...)
	}
	if payload.Comments != nil {
		reviewPayload.Comments = append([]prComment(nil), (*payload.Comments)...)
	}
	if payload.UpdatedAt != nil {
		reviewPayload.UpdatedAt = *payload.UpdatedAt
	}
	return reviewPayload, true
}

func (c *gitHubCLIClient) run(ctx context.Context, args ...string) ([]byte, error) {
	done := pollTickGitHubCall(ctx)
	defer done()

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
		strings.Contains(message, "api rate limit already exceeded") ||
		strings.Contains(message, "api rate limit exceeded") ||
		strings.Contains(message, "rate limit already exceeded") ||
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

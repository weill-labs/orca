package daemon

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

func (a *mergeQueueActor) landDirect(ctx context.Context, entry MergeQueueEntry) directLandingOutcome {
	baseBranch := directLandingBaseBranch(entry)
	branch := directLandingBranch(entry)
	clonePath := strings.TrimSpace(entry.ClonePath)
	if clonePath == "" {
		return directLandingOutcome{err: errors.New("direct landing entry missing clone path"), failedAction: "load worker clone"}
	}
	commands := []struct {
		action string
		args   []string
	}{
		{action: "git fetch origin " + baseBranch, args: []string{"fetch", "origin", baseBranch}},
		{action: "git checkout " + branch, args: []string{"checkout", branch}},
		{action: "git rebase origin/" + baseBranch, args: []string{"rebase", "origin/" + baseBranch}},
	}

	for _, command := range commands {
		if _, err := a.commands.Run(ctx, clonePath, "git", command.args...); err != nil {
			outcome := directLandingOutcome{err: err, failedAction: command.action}
			if strings.HasPrefix(command.action, "git rebase ") {
				outcome.conflictedFiles = a.conflictedFiles(ctx, clonePath)
				if len(outcome.conflictedFiles) > 0 {
					outcome.conflict = true
					outcome.conflictStatePreserved = true
				}
			}
			return outcome
		}
	}

	if gate := strings.TrimSpace(entry.QualityGate); gate != "" {
		if _, err := a.commands.Run(ctx, clonePath, "sh", "-c", gate); err != nil {
			return directLandingOutcome{err: err, failedAction: "quality gate: " + gate}
		}
	}

	baseCommit, err := a.gitTrim(ctx, clonePath, "rev-parse", "origin/"+baseBranch)
	if err != nil {
		return directLandingOutcome{err: err, failedAction: "git rev-parse origin/" + baseBranch}
	}
	if localOrigin, ok := a.localWorktreeOrigin(ctx, entry, clonePath); ok {
		return a.landLocalWorktreeOrigin(ctx, localOrigin, clonePath, baseBranch, baseCommit)
	}

	pushAction := "git push origin HEAD:" + baseBranch
	if _, err := a.commands.Run(ctx, clonePath, "git", "push", "origin", "HEAD:"+baseBranch); err != nil {
		return directLandingOutcome{err: err, failedAction: pushAction}
	}
	return directLandingOutcome{}
}

func (a *mergeQueueActor) landLocalWorktreeOrigin(ctx context.Context, projectPath, clonePath, baseBranch, expectedBase string) directLandingOutcome {
	if err := a.verifyLocalOriginReady(ctx, projectPath, baseBranch); err != nil {
		return directLandingOutcome{err: err, failedAction: "verify local origin worktree"}
	}
	if outcome, ok := a.localOriginAdvancedOutcome(ctx, projectPath, baseBranch, expectedBase); ok {
		return outcome
	}

	if _, err := a.commands.Run(ctx, projectPath, "git", "fetch", clonePath, "HEAD"); err != nil {
		return directLandingOutcome{err: err, failedAction: "git fetch worker clone HEAD"}
	}
	if outcome, ok := a.localOriginAdvancedOutcome(ctx, projectPath, baseBranch, expectedBase); ok {
		return outcome
	}

	if _, err := a.commands.Run(ctx, projectPath, "git", "merge", "--ff-only", "FETCH_HEAD"); err != nil {
		a.restoreLocalOriginHead(ctx, projectPath)
		if outcome, ok := a.localOriginAdvancedOutcome(ctx, projectPath, baseBranch, expectedBase); ok {
			return outcome
		}
		return directLandingOutcome{err: err, failedAction: "git merge --ff-only FETCH_HEAD"}
	}
	if dirty, err := a.gitTrackedDirty(ctx, projectPath); err != nil {
		return directLandingOutcome{err: err, failedAction: "git status local origin"}
	} else if dirty {
		a.restoreLocalOriginHead(ctx, projectPath)
		return directLandingOutcome{err: errors.New("local origin worktree dirty after landing"), failedAction: "verify local origin worktree"}
	}
	return directLandingOutcome{}
}

func (a *mergeQueueActor) verifyLocalOriginReady(ctx context.Context, projectPath, baseBranch string) error {
	currentBranch, err := a.gitTrim(ctx, projectPath, "symbolic-ref", "--short", "HEAD")
	if err != nil {
		return err
	}
	if currentBranch != baseBranch {
		return fmt.Errorf("local origin is on %s, want %s", currentBranch, baseBranch)
	}
	if dirty, err := a.gitTrackedDirty(ctx, projectPath); err != nil {
		return err
	} else if dirty {
		return errors.New("local origin has tracked worktree changes")
	}
	return nil
}

func (a *mergeQueueActor) localOriginAdvancedOutcome(ctx context.Context, projectPath, baseBranch, expectedBase string) (directLandingOutcome, bool) {
	currentBase, err := a.gitTrim(ctx, projectPath, "rev-parse", "HEAD")
	if err != nil {
		return directLandingOutcome{err: err, failedAction: "git rev-parse local origin HEAD"}, true
	}
	if currentBase == strings.TrimSpace(expectedBase) {
		return directLandingOutcome{}, false
	}
	return directLandingOutcome{
		err:          fmt.Errorf("expected %s at %s, found %s", baseBranch, shortGitCommit(expectedBase), shortGitCommit(currentBase)),
		failedAction: "local origin " + baseBranch + " advanced",
	}, true
}

func (a *mergeQueueActor) restoreLocalOriginHead(ctx context.Context, projectPath string) {
	dirty, err := a.gitTrackedDirty(ctx, projectPath)
	if err != nil || !dirty {
		return
	}
	_, _ = a.commands.Run(ctx, projectPath, "git", "reset", "--hard", "HEAD")
}

func (a *mergeQueueActor) gitTrackedDirty(ctx context.Context, projectPath string) (bool, error) {
	out, err := a.commands.Run(ctx, projectPath, "git", "status", "--short", "--untracked-files=no")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(out)) != "", nil
}

func (a *mergeQueueActor) localWorktreeOrigin(ctx context.Context, entry MergeQueueEntry, clonePath string) (string, bool) {
	projectPath := strings.TrimSpace(entry.Project)
	if projectPath == "" {
		return "", false
	}
	remoteURL, err := a.gitTrim(ctx, clonePath, "remote", "get-url", "origin")
	if err != nil {
		return "", false
	}
	remotePath, ok := localGitRemotePath(remoteURL, clonePath)
	if !ok || !sameFilesystemPath(projectPath, remotePath) {
		return "", false
	}
	bare, err := a.gitTrim(ctx, projectPath, "rev-parse", "--is-bare-repository")
	if err != nil {
		return "", false
	}
	if strings.EqualFold(bare, "true") {
		return "", false
	}
	return projectPath, true
}

func localGitRemotePath(remoteURL, relativeTo string) (string, bool) {
	remoteURL = strings.TrimSpace(remoteURL)
	if remoteURL == "" {
		return "", false
	}
	if parsed, err := url.Parse(remoteURL); err == nil && parsed.Scheme != "" {
		if !strings.EqualFold(parsed.Scheme, "file") {
			return "", false
		}
		return filepath.FromSlash(parsed.Path), true
	}
	if looksLikeSCPRemote(remoteURL) {
		return "", false
	}
	if filepath.IsAbs(remoteURL) {
		return filepath.Clean(remoteURL), true
	}
	return filepath.Clean(filepath.Join(relativeTo, remoteURL)), true
}

func looksLikeSCPRemote(remoteURL string) bool {
	colon := strings.Index(remoteURL, ":")
	if colon < 0 {
		return false
	}
	slash := strings.IndexAny(remoteURL, `/\`)
	return slash < 0 || colon < slash
}

func sameFilesystemPath(left, right string) bool {
	left = canonicalPath(left)
	right = canonicalPath(right)
	return left != "" && right != "" && left == right
}

func canonicalPath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return ""
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	return filepath.Clean(abs)
}

func (a *mergeQueueActor) gitTrim(ctx context.Context, dir string, args ...string) (string, error) {
	out, err := a.commands.Run(ctx, dir, "git", args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (a *mergeQueueActor) conflictedFiles(ctx context.Context, projectPath string) []string {
	out, err := a.commands.Run(ctx, projectPath, "git", "diff", "--name-only", "--diff-filter=U")
	if err != nil {
		return nil
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	files := make([]string, 0, len(lines))
	for _, line := range lines {
		if file := strings.TrimSpace(line); file != "" {
			files = append(files, file)
		}
	}
	return files
}

func shortGitCommit(commit string) string {
	commit = strings.TrimSpace(commit)
	if len(commit) <= 12 {
		return commit
	}
	return commit[:12]
}

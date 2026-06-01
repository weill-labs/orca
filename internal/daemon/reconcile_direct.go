package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
)

func (d *Daemon) directLandingReconcilePRInfo(ctx context.Context, task Task, info reconcilePRInfo) (reconcilePRInfo, bool, error) {
	cfg, err := d.landingConfigForProject(task.Project)
	if err != nil {
		return reconcilePRInfo{}, false, err
	}
	if !cfg.directMode() || task.PRNumber > 0 {
		return info, false, nil
	}

	branch := strings.TrimSpace(task.Branch)
	if branch == "" {
		branch = strings.TrimSpace(task.Issue)
	}
	if branch == "" {
		return info, false, nil
	}
	info.branch = branch

	baseBranch := strings.TrimSpace(cfg.BaseBranch)
	if baseBranch == "" {
		baseBranch = defaultLandingConfig().BaseBranch
	}
	merged, checked := d.localBaseContainsDirectBranch(ctx, task, baseBranch, branch)
	if !checked {
		return info, false, nil
	}
	if merged {
		info.state = reconcilePRStateMerged
		return info, true, nil
	}
	return info, false, nil
}

func (d *Daemon) localBaseContainsDirectBranch(ctx context.Context, task Task, baseBranch, branch string) (bool, bool) {
	projectPath := strings.TrimSpace(task.Project)
	if looksLikeGitWorktree(projectPath) {
		if merged, checked := d.gitContainsRef(ctx, projectPath, "refs/heads/"+baseBranch, "refs/heads/"+branch); checked {
			return merged, true
		}
	}

	clonePath := strings.TrimSpace(task.ClonePath)
	if looksLikeGitWorktree(clonePath) {
		baseRef := "refs/remotes/origin/" + baseBranch
		branchRef := "refs/remotes/origin/" + branch
		if merged, checked := d.gitContainsRef(ctx, clonePath, baseRef, branchRef); checked {
			return merged, true
		}
	}

	return false, false
}

func (d *Daemon) gitContainsRef(ctx context.Context, repoPath, baseRef, branchRef string) (bool, bool) {
	if _, err := d.commands.Run(ctx, repoPath, "git", "rev-parse", "--verify", baseRef); err != nil {
		return false, false
	}
	if _, err := d.commands.Run(ctx, repoPath, "git", "rev-parse", "--verify", branchRef); err != nil {
		return false, false
	}
	if _, err := d.commands.Run(ctx, repoPath, "git", "merge-base", "--is-ancestor", branchRef, baseRef); err != nil {
		return false, true
	}
	return true, true
}

func looksLikeGitWorktree(path string) bool {
	path = strings.TrimSpace(path)
	if path == "" {
		return false
	}
	if _, err := os.Stat(filepath.Join(path, ".git")); err == nil {
		return true
	}
	return false
}

func isNoGitHubRemoteError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, part := range []string{
		"not a git repository",
		"no git remotes",
		"no remotes configured",
		"none of the git remotes configured",
		"could not determine base repo",
	} {
		if strings.Contains(message, part) {
			return true
		}
	}
	return false
}

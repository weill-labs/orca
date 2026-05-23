package daemon

import (
	"context"
	"net/url"
	"sort"
	"strings"
)

type prTerminalStateBatchContextKey struct{}

type prTerminalStateBatchLookup interface {
	lookupPRTerminalStates(context.Context, []githubPRTerminalStateRef) (map[prTerminalStateKey]prTerminalState, error)
}

func (d *Daemon) withBatchedPRTerminalStates(ctx context.Context, assignments []ActiveAssignment) context.Context {
	states, err := d.lookupBatchedPRTerminalStates(ctx, assignments)
	if err != nil {
		if d.logf != nil {
			d.logf("github graphql PR terminal state batch failed; falling back to per-PR polling: %v", err)
		}
		return ctx
	}
	if len(states) == 0 {
		return ctx
	}
	return context.WithValue(ctx, prTerminalStateBatchContextKey{}, states)
}

func (d *Daemon) lookupBatchedPRTerminalStates(ctx context.Context, assignments []ActiveAssignment) (map[prTerminalStateKey]prTerminalState, error) {
	refs, err := d.prTerminalStateRefs(assignments)
	if err != nil || len(refs) == 0 {
		return nil, err
	}

	client, ok := d.gitHubClientForContext(ctx, d.project).(prTerminalStateBatchLookup)
	if !ok {
		return nil, nil
	}
	return client.lookupPRTerminalStates(ctx, refs)
}

func (d *Daemon) prTerminalStateRefs(assignments []ActiveAssignment) ([]githubPRTerminalStateRef, error) {
	if d.detectOrigin == nil {
		return nil, nil
	}

	type githubRepo struct {
		owner string
		name  string
	}
	repos := make(map[string]githubRepo)
	skippedProjects := make(map[string]struct{})
	seen := make(map[prTerminalStateKey]struct{}, len(assignments))
	refs := make([]githubPRTerminalStateRef, 0, len(assignments))

	for _, active := range assignments {
		task := active.Task
		if task.PRNumber <= 0 || !taskStateRunsTerminalPoll(normalizeTaskState(task)) {
			continue
		}

		projectPath := prProjectForTask(task)
		if projectPath == "" {
			continue
		}
		key := prTerminalStateKey{Project: projectPath, PRNumber: task.PRNumber}
		if _, ok := seen[key]; ok {
			continue
		}
		if _, ok := skippedProjects[projectPath]; ok {
			continue
		}

		repo, ok := repos[projectPath]
		if !ok {
			origin, err := d.detectOrigin(projectPath)
			if err != nil {
				d.logPRTerminalStateBatchSkip("detect origin for %s: %v", projectPath, err)
				skippedProjects[projectPath] = struct{}{}
				continue
			}
			owner, name, ok := githubOwnerRepoFromOrigin(origin)
			if !ok {
				d.logPRTerminalStateBatchSkip("origin for %s is not a GitHub repository: %s", projectPath, origin)
				skippedProjects[projectPath] = struct{}{}
				continue
			}
			repo = githubRepo{owner: owner, name: name}
			repos[projectPath] = repo
		}

		seen[key] = struct{}{}
		refs = append(refs, githubPRTerminalStateRef{
			Key:      key,
			Owner:    repo.owner,
			Repo:     repo.name,
			PRNumber: task.PRNumber,
		})
	}

	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Owner != refs[j].Owner {
			return refs[i].Owner < refs[j].Owner
		}
		if refs[i].Repo != refs[j].Repo {
			return refs[i].Repo < refs[j].Repo
		}
		if refs[i].PRNumber != refs[j].PRNumber {
			return refs[i].PRNumber < refs[j].PRNumber
		}
		return refs[i].Key.Project < refs[j].Key.Project
	})
	return refs, nil
}

func (d *Daemon) logPRTerminalStateBatchSkip(format string, args ...any) {
	if d.logf == nil {
		return
	}
	d.logf("github graphql PR terminal state batch skipping project: "+format, args...)
}

func taskStateRunsTerminalPoll(state string) bool {
	switch state {
	case TaskStateDone, TaskStateMerged, TaskStateCloneMissing:
		return false
	default:
		return true
	}
}

func batchedPRTerminalState(ctx context.Context, projectPath string, prNumber int) (prTerminalState, bool) {
	states, ok := ctx.Value(prTerminalStateBatchContextKey{}).(map[prTerminalStateKey]prTerminalState)
	if !ok || len(states) == 0 {
		return prTerminalState{}, false
	}
	state, ok := states[prTerminalStateKey{Project: strings.TrimSpace(projectPath), PRNumber: prNumber}]
	return state, ok
}

func githubOwnerRepoFromOrigin(origin string) (string, string, bool) {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return "", "", false
	}

	if parsed, err := url.Parse(origin); err == nil && parsed.Host != "" {
		if !isGitHubHost(parsed.Host) {
			return "", "", false
		}
		return splitGitHubRepoPath(parsed.Path)
	}

	if at := strings.Index(origin, "@"); at >= 0 && strings.Contains(origin, ":") && !strings.Contains(origin, "://") {
		hostPath := origin[at+1:]
		host, path, ok := strings.Cut(hostPath, ":")
		if !ok || !isGitHubHost(host) {
			return "", "", false
		}
		return splitGitHubRepoPath(path)
	}

	if strings.HasPrefix(strings.ToLower(origin), "github.com/") {
		return splitGitHubRepoPath(strings.TrimPrefix(origin, "github.com/"))
	}
	return "", "", false
}

func isGitHubHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "github.com" || host == "www.github.com"
}

func splitGitHubRepoPath(path string) (string, string, bool) {
	path = strings.Trim(strings.TrimSuffix(strings.TrimSpace(path), ".git"), "/")
	owner, repo, ok := strings.Cut(path, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return "", "", false
	}
	return owner, repo, true
}

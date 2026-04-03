package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

func enforceLifecycleProfile(profile AgentProfile) AgentProfile {
	if !strings.EqualFold(profile.Name, "codex") {
		return profile
	}
	profile.StartCommand = ensureFlag(profile.StartCommand, "--yolo")
	return profile
}

func ensureFlag(command, flag string) string {
	command = strings.TrimSpace(command)
	if command == "" || flag == "" {
		return command
	}

	parts := strings.Fields(command)
	if slices.Contains(parts[1:], flag) {
		return command
	}
	return command + " " + flag
}

func (d *Daemon) ensurePostmortem(ctx context.Context, active *assignment) error {
	command := postmortemCommandForProfile(active.profile)
	if command == "" {
		return nil
	}

	if ok, err := d.postmortemRecorded(active); err != nil {
		return fmt.Errorf("check postmortem: %w", err)
	} else if ok {
		return nil
	}

	if err := d.amux.SendKeys(ctx, active.pane.ID, command, "Enter"); err != nil {
		return fmt.Errorf("request postmortem: %w", err)
	}
	if err := d.amux.WaitIdle(ctx, active.pane.ID, d.mergeGracePeriod); err != nil {
		return fmt.Errorf("wait for postmortem: %w", err)
	}

	if ok, err := d.postmortemRecorded(active); err != nil {
		return fmt.Errorf("verify postmortem: %w", err)
	} else if ok {
		return nil
	}

	return errors.New("postmortem not recorded")
}

func postmortemCommandForProfile(profile AgentProfile) string {
	if !profile.PostmortemEnabled {
		return ""
	}
	return postmortemCommand
}

func (d *Daemon) postmortemRecorded(active *assignment) (bool, error) {
	d.mu.Lock()
	recordedAt := d.postmortems[active.pane.ID]
	d.mu.Unlock()
	if !recordedAt.IsZero() && !recordedAt.Before(active.startedAt) {
		return true, nil
	}

	recordedAt, ok, err := findPostmortem(active, d.project)
	if err != nil || !ok {
		return ok, err
	}

	d.mu.Lock()
	d.postmortems[active.pane.ID] = recordedAt
	d.mu.Unlock()
	return true, nil
}

func findPostmortem(active *assignment, project string) (time.Time, bool, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return time.Time{}, false, fmt.Errorf("resolve home directory: %w", err)
	}

	var newest time.Time
	for _, dir := range []string{
		filepath.Join(home, "sync", "postmortems"),
		filepath.Join(home, ".local", "share", "postmortems"),
	} {
		recordedAt, ok, err := findPostmortemInDir(dir, active, project)
		if err != nil {
			return time.Time{}, false, err
		}
		if ok && recordedAt.After(newest) {
			newest = recordedAt
		}
	}

	if newest.IsZero() {
		return time.Time{}, false, nil
	}
	return newest, true, nil
}

func findPostmortemInDir(dir string, active *assignment, project string) (time.Time, bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("read postmortem dir %q: %w", dir, err)
	}

	var newest time.Time
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return time.Time{}, false, fmt.Errorf("stat postmortem %q: %w", path, err)
		}
		if info.ModTime().Before(active.startedAt) {
			continue
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("read postmortem %q: %w", path, err)
		}
		if !matchesPostmortemSession(string(data), project, active.task.Issue, active.task.Branch) {
			continue
		}
		if info.ModTime().After(newest) {
			newest = info.ModTime()
		}
	}

	if newest.IsZero() {
		return time.Time{}, false, nil
	}
	return newest, true, nil
}

func matchesPostmortemSession(content, project, issue, branch string) bool {
	content = strings.ToLower(content)
	issue = strings.ToLower(strings.TrimSpace(issue))
	branch = strings.ToLower(strings.TrimSpace(branch))
	if issue != "" && !strings.Contains(content, issue) {
		return false
	}
	if branch != "" && !strings.Contains(content, branch) {
		return false
	}
	return true
}

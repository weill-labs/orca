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

const (
	mergedWrapUpPrompt = "PR merged, wrap up.\n"
	postmortemCommand  = "$postmortem"
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

	recordedAt, ok, err := findPostmortem(active)
	if err != nil || !ok {
		return ok, err
	}

	d.mu.Lock()
	d.postmortems[active.pane.ID] = recordedAt
	d.mu.Unlock()
	return true, nil
}

func findPostmortem(active *assignment) (time.Time, bool, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return time.Time{}, false, fmt.Errorf("resolve home directory: %w", err)
	}

	var newest time.Time
	for _, dir := range []string{
		filepath.Join(home, "sync", "postmortems"),
		filepath.Join(home, ".local", "share", "postmortems"),
	} {
		recordedAt, ok, err := findPostmortemInDir(dir, active)
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

func findPostmortemInDir(dir string, active *assignment) (time.Time, bool, error) {
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
		if !matchesPostmortemSession(string(data), active.task.Issue, active.task.Branch) {
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

func matchesPostmortemSession(content, issue, branch string) bool {
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

func (d *Daemon) finishAssignment(ctx context.Context, active *assignment, status, eventType string, merged bool) error {
	if err := d.ensurePostmortem(ctx, active); err != nil {
		return err
	}

	var result error
	active.cleanupOnce.Do(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		active.cancel()

		if merged {
			if err := d.amux.SendKeys(cleanupCtx, active.pane.ID, mergedWrapUpPrompt); err != nil {
				result = errors.Join(result, err)
			}
			if err := d.amux.WaitIdle(cleanupCtx, active.pane.ID, d.mergeGracePeriod); err != nil {
				result = errors.Join(result, d.amux.KillPane(cleanupCtx, active.pane.ID))
			}
		} else {
			result = errors.Join(result, d.amux.KillPane(cleanupCtx, active.pane.ID))
		}

		result = errors.Join(result, d.cleanupCloneAndRelease(cleanupCtx, active.clone, active.task.Branch))

		active.task.Status = status
		active.task.PRNumber = active.prNumber
		active.task.UpdatedAt = d.now()
		result = errors.Join(result, d.state.PutTask(cleanupCtx, active.task))
		result = errors.Join(result, d.state.DeleteWorker(cleanupCtx, d.project, active.pane.ID))

		d.mu.Lock()
		delete(d.assignments, active.task.Issue)
		d.mu.Unlock()

		message := "task finished"
		if status == TaskStatusCancelled {
			message = "task cancelled"
		}
		d.emit(cleanupCtx, Event{
			Time:         d.now(),
			Type:         eventType,
			Project:      d.project,
			Issue:        active.task.Issue,
			PaneID:       active.pane.ID,
			PaneName:     active.pane.Name,
			CloneName:    active.clone.Name,
			ClonePath:    active.clone.Path,
			Branch:       active.task.Branch,
			AgentProfile: active.profile.Name,
			PRNumber:     active.prNumber,
			Message:      message,
		})
	})
	return result
}

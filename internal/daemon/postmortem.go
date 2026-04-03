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

func (d *Daemon) ensurePostmortem(ctx context.Context, active ActiveAssignment) error {
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return fmt.Errorf("load agent profile: %w", err)
	}

	command := postmortemCommandForProfile(profile)
	if command == "" {
		return nil
	}

	if ok, err := postmortemRecorded(active); err != nil {
		return fmt.Errorf("check postmortem: %w", err)
	} else if ok {
		return nil
	}

	if err := d.amux.SendKeys(ctx, active.Task.PaneID, command, "Enter"); err != nil {
		return fmt.Errorf("request postmortem: %w", err)
	}
	if err := d.amux.WaitIdle(ctx, active.Task.PaneID, d.mergeGracePeriod); err != nil {
		return fmt.Errorf("wait for postmortem: %w", err)
	}

	if ok, err := postmortemRecorded(active); err != nil {
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

func postmortemRecorded(active ActiveAssignment) (bool, error) {
	_, ok, err := findPostmortem(active)
	return ok, err
}

func findPostmortem(active ActiveAssignment) (time.Time, bool, error) {
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

func findPostmortemInDir(dir string, active ActiveAssignment) (time.Time, bool, error) {
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

		startedAt := active.Task.CreatedAt
		if startedAt.IsZero() {
			startedAt = active.Task.UpdatedAt
		}
		if !startedAt.IsZero() && info.ModTime().Before(startedAt) {
			continue
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("read postmortem %q: %w", path, err)
		}
		if !matchesPostmortemSession(string(data), active.Task.Issue, active.Task.Branch) {
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

func (d *Daemon) finishAssignment(ctx context.Context, active ActiveAssignment, status, eventType string, merged bool) error {
	if err := d.ensurePostmortem(ctx, active); err != nil {
		return err
	}

	var result error
	cleanupCtx := context.WithoutCancel(ctx)

	if merged {
		if err := d.amux.SendKeys(cleanupCtx, active.Task.PaneID, mergedWrapUpPrompt); err != nil {
			result = errors.Join(result, err)
		}
		if err := d.amux.WaitIdle(cleanupCtx, active.Task.PaneID, d.mergeGracePeriod); err != nil {
			result = errors.Join(result, d.amux.KillPane(cleanupCtx, active.Task.PaneID))
		}
	} else {
		result = errors.Join(result, d.amux.KillPane(cleanupCtx, active.Task.PaneID))
	}

	clone := Clone{
		Name: active.Task.CloneName,
		Path: active.Task.ClonePath,
	}
	if clone.Name == "" && clone.Path != "" {
		clone.Name = filepath.Base(clone.Path)
	}
	result = errors.Join(result, d.cleanupCloneAndRelease(cleanupCtx, clone, active.Task.Branch))

	active.Task.Status = status
	active.Task.UpdatedAt = d.now()
	result = errors.Join(result, d.state.PutTask(cleanupCtx, active.Task))
	result = errors.Join(result, d.state.DeleteWorker(cleanupCtx, d.project, active.Task.PaneID))
	if active.Task.PRNumber > 0 {
		if err := d.state.DeleteMergeEntry(cleanupCtx, d.project, active.Task.PRNumber); err != nil && !errors.Is(err, ErrTaskNotFound) {
			result = errors.Join(result, err)
		}
	}

	message := "task finished"
	if status == TaskStatusCancelled {
		message = "task cancelled"
	}
	profile, err := d.profileForTask(cleanupCtx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}
	d.emit(cleanupCtx, Event{
		Time:         d.now(),
		Type:         eventType,
		Project:      d.project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     active.Task.PaneName,
		CloneName:    clone.Name,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      message,
	})
	return result
}

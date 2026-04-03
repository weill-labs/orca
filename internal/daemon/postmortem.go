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

func (d *Daemon) ensurePostmortem(ctx context.Context, active ActiveAssignment, allowTrigger bool) error {
	status, message, err := d.postmortemStatus(ctx, active, allowTrigger)
	profile, profileErr := d.profileForTask(ctx, active.Task)
	if profileErr != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}
	d.emit(ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerPostmortem,
		Project:      d.project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     active.Task.PaneName,
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      fmt.Sprintf("postmortem %s: %s", status, message),
	})
	return err
}

func (d *Daemon) postmortemStatus(ctx context.Context, active ActiveAssignment, allowTrigger bool) (string, string, error) {
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return "failed", fmt.Sprintf("load agent profile: %v", err), err
	}

	command := postmortemCommandForProfile(profile)
	if command == "" {
		return "skipped", "postmortem disabled for agent profile", nil
	}

	keys := postmortemSessionKeys(active)
	if len(keys) == 0 {
		return "skipped", "no worker session metadata available", nil
	}

	path, err := findRecentPostmortem(d.postmortemDir, keys, d.now(), d.postmortemWindow)
	if err != nil {
		return "failed", fmt.Sprintf("check failed: %v", err), err
	}
	if path != "" {
		return "found", path, nil
	}
	if !allowTrigger {
		return "skipped", "cleanup already had an error before postmortem trigger", nil
	}
	if strings.TrimSpace(active.Task.PaneID) == "" {
		return "skipped", "worker pane missing", nil
	}
	if err := d.amux.SendKeys(ctx, active.Task.PaneID, command, "Enter"); err != nil {
		return "failed", fmt.Sprintf("trigger failed: %v", err), err
	}

	waitErr := d.amux.WaitIdle(ctx, active.Task.PaneID, d.postmortemTimeout)
	path, checkErr := findRecentPostmortem(d.postmortemDir, keys, d.now(), d.postmortemWindow)
	if checkErr != nil {
		return "failed", fmt.Sprintf("recheck failed: %v", checkErr), checkErr
	}
	if path != "" {
		if waitErr != nil {
			return "triggered", fmt.Sprintf("%s (wait idle: %v)", path, waitErr), nil
		}
		return "triggered", path, nil
	}
	if waitErr != nil {
		return "triggered", fmt.Sprintf("wait idle returned %v", waitErr), nil
	}
	return "triggered", "prompt sent and wait completed", nil
}

func postmortemCommandForProfile(profile AgentProfile) string {
	if !profile.PostmortemEnabled {
		return ""
	}
	return postmortemCommand
}

func findRecentPostmortem(dir string, keys []string, now time.Time, window time.Duration) (string, error) {
	if strings.TrimSpace(dir) == "" {
		return "", nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}

	cutoff := now.Add(-window)
	var matchPath string
	var matchTime time.Time

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		matches, err := postmortemMatchesSession(path, entry.Name(), keys)
		if err != nil {
			continue
		}
		if !matches {
			continue
		}
		if matchPath == "" || info.ModTime().After(matchTime) {
			matchPath = path
			matchTime = info.ModTime()
		}
	}

	return matchPath, nil
}

func postmortemMatchesSession(path, name string, keys []string) (bool, error) {
	for _, key := range keys {
		if strings.Contains(name, key) {
			return true, nil
		}
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	text := string(content)
	for _, key := range keys {
		if strings.Contains(text, key) {
			return true, nil
		}
	}
	return false, nil
}

func postmortemSessionKeys(active ActiveAssignment) []string {
	clonePath := strings.TrimSpace(active.Task.ClonePath)
	if clonePath == "" {
		clonePath = strings.TrimSpace(active.Worker.ClonePath)
	}
	paneName := strings.TrimSpace(active.Task.PaneName)
	if paneName == "" {
		paneName = strings.TrimSpace(active.Worker.PaneName)
	}
	paneID := strings.TrimSpace(active.Task.PaneID)
	if paneID == "" {
		paneID = strings.TrimSpace(active.Worker.PaneID)
	}
	keys := []string{
		clonePath,
		strings.TrimSpace(active.Task.Issue),
		strings.TrimSpace(active.Task.Branch),
		paneName,
		paneID,
	}
	if clonePath != "" {
		keys = append(keys, filepath.Base(clonePath))
	}

	out := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

func (d *Daemon) finishAssignment(ctx context.Context, active ActiveAssignment, status, eventType string, merged bool) error {
	var result error
	cleanupCtx := context.WithoutCancel(ctx)

	if merged {
		if err := d.amux.SendKeys(cleanupCtx, active.Task.PaneID, mergedWrapUpPrompt); err != nil {
			result = errors.Join(result, err)
		}
		if err := d.amux.WaitIdle(cleanupCtx, active.Task.PaneID, d.mergeGracePeriod); err != nil {
			result = errors.Join(result, err)
		}
	}

	result = errors.Join(result, d.ensurePostmortem(cleanupCtx, active, result == nil))

	if !merged || result != nil {
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

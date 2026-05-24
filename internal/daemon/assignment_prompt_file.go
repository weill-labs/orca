package daemon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	codexLargePromptThresholdBytes = 500
	assignmentPromptFileDir        = ".orca/prompts"
	assignmentPromptGitExclude     = assignmentPromptFileDir + "/"
)

func (d *Daemon) prepareAssignmentPromptForDelivery(task Task, profile AgentProfile, prompt string) (string, error) {
	if !strings.EqualFold(profile.Name, "codex") || len([]byte(prompt)) <= codexLargePromptThresholdBytes {
		return prompt, nil
	}

	path, err := assignmentPromptFilePath(task)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("create assignment prompt directory: %w", err)
	}
	content := prompt
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return "", fmt.Errorf("write assignment prompt file: %w", err)
	}
	if err := ensureAssignmentPromptFileIgnored(task.ClonePath); err != nil && d.logf != nil {
		d.logf("ignore assignment prompt files failed: clone=%s error=%v", task.ClonePath, err)
	}
	return codexAssignmentPromptFileReference(assignmentPromptFileReferencePath(task, path)), nil
}

func assignmentPromptFilePath(task Task) (string, error) {
	clonePath := strings.TrimSpace(task.ClonePath)
	if clonePath == "" {
		return "", errors.New("assignment prompt file requires clone path")
	}

	name := sanitizeAssignmentPromptFileName(task.Issue)
	if name == "" {
		name = "assignment"
	}
	return filepath.Join(clonePath, assignmentPromptFileDir, name+".md"), nil
}

func assignmentPromptFileReferencePath(task Task, path string) string {
	clonePath := strings.TrimSpace(task.ClonePath)
	if clonePath == "" {
		return path
	}
	rel, err := filepath.Rel(clonePath, path)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return path
	}
	return rel
}

func codexAssignmentPromptFileReference(path string) string {
	return fmt.Sprintf("Read the assignment brief at %s and follow every instruction in it.", path)
}

func ensureAssignmentPromptFileIgnored(clonePath string) error {
	gitDir := filepath.Join(clonePath, ".git")
	infoDir := filepath.Join(gitDir, "info")
	if stat, err := os.Stat(gitDir); err != nil || !stat.IsDir() {
		return nil
	}

	excludePath := filepath.Join(infoDir, "exclude")
	existing, err := os.ReadFile(excludePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if gitExcludeContainsLine(string(existing), assignmentPromptGitExclude) {
		return nil
	}
	if err := os.MkdirAll(infoDir, 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(excludePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	if len(existing) > 0 && !strings.HasSuffix(string(existing), "\n") {
		if _, err := file.WriteString("\n"); err != nil {
			return err
		}
	}
	_, err = file.WriteString(assignmentPromptGitExclude + "\n")
	return err
}

func gitExcludeContainsLine(content, line string) bool {
	for _, existing := range strings.Split(content, "\n") {
		if strings.TrimSpace(existing) == line {
			return true
		}
	}
	return false
}

func sanitizeAssignmentPromptFileName(value string) string {
	var builder strings.Builder
	for _, r := range strings.TrimSpace(value) {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '-', r == '_', r == '.':
			builder.WriteRune(r)
		default:
			builder.WriteByte('-')
		}
	}
	return strings.Trim(builder.String(), "-_.")
}

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
	promptFileDir                  = ".orca/prompts"
	promptFileGitExclude           = promptFileDir + "/"
	assignmentPromptFileDir        = promptFileDir
	assignmentPromptGitExclude     = promptFileGitExclude
)

type promptFileDelivery struct {
	clonePath       string
	fileName        string
	kind            string
	referencePrompt func(path string) string
}

func (d *Daemon) prepareAssignmentPromptForDelivery(task Task, profile AgentProfile, prompt string) (string, error) {
	return d.preparePromptForDelivery(profile, prompt, promptFileDelivery{
		clonePath:       task.ClonePath,
		fileName:        task.Issue,
		kind:            "assignment",
		referencePrompt: codexAssignmentPromptFileReference,
	})
}

func (d *Daemon) preparePromptForDelivery(profile AgentProfile, prompt string, delivery promptFileDelivery) (string, error) {
	if !strings.EqualFold(profile.Name, "codex") || len([]byte(prompt)) <= codexLargePromptThresholdBytes {
		return prompt, nil
	}

	path, err := promptFilePath(delivery.clonePath, delivery.fileName, delivery.kind)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("create %s prompt directory: %w", delivery.kind, err)
	}
	content := prompt
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return "", fmt.Errorf("write %s prompt file: %w", delivery.kind, err)
	}
	if err := ensurePromptFileIgnored(delivery.clonePath); err != nil && d.logf != nil {
		d.logf("ignore %s prompt files failed: clone=%s error=%v", delivery.kind, delivery.clonePath, err)
	}
	return delivery.referencePrompt(promptFileReferencePath(delivery.clonePath, path)), nil
}

func assignmentPromptFilePath(task Task) (string, error) {
	return promptFilePath(task.ClonePath, task.Issue, "assignment")
}

func promptFilePath(clonePath, fileName, kind string) (string, error) {
	clonePath = strings.TrimSpace(clonePath)
	if clonePath == "" {
		return "", fmt.Errorf("%s prompt file requires clone path", kind)
	}

	name := sanitizePromptFileName(fileName)
	if name == "" {
		name = sanitizePromptFileName(kind)
	}
	return filepath.Join(clonePath, promptFileDir, name+".md"), nil
}

func assignmentPromptFileReferencePath(task Task, path string) string {
	return promptFileReferencePath(task.ClonePath, path)
}

func promptFileReferencePath(clonePath, path string) string {
	clonePath = strings.TrimSpace(clonePath)
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
	return ensurePromptFileIgnored(clonePath)
}

func ensurePromptFileIgnored(clonePath string) error {
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
	if gitExcludeContainsLine(string(existing), promptFileGitExclude) {
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
	_, err = file.WriteString(promptFileGitExclude + "\n")
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
	return sanitizePromptFileName(value)
}

func sanitizePromptFileName(value string) string {
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

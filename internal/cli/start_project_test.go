package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPoolCloneParentProject(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	poolClone := filepath.Join(repoRoot, ".orca", "pool", "clone-06")
	nestedPath := filepath.Join(poolClone, "internal", "cli")
	if err := os.MkdirAll(filepath.Join(poolClone, ".git"), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", filepath.Join(poolClone, ".git"), err)
	}
	if err := os.MkdirAll(nestedPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", nestedPath, err)
	}

	tests := []struct {
		name        string
		projectPath string
		wantProject string
		wantOK      bool
	}{
		{
			name:        "clone root",
			projectPath: poolClone,
			wantProject: repoRoot,
			wantOK:      true,
		},
		{
			name:        "nested path in clone",
			projectPath: nestedPath,
			wantProject: repoRoot,
			wantOK:      true,
		},
		{
			name:        "path outside pool clone",
			projectPath: filepath.Join(repoRoot, "internal", "cli"),
			wantProject: "",
			wantOK:      false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotProject, gotOK, err := poolCloneParentProject(tt.projectPath)
			if err != nil {
				t.Fatalf("poolCloneParentProject(%q) error = %v", tt.projectPath, err)
			}
			if gotOK != tt.wantOK {
				t.Fatalf("poolCloneParentProject(%q) ok = %t, want %t", tt.projectPath, gotOK, tt.wantOK)
			}
			if gotProject != tt.wantProject {
				t.Fatalf("poolCloneParentProject(%q) = %q, want %q", tt.projectPath, gotProject, tt.wantProject)
			}
		})
	}
}

func TestAppResolveStartProjectLeavesNonPoolProjectsAlone(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	projectPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(projectPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", projectPath, err)
	}

	app := &App{}

	got, err := app.resolveStartProject(projectPath)
	if err != nil {
		t.Fatalf("resolveStartProject(%q) error = %v", projectPath, err)
	}
	if got != repoRoot {
		t.Fatalf("resolveStartProject(%q) = %q, want %q", projectPath, got, repoRoot)
	}
}

func TestAppResolveStartProjectReturnsPoolParentResolutionError(t *testing.T) {
	t.Parallel()

	parentDir := filepath.Join(t.TempDir(), "parent")
	poolClone := filepath.Join(parentDir, ".orca", "pool", "clone-06")
	if err := os.MkdirAll(filepath.Join(poolClone, ".git"), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", filepath.Join(poolClone, ".git"), err)
	}

	app := &App{}

	_, err := app.resolveStartProject(poolClone)
	if err == nil {
		t.Fatal("resolveStartProject() error = nil, want pool parent resolution error")
	}
	if !strings.Contains(err.Error(), "resolve pool clone parent project") {
		t.Fatalf("resolveStartProject() error = %v, want pool parent resolution error", err)
	}
}

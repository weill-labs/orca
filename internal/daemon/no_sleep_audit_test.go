package daemon

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDaemonPackagesDoNotUseTimeSleep(t *testing.T) {
	t.Parallel()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	internalDir := filepath.Dir(currentFile)
	targets := []string{
		internalDir,
		filepath.Join(filepath.Dir(internalDir), "daemonstate"),
	}
	forbidden := "time." + "Sleep("

	for _, target := range targets {
		err := filepath.WalkDir(target, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}

			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if strings.Contains(string(content), forbidden) {
				t.Fatalf("%s contains forbidden %q", path, forbidden)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("WalkDir(%q) error = %v", target, err)
		}
	}
}

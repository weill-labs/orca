package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ci "github.com/weill-labs/orca/internal/ci"
)

func TestRun(t *testing.T) {
	t.Parallel()

	t.Run("without args prints usage", func(t *testing.T) {
		t.Parallel()

		var stdout bytes.Buffer
		var stderr bytes.Buffer

		code := run(nil, &stdout, &stderr)
		if code != 1 {
			t.Fatalf("run() code = %d, want 1", code)
		}
		if stdout.Len() != 0 {
			t.Fatalf("run() stdout = %q, want empty", stdout.String())
		}
		if !strings.Contains(stderr.String(), usageText) {
			t.Fatalf("run() stderr = %q, want usage text", stderr.String())
		}
	})

	t.Run("unknown command prints usage", func(t *testing.T) {
		t.Parallel()

		var stdout bytes.Buffer
		var stderr bytes.Buffer

		code := run([]string{"nope"}, &stdout, &stderr)
		if code != 1 {
			t.Fatalf("run() code = %d, want 1", code)
		}
		if !strings.Contains(stderr.String(), usageText) {
			t.Fatalf("run() stderr = %q, want usage text", stderr.String())
		}
	})

	t.Run("analyze command succeeds and writes artifacts", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		registryPath := writeFile(t, tempDir, "registry.toml", knownFlakeRegistry())
		inputPath := writeFile(t, tempDir, "results.json", strings.Join([]string{
			`{"Action":"fail","Package":"github.com/weill-labs/orca/internal/daemon","Test":"TestKnownDaemonFlake"}`,
			`{"Action":"fail","Package":"github.com/weill-labs/orca/internal/daemon"}`,
		}, "\n"))
		reportPath := filepath.Join(tempDir, "report.json")
		summaryPath := filepath.Join(tempDir, "summary.md")

		var stdout bytes.Buffer
		var stderr bytes.Buffer

		code := run([]string{
			"analyze",
			"--registry", registryPath,
			"--input", inputPath,
			"--output", reportPath,
			"--summary", summaryPath,
		}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("run() code = %d, want 0, stderr = %q", code, stderr.String())
		}
		if stderr.Len() != 0 {
			t.Fatalf("run() stderr = %q, want empty", stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatalf("run() stdout = %q, want empty", stdout.String())
		}

		reportFile, err := os.Open(reportPath)
		if err != nil {
			t.Fatalf("Open(reportPath) error = %v", err)
		}
		defer reportFile.Close()

		report, err := ci.ReadReport(reportFile)
		if err != nil {
			t.Fatalf("ReadReport() error = %v", err)
		}
		if !report.ShouldRerun {
			t.Fatalf("report.ShouldRerun = false, want true")
		}

		summaryBytes, err := os.ReadFile(summaryPath)
		if err != nil {
			t.Fatalf("ReadFile(summaryPath) error = %v", err)
		}
		summary := string(summaryBytes)
		if !strings.Contains(summary, "Known flaky failures detected") {
			t.Fatalf("summary = %q, want known flake text", summary)
		}
	})

	t.Run("analyze command failure writes stderr", func(t *testing.T) {
		t.Parallel()

		var stdout bytes.Buffer
		var stderr bytes.Buffer

		code := run([]string{"analyze"}, &stdout, &stderr)
		if code != 1 {
			t.Fatalf("run() code = %d, want 1", code)
		}
		if stdout.Len() != 0 {
			t.Fatalf("run() stdout = %q, want empty", stdout.String())
		}
		if !strings.Contains(stderr.String(), "requires --registry") {
			t.Fatalf("run() stderr = %q, want analyze error", stderr.String())
		}
	})
}

func TestRunAnalyze(t *testing.T) {
	t.Parallel()

	t.Run("writes summary to stdout", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		registryPath := writeFile(t, tempDir, "registry.toml", knownFlakeRegistry())
		inputPath := writeFile(t, tempDir, "results.json", `{"Action":"fail","Package":"github.com/weill-labs/orca/internal/daemon","Test":"TestKnownDaemonFlake"}`)

		var stdout bytes.Buffer
		err := runAnalyze([]string{"--registry", registryPath, "--input", inputPath}, &stdout)
		if err != nil {
			t.Fatalf("runAnalyze() error = %v", err)
		}
		if !strings.Contains(stdout.String(), "Known flaky failures detected") {
			t.Fatalf("runAnalyze() stdout = %q, want summary text", stdout.String())
		}
	})

	t.Run("rejects positional arguments", func(t *testing.T) {
		t.Parallel()

		err := runAnalyze([]string{"extra"}, ioDiscard{})
		if err == nil || !strings.Contains(err.Error(), "does not accept positional arguments") {
			t.Fatalf("runAnalyze() error = %v, want positional arg error", err)
		}
	})

	t.Run("returns parse error for unknown flag", func(t *testing.T) {
		t.Parallel()

		err := runAnalyze([]string{"--nope"}, ioDiscard{})
		if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Fatalf("runAnalyze() error = %v, want flag parse error", err)
		}
	})

	t.Run("requires registry", func(t *testing.T) {
		t.Parallel()

		err := runAnalyze([]string{"--input", "results.json"}, ioDiscard{})
		if err == nil || !strings.Contains(err.Error(), "requires --registry") {
			t.Fatalf("runAnalyze() error = %v, want missing registry error", err)
		}
	})

	t.Run("requires input", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		registryPath := writeFile(t, tempDir, "registry.toml", knownFlakeRegistry())

		err := runAnalyze([]string{"--registry", registryPath}, ioDiscard{})
		if err == nil || !strings.Contains(err.Error(), "requires --input") {
			t.Fatalf("runAnalyze() error = %v, want missing input error", err)
		}
	})

	t.Run("returns registry load error", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		inputPath := writeFile(t, tempDir, "results.json", "")

		err := runAnalyze([]string{"--registry", filepath.Join(tempDir, "missing.toml"), "--input", inputPath}, ioDiscard{})
		if err == nil || !strings.Contains(err.Error(), "open registry") {
			t.Fatalf("runAnalyze() error = %v, want registry open error", err)
		}
	})

	t.Run("returns input open error", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		registryPath := writeFile(t, tempDir, "registry.toml", knownFlakeRegistry())

		err := runAnalyze([]string{"--registry", registryPath, "--input", filepath.Join(tempDir, "missing.json")}, ioDiscard{})
		if err == nil || !strings.Contains(err.Error(), "open input") {
			t.Fatalf("runAnalyze() error = %v, want input open error", err)
		}
	})

	t.Run("returns output write error", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		registryPath := writeFile(t, tempDir, "registry.toml", knownFlakeRegistry())
		inputPath := writeFile(t, tempDir, "results.json", `{"Action":"fail","Package":"github.com/weill-labs/orca/internal/daemon","Test":"TestKnownDaemonFlake"}`)

		err := runAnalyze([]string{
			"--registry", registryPath,
			"--input", inputPath,
			"--output", tempDir,
		}, ioDiscard{})
		if err == nil || !strings.Contains(err.Error(), "create report") {
			t.Fatalf("runAnalyze() error = %v, want report create error", err)
		}
	})

	t.Run("returns summary write error", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		registryPath := writeFile(t, tempDir, "registry.toml", knownFlakeRegistry())
		inputPath := writeFile(t, tempDir, "results.json", `{"Action":"fail","Package":"github.com/weill-labs/orca/internal/daemon","Test":"TestKnownDaemonFlake"}`)

		err := runAnalyze([]string{
			"--registry", registryPath,
			"--input", inputPath,
			"--summary", tempDir,
		}, ioDiscard{})
		if err == nil || !strings.Contains(err.Error(), "write summary") {
			t.Fatalf("runAnalyze() error = %v, want summary write error", err)
		}
	})

	t.Run("returns stdout write error", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		registryPath := writeFile(t, tempDir, "registry.toml", knownFlakeRegistry())
		inputPath := writeFile(t, tempDir, "results.json", `{"Action":"fail","Package":"github.com/weill-labs/orca/internal/daemon","Test":"TestKnownDaemonFlake"}`)

		err := runAnalyze([]string{"--registry", registryPath, "--input", inputPath}, errWriter{})
		if err == nil || !strings.Contains(err.Error(), "write summary") {
			t.Fatalf("runAnalyze() error = %v, want stdout write error", err)
		}
	})
}

func TestWriteReport(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	reportPath := filepath.Join(tempDir, "report.json")
	report := ci.Report{ShouldRerun: true}
	if err := writeReport(reportPath, report); err != nil {
		t.Fatalf("writeReport() error = %v", err)
	}

	if _, err := os.Stat(reportPath); err != nil {
		t.Fatalf("Stat(reportPath) error = %v", err)
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}

type errWriter struct{}

func (errWriter) Write(p []byte) (int, error) {
	return 0, errors.New("boom")
}

func writeFile(t *testing.T, dir, name, contents string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
	return path
}

func knownFlakeRegistry() string {
	return `
[[flake]]
package = "github.com/weill-labs/orca/internal/daemon"
test = "TestKnownDaemonFlake"
last_seen = "2026-04-03"
issue = "LAB-693"
`
}

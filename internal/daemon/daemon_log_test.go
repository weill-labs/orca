package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRotateDaemonLogKeepsFiveBackupGenerations(t *testing.T) {
	t.Parallel()

	logPath := filepath.Join(t.TempDir(), "daemon.log")
	writeDaemonLogTestFile(t, logPath, "current")
	for generation := 1; generation <= 5; generation++ {
		writeDaemonLogTestFile(t, numberedDaemonLogPath(logPath, generation), fmt.Sprintf("generation-%d", generation))
	}

	if err := rotateDaemonLogIfOversized(logPath, 1, 5); err != nil {
		t.Fatalf("rotateDaemonLogIfOversized() error = %v", err)
	}

	assertDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 1), "current")
	assertDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 2), "generation-1")
	assertDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 5), "generation-4")
}

func TestRedirectProcessOutputToDaemonLogCapturesStdoutAndStderr(t *testing.T) {
	restoreOutput := saveProcessOutput(t)

	ctx, cancel := context.WithCancel(context.Background())
	logPath := filepath.Join(t.TempDir(), "daemon.log")
	if err := RedirectProcessOutputToDaemonLog(ctx, logPath); err != nil {
		restoreOutput()
		t.Fatalf("RedirectProcessOutputToDaemonLog() error = %v", err)
	}

	_, _ = fmt.Fprintln(os.Stdout, "stdout marker")
	_, _ = fmt.Fprintln(os.Stderr, "stderr marker")
	cancel()
	restoreOutput()

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", logPath, err)
	}
	output := string(data)
	for _, want := range []string{"stdout marker", "stderr marker"} {
		if !strings.Contains(output, want) {
			t.Fatalf("daemon log = %q, want %q", output, want)
		}
	}
}

func TestDaemonLogRedirectorRunRotatesAfterSizeLimit(t *testing.T) {
	restoreOutput := saveProcessOutput(t)

	logPath := filepath.Join(t.TempDir(), "daemon.log")
	redirector := &daemonLogRedirector{
		path:     logPath,
		maxBytes: 1,
		backups:  1,
	}
	if err := redirector.rotateAndRedirect(); err != nil {
		restoreOutput()
		t.Fatalf("rotateAndRedirect() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		redirector.run(ctx, 5*time.Millisecond)
	}()

	_, _ = fmt.Fprintln(os.Stderr, "enough bytes to rotate")
	waitForDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 1))
	cancel()
	<-done
	restoreOutput()

	assertDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 1), "enough bytes to rotate\n")
}

func writeDaemonLogTestFile(t *testing.T, path string, content string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func assertDaemonLogTestFile(t *testing.T, path string, want string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if got := string(data); got != want {
		t.Fatalf("ReadFile(%q) = %q, want %q", path, got, want)
	}
}

func waitForDaemonLogTestFile(t *testing.T, path string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatalf("Stat(%q) error = %v", path, err)
		}
		waitForDuration(t, 10*time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func saveProcessOutput(t *testing.T) func() {
	t.Helper()

	stdoutFD, err := syscall.Dup(int(os.Stdout.Fd()))
	if err != nil {
		t.Fatalf("dup stdout: %v", err)
	}
	stderrFD, err := syscall.Dup(int(os.Stderr.Fd()))
	if err != nil {
		_ = syscall.Close(stdoutFD)
		t.Fatalf("dup stderr: %v", err)
	}

	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		if err := syscall.Dup2(stdoutFD, int(os.Stdout.Fd())); err != nil {
			t.Fatalf("restore stdout: %v", err)
		}
		if err := syscall.Dup2(stderrFD, int(os.Stderr.Fd())); err != nil {
			t.Fatalf("restore stderr: %v", err)
		}
		_ = syscall.Close(stdoutFD)
		_ = syscall.Close(stderrFD)
	}
	t.Cleanup(restore)
	return restore
}

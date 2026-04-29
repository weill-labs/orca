package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
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

func TestRotateDaemonLogRestoresFilesWhenCommitFails(t *testing.T) {
	restoreDaemonLogHooks(t)

	logPath := filepath.Join(t.TempDir(), "daemon.log")
	writeDaemonLogTestFile(t, logPath, "current")
	writeDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 1), "generation-1")

	realRename := renameDaemonLogFile
	renameDaemonLogFile = func(oldpath, newpath string) error {
		if strings.Contains(oldpath, ".rotate-") && newpath == numberedDaemonLogPath(logPath, 2) {
			return errors.New("commit failed")
		}
		return realRename(oldpath, newpath)
	}

	err := rotateDaemonLogIfOversized(logPath, 1, 5)
	if err == nil || !strings.Contains(err.Error(), "commit failed") {
		t.Fatalf("rotateDaemonLogIfOversized() error = %v, want commit failure", err)
	}

	assertDaemonLogTestFile(t, logPath, "current")
	assertDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 1), "generation-1")
}

func TestRotateDaemonLogRestoresCurrentFileWhenStagingBackupFails(t *testing.T) {
	restoreDaemonLogHooks(t)

	logPath := filepath.Join(t.TempDir(), "daemon.log")
	writeDaemonLogTestFile(t, logPath, "current")
	writeDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 1), "generation-1")

	realRename := renameDaemonLogFile
	renameDaemonLogFile = func(oldpath, newpath string) error {
		if oldpath == numberedDaemonLogPath(logPath, 1) && strings.Contains(newpath, ".rotate-") {
			return errors.New("stage failed")
		}
		return realRename(oldpath, newpath)
	}

	err := rotateDaemonLogIfOversized(logPath, 1, 5)
	if err == nil || !strings.Contains(err.Error(), "stage failed") {
		t.Fatalf("rotateDaemonLogIfOversized() error = %v, want stage failure", err)
	}

	assertDaemonLogTestFile(t, logPath, "current")
	assertDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 1), "generation-1")
}

func TestRotateDaemonLogContinuesWhenDroppedBackupCleanupFails(t *testing.T) {
	restoreDaemonLogHooks(t)

	logPath := filepath.Join(t.TempDir(), "daemon.log")
	writeDaemonLogTestFile(t, logPath, "current")
	writeDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 4), "generation-4")
	writeDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 5), "generation-5")

	removeDaemonLogFile = func(string) error {
		return errors.New("remove failed")
	}

	if err := rotateDaemonLogIfOversized(logPath, 1, 5); err != nil {
		t.Fatalf("rotateDaemonLogIfOversized() error = %v", err)
	}

	assertDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 1), "current")
	assertDaemonLogTestFile(t, numberedDaemonLogPath(logPath, 5), "generation-4")
}

func TestRotateDaemonLogSkipsWhenRotationIsNotNeeded(t *testing.T) {
	t.Parallel()

	t.Run("disabled", func(t *testing.T) {
		t.Parallel()

		logPath := filepath.Join(t.TempDir(), "daemon.log")
		writeDaemonLogTestFile(t, logPath, "current")
		if err := rotateDaemonLogIfOversized(logPath, 0, 5); err != nil {
			t.Fatalf("rotateDaemonLogIfOversized() error = %v", err)
		}
		assertDaemonLogTestFile(t, logPath, "current")
	})

	t.Run("missing", func(t *testing.T) {
		t.Parallel()

		logPath := filepath.Join(t.TempDir(), "daemon.log")
		if err := rotateDaemonLogIfOversized(logPath, 1, 5); err != nil {
			t.Fatalf("rotateDaemonLogIfOversized() error = %v", err)
		}
	})

	t.Run("below limit", func(t *testing.T) {
		t.Parallel()

		logPath := filepath.Join(t.TempDir(), "daemon.log")
		writeDaemonLogTestFile(t, logPath, "small")
		if err := rotateDaemonLogIfOversized(logPath, 10, 5); err != nil {
			t.Fatalf("rotateDaemonLogIfOversized() error = %v", err)
		}
		assertDaemonLogTestFile(t, logPath, "small")
		if _, err := os.Stat(numberedDaemonLogPath(logPath, 1)); !os.IsNotExist(err) {
			t.Fatalf("Stat(rotated log) error = %v, want not exist", err)
		}
	})
}

func TestOpenDaemonLogFileSurfacesFilesystemErrors(t *testing.T) {
	t.Parallel()

	t.Run("directory cannot be created", func(t *testing.T) {
		t.Parallel()

		blocker := filepath.Join(t.TempDir(), "blocked")
		if err := os.WriteFile(blocker, []byte("file"), 0o644); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", blocker, err)
		}
		_, err := openDaemonLogFile(filepath.Join(blocker, "daemon.log"))
		if err == nil || !strings.Contains(err.Error(), "create daemon log directory") {
			t.Fatalf("openDaemonLogFile() error = %v, want create directory error", err)
		}
	})

	t.Run("log path cannot be opened", func(t *testing.T) {
		t.Parallel()

		_, err := openDaemonLogFile(t.TempDir())
		if err == nil || !strings.Contains(err.Error(), "open daemon log") {
			t.Fatalf("openDaemonLogFile() error = %v, want open error", err)
		}
	})
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

func TestRedirectProcessOutputToDaemonLogIgnoresEmptyPath(t *testing.T) {
	t.Parallel()

	if err := RedirectProcessOutputToDaemonLog(context.Background(), "   "); err != nil {
		t.Fatalf("RedirectProcessOutputToDaemonLog() error = %v", err)
	}
}

func TestRedirectDaemonOutputFileReturnsDescriptorError(t *testing.T) {
	t.Parallel()

	file := os.NewFile(uintptr(1<<20), "invalid")
	err := redirectDaemonOutputFile(file, "invalid")
	if err == nil || !strings.Contains(err.Error(), "redirect daemon stdout") {
		t.Fatalf("redirectDaemonOutputFile() error = %v, want stdout redirect error", err)
	}
}

func TestRedirectDaemonOutputFileRestoresStdoutWhenStderrRedirectFails(t *testing.T) {
	restoreDaemonLogHooks(t)

	logFile, err := os.CreateTemp(t.TempDir(), "daemon.log")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	defer logFile.Close()

	stdoutFD := 1
	stderrFD := 2
	previousStdout := stdoutFD + 1000
	previousStderr := stderrFD + 1000
	var dup2Calls [][2]int

	stdoutDaemonLogFD = func() int {
		return stdoutFD
	}
	stderrDaemonLogFD = func() int {
		return stderrFD
	}
	dupDaemonLogFD = func(fd int) (int, error) {
		switch fd {
		case stdoutFD:
			return previousStdout, nil
		case stderrFD:
			return previousStderr, nil
		default:
			return 0, fmt.Errorf("unexpected dup fd %d", fd)
		}
	}
	closeDaemonLogFD = func(int) error {
		return nil
	}
	dup2DaemonLogFD = func(oldfd int, newfd int) error {
		dup2Calls = append(dup2Calls, [2]int{oldfd, newfd})
		if oldfd == int(logFile.Fd()) && newfd == stderrFD {
			return errors.New("stderr failed")
		}
		return nil
	}

	err = redirectDaemonOutputFile(logFile, "daemon.log")
	if err == nil || !strings.Contains(err.Error(), "stderr failed") {
		t.Fatalf("redirectDaemonOutputFile() error = %v, want stderr failure", err)
	}

	wantCalls := [][2]int{
		{int(logFile.Fd()), stdoutFD},
		{int(logFile.Fd()), stderrFD},
		{previousStdout, stdoutFD},
		{previousStderr, stderrFD},
	}
	if got, want := dup2Calls, wantCalls; !reflect.DeepEqual(got, want) {
		t.Fatalf("dup2 calls = %#v, want %#v", got, want)
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

func TestDaemonServeArgsIncludesLogFileWhenPresent(t *testing.T) {
	t.Parallel()

	args := daemonServeArgs("/bin/orca", ServeRequest{
		Session: "alpha",
		StateDB: "/tmp/orca.db",
		PIDFile: "/tmp/orca.pid",
		LogFile: "/tmp/daemon.log",
	})

	if !containsArgPair(args, "--log-file", "/tmp/daemon.log") {
		t.Fatalf("daemonServeArgs() = %#v, want log file flag", args)
	}
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

func restoreDaemonLogHooks(t *testing.T) {
	t.Helper()

	originalUserHomeDir := userHomeDir
	originalStatDaemonLogFile := statDaemonLogFile
	originalRenameDaemonLogFile := renameDaemonLogFile
	originalRemoveDaemonLogFile := removeDaemonLogFile
	originalDupDaemonLogFD := dupDaemonLogFD
	originalDup2DaemonLogFD := dup2DaemonLogFD
	originalCloseDaemonLogFD := closeDaemonLogFD
	originalStdoutDaemonLogFD := stdoutDaemonLogFD
	originalStderrDaemonLogFD := stderrDaemonLogFD
	t.Cleanup(func() {
		userHomeDir = originalUserHomeDir
		statDaemonLogFile = originalStatDaemonLogFile
		renameDaemonLogFile = originalRenameDaemonLogFile
		removeDaemonLogFile = originalRemoveDaemonLogFile
		dupDaemonLogFD = originalDupDaemonLogFD
		dup2DaemonLogFD = originalDup2DaemonLogFD
		closeDaemonLogFD = originalCloseDaemonLogFD
		stdoutDaemonLogFD = originalStdoutDaemonLogFD
		stderrDaemonLogFD = originalStderrDaemonLogFD
	})
}

func saveProcessOutput(t *testing.T) func() {
	t.Helper()

	stdoutFD, err := unix.Dup(int(os.Stdout.Fd()))
	if err != nil {
		t.Fatalf("dup stdout: %v", err)
	}
	stderrFD, err := unix.Dup(int(os.Stderr.Fd()))
	if err != nil {
		_ = unix.Close(stdoutFD)
		t.Fatalf("dup stderr: %v", err)
	}

	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		if err := unix.Dup2(stdoutFD, int(os.Stdout.Fd())); err != nil {
			t.Fatalf("restore stdout: %v", err)
		}
		if err := unix.Dup2(stderrFD, int(os.Stderr.Fd())); err != nil {
			t.Fatalf("restore stderr: %v", err)
		}
		_ = unix.Close(stdoutFD)
		_ = unix.Close(stderrFD)
	}
	t.Cleanup(restore)
	return restore
}

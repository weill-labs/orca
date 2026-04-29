package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	daemonLogFileName             = "daemon.log"
	daemonLogMaxBytes       int64 = 10 * 1024 * 1024
	daemonLogBackups              = 5
	daemonLogRotateInterval       = 30 * time.Second
)

var (
	userHomeDir         = os.UserHomeDir
	statDaemonLogFile   = os.Stat
	renameDaemonLogFile = os.Rename
	removeDaemonLogFile = os.Remove
	dupDaemonLogFD      = unix.Dup
	dup2DaemonLogFD     = unix.Dup2
	closeDaemonLogFD    = unix.Close
	stdoutDaemonLogFD   = func() int { return int(os.Stdout.Fd()) }
	stderrDaemonLogFD   = func() int { return int(os.Stderr.Fd()) }
)

type daemonLogRedirector struct {
	path     string
	maxBytes int64
	backups  int
	mu       sync.Mutex
}

func defaultDaemonLogFile() (string, error) {
	homeDir, err := userHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(homeDir, ".local", "state", "orca", daemonLogFileName), nil
}

func (p Paths) daemonLogFile() (string, error) {
	if p.LogFile != "" {
		return p.LogFile, nil
	}
	return defaultDaemonLogFile()
}

func openDaemonLogFile(path string) (*os.File, error) {
	return openDaemonLogFileWithRotation(path, daemonLogMaxBytes, daemonLogBackups)
}

func openDaemonLogFileWithRotation(path string, maxBytes int64, backups int) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create daemon log directory: %w", err)
	}
	if err := rotateDaemonLogIfOversized(path, maxBytes, backups); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open daemon log %s: %w", path, err)
	}
	return file, nil
}

func RedirectProcessOutputToDaemonLog(ctx context.Context, path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}

	redirector := &daemonLogRedirector{
		path:     path,
		maxBytes: daemonLogMaxBytes,
		backups:  daemonLogBackups,
	}
	if err := redirector.rotateAndRedirect(); err != nil {
		return err
	}

	go redirector.run(ctx, daemonLogRotateInterval)
	return nil
}

func (r *daemonLogRedirector) run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.rotateAndRedirect(); err != nil {
				log.Printf("daemon log rotation failed: %v", err)
			}
		}
	}
}

func (r *daemonLogRedirector) rotateAndRedirect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	file, err := openDaemonLogFileWithRotation(r.path, r.maxBytes, r.backups)
	if err != nil {
		return err
	}
	defer file.Close()

	return redirectDaemonOutputFile(file, r.path)
}

func redirectDaemonOutputFile(file *os.File, path string) error {
	stdoutFD := stdoutDaemonLogFD()
	stderrFD := stderrDaemonLogFD()
	daemonLogFD := int(file.Fd())

	previousStdout, err := dupDaemonLogFD(stdoutFD)
	if err != nil {
		return fmt.Errorf("snapshot daemon stdout: %w", err)
	}
	defer closeDaemonLogFD(previousStdout)

	previousStderr, err := dupDaemonLogFD(stderrFD)
	if err != nil {
		return fmt.Errorf("snapshot daemon stderr: %w", err)
	}
	defer closeDaemonLogFD(previousStderr)

	if err := dup2DaemonLogFD(daemonLogFD, stdoutFD); err != nil {
		return fmt.Errorf("redirect daemon stdout to %s: %w", path, err)
	}
	if err := dup2DaemonLogFD(daemonLogFD, stderrFD); err != nil {
		restoreErr := errors.Join(
			restoreDaemonOutputFD(previousStdout, stdoutFD, "stdout"),
			restoreDaemonOutputFD(previousStderr, stderrFD, "stderr"),
		)
		return errors.Join(fmt.Errorf("redirect daemon stderr to %s: %w", path, err), restoreErr)
	}
	return nil
}

func restoreDaemonOutputFD(previousFD int, targetFD int, name string) error {
	if err := dup2DaemonLogFD(previousFD, targetFD); err != nil {
		return fmt.Errorf("restore daemon %s: %w", name, err)
	}
	return nil
}

func rotateDaemonLogIfOversized(path string, maxBytes int64, backups int) error {
	if maxBytes <= 0 || backups <= 0 {
		return nil
	}

	info, err := statDaemonLogFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat daemon log %s: %w", path, err)
	}
	if info.Size() < maxBytes {
		return nil
	}

	staged := make([]stagedDaemonLogFile, 0, backups+1)
	if err := stageDaemonLogFile(&staged, path, numberedDaemonLogPath(path, 1), false); err != nil {
		return err
	}
	for generation := 1; generation <= backups; generation++ {
		source := numberedDaemonLogPath(path, generation)
		destination := numberedDaemonLogPath(path, generation+1)
		drop := generation == backups
		if drop {
			destination = ""
		}
		if err := stageDaemonLogFile(&staged, source, destination, drop); err != nil {
			restoreStagedDaemonLogFiles(staged)
			return err
		}
	}

	committed := make([]stagedDaemonLogFile, 0, len(staged))
	for _, entry := range staged {
		if entry.drop {
			continue
		}
		if err := renameDaemonLogFile(entry.temp, entry.destination); err != nil {
			rollbackCommittedDaemonLogFiles(committed)
			restoreStagedDaemonLogFiles(staged)
			return fmt.Errorf("rotate daemon log %s to %s: %w", entry.temp, entry.destination, err)
		}
		committed = append(committed, entry)
	}

	for _, entry := range staged {
		if !entry.drop {
			continue
		}
		if err := removeDaemonLogFile(entry.temp); err != nil && !os.IsNotExist(err) {
			log.Printf("remove old daemon log backup %s: %v", entry.temp, err)
		}
	}
	return nil
}

type stagedDaemonLogFile struct {
	original    string
	temp        string
	destination string
	drop        bool
}

func stageDaemonLogFile(staged *[]stagedDaemonLogFile, source string, destination string, drop bool) error {
	temp := temporaryDaemonLogPath(source)
	if err := renameDaemonLogFile(source, temp); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stage daemon log %s: %w", source, err)
	}
	*staged = append(*staged, stagedDaemonLogFile{
		original:    source,
		temp:        temp,
		destination: destination,
		drop:        drop,
	})
	return nil
}

func rollbackCommittedDaemonLogFiles(committed []stagedDaemonLogFile) {
	for i := len(committed) - 1; i >= 0; i-- {
		entry := committed[i]
		_ = renameDaemonLogFile(entry.destination, entry.temp)
	}
}

func restoreStagedDaemonLogFiles(staged []stagedDaemonLogFile) {
	for i := len(staged) - 1; i >= 0; i-- {
		entry := staged[i]
		_ = renameDaemonLogFile(entry.temp, entry.original)
	}
}

func temporaryDaemonLogPath(path string) string {
	return fmt.Sprintf("%s.rotate-%d-%d", path, os.Getpid(), time.Now().UnixNano())
}

func numberedDaemonLogPath(path string, generation int) string {
	return fmt.Sprintf("%s.%d", path, generation)
}

package daemon

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	daemonLogFileName             = "daemon.log"
	daemonLogMaxBytes       int64 = 10 * 1024 * 1024
	daemonLogBackups              = 5
	daemonLogRotateInterval       = 30 * time.Second
)

type daemonLogRedirector struct {
	path     string
	maxBytes int64
	backups  int
	mu       sync.Mutex
}

func defaultDaemonLogFile() (string, error) {
	homeDir, err := os.UserHomeDir()
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
	if err := syscall.Dup2(int(file.Fd()), int(os.Stdout.Fd())); err != nil {
		return fmt.Errorf("redirect daemon stdout to %s: %w", path, err)
	}
	if err := syscall.Dup2(int(file.Fd()), int(os.Stderr.Fd())); err != nil {
		return fmt.Errorf("redirect daemon stderr to %s: %w", path, err)
	}
	return nil
}

func rotateDaemonLogIfOversized(path string, maxBytes int64, backups int) error {
	if maxBytes <= 0 || backups <= 0 {
		return nil
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat daemon log %s: %w", path, err)
	}
	if info.Size() < maxBytes {
		return nil
	}

	if err := os.Remove(numberedDaemonLogPath(path, backups)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old daemon log backup: %w", err)
	}
	for generation := backups - 1; generation >= 1; generation-- {
		from := numberedDaemonLogPath(path, generation)
		to := numberedDaemonLogPath(path, generation+1)
		if err := os.Rename(from, to); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("rotate daemon log backup %s to %s: %w", from, to, err)
		}
	}
	if err := os.Rename(path, numberedDaemonLogPath(path, 1)); err != nil {
		return fmt.Errorf("rotate daemon log %s: %w", path, err)
	}
	return nil
}

func numberedDaemonLogPath(path string, generation int) string {
	return fmt.Sprintf("%s.%d", path, generation)
}

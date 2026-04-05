package main

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestVersionOutput(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=TestVersionHelper")
	cmd.Env = append(os.Environ(), "ORCA_TEST_HELPER=version")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(out), "orca ") {
		t.Errorf("expected 'orca ...' prefix, got %q", string(out))
	}
}

func TestVersionHelper(t *testing.T) {
	if os.Getenv("ORCA_TEST_HELPER") != "version" {
		return
	}
	os.Args = []string{"orca", "version"}
	main()
}

func TestUsageExitsNonZero(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=TestUsageHelper")
	cmd.Env = append(os.Environ(), "ORCA_TEST_HELPER=usage")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected non-zero exit")
	}
}

func TestUsageHelper(t *testing.T) {
	if os.Getenv("ORCA_TEST_HELPER") != "usage" {
		return
	}
	os.Args = []string{"orca"}
	main()
}

func TestRunHelpFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
	}{
		{name: "long flag", args: []string{"--help"}},
		{name: "short flag", args: []string{"-h"}},
		{name: "help command", args: []string{"help"}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			var stderr bytes.Buffer

			exitCode := run(tt.args, &stdout, &stderr)
			if exitCode != 0 {
				t.Fatalf("run(%q) exit code = %d, want 0", tt.args, exitCode)
			}
			if !strings.Contains(stdout.String(), "usage: orca <command>") {
				t.Fatalf("stdout = %q, want root usage", stdout.String())
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

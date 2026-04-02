package main

import (
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

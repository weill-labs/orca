package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	ci "github.com/weill-labs/orca/internal/ci"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usageText)
		return 1
	}

	switch args[0] {
	case "analyze":
		if err := runAnalyze(args[1:], stdout); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	default:
		fmt.Fprintln(stderr, usageText)
		return 1
	}
}

const usageText = `usage: ci-flakes <command>

commands:
  analyze  Analyze go test -json output against the flake registry`

func runAnalyze(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("analyze", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var registryPath string
	var inputPath string
	var outputPath string
	var summaryPath string

	fs.StringVar(&registryPath, "registry", "", "path to the flake registry TOML")
	fs.StringVar(&inputPath, "input", "", "path to go test -json output")
	fs.StringVar(&outputPath, "output", "", "path to the JSON report")
	fs.StringVar(&summaryPath, "summary", "", "path to the markdown summary")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("analyze does not accept positional arguments")
	}
	if registryPath == "" {
		return fmt.Errorf("analyze requires --registry")
	}
	if inputPath == "" {
		return fmt.Errorf("analyze requires --input")
	}

	registry, err := ci.LoadRegistry(registryPath)
	if err != nil {
		return err
	}

	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("open input %s: %w", inputPath, err)
	}
	defer inputFile.Close()

	report, err := ci.AnalyzeTestOutput(inputFile, registry)
	if err != nil {
		return err
	}

	if outputPath != "" {
		if err := writeReport(outputPath, report); err != nil {
			return err
		}
	}

	summary := ci.RenderSummary(report)
	if summaryPath != "" {
		if err := os.WriteFile(summaryPath, []byte(summary), 0o644); err != nil {
			return fmt.Errorf("write summary %s: %w", summaryPath, err)
		}
	} else if _, err := io.WriteString(stdout, summary); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}

	return nil
}

func writeReport(path string, report ci.Report) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create report %s: %w", path, err)
	}
	defer file.Close()

	if err := ci.WriteReport(file, report); err != nil {
		return fmt.Errorf("write report %s: %w", path, err)
	}
	return nil
}

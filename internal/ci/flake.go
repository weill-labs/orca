package ci

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const registryDateLayout = "2006-01-02"

type TestKey struct {
	Package string
	Test    string
}

type RegistryEntry struct {
	Package  string
	Test     string
	LastSeen time.Time
	Issue    string
}

type Registry struct {
	Entries map[TestKey]RegistryEntry
}

type Failure struct {
	Package     string `json:"package"`
	Test        string `json:"test"`
	Occurrences int    `json:"occurrences"`
}

type PackageFailure struct {
	Package     string `json:"package"`
	Occurrences int    `json:"occurrences"`
}

type KnownFailure struct {
	Failure  Failure   `json:"failure"`
	LastSeen time.Time `json:"last_seen"`
	Issue    string    `json:"issue"`
}

type Report struct {
	KnownFailures   []KnownFailure   `json:"known_failures"`
	UnknownFailures []Failure        `json:"unknown_failures"`
	PackageFailures []PackageFailure `json:"package_failures"`
	ShouldRerun     bool             `json:"should_rerun"`
}

type rawRegistry struct {
	Flakes []rawRegistryEntry `toml:"flake"`
}

type rawRegistryEntry struct {
	Package  string `toml:"package"`
	Test     string `toml:"test"`
	LastSeen string `toml:"last_seen"`
	Issue    string `toml:"issue"`
}

type testEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test,omitempty"`
}

func ParseRegistry(r io.Reader) (Registry, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return Registry{}, fmt.Errorf("read registry: %w", err)
	}

	var raw rawRegistry
	if _, err := toml.Decode(string(data), &raw); err != nil {
		return Registry{}, fmt.Errorf("parse registry: %w", err)
	}

	registry := Registry{Entries: make(map[TestKey]RegistryEntry, len(raw.Flakes))}
	for _, flake := range raw.Flakes {
		if flake.Package == "" {
			return Registry{}, fmt.Errorf("parse registry: missing package")
		}
		if flake.Test == "" {
			return Registry{}, fmt.Errorf("parse registry: missing test")
		}
		if flake.LastSeen == "" {
			return Registry{}, fmt.Errorf("parse registry: missing last_seen")
		}
		if flake.Issue == "" {
			return Registry{}, fmt.Errorf("parse registry: missing issue")
		}

		lastSeen, err := time.Parse(registryDateLayout, flake.LastSeen)
		if err != nil {
			return Registry{}, fmt.Errorf("parse registry date for %s/%s: %w", flake.Package, flake.Test, err)
		}

		key := TestKey{
			Package: flake.Package,
			Test:    flake.Test,
		}
		if _, exists := registry.Entries[key]; exists {
			return Registry{}, fmt.Errorf("duplicate flake entry for %s/%s", flake.Package, flake.Test)
		}

		registry.Entries[key] = RegistryEntry{
			Package:  flake.Package,
			Test:     flake.Test,
			LastSeen: lastSeen,
			Issue:    flake.Issue,
		}
	}

	return registry, nil
}

func LoadRegistry(path string) (Registry, error) {
	file, err := os.Open(path)
	if err != nil {
		return Registry{}, fmt.Errorf("open registry %s: %w", path, err)
	}
	defer file.Close()

	return ParseRegistry(file)
}

func AnalyzeTestOutput(r io.Reader, registry Registry) (Report, error) {
	testFailures := make(map[TestKey]int)
	packageFailures := make(map[string]int)

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var event testEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return Report{}, fmt.Errorf("parse test output: %w", err)
		}
		if event.Action != "fail" || event.Package == "" {
			continue
		}

		if event.Test == "" {
			packageFailures[event.Package]++
			continue
		}

		key := TestKey{
			Package: event.Package,
			Test:    event.Test,
		}
		testFailures[key]++
	}
	if err := scanner.Err(); err != nil {
		return Report{}, fmt.Errorf("scan test output: %w", err)
	}

	testFailures = suppressParentFailures(testFailures)
	failedPackagesWithNamedTests := make(map[string]struct{}, len(testFailures))
	for key := range testFailures {
		failedPackagesWithNamedTests[key.Package] = struct{}{}
	}

	report := newReport()
	for key, occurrences := range testFailures {
		failure := Failure{
			Package:     key.Package,
			Test:        key.Test,
			Occurrences: occurrences,
		}

		if entry, ok := registry.Entries[key]; ok {
			report.KnownFailures = append(report.KnownFailures, KnownFailure{
				Failure:  failure,
				LastSeen: entry.LastSeen,
				Issue:    entry.Issue,
			})
			continue
		}

		report.UnknownFailures = append(report.UnknownFailures, failure)
	}

	for pkg, occurrences := range packageFailures {
		if _, ok := failedPackagesWithNamedTests[pkg]; ok {
			continue
		}
		report.PackageFailures = append(report.PackageFailures, PackageFailure{
			Package:     pkg,
			Occurrences: occurrences,
		})
	}

	sortKnownFailures(report.KnownFailures)
	sortFailures(report.UnknownFailures)
	sortPackageFailures(report.PackageFailures)

	report.ShouldRerun = len(report.KnownFailures) > 0 && len(report.UnknownFailures) == 0 && len(report.PackageFailures) == 0
	return report, nil
}

func RenderSummary(report Report) string {
	var summary strings.Builder
	summary.WriteString("## Flake Check\n")

	if len(report.KnownFailures) == 0 && len(report.UnknownFailures) == 0 && len(report.PackageFailures) == 0 {
		summary.WriteString("No flaky failures detected.\n")
		return summary.String()
	}

	if len(report.KnownFailures) > 0 {
		summary.WriteString("Known flaky failures detected:\n")
		for _, failure := range report.KnownFailures {
			summary.WriteString(fmt.Sprintf(
				"- `%s` `%s` failed %d time(s); tracked by %s; last seen %s\n",
				failure.Failure.Package,
				failure.Failure.Test,
				failure.Failure.Occurrences,
				failure.Issue,
				failure.LastSeen.Format(registryDateLayout),
			))
		}
		if report.ShouldRerun {
			summary.WriteString("All failures match the flake registry; CI can auto-rerun failed jobs.\n")
		}
	}

	if len(report.UnknownFailures) > 0 || len(report.PackageFailures) > 0 {
		summary.WriteString("new failure — investigate\n")

		if len(report.UnknownFailures) > 0 {
			summary.WriteString("Unknown test failures:\n")
			for _, failure := range report.UnknownFailures {
				summary.WriteString(fmt.Sprintf(
					"- `%s` `%s` failed %d time(s)\n",
					failure.Package,
					failure.Test,
					failure.Occurrences,
				))
			}
		}

		if len(report.PackageFailures) > 0 {
			summary.WriteString("package-level failure:\n")
			for _, failure := range report.PackageFailures {
				summary.WriteString(fmt.Sprintf(
					"- `%s` failed without an individual test name %d time(s)\n",
					failure.Package,
					failure.Occurrences,
				))
			}
		}
	}

	return summary.String()
}

func ShouldAutoRerun(report Report, runAttempt, maxAttempts int) bool {
	if !report.ShouldRerun {
		return false
	}
	if runAttempt < 1 || maxAttempts < 1 {
		return false
	}
	return runAttempt < maxAttempts
}

func WriteReport(w io.Writer, report Report) error {
	normalizeReport(&report)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	return nil
}

func ReadReport(r io.Reader) (Report, error) {
	var report Report
	if err := json.NewDecoder(r).Decode(&report); err != nil {
		return Report{}, fmt.Errorf("decode report: %w", err)
	}
	normalizeReport(&report)
	return report, nil
}

func newReport() Report {
	return Report{
		KnownFailures:   []KnownFailure{},
		UnknownFailures: []Failure{},
		PackageFailures: []PackageFailure{},
	}
}

func normalizeReport(report *Report) {
	if report.KnownFailures == nil {
		report.KnownFailures = []KnownFailure{}
	}
	if report.UnknownFailures == nil {
		report.UnknownFailures = []Failure{}
	}
	if report.PackageFailures == nil {
		report.PackageFailures = []PackageFailure{}
	}
}

func suppressParentFailures(testFailures map[TestKey]int) map[TestKey]int {
	filtered := make(map[TestKey]int, len(testFailures))
	for key, occurrences := range testFailures {
		filtered[key] = occurrences
	}

	for parent := range testFailures {
		if strings.Contains(parent.Test, "/") {
			continue
		}

		prefix := parent.Test + "/"
		for child := range testFailures {
			if child.Package == parent.Package && strings.HasPrefix(child.Test, prefix) {
				delete(filtered, parent)
				break
			}
		}
	}

	return filtered
}

func sortKnownFailures(failures []KnownFailure) {
	sort.Slice(failures, func(i, j int) bool {
		if failures[i].Failure.Package != failures[j].Failure.Package {
			return failures[i].Failure.Package < failures[j].Failure.Package
		}
		return failures[i].Failure.Test < failures[j].Failure.Test
	})
}

func sortFailures(failures []Failure) {
	sort.Slice(failures, func(i, j int) bool {
		if failures[i].Package != failures[j].Package {
			return failures[i].Package < failures[j].Package
		}
		return failures[i].Test < failures[j].Test
	})
}

func sortPackageFailures(failures []PackageFailure) {
	sort.Slice(failures, func(i, j int) bool {
		return failures[i].Package < failures[j].Package
	})
}

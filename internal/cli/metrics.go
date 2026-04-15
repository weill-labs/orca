package cli

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/weill-labs/orca/internal/daemon"
)

const (
	defaultMetricsSinceValue = "7d"
	reviewApprovedEventKind  = "review.approved"
)

var latencyMetricDefinitions = []latencyMetricDefinition{
	{name: "assign->pr", startKind: daemon.EventTaskAssigned, endKind: daemon.EventPRDetected},
	{name: "pr->approved", startKind: daemon.EventPRDetected, endKind: reviewApprovedEventKind},
	{name: "pr->merged", startKind: daemon.EventPRDetected, endKind: daemon.EventPRMerged},
	{name: "assign->merged", startKind: daemon.EventTaskAssigned, endKind: daemon.EventPRMerged},
}

type latencyMetricDefinition struct {
	name      string
	startKind string
	endKind   string
}

type latencyMetricsQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type latencyMetricsReport struct {
	Project     string          `json:"project"`
	Window      string          `json:"window"`
	Since       time.Time       `json:"since"`
	GeneratedAt time.Time       `json:"generated_at"`
	Metrics     []latencyMetric `json:"metrics"`
}

type latencyMetric struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
	P50   string `json:"p50,omitempty"`
	P90   string `json:"p90,omitempty"`
	Mean  string `json:"mean,omitempty"`
	Min   string `json:"min,omitempty"`
	Max   string `json:"max,omitempty"`
}

type issueLatencyTimeline struct {
	assignedAt       time.Time
	prDetectedAt     time.Time
	reviewApprovedAt time.Time
	prMergedAt       time.Time
}

func (a *App) runMetrics(ctx context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("metrics", args); handled {
		return err
	}

	fs := newFlagSet("metrics")
	var projectPath string
	var sinceValue string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.StringVar(&sinceValue, "since", defaultMetricsSinceValue, "lookback window")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("metrics does not accept positional arguments")
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	sinceWindow, err := parseMetricsSince(sinceValue)
	if err != nil {
		return err
	}

	queryer, ok := a.state.(latencyMetricsQueryer)
	if !ok {
		return fmt.Errorf("metrics requires a SQL-backed state store")
	}

	now := time.Now().UTC()
	report, err := queryLatencyMetrics(ctx, queryer, projectPath, sinceValue, now.Add(-sinceWindow), now)
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(a.stdout, report)
	}
	return writeLatencyMetrics(a.stdout, report)
}

func queryLatencyMetrics(ctx context.Context, queryer latencyMetricsQueryer, projectPath, window string, since, now time.Time) (latencyMetricsReport, error) {
	rows, err := queryer.QueryContext(ctx, `
SELECT issue, kind, created_at
FROM events
WHERE project = ?
  AND kind IN (?, ?, ?, ?)
  AND created_at >= ?
ORDER BY issue ASC, created_at ASC, id ASC
`, projectPath, daemon.EventTaskAssigned, daemon.EventPRDetected, reviewApprovedEventKind, daemon.EventPRMerged, since.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return latencyMetricsReport{}, fmt.Errorf("query latency metrics: %w", err)
	}
	defer rows.Close()

	timelines := make(map[string]*issueLatencyTimeline)
	for rows.Next() {
		var issue string
		var kind string
		var createdAtText string
		if err := rows.Scan(&issue, &kind, &createdAtText); err != nil {
			return latencyMetricsReport{}, fmt.Errorf("scan latency metric event: %w", err)
		}
		if strings.TrimSpace(issue) == "" {
			continue
		}

		createdAt, err := time.Parse(time.RFC3339Nano, createdAtText)
		if err != nil {
			return latencyMetricsReport{}, fmt.Errorf("parse latency metric event time %q: %w", createdAtText, err)
		}

		timeline := timelines[issue]
		if timeline == nil {
			timeline = &issueLatencyTimeline{}
			timelines[issue] = timeline
		}
		timeline.record(kind, createdAt.UTC())
	}
	if err := rows.Err(); err != nil {
		return latencyMetricsReport{}, fmt.Errorf("iterate latency metric events: %w", err)
	}

	metrics := make([]latencyMetric, 0, len(latencyMetricDefinitions))
	for _, definition := range latencyMetricDefinitions {
		samples := collectLatencySamples(timelines, definition)
		metrics = append(metrics, buildLatencyMetric(definition.name, samples))
	}

	return latencyMetricsReport{
		Project:     projectPath,
		Window:      window,
		Since:       since.UTC(),
		GeneratedAt: now.UTC(),
		Metrics:     metrics,
	}, nil
}

func (timeline *issueLatencyTimeline) record(kind string, createdAt time.Time) {
	switch kind {
	case daemon.EventTaskAssigned:
		if timeline.assignedAt.IsZero() {
			timeline.assignedAt = createdAt
		}
	case daemon.EventPRDetected:
		if timeline.prDetectedAt.IsZero() {
			timeline.prDetectedAt = createdAt
			return
		}
		if !timeline.assignedAt.IsZero() && timeline.prDetectedAt.Before(timeline.assignedAt) && !createdAt.Before(timeline.assignedAt) {
			timeline.prDetectedAt = createdAt
		}
	case reviewApprovedEventKind:
		if timeline.prDetectedAt.IsZero() || createdAt.Before(timeline.prDetectedAt) {
			return
		}
		if timeline.reviewApprovedAt.IsZero() || timeline.reviewApprovedAt.Before(timeline.prDetectedAt) {
			timeline.reviewApprovedAt = createdAt
		}
	case daemon.EventPRMerged:
		if timeline.prDetectedAt.IsZero() || createdAt.Before(timeline.prDetectedAt) {
			return
		}
		if timeline.prMergedAt.IsZero() || timeline.prMergedAt.Before(timeline.prDetectedAt) {
			timeline.prMergedAt = createdAt
		}
	}
}

func collectLatencySamples(timelines map[string]*issueLatencyTimeline, definition latencyMetricDefinition) []time.Duration {
	samples := make([]time.Duration, 0, len(timelines))
	for _, timeline := range timelines {
		start := timeline.eventTime(definition.startKind)
		end := timeline.eventTime(definition.endKind)
		if start.IsZero() || end.IsZero() || end.Before(start) {
			continue
		}
		samples = append(samples, end.Sub(start))
	}
	return samples
}

func (timeline issueLatencyTimeline) eventTime(kind string) time.Time {
	switch kind {
	case daemon.EventTaskAssigned:
		return timeline.assignedAt
	case daemon.EventPRDetected:
		return timeline.prDetectedAt
	case reviewApprovedEventKind:
		return timeline.reviewApprovedAt
	case daemon.EventPRMerged:
		return timeline.prMergedAt
	default:
		return time.Time{}
	}
}

func buildLatencyMetric(name string, samples []time.Duration) latencyMetric {
	metric := latencyMetric{
		Name:  name,
		Count: len(samples),
	}
	if len(samples) == 0 {
		return metric
	}

	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	var sum int64
	for _, sample := range sorted {
		sum += int64(sample)
	}

	metric.P50 = formatLatencyDuration(percentileDuration(sorted, 0.50))
	metric.P90 = formatLatencyDuration(percentileDuration(sorted, 0.90))
	metric.Mean = formatLatencyDuration(time.Duration(sum / int64(len(sorted))))
	metric.Min = formatLatencyDuration(sorted[0])
	metric.Max = formatLatencyDuration(sorted[len(sorted)-1])
	return metric
}

func percentileDuration(samples []time.Duration, percentile float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	if len(samples) == 1 {
		return samples[0]
	}
	if percentile <= 0 {
		return samples[0]
	}
	if percentile >= 1 {
		return samples[len(samples)-1]
	}

	position := percentile * float64(len(samples)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return samples[lower]
	}

	fraction := position - float64(lower)
	delta := samples[upper] - samples[lower]
	return samples[lower] + time.Duration(float64(delta)*fraction)
}

func parseMetricsSince(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		value = defaultMetricsSinceValue
	}

	if strings.HasSuffix(value, "d") {
		days, err := strconv.ParseFloat(strings.TrimSuffix(value, "d"), 64)
		if err != nil {
			return 0, fmt.Errorf("parse --since %q: %w", value, err)
		}
		duration := time.Duration(days * float64(24*time.Hour))
		if duration <= 0 {
			return 0, fmt.Errorf("--since must be greater than zero")
		}
		return duration, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse --since %q: %w", value, err)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("--since must be greater than zero")
	}
	return duration, nil
}

func writeLatencyMetrics(w io.Writer, report latencyMetricsReport) error {
	if _, err := fmt.Fprintf(w, "project: %s\n", report.Project); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "window: last %s\n", report.Window); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "since: %s\n", report.Since.Format(time.RFC3339)); err != nil {
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "\nMETRIC\tCOUNT\tP50\tP90\tMEAN\tMIN\tMAX"); err != nil {
		return err
	}
	for _, metric := range report.Metrics {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
			metric.Name,
			metric.Count,
			fallback(metric.P50),
			fallback(metric.P90),
			fallback(metric.Mean),
			fallback(metric.Min),
			fallback(metric.Max),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatLatencyDuration(duration time.Duration) string {
	duration = duration.Round(time.Second)
	if duration <= 0 {
		return "0s"
	}

	formatted := duration.String()
	formatted = strings.TrimSuffix(formatted, "0s")
	if strings.HasSuffix(formatted, "h0m") {
		formatted = strings.TrimSuffix(formatted, "0m")
	}
	return formatted
}

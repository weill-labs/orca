package daemon

import (
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
)

const (
	ownershipSourceParsed   = "parsed"
	ownershipSourceOverride = "override"
	ownershipSourceUnknown  = "unknown"
)

var (
	ownershipLinePattern = regexp.MustCompile(`(?i)^\s*(?:[-*]\s*)?(?:files?|owned\s+paths?|ownership|touches|paths?|likely\s+ownership)\s*:\s*(.*)$`)
	pathTokenPattern     = regexp.MustCompile("`([^`]+)`|([A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+|[A-Za-z0-9_.-]+\\.[A-Za-z0-9_.-]+)")
)

type AssignmentPlanRequest struct {
	Project       string              `json:"project,omitempty"`
	Parallel      bool                `json:"parallel"`
	Issues        []string            `json:"issues,omitempty"`
	PathOverrides map[string][]string `json:"path_overrides,omitempty"`
}

type AssignmentPlanCandidate struct {
	Issue string `json:"issue"`
	Title string `json:"title,omitempty"`
	Body  string `json:"body,omitempty"`
}

type AssignmentPlanResult struct {
	Project   string                   `json:"project,omitempty"`
	Parallel  bool                     `json:"parallel"`
	Batches   []AssignmentPlanBatch    `json:"batches"`
	Conflicts []AssignmentPlanConflict `json:"conflicts,omitempty"`
	Warnings  []AssignmentPlanWarning  `json:"warnings,omitempty"`
}

type AssignmentPlanBatch struct {
	Number int                   `json:"number"`
	Issues []AssignmentPlanIssue `json:"issues"`
}

type AssignmentPlanIssue struct {
	Issue           string   `json:"issue"`
	Title           string   `json:"title,omitempty"`
	OwnedPaths      []string `json:"owned_paths,omitempty"`
	OwnershipSource string   `json:"ownership_source"`
}

type AssignmentPlanConflict struct {
	Issue         string   `json:"issue"`
	ConflictsWith string   `json:"conflicts_with"`
	Paths         []string `json:"paths"`
}

type AssignmentPlanWarning struct {
	Issue   string `json:"issue,omitempty"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

func PlanParallelAssignments(req AssignmentPlanRequest, candidates []AssignmentPlanCandidate) (AssignmentPlanResult, error) {
	orderedIssues := assignmentPlanIssueOrder(req.Issues, candidates)
	if len(orderedIssues) == 0 {
		return AssignmentPlanResult{}, errors.New("assignment plan requires at least one issue")
	}

	candidatesByIssue := make(map[string]AssignmentPlanCandidate, len(candidates)*2)
	for _, candidate := range candidates {
		issue := strings.TrimSpace(candidate.Issue)
		if issue == "" {
			continue
		}
		candidatesByIssue[issue] = candidate
		candidatesByIssue[strings.ToLower(issue)] = candidate
	}

	overrides, err := normalizePathOverrides(req.PathOverrides)
	if err != nil {
		return AssignmentPlanResult{}, err
	}

	result := AssignmentPlanResult{
		Project:  strings.TrimSpace(req.Project),
		Parallel: req.Parallel,
	}

	for _, issue := range orderedIssues {
		candidate := candidatesByIssue[issue]
		if strings.TrimSpace(candidate.Issue) == "" {
			candidate = candidatesByIssue[strings.ToLower(issue)]
		}
		planned := planIssueOwnership(issue, candidate, overrides)
		if planned.OwnershipSource == ownershipSourceUnknown {
			result.Warnings = append(result.Warnings, AssignmentPlanWarning{
				Issue:   issue,
				Kind:    "unknown_ownership",
				Message: fmt.Sprintf("no owned paths found; use --path %s=path/to/file to override", issue),
			})
		}

		batchIndex, conflicts := assignmentPlanBatchForIssue(result.Batches, planned)
		result.Conflicts = append(result.Conflicts, conflicts...)
		if batchIndex == -1 {
			result.Batches = append(result.Batches, AssignmentPlanBatch{
				Number: len(result.Batches) + 1,
				Issues: []AssignmentPlanIssue{planned},
			})
			continue
		}
		result.Batches[batchIndex].Issues = append(result.Batches[batchIndex].Issues, planned)
	}

	return result, nil
}

func assignmentPlanIssueOrder(requested []string, candidates []AssignmentPlanCandidate) []string {
	if len(requested) > 0 {
		return normalizeIssueList(requested)
	}

	issues := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if issue := strings.TrimSpace(candidate.Issue); issue != "" {
			issues = append(issues, issue)
		}
	}
	return issues
}

func normalizeIssueList(issues []string) []string {
	out := make([]string, 0, len(issues))
	seen := make(map[string]struct{}, len(issues))
	for _, issue := range issues {
		issue = normalizeIssueIdentifier(issue)
		if issue == "" {
			continue
		}
		key := strings.ToLower(issue)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, issue)
	}
	return out
}

func normalizePathOverrides(overrides map[string][]string) (map[string][]string, error) {
	if len(overrides) == 0 {
		return nil, nil
	}

	normalized := make(map[string][]string, len(overrides))
	for rawIssue, rawPaths := range overrides {
		issue := normalizeIssueIdentifier(rawIssue)
		if issue == "" {
			return nil, errors.New("path override requires an issue id")
		}
		paths := make([]string, 0, len(rawPaths))
		for _, rawPath := range rawPaths {
			ownedPath, ok := normalizeOwnedPath(rawPath)
			if !ok {
				return nil, fmt.Errorf("invalid path override for %s: %q", issue, rawPath)
			}
			paths = append(paths, ownedPath)
		}
		paths = uniqueSortedStrings(paths)
		if len(paths) == 0 {
			return nil, fmt.Errorf("path override for %s requires at least one path", issue)
		}
		normalized[issue] = paths
		normalized[strings.ToLower(issue)] = paths
	}
	return normalized, nil
}

func planIssueOwnership(issue string, candidate AssignmentPlanCandidate, overrides map[string][]string) AssignmentPlanIssue {
	issue = normalizeIssueIdentifier(issue)
	if override := overrides[issue]; len(override) > 0 {
		return AssignmentPlanIssue{
			Issue:           issue,
			Title:           strings.TrimSpace(candidate.Title),
			OwnedPaths:      override,
			OwnershipSource: ownershipSourceOverride,
		}
	}
	if override := overrides[strings.ToLower(issue)]; len(override) > 0 {
		return AssignmentPlanIssue{
			Issue:           issue,
			Title:           strings.TrimSpace(candidate.Title),
			OwnedPaths:      override,
			OwnershipSource: ownershipSourceOverride,
		}
	}

	paths := parseOwnedPaths(candidate.Body)
	source := ownershipSourceParsed
	if len(paths) == 0 {
		source = ownershipSourceUnknown
	}
	return AssignmentPlanIssue{
		Issue:           issue,
		Title:           strings.TrimSpace(candidate.Title),
		OwnedPaths:      paths,
		OwnershipSource: source,
	}
}

func assignmentPlanBatchForIssue(batches []AssignmentPlanBatch, issue AssignmentPlanIssue) (int, []AssignmentPlanConflict) {
	if len(issue.OwnedPaths) == 0 {
		return -1, nil
	}

	conflicts := make([]AssignmentPlanConflict, 0)
	for i, batch := range batches {
		if batchHasUnknownOwnership(batch) {
			continue
		}
		batchConflicts := conflictsWithBatch(issue, batch)
		if len(batchConflicts) == 0 {
			return i, conflicts
		}
		conflicts = append(conflicts, batchConflicts...)
	}
	return -1, conflicts
}

func batchHasUnknownOwnership(batch AssignmentPlanBatch) bool {
	for _, issue := range batch.Issues {
		if len(issue.OwnedPaths) == 0 {
			return true
		}
	}
	return false
}

func conflictsWithBatch(issue AssignmentPlanIssue, batch AssignmentPlanBatch) []AssignmentPlanConflict {
	conflicts := make([]AssignmentPlanConflict, 0)
	for _, existing := range batch.Issues {
		paths := overlappingOwnedPaths(issue.OwnedPaths, existing.OwnedPaths)
		if len(paths) == 0 {
			continue
		}
		conflicts = append(conflicts, AssignmentPlanConflict{
			Issue:         issue.Issue,
			ConflictsWith: existing.Issue,
			Paths:         paths,
		})
	}
	return conflicts
}

func parseOwnedPaths(text string) []string {
	var paths []string
	collecting := false

	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			collecting = false
			continue
		}

		if match := ownershipLinePattern.FindStringSubmatch(trimmed); len(match) == 2 {
			collecting = true
			paths = append(paths, extractOwnedPathTokens(match[1])...)
			continue
		}

		if collecting {
			if looksLikeMarkdownHeading(trimmed) {
				collecting = false
				continue
			}
			paths = append(paths, extractOwnedPathTokens(trimmed)...)
		}
	}

	return uniqueSortedStrings(paths)
}

func extractOwnedPathTokens(text string) []string {
	matches := pathTokenPattern.FindAllStringSubmatch(text, -1)
	paths := make([]string, 0, len(matches))
	for _, match := range matches {
		raw := match[1]
		if raw == "" {
			raw = match[2]
		}
		ownedPath, ok := normalizeOwnedPath(raw)
		if ok {
			paths = append(paths, ownedPath)
		}
	}
	return paths
}

func normalizeOwnedPath(raw string) (string, bool) {
	value := strings.TrimSpace(raw)
	value = strings.Trim(value, "`'\"[](){}<>.,;:")
	value = strings.ReplaceAll(value, "\\", "/")
	value = strings.TrimPrefix(value, "./")
	if value == "" || strings.Contains(value, "://") || strings.HasPrefix(value, "#") || path.IsAbs(value) {
		return "", false
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || cleaned == "..." || strings.Contains(cleaned, " ") {
		return "", false
	}
	if !strings.Contains(cleaned, "/") && path.Ext(cleaned) == "" {
		return "", false
	}
	return cleaned, true
}

func overlappingOwnedPaths(left, right []string) []string {
	overlaps := make([]string, 0)
	for _, l := range left {
		for _, r := range right {
			if !ownedPathsOverlap(l, r) {
				continue
			}
			overlaps = append(overlaps, overlapPathLabel(l, r))
		}
	}
	return uniqueSortedStrings(overlaps)
}

func ownedPathsOverlap(left, right string) bool {
	left = strings.Trim(path.Clean(left), "/")
	right = strings.Trim(path.Clean(right), "/")
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}

func overlapPathLabel(left, right string) string {
	if len(left) <= len(right) {
		return left
	}
	return right
}

func looksLikeMarkdownHeading(line string) bool {
	if strings.HasPrefix(line, "#") {
		return true
	}
	return strings.HasSuffix(line, ":") && len(extractOwnedPathTokens(line)) == 0
}

func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

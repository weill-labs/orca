package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/weill-labs/orca/internal/project"
)

func (c *LocalController) Plan(ctx context.Context, req AssignmentPlanRequest) (AssignmentPlanResult, error) {
	projectPath, err := projectPathForPlan(req.Project)
	if err != nil {
		return AssignmentPlanResult{}, err
	}
	req.Project = projectPath

	candidates, err := assignmentPlanCandidatesFromProject(ctx, projectPath, req.Issues)
	if err != nil {
		return AssignmentPlanResult{}, err
	}
	return PlanParallelAssignments(req, candidates)
}

func projectPathForPlan(rawProject string) (string, error) {
	projectPath := strings.TrimSpace(rawProject)
	if projectPath == "" {
		return "", errors.New("project is required")
	}
	resolved, err := project.CanonicalPath(projectPath)
	if err != nil {
		return "", fmt.Errorf("resolve project: %w", err)
	}
	return resolved, nil
}

type beadsPlanIssue struct {
	Type            string `json:"_type"`
	ID              string `json:"id"`
	Title           string `json:"title"`
	Description     string `json:"description"`
	Status          string `json:"status"`
	IssueType       string `json:"issue_type"`
	ExternalRef     string `json:"external_ref"`
	DependencyCount int    `json:"dependency_count"`
}

func assignmentPlanCandidatesFromProject(ctx context.Context, projectPath string, requested []string) ([]AssignmentPlanCandidate, error) {
	issues, err := loadBeadsPlanIssues(ctx, filepath.Join(projectPath, ".beads", "issues.jsonl"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && len(requested) > 0 {
			return unknownAssignmentPlanCandidates(requested), nil
		}
		return nil, err
	}
	if len(requested) == 0 {
		return readyBeadsPlanCandidates(issues), nil
	}

	byRef := make(map[string]beadsPlanIssue, len(issues)*2)
	for _, issue := range issues {
		if issue.ID != "" {
			byRef[issue.ID] = issue
			byRef[strings.ToLower(issue.ID)] = issue
		}
		if issue.ExternalRef != "" {
			byRef[issue.ExternalRef] = issue
			byRef[strings.ToLower(issue.ExternalRef)] = issue
		}
	}

	candidates := make([]AssignmentPlanCandidate, 0, len(requested))
	for _, rawIssue := range normalizeIssueList(requested) {
		issue, ok := byRef[rawIssue]
		if !ok {
			issue, ok = byRef[strings.ToLower(rawIssue)]
		}
		if !ok {
			candidates = append(candidates, AssignmentPlanCandidate{Issue: rawIssue})
			continue
		}
		candidates = append(candidates, AssignmentPlanCandidate{
			Issue: rawIssue,
			Title: issue.Title,
			Body:  issue.Description,
		})
	}
	return candidates, nil
}

func loadBeadsPlanIssues(ctx context.Context, path string) ([]beadsPlanIssue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read beads issues for assignment plan: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	issues := make([]beadsPlanIssue, 0)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var issue beadsPlanIssue
		if err := json.Unmarshal([]byte(line), &issue); err != nil {
			return nil, fmt.Errorf("parse beads issue for assignment plan: %w", err)
		}
		if issue.Type != "" && issue.Type != "issue" {
			continue
		}
		issues = append(issues, issue)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read beads issues for assignment plan: %w", err)
	}
	return issues, nil
}

func readyBeadsPlanCandidates(issues []beadsPlanIssue) []AssignmentPlanCandidate {
	candidates := make([]AssignmentPlanCandidate, 0, len(issues))
	for _, issue := range issues {
		if issue.ID == "" || issue.Status != "open" || issue.IssueType == "epic" || issue.DependencyCount > 0 {
			continue
		}
		candidates = append(candidates, AssignmentPlanCandidate{
			Issue: beadsPlanIssueIdentifier(issue),
			Title: issue.Title,
			Body:  issue.Description,
		})
	}
	return candidates
}

func beadsPlanIssueIdentifier(issue beadsPlanIssue) string {
	if externalRef := strings.TrimSpace(issue.ExternalRef); externalRef != "" {
		return externalRef
	}
	return issue.ID
}

func unknownAssignmentPlanCandidates(issues []string) []AssignmentPlanCandidate {
	normalized := normalizeIssueList(issues)
	candidates := make([]AssignmentPlanCandidate, 0, len(normalized))
	for _, issue := range normalized {
		candidates = append(candidates, AssignmentPlanCandidate{Issue: issue})
	}
	return candidates
}

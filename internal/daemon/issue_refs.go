package daemon

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var gitHubIssueIdentifierPattern = regexp.MustCompile(`(?i)^gh-(\d+)$`)
var gitHubIssueAliasPattern = regexp.MustCompile(`^#(\d+)$`)

func normalizeIssueIdentifier(issue string) string {
	issue = strings.TrimSpace(issue)
	if number, ok := parseGitHubIssueNumber(issue); ok {
		return canonicalGitHubIssueIdentifier(number)
	}
	return issue
}

func NormalizeIssueIdentifier(issue string) string {
	return normalizeIssueIdentifier(issue)
}

func isGitHubIssueIdentifier(issue string) bool {
	_, ok := parseGitHubIssueNumber(issue)
	return ok
}

func parseGitHubIssueNumber(issue string) (int, bool) {
	issue = strings.TrimSpace(issue)
	if issue == "" {
		return 0, false
	}

	if match := gitHubIssueIdentifierPattern.FindStringSubmatch(issue); len(match) == 2 {
		number, err := strconv.Atoi(match[1])
		if err == nil && number > 0 {
			return number, true
		}
	}
	if match := gitHubIssueAliasPattern.FindStringSubmatch(issue); len(match) == 2 {
		number, err := strconv.Atoi(match[1])
		if err == nil && number > 0 {
			return number, true
		}
	}
	return 0, false
}

func canonicalGitHubIssueIdentifier(number int) string {
	return fmt.Sprintf("GH-%d", number)
}

func issuePromptAliases(issue string) []string {
	normalized := normalizeIssueIdentifier(issue)
	if normalized == "" {
		return nil
	}

	aliases := []string{normalized}
	if number, ok := parseGitHubIssueNumber(normalized); ok {
		aliases = append(aliases, fmt.Sprintf("#%d", number))
	}
	return aliases
}

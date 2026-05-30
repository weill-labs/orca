package daemon

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/weill-labs/orca/internal/project"
)

type enqueueProjectScope struct {
	requested string
	project   string
}

func newEnqueueProjectScope(projectPath string) enqueueProjectScope {
	requested := strings.TrimSpace(projectPath)
	resolved := project.NormalizeWorkerProjectPath(requested)
	if strings.TrimSpace(resolved) == "" {
		resolved = requested
	}
	return enqueueProjectScope{
		requested: requested,
		project:   resolved,
	}
}

func (s enqueueProjectScope) projectPath() string {
	return s.project
}

func (s enqueueProjectScope) activeAssignmentError(subject, target string) error {
	if s.resolvedFromClonePool() {
		return fmt.Errorf("%s is not associated with an active assignment under project %q (resolved from clone-pool path %q); retry with `%s`", subject, s.project, s.requested, s.retryCommand(target))
	}
	return fmt.Errorf("%s is not associated with an active assignment", subject)
}

func (s enqueueProjectScope) resolvedFromClonePool() bool {
	return s.requested != "" && s.project != "" && s.requested != s.project
}

func (s enqueueProjectScope) retryCommand(target string) string {
	return fmt.Sprintf("orca enqueue %s --project %s", shellToken(target), shellToken(s.project))
}

func shellToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "''"
	}
	if strings.ContainsAny(value, " \t\n\"'\\$`") {
		return strconv.Quote(value)
	}
	return value
}

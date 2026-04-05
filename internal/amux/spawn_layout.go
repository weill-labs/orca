package amux

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type sessionCapture struct {
	Panes []sessionCapturePane `json:"panes"`
	Error *captureCommandError `json:"error,omitempty"`
}

type sessionCapturePane struct {
	ID          uint64          `json:"id"`
	Name        string          `json:"name,omitempty"`
	Lead        bool            `json:"lead,omitempty"`
	ColumnIndex int             `json:"column_index"`
	Position    *capturePanePos `json:"position,omitempty"`
}

type capturePanePos struct {
	X      int `json:"x"`
	Y      int `json:"y"`
	Width  int `json:"width"`
	Height int `json:"height"`
}

type spawnPlacement struct {
	atPane     string
	rootLevel  bool
	horizontal bool
}

func (c *CLIClient) spawnPlacementArgs(ctx context.Context, session, leadPane string) ([]string, error) {
	layout, err := c.captureSessionLayout(ctx, session)
	if err != nil {
		return nil, err
	}

	placement := planSpawnPlacement(layout, leadPane)
	args := make([]string, 0, 4)
	if placement.atPane != "" {
		args = append(args, "--at", placement.atPane)
	}
	if placement.rootLevel {
		args = append(args, "--root")
	}
	if placement.horizontal {
		args = append(args, "--horizontal")
	} else {
		args = append(args, "--vertical")
	}
	return args, nil
}

func (c *CLIClient) captureSessionLayout(ctx context.Context, session string) (sessionCapture, error) {
	output, err := c.run(ctx, session, "capture", "--format", "json")
	if err != nil {
		return sessionCapture{}, err
	}

	var layout sessionCapture
	if err := json.Unmarshal(output, &layout); err != nil {
		return sessionCapture{}, fmt.Errorf("parse session capture json: %w", err)
	}
	if layout.Error != nil {
		return sessionCapture{}, paneCaptureError(layout.Error)
	}
	return layout, nil
}

func planSpawnPlacement(layout sessionCapture, leadPane string) spawnPlacement {
	trimmedLeadPane := strings.TrimSpace(leadPane)
	columns := make(map[int][]sessionCapturePane)
	for _, pane := range layout.Panes {
		if paneExcludedFromWorkerColumns(pane, trimmedLeadPane) {
			continue
		}
		columns[pane.ColumnIndex] = append(columns[pane.ColumnIndex], pane)
	}

	if len(columns) == 0 {
		return spawnPlacement{
			atPane:    trimmedLeadPane,
			rootLevel: true,
		}
	}

	columnIndexes := make([]int, 0, len(columns))
	for columnIndex := range columns {
		columnIndexes = append(columnIndexes, columnIndex)
	}
	sort.Ints(columnIndexes)

	maxPanesPerColumn := len(columnIndexes) + 1
	for _, columnIndex := range columnIndexes {
		panes := columns[columnIndex]
		if len(panes) >= maxPanesPerColumn {
			continue
		}
		return spawnPlacement{
			atPane:     paneRef(bottomMostPane(panes)),
			horizontal: true,
		}
	}

	rightmostColumn := columns[columnIndexes[len(columnIndexes)-1]]
	return spawnPlacement{
		atPane:    paneRef(bottomMostPane(rightmostColumn)),
		rootLevel: true,
	}
}

func paneExcludedFromWorkerColumns(pane sessionCapturePane, leadPane string) bool {
	if pane.Lead {
		return true
	}
	if leadPane == "" {
		return false
	}
	return paneMatchesRef(pane, leadPane)
}

func paneMatchesRef(pane sessionCapturePane, ref string) bool {
	if ref == "" {
		return false
	}
	if pane.Name != "" && pane.Name == ref {
		return true
	}
	if pane.ID == 0 {
		return false
	}
	return strconv.FormatUint(pane.ID, 10) == ref
}

func paneRef(pane sessionCapturePane) string {
	if pane.ID != 0 {
		return strconv.FormatUint(pane.ID, 10)
	}
	return pane.Name
}

func bottomMostPane(panes []sessionCapturePane) sessionCapturePane {
	best := panes[0]
	for _, pane := range panes[1:] {
		if paneRanksLower(best, pane) {
			best = pane
		}
	}
	return best
}

func paneRanksLower(current, candidate sessionCapturePane) bool {
	currentY, currentHasY := paneY(current)
	candidateY, candidateHasY := paneY(candidate)
	switch {
	case currentHasY && candidateHasY:
		if candidateY != currentY {
			return candidateY > currentY
		}
	case !currentHasY && candidateHasY:
		return true
	case currentHasY && !candidateHasY:
		return false
	}

	if candidate.ID != current.ID {
		return candidate.ID > current.ID
	}
	return candidate.Name > current.Name
}

func paneY(pane sessionCapturePane) (int, bool) {
	if pane.Position == nil {
		return 0, false
	}
	return pane.Position.Y, true
}

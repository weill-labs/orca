package amux

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestPlanSpawnPlacement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		layout   sessionCapture
		leadPane string
		want     spawnPlacement
	}{
		{
			name: "excludes lead pane passed by caller when there are no worker columns",
			layout: sessionCapture{
				Panes: []sessionCapturePane{
					{ID: 1, Name: "lead-pane", ColumnIndex: 0, Position: &capturePanePos{X: 0, Y: 0}},
				},
			},
			leadPane: "lead-pane",
			want: spawnPlacement{
				atPane:    "lead-pane",
				rootLevel: true,
			},
		},
		{
			name: "excludes amux lead pane even without caller override",
			layout: sessionCapture{
				Panes: []sessionCapturePane{
					{ID: 1, Name: "lead-pane", Lead: true, ColumnIndex: 0, Position: &capturePanePos{X: 0, Y: 0}},
				},
			},
			want: spawnPlacement{
				rootLevel: true,
			},
		},
		{
			name: "fills the first column with room using its bottom pane",
			layout: sessionCapture{
				Panes: []sessionCapturePane{
					{ID: 1, Name: "lead-pane", Lead: true, ColumnIndex: 0, Position: &capturePanePos{X: 0, Y: 0}},
					{ID: 2, Name: "worker-a", ColumnIndex: 1, Position: &capturePanePos{X: 40, Y: 0}},
					{ID: 4, Name: "worker-c", ColumnIndex: 1, Position: &capturePanePos{X: 40, Y: 12}},
					{ID: 3, Name: "worker-b", ColumnIndex: 2, Position: &capturePanePos{X: 80, Y: 0}},
				},
			},
			want: spawnPlacement{
				atPane:     "4",
				horizontal: true,
			},
		},
		{
			name: "creates a new column from the rightmost full column",
			layout: sessionCapture{
				Panes: []sessionCapturePane{
					{ID: 1, Name: "lead-pane", Lead: true, ColumnIndex: 0, Position: &capturePanePos{X: 0, Y: 0}},
					{ID: 2, Name: "worker-a", ColumnIndex: 1, Position: &capturePanePos{X: 40, Y: 0}},
					{ID: 3, Name: "worker-b", ColumnIndex: 1, Position: &capturePanePos{X: 40, Y: 8}},
					{ID: 4, Name: "worker-c", ColumnIndex: 1, Position: &capturePanePos{X: 40, Y: 16}},
					{ID: 5, Name: "worker-d", ColumnIndex: 2, Position: &capturePanePos{X: 80, Y: 0}},
					{ID: 6, Name: "worker-e", ColumnIndex: 2, Position: &capturePanePos{X: 80, Y: 8}},
					{ID: 7, Name: "worker-f", ColumnIndex: 2, Position: &capturePanePos{X: 80, Y: 16}},
				},
			},
			want: spawnPlacement{
				atPane:    "7",
				rootLevel: true,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := planSpawnPlacement(tt.layout, tt.leadPane); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("planSpawnPlacement() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func testSessionCaptureJSON(tb testing.TB, layout sessionCapture) string {
	tb.Helper()

	data, err := json.Marshal(layout)
	if err != nil {
		tb.Fatalf("json.Marshal(layout) error = %v", err)
	}
	return string(data)
}

package daemon

import (
	"context"
	"strings"
	"testing"

	"github.com/weill-labs/orca/internal/worksource"
)

func TestRunPullTickLogsSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		enabled     bool
		freeClones  int
		readyItems  []worksource.WorkItem
		claimErrs   map[string]error
		startDaemon bool
		wantLog     string
	}{
		{
			name:       "disabled worksource does not log",
			enabled:    false,
			freeClones: 1,
		},
		{
			name:       "no free clones logs idle tick",
			enabled:    true,
			freeClones: 0,
			wantLog:    "daemon worksource pull tick: free_clones=0 ready=0 dispatched=0 skipped_already_claimed=0",
		},
		{
			name:       "free clones with no ready work logs idle tick",
			enabled:    true,
			freeClones: 2,
			wantLog:    "daemon worksource pull tick: free_clones=2 ready=0 dispatched=0 skipped_already_claimed=0",
		},
		{
			name:       "dispatch and already claimed skips are summarized",
			enabled:    true,
			freeClones: 3,
			readyItems: []worksource.WorkItem{
				{ID: "LAB-701", Body: "Claimed elsewhere"},
				{ID: "LAB-702", Body: "Dispatch first"},
				{ID: "LAB-703", Body: "Dispatch second"},
			},
			claimErrs: map[string]error{
				"LAB-701": worksource.ErrAlreadyClaimed,
			},
			startDaemon: true,
			wantLog:     "daemon worksource pull tick: free_clones=3 ready=3 dispatched=2 skipped_already_claimed=1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			configurePullTestClones(t, deps, tt.freeClones)
			if tt.freeClones > 0 {
				deps.amux.spawnPanes = pullTestPanes(tt.freeClones)
			}
			logs := &fakeLogSink{}
			source := &fakePullWorkSource{
				readyItems: tt.readyItems,
				claimErrs:  tt.claimErrs,
			}
			d := deps.newDaemonWithOptions(t, func(opts *Options) {
				opts.WorkSourceEnabled = tt.enabled
				opts.WorkSource = source
				opts.Logf = logs.Printf
			})
			if tt.startDaemon {
				if err := d.Start(context.Background()); err != nil {
					t.Fatalf("Start() error = %v", err)
				}
				t.Cleanup(func() {
					_ = d.Stop(context.Background())
				})
			}

			d.runPullTick(context.Background())

			message, ok := findWorkSourcePullTickLog(logs.messages())
			if tt.wantLog == "" {
				if ok {
					t.Fatalf("worksource pull tick log = %q, want none", message)
				}
				return
			}
			if !ok {
				t.Fatalf("missing worksource pull tick log in %#v", logs.messages())
			}
			if message != tt.wantLog {
				t.Fatalf("worksource pull tick log = %q, want %q", message, tt.wantLog)
			}
		})
	}
}

func findWorkSourcePullTickLog(messages []string) (string, bool) {
	for _, message := range messages {
		if strings.HasPrefix(message, "daemon worksource pull tick:") {
			return message, true
		}
	}
	return "", false
}

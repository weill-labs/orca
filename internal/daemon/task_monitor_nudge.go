package daemon

import (
	"context"
	"sync"
)

type taskMonitorNudge func(context.Context, *Daemon, *TaskStateUpdate)

const taskMonitorNudgeConcurrency = 4

func (d *Daemon) executeTaskMonitorNudges(ctx context.Context, results []taskMonitorResult) {
	limit := min(len(results), taskMonitorNudgeConcurrency)
	if limit == 0 {
		return
	}

	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup

launchLoop:
	for i := range results {
		if !results[i].update.hasNudges() {
			continue
		}

		select {
		case <-ctx.Done():
			break launchLoop
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(result *taskMonitorResult) {
			defer wg.Done()
			defer func() {
				<-sem
			}()

			result.update.runNudges(ctx, d)
		}(&results[i])
	}

	wg.Wait()
}

func (u *TaskStateUpdate) queueNudge(nudge taskMonitorNudge) {
	if nudge == nil {
		return
	}
	u.nudges = append(u.nudges, nudge)
}

func (u *TaskStateUpdate) hasNudges() bool {
	return len(u.nudges) > 0
}

func (u *TaskStateUpdate) runNudges(ctx context.Context, d *Daemon) {
	for _, nudge := range u.nudges {
		if ctx.Err() != nil {
			break
		}
		nudge(ctx, d, u)
	}
	u.nudges = nil
}

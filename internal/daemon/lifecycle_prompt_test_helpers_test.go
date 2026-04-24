package daemon

import (
	"errors"
	"time"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

func setLifecyclePromptActiveAfterIdleProbes(deps *testDeps, idleProbes int) {
	probeCalls := 0
	deps.amux.waitIdleHook = func(_ string, timeout, _ time.Duration) {
		if timeout != codexPromptRetryIdleProbeTime {
			deps.amux.waitIdleErr = nil
			return
		}
		probeCalls++
		if probeCalls > idleProbes {
			deps.amux.waitIdleErr = errors.New("idle timeout")
			return
		}
		deps.amux.waitIdleErr = nil
	}
}

type pasteCollapseSubmitScenario struct {
	idleWaits       int
	workingObserved bool
}

func simulateCodexSubmitPasteCollapse(deps *testDeps, idleBeforeActive int) *pasteCollapseSubmitScenario {
	scenario := &pasteCollapseSubmitScenario{}
	deps.amux.waitIdleHook = func(_ string, timeout, settle time.Duration) {
		if timeout != codexPromptRetryIdleProbeTime {
			return
		}
		if settle != 0 {
			deps.amux.waitIdleErr = errors.New("unexpected settle during paste-collapse probe")
			return
		}
		scenario.idleWaits++
		if scenario.idleWaits > idleBeforeActive {
			scenario.workingObserved = true
			deps.amux.waitIdleErr = errors.New("idle timeout")
			return
		}
		deps.amux.waitIdleErr = nil
	}
	deps.amux.waitContentFunc = func(_ string, substring string, timeout time.Duration) (bool, error) {
		if substring != codexWorkingText || timeout != codexPromptRetryIdleProbeTime {
			return false, nil
		}
		if scenario.workingObserved {
			return true, nil
		}
		return true, amuxapi.ErrWaitContentTimeout
	}
	return scenario
}

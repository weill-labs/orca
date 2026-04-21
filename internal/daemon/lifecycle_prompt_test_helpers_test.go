package daemon

import (
	"errors"
	"time"
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

# Task Lifecycle FSM Design

Date: 2026-04-20. Related: [LAB-1408](https://linear.app/weill-labs/issue/LAB-1408/introduce-a-finite-state-machine-for-task-lifecycle-design-spike)

## Goal

Replace the current task-state "bag of facts" derivation with one authoritative
finite state machine that:

- defines every allowed transition in one table,
- rejects impossible transitions loudly,
- centralizes side effects as named hooks instead of scattering them across
  pollers, and
- preserves the current DB contract during migration.

This is a design spike only. It does not propose implementation in this change.

## Out Of Scope

- Collapsing `task.Status` and `task.State` into one field.
- Changing the DB schema.
- Changing persisted task-state strings in `tasks.state`.
- Reworking resume semantics beyond what is needed to keep the migration safe.

## Current-State Audit

Today there is no single authority for task state.

### Direct `setTaskState` callers

| File | Current behavior |
| --- | --- |
| `internal/daemon/prpoll.go` | Sets `pr_detected`, `merged`, and `done` for PR discovery and PR terminal states. |
| `internal/daemon/ci.go` | Sets `ci_pending` and `review_pending` from CI buckets. |
| `internal/daemon/task_state.go` | Sets `escalated` from stuck/pane-missing escalation paths. |

### Other paths that mutate or derive task state

| File | Current behavior |
| --- | --- |
| `internal/daemon/review.go` | On worker output change, recomputes state with `taskStateForAssignment(...)`. This is how escalated workers "recover" today. |
| `internal/daemon/exited.go` | After automatic pane restart, recomputes state with `taskStateForAssignment(...)`. |
| `internal/daemon/recovery.go` | Startup reconciliation writes `escalated` or terminal `done` directly. |
| `internal/daemon/postmortem.go` | `finishAssignment(...)` always writes terminal `done` after wrap-up/cleanup. |
| `internal/daemon/assign.go` | Initializes tasks with `initialTaskState(...)`. |
| `internal/daemon/relay.go` and `internal/daemon/reconcile_pr.go` | Bind PR numbers and emit `pr.detected`, but often rely on later reads to derive `pr_detected`. |
| `internal/daemon/runtime_adapters.go` | `convertStateTask(...)` runs `normalizeTaskState(...)`; `convertAssignment(...)` runs `taskStateForAssignment(...)`. Read-time hydration changes the visible state without any transition record. |
| `internal/daemon/task_monitor_actor.go` | Normalizes state before choosing which monitors run. |
| `internal/daemon/resume.go` | Resets `task.Status` to `active` but keeps `task.State` as-is; later reads repair the state via hydration. |
| `internal/daemonstate/sqlite.go` and `postgres_*` | Backfill empty `tasks.state` values with `assigned`, `pr_detected`, or `done`. |

### Important mismatches the FSM must account for

1. `PR closed without merge` currently produces `task.Status = failed` and
   persisted `task.State = "done"`. It is not a `Cancelled` transition today.
2. CI nudge exhaustion and review nudge exhaustion do not currently move the
   task into `escalated`. They keep the task in `ci_pending` or
   `review_pending` and emit worker-level escalation events.
3. `Escalated -> recovered` is not always `Escalated -> PRDetected`. The
   current code resumes to `assigned`, `pr_detected`, `ci_pending`, or
   `review_pending` depending on worker facts.
4. Several code paths update facts like `PRNumber`, `LastCIState`, or review
   counters and rely on later hydration to "discover" the state. The migration
   needs an explicit legacy hydration layer while those call sites are being
   moved.

## Proposed Package

Introduce a pure package at `internal/daemon/taskfsm`.

The package is intentionally deterministic and side-effect free. It should not
know about amux, GitHub, Linear, or `TaskStateUpdate`. It only consumes a typed
snapshot and returns a transition result plus named hook invocations.

## Type Signatures

```go
package taskfsm

type State uint8

const (
	StateAssigned State = iota
	StatePRDetected
	StateCIPending
	StateReviewPending
	StateMerged
	StateEscalated
	StateDone
	StateCancelled
	StateFailed
)

type EventKind uint8

const (
	EventPRDiscovered EventKind = iota
	EventCIObserved
	EventPRMergedObserved
	EventPRClosedObserved
	EventReviewObserved
	EventReviewApprovedObserved
	EventMergeConflictObserved
	EventWorkerEscalated
	EventWorkerRecovered
	EventUserCancelled
	EventLifecycleCompleted
	EventFatalErrorObserved
)

type Event interface {
	Kind() EventKind
}

type Snapshot struct {
	TaskStatus            string
	PersistedTaskState    string
	PRNumber              int
	WorkerHealth          string
	LastCIState           string
	LastMergeableState    string
	LastReviewCount       int
	LastInlineCommentCount int
	LastIssueCommentCount int
	ReviewNudgeCount      int
	CINudgeCount          int
	CIEscalated           bool
	ReviewApproved        bool
}

type ResumeTarget uint8

const (
	ResumeAssigned ResumeTarget = iota
	ResumePRDetected
	ResumeCIPending
	ResumeReviewPending
)

type PRDiscovered struct {
	PRNumber int
	Source   string // prpoll, relay, reconcile
}

type CIObserved struct {
	Bucket string // pending, fail, pass, cancel, skipping
}

type ReviewObserved struct {
	BlockingCount int
	Idle          bool
	NudgeBudgetExhausted bool
}

type MergeConflictObserved struct {
	State string // CONFLICTING, MERGEABLE, UNKNOWN...
}

type WorkerEscalated struct {
	Reason string
}

type WorkerRecovered struct {
	Target ResumeTarget
}

type LifecycleCompleted struct {
	Outcome string // done, cancelled, failed
}

type FatalErrorObserved struct {
	Outcome string // failed or cancelled
	Reason  string
}

type Guard func(Snapshot, Event) error

type HookID string

const (
	HookSyncPRMetadata             HookID = "sync_pr_metadata"
	HookEmitPRDetected             HookID = "emit_pr_detected"
	HookPersistCIObservation       HookID = "persist_ci_observation"
	HookMaybeQueueCINudge          HookID = "maybe_queue_ci_nudge"
	HookMaybeEmitCIEscalation      HookID = "maybe_emit_ci_escalation"
	HookPersistReviewObservation   HookID = "persist_review_observation"
	HookMaybeQueueReviewNudge      HookID = "maybe_queue_review_nudge"
	HookMaybeEmitReviewApproved    HookID = "maybe_emit_review_approved"
	HookMaybeEmitReviewEscalation  HookID = "maybe_emit_review_escalation"
	HookMaybeQueueConflictNudge    HookID = "maybe_queue_conflict_nudge"
	HookMarkWorkerEscalated        HookID = "mark_worker_escalated"
	HookEmitWorkerEscalated        HookID = "emit_worker_escalated"
	HookClearWorkerEscalation      HookID = "clear_worker_escalation"
	HookEmitWorkerRecovered        HookID = "emit_worker_recovered"
	HookEmitPRClosed               HookID = "emit_pr_closed"
	HookEmitPRMerged               HookID = "emit_pr_merged"
	HookRequestCompletion          HookID = "request_completion"
)

type HookInvocation struct {
	ID   HookID
	Args map[string]string
}

type Transition struct {
	From   State
	On     EventKind
	Guard  Guard
	To     func(Snapshot, Event) (State, error)
	Enter  []HookID
}

type Result struct {
	From           State
	To             State
	Changed        bool
	PersistedState string
	TerminalStatus string // "", "done", "cancelled", "failed"
	Hooks          []HookInvocation
}

type FSM interface {
	Apply(current State, snapshot Snapshot, event Event) (Result, error)
	HydrateLegacy(snapshot Snapshot) State
}
```

## Persistence Contract

The FSM should expose more precise internal states than the DB can store today.

```go
func PersistedTaskState(state State) string {
	switch state {
	case StateAssigned:
		return "assigned"
	case StatePRDetected:
		return "pr_detected"
	case StateCIPending:
		return "ci_pending"
	case StateReviewPending:
		return "review_pending"
	case StateMerged:
		return "merged"
	case StateEscalated:
		return "escalated"
	case StateDone, StateCancelled, StateFailed:
		return "done"
	default:
		panic("unknown taskfsm.State")
	}
}
```

This keeps `tasks.state` backward-compatible while allowing the FSM to model the
logical distinction between `Done`, `Cancelled`, and `Failed`. `task.Status`
remains the authoritative terminal outcome until a separate schema decision is
made.

## Authoritative Runtime Transition Table

This table is the target source of truth for live task lifecycle changes after
call sites are migrated. Self-loops are explicit because they still carry hooks.

| From | Event | Guard / qualifier | To | Hooks |
| --- | --- | --- | --- | --- |
| `Assigned` | `PRDiscovered` | `PRNumber > 0` | `PRDetected` | `sync_pr_metadata`, `emit_pr_detected` |
| `Assigned` | `WorkerEscalated` | none | `Escalated` | `mark_worker_escalated`, `emit_worker_escalated` |
| `Assigned` | `UserCancelled` | none | `Cancelled` | `request_completion` |
| `Assigned` | `FatalErrorObserved` | `Outcome == failed` | `Failed` | `request_completion` |
| `PRDetected` | `CIObserved` | `Bucket in {pending, fail}` | `CIPending` | `persist_ci_observation`, `maybe_queue_ci_nudge`, `maybe_emit_ci_escalation` |
| `PRDetected` | `CIObserved` | `Bucket in {pass, cancel, skipping}` | `ReviewPending` | `persist_ci_observation` |
| `PRDetected` | `ReviewObserved` | any | `PRDetected` | `persist_review_observation`, `maybe_queue_review_nudge`, `maybe_emit_review_escalation` |
| `PRDetected` | `ReviewApprovedObserved` | none | `PRDetected` | `persist_review_observation`, `maybe_emit_review_approved` |
| `PRDetected` | `MergeConflictObserved` | `State == CONFLICTING` | `PRDetected` | `maybe_queue_conflict_nudge` |
| `PRDetected` | `PRMergedObserved` | none | `Merged` | `emit_pr_merged`, `request_completion` |
| `PRDetected` | `PRClosedObserved` | none | `Failed` | `emit_pr_closed`, `request_completion` |
| `PRDetected` | `WorkerEscalated` | none | `Escalated` | `mark_worker_escalated`, `emit_worker_escalated` |
| `PRDetected` | `UserCancelled` | none | `Cancelled` | `request_completion` |
| `PRDetected` | `FatalErrorObserved` | `Outcome == failed` | `Failed` | `request_completion` |
| `CIPending` | `CIObserved` | `Bucket == pending` | `CIPending` | `persist_ci_observation` |
| `CIPending` | `CIObserved` | `Bucket == fail` | `CIPending` | `persist_ci_observation`, `maybe_queue_ci_nudge`, `maybe_emit_ci_escalation` |
| `CIPending` | `CIObserved` | `Bucket in {pass, cancel, skipping}` | `ReviewPending` | `persist_ci_observation` |
| `CIPending` | `ReviewObserved` | any | `CIPending` | `persist_review_observation`, `maybe_queue_review_nudge`, `maybe_emit_review_escalation` |
| `CIPending` | `ReviewApprovedObserved` | none | `CIPending` | `persist_review_observation`, `maybe_emit_review_approved` |
| `CIPending` | `MergeConflictObserved` | `State == CONFLICTING` | `CIPending` | `maybe_queue_conflict_nudge` |
| `CIPending` | `PRMergedObserved` | none | `Merged` | `emit_pr_merged`, `request_completion` |
| `CIPending` | `PRClosedObserved` | none | `Failed` | `emit_pr_closed`, `request_completion` |
| `CIPending` | `WorkerEscalated` | none | `Escalated` | `mark_worker_escalated`, `emit_worker_escalated` |
| `CIPending` | `UserCancelled` | none | `Cancelled` | `request_completion` |
| `CIPending` | `FatalErrorObserved` | `Outcome == failed` | `Failed` | `request_completion` |
| `ReviewPending` | `ReviewObserved` | `BlockingCount == 0` | `ReviewPending` | `persist_review_observation` |
| `ReviewPending` | `ReviewObserved` | `BlockingCount > 0 && !Idle` | `ReviewPending` | `persist_review_observation` |
| `ReviewPending` | `ReviewObserved` | `BlockingCount > 0 && Idle && !NudgeBudgetExhausted` | `ReviewPending` | `persist_review_observation`, `maybe_queue_review_nudge` |
| `ReviewPending` | `ReviewObserved` | `BlockingCount > 0 && NudgeBudgetExhausted` | `ReviewPending` | `persist_review_observation`, `maybe_emit_review_escalation` |
| `ReviewPending` | `ReviewApprovedObserved` | none | `ReviewPending` | `persist_review_observation`, `maybe_emit_review_approved` |
| `ReviewPending` | `MergeConflictObserved` | `State == CONFLICTING` | `ReviewPending` | `maybe_queue_conflict_nudge` |
| `ReviewPending` | `PRMergedObserved` | none | `Merged` | `emit_pr_merged`, `request_completion` |
| `ReviewPending` | `PRClosedObserved` | none | `Failed` | `emit_pr_closed`, `request_completion` |
| `ReviewPending` | `WorkerEscalated` | none | `Escalated` | `mark_worker_escalated`, `emit_worker_escalated` |
| `ReviewPending` | `UserCancelled` | none | `Cancelled` | `request_completion` |
| `ReviewPending` | `FatalErrorObserved` | `Outcome == failed` | `Failed` | `request_completion` |
| `Escalated` | `WorkerRecovered` | `Target == ResumeAssigned` | `Assigned` | `clear_worker_escalation`, `emit_worker_recovered` |
| `Escalated` | `WorkerRecovered` | `Target == ResumePRDetected` | `PRDetected` | `clear_worker_escalation`, `emit_worker_recovered` |
| `Escalated` | `WorkerRecovered` | `Target == ResumeCIPending` | `CIPending` | `clear_worker_escalation`, `emit_worker_recovered` |
| `Escalated` | `WorkerRecovered` | `Target == ResumeReviewPending` | `ReviewPending` | `clear_worker_escalation`, `emit_worker_recovered` |
| `Escalated` | `UserCancelled` | none | `Cancelled` | `request_completion` |
| `Escalated` | `FatalErrorObserved` | `Outcome == failed` | `Failed` | `request_completion` |
| `Merged` | `LifecycleCompleted` | `Outcome == done` | `Done` | none |
| `Merged` | `LifecycleCompleted` | `Outcome == failed` | `Failed` | none |
| `Merged` | `LifecycleCompleted` | `Outcome == cancelled` | `Cancelled` | none |

### Notes on deliberate corrections to the issue sketch

- `PRClosedObserved` maps to `Failed`, not `Cancelled`, because that is what the
  current daemon does.
- CI/review nudge exhaustion is modeled as a self-loop with escalation hooks,
  not a forced transition to `Escalated`, because that matches current behavior.
- Review and merge-conflict hooks stay legal from `PRDetected` and `CIPending`,
  because those pollers already run before the task becomes `review_pending`.
- Recovery from `Escalated` is parameterized by a resume target, because the
  current daemon resumes to different non-terminal states based on worker facts.
- `Merged` stays explicit even though current code usually collapses it to
  `done` in the same update. The explicit state makes hooks reviewable.

## Legacy Hydration And Reconciliation Table

The end-state FSM above should not encode the current read-time derivation hacks
as normal transitions. Instead, the migration keeps a separate
`HydrateLegacy(snapshot)` adapter until every writer emits real events.

| Legacy snapshot condition | Hydrated state | Current sources |
| --- | --- | --- |
| `TaskStatus in {done, cancelled, failed}` | `Done`, `Cancelled`, or `Failed` | `normalizeTaskState`, `taskStateForAssignment`, storage backfill |
| `WorkerHealth == escalated` | `Escalated` | `taskStateForAssignment`, startup reconciliation |
| `PersistedTaskState == "merged"` | `Merged` | `taskStateForAssignment` preserves merged until completion |
| `LastCIState in {pending, fail}` | `CIPending` | `taskStateForAssignment` |
| `LastCIState in {pass, cancel, skipping}` | `ReviewPending` | `taskStateForAssignment` |
| `PersistedTaskState == "review_pending"` | `ReviewPending` | `taskStateForAssignment` |
| `LastMergeableState != ""` | `ReviewPending` | `taskStateForAssignment` fallback |
| `LastReviewCount > 0 || LastInlineCommentCount > 0 || LastIssueCommentCount > 0 || ReviewNudgeCount > 0` | `ReviewPending` | `taskStateForAssignment` fallback |
| `PRNumber > 0` | `PRDetected` | `normalizeTaskState`, storage backfill |
| otherwise | `Assigned` | `initialTaskState`, `normalizeTaskState`, storage backfill |

### Startup-only reconciliation transitions

These are not normal runtime events, but the migration needs to keep them
explicit because they currently write task state directly.

| Situation | Result | Current source |
| --- | --- | --- |
| Persisted task has no pane ID on daemon startup | `Failed` | `recovery.go: failTaskWithoutWorker(...)` |
| Persisted task pane exists check fails or capture fails | `Escalated` | `recovery.go: escalateAssignmentError(...)` |
| Persisted task pane is missing on daemon startup | `Escalated` | `recovery.go: escalateAssignmentError(...)` |
| Persisted task was `starting` and pane exited on startup | `Failed` | `recovery.go: failAssignment(...)` |
| Exited pane auto-restart succeeds | `WorkerRecovered(Target=...)` | `exited.go` via `taskStateForAssignment(...)` |

## Guard And Hook Design

### Guard rules

- Guards are pure functions. They inspect `Snapshot` plus the typed event and
  either allow the transition or return an error.
- Guards do not mutate worker counters, queue nudges, or emit events.
- Impossible transitions return an error immediately. Example:
  `Apply(StateDone, PRDiscovered{PRNumber: 42})` should fail loudly instead of
  silently overwriting terminal state.

### Hook rules

- Hooks are named in the transition table by `HookID`.
- The FSM returns hook invocations; it does not run them.
- The daemon owns the hook dispatcher. That dispatcher translates hook IDs into
  existing `TaskStateUpdate` mutations, emitted events, queued nudges, pane
  metadata changes, and completion requests.
- Hooks may inspect the event payload, but only after the transition has been
  validated and the next state chosen.

This split keeps the FSM table reviewable while still letting the daemon reuse
its current side-effect machinery.

### Suggested daemon-side dispatcher shape

```go
type HookContext struct {
	Now      time.Time
	From     taskfsm.State
	To       taskfsm.State
	Snapshot taskfsm.Snapshot
	Event    taskfsm.Event
}

type HookRunner interface {
	Run(ctx context.Context, hook taskfsm.HookInvocation, hc HookContext, update *TaskStateUpdate) error
}
```

`TaskStateUpdate` remains the integration boundary for the existing daemon. The
FSM package just decides state and hook IDs.

### Concrete example: `PRDetected + CIObserved(fail) -> CIPending`

Input:

```go
current := taskfsm.StatePRDetected
event := taskfsm.CIObserved{Bucket: "fail"}
```

Result:

```go
taskfsm.Result{
	From:           taskfsm.StatePRDetected,
	To:             taskfsm.StateCIPending,
	Changed:        true,
	PersistedState: "ci_pending",
	Hooks: []taskfsm.HookInvocation{
		{ID: taskfsm.HookPersistCIObservation},
		{ID: taskfsm.HookMaybeQueueCINudge},
		{ID: taskfsm.HookMaybeEmitCIEscalation},
	},
}
```

Daemon-side behavior:

1. `persist_ci_observation` updates `Worker.LastCIState`, resets or increments
   CI counters, and marks the worker dirty.
2. `maybe_queue_ci_nudge` decides whether this `fail` bucket should send the
   existing "fix CI and push" prompt.
3. `maybe_emit_ci_escalation` emits `worker.ci_escalated` only when the CI
   nudge budget is exhausted.

The current transition and its side effects become visible in one place instead
of being split across `task_state.go` and `ci.go`.

### Concrete example: `Escalated + WorkerRecovered(Target=ReviewPending) -> ReviewPending`

Input:

```go
current := taskfsm.StateEscalated
event := taskfsm.WorkerRecovered{Target: taskfsm.ResumeReviewPending}
```

Result:

```go
taskfsm.Result{
	From:           taskfsm.StateEscalated,
	To:             taskfsm.StateReviewPending,
	Changed:        true,
	PersistedState: "review_pending",
	Hooks: []taskfsm.HookInvocation{
		{ID: taskfsm.HookClearWorkerEscalation},
		{ID: taskfsm.HookEmitWorkerRecovered},
	},
}
```

This matches the real behavior better than hard-coding `Escalated -> PRDetected`.

## Migration Plan

### Phase 1: Introduce the FSM package beside the existing code

Ship `internal/daemon/taskfsm` with:

- typed `State` and `Event` definitions,
- the authoritative transition table,
- `PersistedTaskState(...)`,
- `HydrateLegacy(snapshot)`, and
- table-driven tests that prove every row and every impossible transition.

No behavior change in this phase. Existing call sites continue to mutate
`TaskStateUpdate` as they do today.

### Phase 2: Add compare mode, but keep legacy writes authoritative

Add a daemon helper that computes:

1. `legacyState` from current code, and
2. `fsmState` from `HydrateLegacy(snapshot)` or `Apply(...)`.

If they differ, emit a trace event or test failure. Do not write the FSM result
yet. This catches mismatches before migration deletes the derivers.

### Phase 3: Migrate writers by concern

Migrate one concern at a time so each change stays reviewable:

1. `PR detection + PR terminal states`
   Files: `prpoll.go`, `relay.go`, `reconcile_pr.go`
2. `CI transitions`
   Files: `ci.go`
3. `Review + conflict self-loops`
   Files: `review.go`, `conflict.go`
4. `Worker escalation + recovery`
   Files: `task_state.go`, `review.go`, `exited.go`, `recovery.go`
5. `Assignment/startup/resume hydration`
   Files: `assign.go`, `runtime_adapters.go`, `task_monitor_actor.go`,
   `resume.go`, `daemonstate/*`
6. `Completion`
   Files: `prpoll.go`, `postmortem.go`, `daemon.go`

Each migrated caller should:

- construct a typed event,
- call `fsm.Apply(...)`,
- write `result.PersistedState` back to `task.State`,
- copy `result.TerminalStatus` into `task.Status` when present, and
- run the returned hooks through the daemon hook dispatcher.

### Phase 4: Delete the legacy derivers

After all writers have moved:

- delete `normalizeTaskState(...)`,
- delete `taskStateForAssignment(...)`,
- delete `taskStateForCIState(...)`,
- delete `setTaskState(...)`, and
- collapse read-time normalization to `HydrateLegacy(...)` only for old rows, or
  delete it too if all persisted rows are guaranteed to be explicit.

## Child Issues To Open After Approval

1. Introduce `internal/daemon/taskfsm` alongside existing derivers, with
   exhaustive transition-table tests and no behavior change.
2. Migrate daemon call sites to `fsm.Apply(event)`, delete
   `taskStateForAssignment(...)`, `normalizeTaskState(...)`, and
   `setTaskState(...)`.

## Review Checklist

A reviewer should be able to answer the following from this document alone:

- From state `X` and event `Y`, is the transition allowed?
- If allowed, what is the next typed state?
- What persisted `tasks.state` string is written?
- Which hooks run on entry, and which file family owns them today?
- Which current behaviors are preserved exactly, and which issue-sketch rows were
  corrected after reading the code?

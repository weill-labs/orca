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

## Preferred Architecture

The preferred design is not "many daemon call sites import a shared FSM helper."
It is "each active task owns a lifecycle actor, and that actor owns the FSM."

This should build on patterns orca already uses:

- `internal/daemon/task_monitor_actor.go` already provides one goroutine plus
  mailbox per active task.
- `internal/daemon/merge_queue_actor.go` already provides actor-style serialized
  ownership of one workflow with an update channel back into the daemon.

The design for LAB-1408 should evolve the current `TaskMonitor` into a
`TaskLifecycleActor` rather than introducing a second competing per-task
concurrency model.

Because the actor stores `Task`, `Worker`, and `taskMonitorNudge`, the actor and
machine types should live in `internal/daemon`, not a separate
`internal/daemon/taskfsm` package.

### Actor model

- One actor per active task.
- One goroutine plus one inbox channel per actor.
- Pollers and external commands become event producers only.
- The actor is the sole caller of `fsm.Apply(...)`.
- On-enter hooks run inside the actor while it has exclusive access to the
  task's working snapshot.
- The persisted task/worker rows remain authoritative. The actor keeps only an
  ephemeral working copy rebuilt from the store on bootstrap or restart.

This keeps transitions atomic per task without changing the larger daemon model:
the daemon still reconstructs runtime orchestration from persisted rows on
startup instead of depending on long-lived in-memory state.

### Event producers

| Producer | Emits |
| --- | --- |
| `prpoll.go`, `relay.go`, `reconcile_pr.go` | `PRDiscovered`, `PRMergedObserved`, `PRClosedObserved` |
| `ci.go` | `CIObserved` |
| `review.go` | `ReviewObserved`, `ReviewApprovedObserved`, `WorkerRecovered` |
| `conflict.go` | `MergeConflictObserved` |
| `task_state.go`, `recovery.go`, exited-pane paths | `WorkerEscalated`, `FatalErrorObserved`, `WorkerRecovered` |
| `daemon.go` cancel path | `UserCancelled` |
| `postmortem.go` / completion path | `LifecycleCompleted` |

### Why actor-owned FSM is the preferred shape

1. It eliminates the race class this issue exists to remove: multiple poll paths
   producing concurrent state writes for the same task.
2. It gives `onEnter` hooks a natural home with exclusive access to the
   task-local snapshot.
3. It aligns with existing orca code. This is an evolution of current actor
   patterns, not a novel concurrency subsystem.
4. Goroutine overhead is negligible at orca's current scale. Roughly 20 active
   tasks means 20 mailbox goroutines, which is operationally trivial.
5. Actor bootstrap is already conceptually present: `reconcileNonTerminalAssignments`
   plus per-row actor creation. Completion/cancel tears the actor down.
6. Cross-cutting concerns such as circuit breaking and retry budgets can live in
   the producer/supervisor layer instead of being smeared into transition code.

### Relationship to the stateless-daemon rule

Orca's source of truth must remain the DB row, not the goroutine. The actor owns
serialization, not authority.

- The actor's `task` and `worker` snapshot is a disposable cache of the current
  persisted row.
- The actor must durably persist task/worker changes before acknowledging an
  event as applied.
- If an actor dies, the daemon rebuilds it by rereading the row from the store.
- Startup reconciliation remains the mechanism that recreates actors after
  daemon restart.

This is how the actor model fits the repo's recoverability goals without turning
the in-memory goroutine into a hidden source of truth.

## Type Signatures

```go
package daemon

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

type PRDiscovered struct {
	PRNumber int
	Source   string // prpoll, relay, reconcile
}

func (PRDiscovered) Kind() EventKind { return EventPRDiscovered }

type CIObserved struct {
	Bucket string // pending, fail, pass, cancel, skipping
}

func (CIObserved) Kind() EventKind { return EventCIObserved }

type PRMergedObserved struct{}

func (PRMergedObserved) Kind() EventKind { return EventPRMergedObserved }

type PRClosedObserved struct{}

func (PRClosedObserved) Kind() EventKind { return EventPRClosedObserved }

type ReviewObserved struct {
	BlockingCount        int
	Idle                 bool
	NudgeBudgetExhausted bool
}

func (ReviewObserved) Kind() EventKind { return EventReviewObserved }

type ReviewApprovedObserved struct{}

func (ReviewApprovedObserved) Kind() EventKind { return EventReviewApprovedObserved }

type MergeConflictObserved struct {
	State string // CONFLICTING, MERGEABLE, UNKNOWN...
}

func (MergeConflictObserved) Kind() EventKind { return EventMergeConflictObserved }

type WorkerEscalated struct {
	Reason string
}

func (WorkerEscalated) Kind() EventKind { return EventWorkerEscalated }

type WorkerRecovered struct{}

func (WorkerRecovered) Kind() EventKind { return EventWorkerRecovered }

type UserCancelled struct{}

func (UserCancelled) Kind() EventKind { return EventUserCancelled }

type LifecycleCompleted struct {
	Outcome string // done, cancelled, failed
}

func (LifecycleCompleted) Kind() EventKind { return EventLifecycleCompleted }

type FatalErrorObserved struct {
	Outcome string // always "failed"
	Reason  string
}

func (FatalErrorObserved) Kind() EventKind { return EventFatalErrorObserved }

type TaskLifecycleActor struct {
	key    string
	daemon *Daemon
	inbox  chan TaskEventEnvelope
	done   chan struct{}

	// Ephemeral working copy. The persisted row is authoritative.
	task   Task
	worker Worker
	fsm    *Machine
}

type TaskEventEnvelope struct {
	Ctx   context.Context
	Event Event
	Ack   chan TaskActorResult
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

func ResumeTargetFor(snapshot Snapshot) (ResumeTarget, error)

type Machine struct {
	state State
	table map[State]map[EventKind]Transition
}

type Guard func(Snapshot, Event) error

type OnEnterHook func(context.Context, *TaskLifecycleActor, Event, *ActorEffects) error

type NextState func(Snapshot, Event) (State, error)

type Transition struct {
	From   State
	On     EventKind
	Guard  Guard
	To     NextState
	Enter  []OnEnterHook
}

type ActorEffects struct {
	TaskChanged            bool
	WorkerChanged          bool
	PaneMetadata           map[string]string
	PaneMetadataRemovals   []string
	Events                 []Event
	CompletionStatus       string
	CompletionEventType    string
	CompletionMerged       bool
	CompletionWrapUpPrompt string
	CompletionMessage      string
	nudges                 []taskMonitorNudge
}

type TaskActorResult struct {
	From           State
	To             State
	Changed        bool
	PersistedState string
	TerminalStatus string // "", "done", "cancelled", "failed"
	Effects        ActorEffects
}

func (m *Machine) Apply(snapshot Snapshot, event Event) (TaskActorResult, error)

type FSM interface {
	Apply(snapshot Snapshot, event Event) (TaskActorResult, error)
	HydrateLegacy(snapshot Snapshot) (State, error)
}
```

### Actor loop sketch

```go
func (a *TaskLifecycleActor) run() {
	for msg := range a.inbox {
		snapshot := snapshotFromRows(a.task, a.worker)
		result, err := a.fsm.Apply(snapshot, msg.Event)
		if err == nil {
			a.task.State = result.PersistedState
			if result.TerminalStatus != "" {
				a.task.Status = result.TerminalStatus
			}
			flushActorEffects(msg.Ctx, a, result.Effects)
		}
		msg.Ack <- result
	}
}
```

The critical property is that `fsm.Apply(...)` and every `onEnter` hook run on
the actor goroutine. Producers never mutate `task.State` directly.

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
		panic("unknown State")
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
| `PRDetected` | `MergeConflictObserved` | `State == CONFLICTING` | `PRDetected` | `persist_mergeability_observation`, `maybe_queue_conflict_nudge` |
| `PRDetected` | `MergeConflictObserved` | `State != CONFLICTING` | `PRDetected` | `persist_mergeability_observation` |
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
| `CIPending` | `MergeConflictObserved` | `State == CONFLICTING` | `CIPending` | `persist_mergeability_observation`, `maybe_queue_conflict_nudge` |
| `CIPending` | `MergeConflictObserved` | `State != CONFLICTING` | `CIPending` | `persist_mergeability_observation` |
| `CIPending` | `PRMergedObserved` | none | `Merged` | `emit_pr_merged`, `request_completion` |
| `CIPending` | `PRClosedObserved` | none | `Failed` | `emit_pr_closed`, `request_completion` |
| `CIPending` | `WorkerEscalated` | none | `Escalated` | `mark_worker_escalated`, `emit_worker_escalated` |
| `CIPending` | `UserCancelled` | none | `Cancelled` | `request_completion` |
| `CIPending` | `FatalErrorObserved` | `Outcome == failed` | `Failed` | `request_completion` |
| `ReviewPending` | `CIObserved` | `Bucket in {pending, fail}` | `CIPending` | `persist_ci_observation`, `maybe_queue_ci_nudge`, `maybe_emit_ci_escalation` |
| `ReviewPending` | `CIObserved` | `Bucket in {pass, cancel, skipping}` | `ReviewPending` | `persist_ci_observation` |
| `ReviewPending` | `ReviewObserved` | `BlockingCount == 0` | `ReviewPending` | `persist_review_observation` |
| `ReviewPending` | `ReviewObserved` | `BlockingCount > 0 && !Idle` | `ReviewPending` | `persist_review_observation` |
| `ReviewPending` | `ReviewObserved` | `BlockingCount > 0 && Idle && !NudgeBudgetExhausted` | `ReviewPending` | `persist_review_observation`, `maybe_queue_review_nudge` |
| `ReviewPending` | `ReviewObserved` | `BlockingCount > 0 && NudgeBudgetExhausted` | `ReviewPending` | `persist_review_observation`, `maybe_emit_review_escalation` |
| `ReviewPending` | `ReviewApprovedObserved` | none | `ReviewPending` | `persist_review_observation`, `maybe_emit_review_approved` |
| `ReviewPending` | `MergeConflictObserved` | `State == CONFLICTING` | `ReviewPending` | `persist_mergeability_observation`, `maybe_queue_conflict_nudge` |
| `ReviewPending` | `MergeConflictObserved` | `State != CONFLICTING` | `ReviewPending` | `persist_mergeability_observation` |
| `ReviewPending` | `PRMergedObserved` | none | `Merged` | `emit_pr_merged`, `request_completion` |
| `ReviewPending` | `PRClosedObserved` | none | `Failed` | `emit_pr_closed`, `request_completion` |
| `ReviewPending` | `WorkerEscalated` | none | `Escalated` | `mark_worker_escalated`, `emit_worker_escalated` |
| `ReviewPending` | `UserCancelled` | none | `Cancelled` | `request_completion` |
| `ReviewPending` | `FatalErrorObserved` | `Outcome == failed` | `Failed` | `request_completion` |
| `Escalated` | `PRMergedObserved` | none | `Merged` | `emit_pr_merged`, `request_completion` |
| `Escalated` | `PRClosedObserved` | none | `Failed` | `emit_pr_closed`, `request_completion` |
| `Escalated` | `WorkerRecovered` | `ResumeTargetFor(snapshot) == ResumeAssigned` | `Assigned` | `clear_worker_escalation`, `emit_worker_recovered` |
| `Escalated` | `WorkerRecovered` | `ResumeTargetFor(snapshot) == ResumePRDetected` | `PRDetected` | `clear_worker_escalation`, `emit_worker_recovered` |
| `Escalated` | `WorkerRecovered` | `ResumeTargetFor(snapshot) == ResumeCIPending` | `CIPending` | `clear_worker_escalation`, `emit_worker_recovered` |
| `Escalated` | `WorkerRecovered` | `ResumeTargetFor(snapshot) == ResumeReviewPending` | `ReviewPending` | `clear_worker_escalation`, `emit_worker_recovered` |
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
- `ReviewPending + CIObserved` stays legal because the current daemon can re-run
  CI after review has already started, for example after a new push.
- Review and merge-conflict hooks stay legal from `PRDetected` and `CIPending`,
  because those pollers already run before the task becomes `review_pending`.
- Recovery from `Escalated` is parameterized by `ResumeTargetFor(snapshot)`,
  because the current daemon resumes to different non-terminal states based on
  worker facts and that derivation should stay inside the actor/FSM boundary.
- `Merged + UserCancelled` is intentionally invalid. Once the PR is merged, the
  actor only accepts `LifecycleCompleted`; a later cancel cannot "unmerge" work.
- `Merged` stays explicit even though current code usually collapses it to
  `done` in the same update. The explicit state makes hooks reviewable.

## Legacy Hydration And Reconciliation Table

The end-state FSM above should not encode the current read-time derivation hacks
as normal transitions. Instead, the migration keeps a separate
`HydrateLegacy(snapshot)` adapter until every writer emits real events.

Rows are evaluated top-to-bottom. First match wins. Inconsistent snapshots
should return an error from `HydrateLegacy(...)` rather than silently inventing
state.

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
| Exited pane auto-restart succeeds | `WorkerRecovered` + `ResumeTargetFor(snapshot)` | `exited.go` via `taskStateForAssignment(...)` |

## Guard And Hook Design

### Guard rules

- Guards are pure functions. They inspect `Snapshot` plus the typed event and
  either allow the transition or return an error.
- Guards do not mutate worker counters, queue nudges, or emit events.
- Impossible transitions return an error immediately. Example:
  `Apply(StateDone, PRDiscovered{PRNumber: 42})` should fail loudly instead of
  silently overwriting terminal state.

### Hook rules

- Hooks are attached directly to transitions, but they should still have stable,
  reviewable names in the transition table and tests.
- Hooks run inside the actor after the transition has been validated and the
  next state chosen.
- Hooks have exclusive access to the actor's working copy of `task` and
  `worker`, so they are the natural home for stateful side effects like CI
  counters, review watermarks, queued nudges, and completion requests.
- Hooks do not call `fsm.Apply(...)` recursively. They mutate actor-local
  effects and rows only.

This keeps the transition table reviewable while giving side effects a single
serialized execution site.

### Suggested actor hook shape

```go
type OnEnterHook func(
	ctx context.Context,
	actor *TaskLifecycleActor,
	event Event,
	effects *ActorEffects,
) error

func onEnterPersistCIObservation(ctx context.Context, actor *TaskLifecycleActor, event Event, effects *ActorEffects) error
func onEnterMaybeQueueCINudge(ctx context.Context, actor *TaskLifecycleActor, event Event, effects *ActorEffects) error
func onEnterEmitWorkerEscalated(ctx context.Context, actor *TaskLifecycleActor, event Event, effects *ActorEffects) error
```

`ActorEffects` is the migration bridge to the existing daemon plumbing. It plays
the role `TaskStateUpdate` plays today, but it is assembled inside the actor
instead of being built by many concurrent call sites.

### Concrete example: `PRDetected + CIObserved(fail) -> CIPending`

Input:

```go
actor.fsm.state = StatePRDetected
event := CIObserved{Bucket: "fail"}
```

Actor-owned result:

```go
TaskActorResult{
	From:           StatePRDetected,
	To:             StateCIPending,
	Changed:        true,
	PersistedState: "ci_pending",
}
```

Actor-side behavior:

1. `onEnterPersistCIObservation` updates `Worker.LastCIState`, resets or increments
   CI counters, and marks the worker dirty.
2. `onEnterMaybeQueueCINudge` decides whether this `fail` bucket should send the
   existing "fix CI and push" prompt.
3. `onEnterMaybeEmitCIEscalation` emits `worker.ci_escalated` only when the CI
   nudge budget is exhausted.

The current transition and its side effects become visible in one place instead
of being split across `task_state.go`, `ci.go`, and post-apply daemon code.

### Concrete example: `Escalated + WorkerRecovered -> ReviewPending`

Input:

```go
actor.fsm.state = StateEscalated
event := WorkerRecovered{}
next, err := ResumeTargetFor(snapshot) // returns ResumeReviewPending
```

Actor-owned result:

```go
TaskActorResult{
	From:           StateEscalated,
	To:             StateReviewPending,
	Changed:        true,
	PersistedState: "review_pending",
}
```

This matches the real behavior better than hard-coding `Escalated -> PRDetected`
or forcing producers to re-implement the old `taskStateForAssignment(...)`
derivation.

### Explicit producer obligations

- `PRDiscovered` is emitted only on `PRNumber == 0 -> PRNumber > 0`. Once a task
  already has a PR number, duplicate `PRDiscovered` events are treated as a
  producer bug and rejected by the impossible-transition guard.
- `MergeConflictObserved` is emitted for the full mergeability stream. Non-
  conflicting values are explicit self-loops because the actor still needs to
  persist `LastMergeableState` even when no task-state transition occurs.

## Migration Plan

### Phase 1: Evolve `TaskMonitor` into a lifecycle actor that owns the FSM

Ship an actor-owned FSM alongside the existing logic:

- typed `State` and `Event` definitions,
- a `TaskLifecycleActor` that serializes per-task event handling,
- the authoritative transition table,
- `PersistedTaskState(...)`,
- `HydrateLegacy(snapshot) (State, error)`,
- `ResumeTargetFor(snapshot) (ResumeTarget, error)`, and
- table-driven tests that prove every row and every impossible transition.

No behavior change in this phase. Existing producers can still route through the
current `TaskMonitor` check kinds while the actor bootstraps from persisted
rows.

### Phase 2: Add compare mode, but keep legacy writes authoritative

Add actor-side compare mode that computes:

1. `legacyState` from current code, and
2. `fsmState` from `HydrateLegacy(snapshot)` or `Apply(...)`.

If they differ, emit a trace event or test failure. Do not write the FSM result
yet. This catches mismatches before migration deletes the derivers.

### Phase 3: Convert pollers and commands into event producers

Migrate one producer family at a time so each change stays reviewable:

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

Each migrated producer should:

- construct a typed event only,
- enqueue it to the task actor inbox, and
- stop mutating `task.State` directly.

The actor should:

- call `fsm.Apply(...)`,
- write `result.PersistedState` back to `task.State`,
- copy `result.TerminalStatus` into `task.Status` when present,
- run transition hooks on the actor goroutine, and
- flush resulting task/worker/event updates to the store before ack.

### Phase 4: Delete legacy derivation and check-kind-specific state logic

After all writers have moved:

- delete `normalizeTaskState(...)`,
- delete `taskStateForAssignment(...)`,
- delete `taskStateForCIState(...)`,
- delete `setTaskState(...)`, and
- collapse `taskMonitorCheckKind` handlers into thin event producers,
- collapse read-time normalization to `HydrateLegacy(...)` only for old rows, or
  delete it too if all persisted rows are guaranteed to be explicit.

## Child Issues To Open After Approval

1. Evolve `TaskMonitor` into a per-task lifecycle actor that owns the FSM,
   with exhaustive transition-table tests and no behavior change.
2. Convert daemon pollers and commands into typed event producers, delete
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

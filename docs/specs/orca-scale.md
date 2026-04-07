# Scaling orca to hundreds of workers across multiple machines

Date: 2026-04-07. Related: [orca-design.md](orca-design.md)

## Motivation

The design captured in [orca-design.md](orca-design.md) assumes a small fleet:
a few dozen workers on one or two machines, a single coordinator daemon on the
user's laptop, and a human lieutenant in the lead pane dispatching tasks
interactively. At that scale the current architecture is comfortable and the
safety rules (`never merge PRs`, `stateless binary, all state in SQLite`,
`single coordinator per project`) are not constraints — they are features.

The question this document addresses: **what has to change if orca needs to
drive 100 to 1000 codex/claude workers across multiple machines?**

Scaling by an order of magnitude is not a code-quality problem. It is a
sequence of concrete failure modes that bite at concrete thresholds. This
document enumerates those failure modes against the current architecture,
presents three viable paths forward, commits to a phased roadmap, and calls
out the open questions that require product-level decisions before the work
can start.

## Target scenarios

The roadmap targets three working scales:

| Scale  | Workers    | Machines | When needed        | Architecture   |
|--------|------------|----------|--------------------|----------------|
| Small  | 5 — 30     | 1        | Today              | Single daemon  |
| Medium | 100 — 300  | 2 — 10   | Phase 1            | Single daemon, concurrent internals |
| Large  | 300 — 1000 | 10 — 30  | Phase 2            | Split control + data planes |
| Huge   | 1000+      | 30+      | Deferred (Phase 3) | Multi-coordinator |

Out of scope for this document:

- Multi-tenant deployments (multiple organizations sharing a daemon).
- Cross-region fleets.
- Running orca as a managed SaaS.
- Replacing amux as the pane/PTY layer.

## What breaks at scale, in the order it breaks

Walking the failure modes against the current design in the order they
actually surface as worker count grows.

### ~50 workers — GitHub API rate limits

`orca` polls PR merge state via `gh pr view` on a 30s interval per active
task. At 100 active PRs that is 12,000 requests/hour — GitHub's authenticated
REST rate limit is 5,000/hour. The daemon hits throttling in under 30 minutes.

**Fix:** batch all open PR statuses into a single GraphQL query (one request
returns status for dozens of PRs), eventually move to webhooks. Cheap win.
This is a Phase 0 prerequisite that should ship independent of any scale
target.

### ~100 workers — the poll loop serializes

The daemon's stuck-detection loop iterates workers sequentially, running
`amux capture` against each. A capture over SSH takes ~100-300ms. At N=100
workers, a single poll pass is 10-30 seconds of wall-clock — longer than the
30s poll interval itself. The daemon stops sleeping between cycles and
dedicates 100% of its time to polling without catching up.

**Fix:** parallelize the poll loop with a bounded goroutine worker pool. This
buys a factor of ~10 on wall-clock per cycle before fanout overhead dominates.

### ~100 workers — SSH connection fanout

`amux` commands against remote hosts are delivered via SSH. OpenSSH's default
`MaxSessions` per connection is 10. Even with `ControlMaster` multiplexing (a
single TCP connection reused across sessions), running 30+ concurrent commands
against one host queues them head-of-line behind the session limit.

**Fix:** raise `MaxSessions` on fleet hosts, enforce `ControlMaster` with
persistent masters per host, or — better — terminate SSH entirely and add a
per-host agent that speaks an RPC protocol. The second option is Phase 2.

### ~100 workers — clone disk pressure forces multi-machine

Each clone is an independent git checkout: full `.git`, full `node_modules`,
full build cache. At 1-3 GB per clone for a realistic project, 100 clones is
100-300 GB. On a single coordinator machine, this is painful. At 300 workers
it is simply impossible to fit on one disk.

Clones must spread across machines, which means workers must spread across
machines, which means the allocator has to be host-aware: "give me a free
clone" becomes "give me a free clone on a host that has spare pool slots and
pin the pane to that host." The current `pool.pattern` glob assumes a single
machine.

**Fix:** the existing `[fleet.host-N]` config in `orca-design.md` already
anticipates per-host pool patterns. The allocator code does not yet enforce
locality. Phase 1 hardens this.

### ~300 workers — single-coordinator recovery window

The `stateless binary, all state in SQLite` rule makes `orca stop && orca
start` safe and fast. But at 300 active workers, the recovery window during
which no one is polling health, detecting stuck states, or tracking PR merges
is a real incident risk. A few seconds of unmonitored operation at this scale
produces real consequences: missed stuck escalations, stale clone pool state,
delayed PR-merged cleanups.

**Fix:** move health monitoring out of the coordinator. Per-host agents own
local polling and push events to the coordinator. Agents keep running when
the coordinator restarts; they simply queue events until it comes back. This
is Phase 2.

### ~1000 workers — SQLite becomes cramped

SQLite in WAL mode handles thousands of writes per second on local disk. That
is not the ceiling — event_log volume is. At 1000 workers emitting roughly
10 events per minute each (state transitions, captures, pings), the daemon
generates ~600k rows/hour, ~14M rows/day. Writes keep up fine; queries
against a multi-gigabyte log get slow; `VACUUM` becomes a scheduled-downtime
operation.

**Fix:** split the event_log off the main state file. Either rotate it into
per-day partition files (still SQLite), or migrate to Postgres for the log
only while keeping tasks/workers/clones in SQLite, or move the whole store to
Postgres. The decision is a Phase 2 open question.

### ~1000 workers — the human-in-the-loop rule snaps

The safety rule `orca must never merge PRs` (CLAUDE.md) assumes a human can
review every merge. At 100 PRs/day this is a full-time job. At 500 PRs/day it
is two full-time jobs. The rule has to evolve — not be abandoned, but
redefined.

**This is the hardest problem in the roadmap** because it is a *policy*
question disguised as a technical one. See the Open Questions section.

### Cross-cutting — the lieutenant model does not scale

The current dispatch model has Claude Code in the lead pane calling
`orca assign` per ticket. The lieutenant holds the state needed to make
assignment decisions (which profile, which worker, what prompt) in its
context window.

At 100 workers the state does not fit in context. The lieutenant cannot
issue per-ticket decisions fast enough to keep the fleet saturated. The
control interface has to invert: orca exposes structured queries and
aggregations, the lieutenant makes *policy-level* calls ("assign the next 20
critical tickets to codex workers on devbox-2"), and orca handles mechanism.

**This is an API redesign**, not a new feature. Scoped as a cross-cutting
work item in the roadmap.

### Cross-cutting — cost control is absent

At 100 codexes running continuously, a runaway task (tool loop, infinite
retry) can burn four-figure dollar amounts of tokens in a day. Orca has no
per-task cost tracking, no budget enforcement, no automatic kill-switch on
cost overrun.

**This ships before anything else in the roadmap.** At small scale the risk
is ignorable; at large scale it is the dominant operational risk.

## Three architectural paths

### Path A — vertical scale

Keep the single-coordinator daemon. Make its internals concurrent and
I/O-efficient. Specifically:

- Goroutine pool for the health-check loop with bounded fanout.
- Persistent SSH masters per fleet host via `ControlMaster`.
- GraphQL batching for GitHub PR status.
- Real task queue (priority, retry with backoff, dead-letter, backpressure).
- Expanded worker lifecycle state machine with per-phase timeouts.
- Per-worker token budgets with automatic drain on overrun.
- `orca dash` upgrades for aggregated views.

**Ceiling:** ~300 workers comfortably, ~500 if aggressively tuned. Above
that, the single-coordinator SSH fanout becomes the bottleneck no matter how
concurrent the daemon is.

**Cost:** moderate. Every change is incremental on the existing codebase and
preserves the stateless-binary rule.

### Path B — split control plane from data plane

Introduce a tiny `orca-agent` binary that runs on each fleet host. The agent
owns:

- Local `amux` polling and capture.
- Local stuck detection and pattern matching.
- Local clone pool state.
- Pushing structured events to the central coordinator over an RPC protocol.

The central orca daemon owns:

- Task dispatch and queue management.
- Global SQLite state.
- PR tracking and GitHub/Linear API calls.
- Policy decisions (when to nudge, when to escalate, when to clean up).

The two tiers talk over a bidirectional stream: agent pushes events, central
pushes commands. Both tiers remain stateless-binary: the agent holds no
durable state (re-derives it from local amux on restart), and the central
daemon still writes everything to SQLite.

**Ceiling:** ~1000-2000 workers. The central daemon becomes the bottleneck
around there, but its per-worker cost is much smaller (it processes
aggregated events instead of raw captures).

**Cost:** substantial. A new binary, a wire protocol, a deployment story.
But the architectural shape is clean and preserves orca's philosophical
constraints.

### Path C — multi-coordinator federation

Multiple orca brains, coordinated via shared state. Shared state options
include central Postgres with a lease mechanism (one coordinator is the
active dispatcher, others are warm standbys), or a Dolt-style fork-and-merge
state store, or a Raft-replicated SQLite.

**Ceiling:** much higher, potentially unbounded.

**Cost:** very high. This is a distributed systems project, not a feature.
Failure modes include split-brain, stale leases, replication lag, merge
conflicts on state.

### Decision

- **Phase 1 commits to Path A.** The vertical-scale work is high-value at any
  scale target and is a strict subset of what Path B also needs. Nothing in
  Phase 1 is wasted if Phase 2 happens.
- **Phase 2 commits to Path B.** The split control/data plane is the right
  architectural shape for the 300-1000 worker regime. The wire protocol is
  the main design decision; see Open Questions.
- **Path C is explicitly deferred.** It is not part of this roadmap. If the
  central daemon in Path B becomes a bottleneck, that is a future design
  discussion, not a commitment today.

The logic: Path A is always the right next step even if Path B never happens,
because every improvement in it is either strictly necessary today or trivially
reused in Path B. Path B is the right architecture for the realistic medium-
term target. Path C is prior art worth reading, not a commitment worth making
speculatively.

## Phased roadmap

### Phase 0 — prerequisites (ship before any scale work)

These items are not tied to a specific worker count. They are investments
that unblock everything else and that carry independent value.

1. **Stable worker identity decoupled from pane ID** (already filed as
   LAB-894). The ID format must include host and profile (e.g.
   `orca/devbox-2/codex/worker-07`) so cross-machine workers cannot
   collide. The existing issue description assumes a single host and should
   be updated.
2. **Structured validation records on task completion** (already filed as
   LAB-895). Post-mortems at scale are impossible without this.
3. **Per-task cost tracking in the event log.** Tokens in, tokens out,
   wall-clock, per worker and per task. No enforcement yet — the goal is to
   have the *data* before building budgets.
4. **GraphQL batched PR polling.** Replace the per-task `gh pr view` with a
   single GraphQL query per poll interval that returns status for all active
   PRs across the fleet. Reduces GitHub API consumption by roughly 50x.

### Phase 1 — vertical scale, target 100-300 workers

5. **Concurrent poll loop with bounded goroutine worker pool.** Parallelize
   stuck detection, screen capture, and per-worker health checks. Enforce a
   concurrency ceiling to bound SSH fanout.
6. **Persistent SSH multiplexing with explicit `ControlMaster` management.**
   One long-lived master per fleet host, reused for all `amux` commands.
   Handles reconnection when masters die.
7. **Explicit task queue with priority, retry, dead-letter, and backpressure.**
   Replaces the implicit "whatever order the lieutenant called `assign` in"
   model. Queue is a SQLite table; dispatch is a dedicated goroutine reading
   from it.
8. **Explicit worker lifecycle state machine.** Phases: `spawning`, `warming`,
   `running`, `idle`, `draining`, `terminated`. Each with independent
   timeouts. Resolves the "codex hung in spawning holds a clone slot"
   failure mode.
9. **Per-worker token budgets with automatic drain on overrun.** Depends on
   Phase 0 cost tracking. Daily budget per worker, per-task hard cap.
   Exceeding the cap triggers an `orca cancel` equivalent with preserved
   state for forensics.
10. **`orca dash` aggregations and filters.** Aggregate views by host,
    profile, state, priority. "Show me only the stuck ones." Pre-Phase-1
    the TUI is a flat table; Phase 1 it is a queryable view.
11. **Host-aware clone allocator.** Enforce locality: a task assigned to
    devbox-2 uses a clone on devbox-2. The `[fleet.host-N]` config is honored
    for allocation, not just discovery.

### Phase 2 — split control and data planes, target 300-1000+ workers

12. **`orca-agent` per-host binary.** New Go binary, minimal dependencies,
    runs as a systemd/launchd service. Owns local amux polling, stuck
    detection, capture parsing, and local clone pool state. Zero durable
    state — re-derives from local amux on restart.
13. **Wire protocol between central daemon and `orca-agent`.** Bidirectional
    RPC stream: agent pushes events, central pushes commands. Protocol
    choice is an Open Question.
14. **Central daemon transitions from SSH to RPC.** The existing SSH code
    paths remain for fallback and for fleet hosts without an agent
    installed, but the default transport becomes RPC to the agent.
15. **Structured log streaming from agents to central.** Per-worker logs
    ship to a central aggregator. Enables forensic replay of "why did
    LAB-987 fail 3 hours ago" which is impractical today.
16. **Event log partitioning or Postgres migration.** Depends on measured
    growth rates from Phase 1 observability work. Decision point, not a
    committed implementation.

### Cross-cutting — policy redesigns

17. **Merge queue preparation (not autonomous merge).** Orca tracks PR
    readiness: CI status, review state, merge conflicts, pending comments.
    A PR reaches `ready_to_merge` when all gates are green. A human (or
    lieutenant) issues a bulk merge command to land N ready PRs. Orca
    still never calls `gh pr merge` without explicit authorization, but the
    authorization can be bulk rather than per-PR. This is the minimum
    evolution of the `never merge PRs` rule that scales.
18. **Lieutenant control interface redesign.** Orca exposes structured
    queries (`orca query "stuck tasks on devbox-2 running > 30m"`) and bulk
    commands (`orca assign-next --count 20 --profile codex --host devbox-2`).
    The lieutenant operates at policy level, not per-ticket level. This is
    an API design project in its own right.

## Open questions

These require product-level input before the corresponding work can start.

### 1. What is the Phase 1 worker count target?

The phased plan assumes 100-300 for Phase 1 and 300-1000+ for Phase 2. If the
real target is "comfortably run 50 workers and scale to 100 eventually," Phase
2 can be deferred indefinitely and Phase 1 items #11 (host-aware allocator)
and #6 (SSH multiplexing) drop in priority. If the target is "we need to run
500 codexes by end of year," Phase 0 and Phase 1 need to run in parallel and
Phase 2 starts immediately after.

**This is the most important decision in the document** because it changes
which items are critical path.

### 2. What wire protocol should `orca-agent` use?

Three candidates:

- **gRPC bidirectional streaming.** Strong typing, good tooling, first-class
  streaming. Adds a protobuf build step and a new dependency.
- **HTTP/2 with long-polling or server-sent events.** Simpler, fewer
  dependencies, uses existing Go stdlib and gorilla/websocket. Weaker
  schema discipline.
- **NATS.** Purpose-built for this shape of problem, strong delivery
  semantics, adds an external broker as a deployment dependency.

I lean gRPC — the schema discipline is worth the build complexity and we will
not regret the strong types when debugging a 500-worker fleet. But this is the
kind of decision that benefits from user input before we commit.

### 3. What is the cost enforcement model?

Four candidates:

- **Per-worker daily budget.** Each worker has a token cap per 24h. Simple,
  isolates blast radius, but wastes budget on workers that idle.
- **Per-task budget.** Each task has a hard cap. Ties directly to work
  complexity, but requires the lieutenant to guess cost up front.
- **Fleet-wide daily budget.** One pool, consumed by any worker. Maximizes
  utilization, weakest blast-radius isolation.
- **Layered.** Per-task soft cap + per-worker daily hard cap + fleet-wide
  kill switch. Highest ceiling, highest implementation cost.

The layered model is correct for the target scale. The question is whether
Phase 1 ships all three layers or starts with just per-worker daily.

### 4. How aggressive should the merge queue redesign be?

Two extremes:

- **Minimum viable:** orca tracks PR readiness and exposes a `ready_to_merge`
  view. Merge is still per-PR via the user or lieutenant.
- **Bulk authorize:** `orca merge-ready --count N` merges up to N ready PRs
  after a one-time human authorization. Still never merges without the
  authorization, but the authorization is a bulk command.

The minimum viable model is strictly safer. The bulk model is necessary above
~50 PRs/day. Where to draw the line depends on the Phase 1 scale target.

### 5. What is the scope of the lieutenant interface redesign?

The redesign can be as narrow as "add a few bulk commands" or as broad as
"orca grows a real query language and the lieutenant is a client." The broad
version is genuinely useful but is a multi-month project on its own.

## Relationship to existing issues

- **LAB-894** (decouple worker identity from pane ID) is Phase 0 prerequisite
  #1. Needs a description update: the stable ID format must include host
  segment so cross-machine workers cannot collide. Example format:
  `orca/<host>/<profile>/worker-<seq>`.
- **LAB-895** (structured validation records) is Phase 0 prerequisite #2. No
  scope change.
- **LAB-896** (`orca stats` per-worker CV) becomes critical at scale — it is
  the observability primitive the lieutenant redesign (cross-cutting #18)
  depends on. No scope change, but its priority rises if Phase 1 ships.
- **Multi-tenant clone pool** (tracked as a separate issue) is architecturally
  adjacent but not a scale concern. It asks: can two lieutenants delegate to
  the same project and share the same clone pool without racing on
  allocation? The Phase 2 `orca-agent` is the natural answer — if the agent
  is the per-host owner of clone pool state, multiple central daemons can
  all RPC the same agent to allocate clones without filesystem races. Phase
  1 can address multi-tenancy with a lighter-weight filesystem-lock
  mechanism. See the separate Linear issue for scope and tradeoffs.

## Non-goals and explicit rejections

- **Kubernetes, Nomad, or a microservice rewrite.** The problem is task
  orchestration, not service orchestration. Using infrastructure-scale tools
  here is pure overhead.
- **Dolt or Wasteland-style federated state.** Still solving a problem orca
  does not have. The coordinator remains logically single at the target
  scale.
- **Abandoning SQLite.** It is the right tool for this scale. A Postgres
  migration is a Phase 2 decision driven by measured event_log growth, not
  a preemptive rewrite.
- **Autonomous PR merge.** The rule `orca must never merge PRs` does not go
  away. What evolves is the authorization boundary: bulk rather than
  per-PR.

## Next steps

1. User answers the Open Questions, at minimum #1 (scale target) and #2
   (wire protocol). Other open questions can be deferred to their phase.
2. Phase 0 items are filed as Linear issues and scheduled. These should
   ship in the next week or two regardless of scale decisions.
3. This document is revisited after the Phase 0 work produces measurements
   from the current daemon. Any scale plan built on assumptions instead of
   numbers is suspect. Concretely: the Phase 1 concurrent poll loop should
   only be built after we know the serial poll loop's actual bottleneck.

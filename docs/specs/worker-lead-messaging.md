# Worker ↔ Lead Messaging (`orca mail`)

Date: 2026-05-30. Supersedes the raw `send-keys` channel in
[worker-lieutenant-comms.md](worker-lieutenant-comms.md) (open question #1).
Status: **proposed** — design for review, no code yet.

## Motivation

Today a worker reaches its lead by typing into the lead's terminal. On every
assignment orca appends a "notify convention" to the worker's prompt
(`internal/daemon/assign_prompt.go` `notifyConvention`, injected at
`internal/daemon/assign.go:91`):

> To notify or ask the lieutenant, run `amux send-keys '<pane>' "<one-line message>"`.

This is raw PTY injection. The message:

- **bypasses orca** — the daemon never sees it, so it cannot be logged,
  rate-limited, or replayed;
- **lands mid-task** in the lead's input as unstructured text addressed only by
  pane id;
- **is fragile** — if the lead pane is busy, gone, or the wrong pane id was
  handed over, the message is lost with no record.

It also violates orca's own layering principle ("orca is the broker; amux knows
nothing about orca") for this one path: the worker talks *around* orca directly
to amux.

`orca mail` replaces it with a structured, background channel: the worker tells
**orca**, orca persists the message in its existing state store, and the lead
**pulls** it (`orca mail inbox`) instead of having it injected into its PTY.

## Goals / non-goals

**Goals**
- Worker → lead messaging that is durable, auditable, and survives
  `orca stop && orca start` (no message lives only in a PTY or in RAM).
- Two interaction modes: **notify** (fire-and-forget milestones/blocking notes)
  and **ask-and-wait** (worker pauses for a lead answer).
- Works on **both** state backends orca supports: Postgres (default) and SQLite.
- Keep orca deterministic and stateless: messages are rows, not goroutine state.

**Non-goals (this iteration)**
- Cross-host fleet delivery. Remote workers keep `send-keys` until a later
  shared-store step (see Fleet, below).
- Inter-worker messaging, broadcast/channels, attachments, file reservations.
- A standalone binary or separate repo. `orca mail` ships *in* orca behind a
  severable package boundary; extraction is a future option, not a goal.

## Why not adopt `am` (MCP Agent Mail)

An existing tool, MCP Agent Mail (`am`, Rust v0.3.7), is installed and running on
the dev host and provides exactly this shape (send/inbox/read/reply, thread tags,
importance, acks). We evaluated adopting it and **declined**, for three
independent reasons:

1. **License.** `am` ships under "MIT License **with OpenAI/Anthropic Rider**"
   (`SPDX: NOASSERTION`). The rider denies all rights to any party acting "on
   behalf of, for the benefit of, or under the direction of" OpenAI or Anthropic,
   where "use" includes "executing, benchmarking, testing" and incorporation into
   "any pipeline." This is not OSI-permissive and is legally risky to depend on
   or redistribute from org tooling.
2. **Architecture mismatch.** `am` is single-host: one SQLite file + one git
   archive per host, self-described as *"Not a distributed system… no auth
   federation."* Orca is a single-coordinator multi-host fleet backed by one
   stateless store. Adopting `am` would mean N disconnected mailboxes with no
   cross-host view — the opposite of orca's single-source-of-truth model.
3. **Operational cost.** A 72 MB pre-1.0, single-maintainer, nightly-Rust binary
   (built atop the author's own reimplementations of SQLite/MCP/async runtime) as
   a runtime dependency of a Go daemon. Observed friction on the live install:
   `-32002 Forbidden` (RBAC-on-by-default) and `.mailbox.activity.lock`
   "Resource is temporarily busy" contention.

We **keep `am`'s message schema as a design reference** (the columns below are
mined from it). The feature we need — "a worker posts a one-line, thread-tagged
message; the lead pulls read/unread" — is a single table and a handful of
subcommands on infrastructure orca already operates.

## Design

### Surface

```
# Worker (in its pane, identity injected by orca on assign)
orca mail send  "PR #123 up for LAB-456"          # fire-and-forget
orca mail ask   --wait "approach A or B?"          # blocks, polls for a reply

# Lead (the human/Claude pane that ran orca assign)
orca mail inbox                                     # unread, newest first
orca mail inbox --watch                             # stream new messages
orca mail read  <id>                                # mark read
orca mail reply <id> "B"                            # answer an `ask`
```

`orca mail` with no verb is a help stub (matches `orca pool` / `orca workers`).

### Severable package: `internal/mailbox`

All messaging logic lives in a new `internal/mailbox` package with **no import
of the daemon task FSM**. It depends only on the state store interface and plain
types. This keeps the feature extractable to its own repo later if a second
consumer ever appears, and keeps the daemon's assign/monitor code free of
messaging concerns beyond the convention swap.

The daemon is a *consumer*: it injects the convention on assign (with the
worker's identity) and, optionally, surfaces mail in `orca status`. It does not
embed messaging in the task state machine.

### Identity (orca auto-registers)

On assign, orca derives a stable mail identity for the worker and bakes a
ready-to-run command into the prompt. No separate registration step for the
worker. Identity is `issue`-derived so a message maps back to its task:

- **sender** = the worker's identity (e.g. the issue id, which is unique per
  active task and already how orca keys everything).
- **recipient** = the resolved lead, using the existing notify-pane resolution
  order (`--notify-pane` → caller `AMUX_PANE` → `notification_pane` config). If
  none resolves (autonomous pull loop with no lead), the convention is omitted
  and the worker falls back to orca's existing event/escalation path — identical
  to today's `appendNotifyConvention` behavior.
- **thread** = the issue id, so a lead can follow all messages for a task.

### Storage (Postgres **and** SQLite)

Orca has no shared SQL layer: `internal/daemonstate/store.go` defines a `Store`
interface implemented twice (`sqlite.go`, `postgres_*.go`). `orca mail` follows
that pattern exactly. The new `mail` table mirrors the existing `events` table,
which already proves the only three things that differ between backends:

| Concern | SQLite | Postgres |
|---|---|---|
| auto id | `INTEGER PRIMARY KEY AUTOINCREMENT` | `BIGSERIAL PRIMARY KEY` |
| timestamps | `TEXT` (via `formatTime`) | `TIMESTAMPTZ` |
| placeholders | `?` | `$1,$2,…` |

The schema uses only portable column types (no `JSONB`, no arrays), so it is
identical apart from those three points.

```
mail
  id           auto-increment PK
  project      TEXT NOT NULL                 -- scopes every row, like all tables
  thread_id    TEXT NOT NULL DEFAULT ''      -- = issue id
  sender       TEXT NOT NULL                 -- worker identity (issue-derived)
  recipient    TEXT NOT NULL                 -- resolved lead
  body         TEXT NOT NULL                 -- one-line message
  importance   TEXT NOT NULL DEFAULT 'normal'-- low|normal|high|urgent
  kind         TEXT NOT NULL DEFAULT 'note'  -- note | ask | reply
  ask_id       INTEGER  (nullable)           -- for a reply: the ask it answers
  reply_body   TEXT NOT NULL DEFAULT ''      -- filled when an ask is answered
  read_at      <ts>     (nullable)           -- NULL = unread
  created_at   <ts> NOT NULL
```

Indexes mirror `events`: `(project, id)` and `(project, recipient, id)` for
inbox queries, `(project, thread_id, id)` for thread/ask lookups.

### Events integration

Sends and replies also emit into orca's existing `Event` stream so `orca events`
and any monitoring loop see them, with two new kinds:

- `mail.sent` — a worker posted a note/ask
- `mail.replied` — the lead answered an ask

This reuses `AppendEvent` and the `Events(project, afterID)` channel that backs
`orca events` today; `orca mail inbox --watch` is built on the same stream
primitive (filter to `mail.*`), not a new pub/sub.

### Ask-and-wait semantics

`am` has no blocking primitive, and neither will orca's store — blocking is a
**worker-side poll loop**, which keeps the daemon stateless:

1. `orca mail ask --wait "…"` inserts a `kind='ask'` row, emits `mail.sent`, then
   polls `orca mail inbox`-style for a `reply` whose `ask_id` matches, sleeping
   between polls until answered or a timeout elapses.
2. Lead runs `orca mail reply <ask-id> "B"` → inserts a `kind='reply'` row
   referencing `ask_id`, sets the ask's `reply_body`, emits `mail.replied`.
3. The worker's poll observes the reply, prints it, and exits non-zero on
   timeout so the worker can decide whether to proceed on a default.

No daemon goroutine waits; the only waiting process is the worker's own CLI
invocation. This preserves the "never store per-task state in Go" contract.

### Migration from `send-keys`

The `notifyConvention` text in `assign_prompt.go` is replaced wholesale with the
`orca mail send` form. For **local** workers this is a clean swap. For **fleet**
(remote-host) workers, the convention keeps emitting `send-keys` until the
shared-store step lands, because a remote worker cannot reach the coordinator's
store directly (see below). The branch point is the same notify-pane/host
resolution orca already does at assign time.

### Fleet (deferred)

Orca's fleet is single-coordinator: one daemon + one store; remote hosts run only
amux. A remote worker's `orca mail send` would need to reach the coordinator's
store. Options for a later iteration, not built now:

- a thin `orca mail send` RPC the remote worker calls over the coordinator's
  existing transport;
- point remote workers at the coordinator Postgres directly (Tailscale).

Until then, **local-first**: local workers use `orca mail`, remote workers keep
`send-keys`. No regression for either.

## Decision Log

| Decision | Choice | Rationale |
|---|---|---|
| Build vs adopt `am` | **Build** in orca | `am` license rider is non-permissive; single-host SQLite+git contradicts orca's multi-host store; 72 MB pre-1.0 single-maintainer runtime dep. Keep its schema as reference. |
| Home | **In orca**, `internal/mailbox`, severable | One consumer today (YAGNI). Orca already solves cross-host via single-coordinator store. Standalone repo recreates the costs we rejected `am` for; extraction stays a cheap future option (ETC). |
| Name | `orca mail <verb>` | Grouped namespace keeps top-level `--help` clean as orca grows; mirrors the proven `am` surface. |
| Semantics | notify + ask-and-wait | Lead can answer blocking questions; worker need not guess. |
| Blocking impl | worker-side poll, no daemon wait | Preserves stateless-daemon contract; messages are rows, not goroutines. |
| Identity | orca auto-registers, issue-derived | Deterministic; message ↔ task mapping for free; no worker setup step. |
| Storage | new `mail` table, written twice (sqlite + pg) | Matches existing no-shared-SQL store pattern; portable types only. |
| Consumption | `orca mail inbox [--watch]` on the `Event` stream | Reuses `AppendEvent`/`Events`; no new pub/sub. |
| Scope | local-first; fleet later | Remote workers can't reach the coordinator store yet; no regression keeping `send-keys` there. |

## Open questions

1. **Ask timeout policy.** Default wait duration and what the worker does on
   timeout (proceed on a stated default vs escalate via the existing stuck path).
2. **Inbox retention.** Do read messages age out, or persist for postmortems?
   (Leaning persist; they are cheap rows and useful history.)
3. **Rate limiting.** A soft cap so a misbehaving worker can't flood the table.
   Cheap to add at the `send` path; needed before fleet/autonomous scale.
4. **`orca status` surfacing.** Should unread mail for a task show in
   `orca status <issue>`? Likely yes, low effort, high signal.

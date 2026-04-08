---
name: orca
description: >
  Use orca to delegate coding tasks to worker agents (Claude Code, Codex).
  Trigger when the user asks to delegate work, assign issues, spawn workers, check worker
  status, cancel tasks, resume tasks, batch-assign issues, queue PRs for merge, or manage
  the orca daemon. Also trigger when the user mentions orca commands, worker health, clone
  pools, or agent orchestration. Use this skill even if you know orca basics — it contains
  UX patterns and gotchas that prevent common mistakes.
---

# Orca — Agent Orchestration Daemon

Orca manages the full lifecycle of coding tasks: clone allocation, agent spawning, health monitoring, PR detection, and cleanup. It owns no AI — all intelligence lives in the worker agents.

## Architecture

```
User
  └─ Claude Code (any pane)
       └─ orca CLI ─── orca daemon (single global process)
            ├─ amux        (pane/PTY infrastructure)
            ├─ GitHub API  (PR merge detection)
            └─ clone pool  (auto-created under .orca/pool/ per project)
```

The daemon is a single global process per machine — not per project. All state lives in SQLite at `~/.config/orca/state.db`. This means `orca stop && orca start` is seamless; the new daemon picks up exactly where the old one left off.

When assigning work, the CLI reads `AMUX_PANE` from the environment and passes it to the daemon as the split target for spawning worker panes. This means workers are always spawned in the same window as the caller.

## Core Workflow

### 1. Start the daemon

The daemon must be running before any other command:

```bash
orca start
```

Orca auto-detects the amux session from `$AMUX_SESSION`. The daemon is global — start it once, use it from any project directory.

Check if it's already running with `orca status` first — `start` errors if a daemon is already active.

### 2. Assign work

```bash
orca assign ISSUE --prompt "DETAILED INSTRUCTIONS" --agent AGENT
```

**Required flags:**
- `ISSUE` — positional, the issue ID (e.g., `LAB-123`)
- `--prompt` — detailed instructions for the worker. Be specific: include what to change, which files, what to test, and any review feedback to address.

**Optional flags:**
- `--agent` — agent profile to use. Default is `claude`. Use `codex` for Codex workers.
- `--project` — target a specific project. Defaults to cwd.

**Example — fresh issue:**
```bash
orca assign LAB-784 \
  --agent codex \
  --prompt "Implement LAB-784: make orca assign adopt existing PRs instead of rejecting. See the Linear issue for details. Write tests."
```

**Example — existing PR with review feedback:**
```bash
orca assign LAB-761 \
  --agent claude \
  --prompt "Pick up PR #57 (branch LAB-761). Address review feedback: (1) Add t.Parallel() to test functions missing it. (2) Verify patch coverage meets 80%. Push and iterate until CI is green."
```

### 3. Verify the worker started correctly

After assigning, verify the worker received the prompt using orca's own commands:

```bash
# Check worker health and pane assignment
orca workers

# Check detailed task status with event log
orca status ISSUE
```

If the worker shows as `escalated` shortly after assignment, the agent likely failed to start. Cancel and reassign:

```bash
orca cancel ISSUE
orca assign ISSUE --agent AGENT --prompt "PROMPT"
```

### 4. Monitor

```bash
orca status              # overview: daemon state, task counts, worker/pool summary
orca status ISSUE        # detailed status for a specific task with event log
orca workers             # list all workers with health state
orca pool                # list clone pool status
orca events              # stream NDJSON events (daemon, task, worker, PR events)
```

Worker health states:
- `healthy` — worker is active and making progress
- `stuck` — worker appears stuck (idle too long or stuck text pattern detected)
- `escalated` — stuck detection has exhausted nudge retries; needs human attention

### 5. Batch assign

For multiple tasks, create a JSON manifest and use `orca batch`:

```json
[
  {"issue": "LAB-123", "agent": "codex", "prompt": "Fix the auth bug. TDD."},
  {"issue": "LAB-124", "agent": "claude", "prompt": "Add regression tests."}
]
```

```bash
orca batch manifest.json --delay 10s
```

The `--delay` flag (default 5s) spaces out assignments to avoid overwhelming the system.

### 6. Queue PRs for merge

When a PR is ready to land:

```bash
orca enqueue PR_NUMBER
```

Orca manages a merge queue — it rebases, waits for CI checks, and squash-merges one PR at a time.

### 7. Resume a task

If a worker's pane was lost or the task needs to continue:

```bash
orca resume ISSUE
```

Resume requires an existing task in the DB. It restarts the agent in the existing or a fresh pane.

### 8. Cancel and cleanup

```bash
orca cancel ISSUE    # cancel a specific task, kill the worker pane
orca stop            # stop the daemon (tasks persist in DB for later resume)
```

## Common Patterns

### Restart the daemon (e.g., after updating the binary)

```bash
orca stop && orca start
```

Safe because all state is in SQLite. The new daemon picks up all active tasks.

### Delegate a PR with review feedback

When a PR has review comments that need addressing:

1. Read the PR review comments to understand what needs fixing
2. Craft a prompt that summarizes the specific feedback
3. Assign with the prompt referencing the PR number and branch

```bash
orca assign ISSUE \
  --agent codex \
  --prompt "Pick up PR #NUMBER (branch BRANCH). Address blocking review feedback: (1) specific issue. (2) another issue. Push and iterate until CI is green."
```

### Recover from a failed assignment

If `orca assign` fails or the worker didn't get the prompt:

```bash
orca cancel ISSUE
# Then reassign
orca assign ISSUE --agent AGENT --prompt "PROMPT"
```

### Check what a worker is doing

```bash
orca status ISSUE
```

This shows the task's event log with timestamps — handshake steps, stuck detection, nudges, and escalations.

## Known Limitations

- `orca resume` tries to spawn a new pane rather than reconnecting to an existing one. If the worker is still running, use `orca cancel` then `orca assign` instead.
- The default agent is `claude`. Always pass `--agent codex` explicitly when you want Codex.
- Codex workers sometimes exit silently on startup (transient). Orca's prompt delivery verification retries up to 10 times, but if it still fails, cancel and reassign.

## All Commands Reference

| Command | Usage | Description |
|---------|-------|-------------|
| `start` | `orca start [--session S] [--global] [--json]` | Start the global daemon |
| `stop` | `orca stop` | Stop the daemon |
| `status` | `orca status [ISSUE] [--project P]` | Show status (overall or per-task) |
| `assign` | `orca assign ISSUE --prompt P [--agent A] [--project P]` | Assign an issue to a worker |
| `batch` | `orca batch MANIFEST [--delay D] [--project P]` | Batch assign from JSON manifest |
| `enqueue` | `orca enqueue PR_NUMBER [--project P]` | Queue a PR for serialized landing |
| `cancel` | `orca cancel ISSUE [--project P]` | Cancel a task |
| `resume` | `orca resume ISSUE [--project P]` | Resume a task in its existing pane |
| `workers` | `orca workers [--project P]` | List workers and their state |
| `pool` | `orca pool [--project P]` | List clone pool status |
| `events` | `orca events [--project P]` | Stream orchestration events as NDJSON |
| `version` | `orca version` | Print version |

Most commands accept `--project PATH` to target a specific project and `--json` for machine-readable output.

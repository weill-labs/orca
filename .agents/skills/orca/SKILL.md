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
  └─ Claude Code (lead pane)
       └─ orca CLI ─── orca daemon
            ├─ amux        (pane/PTY infrastructure)
            ├─ GitHub API  (PR merge detection)
            └─ clone pool  (auto-created under .orca/pool/)
```

The daemon is stateless in memory — all state lives in SQLite at `~/.config/orca/state.db`. This means `orca stop && orca start` is seamless; the new daemon picks up exactly where the old one left off.

## Core Workflow

### 1. Start the daemon

The daemon must be running before any other command. Start it from the project directory:

```bash
orca start
```

Orca auto-detects the project from the current directory and the amux session from `$AMUX_SESSION`. Clones are created on demand under `.orca/pool/`.

Check if it's already running with `orca status` first — `start` errors if a daemon is already active.

### 2. Assign work

```bash
orca assign ISSUE --prompt "DETAILED INSTRUCTIONS" --agent AGENT --title "PANE TITLE"
```

**Required flags:**
- `ISSUE` — positional, the issue ID (e.g., `LAB-123`)
- `--prompt` — detailed instructions for the worker. Be specific: include what to change, which files, what to test, and any review feedback to address.

**Optional flags:**
- `--agent` — agent profile to use. Default is `claude`. Use `codex` for Codex workers.
- `--title` — sets the amux pane title for easy identification in `orca workers` output. Recommended format: `"ISSUE: short description"`.

**Example — fresh issue:**
```bash
orca assign LAB-784 \
  --agent codex \
  --title "LAB-784: adopt existing PRs in orca assign" \
  --prompt "Implement LAB-784: make orca assign adopt existing PRs instead of rejecting. See the Linear issue for details. Write tests."
```

**Example — existing PR with review feedback:**
```bash
orca assign LAB-761 \
  --agent claude \
  --title "LAB-761: resume terminal tasks" \
  --prompt "Pick up PR #57 (branch LAB-761). Address review feedback: (1) Add t.Parallel() to test functions missing it. (2) Verify patch coverage meets 80%. Push and iterate until CI is green."
```

### 3. Verify the worker started correctly

After assigning, always verify the worker received the prompt:

```bash
# Check worker health
orca workers

# Capture pane output to verify prompt was received
amux -s SESSION capture PANE_ID | tail -40
```

The pane ID appears in `orca workers` output (e.g., `worker-LAB-784`). If the worker didn't receive the prompt (race condition), cancel and reassign:

```bash
orca cancel ISSUE
orca assign ISSUE --agent AGENT --title "TITLE" --prompt "PROMPT"
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
  {"issue": "LAB-123", "agent": "codex", "prompt": "Fix the auth bug. TDD.", "title": "LAB-123: auth fix"},
  {"issue": "LAB-124", "agent": "claude", "prompt": "Add regression tests.", "title": "LAB-124: tests"}
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
  --title "ISSUE: address PR review" \
  --prompt "Pick up PR #NUMBER (branch BRANCH). Address blocking review feedback: (1) specific issue. (2) another issue. Push and iterate until CI is green."
```

### Recover from a failed assignment

If `orca assign` fails or the worker didn't get the prompt:

```bash
orca cancel ISSUE
# Then reassign
orca assign ISSUE --agent AGENT --title "TITLE" --prompt "PROMPT"
```

### Check what a worker is doing

```bash
amux -s SESSION capture PANE_ID | tail -40
```

This shows the last 40 lines of the worker's terminal output.

## Known Limitations

- `orca assign` rejects issues that already have an open PR (`issue X already has open PR #Y`). This is tracked in LAB-784 — the fix will make assign adopt existing PRs instead of rejecting.
- `orca resume` requires an existing task in the DB. Tasks created by orca on another machine won't have local DB entries.
- The default agent is `claude`. Always pass `--agent codex` explicitly when you want Codex.

## All Commands Reference

| Command | Usage | Description |
|---------|-------|-------------|
| `start` | `orca start [--session S] [--lead-pane P]` | Start the daemon |
| `stop` | `orca stop` | Stop the daemon |
| `status` | `orca status [ISSUE]` | Show status (overall or per-task) |
| `assign` | `orca assign ISSUE --prompt P [--agent A] [--title T]` | Assign an issue to a worker |
| `batch` | `orca batch MANIFEST [--delay D]` | Batch assign from JSON manifest |
| `enqueue` | `orca enqueue PR_NUMBER` | Queue a PR for serialized landing |
| `cancel` | `orca cancel ISSUE` | Cancel a task |
| `resume` | `orca resume ISSUE` | Resume a task in its existing pane |
| `workers` | `orca workers` | List workers and their state |
| `pool` | `orca pool` | List clone pool status |
| `events` | `orca events` | Stream orchestration events as NDJSON |
| `version` | `orca version` | Print version |

Most commands accept `--project PATH` to target a specific project and `--json` for machine-readable output. Exceptions: `batch` and `events` do not support `--json`; `version` accepts no flags.

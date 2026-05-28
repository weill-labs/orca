# orca

[![CI](https://github.com/weill-labs/orca/actions/workflows/ci.yml/badge.svg)](https://github.com/weill-labs/orca/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/weill-labs/orca/graph/badge.svg)](https://codecov.io/gh/weill-labs/orca)

Deterministic agent orchestration daemon for [amux](https://github.com/weill-labs/amux).

Orca manages the full lifecycle of coding tasks: clone allocation, agent spawning, health monitoring, serialized landing, and cleanup. It owns no AI — all intelligence lives in the worker agents (Claude Code, Codex, etc.).

## How it works

```
User
  └─ Claude Code (lead pane)
       └─ orca CLI ─── orca daemon
            ├─ amux        (pane/PTY infrastructure)
            ├─ git         (local or remote branch landing)
            └─ clone pool  (filesystem)
```

The lead agent calls `orca assign` with an issue and prompt. Orca allocates git clones, spawns amux panes, starts coding agents, sends prompts, and monitors progress. By default, work lands directly through git with no GitHub or Linear dependency: `orca enqueue ISSUE_OR_BRANCH` rebases a worker branch onto a configured base branch, runs a local quality gate when configured, pushes the base branch, and then cleans up the clone. GitHub PR landing and Linear status updates are optional integrations enabled per project.

## Install

```bash
go install github.com/weill-labs/orca@latest
# or
make install  # builds to ~/.local/bin/orca
```

## State backend

Orca uses Postgres by default. For local development, run:

```bash
make dev-postgres
```

That starts a local Postgres container and writes
`~/.config/orca/config.toml`:

```toml
[state]
dsn = "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"
```

If you already have legacy SQLite state at `~/.config/orca/state.db`, the next
`orca start` auto-migrates it into the configured Postgres database before the
daemon starts.

Need SQLite for a one-off? Set `ORCA_STATE_DB=/absolute/path/to/state.db` or
`ORCA_STATE_DSN=sqlite:///absolute/path/to/state.db` explicitly.

## Quick start

```bash
# 1. Activate git hooks
make setup

# 2. Start local Postgres and write ~/.config/orca/config.toml
make dev-postgres

# 3. Configure clone pool and agent profiles
mkdir -p ~/sync/github/myproject/myproject/.orca
cat > ~/sync/github/myproject/myproject/.orca/config.toml << 'EOF'
[pool]
pattern = "~/sync/github/myproject/myproject-*"

[agents.codex]
start_command = "codex --yolo"
idle_timeout = "30s"
stuck_timeout = "5m"
stuck_text_patterns = ["permission prompt"]
nudge_command = "Enter"
max_nudge_retries = 3

# Optional: pull ready local beads work when clones are free.
[worksource]
enabled = false
source = "manual"
# source = "beads"
# beads_bin = "bd"
# agent = "codex"

[landing]
mode = "direct"
base_branch = "main"
# quality_gate = "uv run pytest -q"

# Optional external integrations. Disabled by default.
[integrations]
github = false
linear = false
EOF

# 4. Create clones with pool markers
for i in 1 2 3; do
  git clone ~/sync/github/myproject/myproject ~/sync/github/myproject/myproject-${i}
  touch ~/sync/github/myproject/myproject-${i}/.orca-pool
done

# 5. Start the daemon
export AMUX_SESSION=my-session
orca start --project ~/sync/github/myproject/myproject

# 6. Assign work
orca assign myproject-123 --prompt "Fix the auth bug. TDD. Commit and queue direct landing when done."

# 7. Monitor
orca status
orca workers
orca pool
orca events        # NDJSON event stream

# 7b. Queue an active issue or branch for serialized direct landing
orca enqueue myproject-123

# 8. Cancel or stop
orca cancel LAB-123
orca stop
```

All commands accept `--project` to target another checkout explicitly. If you omit it, Orca resolves the current working directory to the canonical git repo root. `orca start` uses `AMUX_SESSION` by default when it is set; otherwise it falls back to the repo basename. `orca start` requires `<repo>/.orca/config.toml` and fails clearly when it is missing.

## Landing modes

Direct mode is the default and works with local filesystem remotes and local beads issue tracking without GitHub or Linear:

```toml
[landing]
mode = "direct"
base_branch = "main"
quality_gate = "uv run pytest -q"
```

In direct mode, assignment prompts tell workers to commit, push their branch, and run `orca enqueue ISSUE_OR_BRANCH`; they do not ask workers to open a PR. Orca fetches `origin/<base_branch>`, rebases the worker branch in its managed clone, runs `quality_gate` from that clone when configured, and pushes `HEAD:<base_branch>` to `origin`.

Local filesystem remotes are supported when `origin` is a bare repository path, for example `/path/to/repo.git`. Pushing to a non-bare local checkout's checked-out branch is intentionally not supported because Git rejects that shape by default.

For local beads dispatch and completion, enable the beads worksource:

```toml
[worksource]
enabled = true
source = "beads"
beads_bin = "bd"
agent = "codex"
```

This uses the local `bd` database and closes beads issues after successful direct landing. Linear sync is not required.

GitHub PR mode is preserved for projects that opt in:

```toml
[landing]
mode = "pr"
base_branch = "main"

[integrations]
github = true
linear = true # optional Linear status/title sync
```

In PR mode, workers receive the existing `gh pr create --base main` prompt and `orca enqueue PR_NUMBER` uses GitHub PR checks and squash merge behavior. Set `ORCA_GITHUB_TOKEN` if you want the daemon to use a separate GitHub token from your interactive `gh` session.

## Design principles

- **amux knows nothing about orca.** Orca consumes amux's CLI — other tools could replace orca without amux changes.
- **Orca is 100% deterministic.** No AI, no LLM calls. It fetches issues, manages clones, spawns panes, monitors health, and cleans up.
- **Agent-agnostic.** Pluggable profiles encode agent-specific quirks (start commands, stuck patterns, nudge sequences).
- **Clones, not worktrees.** Each clone has its own `.git`, `node_modules`, and build cache for full isolation.

See [docs/specs/orca-design.md](docs/specs/orca-design.md) for the full design document.

## License

MIT

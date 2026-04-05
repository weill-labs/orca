# orca

[![CI](https://github.com/weill-labs/orca/actions/workflows/ci.yml/badge.svg)](https://github.com/weill-labs/orca/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/weill-labs/orca/graph/badge.svg)](https://codecov.io/gh/weill-labs/orca)

Deterministic agent orchestration daemon for [amux](https://github.com/weill-labs/amux).

Orca manages the full lifecycle of coding tasks: clone allocation, agent spawning, health monitoring, PR merge detection, and cleanup. It owns no AI — all intelligence lives in the worker agents (Claude Code, Codex, Aider, etc.).

## How it works

```
User
  └─ Claude Code (lead pane)
       └─ orca CLI ─── orca daemon
            ├─ amux        (pane/PTY infrastructure)
            ├─ GitHub API  (PR merge detection)
            └─ clone pool  (filesystem)
```

The lead agent calls `orca assign` with an issue and prompt, or `orca batch` with a JSON manifest of multiple assignments. Orca allocates git clones, spawns amux panes, starts coding agents, sends prompts, and monitors progress. When a PR is ready to land, the lead agent can queue it with `orca enqueue PR_NUMBER` so Orca rebases, waits for required checks, and squash-merges one PR at a time. When the PR merges, Orca cleans up the clone and returns it to the pool.

## Install

```bash
go install github.com/weill-labs/orca@latest
# or
make install  # builds to ~/.local/bin/orca
```

## Quick start

```bash
# 1. Configure clone pool and agent profiles
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
EOF

# 2. Create clones with pool markers
for i in 1 2 3; do
  git clone ~/sync/github/myproject/myproject ~/sync/github/myproject/myproject-${i}
  touch ~/sync/github/myproject/myproject-${i}/.orca-pool
done

# 3. Start the daemon
export AMUX_SESSION=my-session
orca start --project ~/sync/github/myproject/myproject

# 4. Assign work
orca assign LAB-123 --prompt "Fix the auth bug. TDD. Open a PR when done."

# 4b. Or batch multiple assignments with a default 5s delay between dispatches
cat > tasks.json <<'EOF'
[
  {"issue":"LAB-123","agent":"codex","prompt":"Fix the auth bug. TDD. Open a PR when done."},
  {"issue":"LAB-124","agent":"claude","prompt":"Add regression coverage and open a PR.","title":"LAB-124 tests"}
]
EOF
orca batch tasks.json --delay 10s

# 5. Monitor
orca status
orca workers
orca pool
orca events        # NDJSON event stream

# 5b. Queue a ready PR for serialized landing
orca enqueue 123

# 6. Cancel or stop
orca cancel LAB-123
orca stop
```

All commands accept `--project` to target another checkout explicitly. If you omit it, Orca resolves the current working directory to the canonical git repo root. `orca start` uses `AMUX_SESSION` by default when it is set; otherwise it falls back to the repo basename. `orca start` requires `<repo>/.orca/config.toml` and fails clearly when it is missing.

## Design principles

- **amux knows nothing about orca.** Orca consumes amux's CLI — other tools could replace orca without amux changes.
- **Orca is 100% deterministic.** No AI, no LLM calls. It fetches issues, manages clones, spawns panes, monitors health, and cleans up.
- **Agent-agnostic.** Pluggable profiles encode agent-specific quirks (start commands, stuck patterns, nudge sequences).
- **Clones, not worktrees.** Each clone has its own `.git`, `node_modules`, and build cache for full isolation.

See [docs/specs/orca-design.md](docs/specs/orca-design.md) for the full design document.

## License

MIT

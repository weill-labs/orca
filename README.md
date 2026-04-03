# orca

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

The lead agent calls `orca assign` with an issue and prompt. Orca allocates a git clone, spawns an amux pane, starts the coding agent, sends the prompt, and monitors progress. When the agent's PR merges, orca cleans up the clone and returns it to the pool.

## Install

```bash
go install github.com/weill-labs/orca@latest
# or
make install  # builds to ~/.local/bin/orca
```

## Quick start

```bash
# 1. Configure clone pool and agent profiles
cat > ~/.config/orca/config.toml << 'EOF'
[pool]
pattern = "~/sync/github/myproject/myproject-*"

[agents.codex]
start_command = "codex --yolo"
idle_timeout = "30s"
stuck_timeout = "5m"
stuck_text_patterns = ["permission prompt"]
nudge_command = "y\n"
max_nudge_retries = 3
EOF

# 2. Create clones with pool markers
for i in 1 2 3; do
  git clone ~/sync/github/myproject/myproject ~/sync/github/myproject/myproject-${i}
  touch ~/sync/github/myproject/myproject-${i}/.orca-pool
done

# 3. Start the daemon
orca start --session my-session --project ~/sync/github/myproject/myproject

# 4. Assign work
orca assign LAB-123 --prompt "Fix the auth bug. TDD. Open a PR when done."

# 5. Monitor
orca status
orca workers
orca pool
orca events        # NDJSON event stream

# 6. Cancel or stop
orca cancel LAB-123
orca stop
```

## Design principles

- **amux knows nothing about orca.** Orca consumes amux's CLI — other tools could replace orca without amux changes.
- **Orca is 100% deterministic.** No AI, no LLM calls. It fetches issues, manages clones, spawns panes, monitors health, and cleans up.
- **Agent-agnostic.** Pluggable profiles encode agent-specific quirks (start commands, stuck patterns, nudge sequences).
- **Clones, not worktrees.** Each clone has its own `.git`, `node_modules`, and build cache for full isolation.

See [docs/specs/orca-design.md](docs/specs/orca-design.md) for the full design document.

## License

MIT

# CLAUDE.md

## What is orca

Orca is a deterministic agent orchestration daemon for amux. It manages the full lifecycle of coding tasks: clone allocation, agent spawning, health monitoring, PR merge detection, and cleanup. It owns no AI — all non-deterministic intelligence lives in the worker agents (Claude Code, Codex, etc.).

See [docs/specs/orca-design.md](docs/specs/orca-design.md) for the full design document.

## Architecture

- **Daemon**: long-lived background process per project, SQLite state at `~/.config/orca/state.db`
- **Stateless binary**: the daemon holds zero state in memory. All state lives in SQLite. The daemon is a pure poll loop that reads active tasks from the DB, runs monitoring checks, and writes results back. This means `orca stop && orca start` is seamless — the new daemon picks up exactly where the old one left off. Never store per-task state in Go structs, maps, or goroutines.
- **Clone pool**: auto-created under `.orca/pool/` on demand — independent git clones (not worktrees)
- **Agent profiles**: built-in defaults for claude, codex — no config file needed
- **amux consumer**: orca calls amux CLI commands and subscribes to amux events — amux knows nothing about orca
- **Zero config**: orca auto-detects everything from the git repo. No `.orca/config.toml` required. Origin detected from `git remote get-url origin` (override with `ORCA_CLONE_ORIGIN` env var).

## Development

### Build and Test

```bash
make setup    # activate git hooks
make install  # install to ~/.local/bin/orca
make test     # run all tests
make coverage # test coverage report
```

### TDD Workflow

Red-green-refactor with separate commits per phase:
1. **Red** — write failing tests, commit alone
2. **Green** — minimal code to pass, commit separately
3. **Refactor** — simplify, commit separately

### Code Organization

- Files should be under 500 lines. When a file grows past ~500 LOC, split it by concern. Each concern (stuck detection, PR polling, review nudge, etc.) belongs in its own file with colocated tests.
- Split by concern within a package, not by creating new packages. Methods on a shared struct can live in separate files.
- Patch coverage on PRs must be at least 80%.

### Test Philosophy

- Table-driven tests with `t.Run` and `t.Parallel()`
- Integration tests for daemon lifecycle and amux interaction
- Golden files for CLI output where applicable
- Run targeted test slices with `-count=100` before calling work done

### Pre-Push

Rebase onto `origin/main` before first push.

## Safety Rules

- **Orca must never kill worker panes automatically.** Stuck detection should notify and set health to "escalated", but leave panes running. Only `orca cancel` (explicit user action) may kill panes. Destroying panes destroys in-progress work.
- **Orca must never merge PRs.** Merge is a user decision. Orca can detect PR state and notify, but never call `gh pr merge`.

## Configuration

```
~/.config/orca/state.db       # global: tasks, workers, clones, event log
.orca/pool/                   # auto-created clone pool (gitignored)
```

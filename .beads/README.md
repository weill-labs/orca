# orca beads database

This directory is orca's [beads](https://github.com/gastownhall/beads) (`bd`)
issue tracker — the in-repo, dependency-aware **work graph** for orca development.

- `issues.jsonl` is the reviewable source of truth (one issue per line).
- The Dolt database (`embeddeddolt/`) and all runtime/lock/credential files are
  gitignored; only `config.yaml`, `metadata.json`, and `issues.jsonl` are tracked.

Workflow, the Linear ↔ beads split, and the `bd` (Go/Dolt) vs `br` (rust)
distinction are documented in the repo-root `CLAUDE.md` → "Task tracking
(beads + Linear)". Linear is the system of record; beads is seeded pull-only.

Use the Go/Dolt `bd` binary. Install per your platform's package manager or the
project's internal install docs — and verify the download checksum before first
run. When initializing in a repo, pass `bd init --skip-agents --skip-hooks` so it
does not overwrite `CLAUDE.md`, the `AGENTS.md` symlink, or git hooks.

# CLAUDE.md

## What is orca

Orca is a deterministic agent orchestration daemon for amux. It manages the full lifecycle of coding tasks: clone allocation, agent spawning, health monitoring, PR merge detection, and cleanup. It owns no AI — all non-deterministic intelligence lives in the worker agents (Claude Code, Codex, etc.).

See [docs/specs/orca-design.md](docs/specs/orca-design.md) for the full design document.

Linear project: https://linear.app/weill-labs/project/orca-0582da3ac28f — file all orca issues under this project.

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

All development follows red-green-refactor with **separate commits** for each phase:

1. **Red** -- Write failing tests. Commit them alone. Confirm they fail for the right reason (missing feature, not a syntax error).
2. **Green** -- Minimal production code to make tests pass. Commit separately.
3. **Refactor** -- Simplify, extract helpers, remove duplication. Commit separately.

### Code Organization

- Files should be under 500 lines. When a file grows past ~500 LOC, split it by concern. Each concern (stuck detection, PR polling, review nudge, etc.) belongs in its own file with colocated tests.
- Split by concern within a package, not by creating new packages. Methods on a shared struct can live in separate files.
- Patch coverage on PRs must be at least 80%.

### Test Philosophy

Tests should read like specs. Minimize logic in assertions so a human can read the test and immediately understand what behavior is expected. Use table-driven tests for unit tests with multiple cases -- define a `tests` slice of structs, iterate with `t.Run(tt.name, ...)`, and call `t.Parallel()` in each subtest. Write integration tests for daemon lifecycle and amux interaction. Use golden files for CLI output where applicable.

When a change adds a new test or modifies an existing test, run that targeted test slice with `-count=100` before calling the work done. Treat any failure in those repeated runs as a flake to investigate, not as an acceptable one-off.

**Fix flaky tests by finding the root cause.** Never make tests serial to avoid a flake — that hides the bug (shared state, resource contention, missing synchronization) instead of fixing it. If tests deadlock under `-count=3 -parallel=2`, the fix is in the code, not the test runner flags.

### Pre-Push Rebase

Rebase onto `origin/main` before the first push (`git fetch origin main && git rebase origin/main`). Multiple features often land in parallel; rebasing before push avoids repeated merge conflict resolution after the PR is open.

Do not `git pull` a dirty local `main`. If `main` has uncommitted work, leave it alone and start the next change from a fresh branch based on `origin/main` instead. Do not use `git worktree` unless the user explicitly asks for it.

If a PR is already open and `git fetch origin main` or `git pull` advances `origin/main`, refresh that PR branch onto `origin/main` before treating it as current again. After the refresh, rerun verification on the rebased branch before pushing.

### PR Title And Description

PR title and description are the permanent record of why a change was made. Write them for a reviewer seeing the diff for the first time.

**Title**: Start with the issue ID, then state what changed in imperative mood, under 70 characters. Example: "LAB-314: Timestamp crash checkpoint filenames to prevent overwriting".

**Description** must include four sections:

1. **Motivation** -- Why this change? What broke, what was missing, or what user need does it address? One to three sentences.
2. **Summary** -- What changed? Bullet the key changes. Describe the PR as a complete unit, not per-commit.
3. **Testing** -- How was it verified? Include the exact test commands a reviewer can copy-paste.
4. **Review focus** -- What should reviewers look at? Call out non-obvious design decisions, edge cases, or areas where you are least confident.

Use matter-of-fact language. State what the PR does, not how good it is. Avoid vague qualifiers like "robust", "comprehensive", "elegant", or "production-ready". If a Linear issue exists, add `Closes LAB-NNN` at the bottom.

### PR CI Ownership

After opening a PR, monitor its required checks. If CI fails, inspect the failed-check summary and failed-step logs, fix issues that plausibly come from your diff, rerun the relevant local tests, and push again. Repeat up to 3 attempts. If the failure looks flaky or unrelated to your change, say so explicitly with evidence instead of churning.

### Review Before Done

After creating or updating a PR, run a review pass and a simplification pass before considering the work done.

If a change in this repo is ready for review, open the PR proactively instead of asking whether to make one.

### User Handoffs

Before stopping to wait for user input, suggest the next concrete action the user should take or approve. Do not end at "waiting on you" without a specific next step.

### Merge Conflict Resolution

After resolving merge conflicts, run `go vet ./...` locally before committing. Git auto-merge can silently produce duplicate declarations (e.g., methods defined in both sides) that compile but fail vet.

### Verify Mergeability Before Declaring PRs Ready

Before telling the user a PR is safe to merge, check for merge conflicts with main: `git fetch origin main && git merge-tree --write-tree origin/main origin/BRANCH`. If there are conflicts, rebase the branch onto `origin/main` and resolve them before declaring ready.

### Merge Policy

GitHub PRs for this repo are squash-only. `gh pr merge --merge` and `gh pr merge --rebase` will fail.

After merging, verify local state explicitly: check that the checkout is on `main`, the worktree is clean, and `HEAD` matches `origin/main`. If you need another change after the merge, start a fresh branch and PR instead of committing follow-up fixes on local `main`.

### Pause Before Irreversible Actions on Workers

Before any irreversible action on worker panes, state what information will be lost and confirm the order of operations:

- **Before killing a process**: capture diagnostics first (`kill -3` / SIGQUIT for Go goroutine trace). The trace is the only evidence for deadlock root cause and is destroyed on kill.
- **Before mass `send-keys`**: if sending to more than 3 panes, present the message and pane list to the user for approval. Batch operations are where mistakes scale.
- **Recovery ≠ assignment**: restoring a codex process is a separate action from giving it work. After recovery, leave workers at idle prompts. Only assign specific, user-approved issues.

## Safety Rules

- **Never run `amux kill` on any pane without explicit user confirmation.** Worker panes contain in-progress work that is destroyed on kill and cannot be recovered. When a spawn or assign fails, report the error and wait — do not kill existing panes to "fix" the problem. If you believe a pane needs to be killed, state which pane, what work it contains, and ask the user before proceeding. This applies to both `amux kill` and `orca cancel` (which kills the pane).
- **Orca must never kill worker panes automatically.** Stuck detection should notify and set health to "escalated", but leave panes running. Only `orca cancel` (explicit user action) may kill panes. Destroying panes destroys in-progress work.
- **Orca must never merge PRs.** Merge is a user decision. Orca can detect PR state and notify, but never call `gh pr merge`.

## Configuration

```
~/.config/orca/state.db       # global: tasks, workers, clones, event log
.orca/pool/                   # auto-created clone pool (gitignored)
```

# Orca Reconcile

`orca reconcile` compares orca task state with live amux panes in both directions and reports drift.

By default the command only reports findings and exits 0. It never kills panes and never cancels tasks.

## Findings

| Finding | Meaning | Automatic `--fix` behavior |
| --- | --- | --- |
| `recoverable_ghost` | Orca has a non-terminal task, the tracked pane is gone, and GitHub confirms the PR is merged. | Mark the task done, emit merge/completion events, release the clone, and skip postmortem because the pane is gone. |
| `abandoned` | Orca has a non-terminal task, the tracked pane is gone, and no merged PR can be confirmed. | Report only. |
| `stuck_cleanup` | Orca has a non-terminal task with a live pane, but GitHub says the PR is merged or closed. | Only merged PRs are completed automatically; closed-unmerged PRs are report-only. |
| `orphan_pane` | An amux pane named `w-LAB-*` exists but orca has no active task for that issue. | Report only. |

`orca reconcile --fix` only completes cleanup for tasks whose PR is confirmably merged on GitHub. It does not kill orphan panes, cancel abandoned tasks, or otherwise destroy active work.

Each finding is also emitted as a `reconcile.finding` orchestration event so it appears in `orca events`.

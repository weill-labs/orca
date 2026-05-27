# Worker → Lieutenant Communication

Date: 2026-05-27. Linear: LAB-1946. Status: design spike (no implementation yet).

## Motivation

orca workers are black boxes. Communication is **one-way and lieutenant-pulled**:
orca monitors a worker and emits events (`orca events` / `orca status`) that the
lieutenant polls. A worker cannot proactively reach the lieutenant to **ask a
question** ("which approach do you want?") or **notify a milestone** ("PR is up").

The `alphazero` amux window demonstrates the pattern we want. Each codex worker is
told its lead pane (`pane-1771`) and simply:

```
amux send-keys 1771 "PR #26"
```

The lead (a Claude Code campaign-runner) receives the message at its prompt,
reviews + merges the PR, and closes the bead. The channel is plain `send-keys` to
a pane id the worker was handed in its brief — lightweight and bidirectional.

## Current state

- Workers do not know who spawned them; the assign prompt carries no spawner id.
- orca's [design doc](orca-design.md) "Notifications" section deliberately
  **avoided** `send-keys` to the lead pane, on the theory it "would corrupt Claude
  Code's state if it's mid-task — it has no input queue, just a PTY."
- Empirical counter-evidence: alphazero sends to the lead pane and it works.
  Claude Code queues injected input at its prompt, and offers a `/btw` affordance
  for "a quick side question without interrupting." So the caution is "be careful,"
  not "impossible."
- orca already has a `notification_pane` config concept (orca-design.md) — a pane
  orca can post to. This feature generalizes it to the **worker→lieutenant**
  direction.

## Proposed design

On assign, orca injects two things into the worker's brief:

1. The **lieutenant/notification pane id** (the destination).
2. A short **"how to reach me" convention**, e.g.:
   > To notify or ask the lieutenant, run `amux send-keys <PANE> "<one-line message>"`.
   > Use it for: a blocking question before you guess, or a milestone (PR opened).
   > Keep messages to one line; do not spam.

No daemon-state change and no FSM change — this is **prompt plumbing plus a
documented convention**. orca already knows the spawning context at assign time;
it just needs to pass the pane id through to the prompt builder.

### Manual vs autonomous spawner

- **Manual `orca assign`:** the spawner *is* a human/lieutenant pane (the caller).
  orca reads `AMUX_PANE` today for the split target; the same value is the natural
  notify target. Closest match to alphazero.
- **Autonomous pull loop:** orca-the-daemon spawned the worker — there is **no
  human pane**. The notify target resolves to the configured `notification_pane`
  (a designated lieutenant/monitor pane), or, absent one, the worker falls back to
  orca's existing escalation/event path (no direct pane to message).

So the destination is: `assign --notify-pane` override → else caller `AMUX_PANE`
(manual) → else `notification_pane` config (autonomous) → else omit the convention.

### send-keys safety

- Workers send **short, single-line** messages; encourage the `/btw`-style
  "side question" affordance so the lead isn't interrupted mid-task.
- Consider a soft guardrail (rate/size) so a misbehaving worker can't spam or
  corrupt the lead. A structured channel (Agent Mail / a message queue the lead
  polls) would be more robust than raw `send-keys` — see open questions.

## Decisions (proposed)

| Decision | Choice | Rationale |
|---|---|---|
| Channel | `amux send-keys` to a pane id, via prompt convention | Matches alphazero; zero daemon code; works today |
| Destination resolution | notify-pane flag → caller `AMUX_PANE` → `notification_pane` → omit | Covers manual + autonomous; degrades gracefully |
| Daemon changes | None (prompt plumbing only) | Keeps the stateless-daemon contract intact |

## Open questions

1. **Raw `send-keys` vs a structured channel.** `send-keys` is simplest and proven,
   but a structured worker→lieutenant message log (Agent Mail-style, or an
   `orca notify` subcommand that records to the event log + posts) would be
   auditable, rate-limitable, and survive a busy/absent lead. Worth comparing
   before committing to raw `send-keys` as the only mechanism.
2. **Acknowledgement.** Does a worker that *asks* a question block until answered,
   or fire-and-continue? alphazero workers only *notify* (fire-and-forget). True
   ask-and-wait needs the worker to watch for a reply (poll its own pane, or a
   reply file).
3. **Autonomous-loop lieutenant.** In the pull-loop model, is there always a
   `notification_pane`? If not, worker questions should escalate via orca's
   existing stuck/escalation path so they aren't lost.
4. **Profile-level vs per-assign.** Should the convention live in the agent
   profile (so every worker gets it) or be per-assign? Likely profile default with
   per-assign override of the target pane.

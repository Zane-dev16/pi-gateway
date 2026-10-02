# Busy queue

Busy queue lets a user send a second prompt while a turn is still running:
the default policy holds it instead of interrupting, and `/queue` lines a
prompt up explicitly for the next turn.

## Sub-features

- `busy-reject-default` holds a second plain prompt while busy (no interrupt).
- `queue-explicit` accepts `/queue <text>` and runs it after the current turn.
- `stop-interrupt` lets `/stop` interrupt the running turn.

## How to get to it (user POV)

- Send a long-running prompt, then send a second message while it runs.
- Send `/queue <text>` while a turn is running.
- Send `/stop` while a turn is running.

## Driving it with control-pi-gateway

Preconditions:

- Gateway launched WITH telegram env and doctor HEALTHY (live path only).
- A prompt that runs long enough to overlap (e.g. asking for a long answer).

- **Start long turn.** Send the long prompt. The bot shows typing / a draft reply.
- **Second prompt held.** Send a second plain message. The turn is not interrupted; the second prompt is held or queued per the L2 `reject` default (busy resolver), never silently dropped.
- **Explicit queue.** Send `/queue second task`. The bot acknowledges the queue and runs it after the current turn finishes.
- **Proof.** Both replies in `artifacts/busy-queue/reply.txt` in order, plus `control-pi-gateway db --home "$HOME_DIR" -- "SELECT COUNT(*) FROM messages"` before and after in `artifacts/busy-queue/db.txt` showing both turns persisted.

## Gotchas

- Timing-sensitive: the second message must land while the first turn is still running. If the first turn already finished, the run proves serial turns, not queueing — retry with a longer prompt.
- `/stop` and `/queue` have `dispatch` busy policy (they bypass the reject gate); plain prompts do not. Do not assert identical handling.
- Live-only feature: no headless scenario covers the guard chain end to end. Report `unreachable without telegram env` rather than substituting a unit-test pass.

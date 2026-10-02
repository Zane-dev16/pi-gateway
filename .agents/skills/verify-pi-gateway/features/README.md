# pi-gateway verification map

This directory is the maintained source for verifying the user-facing
behavior of pi-gateway. Read this index before driving the app, then use
the matching feature file as the recipe.

## Baseline preconditions

- Never touch the live `~/.pi` home; every run uses a disposable home
  `HOME_DIR=/tmp/pi-verify-$RUN_ID` passed as `--home` to every command.
- `bin/pi-gateway` runs from `/root/pi-gateway/pi-gateway`.
- Run `scripts/control-pi-gateway doctor --home "$HOME_DIR"` and require
  HEALTHY before driving chat features.
- Live-chat runs need `TELEGRAM_BOT_TOKEN` + `TELEGRAM_ALLOWED_USERS` in
  the launcher env and are exclusive per bot token (409 Conflict on
  double-poll). Headless and CLI runs are parallel-safe.
- Never drive an instance that was not started by this verification run.

## Driving conventions

- Start every recipe from the baseline state unless its preconditions say
  otherwise.
- Prefer stable handles (chat URL fragment `#8979458891`, command text
  `/help`, reply text quoting a builtin name) over coordinates and tab order.
- Treat every command as literal. Keep quoted names and flags unchanged.
- Run CLI actions through `control-pi-gateway cli --home "$HOME_DIR" -- …`.
- Run DB reads through `control-pi-gateway db --home "$HOME_DIR" -- "…"`.
- Restore nothing (gateway state is append-only); do not remove proof
  artifacts during cleanup.

## Proof and skip reporting

- Capture the user action and the resulting state, not only the final screen.
- CLI proof includes the command, stdout, stderr, and exit code.
- Mutation proof includes a read-only second view of the stored value
  (`messages` / `delivery_obligations` row counts or rows).
- Record the feature ID and entry point used with every artifact.
- Report an unreachable path with the attempted command and the unmet
  precondition (e.g. live chat without `TELEGRAM_BOT_TOKEN`).
- Do not report a skipped entry point as verified through a different path.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the
user-visible behavior. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line for each behavior.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with control-pi-gateway` starts with `Preconditions:` and
   uses labeled bullets that pair each user action with an exact command
   and observable result.
4. `Gotchas` lists traps that can waste or invalidate a verification run.

Keep implementation details out of the map. Name only user paths, stable
handles, required state, commands, and observable proof.

## Features

- [Gateway lifecycle](./gateway-lifecycle.md) covers run, status, stop,
  stale-lock refusal, and log evidence on a disposable home.
- [Chat help and status](./chat-help-status.md) covers `/help` and
  `/status` in the Telegram test chat and their replies.
- [Chat turn](./chat-turn.md) covers a plain prompt turning into an agent
  reply with persistence in `state.db`.
- [Busy queue](./busy-queue.md) covers queuing a second prompt while a
  turn is running and its later execution.

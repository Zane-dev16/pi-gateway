# Chat turn

A chat turn lets a Telegram user send a plain prompt and receive the
agent's reply, persisted as message rows so a restart can replay the
conversation instead of losing it.

## Sub-features

- `turn-reply` answers a plain prompt with an agent reply in the chat.
- `turn-persist` stores the inbound and reply rows in `state.db`.
- `turn-offline-shape` proves the same transport shape headless (Bot-API
  fake, no network, no token).

## How to get to it (user POV)

- Open the test chat `https://web.telegram.org/a/#8979458891` and send a plain message such as `Reply with the word PINEAPPLE`.
- Or run the headless shape check from a terminal (no chat needed).

## Driving it with control-pi-gateway

Preconditions:

- For the live path: gateway launched WITH telegram env (see chat-help-status
  preconditions) and doctor HEALTHY.
- For the headless path: nothing running; repo `node_modules` installed.

- **Live prompt.** Send `Reply with the word PINEAPPLE`. The bot answers with a message containing `PINEAPPLE`.
- **Live persistence.** Confirm the stored rows. Run `control-pi-gateway db --home "$HOME_DIR" -- "SELECT role, substr(content,1,60) FROM messages ORDER BY rowid DESC LIMIT 2"`. The output shows the inbound prompt and the reply.
- **Headless shape.** Run `control-pi-gateway headless --scenario send-receive`. Vitest passes on `src/pi_platforms/telegram/telegram.test.ts`, proving the real adapter engine sends/receives over Telegram wire shapes with zero network.
- **Proof.** Live: reply text in `artifacts/chat-turn/reply.txt` plus the DB rows in `artifacts/chat-turn/db.txt`. Headless: vitest tail in `artifacts/chat-turn/headless.txt` labeled with the scenario.

## Gotchas

- A plain prompt needs a configured model/provider; `/help` does not. No reply to a plain prompt with a working `/status` means the provider (not the gateway) is down — check the provider before blaming the turn path.
- `messages.content` may be long; the recipe truncates to 60 chars for the assertion. Assert the marker word, not full equality.
- The DB lives inside `$HOME_DIR`; capture `db.txt` before cleanup.
- Headless proves wire shape, not model output. Never report a headless pass as a live-turn proof.

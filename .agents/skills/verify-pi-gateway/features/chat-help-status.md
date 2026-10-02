# Chat help and status

Chat help and status let a Telegram user ask the bot what it can do and
what its session looks like, served from the single command registry
without needing a model call.

## Sub-features

- `help-list` answers `/help` with the available commands.
- `help-filter` answers `/help <text>` filtered to matching commands.
- `status-show` answers `/status` with session, model, and token info.

## How to get to it (user POV)

- Open the test chat `https://web.telegram.org/a/#8979458891` and send `/help`.
- Send `/help status` in the same chat.
- Send `/status` in the same chat.

## Driving it with control-pi-gateway

Preconditions:

- Gateway launched WITH `PI_GATEWAY_PLATFORMS=telegram`,
  `TELEGRAM_BOT_TOKEN`, and `TELEGRAM_ALLOWED_USERS` containing the sender id.
- `control-pi-gateway doctor --home "$HOME_DIR"` reports HEALTHY.
- The loki Chrome CDP endpoint answers (`control-pi-gateway cdp-targets`
  ends with `CDP_OK`).

- **Open chat.** Open the test chat. Attach via CDP to loki
  `localhost:9222` and navigate to `https://web.telegram.org/a/#8979458891`. The chat with the bot is visible.
- **Help.** Send `/help`. The bot replies listing commands; the reply text contains `status` and `new`.
- **Help filter.** Send `/help status`. The reply names `/status` and does not list unrelated commands.
- **Status.** Send `/status`. The reply contains session info (a session or model line, not an error).
- **Proof.** Save the reply texts to `artifacts/chat-help-status/reply.txt` with the feature ID and entry point, plus a CDP screenshot to `artifacts/chat-help-status/reply.png` showing the chat and the bot reply.

## Gotchas

- If the sender id is not in `TELEGRAM_ALLOWED_USERS` the adapter drops the message silently — no reply is an authz misconfiguration, not a help defect. Check the env first.
- Two gateways on one bot token produce 409 Conflicts and flapping replies. Live-chat runs are exclusive; confirm no other run holds the token.
- Web-Telegram needs the loki Chrome profile signed in. If the chat shows a login page, report the unmet precondition instead of driving further.
- Headless `send-receive` does not cover `/help` text; it proves transport shape only. Do not claim live help from a headless pass.

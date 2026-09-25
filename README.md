# Pi Gateway

Messaging gateway for the pi coding agent. It runs as one long-lived pi extension and turns chat messages into serialized, durable agent turns on a single SQLite database. It reuses the host pi agent loop, so it needs a working pi install with a provider configured.

## Setup

1. Install as a pi package:

   ```sh
   pi install npm:@irellzane/pi-gateway
   ```

2. Choose platforms and set secrets:

   ```sh
   export PI_GATEWAY_PLATFORMS="telegram,matrix"
   export TELEGRAM_BOT_TOKEN="123456:ABC..."
   export TELEGRAM_ALLOWED_USERS="your-telegram-user-id"
   ```

   Authorization is deny-by-default, so the adapter accepts no one without the allowlist.

3. Run pi in RPC mode with auto-start:

   ```sh
   PI_GATEWAY_AUTO_START=1 PI_GATEWAY_PLATFORMS="telegram,matrix" pi --mode rpc
   ```

4. Message your bot from an allowed user and check `/help` and `/status` in chat.

5. State lives under your profile home (default `~/.pi`, override with `PI_HOME`): `state.db`, `logs/agent.log`, `logs/errors.log`, `gateway_state.json`.

More detail: `docs/quickstart.md`, `docs/installation.md`, `docs/configuration.md`, `docs/platforms.md`, `docs/operations.md`, `docs/troubleshooting.md`, `docs/architecture.md`.

License: MIT, see `LICENSE`.

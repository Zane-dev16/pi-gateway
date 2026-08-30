# Pi Gateway

[![license](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

Pi Gateway is a messaging gateway for the
[pi coding agent](https://www.npmjs.com/package/@earendil-works/pi-coding-agent).
It is one long-lived process that converts asynchronous chat traffic into
serialized, durable agent turns against a single shared SQLite substrate, and
a TypeScript port of the
[Hermes Gateway](https://github.com/NousResearch/hermes-agent) architecture
(spec 01 §1) that reuses the host pi agent loop directly (DEC-023).

## Quick start: chat app in 5 minutes

You need a working pi installation with a model/provider configured — the
gateway reuses the host pi agent loop directly (DEC-023), so turns cannot
run until pi can reach a provider — plus Node.js 26+ at runtime
(better-sqlite3 native driver).

1. Install as a pi package:

   ```sh
   pi install npm:@irellzane/pi-gateway
   ```

   Or from git (same code, no registry):

   ```sh
   pi install git:github.com/Zane-dev16/pi-gateway
   ```

   For local development, clone and link instead (see
   [CONTRIBUTING.md](CONTRIBUTING.md)):

   ```sh
   git clone https://github.com/Zane-dev16/pi-gateway
   cd pi-gateway
   pi install . -l
   ```

2. Set one platform secret. This example uses Telegram; every adapter declares
   its required secrets in a manifest (spec 04 §4):

   ```sh
   export TELEGRAM_BOT_TOKEN="123456:ABC..."
   export TELEGRAM_ALLOWED_USERS="your-telegram-user-id"
   ```

   `TELEGRAM_ALLOWED_USERS` is the sender allowlist. Authorization is
   deny-by-default (spec 06 §2), so without it the bot accepts no one.

3. Run the gateway:

   ```sh
   pi gateway run          # see docs/quickstart.md for first-run setup
   ```

4. Message your bot `hello` from an allowed user, then try `/help` (command
   list) and `/status` (adapters, worker pool, leases, delivery backlog) in
   chat.

5. Verify state on disk under `PI_HOME` (default `~/.pi`): `state.db` (the
   SQLite substrate), `logs/`, and `gateway_state.json`.

The full walkthrough, including shutdown behavior and what each file is, is
[docs/quickstart.md](docs/quickstart.md).

## At a glance

- 31 platform surfaces across three transport shapes: polling, persistent
  WebSocket, and webhook (DEC-002), all gated by one executable conformance
  suite (spec 04 §8).
- Turn serialization that holds under contention: in-process guard layers +
  a cross-process lease keyed on the compression-lineage root, TTL 300s
  (spec 02 §5, DEC-004).
- Streaming with a contract: prefix-stable drafts, `finish(final_text)` is
  authoritative, and seal interception at every egress door (DEC-006,
  spec 04 §5).
- Durable delivery: an obligations ledger with capped attempts and retention;
  timeout-classified send failures are never blind-retried (DEC-046, DEC-054).
- One command registry: busy policies, help, menus, and completions all derive
  from a single frozen registry (spec 07 §1, DEC-005).
- Fail-closed security: scoped secrets, deny-by-default authorization,
  pairing, per-credential token locks, and signed webhooks (spec 06,
  DEC-003/009).
- Transactional operations: plan → snapshot → apply → restart-per-kind →
  verify → receipt; a mixed-version fleet is never "healthy" (spec 08 §8).

## Documentation

| Document                                       | Contents                                                        |
| ---------------------------------------------- | --------------------------------------------------------------- |
| [docs/quickstart.md](docs/quickstart.md)       | First run in ~5 minutes: install, one platform, say hello       |
| [docs/installation.md](docs/installation.md)   | Requirements, `PI_HOME` layout, profiles, service installation  |
| [docs/configuration.md](docs/configuration.md) | Config vs secrets, authorization env vars, platform manifests   |
| [docs/architecture.md](docs/architecture.md)   | Layers, turn lifecycle, invariants, embedded services           |
| [docs/platforms.md](docs/platforms.md)         | The 31 supported surfaces and their transport shapes            |
| [docs/adding-a-platform.md](docs/adding-a-platform.md) | Build a new adapter and pass the conformance gate       |
| [docs/operations.md](docs/operations.md)       | Signals, health, logs, takeover, update pipeline                |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Symptom → cause → fix for common failure modes              |
| [CHANGELOG.md](CHANGELOG.md)                   | Release history                                                 |
| [CONTRIBUTING.md](CONTRIBUTING.md)             | Dev setup, test gates, divergence rule, platform checklist      |
| [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)       | Contributor Covenant 2.1                                        |
| [SUPPORT.md](SUPPORT.md)                       | Where to ask questions and report bugs                          |
| [LICENSE](LICENSE)                             | MIT license                                                     |

## How a turn flows

Platform adapters receive inbound events from messaging surfaces (Telegram,
Discord, Slack, WhatsApp, …), pass them through a two-level busy guard into a
two-layer turn lease (DEC-004), and stream agent output into platform-native
drafts that are edited and sealed. Final responses flow through a delivery
obligations store before egress (DEC-054). Cron, kanban, handoff, hooks, and
plugins are embedded services inside the same process (spec 01 §4); CLI, TUI,
and dashboard are clients of the same `state.db`, never co-owners.

1. An adapter normalizes an inbound event into a `MessageEvent`; the adapter's
   L1 guard merges or queues it (spec 03 §2).
2. The runner's L2 guard applies the registry-driven busy policy:
   `dispatch | reject | interrupt_then_dispatch` (DEC-005).
3. The turn lease is acquired in two layers: in-process registry +
   cross-process DB row keyed on the compression-lineage root, TTL 300s,
   bounded wait (spec 02 §5, DEC-004).
4. The synchronous agent core runs on a bounded worker pool; stream deltas pipe
   into the stream consumer, which edits and seals platform-native drafts
   (spec 01 §2, DEC-006).
5. Egress goes through the delivery-obligations ledger (caps: 3 attempts,
   24h stale, 7d retention, 500 rows) and then the adapter's audited send doors
   (DEC-053/054). A drain boundary hands any overflow to a fresh task
   (spec 03 §5).

The full picture, including the state schema and the embedded services, is in
[docs/architecture.md](docs/architecture.md).

## Fidelity

Pi Gateway ports Hermes Gateway's *architecture*, not just its features: the
load-bearing mechanisms (guards, turn lease, stream consumer, delivery
obligations, command registry, session store, secret scoping, profile scoping)
are reproduced semantically, with every behavioral claim verified against the
reference source at a named `file:symbol` anchor. Any deliberate divergence is
recorded in the decision log before implementation; zero sanctioned silent
drift (DEC-026).

The specification lives in the parent workspace: the normative document set
starts at [../README.md](../README.md), and the decision log (DEC-001…069) is
[../09-open-questions.md](../09-open-questions.md). Citations in these docs
such as "02 §5, DEC-004" refer to that set.

## Repository layout

```
pi-gateway/
├── docs/               ← the documentation you are reading
├── extensions/         ← pi extension bindings
├── scripts/            ← layering + secret-scope gates
└── src/
    ├── entrypoints/    ← 'pi gateway run' composition root (DEC-058)
    ├── pi_home.ts      ← PI_HOME accessor (spec 01 §6)
    ├── pi_state/       ← SQLite substrate: schema, leases, messages, usage
    ├── pi_agent_core/  ← worker pool, agent runner, cache, alternation repair
    ├── pi_gateway/     ← guards, streaming, obligations, registry, security
    ├── pi_platforms/   ← adapters, kit, conformance suite
    └── pi_embedded/    ← cron, kanban, handoff, hooks, update
```

Dependencies flow downward only (`pi_home → pi_state → … → entrypoints`);
`scripts/check-layering.mjs` enforces it in CI (spec 01 §5.3).

## Status

v0.1.0: the full spine plus the 31-surface platform census is implemented and
covered by 3096 behavior-contract tests across 237 files. See
[CHANGELOG.md](CHANGELOG.md) for what shipped.

## Contributing, support, conduct

- To file bugs or ask questions, start with [SUPPORT.md](SUPPORT.md).
- Before opening a PR, read [CONTRIBUTING.md](CONTRIBUTING.md), which covers
  the required test gates and the divergence rule (DEC-026).
- This project follows the Contributor Covenant:
  [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## License

[MIT](LICENSE), Copyright (c) 2026 Irell Zane.

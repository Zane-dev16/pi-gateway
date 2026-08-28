# Support

Pi Gateway is a messaging gateway for the [pi coding agent](README.md) that
turns chat traffic from 31 platforms into serialized agent turns. This file
explains where to get help and how to report problems well.

## Where to ask

| You want to…                              | Go to                                                        |
| ----------------------------------------- | ------------------------------------------------------------ |
| Ask a question / get setup help           | GitHub Discussions                                           |
| Report something broken                   | GitHub Issues (use the bug template below)                   |
| Ask about a specific platform adapter     | GitHub Discussions, tagged with the platform name            |
| Propose a behavior change                 | Read [CONTRIBUTING.md](CONTRIBUTING.md) first (DEC rule)     |
| Report a security vulnerability           | Privately — see "Security" below                             |

Before filing, please check
[docs/troubleshooting.md](docs/troubleshooting.md) — many startup, delivery,
and configuration questions are answered there — and search existing issues.

## Filing a good bug report

Include the following where relevant:

1. **What you ran** — command line, `pi gateway run` or a client surface, and
   the gateway version (`gateway_state.json` under your `PI_HOME` records
   `code_sha`/`code_version`).
2. **What you expected vs what happened** — one clear sentence each.
3. **Platform surface** — e.g. Telegram, Discord, Slack, and whether the
   failure was inbound (receiving) or outbound (delivery).
4. **Logs** — the relevant tail of `logs/errors.log` and
   `logs/gateway.log` under your `PI_HOME`. Reason codes in denial/retry
   lines are especially useful.
5. **Config** — the platform env vars you set (redact secrets!), your
   `dm_policy`/`group_policy`, and whether you run multiplex profiles.
6. **Reproduction** — minimal steps, or "intermittent" with frequency.

Please do not paste bot tokens, pairing codes, or message content from private
conversations.

## Asking about adapters and platforms

- Supported surfaces and their transport shapes:
  [docs/platforms.md](docs/platforms.md).
- Per-surface configuration (secrets, allowlists, policies):
  [docs/configuration.md](docs/configuration.md).
- Building a new adapter and passing the conformance gate:
  [docs/adding-a-platform.md](docs/adding-a-platform.md) and
  [CONTRIBUTING.md](CONTRIBUTING.md).

For "does surface X support Y" questions, the manifest is the source of truth
(secrets, capabilities, rate-tier and trust-boundary data are declared per
platform, per spec 04 §4 and DEC-017) — tag your question with the platform
name and link the manifest you looked at.

## Security

Do **not** open a public issue for security-sensitive reports (secret-scope
bypasses, authorization bypasses, webhook signature issues). Report them
privately to the maintainers — open a GitHub issue marked private to
maintainers or contact the owner listed in the git history — and include
reproduction steps and impact. We will acknowledge and work with you before
any public disclosure.

## Scope of support

Pi Gateway ports the Hermes Gateway architecture on top of the pi agent loop;
questions about pi itself (skills, extensions, providers) belong to the pi
project. Gateway-adjacent questions (routing, adapters, state) are in scope
here.

## Project status

See [CHANGELOG.md](CHANGELOG.md) for the current release and
[docs/operations.md](docs/operations.md) for health, logs, and update
semantics.

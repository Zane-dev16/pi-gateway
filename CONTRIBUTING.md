# Contributing to Pi Gateway

Thanks for your interest in contributing. Pi Gateway is a fidelity port: its
value is architectural parity with the Hermes Gateway reference, so changes are
held to a stricter-than-usual bar. This document explains the ground rules and
the required gates.

## Development setup

Requirements: Node.js 26+, npm, git.

```sh
git clone <your fork>
cd pi-gateway
npm ci                # installs exact lockfile deps (includes better-sqlite3)
npm run build         # tsc --noEmit — must be clean
npm test              # full vitest suite (~3096 tests, 237 files)
```

## Test suite

Tests are **behavior contracts**: they assert how two pieces of data must
relate — invariants, state machines, race outcomes, two-process contention,
byte-exact round-trips. Change-detector tests (model catalogs, version
literals, source-text regexes) and snapshot tests of vendor error strings are
banned. Host-dependent behavior runs on its host via OS-marked lanes, never by
faking the platform.

The suite runs as two vitest projects (DEC-041):

- `default` — everything, in parallel.
- `heavy-process` — specs that spawn real OS child processes (two-process
  contention, takeover, token-lock racers, WAL writers). These run with
  `fileParallelism: false` because parallel fork load starves their children on
  small-CPU hosts. If your spec imports `node:child_process`, add it to the
  `heavy-process` list in `vitest.config.ts`.

`npm test` runs both projects. Run one during iteration, e.g.
`npx vitest run src/pi_state`.

## Required gates

A PR is not reviewable until all of the following pass:

| Gate                       | Command                                | What it enforces                                                   |
| -------------------------- | -------------------------------------- | ------------------------------------------------------------------ |
| Types                      | `npm run build`                        | `tsc --noEmit`, zero errors tree-wide                              |
| Tests                      | `npm test`                             | Full behavior-contract suite, both projects                        |
| Layering                   | `npm run check:layering`               | Downward-only dependency rule (spec 01 §5.3)                       |
| Secret scope               | `npm run check:secrets`                | Fail-closed secret scope grep gate (spec 06 §3.2, DEC-003/009)     |

The layering gate rejects upward imports and any adapter/embedded-service
import of runner internals. The secret-scope gate bans after-a-scoped-miss
fallback to raw env except inside the one canonical wrapper — adapters must
not hand-roll variants.

## The divergence rule (DEC-026)

**Zero sanctioned divergences.** Any semantic divergence from Hermes Gateway
behavior requires a decision-log entry (a "DEC") **before** implementation.
The log is append-only and lives with the specification
([../09-open-questions.md](../09-open-questions.md)); the normative docs are
introduced in [../README.md](../README.md). When a PR changes behavior, cite
the spec section and DEC anchor (e.g. "02 §5, DEC-004") in the description. If
you believe a divergence is needed, propose the DEC first — do not implement
and self-log after the fact.

## Adding a platform

New platforms inherit one of the three reference transport shapes (polling,
persistent WebSocket, webhook — DEC-002) and must pass the 04 §8 conformance
suite before merge. In practice:

1. Pick the transport shape and read its reference adapter under
   `src/pi_platforms/` (`polling/`, `persistent-ws/`, `webhook/`).
2. Implement your adapter against `src/pi_platforms/kit/base-adapter.ts`:
   capabilities are manifest data, the base owns guards/chunking/retry, you
   supply transport, formatting, and per-chat probes.
3. **Inherit the reference adapter's conformance fixture** for your shape —
   the polling/persistent-ws/webhook fixtures already encode the transport
   rows; write only your shape deltas.
4. **Declare manifest data**: required/optional secrets, rate tiers, trust
   boundaries for HTTP ingress (DEC-017), callback/format capability data.
5. **Pass the §8 gate**: all applicable conformance rows green with zero
   deferred (`allApplicablePassed === true`). No core diff beyond
   registration is accepted.

Full walkthrough: [docs/adding-a-platform.md](docs/adding-a-platform.md).

## Pull requests

- One logical change per PR; keep the gates green.
- New behavior needs either a spec citation or a pre-logged DEC (see above).
- New test files follow the behavior-contract rules above; prefer
  real-path/two-process contracts over mocks for integration claims.
- Update [docs/](docs/) when you change operator-visible behavior, and add a
  line under [CHANGELOG.md](CHANGELOG.md) "Unreleased".

## Conduct

This project follows the Contributor Covenant:
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## License

By contributing you agree that your contributions are licensed under the
[MIT License](LICENSE).

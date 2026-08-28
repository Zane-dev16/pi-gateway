# Adding a platform

How to add a new platform adapter to Pi Gateway. The short version: inherit
one of the three reference transport shapes, encode the conformance rows for
your shape, and merge with zero core diff beyond registration
(spec 04 §8, DEC-002).

Three ground rules apply. Any semantic divergence from Hermes behavior needs
a decision-log entry before implementation (DEC-026, see
[../CONTRIBUTING.md](../CONTRIBUTING.md)). Adapters never import the runner:
they depend on the base kit and registries, and the runner sees them only
through registration (spec 01 §5.3); `npm run check:layering` enforces this.
Capabilities are data: shared code contains no per-platform branching, and
call sites read manifest and capability data (spec 04 §1).

## 1. Choose the transport shape

| Shape   | Choose when the platform…                                      | Reference |
| ------- | -------------------------------------------------------------- | --------- |
| Polling | offers a long-poll/sync SDK you drive on a cadence             | `src/pi_platforms/polling/` |
| WS      | offers a persistent websocket/stream with replay + keepalive   | `src/pi_platforms/persistent-ws/` |
| Webhook | pushes signed HTTP callbacks you must verify and bound         | `src/pi_platforms/webhook/` |

Read the reference adapter end to end: it encodes the shape's obligations
(polling: generations, queue preservation, 409-conflict recovery; ws: replay,
keepalive, `retry_after` capture (DEC-044); webhook: HMAC, replay windows,
trust boundaries (DEC-017)).

## 2. Implement against the base kit

Build on `src/pi_platforms/kit/base-adapter.ts`. The base owns the L1 guard,
pending-merge, chunking, send-retry, token locks, log redaction, and the
egress-door discipline. You supply the rest:

| Piece | What you provide |
| ----- | ---------------- |
| Transport | `connect(*, is_reconnect)` / `disconnect()`. A reconnect must preserve server-side queues (polling) or replay (ws); cold boots may drop stale ones. |
| Ingress | Normalize vendor events into `MessageEvent`s and hand them to the base. Never implement your own busy or queueing logic. |
| Egress | `send()` and `edit_message()` (both audited doors: the seal check rides them, and a failed seal must still deliver the final), plus `SendResult` with `retryable` / `retry_after` from server flood signals. |
| Formatting and probes | Per-chat length pair, streaming/draft probes, formatting ladder. Override the probe pair together where chats differ (spec 04 §6.3). |
| Interactive UX (optional) | `send_clarify`, `send_exec_approval`, and similar must match the one namespaced callback grammar (DEC-016). |

## 3. Declare manifest data

`src/pi_platforms/<name>/manifest.ts` carries the data other layers derive
from (spec 04 §4.2):

- `transportShape`: the conformance family.
- `requiresEnv` / `optionalEnv`: secrets. A missing required secret must
  disable the adapter loudly, never silently.
- `capabilities`: async delivery, native chunking, command prefix,
  interactive resume, edit-finalize requirements.
- Rate tiers and, for HTTP ingress, signature schemes and limits as data
  (DEC-017). Hardcoded flood/breaker constants are a review reject.
- Env var names for platform config (tokens, allowlists) follow the
  `{PLATFORM}_…` conventions used by the authz layer.

## 4. Inherit the reference fixture and encode your rows

Under `src/pi_platforms/conformance/`, each shape has a fixture implemented
against the real reference engine. Inherit it rather than stubbing it. Write
only your shape deltas and vendor wire contracts:

1. Instantiate the fixture against your adapter (see
   `polling-rows.test.ts` / `ws-rows.test.ts` / `webhook` rows for the
   pattern).
2. Supply every transport row your shape requires. A row that isn't encoded
   counts as deferred, and the suite reports
   `allApplicablePassed === false`; the merge gate requires zero deferred.
3. Tests are behavior contracts: races, seal checks, retry ladders,
   byte-level wire parity at the transport seam (DEC-059). No
   change-detector tests, no snapshot tests of vendor error strings.

## 5. Pass the §8 gate

The executable conformance suite (`src/pi_platforms/conformance/runner.ts`)
is the merge gate: all applicable rows pass, zero deferred, plus the standing
repo gates (`npm run build`, `npm test`, `check:layering`,
`check:secrets`; see [../CONTRIBUTING.md](../CONTRIBUTING.md)).

Review also checks two classic failure modes from the roadmap risk register.
One is declaring streaming or chunking that the adapter cannot actually seal
or split; the mutation suite catches this. The other is patching
per-platform quirks into the runner, a layering violation; quirks belong in
the adapter or the manifest.

## 6. Register and document

- Register through the platform registry (registration is the only core diff
  allowed).
- Update the census in [docs/platforms.md](platforms.md) and add your
  platform to [CHANGELOG.md](../CHANGELOG.md).
- If your surface behaves differently from Hermes in any observable way, the
  DEC must exist first (DEC-026).

## See also

[docs/platforms.md](platforms.md) lists the existing surfaces and shapes,
[docs/architecture.md](architecture.md) covers guards, leases, and
streaming, and [README.md](../README.md) is the project hub.

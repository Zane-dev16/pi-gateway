# PHASE 6 REPORT — Platform Census Ports Behind the Conformance Gate

**Verifier verdict: PASS.**

| Gate | Result |
| --- | --- |
| `npx tsc --noEmit` | **exit 0** |
| `node scripts/check-layering.mjs` | **OK** (downward-only holds across `src/`) |
| `node scripts/check-secret-scope.mjs` | **OK** (291 files) |
| `npx vitest run` — run 1 | **1842/1842 passed, 167/167 files, exit 0** |
| `npx vitest run` — run 2 (DEC-041) | **1842/1842 passed, 167/167 files, exit 0** |
| Baseline delta | +148 tests / +14 files vs 1694/1694 @ 153 files — all additive |
| Registration-only diff rule | **PASS** — `git status`: ZERO tracked files modified; entire footprint is new `src/pi_platforms/{telegram,slack,discord,whatsapp-cloud}/` (42 .ts) + 5 conformance wiring files. No `pi_gateway`/`pi_agent_core`/`pi_state`/core file touched. |

## Per-port summaries

### Telegram — EXIT GATE GREEN (shape: polling)

`manifest.ts` (Q17/DEC-017 data, every constant anchored), `markdown-v2.ts`
(escape/finalize lanes), `reactions.ts` (A1/A2), typing variants, sticker cache
(M7), FloodWait method classes. Suite: 23 shared rows + ALL FOUR inherited §3.1
polling transport rows against the REAL engine fixture
(`makeRealTelegramPollingFixture`, no stubs) + EIGHT fresh `tg.*` shape rows +
gate-negative validation (lying fixture fails its named rows).
`allApplicablePassed === true`, `deferred === []`.

### Slack — EXIT GATE PASS (shape: ws)

`SlackAdapter extends PersistentWsAdapter` — inherits the whole ws engine;
adds Socket-Mode deltas (`fake-socket-mode.ts`: hello on accept, envelopeId/
retryAttempt envelopes, cursor-driven replay #4777, Retry-After close payloads).
Suite: 23 shared rows + FIVE inherited ws-family transport rows over the REAL
adapter + SIX slack-specific required rows (envelope shapes, retry-dedup ×
cursor replay exactly-once, Block Kit round-trip through THE kit grammar,
byte-exact mrkdwn decision matrix vs RAW-native drafts, rate-tier gating BEFORE
egress w/ injected-clock rotation, approvals card-first e2e through the real
`DeliveryBridge`). Zero deferred.

### Discord — EXIT GATE PASS (shape: ws)

`manifest.ts` (opcodes, rate buckets, ping-safety defaults as DATA),
`rate-buckets.ts` (gate-BEFORE-egress ledger, authoritative 429 freeze ≥1s
clamp), `gateway-fake.ts` (v10 op envelope `{t,s,d}`, seq-keyed replay). Suite:
23 shared rows + ALL FIVE inherited ws-family transport rows over the REAL
engine fixture + gate-negative validation across all five row dimensions;
shape deltas (rate buckets, INVALID_SESSION ladder, recovery sweep, ping
safety) covered by 29 in-dir behavior tests. Zero deferred.

### WhatsApp Cloud — EXIT GATE PASS (shape: webhook)

`wa-cloud-adapter.ts` (kit base + webhook ingress + Graph edges),
`graph-wire.ts` (scriptable Meta error shapes), `window-policy.ts` (24h
classifier as DATA), `wa-markdown.ts`, LID identity. Suite: applicability
COMPUTED from capability data — the 3 streaming rows are excluded BY THE PROBE
(`supportsDraftStreaming()===false` asserted; a capability flip re-includes
them, never a hardcoded skip) — plus inherited webhook transport rows over REAL
probes (bounded-window measured while a turn is HELD) + SIX fresh `transport.wa.*`
rows (signature negative matrix 401×3, status-callback/wamid exactly-once dedup,
window class flip at EXACTLY 24h on injected clock, media caps refused PRE-upload
w/ caption riding the media block, phone↔LID canonical collapse, read-receipt
lifecycle w/ stale-wamid 131009 containment) + mutant detection (phantom-upload
fixture fails `transport.wa.media-cap-preupload` BY NAME).

## Row coverage table

| Row family | Count | TG | SL | DC | WA |
| --- | --- | --- | --- | --- | --- |
| shared (§8: ingress 4, egress 5, streaming 3, interactive 4, formatting 2, identity 3, logs.redaction 1, wake-lane 1) | 23 | ✅ all | ✅ all | ✅ all | ✅ 20 applicable (streaming×3 probe-excluded) |
| transport.polling.* (outage-reconnect-preserves-queue, held-inbound-redispatch, conflict-zombie-eviction, heartbeat-escalation) | 4 | ✅ real engine | — | — | — |
| transport.ws.* (resubscribe-replay, heartbeat-watchdog-recovery, retry-after-capture, capability-latch-permanent, dual-path-markdown) | 5 | — | ✅ real fixture | ✅ real fixture | — |
| transport.webhook.* (flags-and-trust-boundary, bounded-window-answer) | 2 | — | — | — | ✅ real probes |
| shape deltas | 8/6/0/6 | tg.* ×8 | slack.* ×6 | in-dir ×29 tests | transport.wa.* ×6 |
| deferred hooks | 0 | 0 | 0 | 0 | 0 |
| negative gate validation in-suite | — | ✅ | ✅ | ✅ | ✅ mutant-by-name |

Every suite ends with a full-catalog assertion:
`report.failed === 0 && report.deferred.length === 0 && report.allApplicablePassed === true`.

## Manifest provenance (spot-checked against READ-ONLY Hermes reference)

- **telegram**: cites `plugins/platforms/telegram/adapter.py` — VERIFIED:
  `adapter.py:650 MAX_MESSAGE_LENGTH = 4096`, `:655 RICH_MESSAGE_MAX_CHARS = 32768`,
  `_SPLIT_THRESHOLD`, FloodWait ladder wording match.
- **whatsapp-cloud**: cites `gateway/platforms/whatsapp_cloud.py` — VERIFIED:
  `:98 WEBHOOK_MAX_BODY_BYTES = 3*1024*1024`, `:103 WAMID_DEDUP_CACHE_SIZE = 5000`,
  `_MEDIA_SIZE_LIMITS`, signature scheme `_verify_signature`, window semantics
  from module docstring. Vendor-only constants labeled per Q17.
- **slack**: `plugins/platforms/slack/**` anchors throughout manifest/adapter/
  mrkdwn/block-cards/rate-gate (Socket-Mode envelope + retry_reason semantics).
- **discord**: `plugins/platforms/discord/**` anchors for opcodes, rate buckets,
  ping-safety defaults.

## Silent-capability-lie scan

Declared streaming matches seal reality everywhere: Slack/Discord declare
`supportsDraftStreaming()` true behind the A23 latch AND execute+pass the three
shared streaming rows (seal-once/no-dupe/fail-fallthrough) against their own
subjects; Telegram runs the full shared catalog including streaming; WhatsApp
Cloud declares reply-only and the exclusion is COMPUTED and asserted, not
skipped. Wake-lane row ties declaration to `supportsAsyncDelivery` per DEC-022.

## Fix applied during verification (trivial+safe, documented)

- **`src/pi_platforms/discord/gateway-fake.ts` — `GatewayClientSocketImpl.serverAccept()`**:
  Run 1 (pre-fix) had 1842/1842 tests pass but exited 1 with TWO uncaught
  `TypeError: Cannot set properties of null (setting 'helloSent')` at
  gateway-fake.ts:413, originating in `discord.test.ts`. Root cause: `connect()`
  defers accept via `queueMicrotask`; inside `serverAccept()` the synchronous
  `listener.onOpen()/onFrame(HELLO)` callbacks can trigger concurrent teardown
  (test cleanup → `serverClose` → `detach()` → `this.conn = null`) before the
  final `this.conn.helloSent = true` assignment. Fix: null guard — mark
  `helloSent` only when the connection survived the handshake. No behavioral
  change on any live path (conn is always non-null there when not torn down).
  Post-fix: both full runs exit 0 with zero unhandled errors.

## Remaining census platforms NOT yet ported (from `/usr/local/lib/hermes-agent/plugins/platforms/` + built-ins under `gateway/platforms/`)

Ported so far: telegram, slack, discord (plugins) · whatsapp_cloud (built-in).
Remaining:

- **Plugins:** matrix (long-poll sync v2), signal→built-in, feishu (ws+webhook,
  A12 cards), google_chat, teams (webhook), line (webhook), email, irc
  (persistent TCP), sms, whatsapp (plugin variant), mattermost, rocketchat-class
  simplex, dingtalk, wecom, ntfy, a2a, buzz, photon, raft, homeassistant.
- **Built-ins:** signal (+signal_format/signal_rate_limit — SSE stream family,
  A18 formatting/budget modules), weixin, yuanbao (+media/proto/sticker), qqbot,
  bluebubbles, msgraph_webhook.

**Recommended next order (roadmap §Phase 6 heuristics — transport shape first,
feature ride-alongs per platform):**

1. **matrix** — long-poll reuses the polling family fixture wholesale; heavy
   user demand; A9 channel-directory/alias ride-along.
2. **signal** — extends persistent-stream family (SSE); A18 formatting/budget
   modules are explicitly named heuristic ride-alongs.
3. **feishu** — webhook+ws hybrid; A12 card/VC/Drive-comment ingress ride-along.
4. **line / teams / google_chat / msgraph_webhook** — pure webhook family;
   cheapest ports, inherit wa-cloud/webhook fixtures near-wholesale.
5. Long tail: irc, email, sms, mattermost, dingtalk, wecom, simplex, ntfy,
   weixin, qqbot, bluebubbles, yuanbao, plugin-whatsapp reconciliation vs
   ported wa-cloud.

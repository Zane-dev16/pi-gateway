# PHASE 6 REPORT — Platform Census Ports Behind the Conformance Gate

**Verifier verdict: PASS.**

---

## CENSUS CLOSURE VERIFICATION (final — this section supersedes the wave-0 numbers below)

**Verdict: PASS — 2152/2152 ×2 runs, full census accounted (31 surfaces PORTED, 3 EXCLUDED-with-reason, dingtalk recorded OPEN → proposed DEC-043).**

| Gate | Result |
| --- | --- |
| `npx tsc --noEmit` | **exit 0** (441 files) |
| `node scripts/check-layering.mjs` | **OK** downward-only holds |
| `node scripts/check-secret-scope.mjs` | **OK** (441 files) |
| Tracked-diff rule | **PASS** — zero tracked files modified by census waves. (A 2-line formatter-whitespace diff in `src/pi_gateway/delegation/two-process.test.ts` found in the worktree at verification start was REVERTED to HEAD.) |
| `npx vitest run` — run 1 | **2152/2152 passed, 199/199 files, exit 0** |
| `npx vitest run` — run 2 (DEC-041) | **2152/2152 passed, 199/199 files, exit 0** (124.6s) |
| Baseline delta vs wave-0 closure | +310 tests / +32 files vs 1842/1842 @ 167 files — all additive |
| Silent-capability-lie scan | **PASS** — every suite computes streaming applicability from `supportsDraftStreaming()` probe reality; flipping the declaration re-includes the 3 streaming rows and fails seal-discipline BY NAME (verified present in all 24 new wiring files) |

### Final census table (Hermes ground truth: `Platform` enum @ gateway/config.py:317, plugins @ plugins/platforms/, built-ins @ gateway/platforms/)

**Built-in enum members (24):**

| Hermes member | Pi Gateway surface | Status |
| --- | --- | --- |
| telegram | `pi_platforms/telegram` (reference-family) | PORTED — gate green |
| discord | `pi_platforms/discord` | PORTED — gate green |
| slack | `pi_platforms/slack` | PORTED — gate green |
| whatsapp | `pi_platforms/whatsapp-personal` | PORTED — gate green |
| whatsapp_cloud | `pi_platforms/whatsapp-cloud` | PORTED — gate green (probe-excluded streaming ×3) |
| signal | `pi_platforms/signal` (+A18 formatting/budget) | PORTED — gate green |
| mattermost / matrix | `pi_platforms/{mattermost,matrix}` (polling shape) | PORTED — gates green |
| homeassistant / email / sms | `pi_platforms/{homeassistant,email,sms}` | PORTED — gates green |
| api_server | **absorbed**: webhook platform ships `/v1/chat/completions`, `/v1/runs`(+SSE/steer/stop), session headers (`webhook/completions.ts`, `webhook/runs.ts`; Phase-3 E2E loopback row) | PORTED via webhook surface |
| webhook | `pi_platforms/webhook` + `polling`/`persistent-ws` reference transports | PORTED (Phase 3) |
| msgraph_webhook | `pi_platforms/msgraph-webhook` | PORTED — gate green |
| feishu | `pi_platforms/feishu` (ws+webhook hybrid, A12 cards) | PORTED — gate green |
| wecom + wecom_callback | `pi_platforms/wecom` (callback-mode manifest anchors `plugins/platforms/wecom/callback_adapter.py`) — both enum members | PORTED — gate green |
| weixin | `pi_platforms/weixin` (long-poll, -14 ladder, breaker) | PORTED — gate green |
| bluebubbles | `pi_platforms/bluebubbles` | PORTED — gate green |
| qqbot | `pi_platforms/qqbot` (AES-GCM, keyboards, chunked upload, op-code gateway) | PORTED — gate green |
| yuanbao | `pi_platforms/yuanbao` (binary protobuf ConnMsg plane) | PORTED — gate green **after verifier fixes, see below** |
| local | CLI/TUI programmatic lane — host pi agent loop IS the surface (DEC-023); Hermes has NO adapter module either (`run.py:377`) | EXCLUDED — not an adapter surface |
| relay | `EXPERIMENTAL` "generic relay adapter fronted by the connector" — no adapter module exists in Hermes `gateway/platforms/` | EXCLUDED — nothing to port against; experimental |
| dingtalk | — none | **OPEN — not ported, proposed DEC-043** |

**Bundled plugin dirs (22):** 21/22 ported through the gate — a2a, buzz, discord*, email, feishu, google_chat, homeassistant, irc, line, matrix, mattermost, ntfy, photon, raft, simplex, slack*, sms, teams, telegram*, wecom, whatsapp (→whatsapp-personal).* = reference-family. **dingtalk is the single unported plugin** (see below).

Net: **31 adapter surfaces PORTED behind the gate · 2 EXCLUDED by nature (local, relay) · 1 OPEN (dingtalk)**.

### Verifier fixes applied during closure (all confined to `src/pi_platforms/yuanbao/**`, its wiring file, and ONE kit defect)

Yuanbao arrived PARTIAL from its wave (honestly reported: core port green, suite integration unfinished — 4 reproducibly RED tests). Closure required it green; diagnosis traced every failure to a named cause; fixes are minimal and Hermes-consistent:

1. **Fatal now stops everything** (`yuanbao-adapter.ts::handleClose`/`scheduleReconnect`): NO_RECONNECT close codes, resign-failure, and max-attempts fatals now clear `running` — previously the heartbeat loop kept ticking post-fatal and scheduled a reconnect BEHIND a fatal state (`yb.auth-handshake-close-matrix` caught it). ConnectionManager fatal parity.
2. **Reply-heartbeat/slow-response lifecycle moved INTO the guard messageHandler** (`attachStandardGuard`): turns run BACKGROUND (`l1-adapter-guard.startSessionProcessing` fire-and-forget spawn — correct Hermes parity), so wrapping `deliverInbound` never spans the turn; RUNNING/FINISH + slow-response notice were torn down before the turn processed. The dead `slowResponseTimeoutMs` knob is now live (was plumbed into the constructor but the timer read the raw constant — the original lens blocker on this file), and the auto-stop unit bug is fixed (`elapsed` ms compared against `REPLY_HEARTBEAT_TIMEOUT_S` seconds).
3. **JSON-push parity wired**: `handleFrame` called binary `decodeInboundPush` directly, never trying the existing JSON-first `decodeFramePayload`; PascalCase callback pushes and recall lists now decode (`decodeRecallList` added; recall id lands in the synthetic CRITICAL turn).
4. **Fake gateway offline replay queue** (`fake-yuanbao.ts`): pushes emitted while no session is live buffer and flush after AUTH_BIND — the family convention qqbot's fake already implements and the fixture contract documented; also added `pushJson` for RAW-JSON frame delivery.
5. **Kit defect — `kit/base-adapter.ts::deliverChunk`**: the session ladder's `sendConverted/sendPlain` closures captured the FIRST delivery's `chatId`, silently misrouting every later cross-chat tier-2/tier-3 send (yuanbao dual-path row caught it; other adapters masked it by using fresh worlds per leg). Fix: fresh ladder per chunk with `richSendDisabled`/`richLatchCount` carried in and out — identical observable latch semantics (§10.1 probe-once preserved), correct chat routing. Full suite ×2 green across ALL adapters confirms no sibling regression.
6. **Wiring honesty**: shared-row subject egress capture now RETURNS the recorded result instead of falling through to the disconnected binary WS face; latch worlds connect before asserting tier-2 delivery; replay waits pump the injected clock (guard debounce window is injected time); dual-path asserts wire chunks BYTE-EXACT against the computed `chunkWithFenceCarry` plan plus whitespace-normalized content reconstruction (label relocation consumes exactly the seam separator — vendor chunker property, documented in-row).

### Per-suite wiring counts (24 new conformance files, all ending in the full-gate assertion `failed===0 && deferred===0 && allApplicablePassed===true` + lying/mutant-fixture negative validation)

a2a 6 · bluebubbles 7 · buzz 5 · email 7 · feishu 8 · gchat 6 · homeassistant 8 · irc 7 · line 7 · matrix 6 · mattermost 5 · msgraph 6 · ntfy 7 · photon 5 · qqbot 8 · raft 5 · signal 7 · simplex 8 · sms 7 · teams 6 · wa-personal 5 · wecom 6 · weixin 8 · yuanbao 8 (=165 wiring tests; in-dir behavior suites bring the total to 2152).

### Proposed DECs collected at closure (none implemented silently)

- **DEC-043 (proposed): dingtalk census item** — Hermes plugin `plugins/platforms/dingtalk/` delegates its ENTIRE transport to the external `dingtalk-stream>=0.20` SDK (Stream Mode WS); pi-gateway has no port. Options: (a) port manifest+Stream-Mode protocol with probe-computed SDK-delegated exclusions per DEC-002 gate before merge, or (b) explicit scope exclusion. Recorded OPEN rather than silently dropped; every other surface is closed.
- ws-family Retry-After capture knob is adapter-level data over a shared kit extractor (qqbot REST leg + yuanbao close-reason "retry-after:N") — Hermes parses neither (cn-cluster).
- Weixin -14 streak escalation pause→recycle(gen-bump)→fatal minimally extends Hermes' plain 600s pause to satisfy the conflict-zombie-eviction row (cn-cluster).
- Timeout-classified send failures are NOT retried inside platform ladders (qqbot/weixin/yuanbao) — base.py §6.1 parity overriding Hermes qqbot's retry-everything loop; the shared row is the gate (cn-cluster).
- Dual-path markdown reconstruction convention: byte-exact against the kit chunker PLAN + whitespace-normalized content equality (label relocation consumes the seam separator) replaces naive string-concat reconstruction (this verification; qqbot passed the naive form only because its cut points landed on paragraph boundaries).

---

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

## Historical: remaining census AFTER wave-0 (superseded by the closure table above)

Ported in wave-0: telegram, slack, discord (plugins) · whatsapp_cloud (built-in). Everything else landed in waves 1–2; final accounting above.

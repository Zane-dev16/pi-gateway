# PHASE 3 REPORT — Three Reference Adapters + Executable Conformance Suite

**Verifier sweep:** independent re-run of every gate; nothing taken from agent reports on trust.
**Date:** Phase 3 close · **Repo:** `/root/pi-gateway/pi-gateway`

## 1. Gates (measured by verifier)

| Gate | Result |
| --- | --- |
| `npx tsc --noEmit` | **exit 0** |
| `node scripts/check-layering.mjs` | **exit 0** — `layering OK (downward-only holds across src/)`; `pi_platforms` validated as top layer; script itself **unmodified** (already knew rank-4) |
| `npx vitest run` (full repo) | **844/844 PASSED, 72 files, ~14 s**, 0 failures |
| Baseline reconciliation | Phase-2 exit recorded 60 files/678 tests — its run already included 4 then-untracked conformance-harness files. HEAD tracks **56** test files + **16 new** under `src/pi_platforms/` = 72. Net new this phase: **+166 tests** (678→844) |
| Footprint | exactly one untracked dir `src/pi_platforms/**` (62 files); **zero modified tracked files**; `package.json` untouched; `ws` dep **not installed** (not needed — pure in-memory/in-process fakes) |

## 2. Per-adapter summary

### Polling (Telegram-like) — ✅ COMPLETE

`src/pi_platforms/polling/` (6 modules): engine (`polling-adapter.ts`, offset-commit-before-enqueue, generations, heartbeats, FloodWait ladder), in-process `FakeTelegramServer` (getUpdates long-poll, 2 ms coalescing, offset confirmation/pruning, ONE-consumer sessions, 409s), injected `ManualPollingClock` (progressive virtual-time advance), subject, **real-engine transport fixture**, 18 engine tests + 4-row wiring suite. Shared rows (22) run against the polling subject; all four §3.1 transport rows run against `makeRealPollingFixture` — actual getUpdates cycles, generations, heartbeats, held-inbound drains, **zero deferred**. Negative validation included: a lying fixture fails its rows.

### Webhook (WhatsApp-Cloud/api_server-like) — ✅ COMPLETE

`src/pi_platforms/webhook/` (21 files): manifest trust boundary as DATA (DEC-017), constant-time HMAC signature validation, sliding-window rate limit + delivery-id idempotency, body caps rejected BEFORE parse, bounded-window sync answers, api_server-class SSE lane (approval/steer/stop), DEC-022 stateless wake rail. 85 tests: shared rows vs webhook subject; both transport rows over the REAL adapter (+ a violating-mutant detector); **E2E over real loopback sockets** (`127.0.0.1:0`) — signed ingress→guards→bounded-window answer, replay cache, oversize rejection pre-parse, SSE approval/steer/stop reachable end-to-end, wake rail self-posting the RAW-key direct turn through the real server.

### Persistent-WS (Slack/Discord-like) — ❌ INCOMPLETE (blocked thread)

`src/pi_platforms/persistent-ws/` (7 modules, 1,536 lines): `PersistentWsAdapter` (784 lines, extends kit base, compiles; seal-chat knob landed), reconnect ladder, capability latch (A23), event cursor/dedup, dual-path markdown, `FakeWsServer` in-memory socket factory, manual clock. **Zero test files. No `ConformanceSubject`. No conformance wiring.** No shared §8 row has ever executed against this adapter; the two `transport.ws.*` rows exist as hook contracts but are exercised only against stub fixtures inside the harness self-test. The ws agent's last transmission was mid-flight ("building the conformance subject") and never completed.

### Kit — ✅ COMPLETE

`src/pi_platforms/kit/` (14 modules): THE ONE length-policy pair (§6.3/A15), chunking with fence carry, send-retry ladder (FloodWait honored; timeout never retried), callback grammar + router (13 prefix families ≤64 bytes), Block Kit caps + action-handler registry, formatting ladder (probe-once latch), token lock, lifecycle state, trust (scoped reads fail closed), registration, capabilities, base adapter. 87 kit tests.

### Conformance core — ✅ COMPLETE

Executable matrix: **22 shared rows** (`rows.ts`) + per-shape requirements catalog + runner with honest deferred reporting (`allApplicablePassed` = zero failed AND zero deferred). Reference fake subject. **Mutant gate**: control (all green) + 8 single-property mutants each rejected by an EXACT named-row set with invariant-naming detail fragments + all-mutations gauntlet + fresh-subject sanity = 11 tests. Gate has teeth in both directions.

## 3. 04 §8 automation coverage (row → status)

Legend: ✅ automated & green · ❌ not run against that shape · ⚠️ partial/other-level. P=polling W=webhook S=ws.

| §8 checklist item | Encoded row(s) | P | W | S |
| --- | --- | --- | --- | --- |
| burst 10 while busy → one slot, no dup turns | `ingress.burst-single-slot` | ✅ | ✅ | ❌ |
| control bypass; unknown `/foo` doesn't | `ingress.control-bypass` | ✅ | ✅ | ❌ |
| clarify intercept inline resolve | `ingress.clarify-intercept` | ✅ | ✅ | ❌ |
| self/echo filtered | `ingress.self-echo-filtered` | ✅ | ✅ | ❌ |
| **…sensitive ids redacted in logs** | — **NOT AUTOMATED anywhere** | ⚠️ | ⚠️ | ⚠️ |
| single chokepoint, both doors audited | `egress.single-chokepoint` | ✅ | ✅ | ❌ |
| chunk fence-carry `(i/n)`; FloodWait; timeout not retried | `egress.chunk-flood` + `egress.timeout-not-retried` | ✅ | ✅ | ❌ |
| plain-text fallback on rejection | `egress.plain-text-fallback` (+ `formatting.parse-failure-resend`) | ✅ | ✅ | ❌ |
| per-chat length pair, UTF-16 code units | `egress.per-chat-length-pair` | ✅ | ✅ | ❌ |
| prefix-stability mutation | `streaming.prefix-mutation-detected` | ✅ | ✅ | ❌ |
| seal discipline, final never duplicated | `streaming.seal-discipline` | ✅ | ✅ | ❌ |
| BOTH doors; failed seal still delivers | `streaming.failed-seal-still-delivers` | ✅ | ✅ | ❌ |
| segment/tool-boundary behavior | consumer level (Phase-2 `gateway-stream-consumer.test.ts`) | ⚠️ | ⚠️ | ⚠️ |
| builder→handler→resolver, EVERY family; ONE handler | `interactive.roundtrip-every-family` (13 families) | ✅ | ✅ | ❌ |
| ids ≤ strictest cap (64 B) | folded into roundtrip row | ✅ | ✅ | ❌ |
| stale/expired taps answered, never turns | `interactive.stale-expiry-answered` | ✅ | ✅ | ❌ |
| unauthorized ignored; consumed stripped; double-tap once | `interactive.unauthorized-and-consumed` | ✅ | ✅ | ❌ |
| Block Kit caps; mrkdwn fallback; ack-on-raise | `interactive.block-kit-caps` | ✅ | ✅ | ❌ |
| rich ladder latches ONCE; transient ≠ legacy-resend | `formatting.downgrade-latch` | ✅ | ✅ | ❌ |
| parse-failure plain resend | `formatting.parse-failure-resend` | ✅ | ✅ | ❌ |
| dual-path: native RAW / REST converts / link-preview split | — **no row**; impl exists untested in `persistent-ws/dual-path-markdown.ts` | n/a | n/a | ❌ |
| polling outage/reconnect preserves queue | `transport.polling.outage-reconnect-preserves-queue` | ✅ real fixture | — | — |
| held-inbound redispatch covers ack window | `transport.polling.held-inbound-redispatch` | ✅ | — | — |
| 409 zombie eviction, fresh generation | `transport.polling.conflict-zombie-eviction` | ✅ | — | — |
| heartbeat stuck-probe escalation | `transport.polling.heartbeat-escalation` | ✅ | — | — |
| ws resubscribe replay | hook defined; stub-tested only | — | — | ❌ real adapter |
| ws watchdog recovery | hook defined; stub-tested only | — | — | ❌ real adapter |
| webhook flags False×2 + DEC-017 boundary | `transport.webhook.flags-and-trust-boundary` | — | ✅ real adapter | — |
| webhook bounded-window answer | `transport.webhook.bounded-window-answer` | — | ✅ | — |
| api_server holds window; steer/stop/SSE reachable | E2E loopback row (beyond named rows) | — | ✅ | — |
| token-lock refusal names holder, FATAL | `identity.token-lock-refusal` | ✅ | ✅ | ❌ |
| missing secret ⇒ loud disable | `identity.missing-secret-loud-disable` | ✅ | ✅ | ❌ |
| scoped authz fail-closed (no env borrow) | `identity.scoped-authz-fail-closed` | ✅ | ✅ | ❌ |

## 4. Exit criteria

| # | Criterion | Verdict |
| --- | --- | --- |
| a | EACH adapter passes ALL applicable §8 rows | **PARTIAL** — polling ✅ (26/26, zero deferred) · webhook ✅ (24/24 + 3 E2E rows) · **ws ❌ (0 rows executed; 2 required rows deferred)** |
| b | Headless vs in-process fake servers, no network | **PASS** — `FakeTelegramServer` in-proc; webhook on `127.0.0.1:0`; `FakeWsServer` in-memory; grep shows zero external dials |
| c | Mutant adapter REJECTED, named-row failures | **PASS** — control green; 8 mutants each rejected by exact row set naming the invariant; gauntlet rejects all simultaneously; lying polling fixture + violating webhook fixture also detected |
| d | Wake lanes complete (DEC-022 both lanes closed) | **PASS on webhook shape** — raw-key-direct declaration from `supports_async_delivery=False`; self-post Bearer + RAW `X-Hermes-Session-Id` through own `/v1/chat/completions`; 429 ladder 2/5/10 s then success on 4th (**ManualTimers**, no wall sleeps); exhaustion raises loudly; missing key / empty raw-key refuse pre-post; forged-event lane traverses BOTH guards incl. busy ladder. Push lane closed in Phase 2 |
| e | Token-lock refusal + loud-missing-secret per adapter | **PARTIAL** — green on polling + webhook subjects (+ reference fake); **never executed for ws (no subject)** |
| f | Layering: downward-only, no cross-adapter imports | **PASS** — script exit 0 + full manual import audit of all 62 files: only `pi_gateway/*` (downward), `kit/*` + `conformance/*` (same layer), node builtins; zero cross-adapter imports; adapters never import runner internals or upward |

## 5. Counts

| Metric | Value |
| --- | --- |
| Full repo | 844/844 tests, 72 files (baseline 678/56-tracked+4-scaffold) |
| `src/pi_platforms/` | **212 tests / 16 files** — kit 87 · webhook 85 · polling 18+wiring 4 · conformance-core 7+11 · ws **0** |
| Net new this phase | +166 tests |
| Behavior contracts | 22 shared rows × shapes executed (polling, webhook, reference fake) + 8 transport rows wired + E2E pipeline/SSE/wake rows |
| Race-shaped contracts | burst/single-slot under `ManualScheduler`; busy-wake ladder; long-poll coalescing; ONE-consumer session contention (fake server); held-inbound redispatch window |
| Mutation/negative-validation tests | 15 — mutant suite 11 (control + 8 mutants + gauntlet + freshness), lying polling fixture 1, violating webhook fixture 1, ws/webhook stub-violation 1, gate-specificity assertions throughout |
| Wall-clock discipline | injected clocks everywhere (`ManualPollingClock`, `ManualTimers`, `ManualClock`, fixed `nowMs` seams); no ≥2 s wall bounds anywhere in phase code |

## 6. Blocked threads

1. **Persistent-WS completion (the only blocker).** Adapter + support modules compile and are architecturally aligned (seal knob present), but the agent was cut off before delivering: `makeWsSubject` (ConformanceSubject over `PersistentWsAdapter` + `FakeWsServer`), the wiring test mirroring `polling-rows.test.ts` (shared rows + `makeWsRows` over a real resubscribe/watchdog fixture using `ManualClock`), and any unit tests for `reconnect-ladder` / `capability-latch` / `event-cursor` / `dual-path-markdown` / `manual-clock` (all at 0% test coverage). Until then `runConformanceSuite({shape:"ws"}).allApplicablePassed` is false by construction (deferred rows), so the merge gate correctly refuses ws registration today.
2. **§8 log-redaction clause unowned.** No row, no kit helper, no test anywhere asserts sensitive identifiers are redacted from logs.
3. **Dual-path §10.2 row unencoded** (blocked on ws subject; conversion helpers exist but untested).

## 7. Proposed DECs (smallest Hermes-consistent behavior; for ratification, not yet binding)

- **DEC-032 (proposed) — ws merge-gate completion:** persistent-ws may register as a passing reference adapter only when a wired subject executes all 22 shared rows + both `transport.ws.*` rows against `FakeWsServer` with injected clock; until that lands, the ws shape remains explicitly deferred by the runner (honest-deferral, not silent skip).
- **DEC-033 (proposed) — log redaction ownership:** the §8 redaction clause is a guard/logger-level property, not per-adapter code: encode ONE shared row with an injected log sink asserting session keys, tokens, and secrets never appear in emitted lines; adapters inherit it via the kit base logger.
- **DEC-034 (proposed) — dual-path row encoding:** once the ws subject exists, assert (i) native-stream path ships RAW prefix-stable markdown, (ii) REST path routes through `convertMarkdownToMrkdwn`, (iii) link-preview suppression is a text-send-only metadata flag, absent on media sends.

## 8. Verdict

**FAIL (partial)** — 2 of 3 reference adapters fully satisfy every applicable §8 row with genuine behavior contracts, negative validation, and clean layering/footprint; the persistent-WS adapter compiles but has **zero conformance execution**, so the phase goal ("prove the seams hold for all three transport shapes", DEC-002) is not yet met. Everything else — gates, headlessness, mutant teeth, webhook wake-lane close-out, layering, footprint — is green. Unblocking requires exactly the ws completion thread above; no rework of polling, webhook, kit, or the conformance core is needed.

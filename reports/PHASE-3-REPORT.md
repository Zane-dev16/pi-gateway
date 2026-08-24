# PHASE 3 REPORT — Three Reference Adapters + Executable Conformance Suite

**Final verifier sweep:** independent re-run of every gate after the persistent-ws completion thread; nothing taken from agent reports on trust.
**Status:** Phase 3 close · **Repo:** `/root/pi-gateway/pi-gateway` · **VERDICT: PASS**

## 1. Gates (measured by verifier, final)

| Gate | Result |
| --- | --- |
| `npx tsc --noEmit` | **exit 0** |
| `node scripts/check-layering.mjs` | **exit 0** — `layering OK (downward-only holds across src/)` |
| `npx vitest run` (full repo) | **873/873 PASSED, 74 files, ~15 s**, 0 failures |
| Reconciliation vs interim baseline | 844/844 (72 files) before the ws thread → **+29 tests / +2 test files** (`persistent-ws/persistent-ws.test.ts`, `conformance/ws-rows.test.ts`) |
| Footprint (final delta) | modified: `conformance/{conformance.test,rows,shapes}.ts`, `kit/{base-adapter,index}.ts`; new: `persistent-ws/**` (subject, fixture, tests), `kit/log-redaction.ts`. **All inside `src/pi_platforms/{persistent-ws,conformance,kit}/`; zero changes elsewhere** |

## 2. Per-adapter summary (final)

### Polling (Telegram-like) — ✅ COMPLETE

Unchanged from interim report: real-engine transport fixture, 22 shared rows + 4 §3.1 transport rows, zero deferred, lying-fixture negative validation.

### Webhook (WhatsApp-Cloud/api_server-like) — ✅ COMPLETE

Unchanged: trust boundary as DATA (DEC-017), HMAC, rate limit + idempotency, bounded windows, SSE lane, DEC-022 wake rail closed over real loopback sockets; both transport rows over the REAL adapter + violating-mutant detector.

### Persistent-WS (Slack/Discord-like) — ✅ COMPLETE (closed this thread)

`src/pi_platforms/persistent-ws/` (11 modules): `PersistentWsAdapter` (real engine, extends kit base, registers resolved secrets into the log redactor), reconnect ladder, capability latch (A23), event cursor/dedup (#4777), dual-path markdown converters, `FakeWsServer` in-memory replay window, `ManualClock`.

Completion deliverables verified by execution:

- **`ws-subject.ts`** — `WsSubject` wires the REAL `PersistentWsAdapter` over the shared harness wire through a subject-level `RestPlane` wrapper (models `forceFormattingError` markdown-rejection exactly like the reference fixture), `FakeWsServer` as transport factory, `ManualClock`, scoped secret reader (`WS_BOT_TOKEN`; miss ⇒ loud-disable), standard guard + harness-stamped `ManualScheduler` under `SCHEDULER_SYMBOL`. Surface mirrors polling/webhook subjects 1:1. No stubs — the subject holds the actual adapter (`subject.adapter: PersistentWsAdapter`).
- **`ws-fixture.ts`** — all five transport scenarios against LIVE worlds (fresh world per row, injected clock, no wall-time waits beyond a tiny poll budget): outage mid-life → messages pushed during disconnect → ladder sleep → resubscribe; wedged-pong socket reaped under injected clock; close-payload Retry-After 7 s honored verbatim as authoritative delay + REST-side capture; feature-gate latch vs transient-failure control world; dual-path markdown legs (i)-(iii).
- **`persistent-ws.test.ts`** (25 tests) — ManualClock discipline, ladder escalation + Retry-After authority, latch-once semantics, dedup/cursor TTL+LRU+monotonicity, byte-exact fenced-block conversion, FakeWsServer cursor-exact replay, lifecycle: null-cursor cold boot / resume-from-cursor / **exactly-once across overlapping windows** / contained dispatch errors / self-echo filter pre-dispatch / watchdog reap within `pingInterval·factor + one tick` / close-payload Retry-After verbatim; **E2E row**: fake ws inbound → guards → scripted-model runner → native RAW `*Stream` egress.
- **`ws-rows.test.ts`** (4 tests) — 23/23 shared rows vs ws subject; 5/5 transport rows vs real fixture; full-catalog gate `allApplicablePassed === true`, `deferred === []`; **negative validation**: a lying fixture fails each dimension's OWN named row.

### Kit — ✅ COMPLETE (+DEC-033)

14→15 modules: `log-redaction.ts` adds `SecretRedactor` + `createRedactingLogger`; `BasePlatformAdapter` wraps EVERY injected logger so all adapters inherit §8 log hygiene, with `registerLogSecret` for post-enablement values and credential-shape scrubbing even unregistered.

### Conformance core — ✅ COMPLETE

23 shared rows (the prior 22 + `logs.sensitive-redacted`, DEC-033) × shapes; ws requirement catalog grown to five named transport rows; runner unchanged (honest deferral: rows are EXECUTED, never inspected; `allApplicablePassed` ⇔ zero failed AND zero deferred). Mutant gate unchanged-green.

## 3. 04 §8 automation coverage (row → status, FINAL)

Legend: ✅ automated & green · ⚠️ partial/other-level. P=polling W=webhook S=ws.

| §8 checklist item | Encoded row(s) | P | W | S |
| --- | --- | --- | --- | --- |
| burst 10 while busy → one slot, no dup turns | `ingress.burst-single-slot` | ✅ | ✅ | ✅ |
| control bypass; unknown `/foo` doesn't | `ingress.control-bypass` | ✅ | ✅ | ✅ |
| clarify intercept inline resolve | `ingress.clarify-intercept` | ✅ | ✅ | ✅ |
| self/echo filtered | `ingress.self-echo-filtered` | ✅ | ✅ | ✅ |
| **sensitive ids redacted in logs (DEC-033)** | `logs.sensitive-redacted` | ✅ | ✅ | ✅ |
| single chokepoint, both doors audited | `egress.single-chokepoint` | ✅ | ✅ | ✅ |
| chunk fence-carry `(i/n)`; FloodWait; timeout not retried | `egress.chunk-flood` + `egress.timeout-not-retried` | ✅ | ✅ | ✅ |
| plain-text fallback on rejection | `egress.plain-text-fallback` (+ `formatting.parse-failure-resend`) | ✅ | ✅ | ✅ |
| per-chat length pair, UTF-16 code units | `egress.per-chat-length-pair` | ✅ | ✅ | ✅ |
| prefix-stability mutation | `streaming.prefix-mutation-detected` | ✅ | ✅ | ✅ |
| seal discipline, final never duplicated | `streaming.seal-discipline` | ✅ | ✅ | ✅ |
| BOTH doors; failed seal still delivers | `streaming.failed-seal-still-delivers` | ✅ | ✅ | ✅ |
| segment/tool-boundary behavior | consumer level (Phase-2 `gateway-stream-consumer.test.ts`) | ⚠️ | ⚠️ | ⚠️ |
| builder→handler→resolver, EVERY family; ONE handler | `interactive.roundtrip-every-family` (13 families) | ✅ | ✅ | ✅ |
| ids ≤ strictest cap (64 B) | folded into roundtrip row | ✅ | ✅ | ✅ |
| stale/expired taps answered, never turns | `interactive.stale-expiry-answered` | ✅ | ✅ | ✅ |
| unauthorized ignored; consumed stripped; double-tap once | `interactive.unauthorized-and-consumed` | ✅ | ✅ | ✅ |
| Block Kit caps; mrkdwn fallback; ack-on-raise | `interactive.block-kit-caps` | ✅ | ✅ | ✅ |
| rich ladder latches ONCE; transient ≠ legacy-resend | `formatting.downgrade-latch` | ✅ | ✅ | ✅ |
| parse-failure plain resend | `formatting.parse-failure-resend` | ✅ | ✅ | ✅ |
| **dual-path: native RAW / REST converts / link-preview split (DEC-034)** | `transport.ws.dual-path-markdown` | n/a | n/a | ✅ |
| polling outage/reconnect preserves queue | `transport.polling.outage-reconnect-preserves-queue` | ✅ real fixture | — | — |
| held-inbound redispatch covers ack window | `transport.polling.held-inbound-redispatch` | ✅ | — | — |
| 409 zombie eviction, fresh generation | `transport.polling.conflict-zombie-eviction` | ✅ | — | — |
| heartbeat stuck-probe escalation | `transport.polling.heartbeat-escalation` | ✅ | — | — |
| ws resubscribe replay (cursor-exact, exactly-once incl. overlap dup suppression) | `transport.ws.resubscribe-replay` | — | — | ✅ real adapter |
| ws heartbeat watchdog recovery (reap + no loss) | `transport.ws.heartbeat-watchdog-recovery` | — | — | ✅ real adapter |
| ws Retry-After from close AND REST; capture IS next delay (authoritative) | `transport.ws.retry-after-capture` | — | — | ✅ real adapter |
| ws capability latch permanent; wire skipped post-latch; transient never latches | `transport.ws.capability-latch-permanent` | — | — | ✅ real adapter |
| webhook flags False×2 + DEC-017 boundary | `transport.webhook.flags-and-trust-boundary` | — | ✅ real adapter | — |
| webhook bounded-window answer | `transport.webhook.bounded-window-answer` | — | ✅ | — |
| api_server holds window; steer/stop/SSE reachable | E2E loopback row (beyond named rows) | — | ✅ | — |
| ws E2E: inbound → guards → runner → native RAW egress | E2E describe (`persistent-ws.test.ts`) | — | — | ✅ |
| token-lock refusal names holder, FATAL | `identity.token-lock-refusal` | ✅ | ✅ | ✅ |
| missing secret ⇒ loud disable | `identity.missing-secret-loud-disable` | ✅ | ✅ | ✅ |
| scoped authz fail-closed (no env borrow) | `identity.scoped-authz-fail-closed` | ✅ | ✅ | ✅ |

## 4. Exit criteria (FINAL)

| # | Criterion | Verdict |
| --- | --- | --- |
| a | EACH adapter passes ALL applicable §8 rows | **PASS** — polling 26/26 zero deferred · webhook 24/24 + E2E rows · **ws 23/23 shared + 5/5 transport, `runConformanceSuite({shape:"ws"}).allApplicablePassed === true`, `deferred === []`** |
| b | Headless vs in-process fake servers, no network | **PASS** — `FakeTelegramServer` in-proc; webhook on `127.0.0.1:0`; `FakeWsServer` in-memory; zero external dials |
| c | Mutant adapter REJECTED, named-row failures | **PASS** — control green; 8 mutants each rejected by exact named-row set; gauntlet rejects all; lying ws fixture + lying polling fixture + violating webhook fixture each detected by their own rows |
| d | Wake lanes complete (DEC-022 both lanes closed) | **PASS** — webhook raw-key-direct self-post through own `/v1/chat/completions` (429 ladder under ManualTimers), forged-event lane traverses both guards; push lane closed Phase 2; ws declares its lane via `wake.lane-declaration-consistent` |
| e | Token-lock refusal + loud-missing-secret per adapter | **PASS** — green on polling + webhook + **ws subjects** (real `TokenLockManagerSeam` second-acquisition refusal naming holder; real sibling lifecycle loud-disable surfacing secret in status detail) |
| f | Layering: downward-only, no cross-adapter imports | **PASS** — script exit 0; adapters import only `pi_gateway/*` (downward), `kit/*` + `conformance/*` (same layer); zero cross-adapter imports |

## 5. Counts (final)

| Metric | Value |
| --- | --- |
| Full repo | **873/873 tests, 74 files** (interim baseline 844/72 → +29/+2 this thread) |
| `src/pi_platforms/` | **241 tests / 17 files** — kit 87 · webhook 85 · conformance-core+wiring 26 (incl. 4 ws wiring) · polling 18 · **persistent-ws 25** |
| Behavior contracts added this thread | 5 transport rows over live worlds + 23rd shared row (redaction) ×3 shapes + 25 engine unit contracts + lying-fixture gate validation |
| Mutation/negative-validation tests | 18 total — mutant suite 11, lying polling 1, violating webhook 1, stub-violation 1, **lying ws fixture 1**, gate-specificity assertions throughout |
| Wall-clock discipline | injected clocks everywhere (`ManualPollingClock`, `ManualTimers`, `ManualClock`, fixed `nowMs`); fixture `eventually()` is a condition-poll, never a timing assert |

## 6. Blocked threads

None. All three interim blockers closed:

1. ~~Persistent-WS completion~~ → subject + fixture + wiring + 25 engine tests delivered; merge gate passes for ws by wired execution.
2. ~~§8 log-redaction clause unowned~~ → DEC-033: kit-level `SecretRedactor` inherited by every adapter via base logger; ONE shared row (`logs.sensitive-redacted`) executed against all three adapters.
3. ~~Dual-path §10.2 row unencoded~~ → DEC-034 encoded as `transport.ws.dual-path-markdown`: (i) native stream ships RAW prefix-stable bytes (start frame = full accumulator, appends = RAW suffix deltas, fragments reconstruct exact cumulative content, zero `<url|label>` conversion), (ii) REST path converts bold/link/table, (iii) link-preview suppression present on ALL text sends, absent on draft/seal/rich ops.

## 7. DECs

**DEC-032 / DEC-033 / DEC-034 — conditions now SATISFIED BY EXECUTION; ready for ratification** (they were proposed in the interim report; the ws thread met them):

- **DEC-032**: ws registers as passing ONLY via wired execution — the runner executes every row (`await row.run()`), deferral lists unsupplied required IDs, and `allApplicablePassed` requires zero failures AND zero deferred; the lying-fixture test proves rows are detectors, not rubber stamps. No skip/todo/deferral markers exist anywhere under `persistent-ws/`.
- **DEC-033**: redaction is a guard/logger-level property owned by the kit base; verified end-to-end (registered token, session key, unregistered `sk-proj-*`/`ghp_*` shapes, nested/array meta, mid-message embedding — `[redacted]` span replacement preserves benign text).
- **DEC-034**: triple assertion encoded and green for ws (see §6.3).

**New DECs proposed by the verifier: none.** The sweep found zero behavioral divergences requiring a DEC-027+ entry.

## 8. Verdict

**PASS** — all three reference adapters satisfy every applicable §8 row with genuine behavior contracts against real engines; the DEC-032 merge gate passes for all three shapes; negative validation has teeth in both directions; layering, headlessness, wall-clock discipline, and footprint are clean. Suite: **873/873, tsc clean, layering clean**. Phase 3 may close.

*Hygiene note (non-blocking, for the record):* `ws-fixture.ts::resubscribeReplay` retains a dead `const resume …; void resume;` leftover — cursor-exactness itself is proven where it belongs (FakeWsServer "replays strictly AFTER the resume cursor" + engine "reconnect resumes FROM the cursor"), while the conformance row asserts gap-free exactly-once coverage. Cosmetic only; safe to drop in any later touch of the file.

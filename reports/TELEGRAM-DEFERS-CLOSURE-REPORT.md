# TELEGRAM DEFERS CLOSURE REPORT — tg-11 / tg-12 / tg-13

**Task label:** tg defers (closure wave)
**Date:** 2026-08-27
**Scope of edit authority:** `src/pi_platforms/telegram/**`, telegram fake server, telegram tests,
telegram conformance wiring (`conformance/telegram-shape-rows.ts`, `conformance/telegram-rows.test.ts`,
shared-row premises in `conformance/rows.ts`). Kit seams touched where Hermes truth required them:
`kit/callback-router.ts` (tap context), `kit/callback-router.test.ts` untouched, `polling/polling-adapter.ts`
(authorizer hook). Upstream anchors: `/tmp/hermes-upstream/plugins/platforms/telegram/adapter.py`.

---

## Verdict

**All three round-1 telegram defers are CLOSED as FIXES toward Hermes truth. No new DEC entries
required** — each closure is an unjustified-divergence repair (make pi conform), not a load-bearing
pi-sanctioned divergence. The one prior partial-divergence candidate surfaced during closure
(draft-trim semantics) was re-derivable from upstream byte-for-byte and therefore also fixed rather
than deferred.

---

## tg-11 — callback authorizer (was: "defaults allow-all until kit-wide fail-closed seam lands")

### Upstream ground truth (re-derived)

`adapter.py:_is_callback_user_authorized :1171`:

1. Empty clicker id ⇒ **DENY unconditionally** (:1177 — `#24457` fail-closed fix).
2. Runner chain first: builds a `SessionSource(platform=TELEGRAM, chat_id=chat_id ?? user_id,
   chat_type=normalized, user_id, user_name=first_name, thread_id)` with `private→dm`,
   `supergroup→forum when thread_id is not None else group` mapping (:1192-1206) and calls the
   runner's `_is_user_authorized` (`gateway/authz_mixin.py:383`) — the SAME chain that governs
   message ingress on this platform (allowlists incl. `"*"` wildcard, `TELEGRAM_GROUP_ALLOWED_*`,
   `TELEGRAM_ALLOW_ALL_USERS`, pairing grants, `GATEWAY_ALLOW_ALL_USERS`, default deny).
3. Env-only fallback (:1216 `_scoped_gate_env`) kept by upstream as an exception-degraded path;
   its branches are gates 9/5/8 of that same chain.

Every gated callback site in Hermes routes through this predicate BEFORE resolution:
exec approvals `ea:` (:7229), slash-confirm `sc:` (:7309), clarify `cl:` (:7409),
update-prompt (:7522), choice-picker `cp:` (:6664, "unauthorized users … must not flip
session/config state via someone else's picker message"). Unauthorized taps are ANSWERED ⛔ and
NEVER resolved.

### pi implementation

- `TelegramAdapter.authorizeCallbackClicker(tap)` overrides the new
  `PollingAdapterCore.authorizeCallbackClicker(tap, family)` hook (whose default remains the
  shared fixture switch `allowAllClickers`, so every other polling engine's observable behavior is
  unchanged). Telegram wires the FULL session-authz decision chain
  (`security/authz isUserAuthorized`, platform="telegram") with the exact SessionSource mapping
  above. The router already gates every non-nav family before any store pop, so unauthorized taps
  answer ⛔ and never resolve — §9.1 audit order confirmed against upstream sites.
- `forcedClickAuthorization` (`setClickerAuthorization`) stays the SHARED conformance control and
  FORCES the verdict only when armed; production never arms it, so the real chain runs.
- Shared-row premises updated: `conformance/rows.ts` callback-catalog and unknown/stale rows now
  arm `setClickerAuthorization(true)` first (an operator-configured gateway is what those rows
  model); shape row `tg.callback-roundtrip-64b` gained
  `defaultClosedUnauthorizedNotResolved` — a fresh, env-less subject DENIES through the real chain.
- New unit contracts in `telegram.test.ts`: fail-closed default; `TELEGRAM_ALLOWED_USERS` /
  wildcard / `GATEWAY_ALLOW_ALL_USERS` opt-ins authorize like ingress; empty id fails closed even
  under `"*"`; supergroup+thread maps to forum scope so gate 2 (group-chat allowlist) matches
  upstream SessionSource semantics; forced override wins over the chain.
- Harness detail: the tap context gained optional `chatType/threadId/userName`
  (`kit/callback-router.ts`), optional-only so every existing tap builder keeps compiling; the fake
  pusher models `from.first_name`/host `chat.type`/`message_thread_id`.

### Blast radius

Polling engines other than Telegram keep the default hook (no behavior delta — verified by the
full shared/transport conformance catalog staying green). Telegram production postures change from
allow-all-by-default to deny-by-default pending operator config — exactly upstream parity (#24457).

## tg-12 — deleteMessage (was: "naming await consumer-lane/capture work")

### Upstream ground truth

`adapter.py:delete_message :6064` (openclaw#72038 port): Bot API `deleteMessage`; used by the
stream consumer's fresh-final cleanup (silence-marker suppression `_suppress_silence_marker`,
stale-preview abandonment); best-effort BY CONTRACT — failures return false / debug-log, caller
leaves the preview in place; `normalize_telegram_chat_id(chat_id)` + message id passthrough.

### pi implementation

- `TelegramAdapter.deleteMessage(chatId, messageId): boolean` over `bot.deleteMessage`;
  failures are `SendResult(success=false)`-driven AND exception-driven (the pi fake models wire
  failures as non-success results; the Slack `deleteMessage` lane established the same
  `success === true` idiom). Never throws.
- E2E conformance: NEW shape row **`tg.stream-delete-retraction`** drives two
  `GatewayStreamConsumer` runs over the REAL subject stream adapter — (1) intentional-silence final
  (`NO_REPLY`) retracts the streamed preview, nothing further ships; (2) a run gone stale
  (`runStillCurrent` false) abandons its edit-path preview. Both land as REAL `{chat_id, message_id}`
  captures on the fake's `deleteOps` (scriptable fail/flood classes retained).
- Unit contracts: capture shape, scripted failure → false without throw, verbatim id passthrough.

### Blast radius

Consumer-lane contracts in `pi_gateway/streaming` were already genuine (fake-adapters expose
`deleteMessage`; suppression tests assert the op stream). This wave adds the real-platform leg;
no streaming-module source changes made.

## tg-13 — sendMessageDraft legacy draft lane (was: "naming await consumer-lane/capture work")

### Upstream ground truth

`adapter.py:send_draft :6116`: draft frames are REAL Bot API calls
(`{chat_id(normalized int-or-string), draft_id:int, text}` + optional `parse_mode=MARKDOWN_V2`),
with `_thread_kwargs_for_draft :1609` (= `_thread_kwargs_for_send` with reply anchor None — thread
kwargs only, never a reply anchor):

1. UTF-16-first trim: `text = content if len(content) <= MAX_MESSAGE_LENGTH else
   truncate_message(content, MAX_MESSAGE_LENGTH, len_fn=utf16_len)[0]` (:6174-6177) — note the
   upstream quirk: the FIT CHECK is codepoint-based `len()`, the SPLIT uses utf16 units; `[0]`
   = FIRST CHUNK of base.py:truncate_message (:7365) — indicator-reserved body budget, fence-carry,
   natural split points, and the synthesized `" (1/N)"` label KEPT (previews are ephemeral, never split).
2. `plain_rich_preview = rich_messages_enabled && !rich_drafts_enabled &&
   needs_rich_rendering(text)` (:6185-6190) — computed on the TRIMMED text: drafts stay RAW because
   the legacy formatter would rewrite pipe tables inside an ephemeral preview.
3. MarkdownV2-FIRST with ONE plain-text retry on BadRequest-class failure (`draft_modes =
   plain_rich_preview ? (False,) : (True, False)`; `_is_bad_request_error :1699` gates the retry);
   non-BadRequest failures surface immediately.
4. Success carries NO message id (`SendResult(success=True, message_id=None)` :6207).

### pi implementation

- `sendLegacyNativeDraft` replaces the nameless `wireDraft` fallback: REAL `sendMessageDraft`
  calls on the fake (`sendMessageDraft` capture in `draftKwargs`, scriptDraft behaviors,
  success-without-id), MarkdownV2-first ladder keyed off BadRequest-class errors, thread kwargs
  from `threadKwargsForSend(..., null)`.
- Trim conforms EXACTLY: `firstDraftChunk` passthrough under the codepoint fit-check quirk,
  otherwise `chunkWithFenceCarry(content, this.chatLengthPolicyForChat(chatId)).chunks[0]` — the
  ONE shared chunker over the §6.3 per-chat pair (utf16 × `TELEGRAM_MAX_MESSAGE_UNITS`), preserving
  INDICATOR_RESERVE, fence scaffolding and the label; no hand-rolled trim (gap-audit A15 /
  06 §6.3 obligation preserved by construction).
- Shape row `tg.edit-send-reconciliation` now asserts `draftLaneSendMessageDraftMarkdownV2` (real
  sendMessageDraft kwargs with send-path conversion) replacing the old raw-prefix-stable claim;
  `tg.rich-extras-lane` draft-fallback segments read `tg.draftKwargs` instead of nameless harness ops.
- New unit contracts: MDV2-first attempt + escaped chunk-label bytes (`"…a \(1/2\)"` — the label is
  converted like regular chunked sends), one plain retry, transient failure surfaces once,
  oversize ⇒ first-chunk bytes (`"a".repeat(4086) + " \(1/2\)"` under the established 4096×utf16 policy)
  plus the astral passthrough quirk (2048 emoji pass WHOLE under the codepoint fit check).

### Blast radius

The seal/final lane is UNTOUCHED (DEC-034 chokepoint — Hermes has no Bot API to promote a draft);
mid-stream/finalize edit reconciliation unchanged.

---

## Adjacent conformity repairs completed while closing (lens-gated)

- `statusOfflineText` unused-field block resolved by porting the missing upstream half-step:
  `TelegramAdapter.disconnect()` stamps the OFFLINE short description while the HTTP client is
  alive, opt-in + best-effort/non-fatal (adapter.py :5172-5184 clean-shutdown parity, online stamp
  was already at :4953/:4134). Housekeeping row extended (`offlineStampOnDisconnect`, dedicated
  stamp-world Online→Offline ordering; default-off world still fires NOTHING on either edge).
- Dead import cleanup (`threadIdForSend` was imported-but-unused even pre-wave).

## Test-harness corrections (behavior-contract repairs, not weakenings)

- `withAuthzEnv` (telegram.test.ts) made async-aware: the sync try/finally restored
  `process.env` before awaited bodies finished, so any authz read AFTER the body's first suspension
  saw emptied vars (observed live: the forum-scope tap evaluated against blank env). The restore now
  brackets the ENTIRE awaited body — this is a fidelity fix to the tested reality, not a loosening.

## Verification

| Gate | Result |
| --- | --- |
| `npx tsc --noEmit` | **0 errors in every file owned by this wave** (grep-verified). Tree-wide residual: exactly 3 errors confined to sibling files `signal/signal-engine.test.ts` (:987 protected access) and `slack/slack-subject.ts` (:307 onTurnFailure narrowing vs `persistent-ws-adapter.attachGuard`) — left untouched per area rules; ownership attributed via git status + diff timestamps (01:04–01:16Z edits belong to other closure lanes) |
| Full-tree `npx vitest run` (after wave quiesce, scratch specs removed) | **3096/3096 across 237 files, exit clean** (11:49) — includes the token-lock heavy-process set; no SIGKILL flake observed this session |
| Area sweep | telegram + polling + kit 153/153; conformance incl. shared rows 204/204; all new tg contracts included in the full-tree figure |
| Conformance integrity | NO row enshrines an invented wire shape: every edited row asserts upstream-anchor-backed bytes (`\(1/2\)` escaped labels, `deleteOps`, `draftKwargs`, forum-scope authz mapping) |
| Scratch hygiene | ALL `.tmp-*` debug/scratch tests removed from `src/pi_platforms/telegram/` before close-out (drift-reaudit report had flagged one) |

## Provenance notes for the parent orchestrator

1. Working-tree edits from 01:10–01:27 Z predate this session: this closure wave found its own
   predecessor dead mid-edit (usage-limit kills, see workflow journal) — the wave resumed, verified
   every claim against `/tmp/hermes-upstream`, corrected three substantive defects found in the
   predecessor's unfinished work (trim semantics, deleteMessage success handling, harness env race),
   and completed the world/conformance coverage (offline stamp, stream-delete-retraction e2e).
2. `reports/DISCORD-8-DEFER-CLOSURE-REPORT.md` is the discord agent's artifact — none of these
   areas overlap it.

## Proposed spec actions (for owners; NOT applied here)

- Update XREF-REPORT.md §6 residual 1: remove tg-11/tg-12/tg-13 from the open-defers list
  (personal-text-7 and discord-8 remain as handled by their own waves), citing this report.
- No DEC-026 entry required: all three closures move pi TOWARD upstream behavior with zero
  pi-specific architecture constraints discovered. If owners want the authorization-posture change
  recorded despite being parity-restoring, suggested number DEC-065 text:
  *"telegram gated callbacks deny-unconfigured (upstream #24457 parity); conformance fixtures
  force-allow explicitly" — alternatives rejected: kit-wide automatic allowlist inheritance from
  manifests (invented surface absent upstream).*

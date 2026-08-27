# XREF-DRIFT-REAUDIT.md — Owed upstream drift re-audit (XREF-BASELINE §3)

**Task label:** drift re-audit · Closure wave
**Date:** 2026-08-27
**Assigned edit areas (complete list):** `src/pi_embedded/kanban/notifier.ts` + `src/pi_embedded/kanban/notifier.test.ts` — nothing else was modified by this audit.

---

## 1. Upstream refresh (fallback exercised)

`git -C /tmp/hermes-upstream pull --ff-only` **failed twice** (HTTP 429 on first attempt, TLS `-110` termination on retry). Per task fallback the existing clone continues to serve, with one caveat:

| Item | Value |
| Upstream clone HEAD | `77001a6be76f…` (`fmt(js): npm run fix on merge (#95924)`), 2026-08-26 23:21 UTC |
| Audited baseline | `1bbb6e5bce56…` (XREF-BASELINE ruling) |
| Drift distance | clone is **389 commits ahead of the audited baseline**, all objects local (clone is shallow elsewhere but diff `1bbb6e5b..HEAD` is complete) |
| Caveat | upstream commits landing **after 2026-08-26 23:21 UTC are beyond this audit** |

This means "old → new" is `1bbb6e5b → 77001a6b`, restricted to XREF-BASELINE §3 regions and their direct dependencies. Notably, one discovery changed the framing mid-audit: **the working tree was NOT uniformly anchored to `1bbb6e5b`.** pi's kanban notifier already carried post-baseline behavior (`changes_requested` landed upstream only in a699234f8, *after* the campaign closed). Consequently each region was verified individually against NEW truth rather than assuming the campaign's snapshot.

## 2. Regions changed since the audited baseline

```
1bbb6e5b..77001a6b -- gateway/:        run.py(+405) delivery_ledger.py(+192)
                                       platforms/base.py(+35) kanban_watchers.py(+78)
                                       slash_commands.py(+16) scale_to_zero.py(+4)
                                       control_socket.py(+15, upstream-only module)
   -- plugins/:          platforms/slack/adapter.py(+436!) memory/openviking(+11)
                         supermemory(+4) model-providers/nvidia(+40) video_gen/fal(+36)
                         teams_pipeline/meetings.py(+7)
```

`session.py`, `platforms/{signal,api_server}.py`, `config.py`, `telegram`, `teams`, `wecom`, `discord`, `bluebubbles`, `yuanbao`, `pairing.py`, `authz_mixin.py`, `webhook.py`, `readiness.py`, `weixin.py`, `status.py`, `shutdown_*` did **not** move since the audited baseline — out of bounded scope.

## 3. Region-by-region verdicts

### R1. Kanban watcher notifications (`kanban_watchers.py`; upstream commits 7700d3a01, 1f92c5d4c, a699234f8) — **FIXED here**

pi surface: `src/pi_embedded/kanban/notifier.ts:renderNotifyMessage`. Terminal-kind set, cursor discipline, redaction-clamp architecture, and the `changes_requested` message skeleton were already in place; byte comparison vs HEAD truth found real divergences on wire-visible strings:

| Delta | Old pi | New truth | Action |
| --- | --- | --- | --- |
| Per-kind glyph prefixes | absent | `✔ ⏸ ✖ ✖ ⏱ 🔄 👀 🛑` | added |
| `completed` handoff fallback slice | merged summary/result at [:200] | payload.summary first-line [:200]; `task.result` fallback **[:160]** | split |
| `blocked` reason | clamped via `safeReviewReason` | RAW `[:160]` slice | conformed |
| `review_requested` summary | whitespace-collapsed + clamped | RAW multi-line `[:200]` slice | conformed |
| `block_loop_detected` recurrences | finite-number-guarded truncate | any truthy value rendered via `str()`; reason RAW `[:160]` | conformed |
| `changes_requested` composition | composed string re-clamped; @assignee tag present | parts clamped (`_safe_review_reason`: reason@160, identities@48), composite NOT re-clamped; **tag omitted** for this kind only | conformed |
| Python `splitlines()` semantics | naive `\n` split incl. raw-fallback edge | faithful separator set + raw-text fallback for stripped-empty input | conformed |

The removed over-redaction (clamping bodies Hermes sends raw) had no logged DEC — it was an unratified divergence, so fidelity (DEC-026) wins; `safeReviewReason` remains exactly where Hermes clamps. Tests conformed IN-COMMIT (removed stale expectations; added rows for glyph prefixes, [:160] fallback slice, raw-slice semantics, tag omission) — suite `notifier.test.ts` 101/101 within the 9-file kanban group green.

### R2. Obligations runtime recovery + adapter profile scoping (`run.py`, `delivery_ledger.py`, `platforms/base.py`) — **LATENT DIVERGENCE ⇒ propose DEC-A**

New truth adds: `adapter_profile` column (+migration), profile-scoped `deliverable_targets` claiming (legacy un-profiled rows claimable only non-multiplexed), `sweep_failed_for_runtime(platform, profile)` replaying ONLY this-process `failed` rows with `last_error ∈ {send_path_degraded}` and a distinct `RECONNECTED_MARKER`, `release_runtime_claim` returning undispatched claims WITHOUT spending an attempt, `require_success=True` resume-clearing split, per-platform reconnect hook, and base.py resweep signal when a degraded failure lands under a replacement adapter.

pi surface (`obligations/ledger.ts·scheduler.ts·sender.ts`, `entrypoints/gateway-run.ts` bootSends): matches **old truth** exactly. Every NEW consumer path has no production analog today:

- the universal record-on-final-send (`base.py:_final_delivery` records every non-slash/non-ephemeral response) **is not part of pi's port** — rows originate only from the webhook held-open seam (deliberate C5 split);
- the `send_path_degraded` producer lives in Hermes' telegram `_send_path_degraded` flag — that ledger-classification path did not change since baseline (telegram adapter unchanged in the diff window) and its producer-side parity posture was settled in earlier rounds;
- runtime adapters-vs-profile routing rides multiplex composition that is not wired into `gateway-run.ts`.

⇒ No live behavioral divergence executes anywhere; implementing dead contract surface would violate repo norms. Proposal below routes this as wiring constraints instead (DEC text in §5).

### R3. Multiplex auth-home separation (`run.py:_make_default_profile_message_handler`, `_is_user_authorized_for_source`) — **FUTURE-WIRING CONSTRAINT ⇒ same DEC-B**

Upstream now stamps the transport home as `source._authorization_profile_home` before entering the routed scope so authorization runs under the admitting bot's own scope, resolves home PER EVENT, and stamps `profile_route_rejected` fail-closed on `ProfileRouteRejected` before ingress. pi's `security/multiplex/*` library implements `withProfileIsolation` (= `_profile_runtime_scope`) but the primary-message-handler wiring it would harden does not exist in composition yet (verified: no non-test consumer outside `security/multiplex`). Same treatment as R2: record as binding constraints for the moment multiplex adapters get wired.

### R4. Slack inbound extras overhaul (`plugins/platforms/slack/adapter.py`) — rides a PRE-EXISTING ABSENCE ⇒ note-only

New truth changes inbound rich-text handling materially (per-element additional-only extraction with entity/date/permalink/code/style-aware dedupe normalization, `is_msg_unfurl` attachment skip, unknown-inline-type fallbacks, rich_text exclusion from agent serialization, reply_to_text withdrawal for thread replies, post-filter/pre-enrichment ts claim placement, bolt per-request proxy middleware, URL-extras entity dedupe). Audit finding: pi's slack port performs **no inbound Block Kit extraction at all** (flat `env.text` through socket-mode pipeline); the claim-lifecycle half (fresh-claim release on failure) IS ported (`processedMessageTs` + `trackFreshClaim`/`noteFailedInvocation` = thin-wrapper parity). Since waves 1–2 audited Slack outbound wire-parity (slack-1..5) and never adjudicated inbound extraction, this is a pre-existing feature absence whose surroundings churned — not drift introduced since baseline. Flagged as residual item needing its own audit + DEC before implementation.

### R5–R12. Not ported / excluded — note-only

| Region | pi analog | Verdict |
| --- | --- | --- |
| Kanban origin-wake synth (`_WAKE_KINDS` extension, `wake_review_detail`, i18n lines) | none (wake-synth never ported) | pre-existing absence; user-facing notify half conforms via R1 fix |
| `slash_commands.py` rollback display fields (`skipped_oversize`/`failed_deletes`) | `/rollback` command not ported; producer lives in `hermes_cli/cli_commands_mixin.py` (outside gateway/plugins scope anyway) | note-only |
| `scale_to_zero.py` off-Fly quiesce gating | scale-to-zero machinery not ported (no grep hits) | note-only |
| `control_socket.py` pause-for-update (#92091) | not ported; upstream-only module excluded by XREF-BASeline §4.3 | note-only |
| `compression.tail_mode` forwarding-list entry | hermes_cli config forwarding table not ported | note-only |
| `memory/openviking`, `supermemory`, `model-providers/nvidia`, `video_gen/fal`, `teams_pipeline/meetings.py` | no ports exist | out-of-port scope |

## 4. Discovered mid-flight observations (no action taken)

1. **Slack inbound extraction absence predates this audit** (see R4) — recommend a dedicated look rather than folding into a wave.
2. **Concurrent closure-wave siblings share the tree:** `discord-8` defer-closure artifacts + tg-11/12/13 contracts are landing in parallel (30 dirty files across `telegram/discord/slack/signal/tokenlock/webhook/conformance-rows` etc., plus an untracked scratch test `src/pi_platforms/telegram/.tmp-tg11-debug.test.ts`). None touched by this audit.
3. Baseline staleness: XREF campaigns should record clone-HEAD refreshes; kanban's post-baseline sync happened silently between rounds.

## 5. Proposed DEC text (for the log owner; not self-edited)

> **DEC-065 (proposed): Delivery-obligation runtime-recovery & adapter-profile contract stays UNWIRED pending features**
> Against `/tmp/hermes-upstream@77001a6b` (`gateway/delivery_ledger.py:sweep_failed_for_runtime/release_runtime_claim/_RUNTIME_RETRYABLE_ERRORS`, `RECONNECTED_MARKER`, `record_obligation(adapter_profile=...)`, `sweep_recoverable(deliverable_targets=...)`; `gateway/run.py:_clear_resume_pending_for_claimed_obligations(require_success=)`, `_redeliver_failed_obligations_for_platform`, reconnect-hook invocation; `gateway/platforms/base.py:_final_delivery` degraded-resweep signal): pi's obligations engine keeps the OLD (audited-baseline) contract because every new consumer path requires machinery pi deliberately does not compose (universal final-send recording per the C5 held-open split; producer-side send-path degradation classification; multiplex adapter composition). BINDING CONSTRAINTS when any of those lands: (a) obligation rows MUST stamp `(platform, adapter_profile)` with default profile `"default"` and schema migration; startup deliverability MUST filter exact `(platform, profile)` targets, legacy NULL-profile rows claimable only while the gateway hosts NO profiles; (b) ANY platform whose degraded send can yield a stable transient error class MUST replay such rows after adapter reconnect against the frozen allowlist (`send_path_degraded`), stamped with the RECONNECTED marker (distinct wording), releasing unsafe/undispatched claims without spending attempts; (c) resume-flag clearing during RUNTIME (as opposed to boot) recovery MUST hard-gate the send on successful store writes.

> **DEC-066 (proposed): Multiplex primary-handler authorization scoping (wiring precondition)**
> When shared/primary-adapter multiplex routing is wired into composition (`gateway-run.ts` currently composes no profile handlers), the following upstream hardening is REQUIRED, not optional: per-event transport-home stamping (`_authorization_profile_home`) consumed by a source-scoped authorize variant (`_is_user_authorized_for_source` semantics); authorization executed OUTSIDE the routed profile scope; per-event home resolution; fail-closed `profile_route_rejected` marker at ingress for rejected explicit routes; secondary-profile startup-failure reconnect bridging across `_running` flip (`_schedule_secondary_profile_startup_reconnect`).

## 6. Gates (this session)

| Gate | Result |
| Kanban scoped suites | **101/101** (9 files) |
| `npx vitest run src/pi_embedded` | **640/640** |
| Full-tree `npx vitest run` (first pass, during concurrent edits) | 3096/3098 — both failures in `telegram.test.ts` "telegram closures wave (tg-11/tg-12/tg-13)", owned by a SIBLING agent mid-edit; zero failures in kanban |
| Full-tree re-run at close-out | see addendum below |
| `npx tsc --noEmit` | 0 errors in audit-owned files; residual 3 errors confined to sibling files `slack-subject.ts`/`signal-engine.test.ts` (re-checked twice, ownership attributed via git status + diff) |
| Full-tree re-run at close-out (11:13) | 3096/3099 — remaining 3 failures ALL telegram-closures sibling WIP (`telegram.test.ts` tg-11/12/13 contracts, `conformance/telegram-rows.test.ts`, scratch probe `.tmp-chunk.test.ts`); kanban zero failures in every run |

## 7. Residuals accepted

- Beyond-clone upstream commits (post 2026-08-26 23:21Z).
- R4/R5 pre-existing absences (slack inbound extraction, wake synth) need dedicated audits.
- Known environmental token-lock SIGKILL flake (DEC-041 class) may appear on this host independent of code state.

— Report ends.

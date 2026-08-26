# XREF-REPORT.md — Hermes-Conformity Cross-Reference Campaign: Final Report

**Task label:** final xref verdict
**Date:** 2026-08-26
**Scope:** `/root/pi-gateway/pi-gateway` vs baseline `/tmp/hermes-upstream` (HEAD `1bbb6e5b`, per `reports/XREF-BASELINE.md` ruling: baseline wins over the local `8e475ed` snapshot)
**Pre-campaign HEAD:** `82f96b2` (`docs: census CLOSED` — Phase-6 census closure)

---

## 1. Verdict

## **STABLE**

Zero open high/medium unjustified findings. Every one of the 326 cross-reference
findings is either FIXED (306), DISMISSED-with-verification (2), KEPT-under-a-
logged-DEC (13), or a DOCUMENTED low-severity DEFER (4 fully open + 1 partially
resolved — see §6). All executable gates are green on the tree of record,
including two consecutive identical full-suite runs (DEC-041).

---

## 2. Methodology

1. **Baseline:** auditors cited `/tmp/hermes-upstream` `file:symbol` anchors;
   the local `/usr/local/lib/hermes-agent` snapshot (686 commits behind) was
   used only where the drift tables (XREF-BASELINE §3) marked regions stable.
   The tg-8 baseline-wins ruling (`_FLOOD_INLINE_WAIT_CAP_SECS/#91969` exist
   only upstream) was applied during round-1 synthesis.
2. **Two-round protocol:** round 1 audited nine axes (154 findings), an
   adjudication produced a 6-cluster fix plan (147 findings routed, 2
   dismissed, 5 deferred); round 2 re-audited the post-fix tree against the
   same baseline (172 new findings, all upheld after anchor verification),
   routing 159 to 39 stability clusters and ratifying 13 as DEC-tracked
   posture deltas.
3. **Verification gates (this session, on the final tree):**

   | Gate | Result |
   | --- | --- |
   | `npx tsc --noEmit` | clean (exit 0) |
   | `node scripts/check-layering.mjs` | OK (downward-only holds incl. new entrypoints rank) |
   | `node scripts/check-secret-scope.mjs` | OK (475 files) |
   | `npx vitest run` — run A | **3076/3076 across 237 files** |
   | `npx vitest run` — run B (consecutive) | **3076/3076 across 237 files** |
   | lens diagnostics (touched files) | 0 errors (style-level warnings only; `hass` typos are false positives) |

   An earlier full run observed exactly one failure: the token-lock
   doomed-holder SIGKILL close-wait test — the known environmental flake of
   the DEC-041 real-child-process class, reproduced at pristine HEAD via git
   worktree during the composition-root cluster (recorded in PROGRESS.md);
   `src/pi_gateway/security/tokenlock/` is byte-identical to pre-campaign
   (`git diff 82f96b2..HEAD` empty there) and passed in both runs of record.
4. **End-to-end spot-checks (pi anchor ↔ upstream anchor ↔ asserting test):**
   - **core-routing-8 (HIGH)** — upstream `agent/conversation_loop.py`
     repairs + `_sanitize_api_messages` unconditionally before EVERY API call
     over the list including the fresh user turn (:2229/:2480); pi arms
     `hostAgent.transformContext` in `runner.ts:703-723` (armed for the prompt
     duration, restored in finally) with `repairMessageSequence` +
     `sanitizeToolCallArguments`; `runner.test.ts:208-300` asserts the freshly
     appended ask merges so NO `user;user` pair reaches the wire, plus a
     TWO-PROCESS lease-wait interleave contract (:318).
   - **stream-egress-1 (HIGH)** — upstream `stream_consumer.py:_clean_for_display`
     applied at every send/edit/commentary site; pi `cleanForDisplay` =
     `stripMediaDirectivesForDisplay` applied at every frame/final path AND
     before silence detection (:777) and delivered-payload comparisons;
     `gateway-stream-consumer.test.ts:1022-1074` asserts raw `MEDIA:` /
     `[[audio_as_voice]]` never reach the wire.
   - **gchat-1 (HIGH)** — upstream `_patch_message :2346` computes
     `update_mask = ",".join(fields) or "text"` and strips thread from the
     patch body; pi `google-chat-adapter.ts:1410-1414` mirrors this onto
     `transport.patchMessage(..., updateMask)`; `gchat-rows.test.ts:508/:698/:887`
     assert the mask on captured patches.
5. **Kept-item spot-checks:** all 13 kept postures verified LIVE in code and
   now backed by logged DECs (§5). Deep-checked: IRC held-inbound window
   (`IRC_HELD_INBOUND_MAX=64`, drop-oldest, drain-on-reconnect) and raft
   never-spawn argv/token data contract — both byte-match their DEC text.
6. **Footprint audit:** committed diff `82f96b2..HEAD` = 232 files, ALL under
   `src/**` except `PROGRESS.md` (bookkeeping) and
   `scripts/check-layering.mjs` (the DEC-058-documented entrypoints rank row).
   Working tree adds only `src/**` cluster files + PROGRESS.md. **Zero**
   package.json/tsconfig/vitest.config/spec-doc edits by fixers. One stray
   fixer scratch dir (`.tmp-dbg/`) removed at close-out.

---

## 3. Findings ledger

### Round 1 — 154 findings across 9 axes

| Axis | Findings | Disposition |
| --- | --- | --- |
| telegram-wire | 14 | 11 fixed · 3 deferred (tg-11/12/13) |
| ws-connectors | 32 | 31 fixed · 1 deferred (discord-8) |
| webhook-connectors | 51 | 49 fixed · 2 dismissed |
| cn-exotics | 13 | 13 fixed |
| personal-text | 9 | 7 fixed · 1 dismissed · 1 deferred (personal-text-7) |
| core-routing-state | 7 | 7 fixed |
| streaming-egress-ledger | 12 | 12 fixed |
| security-ops | 10 | 10 fixed |
| structure-topology | 6 | 6 fixed |
| **Total** | **154** | **147 fixed · 5 deferred · 2 dismissed** |

- **Dismissed (verified REJECT):** `webhook-21` (transmitSeal is the draft-seal
  door; 'Not supported' matches Hermes parity; no live delete call-site) ·
  `personal-text-8` (INDICATOR_RESERVE '(XX/XX)' labels ARE Hermes truth; pi
  ports them verbatim).
- **Deferred:** see §6.

### Round 2 — 172 findings, zero dismissed

159 fixed across 39 stability clusters (all clusters reported complete and
were spot-verified); 13 kept as sanctioned posture deltas:

| Kept id(s) | Posture | DEC |
| --- | --- | --- |
| wa-1 | vendor HTTP transports bind behind Transport seams; conformance asserts arg-level wire parity AT THE SEAM | **DEC-059** |
| qq-5, qq-6 | connector branding tokens carry pi-gateway identity (identify properties, UA tail, QR source token, notification-title analogues) | **DEC-060** |
| ircsms-3, nthaha-7, nthaha-9, nthaha-11 | personal-text inbound resilience deltas (IRC held-inbound replay 64/drop-oldest; ntfy hold/redelivery window; HA fake backlog-flush modeling annotated pi-sanctioned; HA interleaved-frame tolerance) | **DEC-061** |
| raft-1 | bridge never spawned headless; exported argv + RAFT_CHANNEL_TOKEN stay the byte-exact data contract | **DEC-062** |
| raft-6 | stateless webhook-shaped adapters declare supports_async_delivery=false + interactive_resume=false | **DEC-063** |
| a2a-3, a2a-4, a2a-6, a2a-7 | a2a scope boundaries (non-local STATE_FAILED fail-closed; client tools excluded; registry skills deferred) + bind-safety escalation (widened host without credential ⇒ loud disable) | **DEC-064** |

**Totals: 326 findings → 306 fixed · 2 dismissed · 13 DEC-ratified · 5 deferred.**

### Close-out actions taken this session

- Logged **DEC-059..DEC-064** in `/root/pi-gateway/09-open-questions.md`
  (the adjudication's proposed numbering had been consumed by implementer
  DECs 055–058 for qqbot-live-gate / BB-attachment-cache / kanban-singleton /
  composition-root; kept-item decisions re-numbered, provenance noted).
- Swept the corresponding `proposed DEC text` comments to the logged ids in
  `raft/manifest.ts`, `a2a/manifest.ts`, `a2a/a2a-adapter.ts`,
  `irc/irc-world.ts`, `ntfy/ntfy-adapter.ts`,
  `homeassistant/homeassistant-adapter.ts`, `whatsapp-cloud/graph-wire.ts`,
  `qqbot/manifest.ts`, `qqbot/qqbot-adapter.ts`.
- Removed dead code surfaced by the gate: unused `imageExtensionFor`
  (qqbot), write-only `streamTask` field (ntfy), and guarded the
  `new URL` parse in qqbot's production byte-fetch (unreachable branch made
  explicit). Removed fixer scratch `.tmp-dbg/`.
- Re-verified after edits: tsc clean, both CI gates OK, affected suites
  386/386, full tree 3076/3076 twice consecutively.

---

## 4. Per-cluster state (round 2)

All 39 fix clusters reported complete with green scoped suites; commits land
under `fix(...)` messages (`3c37c13` … `dd9d6f1`; several later clusters share
the working tree). Representative anchors: telegram-wire-r2 (tg2-1..12),
discord/mattermost/matrix/slack r2 (typing + processing-hook lifecycle),
whatsapp-cloud-r2 (hintMime-first, document-content injection, voice opus
ladder, slash-confirm card, reply_to_text, verbatim `to`), line/gchat/wecom/
feishu/teams/msgraph r2, api-server-r2 (`3e49173`), qqbot-r2 (`96cdb50`),
yuanbao-r2 (`f8bff2f`), weixin-r2 (`5b39093`), buzz/simplex/photon/raft/a2a/
signal/bluebubbles/email/irc-sms/ntfy-ha/cron-standalone r2, agent-loop-repair
(`b21e780`), telegram-topic-bindings (pi_state migration + two-process tests),
slash-access (`3f96df5`), resolution-state (`cacfa9b`), streaming-consumer,
egress-router (`341a009`), obligations-resume (#91969 claim-time clear),
embedded-security (`399b5b4`), agent-cache evictability (`5ae4c17`),
composition-root (entrypoints layer, DEC-058), shutdown-drain, loop-wakeup
(`dd9d6f1`). Round-1's six clusters (147 findings) are folded into the same
history (squashed into `3c37c13` during the identity history rewrite).

---

## 5. Conformance-suite integrity

Fakes/fixtures/conformance rows were rewritten to Hermes truth IN-COMMIT per
the fix plan (e.g. feishu split-row encoder + vendor reaction payloads, teams
real `{type:"typing"}` activity assertion, gchat updateMask rows, eml Message-ID
passthrough, mm/discord/slack invented-key removals). The executable 04 §8-style
gate is green tree-wide; no row enshrines an invented wire shape anymore.

---

## 6. Residual risks (accepted, documented)

1. **Open low-severity defers (4 + 1 partial):** tg-11 (callback authorizer
   defaults allow-all until the kit-wide fail-closed seam lands — exec
   approvals clickable by unauthorized chat members in that window), tg-12/
   tg-13 (deleteMessage + sendMessageDraft naming await consumer-lane/capture
   work), discord-8 (post-turn rename needs an LLM session-title source pi
   lacks). personal-text-7 is PARTIALLY resolved: the standalone-cron-sender
   kit seam exists with ntfy/HA senders (nthaha-10); remaining platforms ride
   the same seam on demand. Each needs its own DEC before implementation
   (DEC-026).
2. **Baseline duality:** fixes target `/tmp/hermes-upstream`; the local
   snapshot lacks newer behavior in the §3.1/§3.2 drift regions (telegram
   adapter ±309 lines, gateway/run.py ±889 lines). A future upstream sync must
   re-run conformity over those regions.
3. **Deployment-time integrations:** until fetch-backed transports land under
   DEC-059, reachability/auth-header shaping/vendor-side validation of some
   response bodies is asserted only arg-level at seams.
4. **Truncation-parity regressions accepted per DEC-026 fidelity:** line ≤5
   bubbles, ntfy/HA 4096 truncation, single chat/new — flag in release notes.
5. **Branding tokens** stay Hermes-divergent unless a DEC-060 owner ruling
   restores Hermes byte-forms.
6. **Environmental flake:** the SIGKILL close-wait heavy-process test can flake
   on this 4-CPU host (pre-existing, pristine-HEAD-reproduced, passes in the
   two runs of record).
7. **Commit bookkeeping:** the identity history rewrite squashed the round-1
   clusters into `3c37c13`; content is verifiable via the suites and this
   report, but per-cluster commit isolation for round 1 is lost.

---

## 7. Counts (of record)

- Findings: **326** (154 round-1 + 172 round-2)
- Fixed: **306** · Dismissed: **2** · Kept/DEC-ratified: **13** · Deferred: **5**
- Suites: **3076/3076 across 237 files, twice consecutively** (baseline at
  campaign start: 2152/2152 × 199 files → +924 contracts)
- Gates: tsc clean · layering OK · secret-scope OK (475 files)
- DECs logged during campaign: DEC-044..DEC-064 (21), each with verification entries

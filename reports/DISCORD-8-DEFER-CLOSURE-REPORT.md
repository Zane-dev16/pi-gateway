# DISCORD-8 DEFER CLOSURE REPORT

**Task label:** discord defer (closure wave) · **Date:** 2026-08-27
**Scope:** `src/pi_platforms/discord/**` (+ fixture/tests/wiring) vs upstream truth
at `/tmp/hermes-upstream/plugins/platforms/discord/**` and `/tmp/hermes-upstream/gateway/run.py`,
`agent/title_generator.py`, `agent/turn_context.py`, `hermes_cli/platform_actions.py`.
Baseline: `/tmp/hermes-upstream` HEAD `1bbb6e5b` (XREF-BASELINE ruling unchanged).
Owner rule applied: unjustified divergence ⇒ conform to Hermes truth;
justified divergence ⇒ smallest change + proposed DEC entry (below).

---

## 1. Verdict

**discord-8 CLOSED as: in-area carrier brought to byte-parity where live
consumers exist; the remaining consumer lane (post-turn semantic rename)
adjudicated JUSTIFIED-DEFERRED behind a proposed DEC-065** — it cannot exist
in this port until a platform-agnostic LLM session-title source lands, which
is cross-core work outside every platform area (see §3).

Zero unjustified divergences remain in `src/pi_platforms/discord/**` with
respect to the rename lane; two latent in-area divergences were found during
the sweep and **fixed to upstream truth** this session (§4).

## 2. Upstream truth (the chain discord-8 deferred)

The post-turn rename is ONE consumer at the tail of a five-stage chain:

| Stage | Upstream anchor | Pi carrier status before this sweep |
| --- | --- | --- |
| S1 instant derived title | `agent/title_generator.py` `derive_title` / `_summarize_user_message` (`maybe_auto_title` fired at turn prologue, `turn_context.py:206/:235`, first two exchanges :422) | ABSENT (no titler anywhere in tree; `/title` executes host-side with no adapter-visible signal) |
| S2 LLM title | `generate_title` aux-tier call, `{title:"..."}` response contract (:389+), `title_source=="llm"` gate | ABSENT (model access bound inside DEC-023 host-loop runs) |
| S3 title→consumer callback | `run.py:_attach_session_title_callback` → `agent._on_session_title`; Discord arm fires ONLY `title_source=="llm"` (derived titles skipped deliberately — rate-budget protection, "Discord's 2-per-10-minutes channel budget") | ABSENT |
| S4 schedule+sanitize | `run.py:_schedule_discord_semantic_thread_rename` / `_rename_discord_auto_thread_for_session_title` (:23421+) / `_sanitize_discord_thread_title` (UTF-16 80→77+`"..."`) / lane predicates `_is_discord_auto_thread_lane` (:23322+) + relay siblings | ABSENT (no relay connector lane either) |
| S5 guarded rename primitive | `plugins/platforms/discord/adapter.py:rename_thread` (:7281-7339): best-effort; int-parse else false; whitespace-clean else false; UTF-16 truncate; resolve channel cache-or-fetch else false; `only_if_current_name` no-clobber skip (false); equal-name short-circuit true; `edit(name, reason=...)` | ABSENT — deliberately left so (see §3; tg-12/tg-13 protocol: primitives land WITH their consumer lane; the campaign removed orphan carriers at close-out, e.g. ntfy write-only `streamTask`) |
| S0 evidence the lane keys on | `_hermes_auto_thread_initial_name` stamping (:7244/:7260) → `build_source(auto_thread_created=True, auto_thread_initial_name=…)` (:8283-8288); guard prevents clobbering human-renamed threads | pi holds equivalent state (`prospectiveThreadId` ⇔ `auto_thread_created`; name ⇔ `deriveThreadName(content)`); NOT stamped onto flowing events since no consumer parses it — write-only metadata, same class the campaign deleted |

Everything S1–S5 requires S2; S2 must be designed against the DEC-023 host-loop
boundary (auxiliary model tier, turn-prologue hook, session-store title writes
with upstream's precedence rules `set_auto_title/source=llm`). That is core
work owned by nobody's platform area — exactly why the finding was deferred.

**personal-text-7 leftover sweep:** personal-text never covered discord (its
partially-closed residue is the standalone-cron-sender seam awaiting other
platform riders). No discord-facing personal-text-7 item exists. Confirmed by
reading XREF-REPORT §3/§6 and grepping all campaign docs.

## 3. Proposed DEC text (owner to log — DEC-065 next free number)

> ### DEC-065: discord post-turn semantic thread rename stays deferred until a platform-agnostic LLM session-title source exists; adapter-side derivation and continuity are complete carriers
>
> - **Date / Phase:** 2026-08-27 / closure wave (deferred finding discord-8 from the xref round-1 ws-connectors axis)
> - **Status:** accepted-as-posture (proposed; not yet logged in this file)
> - **Question:** Hermes renames a just-auto-created Discord thread after the LLM-generated session title on the opening exchange (`run.py:_attach_session_title_callback` → `_on_session_title(title, title_source)` firing only for `title_source=="llm"`, then `_schedule_discord_semantic_thread_rename` → `discord/adapter.py:rename_thread(:7281)` guarded PATCH). pi has none of these stages. Is that an unadjudicated divergence?
> - **Decision:** Justified defer of the WHOLE rename consumer chain. The chain's premise is an LLM session-title source generated in the gateway process (Hermes `agent/title_generator.py`: prologue `maybe_auto_title`, aux-model-tier `{"title": …}` call, `set_auto_title(sid, title, source=llm)` precedence writes). pi binds model access inside DEC-023 host-loop runs; no gateway-side title signal exists (`/title` executes host-side with no adapter callback; no `sessions.title` writer in `src/` besides session-mint seeding). Building an auxiliary-model pipeline is a new architecture surface, out of every platform area's remit. Per tg-12/tg-13 protocol NO orphan primitives are pre-implemented (no `renameThread` method, no unwired source markers — the close-out already established that write-only carriers are banned, cf. ntfy `streamTask` removal). The lane lands together with the shared titler via a follow-up implementer DEC; its platform-side spec is frozen by this entry: `rename_thread` semantics per upstream :7281-7339 INCLUDING the `only_if_current_name` no-clobber guard, equal-name `true` short-circuit, UTF-16 80→77+`"..."` truncation and `X-Audit-Log-Reason: …semantic session title` header; `auto_thread_initial_name` stamped from `deriveThreadName(raw content)` at thread creation; fire ONLY for `title_source=="llm"` on the exchange whose dispatch created the thread (guard = creation-time name).
> - **Carriers already conformant (this wave made them byte-exact):** `deriveThreadName` = upstream `_derive_auto_thread_name` :7200-7216 numeric-only mention strips + Python code-point [:80]/[:77]+`"..."` slicing (§4a); thread creation/retry/abort/dedup/keying per DEC-028 + #51057/#20243 fixes (audited xref round-2).
> - **Alternatives rejected:** implementing `renameThread` ahead of any caller (dead API surface; can't assert end-to-end; the campaign deletes such carriers); renaming threads from DERIVED titles today (violates upstream's explicit rationale — spends Discord's 2-per-10-min rename budget on throwaway names); reverting pi's safer astral passthrough to pre-existing truncation (that truncation could emit LONE SURROGATES — see §4b).
> - **Hermes parity:** structural parity of everything model-free; behavioral parity of the tail stage explicitly time-boxed behind the titler prerequisite (same posture family as tg topic titles).
> - **Blast radius:** none today (no behavior consumers); future work touches discord adapter REST plane + gateway composition only.
> - **Verification:** derive rows pin byte-parity (numeric-only strips, non-numeric survival row, 80-codepoint boundary, emoji-whole quirk) in discord.test.ts; auto-thread world pins numeric-vendor-mention naming end-to-end at the wire; suite green.

## 4. Fixes landed this session (unjustified divergences made conformant)

Both were in `deriveThreadName` (the ONE function of the deferred lane that has
live consumers TODAY — auto-thread names are wire traffic):

**(a) Mention/channel strips were broader than upstream.**
pi stripped `<@!?[^>]+>` / `<@&[^>]+>` / `<#[^>]+>`; upstream matches digits
ONLY (`<@[!&]?\d+>`, `<#\d+>` — adapter.py:7209-7212). Non-numeric bracketed
text like `<@team>` is real content Hermes keeps in titles. Conformed; the
fake-world convention `<@bot-self>` violates vendor syntax (mentions are always
numeric), so the fixture gained `MakeDiscordWorldOptions.botUserId` (READY
identity grounding passthrough) and the auto-thread tests now model vendor
truth with a numeric snowflake.

**(b) The cap used UTF-16 units but truncated by JS code units — a hybrid neither
upstream budget uses.** Upstream derive caps at 80 PYTHON CODE POINTS
(`len(content)>80`, `[:77]+"..."` — emoji stay whole even though the vendor
80-UNIT budget then overruns: upstream's accepted derive-path quirk, which is
precisely why its RENAME path re-truncates with utf16 helpers). pi checked
`utf16Len>80` then sliced `slice(0,77)` by UTF-16 CODE UNITS — for
emoji-heavy openers that emits a LONE HIGH SURROGATE into the wire payload.
Conformed to Python semantics (boundary row asserts exactly-80-codepoints
passes whole; over-edge cuts to whole 77 code points + `"..."`). The quirk and
its rename-path remedy are documented at the call site; `manifest.ts` comment
now distinguishes the derive (code-point) and rename (UTF-16 unit) budgets.

**Deliberately NOT done:** adding a `renameThread` primitive or inert marker
metadata (dead carriers — see DEC text); changing `THREAD_NAME_FALLBACK`
("Pi", upstream "Hermes") — flagged as a DEC-060-family branding token needing
either an enumerated extension of DEC-060 or an owner revert ruling; the
absent inbound `THREAD_UPDATE→thread_renamed` hook event (adapter.py:1852-1892)
belongs to the platform-events/hooks subsystem that no pi adapter ports — new
scope beyond this defer, flagged for the owner.

## 5. Diff

```
src/pi_platforms/discord/discord-adapter.ts | 27 +++++++++++++-------
src/pi_platforms/discord/discord-fixture.ts |  9 +++++++-
src/pi_platforms/discord/discord.test.ts    | 35 ++++++++++++++++++++++++-----
src/pi_platforms/discord/manifest.ts        |  9 ++++++--
```

No changes outside the assigned area. No package/tsconfig/vitest/spec/report-ledger edits.

## 6. Gates

| Gate | Result |
| --- | --- |
| `npx tsc --noEmit` (tree-wide) | exit 0 |
| `npx vitest run src/pi_platforms/discord/discord.test.ts` | 37/37 |
| lens diagnostics (discord files, mode=full) | 0 errors; pre-existing style notes only |
| Full `npx vitest run` | 3077/3078 across 237 files — sole failure `slack.test.ts "FAILED turn releases its fresh processed-ts claim"` on the SHARED closure-wave working tree while another worker's slack edits were mid-flight (last write 00:56, my run 00:54-00:57). Slack/tokenlock/webhook diffs are not mine; tokenlock itself was green in the same run. Re-run recommended after wave quiesce. |
| Full `npx vitest run` (re-adjudication pass) | 3092/3094 across 237 files — the two failures are `telegram.test.ts` tg-12 contracts, i.e. the concurrent telegram-defer worker's mid-flight edits, not this area. Discord 37/37 green incl. inside this full run. |
| Tree-wide `npx tsc --noEmit` (re-adjudication pass) | errors confined to `signal/signal-engine.test.ts` + `slack/slack-subject.ts` (other workers' mid-flight areas); ZERO errors under `src/pi_platforms/discord/**` |

Shared-tree note: concurrent closure-wave workers hold in-flight diffs in
slack/polling/webhook/tokenlock areas; my diff is confined to the four discord
files above.

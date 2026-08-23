# PHASE 2 REPORT — Streaming · Obligations · Commands · Outbound/Media

**Verifier pass:** independent re-run of every gate + open-file audit of each claimed
contract + one live source-mutation experiment. Nothing taken from builder reports on faith.

---

## 1. Gates (re-run by verifier)

| Gate | Result |
| --- | --- |
| `npx tsc --noEmit` | **exit 0** |
| `node scripts/check-layering.mjs` | **exit 0** — "downward-only holds across src/" |
| `npx vitest run` (full repo) | **60 files / 678 tests PASSED**, 0 failures |
| Unhandled errors | 1 × EPIPE teardown artifact in `spike/tests/lease.spike.test.ts` — **pre-existing Phase-0 throwaway**, documented in PROGRESS, outside Phase-2 footprint |
| Baseline reconciliation | 678 total − **349 new** = **329 prior baseline** ✓ exact |

### Per-module counts (verifier-measured)

| Module | Src units | Test files | Tests |
| --- | --- | --- | --- |
| `src/pi_gateway/streaming/` | stream-events, adapter-seam, capability, egress-door, gateway-stream-consumer, dispatcher, testing/fake-adapters | 4 | **58** |
| `src/pi_gateway/obligations/` | ledger, sender, scheduler, clock, health (+ testing/manual-clock, testing/ledger-driver) | 4 | **37** |
| `src/pi_gateway/commands/` | command-def, registry, config-gates, derived, busy-resolver, slash-intake, inject | 9 | **82** |
| `src/pi_gateway/outbound/` | response-filters, media-policy, media-grammar, segmentation, delivery-router, delivery-targets, dead-targets, post-stream-rescan, auto-tts | 8 | **172** |
| **Total** | | **25** | **349** |

## 2. Exit checklist (roadmap §Phase 2)

### a. Streaming vs BOTH adapter shapes + non-prefix mutation — **PASS**

- `testing/fake-adapters.ts` defines `FakeStreamIsMessageAdapter` (relay-shaped: turn-final
  send absorbed into stream via seal-interception) and `FakeDraftStreamAdapter`
  (Telegram-shaped: drafts clear client-side, final is ordinary send). Both are built on the
  **production `EgressChokepoint`** — the single-chokepoint property is exercised by shared code,
  not duplicated fixtures (DEC-006).
- `gateway-stream-consumer.test.ts` runs its whole contract set under
  `describe.each(SHAPES)` — every contract holds against **both** shapes.
- Mutation proof: the suite injects a mid-stream prefix-breaking `composeFrame`
  (`acc.slice(1)` after frame 1) and asserts three observable effects:
  1. violation recorded with both frames (`non_prefix_frame`, prev/next captured);
  2. mutated frame **never reaches the wire** (ops-log-wide negative assertion);
  3. draft lane **permanently disabled** — subsequent deltas reroute through the edit path,
     plain-draft count frozen;
  then final delivery byte-exact despite the mutation. An honest-compose self-check asserts
  zero violations (mutation detector itself mutation-tested).
- All timing via injected clock (`makeClock`/`clock.advance`); adapter sync is event-based
  waiters, no sleeps.

### b. Obligations state machine + caps under injected clock — **PASS**

- **3-attempt cap**: three doomed-boot rounds claim & fail (`attempts` observed 1→3),
  4th boot's sweep refuses the claim → row `abandoned`, `sender.callCount === 3`.
- **24h stale**: injected clock, **strict `>` boundary tested from both sides**
  (age == STALE_AFTER_SECONDS still claimed; +1s abandoned).
- **7d retention**: delivered rows pruned after confirmation window (undelivered retained);
  abandoned rows have their own inspection window.
- **500-row cap**: Hermes eviction rank order (delivered → abandoned → oldest active) plus a
  continuous-admission test that `record()` keeps total ≤ 500 under load.
- **Crash-recovery exactly-once**: `two-process.test.ts` spawns **real OS processes**
  (`ledger-driver.ts`): boot 1 records + begins attempt then dies; boot 2's sweep claims
  exactly 1 row, sends with `RECOVERED_MARKER`-prefixed content (byte-checked suffix),
  `attempts === 1`, lands `delivered`; boot 3 claims **0** — budget spent exactly once.
  Plus an absent-platform boot leaves rows untouched, and two-engine guarded-claim races
  never double-send. `mkdtempSync` isolation throughout.
- Scheduler: backoff slots 60s/240s/960s under injected clock; inter-send gaps never shrink;
  dead-owner immediate recovery at restart boundary; stop breaks sleep cleanly.

### c. Command derivation property — **PASS**

- `derivation.test.ts`: baseline-absence asserted first, then **one runtime registration**
  of a test-only `CommandDef` (`deploy`, alias `dp`, policy `reject`) and **all six derived
  surfaces asserted simultaneously** — help line (exact string), known-command set
  (canonical + alias, case/slash-insensitive), completion catalog, Telegram menu model
  (sanitized canonical only), busy resolver (alias routes, policy derives, bypass-set delta
  computed exactly), slash-intake flips `unknown-command → command`. Zero consumer-code change:
  every surface is re-derived from `registry.rows()`.
- Unknown `/foo`: `"/foo bar baz"` falls back to TEXT with **original bytes untouched**
  (`intake.text === raw` identity asserted); guards-side double-check resolves unknown → null
  → caller queues as text.
- Busy-policy catch-all byte-stable: `guard-feed.test.ts` feeds the real `RunnerBusyGuard`
  **from the registry** via `toGuardRows` and asserts the catch-all reject equals the exact
  literal `⏳ Agent is running — \`/title\` can't run mid-turn. Wait for the current response or \`/stop\` first.`
  through the same resolver the runner uses; per-command override (`/model`) honored via
  `busy_handler`.

### d. Media grammar offset-mask integrity under nested spans — **PASS (mutation-checked live)**

- Nested protection spans mask their **union** (blockquote lines inside fences, fences inside
  quotes): only the real tag delivers; every protected example survives **verbatim** in cleaned
  text. ASTRAL-plane content keeps code-unit offsets valid (`masked.length === content.length`,
  outside tag parses at its exact original offset). Example/stored paths never deliver
  (#35695 blockquote examples, #34375 JSON-string stored tags); inline-code exception is
  validation-gated.
- **Live mutation experiment (verifier-run)**: broke `mergeSpans` union
  (`s <= last[1]` → `false && …`) → suite **failed** (caught by the dedicated mergeSpans unit
  contract asserting exact unions); restored → **34/34 green**. Detection empirically proven.
  *Observation:* the nested-span integration fixtures happen to feed non-overlapping span sets,
  so the union property is held by the mergeSpans unit contract rather than the integration
  cases — noted, not a gap.
- File restored byte-exact after experiment; `git status` clean of modifications.

### e. No second command list — **PASS**

- No src file outside `commands/` imports `commands/` yet (runner wiring is Phase 3); the only
  cross-module consumption is by **injection** — `toGuardRows(registry.rows())` feeding guards,
  proven by the guard-feed wiring test. Consumers derive everything; none hardcodes a catalog.
- Static name sets that do exist in `guards/busy-policy.ts` (`SPECIAL_BUSY_HANDLERS`,
  `PREGATE_COMMANDS`) are §5.4 busy-handler-key vocabulary / pre-gate protocol tables ported
  from `run.py` — pre-existing Phase-1 spine, untouched this phase (see §3), not derived state.

## 3. Footprint audit

`git status --porcelain` = exactly four untracked directories
(`src/pi_gateway/{streaming,obligations,commands,outbound}/`). **Zero modified tracked files** —
builders touched nothing else (package.json/tsconfig/vitest config/spike/PROGRESS/reports/spine
all pristine).

## 4. Fixes applied during verification

None required — nothing trivially broken found. One temporary mutation was injected and
reverted as part of criterion (d); post-restore state verified green and byte-exact.

## 5. Blocked threads

None blocking exit. Standing notes:

1. Spike EPIPE teardown error (Phase-0 throwaway) will keep surfacing in full-suite stderr
   until the spike is retired; harmless, documented.
2. mergeSpans observation above (detector lives in the unit contract).

## 6. Proposed DECs collected (from builder source markers — require logging)

| # | Proposal | Source |
| --- | --- | --- |
| P1 | Media-policy env var names stay **byte-identical to the Hermes bridge**; renaming to `PI_MEDIA_*` is cosmetic-only and must not drift behavior | `outbound/media-policy.ts:25` |
| P2 | Pi-parity home root: **one root** (`context override → PI_HOME → ~/.pi`) plays both Hermes' active-profile-`HERMES_HOME` and shared-default roles | `outbound/media-policy.ts:108–114` |
| P3 | Synthetic `/workspace` and `/root` sandbox Docker mounts (Desktop-backend state) **omitted** from container-path translation | `outbound/media-policy.ts:340–347` |

## 7. Verdict

**PHASE 2: PASS** — all five exit criteria met with genuine behavior contracts;
678/678 green (349 new), tsc clean, layering clean, footprint confined.

# PROGRESS — live gateway answers on RPC build (2026-10-07)

## Live swap `afe462e` → `28433a1` plus Telegram round 2 (2026-10-10)

- Installed checkout fast-forward only, `afe462e` → `28433a1` (contains
  `5682a68`). Tree clean before merge. No force. Old pid `1421693`
  (sha `afe462ee46`) stopped via `bin/pi-gateway`, state
  `stopped`/`planned_stop`. Relaunched on the identical production boot
  path, log appended to `gateway-live.log`. New pid `1632198`
  (sha `28433a1556`), READY at log line 119. Doctor HEALTHY before
  and after, new pid alive, 12 tables ok, cwd is the installed
  checkout. Zero conflict lines after the launch mark. One old 409 hit
  is a UUID fragment at line 47, not a Bot API conflict.
- Live Telegram proof (whoami Irell id 8469032365, peer ZanishPiBot,
  doctor HEALTHY on `/root/.pi` pid `1632198` before first send):
  `/help` send 3434 drew replies 3435-3442 within 8s. Pineapple send
  3443 with token `pineapplesierra10101926` drew reply 3444 echoing the
  token within 5s. `/new` send 3445 drew reply 3446 starting session
  `01a12747`. Window 19:23:45Z-19:26:18Z had no interleaving send.
  `state.db` messages 56 → 62 with rows 57-62 holding the three new
  user plus assistant pairs, sessions 1 → 1. Live was never restarted,
  stopped, or cleaned during proof.
- Gates at tip `28433a1`: `tsc` zero errors, vitest 193 files 2573
  tests zero failures, layering downward only, secret scope clean.
  Sanity round 2 on a disposable home passed boot HEALTHY plus CLI
  status plus headless 44 green plus stop `planned_stop`, then cleaned.
- Correction: the prior entry cited `artifacts/fix-unit1/`. Verified
  this session that path does not exist in the tree, so treat that
  citation as disposable logs outside the repo, not committed evidence.

## Fix live ingress stall plus live doctor path (2026-10-10)

- Root cause (read-only live proof, no restart): the poll recovery ladder
  went FATAL after 5 straight transient failures, discarding held inbound
  and stranding the adapter with the process alive. Measured live: pid
  1421693 alive with guard wired, `state.db` messages frozen at 56 across
  the three verifier sends, zero replies even for native `/help` and
  `/new`, no ADAPTER FATAL line in `gateway-live.log`, and Bot API
  `getWebhookInfo` reporting `pending=3` with no webhook set. The fatal
  was silent because production adapters boot with no logger and runtime
  fatals never requeue to the reconnect watcher. Reproduced exactly on a
  disposable seam: 5 virtual sleeps then `recovery-exhausted` plus fatal.
- Fix (`polling-adapter.ts`, one ladder): transient-class `scheduleRecovery`
  retries unbounded with backoff capped at the top rung (60s) and a warn
  line per attempt; held inbound is kept. The 409-conflict ladder keeps
  its bounded-to-fatal shape, so rival-consumer loops still need an
  operator. New contract pins an outage longer than the old budget
  healing once the server returns. Follow-up, not this change: plumb a
  logger into the production adapter factory and requeue runtime fatals
  so any future terminal state is loud plus self-healing.
- Doctor (`control-pi-gateway`, harness, gitignored): live homes log READY
  to `gateway-live.log`, never `$HOME.launch.log`, so live doctor always
  cried UNHEALTHY. It now falls back to `gateway-live.log`. Measured:
  `doctor --home /root/.pi` reports HEALTHY, read-only, live pid
  undisturbed.
- Verify: `tsc` clean, polling 19 green, headless Telegram 44 green,
  targeted battery 9 files 134 green, layering plus secret-scope OK,
  disposable boot HEALTHY plus CLI status running plus stop `planned_stop`.
  Evidence under `artifacts/fix-unit1/`. Live replies still need an
  operator restart to pick up the fix; nothing was restarted here.

## VerifyLive `afe462e` on tip plus live swap and Telegram proof (2026-10-09)

- Disposable homes first, live read-only until swap. Boot READY plus
  HEALTHY on a disposable home, CLI status running plus stop
  `planned_stop`. Throwaway in-repo probe (mirrored the production
  factory exactly, deleted before commit) under an isolated
  `PI_CODING_AGENT_DIR` with seeded auth plus model config: `/help`
  finalized, plain echo `pineapple-verifylive-1` finalized on the live
  model path, native `/new`, bare `/switch-path` populated, `/new-path`
  moved child cwd, `/switch-path` rebound to the resumed session, 12 db
  rows with user plus assistant pairs. Headless Telegram 44 green. `tsc`
  clean. Targeted suites (status-stamp plus discovery plus
  rpc-turn-runner, 51 tests) green. Artifacts under `verifylive/`.
  Fallout fixed in the probe: the isolated agent dir needed the live
  model config (settings plus models) beside auth, else children default
  to a dead endpoint; one cold-start `/help` timeout passed on retry.
- Installed checkout fast-forward only, `81e5966` → `afe462e`
  (`ca85df8` extension discovery wiring plus `afe462e` status guard).
  Old pid `1113424` stopped via `bin/pi-gateway stop`, exited at once,
  state `stopped`/`planned_stop`. Relaunched on the identical production
  boot path with the profile env file sourced in memory (never printed).
  New pid `1421693`, `gateway READY`, code_sha `afe462ee46`,
  `platform adapter telegram guard wired`. Zero `409` lines.
- Live Telegram proof (user Irell @irellzane id=8469032365 → ZanishPiBot,
  `whoami` first): `/help` answered natively (msgs 3414–3421, opens with
  `` `/copyfile` ``); plain `reply with the word pineapple-live-10`
  answered `pineapple-live-10` (msg 3423, model turn live); `state.db`
  messages 48 → 56 with user plus assistant rows for all four turns;
  `/new` answered `Started a new session
  (01a12274-76fb-7493-8493-e59b6d740d1f).` (msg 3425, native reset);
  bare `/switch-path` answered a populated listing (`/root`, `/root/.pi`,
  `/root/pi-gateway`, and more, msg 3427), closing the prior open gap.
- Guard proven live: 3 competing starters aborted at `duplicate_guard`
  during the phase while `gateway_state.json` kept naming the live
  holder `1421693` running. No probe rows deleted (zero). Live pid
  undisturbed except the planned swap stop.

## Live swap `1131270` → `fd7b9b5` plus Telegram proof (2026-10-08)

- Installed checkout fast-forward only, `aca2c85` → `fd7b9b5` (3 commits:
  `cb17c5f` path-switch, `f060301` status-file, `fd7b9b5` discovery-root).
  Repo checkout already at tip. Identity `Irell Zane` verified in both.
- Live gateway: old pid `893992` stopped via `bin/pi-gateway stop`
  (`gateway: stop signalled`), exited at once, state `stopped`/`planned_stop`.
  Relaunched on the identical production boot path (`PI_HOME=/root/.pi`,
  `PI_GATEWAY_AUTO_START=1`, `PI_GATEWAY_PLATFORMS=telegram`, cwd installed
  checkout, `tail -f /dev/null | exec pi --mode rpc`, log appended to
  `/root/.pi/gateway-live.log`). Bot token re-supplied from a local session
  artifact without printing. New pid `1113424`, `gateway READY`, code_sha
  `fd7b9b5ed3`, `platform adapter telegram guard wired`. Zero `409`/conflict
  lines in the fresh log (one uuid false positive only).
- Live Telegram proof (user Irell @irellzane id=8469032365 → ZanishPiBot,
  `whoami` first): `/help` answered natively via host catalog (msgs
  3395–3402, skill list, no model call); plain `reply with the word
  pineapple-live-8` answered `pineapple-live-8` (msg 3404, model turn NOT
  policy-blocked); `state.db` messages 38 → 40 with user plus assistant rows
  under session `tg:8469032365`; `/new` answered `Started a new session
  (01a11b36-d026-7346-9324-95a8efc476ce).` (msg 3406, native reset);
  `/new-path /tmp/live-path-probe-dirb` answered `Started a fresh session
  (01a11b37-f048-74ca-8557-015d10f69f83) under /tmp/live-path-probe-dirb.`
  (msg 3410, child cwd moved); `/switch-path /root/.pi` answered `Switched
  to /root/.pi. No sessions there yet — starting fresh.` (msg 3412, cwd
  moved back). No blockers.
- Open gap (not a blocker): bare `/switch-path` on live answers `No paths
  holding pi sessions.` Root cause read from tree: the standalone factory
  in `entrypoints/pi-gateway.ts` closes over both discovery closures, but
  the live extension factory in `extensions/pi-gateway.ts` passes none, so
  the runner falls back to an empty list and the rebind lookup stays null.
  Dir moves still land both ways. Follow-up wires the same two closures
  into the extension factory.

## Verify tip plus fix DEC-087 discovery root (2026-10-08)

- Root cause: the production turn factory rooted session discovery at the
  gateway home (`<home>/sessions`), but pi writes sessions under its agent
  dir (`<os home>/.pi/agent/sessions`, `PI_CODING_AGENT_DIR` override),
  measured this session via the installed pi `config.js:getAgentDir` plus
  real child session files. Bare `/switch-path` always reported none and
  `/switch-path <path>` never rebound, always starting fresh.
- Fix: `discovery.ts:resolveAgentDir` mirrors the host rule exactly
  (env override else os-home agent dir with tilde expansion); the factory
  in `entrypoints/pi-gateway.ts` resolves it once for both discovery
  closures. Three tests pin override plus tilde plus fallback.
- Verify (disposable homes only, live read-only, live pid undisturbed):
  boot READY plus HEALTHY, CLI status running plus stop `planned_stop`,
  headless Telegram 44 green, real-registry probe under an isolated
  `PI_CODING_AGENT_DIR`: `/help` finalized, plain-turn echo with 12 db
  rows, native `/new`, bare list with real paths, `/new-path` re-root
  plus `/switch-path` rebind to the resumed session with child cwd moved
  both ways. Gates plus full suite green (193 files, 2571 tests).
  Artifacts under `verify-tip/`. Note: the doctor helper still expects
  the DEC-084-dissolved `session_turn_leases` table and reports
  UNHEALTHY on healthy tip homes; health was proven direct.

## Fix `1131270`: extension boot composes the production turnRunnerFactory

- Root cause: `extensions/pi-gateway.ts` composed
  `composeGatewayLifecycle({home, platforms})` with no `turnRunnerFactory`
  while the standalone CLI composed one. Stage 9 connected telegram
  `guard_unwired` and ingress kept the `no guard attached` throw, so the
  bot answered nothing on build `5640884`.
- Fix: one shared `ChatProcRegistry` per extension process plus one
  `RpcTurnRunner` closing over the lifecycle-owned stage-6 store, mirroring
  `runCommand`. Children share the gateway home. Lazy import keeps
  platform-less boots adapter-free. No new data shape: `TurnRequest`
  (sessionId, routingKey, text) in, `TurnOutcome` out.
- Checks: `tsc` clean, layering plus secret-scope clean, targeted suites
  (rpc-turn-runner plus entrypoints, 69 tests) green, full suite green
  (191 files, 2543 tests). Committed as `1131270`, pushed to origin.

## Swap `5640884` → `1131270` plus live proof (2026-10-07)

- Installed checkout fast-forward only, `5640884` → `1131270`. Old pid
  `877020` SIGTERM'd, exited cleanly, state `planned_stop`. Relaunched on
  the identical production boot path with the snapshotted live env (held in
  memory, never printed). New pid `893992`, `gateway READY`, code_sha
  `113127069a`, `platforms=[telegram]`. Log line: `platform adapter
  telegram guard wired` (was `guard_unwired`). Zero `409`/conflict lines.
- Live Telegram proof (user Irell @irellzane id=8469032365 → ZanishPiBot):
  `/help` answered natively via `get_commands` (msgs 3379–3386, opens clean
  with `` `/copyfile` `` head line, MarkdownV2 intact); plain turn `proof
  ping reply with the word banana` answered `banana` (msg 3388, model turn
  NOT policy-blocked); `state.db` messages 30 → 34 with user plus assistant
  rows under session `tg:8469032365`; `/new` answered `Started a new
  session (01a115d6-dcb5-7672-afdf-592c5805ecdb).` (msg 3390, native
  reset). No blockers. MarkdownV2 comment: help opens clean, no escape
  garbage.

## Archive: live swap to RPC-rewrite build (2026-10-07)

## Swap

- Installed checkout `/root/.pi/agent/git/github.com/Zane-dev16/pi-gateway`:
  fast-forward only, `36a4a86` → `5640884` (5 commits: `592c2be` through
  `5640884`). One pre-existing dirty `package-lock.json` backed up to
  `/tmp/pi-gateway-package-lock-dirty-backup.json`, restored to HEAD, then
  `git merge --ff-only origin/main`. No force.
- Live gateway: old pid 357413 (code_sha `36a4a86`) SIGTERM'd, exited in ~2s,
  state `stopped`/`planned_stop`. Relaunched on the identical production boot
  path (`PI_GATEWAY_AUTO_START=1`, `PI_HOME=/root/.pi`, cwd installed
  checkout, `tail -f /dev/null | exec pi --mode rpc`), stdout/err to
  `/root/.pi/gateway-live.log` (new; no state files touched).
- New pid 877020, `gateway READY` in ~1s, state `running`, code_sha
  `5640884e4c`, `platforms=[telegram]`. Zero `409`/conflict lines in the
  launch log. Doctor: pid alive, pid matches state file, 12 tables ok;
  helper still prints UNHEALTHY only because it looks for READY in its own
  disposable-home launch-log path, not the live log.

## Live Telegram proof (user Irell @irellzane id=8469032365 → ZanishPiBot)

- `/help` (msg 3375, 09:42:29Z): NO REPLY after 6+ min.
- Plain `ping proof` (msg 3376): NO REPLY after 45s+.
- `/new` (msg 3377): NO REPLY after 45s+.
- `state.db`: messages 30 rows, MAX(timestamp) pre-swap (Oct 5); sessions 1
  (pre-existing); delivery_obligations 0. No new rows from any proof.
- Launch log frozen after READY: zero ingress lines for 3 inbound messages.

## Verdict

- Steps 1–2 PASS. Step 3 BLOCKED. Step 4 (this file + commit) PASS.
- Root cause (read from tree, not guessed): `extensions/pi-gateway.ts`
  composes `composeGatewayLifecycle({home, platforms})` with NO
  `turnRunnerFactory` ("Platforms compose without a factory until Todo 3
  lands"); stage 9 therefore connects telegram `guard_unwired`
  (`gateway-run.ts:414`) and ingress keeps the `no guard attached` throw
  (`kit/base-adapter.ts:248`). The pushed stack built the per-chat registry
  (`fef576e`) and the RPC turn runner (`53cc407`) but never composed the
  production factory, so NOTHING Telegram-side can answer — not even /help.
- What unblocks: compose the production `turnRunnerFactory` (RPC per-chat
  registry + message handler) in the extension boot path, relaunch live,
  re-run this file's three proofs. MarkdownV2 formatting comment: unprovable
  until /help answers.

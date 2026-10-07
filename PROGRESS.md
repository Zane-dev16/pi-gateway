# PROGRESS — live swap to RPC-rewrite build (2026-10-07)

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

# XREF-BASELINE.md — Upstream Comparison Baseline for Conformity Audit

**Task label:** upstream sync
**Date:** 2026-08-25 (audit session)
**Ruling:** auditors compare against **`/tmp/hermes-upstream`** (user directive), citing `file:symbol` there. The local reference at `/usr/local/lib/hermes-agent` is an older snapshot; where it differs from the baseline, the baseline wins.

---

## 1. Upstream reachability

| Item | Value |
| --- | --- |
| Upstream reachable | **YES** (GitHub clone succeeded) |
| Clone source | `https://github.com/nousresearch/hermes-agent` |
| Baseline location | `/tmp/hermes-upstream` |
| Upstream HEAD | `1bbb6e5bce56e721ab685af4cd87df21bbff4d35` |
| HEAD subject | `docs(skill): troubleshoot stale web_extract pages — cache carveouts + cache_exempt_hosts` |
| Clone depth | initially `--depth 80`, widened via `git fetch --depth=700 origin main` |

## 2. Local reference version state

| Item | Value |
| --- | --- |
| Local path | `/usr/local/lib/hermes-agent` |
| Is a git repo | **YES** (not merely a snapshot install) |
| Local HEAD | `8e475ed27b1199b8d0bbf094cf2e15fcd555f8cf` |
| HEAD date | 2026-08-22 15:07:05 +0530 |
| HEAD subject | `refactor(terminal): extract _current_session_key() helper for session-key lookups` |
| Working tree | clean (`git status --porcelain` empty) |
| Relationship to upstream | `8e475ed` is a **direct ancestor** of upstream HEAD (merge-base = local HEAD) |
| Drift distance | upstream is **686 commits ahead** of the local snapshot |
| Local origin remote | `https://github.com/NousResearch/hermes-agent.git` (same project, no fork divergence) |

Conclusion: the local install is a pinned, unmodified checkout of upstream at `8e475ed`. All observed drift is pure upstream forward progress — zero local patches.

## 3. Directory drift tables

Method: `diff -rq <local-dir> /tmp/hermes-upstream/<dir> -x .git -x __pycache__ -x venv -x node_modules -x '*.pyc'`. Changed-line count = added+removed non-marker lines in unified diff.

### 3.1 `gateway/` (56 local .py files → 59 upstream)

| File | Status | ± lines | Materiality | What changed |
| --- | --- | --- | --- | --- |
| `gateway/run.py` | changed | **889** | MATERIAL | obligation claim/redelivery (`_claim_pending_obligations`, `_redeliver_claimed_obligations`, `_redeliver_pending_obligations`), supervised respawn with backoff (`_supervised_backoff`, `_spawn_supervised`, slow-reconnect-watcher respawn), boot-send awaiting (`_await_startup_boot_sends`), late background-failure logging, SessionDB recovery hooks |
| `gateway/session.py` | changed | 248 | MATERIAL | recoverable per-path SessionDB handle caches (`_open`/`_close` recovery closures), `_reconcile_recovered_routing_locked`, transcript drain lock, new `rewind_session` |
| `gateway/platforms/base.py` | changed | 181 | MATERIAL | docker workspace/media path resolution now session-key-scoped: `_docker_sandbox_dir_candidates(session_key)`, `_default_docker_workspace_host_roots()`, and signature change `validate_media_delivery_path(path, session_key="")` |
| `gateway/platforms/signal.py` | changed | 156 | MATERIAL | signal adapter behavior updates |
| `gateway/slash_commands.py` | changed | 144 | MATERIAL | slash command surface changes |
| `gateway/platforms/api_server.py` | changed | 92 | MATERIAL | API server behavior updates |
| `gateway/config.py` | changed | 82 | MATERIAL | config schema additions |
| `gateway/control_socket.py` | **added upstream** | +545 (new file) | MATERIAL | new control-socket module (unix socket path handling, `sun_path` limit logic) |
| `gateway/session_db_recovery.py` | **added upstream** | +189 (new file) | MATERIAL | recoverable per-path SessionDB handle caches |
| `gateway/media_repair.py` | **added upstream** | +213 (new file) | MATERIAL | repairs model-mangled `computer_use` screenshot paths before delivery validation |
| `gateway/kanban_watchers.py` | changed | 45 | likely material | watcher updates |
| `gateway/platforms/bluebubbles.py` | changed | 40 | material | adapter updates |
| `gateway/yuanbao.py` (`platforms/`) | changed | 33 | material | adapter updates |
| `gateway/pairing.py` | changed | 33 | material | pairing flow updates |
| `gateway/authz_mixin.py` | changed | 29 | material | authorization updates |
| `gateway/platforms/webhook.py` | changed | 23 | material | webhook updates |
| `gateway/readiness.py` | changed | 16 | possibly cosmetic-to-minor | readiness probe updates |
| `gateway/platforms/weixin.py` | changed | 14 | minor | adapter updates |
| `gateway/status.py` | changed | 9 | minor/cosmetic | status reporting tweaks |
| `gateway/shutdown_forensics.py` | changed | 7 | minor/cosmetic | forensics tweaks |
| `gateway/shutdown_watchdog.py` | changed | 6 | minor/cosmetic | watchdog tweaks |

Removed files: **none** (no "Only in /usr/local/lib" entries). Net: +3 modules upstream.

### 3.2 `plugins/` incl. `plugins/platforms/`

| File | Status | ± lines | Materiality |
| --- | --- | --- | --- |
| `plugins/platforms/telegram/adapter.py` | changed | **309** | MATERIAL — flood-cap result path, polling-stall detection (`_check_polling_stall`), reconnection wait (`_wait_for_reconnection`); removed loop-blocked diagnostics dump |
| `plugins/memory/openviking/__init__.py` | changed | 142 | MATERIAL |
| `plugins/kanban/dashboard/plugin_api.py` | changed | 86 | MATERIAL |
| `plugins/platforms/teams/adapter.py` | changed | 32 | material |
| `plugins/memory/hindsight/__init__.py` | changed | 32 | material |
| `plugins/memory/openviking/README.md` | changed | 17 | cosmetic |
| `plugins/platforms/wecom/adapter.py` | changed | 12 | minor |
| `plugins/platforms/discord/adapter.py` | changed | 12 | minor |

### 3.3 Verdict on drift

**Drift is MATERIAL (behavioral), not cosmetic.** Evidence:

1. Three brand-new behavioral modules exist only in the baseline: `gateway/control_socket.py`, `gateway/session_db_recovery.py`, `gateway/media_repair.py`.
2. New delivery-reliability machinery in `run.py` (obligation claiming/redelivery, supervised respawns with backoff) directly touches gateway lifecycle semantics that this port replicates.
3. Signature-level API changes with call-site impact: `validate_media_delivery_path(path)` → `validate_media_delivery_path(path, session_key)` in `gateway/platforms/base.py`.
4. Platform adapters (telegram especially) gained reconnection/stall/flood-control paths.

The Hermes Gateway implementation in this repo was built against semantics closer to the local snapshot (`8e475ed`); auditors must therefore cite the **baseline** (`/tmp/hermes-upstream`) but flag any conformity finding whose relevant code region appears in §3.1/§3.2 drift tables as potentially version-sensitive.

## 4. Auditor protocol

1. Cite `file:symbol` under `/tmp/hermes-upstream` (e.g. `/tmp/hermes-upstream/gateway/delivery_ledger.py:DeliveryLedger`). Unchanged files are identical byte-for-byte in both trees, so citations transfer safely.
2. If the cited symbol/file is listed as drifted in §3.1 or §3.2, note the local variant exists at `/usr/local/lib/hermes-agent/<same path>` and may differ; prefer baseline semantics unless the audit scope pins an older commit.
3. Do not cite the three upstream-only modules (`control_socket.py`, `session_db_recovery.py`, `media_repair.py`) as *required* for parity unless the audit's normative docs demand them — they postdate the build directive's evidence base (`gap-audit.md` / `CONVERGENCE.md` were produced against the older snapshot).
4. This report makes no claims about files outside `gateway/` and `plugins/`; other trees (e.g. `agent/`, `cli.py`) were not diffed and are presumed out of audit scope.

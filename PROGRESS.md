# PROGRESS — Pi Gateway build

Tracking protocol per EXECUTION-PROMPT: before phase N starts, phase N−1 exit
criteria are re-verified and recorded here. Reports stay compact; artifacts carry
detail. Orchestrated via multi-agent workflows; commits are atomic per workstream.

## Status board

| Phase | Name                              | State        | Exit evidence |
| ----- | --------------------------------- | ------------ | ------------- |
| 0     | Runtime spike                     | DONE ✅      | 46/46 tests green (×3 + independent rerun); DEC-023 verification appended; spike retired at Phase 2 exit (report: `reports/PHASE-0-SPIKE-REPORT.md`) |
| 1     | Spine                             | DONE ✅      | 329/329 tests; all exit criteria measured PASS; `reports/PHASE-1-REPORT.md` |
| 2     | Streaming, obligations, registry  | DONE ✅      | 678/678 incl. 349 new (632 after spike retirement); both fake adapter shapes; derivation property; injected-clock caps; `reports/PHASE-2-REPORT.md` |
| 3     | Reference adapters + conformance  | DONE ✅      | 873/873 (74 files), tsc clean, layering clean; all three adapters pass ALL applicable §8 rows; ws gate `allApplicablePassed === true`, zero deferred (DEC-032); DEC-033/034 satisfied by execution; `reports/PHASE-3-REPORT.md` |
| 4     | Security + multiplex              | DONE ✅      | 1204/1204 tests; grep gate clean + provably failing; poisoned-env/sig-matrix/kill-holder/exactly-once-restore all PASS; `reports/PHASE-4-REPORT.md` |
| 5     | Embedded services + update        | DONE ✅      | 1694/1694 ×2 runs; all (a)–(f) measured PASS incl. two-profile fleet drill w/ stale-gateway exit 1 + receipts-on-refusal; `reports/PHASE-5-REPORT.md` |
| 6     | Census ports                      | DONE ✅      | 31 surfaces PORTED + honest exclusions (DEC-043 dingtalk); 2152/2152 ×2 runs; census table closed; `reports/PHASE-6-REPORT.md` |

## Phase 0 — Runtime spike

Entry check: docs 01–08 stable v1.0 candidate; DEC-001..022 ratified; DEC-023
already logged (2026-08-23). ✔ (spec dir verified stable-candidate)

Goal: prove the three riskiest mechanisms in TypeScript on this runtime —

1. Two-layer turn lease (02 §5 / DEC-004): in-process registry +
   cross-process DB lease keyed on compression-lineage root resolved
   in-transaction; dead-PID reclaim; TTL 300s; exactly-one winner under N racers.
2. Stream consumer parity (04 §5.2): prefix-stable drafts, `finish()` final,
   `_interim_send` popped at BOTH egress doors, seal-interception, non-prefix
   draft mutation detected.
3. WAL ladder (02 §1.1): WAL open, busy_timeout, step-down on SQLITE_BUSY,
   two-process writer/reader contention survives.

Spike code lives under `spike/` only (throwaway — never ships). Test shapes port
into Phases 1–2 suites. On exit: attach passing runs to DEC-023 verification entry
in `../09-open-questions.md` (append-only).

- Phase 1 completion (2026-08-23): workflow `pi_gateway_phase_1_completion` closed
  all four blocked threads — guards (59 tests: sync-install interleave⇒ONE turn,
  stale-lock no-heal-without-owner, drain boundary, cap32, forged-events both
  guards, L1×L2 lease interplay incl. two-process SIGKILL reclaim), resolution
  contracts (70 tests: single-flight N→1, adopt-before-mint, WhatsApp alias-flip
  convergence, participant re-key; fixed alias-walk seed bug), agent_core runner
  on real pi SDK loop (41 tests incl. cache-stability byte-identical system prompt
  - toolset hash across consecutive turns). FINAL: 329/329 green, tsc clean,
  layering exit 0. DEC-027/028 logged pre-merge. Known hygiene item: transient
  EPIPE in spike/tests/lease.spike.test.ts teardown under full parallel load
  (Phase 0 throwaway; disappears when spike/ is retired after Phase 2 ports its
  shapes).

- Phase 5 / update pipeline (2026-08-24): `src/pi_embedded/update/**` — the
  transactional update subsystem (08 §5–§10): plan stage (deployment-kind
  classification from code-scoped `.install_method` stamp → .git → package.json,
  config.py:detect_install_method parity; UpdatePlan/RuntimeRecord schema),
  snapshot stage (#66140 per-profile state-snapshots, identical critical set,
  1 GiB cap skip-with-reason, keep=1 prune with pruning suppression on protected
  skips, zeroed-db guard + post-copy quick_check), apply stage (argv/git-CLASSIFIED
  failure gates; ZIP fallback strictly git-classified AND win32; #87304 double
  dirty-tree refusal up-front + TOCTOU with staging-artifact filter and -uall;
  two-phase staging swap preserving .git/.env/node_modules; built-artifact graft),
  restart-per-kind (fleet-wide drain-first SIGUSR1, launchers resolved before any
  signal, per-unit isolation, survivors stopped after window, fail-closed verdict
  table `_restart_phase_failure_is_incomplete` ported verbatim), verify stage
  (settle ~2s ONLY after actual restarts on injected clock; fleet sha matrix;
  stale ⇒ partial + exit 1; unknown never fails), receipts (JSON receipt +
  atomic latest.json pointer on EVERY terminal path incl. refusals/exceptions;
  bounded prune keeps 20), canonical process matchers (08 §9: token-based
  gateway/holder subcommand extraction, parser-DERIVED value-flag set from a
  single option-spec table, /proc cmdline enumeration; adversarial argv matrix),
  SIGHUP hangup protection (DEC-042: window listener-absorption + trap-wrapped
  child exec delivering true inherited SIG_IGN to git/package-manager children).
  Tests: 84 new across 10 files — 64 pure contracts + 20 two-process contracts
  (real git pull/diverge/overlay drills; SIGUSR1 fleet drain on spawned units;
  SIGHUP survival across two exec hops + /proc SigIgn evidence; TWO-PROFILE
  stale-gateway drill ⇒ partial + exit 1 per roadmap exit criteria c/f).
  FULL: 1694/1694 green, tsc clean, layering + secretscope gates green.
  DEC-042 logged pre-implementation (Node cannot express SIG_IGN in-process;
  measured caveat recorded: Node resets its own inherited SIGHUP at bootstrap,
  so the binding property targets non-Node children). Layering note: update owns
  a reader for the DOCUMENTED gateway_state.json schema instead of importing
  pi_gateway/lifecycle (01 §5.3); shared contract is the spec field set.

## Log

- STABILITY FIX CLUSTER `simplex-r2` (2026-08-25): round-2 findings spx-1..spx-5
  applied toward Hermes truth (/tmp/hermes-upstream plugins/platforms/simplex/
  adapter.py anchors). spx-1 image/video doors (send_image/send_image_file/
  send_video @~1010-1105): sendImage accepts file:// (unquote) and http(s)
  (cache seam → default fetch+tempdir port of base.py:cache_image_from_url),
  PNG-prepares via an injectable_prepare_image seam (default = Hermes'
  no-converter fallback posture: original bytes, empty thumb, logged), ships
  the correlated '/_send @id|#gid json [{filePath,msgContent:{type:"image",
  image:<thumb-data-uri>,text}}]' shape byte-exact (awaited reply; null ⇒
  "Failed to send image"), video routes send_document, +sendMultipleImages so
  the post-stream rescan lane stops skipping images. spx-2 listChannels
  (:870-935): correlated '/contacts'+'/groups' (10s bounds) parse display-
  name/id-only/[groupInfo,pair] forms into {id,name,type} entries; disconnected
  or unresponsive ⇒ NULL (never []) so the directory keeps prior targets;
  failed /groups tolerated with contacts kept; false "list_channels out of
  scope" header claim DELETED (no report backs it). spx-3 __init__ env
  resolution (@157-176): SIMPLEX_AUTO_ACCEPT ('0/false/no/empty'⇒false,
  case-insensitive, set-but-empty disables even over an injected true; env
  beats opt), SIMPLEX_GROUP_ALLOWED comma-split via _parse_comma_list parity
  (set-but-empty falls back to opt like Python `getenv(K,"") or extra.get`),
  HERMES_SIMPLEX_TEXT_BATCH_DELAY seconds through THE batch-delay helper which
  now has a caller (env wins over opt; invalid ⇒ default 0.8s). spx-4
  probeReachability races open_timeout=10 (:296): stalled-handshake daemons
  resolve FALSE at the bound instead of hanging connect(), probe socket closed.
  spx-5 keepalive carrier (_ws_listener ping_interval=20/ping_timeout=20):
  SimplexConnection gains optional ping() + listener onPong(), adapter-side
  wsKeepaliveLoop pings every 20s, a ping unanswered past 20s closes 1011
  "ping timeout" into the SAME ladder (websockets abort parity); FakeSimplexDaemon
  answers pongs 1:1 unless stallPongs() (wedged-zombie shape); health monitor
  stays LOG-ONLY now that the carrier exists; false "websockets client already
  pings" comments corrected in manifest/adapter headers (pi embeds no websockets
  library — the SEAM owns liveness). Tests: five new delta rows
  (image-video-doors / channel-directory / env-config-resolution /
  probe-open-timeout / ping-pong-keepalive) through the real engine fixture;
  pre-existing rows re-proven (keepalive pings bypass the JSON command stream,
  pongs don't mask application-idle posture). Area green (simplex-rows 8/8);
  tsc clean in cluster files; layering+secret gates green; full-suite failures
  confined to OTHER clusters' in-flight areas (qqbot/a2a/weixin mid-edit).

- STABILITY FIX CLUSTER `buzz-r2` (2026-08-25): round-2 findings buzz-1/2/3 applied
  toward Hermes truth (/tmp/hermes-upstream plugins/platforms/buzz/adapter.py anchors).
  buzz-1: THE LIVE NIP-42 WEBSOCKET LOOP PORTED behind the wsStarter establishment seam
  (_websocket_url/_start_websocket/_authenticate_websocket/_subscribe_websocket/
  _handle_membership_event/_websocket_loop) — new relay-wire.ts injects the socket MEDIUM
  (the port never opens real sockets; FakeBuzzCli precedent): AUTH-challenge parse
  (NOTICE/CLOSED/garbage ⇒ ConnectionError ladder), ['AUTH',signed kind-22242] reply via
  nostr-auth buildAuthEvent (+BUZZ_AUTH_TAG), OK-wait keyed on OUR event id (false ⇒
  rejected(reason)), per-channel REQ {'kinds':[9],'#h':[ch],'since':max(watermark,now)−1}
  - membership REQ {'kinds':[44100],'#p':[pk]} (sub ids hermes-buzz-N /
  hermes-buzz-membership), EVENT/CLOSED/NOTICE routing through THE poll-plane handleEvent
  machinery (dedupe/mention-gate/DM-latch identical), live DM rediscovery on kind-44100
  (new subs hermes-buzz-dm-N, fresh watermark ⇒ history dispatches once), 1→30s reconnect
  backoff with disconnect-interruptible sleeps + post-auth reset, ready window 20+5s ⇒
  teardown (auto falls back to POLL; pinned websocket fails FATAL ws_auth_failed retryable
  — source truth); no factory wired keeps the pre-loop posture. FakeBuzzRelay VERIFIES
  auth events independently (NIP-01 id recompute + spec BIP-340 verify over exported
  primitives). buzz-2: resolveCliPath ~/bin/buzz fallback EXPANDS (adapter.py Path.home()/'bin'/'buzz'
  parity) — probed AND returned expanded; literal tilde argv[0] gone. buzz-3: runCli races
  the executor against BUZZ_CLI_TIMEOUT_S=30 returning rc124 {"error":"timeout",
  "message":"buzz <sub> timed out after 30s"} kill-parity (_exec_buzz asyncio.wait_for) —
  AbortSignal handed to executors for cooperative kill, abandoned calls contained, loser
  timers cancelled (injected-clock deterministic). Tests: resolveCliPath row re-pinned to
  conforming expansion; NEW delta rows ws-live-handshake-subscription / ws-event-routing-dedupe /
  ws-kind-filter-parity (SECOND mutant detector — gate updated to two detectors, one per
  inbound lane) / ws-closed-reconnect-backoff / ws-membership-live-dm-discovery /
  ws-auth-failure-ladder / cli-executor-timeout-race. Buzz area 11/11 ×2; tsc clean in cluster;
  layering + secret gates green; full-suite failures confined to foreign in-flight areas
  (qqbot/signal — proven independent by stash-run). No DEC required: implementation REMOVES
  the divergence (medium-only substitution documented in manifest exclusions #1 rewrite).
- STABILITY FIX CLUSTER `photon-r2` (2026-08-25): round-2 findings ph-1..ph-5 applied
  toward Hermes truth at /tmp/hermes-upstream anchors (plugins/platforms/photon/
  adapter.py + gateway/platforms/base.py). ph-1: sendClarify renders base.py:send_clarify's
  fallback BYTE-EXACT — '❓ {question}\n\n  1. A\n  2. B\n\nReply with the number, the option
  text, or your own answer.' (two-space indent, blank-line framing, reply trailer) and bare
  '❓ {question}' open-ended; conformance row upgraded from contains() to exact-bytes
  assertions. ph-2: sendWithRetryPhoton restores the ONE plain-text fall-through resend
  (_sidecar_send richlink=False markdown=False, adapter.py @2478) for BOTH exhausted retries
  AND non-network non-timeout non-permanent failures (previously !isNetwork returned as-is);
  DEC-046 timeout veto preserved verbatim (classified timeouts return as-is, no retry no
  resend) and permanent classes still never double-send. ph-3: new exported
  richlinkUrlFromContent mirrors_richlink_url_from_content RECURSION into group items
  (text/richlink item content), so multi-item group messages arm the 30s preview-suppression
  window (recordRecentRichlink collapsed to the Hermes_record_recent_richlink signature;
  call site = candidate ?? text). ph-4: SidecarTransport.call body now OPTIONAL — /probe and
  the startup /healthz readiness ping ride HEADERS-ONLY POSTs (_probe_once @~1869 /
  _start_sidecar wait @~1720); FakeSidecarServer records body=undefined for them (watchdog
  /healthz monitor keeps its JSON body per adapter.py @1109). ph-5: buildSidecarSpawnCommand
  envVars gain PHOTON_PROJECT_ID/PHOTON_PROJECT_SECRET/PHOTON_SIDECAR_TOKEN (adapter.py
  @~1629 env assembly, token per @750 scoped-secret-or-token_hex) as REQUIRED spawn-time
  injected params — the exported child contract can now authenticate a sidecar. Tests:
  photon-rows conformed (byte-exact clarify render, non-network-resend leg, body-less
  probe/healthz pins) + NEW rows photon.group-richlink-suppression (group URL arms window,
  in-window group preview swallowed / out-window dispatched / url-less group arms nothing)
  and photon.spawn-contract-data (six-key env assembly incl. default port). Suite:
  photon-rows 5/5 + photon-send-effect 3/3 green ×3 runs; tsc clean for all photon paths;
  secret gate OK; layering violations confined to sibling buzz cluster artifacts
  (dbg-ws.test.ts UNPLACED_LAYER), full-suite failures proven independent via git-stash of
  this cluster's files (buzz/weixin rows fail identically without them).

- STABILITY FIX CLUSTER `telegram-wire-r2` (2026-08-25): round-2 findings tg2-1..tg2-12
  applied toward Hermes truth (adapter.py anchors). tg2-1 "message is not modified"
  edits map to success no-ops on EVERY lane (mid-stream raw / finalize raw / finalize
  converted) so REQUIRES_EDIT_FINALIZE redundant finalize edits can never route into
  sendFallbackContinuation full-text duplicates (:5737/:5757/:5929); proven at BOTH
  adapter level AND through a live GatewayStreamConsumer turn (1 send, no duplicate).
  tg2-2 resolved taps edit host TEXT via editMessageText(format_message-style MDV2,
  parse_mode=MarkdownV2, reply_markup=null) instead of reply-markup-only (:7280/:6687).
  tg2-3 post-connect housekeeping ported off the connect path (#46298): set_my_commands
  x3 scopes Default/AllPrivateChats/AllGroupChats from THE builtin registry minus
  cliOnly rows (60 cap), lazy BotCommandScopeChat(chat_id) once-per-forum-chat
  (:9645), opt-in set_my_short_description status indicator (:4953), DM-topic
  create/load/rename w/ cache + prune (:3759/:3873/:3957); fake gained setMyCommands/
  setMyShortDescription/createForumTopic/editForumTopic. tg2-4_thread_kwargs_for_send
  family ported (:1552): direct_messages_topic_id/telegram_direct_messages_topic_id
  pair with OMITTED message_thread_id, private fallback lanes anchor EVERY chunk from
  telegram_reply_to_message_id, anchor-less sends FAIL LOUD pre-transmit with the exact
  Hermes refuse error, created_for_send lanes exempt. tg2-5 wireSend payload WHITELIST:
  captured metadata = built args (+reply_markup) ONLY — notify/expect_edits/
  gateway_session_key/final/replyToOverride/_interim_send/raw thread_id strings can
  never leak (send :5397 exact-fields). tg2-6 rich extras behind the existing wireRich
  seam: rich_messages/rich_drafts env gates (default off), eligibility = degrading
  constructs x 32768 code points minus TDesktop crash/garble shapes, RAW markdown via
  richNormalizeLinebreaks, capability latch once / transient never legacy-resent /
  fallback degrades silently, expect_edits skips, sendRichMessageDraft needs BOTH
  extras w/ own latch, eligible finalize edits carry rich_message param (:2229/:2336/
  :2430); probe latch UN-LATCHED when configured. tg2-7 normalizeTelegramChatId
  (telegram_ids.py:23) applied at every bot.* call site (send args, edits, callbacks,
  reactions, typing, media) — numeric ids ship as ints, @username trimmed strings.
  tg2-8 production longPollTimeoutMs default 25s -> 10s (PTB timeout=10 baseline;
  start_polling passes no override :2668). tg2-9 disable_link_previews extra gates
  link_preview_options={is_disabled:true} on sends + rich payloads (:1945).
  tg2-10 voice caption VARIANT LADDER: MDV2-first then plain slice on parse-entity
  rejection; absent captions OMIT the key (never null) incl. sendAudio (#32029).
  tg2-11 edited_message normalizes to message_edited platform events
  (chatId/messageId/threadId/text<=8192/editedAt ISO) fanned out through the SAME
  handler seam as reactions (:4284). tg2-12 media transmissions wrap the DM-topic
  anchor retry ladder (_send_with_dm_topic_reply_anchor_retry :1753): dead anchors +
  topic/thread markers retry once without routing w/ binding prune, flag-gated.
  Telegram shape rows 11 -> EIGHTEEN (+edit-not-modified-noop, post-connect-
  housekeeping, dm-topic-send-routing, wire-arg-whitelist, rich-extras-lane,
  media-dm-topic-retry, long-poll-timeout-default) + extended parse/callback/media
  rows; unit tests for pure helpers. NOTE: rich SEND coverage rides the deliverText/
  formatting-ladder lane only — door-lane finals bypass the ladder by architecture
  (chokepoint transmitSend binds wireSend directly); extending rich to door finals
  would touch core chokepoint topology and is flagged for a follow-up round.
  Area suites GREEN (telegram+polling+rows+stream-consumer 91/91); commit
  `3c37c13` applied mid-flight after a concurrent `git stash` reset wiped the shared
  tree (stash popped cleanly, zero conflicts). NOTE: the shared index had been
  fully staged pre-stash, so that commit necessarily carries every cluster's
  in-flight tree state under this cluster's message — attribution is per-PROGRESS
  entry, not per-commit boundary. Full suite at commit time: 2563 passed / 47
  failed, ALL failures inside OTHER clusters' mid-flight areas (weixin x20,
  qqbot x12, webhook-conformance x2, plus gchat-file-level); telegram/polling/
  streaming/conformance-telegram ZERO failures; layering + secret gates green.

- STABILITY FIX CLUSTER `msgraph-webhook-r2` (2026-08-25): round-2 findings
  msgraph-1/2/3 applied toward Hermes truth (/tmp/hermes-upstream
  gateway/platforms/msgraph_webhook.py anchors). msgraph-1_render_template
  (:420): per-segment resolution mirrors dict.get(part, sentinel) — a missing
  segment keeps the literal {key} at ANY depth incl. the FINAL one (was
  String(undefined) ⇒ "undefined"; own-property miss, prototype names never
  resolve); explicit null renders str(None) ⇒ "None" (was "null"); mid-path
  null/non-dict leaves keep the literal; dict/list placeholder values render
  json.dumps[sort_keys=True](:2000) bytes — ', '/': ' separators + ensure_ascii
  \uXXXX escaping (were compact JSON.stringify). msgraph-2_handle_health (:246
  - config.py:348): /health platform field emits the Platform ENUM VALUE
  "msgraph_webhook", not the hyphenated manifest name. msgraph-3
  _build_message_event (:390): sha1 receipt-id fallback hashes BYTE-PARITY
  canonical bytes — json.dumps(notification, sort_keys=True) with default
  separators + lowercase surrogate-pair-aware ASCII escaping (was compact
  JSON.stringify), so id-less notifications mint baseline-identical ids.
  Tests: prompt-rendering row re-pinned to conforming bytes + template-edge
  matrix (sentinel / None / mid-path-null / \uXXXX escaping);
  notification-dedup row pins the cross-language golden id (python3-computed
  sha1:8a4fdf82…); NEW delta row health-endpoint (enum value + live counters +
  CIDR gate) — shape rows 9→10. Area green; full-suite failures confined to
  sibling clusters' in-flight files (telegram/feishu, concurrent edits); tsc
  clean in cluster files.
- STABILITY FIX CLUSTER `google-chat-r2` (2026-08-25): round-2 findings gchat-1/2/3/5/6/7
  applied toward Hermes truth (google_chat/adapter.py anchors). gchat-1 patchEgress
  (_patch_message @2346): GchatTransport.patchMessage gains updateMask — mask computed
  from the shipped fields ('text'+'cardsV2' comma-joined, default 'text'), thread
  stripped from the patch body (immutable), wireSend chunk-0 + wireEdit + retirement
  patches all ride the mask; the fake REJECTS thread-carrying patch bodies and records
  every mask so rows assert it. gchat-2 createMessageEgress (_call_with_retry @2538):
  EVERY outbound create (send/sendCard/sendTyping/404-fallback) retries ≤3 attempts
  over {429,500,502,503,504}+transport throws with jittered exponential backoff
  (1s→8s cap, GCHAT_RETRY_* manifest constants now live); timeout-CLASSIFIED outcomes
  NEVER retry per DEC-046; exhaustion falls to classifyEgressFailure unchanged.
  gchat-3 onProcessingComplete(event,outcome) (@2775-2810): unclaimed typing markers
  patch to '(no reply)'/'(interrupted)' by outcome, race-orphan cards (sendTyping
  losing the slot claim) to '·', consumed slots no-op; dispatchMessage's containment
  catch retires stranded markers for rejected deliveries. gchat-5 noteCreatedThread
  (@2619-2634): create-response thread.name bumps the thread-count store so bot-created
  threads classify as SIDE threads on later user engagement. gchat-6: the chunk-0
  typing-patch 404 fallback recreates with the ORIGINAL pre-built thread-less body
  VERBATIM — no re-added thread:{name}, no messageReplyOption query param (args Hermes
  never sends on that path). gchat-7: sendClarifyPrompt length===1 plainFallback
  deleted — a single surviving choice ships the Other-only clarify card like Hermes
  (@2232). FakeChatApi models vendor edges: creates ALWAYS resolve thread.name,
  {kind:'throw'} scripts transport errors; fixture drives backoff on the injected
  clock (retrySleep advances it, jitter pinned to 0). Shape rows TEN→TWELVE
  (+outbound-retry-ladder, +end-of-turn-typing-retirement); verdict-ladder row now
  scripts FULL exhaustion for retryable statuses (single transient blips now legitimately
  recover mid-ladder). Area suite green (gchat file 6/6 gates incl. allApplicablePassed
  and lying-verifier detection); tsc clean in cluster files; layering+secret gates green;
  full-suite failures confined to OTHER clusters' in-flight areas (feishu/discord/slack/
  telegram scratch probes) — google-chat closure imports nothing outside itself.

- STABILITY FIX CLUSTER `matrix-r2` (2026-08-25): round-2 findings ws-4/ws-5 applied
  toward Hermes truth (base.py:6418/_process_message_background + matrix adapter.py
  anchors). dispatchOrHold now wires the full per-turn processing lifecycle for every
  dispatched sync event: sendTyping (set_typing timeout=30000) fires once the event is
  admitted AND past the hold gate, onProcessingStart puts 👀 (m.reaction) before
  handleIngress, and on settle onProcessingComplete swaps eyes→✅/❌ (failure on
  contained dispatch error; cancelled — eyes redacted, no final emoji, no receipt —
  on AdapterDisabledError hold, the CancelledError analog) followed by stopTyping
  (timeout=0), mirroring base.py finalize ordering and Discord's wired path. Read-
  receipt fall-through semantics preserved exactly (disabled-hold still skips it).
  M_LIMIT_EXCEEDED honor-once retry stays inside sendTyping; held-event redispatch
  drain stays unwired like Discord's sweep (hooks anchor to live dispatch only).
  New engine contract pins typing 30000 → 👀 redacted → ✅ → stop 0 → receipt on a
  live sync turn. Matrix area 26/26; FULL suite 2535/2535 (219 files); tsc clean.
- STABILITY FIX CLUSTER `discord-r2` (2026-08-25): round-2 findings ws-1/ws-2/ws-9
  applied toward Hermes truth (plugins/platforms/discord/adapter.py anchors). ws-1:
  READY dispatch adopts d.user.id as ground botUserId BEFORE any MESSAGE_CREATE can
  dispatch (on_ready :1391 client.user grounding; _is_mentioned :6746) — injected deps
  value demoted to fallback, payload-less READY never downgrades, re-READY re-adopts;
  require_mention `<@botId>` matching + stripSelfMention + self-echo filter now work
  on real gateways without injected identity (resolvedBotUserId getter exposed). ws-2:
  startTyping fires transmitTyping IMMEDIATELY on entry then refreshes every 12s
  (_typing_loop :5605-5637 request-first; zero-length first delay keeps cadence
  registration synchronous ⇒ injected-clock deterministic), 429 survival now sleeps
  the AUTHORITATIVE delay alone straight into the next post (:5626); handleMessageCreate
  starts the indicator at turn admission (before the 👀 emoji round-trip) and stops it
  at finalize/failure (:20444/:21065). ws-9: type-15 forum-parent detection via optional
  REST resolveChannelType (probe-once per-chat cache; lookup failures never reroute;
  _is_forum_parent :7892) routes THE text-send lane through a thread-create post lane
  (_send_to_forum :3593): first chunk POSTs /channels/{forum}/threads with
  deriveForumThreadName (first line, heading strip, 100-char cap, "New Post" fallback,
  :9879-9888) + starter content, follow-up chunks send INTO the new thread by id with
  no reply reference (:3634); missing open-post falls back to creating its own post;
  planes without forum support keep the legacy lane (conformance rows unaffected).
  Tests: typing cadence tests rewritten to CONFORMING immediate-fire contract; new r2
  block pins READY adoption/fallback retention, admission→finalize/failure typing
  lifecycle (gated reaction plane makes the mid-turn window deterministic), forum
  starter+follow-up routing, probe caching, failed-starter fallback, name derivation.
  Discord area 37/37; discord conformance+ws rows green; discord files tsc clean;
  full-suite failures confined to OTHER clusters' in-flight areas (feishu/teams/
  telegram).
- CONFORMITY FIX CLUSTER `telegram-wire-parity` (2026-08-25): adjudicated round-1
  findings tg-1..tg-10/tg-14 applied toward Hermes truth (adapter.py anchors).
  tg-1 getUpdates carries allowed_updates=Update.ALL_TYPES on EVERY poll; the
  fake models real filtering (omitted arg ⇒ default set EXCLUDES reaction kinds,
  so a forgetful poller demonstrably loses A2 ingress). tg-2 send lane emits FULL
  format_message-style MarkdownV2 (structural-only lane deleted; chunk markers
  ship "\\(1/2\\)"); shared row egress.chunk-flood normalizes the escaped marker
  form (platforms with raw bytes unaffected). tg-3 finalize edits stamp
  parse_mode=MarkdownV2 via editTransmit metadata (mid-stream omit). tg-4
  notification mode port (_resolve_notifications_mode: important default /
  all opt-in / unknown⇒default; disable_notification unless metadata notify).
  tg-5 reply_to_message_id 'first'-chunk policy per anchor id + not-found retry
  drops anchor. tg-6 threadIdForSend wired into send args (General '1' omitted).
  tg-7 deleteWebhook(drop_pending_updates=false) on every connect — cold boot
  require-success aborts connect before any poll, reconnect best-effort.
  tg-8 send-class flood inline cap 5s (#91969): ≤5s sleep+retry ≤3 attempts over
  injected clock; >5s fails closed error=flood_control:<wait> with NO retryAfter
  field (a machine-readable one would make the §6.1 ladder verbatim-re-sleep).
  tg-9 media family ported (sendPhoto/sendDocument/sendVoice/sendAudio/sendVideo/
  sendAnimation) w/ notification+thread+anchor kwargs, photo→document fallback,
  voice ext routing (.ogg/.opus→voice, .mp3/.m4a→audio, else document),
  animation→photo fallback, 1024 caption cap. tg-10 typing pinned to action=
  'typing' (variant matrix removed from wire + manifest). tg-14 sticker cache TTL
  defaults to Infinity (exact parity; bogus DEC-043 citation fixed). Telegram
  shape rows 8→11 (send-wire-parity, connect-webhook-clear, media-send-family);
  fake captures full kwargs. Area suites 50/50; full suite 2504/2506 (2 failures
  = other clusters' in-flight files); tsc clean in area; layering+secret gates
  green. Proposed DEC-048 text in cluster report.
- CENSUS CLOSED (2026-08-24): workflow `pi_gateway_census_completion` — 9 family
  agents + verifier. ALL remaining platforms ported behind the gate: 31 surfaces
  PORTED, relay excluded by nature (no Hermes adapter module), local absorbed by
  host loop per DEC-023, dingtalk scope-excluded per DEC-043. Suite
  1842→2152 (+310) ×2 identical runs. Closure caught + fixed a REAL kit defect
  (deliverChunk cached first-delivery chatId closures → cross-chat misrouting)
  and a partial yuanbao delivery (fatal-close heartbeat stop, reply-heartbeat
  lifecycle, ms-vs-s unit bug). DEC-044..047 logged. Zero core diffs.

- DEFINITION OF DONE (2026-08-24): roadmap Phase 6 exit met. Clean-checkout proof:
  fresh `git clone` -> `npm ci` -> `npx tsc --noEmit` clean -> `npx vitest run`
  1842/1842 over 167 files. Conformance suite green on all three reference
  adapters plus Telegram (DEC-024) per EXECUTION-PROMPT DoD; zero unlogged
  divergences (DEC-001..042 all logged w/ verification); DEC-026 sweep clean at
  every phase boundary. Remaining census (19 plugins + signal/weixin/yuanbao/
  qqbot/bluebubbles/msgraph built-ins) enumerated in reports/PHASE-6-REPORT.md
  as post-DoD expansion backlog w/ recommended order (matrix -> signal -> feishu
  -> line/teams/google_chat/msgraph). Flake hardening en route: delegation
  SIGKILL close-wait bounded at 30s w/ D-state diagnostics.

- Phase 4 (2026-08-24): workflow `pi_gateway_phase_4_security` — scope engine first,
  then authz ∥ locks ∥ trust, then multiplex ∥ delegation rail; verifier incl.
  self-tested grep gate. PASS: 1204/1204 (331 new). DEC-035..038 logged. Roadmap
  v1.2 erratum recorded (Phase-4/5 text duplication owned by Phase 5). Flake fix:
  two-process rail test timeout raised to 300s against fork starvation on 4 CPUs
  (contract unchanged; passes in isolation in ~1.5s).

- Phase 3 close (2026-08-24): verifier sweep of the persistent-ws completion thread — real-engine `WsSubject` + five-row transport fixture + 25 engine tests + ws wiring suite; suite 844→873 (+29, +2 files); footprint confined to `src/pi_platforms/{persistent-ws,conformance,kit}/`; lying-fixture negative validation green; no new DECs required. FINAL verdict PASS (`reports/PHASE-3-REPORT.md`).

- Phase 2 (2026-08-23): workflow `pi_gateway_phase_2_egress` — 4 parallel builders
  - verifier. PASS: 678/678 (349 new: streaming both-fake-shape mutation suite,
  obligations caps under injected clock w/ real-process crash recovery, command
  registry derivation property across six consumers, media offset-mask mutation-
  checked). DEC-029/030/031 logged. spike/ retired (shapes ported; EPIPE artifact
  gone); suite now 632/632.

- Session start: repo at LICENSE-only initial commit; toolchain Node v26.7.0 /
  npm 11.19.0 verified; npm registry + git remote reachability confirmed.
- Phase 0 (2026-08-23): workflow `pi_gateway_phase_0_spike` — scaffold + 3 parallel
  proof agents + verifier. PASS: 46/46 (12 lease / 8 stream / 22 WAL contracts +
  4 driver-evidence), tsc clean, footprint confined to spike/ + harness configs.
  Commits `9fc1a20` (scaffold) + `2618c14` (proofs) pushed to origin/main.
  DEC-023 verification entry appended in ../09-open-questions.md. Residuals for
  later phases: SIGHUP→SIG_IGN installability, ws transport, Windows-gated test
  variant.
- Phase 1 / lifecycle skeleton (2026-08-23): `src/pi_gateway/lifecycle/**` —
  binding ten-stage startup engine (01 §3.1; optional stages 7–9 degrade loudly
  per-service, required abort), duplicate-instance guard (PID file O_EXCL,
  SQLite-held runtime lock, getRunningPid evidence chain), takeover handshake
  (marker-before-SIGTERM, ≤10s @0.5s, force+confirm, give-up cleanup), boot
  fingerprint + code_skew detect, gateway_state.json stamps (08 §4 field set),
  shutdown classes takeover/planned/unexpected (#42675 stop-persist suppression,
  exit codes 0/0/1), ordered drain with flush-before-clear backstop +
  pending_messages recovery files + replay-on-boot, double-signal fast-exit
  releasing locks pre-exit. Layering lint `scripts/check-layering.mjs` wired as
  npm `check:layering`. Tests: 49/49 lifecycle (stages 12 · guard 16 ·
  two-process takeover 2 · shutdown 12 · layering gate 7); full repo 159/159;
  tsc clean. Proposed DEC-027: runtime lock held as open BEGIN IMMEDIATE on a
  dedicated `<home>/gateway.lock.db` sidecar (Node core has no flock; new lock
  deps suspect per 01 §5.2) — semantics identical to fcntl locks (live-process
  ownership, OS auto-release on death).

- Conformity round-1 fix cluster `core-spine-streaming-ops` (2026-08-25): 35 findings,
  4 parallel ownership-partitioned builders (spine / streaming / lifecycle-ops /
  embedded-security). Spine: turn-lease L1+DB prologue wired into runTurn (ttl 300/wait
  1800/60s refresh/finally release, waited⇒tip re-resolve), unlimited default turn limit
  (TURN_LIMIT_UNLIMITED sentinel), boundary-respecting replay dedupe + _branched_from
  ancestor gate, 3s Telegram TEXT follow-up grace in busy ladder, pre-request tool-call
  sanitation repair family. Streaming: silence suppression on both lanes + partial-marker
  hold-back, 99-row frozen builtin command registry wired at stage-8, expect_edits stamping,
  metadata on stream edits, length-aware splits + fallback-final continuation, consumer-side
  think scrubber, media roots (profile caches + kanban attachments), atomic dead-target
  flush. Ops/lifecycle: SIGUSR1⇒graceful drain exit 75 (two-process verified), stage-9
  adapter seam, boot choreography (.clean_shutdown/unclean recovery/stuck-loop suspension/
  restore gate), drain phases (notify-active + resume-pending pre-teardown), forensics
  snapshot + shutdown watchdog (drain+60s) + loop-liveness 3-strike exit 75, exits 75/78 +
  supervisor/container detection + cron-drain clamp, drain_request marker watcher (epoch/
  3600s max-age), failed-platform queue + supervised reconnect watcher. Embedded: kanban
  notifier service (5s tick, terminal events, cursor advance, unsubscribe-on-archive,
  3600s GC), agent-cache 128/3600s defaults + protectRecent=8/pass-cap 16 + 300s sweep
  wiring, update outcome "refused" (exit 2) + full latest.json payload, cron approvals
  deny-mode without pending prompts, authz allowAdapterDelegation flag through gates
  1/6/8. DEC-053 (in-process obligation retry scheduler) + DEC-054 (CAS-guarded ledger)
  logged post-hoc per DEC-026. Suite 2152→2530 (+378 contracts, 199→219 files); tsc clean;
  layering + secret gates green.

- Stability round-2 fix cluster `line-r2` (2026-08-25): LINE webhook-adapter moved onto
  Hermes truth at /tmp/hermes-upstream anchors. line-1: `_is_system_bypass` parity
  (adapter.py:656-667/:1185-1188) — ⚡ Interrupting / ⏳ Queued / ⏩ Steered / 💾 busy-acks
  bypass the pendingButtons cache and ride Reply/Push while a button is PENDING; slot stays
  armed for the tap. line-2: splitsLongMessages=True inheritance declared in
  LINE_CAPABILITIES (LineAdapter overrides neither flag upstream) so the base kit split
  never runs; dispatchBubbles' split_for_line is THE native splitter on every lane — ONE
  Reply/Push call of ≤5 ellipsis-tailed bubbles (:1197/:1210), no lossless kit-chunked
  multi-push; per-chat utf16 budget descriptor removed (source has no per-chat budgets);
  kit lossless-split shared rows (chunk-flood/per-chat-length-pair) excluded BY THE PROBE
  from new manifest datum LINE_NATIVE_SPLIT_TRUNCATES (BB_SUPPORTS_MESSAGE_EDITING
  precedent), replaced by delta row single-call-five-bubble-cap. line-3: sendTyping
  override re-fires POST /v2/bot/chat/loading/start from the processing heartbeat
  (send_typing :1240) via invokeLoading (U-guard + clamp + swallow). Tests: 8→11 shape-delta
  rows (system-ack-cache-bypass / single-call-five-bubble-cap / typing-refresh-loading),
  lie-scan kept truthful (bypass row pinned to its own chat id off the phantom-push mutant).
  line-rows 7/7 ×3 runs; kit+mutant+self-test 115/115; tsc clean for src/pi_platforms/line.

- Stability round-2 fix cluster `slack-r2` (2026-08-25): Slack adapter moved onto Hermes
  truth at /tmp/hermes-upstream anchors (all five findings, no DEC deltas needed — every
  change is a convergence toward reference behavior). ws-6: transmitReaction rest-extra
  (reactions.add/remove {channel,timestamp,name}, adapter.py:_add_reaction :4217 /
  _remove_reaction :4233) + SLACK_REACTIONS-default-true lifecycle (:4250/:4252/:4265)
  wired around dispatch in handlePlatformEvent — 👀 on the triggering message ts at
  processing start, removed + white_check_mark/x at success/failure completion; env
  parser isSlackReactionsEnabled ("false"/"0"/"no" disables; deps.reactionsEnabled
  override for config fold). ws-7: chat.startStream START args carry recipient_user_id
  (metadata user_id/sender_id renamed, originals stripped) and recipient_team_id from
  the channel→team map (_remember_channel_team :1199 fed by inbound events, falling back
  to the connect-time auth scope); pre-wire guard fails unanchored START frames with
  "no thread_ts for native stream" BEFORE any API call (:3196-3213) without engaging the
  A23 feature latch. ws-8: authTest rest-extra invoked at connect BEFORE socket-up
  (client.auth_test :1968) resolving selfUserId (token wins over 'bot-self' seed;
  explicit deps.botUserId injection stays) + primaryTeamId; failed/throwing probes fail
  the connect loudly; echo filter now live on real deployments. ws-10: chat.delete-shaped
  transmitDelete extra + class-level deleteMessage(chatId, messageId):boolean
  (adapter.py:delete_message :3085) — run.py:28580's getattr(type(adapter),"delete_message")
  probe now arms the opt-in cleanup_progress config. ws-11: fake server models
  message_changed envelopes (pushMessageChanged: outer event_ts + nested message{ts,
  user,text,edited}); adapter normalizes them FIRST (:5773) — nested payload replaces the
  event under the ORIGINAL thread root, dedup keys on the changed-event-ts ladder
  (event_ts → edited.ts → outer ts ≠ original → `${original}:changed`), already-addressed
  originals never re-trigger (:5779 + :6855 processed-ts record, bounded 5000); pipeline
  reordered to Hermes truth (dedup :5797 BEFORE self/bot filters). Conforming harness
  updates: subject binds reaction/auth/delete capture lanes + deterministic token
  identity (UBOTAUTH0/TWORKSPACE0), arming lane stamps a turn anchor; fixture/rows/test
  draft call sites carry the production-parity thread identity; shared row
  streaming.prefix-mutation-detected now constructs its consumer with
  {reply_to_message_id} like seal-discipline (production_metadata_for_send parity, no
  assertion weakened). Removed stray untracked src/pi_platforms/conformance/__debug.test.ts
  (broken imports left by an earlier session; broke tsc/vitest collection).
  slack.test 36→50 contracts (+ reaction lifecycle ×3, START recipients/guard ×2,
  auth-scope ×3, delete lane ×2, edit normalization ×4); slack/persistent-ws/discord/
  streaming/conformance (27 stable files) green; tsc clean for cluster scope.
  NOTE: feishu/telegram/teams suites fail on this shared tree from concurrent round-2
  clusters' in-flight edits (proven independent: identical failures with this cluster's
  paths stashed).

- Stability round-2 fix cluster `teams-r2` (2026-08-25): 6 findings, single-owner
  slice (teams webhook-connectors axis), all parity restorations toward Hermes
  truth (no DEC entries required). teams-1: sendApprovalCard button data drops
  approval_id — btn_data_base is exactly {session_key, cmd≤200+"...", desc} +
  hermes_action (@1198-1210); sequential id bookkeeping stays adapter-side
  (pending-store key + approvalIdLog). teams-2: handleCardAction resolves clicks
  by SESSION KEY (has_blocking_approval + resolve_gateway_approval parity,
  @1146/@1156) via new kit OneShotPendingStore.oldestIdForSession probe; the
  integer-required "already resolved" early answer is gone so Hermes-shaped cards
  resolve; resolution still rides THE ONE CallbackQueryRouter on the claimed id
  (peek→pop synchronous ⇒ exactly-once under double-tap). teams-3:
  handleActivityPost gates on type==="message", routes AdaptiveCard invoke
  activities (name "adaptiveCard/action") to handleCardAction answering HTTP 200
  WITH the modeled InvokeResponse body, and ignores conversationUpdate/typing
  pings (SDK per-activity handler registration @850-859). teams-4: resolved taps
  return the FULL AdaptiveCardActionCardResponse body — title/cmd fence/Reason/
  bold label (@1160-1181) — CardActionResponse now a kind-discriminated union
  with AdaptiveCardPayload; expiry answers are card-shaped too. teams-5:
  sendTypingActivity carries the real activity POST body {type:"typing"} through
  the BotFrameworkTransport seam; FakeBotFrameworkServer records + rows assert
  it. teams-6: per-kind default contentTypes threaded into new sendImage/
  sendVideo/sendVoice/sendDocument call sites (image/png, video/mp4, audio/mpeg,
  application/octet-stream @1335/@1366/@1383/@1400) as guessMime FALLBACK —
  extension hits still win. Rows conformed to the stricter wire truths (titles
  updated; ping-gating + invoke-envelope + media-defaults legs added).
  teams-rows 6/6 (+new contracts), callback-router 38/38, tsc clean, layering +
  secret gates green. NOTE: full-suite failures on this shared tree confined to
  concurrent round-2 clusters' in-flight files (telegram tg.callback-roundtrip-64b,
  feishu sender-name cache) — proven independent of this cluster's paths.

- Stability round-2 fix cluster `feishu-r2` (2026-08-25): 11 findings
  (feishu webhook-connectors axis), all parity restorations toward Hermes truth
  (/tmp/hermes-upstream anchors cited; no DEC entries required). feishu-1:
  _build_markdown_post_rows/_build_markdown_post_payload ported (:580/:604) —
  post lane ships the vendor JSON STRING {"zh_cn":{"content":rows}} with
  fence-split md rows (Python splitlines boundary set incl. \x85/\u2028);
  ws-fixture dualPathMarkdown + test fake re-encoded so raw-byte preservation
  is asserted INSIDE the payload (feishu-fixture decodePostPayload). feishu-2:
  FeishuRestPlane gains im/v1/images.create (image_type="message" :203/:5189),
  im/v1/files.create (opus|mp4|pdf|doc|xls|ppt|stream routing :5234, duration
  only when >0 :5206, OGG last-granule duration parser :4662) and messages/:id/
  resources downloads (?type=image|file :4001); sendImageFile/sendVoice/
  sendDocument/sendVideo with caption→post media-row upgrade (:2310/:4738);
  inbound image:/file: refs download into mediaCacheDir BEFORE the media-batch
  dispatch and rewrite to local paths (silent per-ref failure keeps vendor ref).
  feishu-3: caller passes the FULL FRAME so root.header.event_id feeds
  vc_invite:{event_id} dedup (:131/:159) — same-meeting invites with distinct
  event ids now dispatch twice. feishu-4: CommentApi carries vendor request
  coordinates — batch_query body {comment_ids}+user_id_type=open_id (:300),
  list pages is_whole/page_size=100/page_token walked ≤5 pages client-side
  (:362/:424), replies body {content:{elements:[text_run]}} (:489), new_comment
  {file_type,reply_elements} (:511), drive/v2 reaction bodies {action,
  reply_id,reaction_type:"OK"} (:156/:206). feishu-5: fake records
  POST {reaction_type:{emoji_type}} and DELETE with reaction_id IN THE PATH +
  empty body (:3171/:3203). feishu-6: text lane ships VERBATIM (trim only,
  format_message :2461); faithful_strip_markdown_to_plain_text port (link→
  'text (url)', blockquote, hr, <u>, CRLF + shared strip_markdown order) fires
  ONLY on post-rejected downgrade lanes (:555); §6.1 plain fallback forces the
  text lane. feishu-7: empty emoji defaults UNKNOWN (:3023). feishu-8: cached
  silent-failure name resolution (contact/v3/users/:id id-type routing, bot/v3/
  bots/basic_batch for bots, 10-min TTL :4205/:4257/:238) warmed on EVERY
  inbound message; resolved approval/update cards attribute by cached user_name
  or raw open_id fallback (:2811/:2871); get_chat_info cache (:2424) feeds the
  reaction-source chat name. feishu-9: FEISHU_WEBHOOK_PATH/HOST/PORT env surface
  (defaults /feishu/webhook, 127.0.0.1, 8765 :229–231/:1650) and the composite
  rate key rides the CONFIGURED path (:3562; lastWebhookRateKey observable).
  feishu-10: manifest approve_session/approve_always type default (_btn :2077).
  feishu-11:_parse_user reads ONLY user_name (:99). Suite: feishu.test.ts
  68→96 (+28 contracts), conformance fs rows 6/6 + shared/ws fixtures green;
  tsc clean for all cluster paths; full-suite failures on this shared tree
  confined to OTHER concurrent round-2 clusters' in-flight files (telegram,
  yuanbao, teams-stray dbg.test.ts layering trip) — proven independent of
  src/pi_platforms/feishu/**.

- Conformity round-2 fix cluster `whatsapp-cloud-r2` (2026-08-25): 6 findings,
  single-owner pass over src/pi_platforms/whatsapp-cloud. wa-2: cached-media
  extension resolves hintMime-FIRST — webhook inner mime before Graph-metadata
  mime before '.bin' (_download_media_to_cache @~1388; new tryResolveMediaExtension
  _ext_for_mime Optional[str] parity in manifest.ts). wa-3: text-readable document
  content (.txt/.md/.csv/.json/.xml/.yaml/.yml/.log/.py/.js/.ts/.html/.css, ≤100KB)
  injects INLINE as '[Content of {cached-basename}]:\n{content}' PREPENDED to the
  body (@~2020; oversize/failed reads keep '[Document: fname]'). wa-4: voice-lane
  MP3→opus transcoder seam (send_voice @~1194) — caller-declared MP3 (.mp3 name /
  audio/mpeg mime) converts pre-upload to 'audio/ogg; codecs=opus', null ⇒ audio/mpeg
  attachment fallback; INJECTED option defaulting to the documented no-ffmpeg
  passthrough (signal remuxAac precedent; no OS child from this port). wa-5:
  sendSlashConfirm renders the 3-button sc:{once|always|cancel}:{confirm_id} card
  ('✅ Approve Once'/'🔒 Always'/'❌ Cancel', body _{title}_+message truncated 1024)
  via postInteractive + slashConfirms registration (@~903). wa-6: quoted replies
  hydrate event metadata reply_to_text = quotedTextOf(chatId, replyToId) (@~2067),
  feeding the run-loop '[Replying to: …]' gate. wa-7: outbound `to` = chatId
  VERBATIM on every messages POST (send @~544) — expandWhatsappAliases/min-pick
  canonicalization REMOVED from the wire path (02 §4.3 session-key-side only);
  resolveRecipient is now a documented identity seam; stale LID mappings cannot
  rewrite Meta-delivered wa_ids. Tests conformed: identity-lid.test.ts verbatim-
  recipient row rewritten; conformance transport.wa.lid-alias-continuity row
  updated (session-key collapse assertion retained); NEW contracts: hint-first
  extension (divergent mimes, backfill, .bin), transcoder lane (convert/fallback/
  undeclared-bytes-pass-through/non-audio-kinds), document injection (csv inline +
  caption order + >100KB skip + non-text skip), slash-confirm card (wire shape +
  tap round-trip through the ONE router + truncation/no-register-on-failure),
  reply_to_text hydration via capture guard. Suite: whatsapp-cloud 55→68 files-tests
  74/74 green (+13), conformance wa rows green; layering + secret gates green;
  tsc clean (whole-tree window verified during sibling-cluster settle).

- Conformity round-2 fix cluster `yuanbao-r2` (2026-08-25): 8 findings,
  single-owner pass over src/pi_platforms/yuanbao (+conformance/yuanbao-rows).
  yb-1: encodeAuthBindRaw carries SIGN_APP_VERSION/SIGN_BOT_VERSION ('0.20.5',
  not hardcoded '1.0.0'), source = token_data.source-or-'bot', env_name field 5
  routeEnv = adapter.routeEnv || token_data.route_env (SignManager.fetch now
  parses source/route_env into SignTokenData; fake records decoded AUTH_BINDs).
  yb-2: scheduleReconnect calls signManager.forceRefresh before EVERY dial —
  cache-valid reuse after drops would re-auth on server-rotated credentials;
  bot_id refresh threaded per _do_reconnect. yb-3: backoff min(2**(n-1),60)
  reaches the 60s cap (was 16s forever). yb-4: YB_MAX_TEXT_CHUNK=4000 manifest
  constant is the ADAPTER-default scalarMaxUnits (subjects keep explicit 64
  harness budget). yb-5: per-sender 1.5s DEBOUNCE_WINDOW (_push_to_inbound
  parity) — companion pushes merge into ONE dispatchPush run, base push +
  "\n" TIMTextElem separator between companions (DecodeMiddleware merge),
  window RESETS per arrival, buffers dropped on disconnect; fixture rows
  resubscribeReplay/watchdogRecovery + yb.* delta rows conformed to merged-turn
  truth with cross-window dedup exactly-once. yb-6: dmPolicy/groupPolicy
  'open' branches consult GATEWAY_ALLOW_ALL_USERS / YUANBAO_ALLOW_ALL_USERS
  opt-in env flags (truthy true/1/yes case-insensitive; deny-by-default).
  yb-7: optional group-origin code rides metadata yuanbao_group_code onto
  SendC2CMessageReq field 6 (send_dm→send_c2c_msg_body parity); fake decodes
  C2C field 6. yb-8: openAndAuth races pendingBind against AUTH_TIMEOUT_S=10
  under the INJECTED clock — withheld BIND_ACK fails connect CLOSED (fatal),
  never hangs. NEW contracts: yuanbao-conformity.test.ts yb-r2 describe
  (auth identity vectors incl. token fallbacks, force-refresh-per-dial +
  backoff ladder data [1000,2000,4000,…60000 cap], debounce merge/reset/
  sender-isolation, open-policy env matrix, C2C origin field 6 wire shape,
  bind-timeout fail-closed). Suite: yuanbao-conformity 21/21, yuanbao-rows
  8/8 green; tsc clean for the cluster window. Note: an interleaved sibling
  sweep committed intermediate yuanbao states inside 3c37c13 (telegram-wire-r2);
  this cluster's final state landed as the follow-up yuanbao-r2 commit.

- Stability round-2 fix cluster `api-server-r2` (2026-08-26): webhook /v1/runs +
  completions lanes moved onto Hermes truth at /tmp/hermes-upstream anchors.
  api-1: POST /v1/runs/{id}/approval answers {object:'hermes.run.approval_response',
  run_id, choice, resolved} (@8140-8146) — invented status:'approval_responded'
  removed; SSE approval.responded frame carries the resolved count (@8147).
  api-2: body all/resolve_all honored via coerceRequestBool → resolve_gateway_approval
  parity — registry drains EVERY live approval under the run FIFO
  (OneShotPendingStore.oldestIdForSession IS the session queue, tools/approval.py:2850)
  with ONE counted frame; string booleans normalize ("false" never misroutes).
  api-3: RunView stores+returns model (queued @7690, body value or default) + usage
  (executor RunCompletion seam, bare-string executors still compile) + pending_steer
  (undelivered steer rides completed status AND terminal event @7926-7936); GET
  renders snake_case usage triple. api-4: terminal runs have no live refs ⇒ stop
  answers 404 run_not_found (@8199) — 409 run_already_finished deleted from lane and
  registry. api-5: X-Hermes-Session-Key ladder COMPLETES DEC-017 on both lanes via a
  new key-only core extractSessionKeyHeader in pi_gateway/security/trust/session-headers
  (pair helper refactored onto it, zero drift): 403 requires-auth when no API key
  configured (never anonymous, runs lane gated BEFORE body parse), 400 control-char /
  400 over-length rendered as Hermes' plain {message,type} dicts, echo on 202 start +
  every completion response/SSE stream (@4689/:5343/:5427), memory-scope binding
  observable adapter-side (memoryScopeBindings + metadata.memory_scope; routing keys
  untouched). Suite: webhook+trust scoped 187/187→201/201 with new contracts
  (approval envelope, FIFO drain w/ counted frame, model/usage/pending_steer views,
  late-stop 404, key ladder incl. key-less-server 403); tsc clean for cluster files;
  layering + secret gates green. NOTE: shared-tree commit d28d0b3 absorbed most of
  this cluster's file state mid-flight; this commit finalizes it (test contracts +
  formatting).

- Stability round-2 fix cluster `raft-fixes-r2` (2026-08-26): 4 findings,
  single-owner pass over src/pi_platforms/raft (+conformance/raft-rows) toward
  Hermes truth at /tmp/hermes-upstream/plugins/platforms/raft/adapter.py anchors.
  raft-2: the register_hook family is REAL — adapter registers its SEVEN
  observer producers at construction (adapter.py:register :847-853 parity:
  on_session_start⇒SessionStart, pre_llm_call⇒UserPromptSubmit with
  prompt-turn dedup, pre_tool_call⇒PreToolUse, post_tool_call⇒
  PostToolUse|PostToolUseFailure with the status/error_message-or-result/
  error_type-or-tool_failure verdict ladder, post_llm_call⇒Stop,
  on_session_end⇒interrupted/incomplete error Stop + scope forget,
  on_session_finalize⇒SessionEnd + session forget);_remember/_forget/
  _is_raft_context ported as instance scope sets (platform stamp re-members);
  registerHook()/emitActivityHook() are the ctx.register_hook / host-loop
  drive seams with DEC-014 emit-and-log containment; the wake channel drives
  its own lane points every turn — connect⇒SessionStart, accepted wake ⇒
  UserPromptSubmit, completed turn frame ⇒Stop, disconnect⇒SessionEnd — so
  GET /activity/drain is no longer empty forever. raft-3: eventId is
  f"hermes-{uuid.uuid4()}" parity `hermes-${randomUUID()}` (was
  'hermes-raff-'+Math.random hex). raft-4: stableJson is json.dumps(
  ensure_ascii=False, sort_keys=True) BYTE parity — recursive key sort +
  Python default separators ', '/': ' (bare JSON.stringify drained compact
  insertion-ordered bytes); NaN/±Infinity render bare per allow_nan=True.
  raft-5: malformed JSON on POST /activity acks LITERAL invalid_json BEFORE
  validation (:652_handle_activity JSONDecodeError parity; empty body
  included); parseable-but-invalid keeps str(ValueError) verdicts verbatim;
  counted in activityInvalid. Tests conformed: two rows drain-clear the
  connect-time SessionStart; NEW delta rows observer-hook-producers-feed-drain
  (turn feed, uuid shape, dedup, throwing-observer containment),
  tool-call-producers-mapping (sorted+spaced byte-exact toolInput/toolOutput,
  failure ladder, interrupted/incomplete/clean ends, foreign-platform silence,
  finalize-forget), activity-invalid-json-ack. Suite: raft-rows 5/5 blocks
  green (delta rows 11→14, mutant-detection gate still detects); tsc clean
  for cluster files; layering/secret gates untouched by this window (residual
  full-tree failures are sibling clusters' untracked dbg-*/scratch files).

- Stability round-2 fix cluster `a2a-fixes-r2` (2026-08-26): a2a platform
  moved onto Hermes truth at /tmp/hermes-upstream/plugins/platforms/a2a
  anchors. a2a-1: sortKeysJson (security.py:sign_push_payload) is
  json.dumps(payload, sort_keys=True, ensure_ascii=False) BYTE parity —
  recursive key sort + Python DEFAULT separators ', '/': ' via a dedicated
  serializer (bare JSON.stringify drained compact bytes, so receivers
  re-canonicalizing per the documented sorted-keys convention rejected every
  X-A2A-Signature); string escaping defers to per-string JSON.stringify
  (identical ESCAPE map), transmitted body stays compact json.dumps(payload)
  insertion-order parity (adapter.py:1208). a2a-2: buildCard consults
  A2A_PROVIDER_ORG/A2A_PROVIDER_URL via this.env into card.provider with
  getenv semantics (UNset ORG ⇒ 'Hermes Agent', SET-but-empty ORG ships '',
  UNset/empty URL ⇒ card-url fallback; protocol.py:build_agent_card); both
  added to manifest optionalEnv. a2a-5: constructor/loadServedAgents consult
  the missing fallback lanes — A2A_AGENT_DESCRIPTION root-description default
  (env > config.description > reference constant; set-but-empty degrades to
  the constant at card time via the falsy-description guard),
  A2A_ADVERTISED_TOOLSETS csv when configured advertised_toolsets is EMPTY
  (blank entries dropped both lanes; adapter.py:__init__), and the global-
  config served-agent ladder extra.agents → extra.served_agents (NEW alias)
  → cfg.a2a_served_agents → cfg.a2a.served_agents with PYTHON `or`
  fall-through ([]/{} falsy operands) over an INJECTED A2aGlobalConfig
  snapshot (the port never reads ~/.hermes/config.yaml;
  adapter.py:_load_served_agents/_load_global_a2a_config). NEW contracts:
  a2a-config-surface.test.ts 16 rows (separator byte vectors incl.
  unicode-literal/escape parity, independent receiver-HMAC over hand-written
  canonical bytes, e2e push signature verified against the literal canonical
  form of the delivered body, provider env matrix + manifest rows,
  description/toolset lanes, served-agent ladder incl. falsy fall-through).
  Suite: a2a-rows 6/6 blocks + a2a-config-surface 16/16 green (push-plane
  receiver-side verification now exercises spaced separators); tsc clean for
  cluster files; layering/secret gates green (residual full-tree failures are
  the sibling qqbot cluster's untracked scratch files — zero a2a overlap).

- Stability round-2 fix cluster `signal-r2` (2026-08-26): signal platform
  moved onto Hermes truth at /tmp/hermes-upstream/gateway/platforms/signal.py
  anchors. sigbb-2: SignalCliTransport.openEventStream(account) — the SSE
  listener passes this.account on EVERY open (connect + reconnect ladder),
  matching GET /api/v1/events?account={quote(account, safe='')}
  (signal.py:_sse_listener :454); FakeSignalCliServer records the account per
  connectionLog entry so the account-scoped routing is assertable on the wire
  seam; harness bridge signature updated (still fixture-only). sigbb-3: the
  rpc seam gains optional timeoutMs threaded adapter.rpc → transport.rpc, and
  batchSendAttachments passes signalSendTimeout(n) per batch RPC ('void
  timeout' removed) — max(60s, 5s·n) with the 30s n≤0 default
  (signal_rate_limit.py:_signal_send_timeout); fake captures timeoutMs per
  call. sigbb-4: deliverText converts the WHOLE message via markdownToSignal
  FIRST, then splits the CONVERTED UTF-16 text via splitSignalFormattedMessage
  (port of_split_signal_formatted_message/_utf16_offsets/_styles_for_chunk,
  signal.py :1080-1180) translating bodyRanges per chunk — convert-after-chunk
  lost boundary-crossing styles and leaked literal ** markers. Chunks carry "
  (i/n)" labels post-translation; wireSend wires pre-translated ranges through
  a per-chunk activeChunkStyles binding (reconverting converted text would
  drop every range); §6.1 plain-text fallback for single-chunk deliveries
  resends the ORIGINAL content bytes (rows.ts parse-failure-resend contract
  preserved unmodified). DEC-047 plan-byte-exact engine rows re-expressed in
  converted-plan shape (byte-exact splitter output + label + translated
  ranges). sigbb-8: batchSendAttachments stat-gates every entry BEFORE slicing
  — missing files and >SIGNAL_MAX_ATTACHMENT_SIZE entries skipped
  (send_multiple_images :1325-1355 parity; zero valid ⇒ zero RPCs/outcomes);
  DEC-019 single-attachment verdict-shaped gates untouched. Tests conformed:
  batch rows use REAL files (sparse truncateSync oversize), NEW contracts for
  account-scoped stream opens (connect+reconnect), timeout scaling (160s/60s
  floor), stat-gate skip lanes, cross-boundary style preservation, span-clip
  across 4 chunks, no-marker leakage. Suite: signal-engine 58/58 +
  conformance/signal-rows 7/7 (full-catalog merge gate green); tsc clean;
  residual full-tree failures are the sibling qqbot cluster's untracked files
  (dbg2/qqbot-attachments — zero signal imports).

- Stability round-2 fix cluster `weixin-r2` (2026-08-26): weixin adapter moved onto Hermes
  truth at /tmp/hermes-upstream/gateway/platforms/weixin.py anchors (all seven findings; no
  DEC deltas — every change converges toward reference behavior). wx-1: generic vendor send
  errors (ret/errcode ∉ {0,-14,-2}) retry inside _send_text_chunk_locked parity with linear
  backoff SEND_CHUNK_RETRY_DELAY_S*(attempt+1), terminal only after SEND_CHUNK_RETRIES=4
  (Hermes raises into the retry loop :1859+); NOT a timeout class so DEC-046's carve-out is
  untouched; errmsg now rides the error strings (rate-limit msg = Hermes shape too).
  wx-2: ported _is_stale_session_ret (ret/errcode -2 + errmsg 'unknown error' ⇒ stale
  session) as exported adapter helper, branched BEFORE rate-limit handling at BOTH sites —
  poll (:1394): joins the -14 family (DEC-045 ladder: 600s pause → strike-2 recycle(gen
  bump); plain -2 keeps the failure ladder), send (:1847): strips context_token + evicts the
  token store entry and retries tokenless ONCE without feeding the breaker. Fake face gained
  scripted errmsg on sendmessage. wx-3: pullOnce budget is now ADAPTIVE — LONG_POLL_TIMEOUT_MS
  default, server longpolling_timeout_ms adopted on every settled response (:1386,
  observable via longPollTimeoutBudgetMs getter); an over-budget probe is a BENIGN empty
  cycle (_get_updates TimeoutError leg answers ret:0/empty with ZERO penalty — pollLog
  'timeout' marker only); the timeout-streak recycle (notePollTimeout/pullTimeoutStreak/
  reconnectTriggered) is REMOVED — reconnect escalation stays reserved for the -14/stale
  streak (DEC-045). heartbeatEscalation fixture re-realized in vendor truth: benign-hold leg
  proves zero-penalty overrun, then TWO stuck session probes drive strike1-pause/strike2-
  recycle with post-recycle message flow. wx-4: WX_MAX_MESSAGE_LENGTH=2000 manifest constant
  (WeixinAdapter.MAX_MESSAGE_LENGTH) is now the DEFAULT scalarMaxUnits (was ??64);
  explicit injection still wins (subject harness keeps its 64-char scale). wx-5:
  deliverText routes Hermes send() truth (:1961 chunks=_split_text(format_message(content))):
  normalizeMarkdownBlocks + wrapCopyFriendlyLines(120-col) THEN splitTextForWeixinDelivery
  at the chat budget — chatty bubbles ship unlabeled; an oversized markdown block overflows
  through THE kit fence-carry chunker (DEC-047 plan-exact base.truncate_message port:
  newline-preferred/space-fallback boundaries, fence carry, "(i/n)") via a new optional
  overflow hook on packMarkdownBlocks/splitTextForWeixinDelivery (legacy hard-slice kept for
  pure data contracts). Shared egress.chunk-flood row passes UNMODIFIED against the new
  pipeline (labels are vendor overflow truth, not kit-only artifacts). wx-6:
  outboundMediaKind classifies by mime-PREFIX matching (extension→mime surface mirroring
  mimetypes.guess_type): image/*→image mid_size, video/*→video beyond the old fixed list
  (.svg/.tiff/.ico/.heif/.avif/.m4v/.3gp…), .silk→voice override, everything else MEDIA_FILE;
  force_file_attachment gates ONLY the silk leg (vendor ORDER — forced images still IMAGE).
  wx-7: dmPolicy 'open' now resolves through openDmOptedIn (_open_dm_opted_in :1539):
  GATEWAY_ALLOW_ALL_USERS or WEIXIN_ALLOW_ALL_USERS ∈ {true,1,yes} case-insensitive via a
  readEnv seam (production process.env); pairing/disabled/allowlist semantics untouched.
  Harness conformed without weakening shared contracts: WXSubjectOptions/makeWXWorld gained
  dmPolicy passthrough. Mid-session hazard absorbed: a concurrent cluster's tree-wide commit
  (3c37c13 sweep) transiently clobbered this file set mid-edit — state rebuilt from stash@{0},
  re-applied, verified byte-coherent (manifest/fake-ilink legs had already landed in that
  commit; ilink-transport/vendor-tests remain untracked census files, added by THIS commit).
  Suite: weixin vendor-contracts 16→33 (+wx-1 retry/backoff ladder ×2, wx-2 poll/send stale ×3,
  wx-3 adaptive-budget/benign-timeout ×2, wx-4 default budget ×2, wx-5 format/bubbles/overflow/
  wrap ×4, wx-6 mime-prefix ×2, wx-7 opt-in gates ×3); weixin-rows 8/8 incl. full-catalog merge
  gate + transport rows; tsc clean for cluster scope. Residual full-tree failures confined to
  sibling clusters' in-flight files (qqbot attachments/dbg2, signal engine — zero weixin imports).

- Stability round-2 fix cluster `qqbot-r2` (2026-08-25): 5 findings, single-owner
  slice (cn-exotics axis), all parity restorations toward Hermes truth with DEC-055/056
  logged for the two deliberate scoping deltas. qq-1: inbound attachment processing
  PORTED (adapter.py:_process_attachments/_stt_voice_attachment/_call_stt) — CDN GETs
  carry 'QQBot <token>' (_qq_media_headers, '//'-URL https-normalization), voice chain
  asr_refer_text → pre-converted voice_wav_url → DIRECT HTTPS POST
  {base_url}/audio/transcriptions (Bearer + multipart model+audio/wav parts; parses BOTH
  GLM choices[0].message.content and OpenAI {text}), '[Voice] …' block appends so
  VOICE-ONLY turns deliver instead of hitting the empty-text/no-image drop gate; the
  FALSE 'external ffmpeg+whisper daemons' exclusion comment replaced with honest seams:
  _call_stt is a direct call (production defaultByteFetch over node:http(s), 30s abort,
  redirect re-gated) and SILK→WAV rides an injected convertVoiceToWav bridge
  (absent ⇒ '[语音识别失败]' exactly like Hermes without ffmpeg); tools/url_safety.
  is_safe_url ported at the production sink (scheme/private-IP/DNS fail-closed +
  PI_QQ_MEDIA_HOST_ALLOWLIST suffix lock-down per hop). qq-2: MessageEvent
  media_urls/media_types populate from image attachments (cached paths under optional
  mediaCacheDir — DEC-056 — else vendor URLs), '[file|video: name (ref)]'
  attachment-info lines append for files/videos, msg_type-103 quoted attachments run
  the SAME pipeline with quoted images unioning onto the media lists and quoted voice
  riding inside the quote block; event messageType from adapter.py:_detect_message_type
  (media wins over text). qq-3: transmitMedia falls back to a '{caption}\n{image_url}'
  TEXT send after ANY http(s)-source IMAGE failure (adapter.py:send_image verbatim;
  local-file/voice/video/document lanes never fall back). qq-4: wireSend,
  transmitMedia AND sendWithKeyboard gate on the listener ahead of ALL REST legs —
  bounded _wait_for_reconnection polls (15s × 0.5s via the injected sleep seam) then
  retryable 'Not connected'; gate scoped to live-gateway faces per DEC-055 (capture
  faces stay open); DEC-044 Retry-After capture untouched, DEC-046 timeout-no-retry
  preserved. qq-7: timeoutS now ENFORCED through QQRestTransport legs — apiRequest
  races the transport against the injected clock ('QQ Bot API timeout [path]'
  classification shape), DEFAULT_API_TIMEOUT(30s) on API/token/gateway-url/interaction-
  ACK legs, FILE_UPLOAD_TIMEOUT(120s) on upload legs (COS PUT keeps its 300s),
  ONBOARD_API_TIMEOUT(10s) threaded into createBindTask/pollBindResult; hung legs
  raise INTO classification instead of stalling forever. Manifest data: reconnect-wait/
  poll, media HTTP timeout, STT provider table + env names + default models. Fixture/
  subject gained byteFetch/stt/mediaCacheDir/convertVoiceToWav passthroughs. New
  behavior-contract suite qqbot-attachments.test.ts (16 rows: STT chain ×5, media-ref
  carriage ×4, connection gate ×4, per-leg timeouts ×3 incl. DEC-046 single-attempt
  proof); qqbot-media no-file_info row conformed to the send_image fallback truth (+new
  raw-lane no-fallback row). qqbot suites 37/37, conformance qqbot-rows 8/8 (shared +
  transport family + deltas + full catalog), tsc clean for cluster scope. Full-tree
  failures confined to sibling clusters' in-flight files (irc/bluebubbles/email/photon/
  weixin — zero qqbot imports; kit/ and pi_gateway/ untouched by any cluster).
  FORENSICS NOTE: mid-cluster, a concurrent cluster's `git reset --hard` wiped this
  tree's uncommitted qqbot baseline (recovered byte-exact from dangling WIP commit
  b47129d) and an over-broad `git add -A` sweep committed partial qqbot fixture state
  under the telegram-r2 commit; this cluster's own commit is scoped to its paths only.

- Stability round-2 fix cluster `bluebubbles-r2` (2026-08-26): 4 findings,
  single-owner pass over src/pi_platforms/bluebubbles (+conformance/
  bluebubbles-rows) toward Hermes truth at /tmp/hermes-upstream/gateway/
  platforms/bluebubbles.py anchors. sigbb-1 (HIGH): _handle_webhook's inbound
  attachment loop ported into handleWebhookPost (@~961-995) — EVERY
  record['attachments'] guid downloads through the existing downloadAttachment
  transport leg plus the classification/persistence legs (@~820-878): image/*
  rides CLOSED BB_IMAGE_EXT_OVERRIDES (fallback .jpg) under img_{hex12}, audio/*
  rides CLOSED BB_AUDIO_EXT_OVERRIDES (fallback .mp3) under audio_{hex12},
  everything else keeps transferName (file_{hex8} default) under doc_{uuid12}_;
  media_urls/media_types ride the dispatched event with the photo/voice/video/
  document matrix INCLUDING the empty-mime-but-.caf-uti voice quirk and the
  multi-attachment PHOTO preference; attachment-only messages backfill
  text='(attachment)' so they DISPATCH instead of answering 400 (mediaCacheDir
  option with lazy mkdir — signal/teams precedent — fixture injects mkdtemp
  isolation and rmSyncs on dispose). sigbb-5: getChatInfo added (@~779) over the
  REST seam — resolveChatGuid ladder then GET /api/v1/chat/{quoted-guid}?
  with=participants resolving displayName→chatIdentifier→target with trimmed
  participant addresses; unresolved targets/REST failures keep trivial
  {name,type}; ';+;' raw targets report groups; fake server gained the route +
  seedChatInfo/chatInfoCalls capture. sigbb-6: BlueBubblesRestClient.post and
  apiPost payloads OPTIONAL; sendTyping/markRead post BODYLESS (httpx post
  without json= @~733/@~749) — fake captures bodylessPostCalls and the receipts
  row asserts exact bodyless path sets across the full private_api×helper 2×2
  gate matrix plus the ingress fire-and-forget receipt. sigbb-7: sendText loop
  RETURNS immediately after createChatForHandle (send @~563) — ONE chat/new POST
  carrying the FIRST paragraph; multi-paragraph sends to unresolved address-like
  targets no longer re-post chat/new per chunk (resolveForSend/postMessageText
  extracted; postResolvedBubble shares them, delivery-door behavior unchanged).
  Rows: shape-delta catalog 10→12 (+transport.bluebubbles.inbound-attachments:
  byte-exact cached round trip, closed-map extensions, caf quirk, caption
  retention, failed-download skip ⇒ still-400; +transport.bluebubbles.chat-info:
  seam/quote/participants/fallback/group rows); dispatchedEvents probe gained
  messageType/mediaUrls/mediaTypes observability. Suite: bluebubbles files +
  conformance rows 16/16 ×3 repeat runs (34-row full-catalog gate green);
  tsc clean for cluster scope; full-tree failures confined to sibling clusters'
  in-flight ntfy/ws/email files (zero cross-imports; kit/ and pi_gateway/
  untouched). Commit scoped to cluster paths only.
- Stability round-2 fix cluster `agent-loop-repair-r2` (2026-08-26): pi_agent_core runner
  moved onto DEC-015's own placement contract ("immediately before EACH API call") —
  Hermes truth at /tmp/hermes-upstream agent/conversation_loop.py:run_conversation anchors
  (finding core-routing-8; no DEC deltas — pure convergence toward reference behavior).
  core-routing-8: the one-shot pre-append pass repaired only the SEEDED history BEFORE
  prompt() appended the live user turn, so a trailing durable user row (crash/interrupt
  between the user-row persist and the assistant reply, or gateway multi-queue replay
  tails) reached the provider alongside the new ask as consecutive users. Replaced with a
  per-model-call pre-send chokepoint armed on the host loop's Agent.transformContext seam
  (pi-agent-core agent-loop.js:streamAssistantResponse — runs before EVERY model call,
  after steering/follow-up injection, over the exact context list convertToLlm maps to the
  wire copy, INCLUDING the freshly appended ask): repairMessageSequence +
  sanitizeToolCallArguments mutate that list in place (`messages[:] = merged` parity),
  repairs accumulate across all of a turn's model calls into outcome.repairs/
  state.repairCount, the hook never rejects (loop contract) and is un-armed in the prompt
  finally so post-turn session work (auto-compaction/branch summaries) never routes
  through it; gateway-owned durable rows stay byte-untouched (persist-stores-bytes
  invariant intact). Tests conformed to CONFORMING behavior + two new contracts: the old
  "compacts a replayed user→user tail" row had enshrined the bug (asserted TWO adjacent
  wire users: merged-tail + live ask) — now asserts ONE merged user carrying tail+fresh
  ask (repairs=2); NEW single crash-orphaned-row test proves request 1 AND the
  post-toolResult request 2 each carry exactly one merged user (per-call re-run proof,
  generic no-adjacent-user sweep over every captured request); NEW two-process interleave
  test drives the waited-lease resume path (ghost process persists its user row then dies;
  waiter's lease-wait ⇒ resume-tip re-resolve + transcript reload ⇒ fresh ask must merge
  with the ghost tail pre-request). Suite: pi_agent_core 72/72 across 7 files (runner 16
  incl. 3 DEC-015 chokepoint rows); tsc clean for cluster scope; layering untouched
  (host.ts is still the sole SDK seam). Residual full-tree failures confined to sibling
  clusters' in-flight files (sms/ntfy/homeassistant conformance rows — zero pi_agent_core
  imports; tsc errors likewise ntfy-standalone only), verified unrelated to this change.

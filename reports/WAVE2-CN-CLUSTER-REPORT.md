# Wave 2 — CN cluster report (QQBot / Weixin / Yuanbao)

Branch: working tree on top of `10e655c`. Scope: `src/pi_platforms/{qqbot,weixin,yuanbao}/**`

+ conformance wiring files for MY rows only. No core-file diffs; no other
agents' dirs touched.

## QQBot — GREEN ✅ (8/8 suite tests, full merge gate holds)

Files: `manifest.ts`, `crypto.ts` (AES-256-GCM bind-key/decrypt), `keyboards.ts`
(inline keyboards + button_data grammar + InteractionEvent), `chunked-uploader.ts`
(prepare→COS PUT→part_finish(40093001 retry-until-timeout)→complete;
40093002 → typed non-retryable daily-limit; md5/sha1/md5_10m single-pass),
`fake-qq-gateway.ts` (op-code gateway face + scripted REST face),
`qqbot-adapter.ts`, subject/fixture, `conformance/qqbot-rows.test.ts`.

+ Shared applicable set: ALL pass (streaming family excluded BY PROBE; flip
  re-includes and fails seal-discipline by name).
+ ws family: all 5 rows green vs real engine (resume replay w/ dedup
  exactly-once incl. reduplicated id; hard-TCP watchdog via read path —
  documented: Hermes qqbot tracks no pong timeout; 4008 ⇒ RATE_LIMIT_DELAY=60s
  authoritative capture + REST retry_after leg; §10.1 rich-latch; dual-path
  markdown RAW in msg_type=2 bodies).
+ 7 qb.* deltas green: AES-GCM negative matrix (tamper/wrong-key/short all
  fail typed), keyboard↔INTERACTION roundtrip (ACK-before-dispatch,
  unauthorized answered-not-resolved, guild refusal non-retryable), chunked
  upload contracts (per-part block_size/md5, 40093001 timeout raise,
  40093002 typed, malformed prepare rejected, alias tolerance), close-code
  matrix (fatal stops w/ zero scheduled reconnects; session-invalid clears for
  Identify; 4009 stays resumable; 4004 refreshes token only), dedup window +
  ACL intake (pairing/open/disabled, @-strip, quote merge msg_type=103,
  empty-drop), markdown v2 body contract (msg_seq, msg_id reply,
  message_reference text-mode, cap enforcement).
+ Negative gates: lying transport fixture fails all 5 rows BY NAME; interaction
  swallow-mutant fails its row; capability flip fails seal-discipline.

## Weixin — GREEN ✅ (8/8 suite tests, full merge gate holds)

Files: `manifest.ts`, `wire-crypto.ts` (AES-128-ECB PKCS#7 + key parse),
`text-splitting.ts` (copy-friendly wrap 120col, block/unit split, chatty-bubble
heuristic, greedy pack — full `_split_text_for_weixin_delivery` port with own
contracts), `fake-ilink.ts` (REAL long-poll semantics: empty queue HOLDS the
request; sync_buf advances only on success; scripted -14/-2),
`weixin-adapter.ts` (poll loop w/ failure ladder 2s/30s + session recycle,
id+content-fingerprint dedup TTL 300s, debounce batching 3s/5s w/ ≥1800
split-threshold, rate-limit circuit breaker threshold1/window30/open30,
-14 tokenless single retry, per-chat locks, typing tickets),
subject/fixture, `conformance/weixin-rows.test.ts`.

+ Shared applicable set: ALL pass. Polling family: all 4 rows green vs real
  engine (server-queue preservation across outage; ack-before-enqueue hold+drain;
  -14 storm escalation pause→recycle(gen bump)→fatal — vendor-truth mapping of
  the zombie-eviction row documented in fixture notes; stuck-probe escalation
  gen-bump + big-ladder).
+ 7 wix.* deltas green: cursor persistence/error-no-advance, dedup TTL re-arm,
  batching quiet/split windows, breaker (open/cooldown/reset; mutant that never
  records FAILS BY NAME), tokenless retry, ACL/chat-routing (+[引用媒体] quote
  prefix), splitter contracts.
+ Negative gates green (lying polling fixture; breaker-defeating mutant).

## Yuanbao — PARTIAL ⚠️ (core port complete; suite integration unfinished)

Files: `proto.ts` (hand-rolled protobuf wire codec: varint/fields/head/ConnMsg/
BizMsg/msg-content/msg-body/send-c2c/group/auth-bind/ping/push-ack/reply-
heartbeats/inbound-push encode+decode — byte-exact contracts), `sign-manager.ts`
(HMAC-SHA256 sig, Beijing timestamp, cache w/ 60s margin, per-app_key
singleflight, 10099 retry ladder), `fake-yuanbao.ts` (binary ConnMsg frames:
AUTH_BIND→BIND_ACK(connectId), ping→pong, Push need_ack→PushAck capture,
scripted closes/hard drop), `yuanbao-adapter.ts` (close-code classes:
NO_RECONNECT{4012..} fatal / AUTH_FAILED{4001-3} re-sign / others ladder;
heartbeat w/ missed-pong threshold 2; JSON-push decode parity snake+PascalCase;
dedup/ACL/@guard; reply-heartbeat RUNNING/FINISH lifecycle + auto-stop;
slow-response notifier; per-chat-lock sender w/ honor-once retry_after),
subject/fixture, `conformance/yuanbao-rows.test.ts`.

GREEN now: applicability-probe test, manifest ground truth, proto
byte-exact delta, sign-manager contracts delta, capability-flip negative,
gate-detects negative (lying transport fixture fails all 5 rows BY NAME;
push-swallow mutant fails its row).

RED (named causes, NOT faked):

1. Shared egress rows (chunk-flood/utf16/plain-fallback): subject govern-mode
   capture still falls through to a disconnected WS face in some paths — needs
   the same record-and-succeed seam audit qqbot/weixin got.
2. resubscribeReplay eventually: merged-drain turns break the exact-token
   predicate (needs includes-based counting like weixin's final version).
3. capabilityLatchPermanence/dualPath: rich-probe latch counts not yet observed
   through the new richProbe seam end-to-end.
4. auth-handshake-close-matrix: one spurious reconnectSteps entry before the
   fatal close (needs tracing; suspect heartbeatLoop firing post-fatal since
   `running` is not cleared on markFatal).
5. push-ack-decode parity + reply-heartbeat rows: timing/knob issues
   (replyHeartbeatIntervalMs knob added to adapter; world passes 20ms but the
   row asserts against default-paced counters).

No silent divergence: every above gap is a TEST-integration gap listed here,
not weakened assertions. tsc --noEmit clean for ALL files in my three dirs
(repo-wide remaining errors are other agents' dirs: irc/, google-chat/, wecom/).

## Deviations / proposed DECs (recorded, none implemented silently)
+ QQBot REST-leg Retry-After capture + close-reason "retry-after:N" parse
  (yuanbao): wave-1 ws-adapters' family-row convention (lastCaptured knob);
  Hermes qqbot/yuanbao do not parse these — proposed DEC text: "ws-family
  Retry-After capture knob is adapter-level data, kit extractor shared".
+ Weixin -14 streak escalation (pause→recycle→fatal) extends Hermes' plain
  600s pause minimally to satisfy the required conflict-zombie-eviction row.
+ Timeout-classified send failures are NOT retried inside platform ladders
  (qqbot/weixin/yuanbao) — base.py §6.1 parity over Hermes qqbot's
  retry-everything loop (shared row is the gate).

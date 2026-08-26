# WAVE-2 cluster report — ws-connectors-wire-parity (round-1 conformity)

Cluster: `ws-connectors-wire-parity` · 31 findings applied (slack 1–5, discord
1–7+9, mattermost 1–8, matrix 1–10). Every fix moves pi TOWARD Hermes truth
(anchors cited in-file); fakes/fixtures/rows updated to assert the CONFORMING
behavior in the same change. No contract weakened to go green.

## Files changed

**Slack** — slack-adapter.ts, fake-socket-mode.ts, slack-subject.ts,
slack-fixture.ts, slack.test.ts, conformance/slack-rows.test.ts
**Discord** — discord-adapter.ts, manifest.ts, discord-subject.ts,
discord-fixture.ts, discord.test.ts
**Mattermost** — mattermost-adapter.ts, mm-fake-server.ts,
mattermost-subject.ts, mattermost-world.ts, mattermost-shape-rows.ts,
mattermost.test.ts
**Matrix** — matrix-adapter.ts, manifest.ts, matrix-fake-server.ts,
matrix.test.ts
**Decision log** — ../09-open-questions.md: DEC-048..DEC-052 appended.

## Per-finding resolution

| Finding | Resolution |
| --- | --- |
| slack-1 | REST boundary resolves thread root (`thread_id`→`thread_ts`→`reply_to_message_id`, `_resolve_thread_ts` order) and emits vendor `thread_ts`; internal stamps stripped; stream START args carry it (chat.startStream requires a thread target) |
| slack-2 | Per-envelope acks: adapter answers EVERY events_api envelope with `{type:"ack",envelope_id}` on receipt (slack_bolt parity); fake records them; tests assert ack frames. Cursor stays the durable replay point (engine bookkeeping) |
| slack-3 | sendTyping/stopTyping emit assistant.threads.setStatus `{channel_id,thread_ts,status}` wired at turn start/finalize in handlePlatformEvent; no-thread ⇒ inert (#24117); failures swallowed |
| slack-4 | files_upload_v2-shaped transmitUpload + conversations.open DM resolution (bounded cache) ahead of send/upload; public deliverFile/sendFile surface |
| slack-5 | Invented wire keys dropped (`buttons_removed`, `blocks_dropped_on_retry`, `blocks_cleared_on_retry`); resolve edit REPLACES blocks `[section(original), context(decision)]` (_handle_approval_action); drops tracked as LOCAL audit (`blockRetryAudit`) |
| discord-1 | IDENTIFY sends INTEGER bitmask `DISCORD_IDENTIFY_INTENTS`=53608189 transcribed from discord.py 2.7.1 (Hermes venv ground truth): Intents.default()+message_content (+dm/guild_messages already in default) |
| discord-2 | suppress_embeds stamp removed everywhere; manifest constant deleted; fixture leg(iii) asserts ABSENCE; DEC-048 corrects DEC-034's false premise |
| discord-3 | allowed_mentions serialized in VENDOR shape `{parse:["users"],replied_user:true}` |
| discord-4 | Same safe default stamped on gatedEdit AND every streaming-edit PATCH lane (discord.py Message.edit client-wide default) |
| discord-5 | transmitReaction(add/remove) seam; 👀 on dispatch → remove-then-add ✅/❌ on completion; ledger.markEmojiAck fed on eyes success |
| discord-6 | `auto_archive_duration` (manifest constant); starter id moved to the PATH parameter of transmitThreadCreate |
| discord-7 | Null thread-create branch posts the visible "⚠️ could not create… Please retry" notice before markFailed (:8196-8206) |
| discord-9 | Split continuations chain message_reference `{message_id, fail_if_not_exists:false}`; reply_to_message_id converts to message_reference on the send body |
| mattermost-1 | files plane ported: POST files (multipart) upload, files/{fid}/info + authed download, file_ids classification PHOTO/VOICE/DOCUMENT onto IncomingEvent mediaUrls/mediaTypes/messageType; sendFile outbound lane |
| mattermost-2 | commandRescued bypass REMOVED — gate returns before any command detection; leading-whitespace rescue kept after the gate; row legs flipped |
| mattermost-3 | DEC-050 sanctions the REST-backfill window (endpoint, since cursor, exactly-once dedup, tracked-channel scope) |
| mattermost-4 | Full patch payload {message, props:{disable_mentions:true}} passes BOTH lanes; fake persists props; patchPayloads audit |
| mattermost-5 | Mention strip is case-INSENSITIVE global regex ('gi', escaped pattern) |
| mattermost-6 | Adapter answers server pings with `{action:"pong",seq_reply}`; keepalive shape ratified by DEC-051 |
| mattermost-7 | In-band challenge reply `status:"FAIL"` is FATAL-equivalent alongside 4001 (shared onAuthRejected escalation, OOF-156) |
| mattermost-8 | DEC-049 keeps ladder+notice as ratified delta (shared flood row requires it) |
| matrix-1 | restSend emits m.relates_to: reply→m.in_reply_to, thread_id→m.thread+is_falling_back(+fallback reply), inside `event_content` (the exact PUT body) |
| matrix-2 | wireEdit ships '* '-prefixed body + m.new_content (full rebuild incl. mentions/formatted_body) + m.relates_to replace |
| matrix-3 | m.mentions:{user_ids} INSIDE content (MSC3952); flat m_mentions_user_ids key removed |
| matrix-4 | Markdown→org.matrix.custom.html renderer PORTED (regex-fallback parity: pre-sanitize/escape/protect code+links/hr/headers/quote/lists/inline family/<br> rules); format/formatted_body attach when html≠text — no exclusion DEC needed |
| matrix-5 | Fire-and-forget hs.sendReadReceipt after successful dispatch (post-processing hook) |
| matrix-6 | sender param DROPPED from hs.sendReaction; fake stamps sender server-side from own identity |
| matrix-7 | hs.login surface (identifier/password/device_name/device_id, M_FORBIDDEN on bad password); password branch skips whoami; whoami resolves user_id AND device_id; token requirement satisfied by USER_ID+PASSWORD pair |
| matrix-8 | hs.uploadMedia + sendMedia building typed events field-for-field ({msgtype,body:caption‖filename,info{mimetype,size},url}) |
| matrix-9 | Sync INVITE memberships processed via bounded-retry joinRoom (startup reconciliation + live); fake surfaces rooms.invite and join clears it |
| matrix-10 | DEC-052 logs room-management/presence/history/account-data plane + inbound-media download as scope exclusions |

## Test deltas

Area suites: slack 41 (+6 new contracts incl. acks/thread_ts/status/upload),
discord 34 (+3: emoji lifecycle, message_reference, abort notice),
mattermost 17 (+4: patch props both lanes, files plane, challenge FAIL fatal,
server-ping pong), matrix 31 (+6: mentions/format, relates_to, replace payload,
receipts, media, invites, login). Conformance rows for all four subjects green.

## Full-suite result

- `npx vitest run`: **2530/2530 passed, 219 files** (baseline 2152 grew from
  concurrent clusters' landed work + this cluster's additions)
- `npx tsc --noEmit`: **0 errors**
- `scripts/check-layering.mjs`: OK · `scripts/check-secret-scope.mjs`: OK

Note: parallel conformity clusters share this working tree; my edits are limited
to the four platform areas + their rows/fakes + the DEC log.

// CONFORMANCE WIRING — the BlueBubbles iMessage census port vs the executable
// 04 §8 matrix (DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape='webhook' against the REAL
//      kit-built BlueBubblesSubject. Applicability is COMPUTED from capability
//      data (04 §8 conditional headers): the streaming family applies only
//      when supportsDraftStreaming()/draft_stream_is_message hold —
//      SUPPORTS_MESSAGE_EDITING=False excludes those three rows BY THE PROBE,
//      never by a hardcoded skip.
//   2. The INHERITED webhook transport rows (reference-fixture inheritance,
//      roadmap §Phase 6 heuristic 2) run over the REAL adapter probes:
//      stateless flag pairing + DEC-017 trust-boundary completeness +
//      bounded-window answer measured posting a tokenized webhook while a
//      turn is HELD.
//   3. Fresh bluebubbles shape-delta rows execute through the real engine
//      fixture (auth-token carrier matrix, payload-record variants, filter
//      chain, mention gating, paragraph split + pagination-strip, GUID
//      resolution LRU + #24157 leak guard, reply enrichment matrix,
//      registration lifecycle, receipts/typing gates, trust-boundary
//      completeness + manifest-data integrity).
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: a token-gate-defeating mutant fails ITS OWN named
//      row while the others stay green on fresh fixtures.
//   6. LIE-SCAN: flipping the BB_SUPPORTS_MESSAGE_EDITING datum makes the
//      streaming family RUN and FAIL (the probe honestly follows the data).

import { describe, expect, it } from 'vitest';

import { ManualScheduler } from '../../pi_gateway/guards/testing/manual-spawner.js';
import { FakePlatformWire } from './wire.js';
import { buildSharedRows } from './rows.js';
import type { ConformanceRow } from './rows.js';
import { runConformanceSuite, formatReport } from './runner.js';
import { makeWebhookRows } from './shapes.js';
import type { ConformanceSubject } from './harness.js';
import {
	makeBlueBubblesSubject,
	type BlueBubblesSubject,
} from '../bluebubbles/bluebubbles-subject.js';
import {
	makeBlueBubblesFixture,
	type BlueBubblesFixture,
} from '../bluebubbles/bluebubbles-fixture.js';
import { FIXTURE_BB_PASSWORD } from '../bluebubbles/fixture-secrets.js';
import {
	BB_AUDIO_EXT_OVERRIDES,
	BB_DEFAULT_MENTION_PATTERNS,
	BB_DEFAULT_WEBHOOK_HOST,
	BB_DEFAULT_WEBHOOK_PATH,
	BB_DEFAULT_WEBHOOK_PORT,
	BB_GUID_CACHE_SIZE,
	BB_IMAGE_EXT_OVERRIDES,
	BB_MAX_TEXT_LENGTH,
	BB_MESSAGE_EVENTS,
	BB_SUPPORTS_MESSAGE_EDITING,
	BB_TAPBACK_ADDED,
	BB_TAPBACK_REMOVED,
	BB_WEBHOOK_MAX_BODY_BYTES,
	validateBlueBubblesTrustBoundary,
} from '../bluebubbles/manifest.js';

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: {
		withSecret?: boolean | undefined;
		name?: string | undefined;
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		declaredMessageEditing?: boolean | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeBlueBubblesSubject({
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
		declaredMessageEditing: opts.declaredMessageEditing,
	});
}

/** §8 streaming family — applicable ONLY when draft streaming is supported. */
const STREAMING_ROW_IDS: readonly string[] = [
	'streaming.prefix-mutation-detected',
	'streaming.seal-discipline',
	'streaming.failed-seal-still-delivers',
];

function computeApplicability(
	opts: { declaredMessageEditing?: boolean | undefined } = {},
): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject({
		...(opts.declaredMessageEditing !== undefined
			? { declaredMessageEditing: opts.declaredMessageEditing }
			: {}),
	});
	// Streaming-family applicability keys on the draft-streaming probe alone
	// (the §8 family exercises draft lanes). The msgraph template's extra
	// supportsAsyncDelivery conjunct is inert there (both flags pair false);
	// here the stateless pairing holds regardless, so the DATUM decides.
	const streamsSupported = probe.adapter.supportsDraftStreaming() === true;
	return {
		streamsSupported,
		excludedIds: streamsSupported ? [] : [...STREAMING_ROW_IDS],
	};
}

// ── bluebubbles shape-delta rows (executed over the REAL engine fixture) ────

type TokenCarrier =
	| 'query-password'
	| 'query-guid'
	| 'header-x-password'
	| 'header-x-guid'
	| 'header-x-bluebubbles-guid'
	| 'none';

const ALL_TOKEN_CARRIERS: readonly Exclude<TokenCarrier, 'none'>[] = [
	'query-password',
	'query-guid',
	'header-x-password',
	'header-x-guid',
	'header-x-bluebubbles-guid',
];

function bluebubblesDeltaRows(
	newFixture: () => BlueBubblesFixture,
): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: BlueBubblesFixture) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(['webhook']),
		run: async () => {
			const fx = newFixture();
			try {
				await body(fx);
				return { id, title, pass: true, shapes: new Set(['webhook']) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(['webhook']),
					detail: err instanceof Error ? err.message : String(err),
				};
			} finally {
				fx.dispose();
			}
		},
	});

	return [
		mk(
			'transport.bluebubbles.auth-token-matrix',
			'bluebubbles: correct password accepted via EACH of the five carriers (query password/guid, x-password/x-guid/x-bluebubbles-guid headers); wrong/missing tokens rejected 401 BEFORE the parse seam; constant-time compare is the declared mechanism',
			async (fx) => {
				for (const carrier of ALL_TOKEN_CARRIERS) {
					const before = fx.adapter.counters.dispatched;
					const resp = await fx.postWebhook(fx.messageEvent(), { carrier });
					expect(resp.status).toBe(200);
					expect(resp.text).toBe('ok');
					expect(fx.adapter.counters.dispatched).toBe(before + 1);
				}
				// Wrong token on BOTH carrier kinds.
				const wrongQuery = await fx.postWebhook(fx.messageEvent(), {
					password: 'attacker-guess',
				});
				expect(wrongQuery.status).toBe(401);
				expect(wrongQuery.json['error']).toBe('unauthorized');
				const wrongHeader = await fx.postWebhook(fx.messageEvent(), {
					carrier: 'header-x-bluebubbles-guid',
					password: 'attacker-guess',
				});
				expect(wrongHeader.status).toBe(401);
				// Missing entirely ⇒ 401 (fail closed).
				const missing = await fx.postWebhook(fx.messageEvent(), {
					carrier: 'none',
				});
				expect(missing.status).toBe(401);
				// Empty-carrier values FALL THROUGH the Python-or chain: an empty
				// query password defers to the next position, not instant 401.
				const blankThenHeader = await fx.postRaw({
					query: { password: '' },
					headers: { 'x-guid': FIXTURE_BB_PASSWORD },
					body: JSON.stringify(fx.messageEvent()),
				});
				expect(blankThenHeader.status).toBe(200);

				expect(fx.adapter.counters.unauthorized).toBe(3);
				// THE gate runs BEFORE the parse seam — rejections never decode.
				expect(fx.adapter.counters.parseInvocations).toBe(
					ALL_TOKEN_CARRIERS.length + 1,
				);
			},
		),
		mk(
			'transport.bluebubbles.payload-record-variants',
			'bluebubbles: payload-record variants ingest — data dict, data list FIRST dict, payload.message dict, top-level record; v1.9+ payloads fall back to chats[0].guid; sender-only payloads backfill the chat surface',
			async (fx) => {
				// data dict (default shape).
				let resp = await fx.postWebhook(fx.messageEvent());
				expect(resp.status).toBe(200);
				expect(fx.adapter.counters.dispatched).toBe(1);

				// data LIST — first dict item wins (test_extract_payload_record_
				// accepts_list_data parity).
				resp = await fx.postWebhook({
					type: 'new-message',
					data: [
						{
							guid: 'list-msg',
							text: 'list hello',
							chatGuid: 'iMessage;-;user@example.com',
							handle: { address: 'user@example.com' },
							isFromMe: false,
						},
						{ not: 'this one' },
					],
				});
				expect(resp.status).toBe(200);
				expect(fx.adapter.turnLog.at(-1)).toBe('list hello');

				// payload.message dict form.
				resp = await fx.postWebhook({
					type: 'new-message',
					message: {
						guid: 'mform-msg',
						text: 'message-key hello',
						chatGuid: 'iMessage;-;user@example.com',
						handle: { address: 'user@example.com' },
						isFromMe: false,
					},
				});
				expect(resp.status).toBe(200);
				expect(fx.adapter.turnLog.at(-1)).toBe('message-key hello');

				// Top-level record IS the payload (extract returns payload itself).
				resp = await fx.postWebhook({
					type: 'new-message',
					guid: 'top-msg',
					text: 'top-level hello',
					chatGuid: 'iMessage;-;user@example.com',
					handle: { address: 'user@example.com' },
					isFromMe: false,
				});
				expect(resp.status).toBe(200);
				expect(fx.adapter.turnLog.at(-1)).toBe('top-level hello');

				// v1.9+ shape: NO chatGuid anywhere except data.chats[0].guid.
				resp = await fx.postWebhook({
					type: 'new-message',
					data: {
						guid: 'v19-msg',
						text: 'nested hello',
						chats: [{ guid: 'iMessage;-;fallback-chat' }],
						handle: { address: 'fallback@example.com' },
						isFromMe: false,
					},
				});
				expect(resp.status).toBe(200);
				const nested = fx.adapter.dispatchedEvents.at(-1);
				expect(nested?.source?.chatId).toBe('iMessage;-;fallback-chat');
				expect(nested?.text).toBe('nested hello');

				// Sender-only: neither chat field present ⇒ sender backfills the
				// identifier slot (test_webhook_can_fall_back_to_sender_… parity).
				resp = await fx.postWebhook({
					type: 'new-message',
					data: {
						guid: 'sender-only',
						text: 'hello from sender',
						handle: { address: 'user@example.com' },
						isFromMe: false,
					},
				});
				expect(resp.status).toBe(200);
				const senderOnly = fx.adapter.dispatchedEvents.at(-1);
				expect(senderOnly?.source?.chatId).toBe('user@example.com');
				expect(fx.adapter.counters.missingFields).toBe(0);
			},
		),
		mk(
			'transport.bluebubbles.event-filter-chain',
			'bluebubbles: non-message event types ack-drop 200; ABSENT event type falls THROUGH (source quirk); isFromMe (three spellings) drops; tapbacks 2000/3003 dropped; missing sender/chat/text ⇒ 400',
			async (fx) => {
				// Non-message event type: acknowledged, never dispatched.
				let resp = await fx.postWebhook({ type: 'ping' });
				expect(resp.status).toBe(200);
				expect(resp.text).toBe('ok');
				expect(fx.adapter.counters.eventFiltered).toBe(1);

				// Alt event key + member event processes.
				resp = await fx.postWebhook({
					event: 'updated-message',
					data: {
						guid: 'upd-1',
						text: 'updated delivery',
						chatGuid: 'iMessage;-;user@example.com',
						handle: { address: 'user@example.com' },
						isFromMe: false,
					},
				});
				expect(resp.status).toBe(200);
				expect(fx.adapter.counters.dispatched).toBe(1);

				// ABSENT event type falls THROUGH the filter (source: `if
				// event_type and …`) — the message still ingests.
				resp = await fx.postWebhook({
					data: {
						guid: 'notype-1',
						text: 'no event type',
						chatGuid: 'iMessage;-;user@example.com',
						handle: { address: 'user@example.com' },
						isFromMe: false,
					},
				});
				expect(resp.status).toBe(200);
				expect(fx.adapter.counters.eventFiltered).toBe(1);

				// Self-authored echoes drop under all three spellings.
				for (const [idx, spelling] of [
					'isFromMe',
					'fromMe',
					'is_from_me',
				].entries()) {
					const base = fx.messageEvent({ guid: `self-${idx}` });
					const data = (base as { data: Record<string, unknown> }).data;
					delete data.isFromMe;
					data[spelling] = true;
					const drop = await fx.postWebhook(base);
					expect(drop.status).toBe(200);
				}
				expect(fx.adapter.counters.fromMeDropped).toBe(3);

				// Tapbacks ride as messages with associatedMessageType codes.
				for (const [idx, code] of [2000, 3003].entries()) {
					const drop = await fx.postWebhook(
						fx.messageEvent({
							guid: `tapback-${idx}`,
							associatedMessageType: code,
						}),
					);
					expect(drop.status).toBe(200);
				}
				expect(fx.adapter.counters.tapbackDropped).toBe(2);

				// Missing everything ⇒ 400 missing message fields.
				const empty = await fx.postWebhook({});
				expect(empty.status).toBe(400);
				expect(empty.json['error']).toBe('missing message fields');
				// Chat + sender present but NO text ⇒ 400 too.
				const noText = await fx.postWebhook(
					fx.messageEvent({ text: undefined }),
				);
				expect(noText.status).toBe(400);
				expect(noText.json['error']).toBe('missing message fields');
				expect(fx.adapter.counters.missingFields).toBe(2);
			},
		),
		mk(
			'transport.bluebubbles.mention-gating',
			'bluebubbles: require_mention drops group chatter without a wake word (200 ack); leading wake word STRIPPED through punctuation; agent-pattern preferred; MID-text wake words preserved; custom patterns compile IGNORECASE (invalid ones skip); DMs unaffected',
			async (fx) => {
				// require_mention ON; receipts off (they have their own row).
				const g = makeBlueBubblesFixture({
					config: { require_mention: true, send_read_receipts: false },
				});
				void fx;
				// Source regression: group chatter without mention ack-drops.
				let resp = await g.postWebhook(g.groupEvent());
				expect(resp.status).toBe(200);
				expect(g.adapter.turnLog.includes('casual family chatter')).toBe(false);
				expect(g.adapter.counters.mentionDropped).toBe(1);

				// Bare wake word LEADING: stripped, punctuation lstrip(' ,:-').
				resp = await g.postWebhook(
					g.groupEvent({ text: '@hermes deploy the fix' }),
				);
				expect(resp.status).toBe(200);
				expect(g.adapter.turnLog.at(-1)).toBe('deploy the fix');

				// Agent pattern wins over bare: strips through ', '.
				resp = await g.postWebhook(
					g.groupEvent({ text: 'hermes agent, run diagnostics' }),
				);
				expect(resp.status).toBe(200);
				expect(g.adapter.turnLog.at(-1)).toBe('run diagnostics');

				// Mid-text occurrence: search matches so the gate PASSES, but only
				// a LEADING match would strip — ordinary words survive verbatim.
				resp = await g.postWebhook(
					g.groupEvent({ text: 'hey @hermes help me' }),
				);
				expect(resp.status).toBe(200);
				expect(g.adapter.turnLog.at(-1)).toBe('hey @hermes help me');

				// DMs are NEVER mention-gated.
				resp = await g.postWebhook(g.messageEvent());
				expect(resp.status).toBe(200);
				expect(g.adapter.turnLog.at(-1)).toBe('hello from iMessage');

				// Custom patterns: JSON-list env shape, compiled IGNORECASE.
				const amos = makeBlueBubblesFixture({
					config: {
						require_mention: true,
						send_read_receipts: false,
						mention_patterns: '["^amos\\\\b"]',
					},
				});
				const amosPass = await amos.postWebhook(
					amos.groupEvent({ text: 'AMOS status now' }),
				);
				expect(amosPass.status).toBe(200);
				expect(amos.adapter.turnLog.at(-1)).toBe('status now');
				amos.dispose();

				// Invalid regex entries warn-and-SKIP: empty pattern set ⇒ every
				// group message drops even WITH the vendor wake word.
				const broken = makeBlueBubblesFixture({
					config: {
						require_mention: true,
						send_read_receipts: false,
						mention_patterns: '([unclosed',
					},
				});
				const dropped = await broken.postWebhook(
					broken.groupEvent({ text: 'hermes anything' }),
				);
				expect(dropped.status).toBe(200);
				expect(broken.adapter.turnLog.length).toBe(0);
				broken.dispose();
			},
		),
		mk(
			'transport.bluebubbles.paragraph-split-pagination',
			'bluebubbles: multi-paragraph sends become SEPARATE bubbles; >4000-char paragraphs truncate with pagination suffix REMOVED and zero byte loss; markdown stripped via the helpers.py ladder; snake_case identifiers survive',
			async (fx) => {
				await expect(fx.connect()).resolves.toBe(true);

				// Each thought its own iMessage bubble.
				const multi = await fx.adapter.sendText(
					'iMessage;-;engine',
					"first thought\n\nsecond thought\n\nthird",
				);
				expect(multi.success).toBe(true);
				expect(fx.server.messageTextCalls.length).toBe(3);
				expect(fx.server.messageTextCalls[0]?.payload['message']).toBe(
					'first thought',
				);
				expect(fx.server.messageTextCalls[1]?.payload['message']).toBe(
					'second thought',
				);
				expect(fx.server.messageTextCalls[2]?.payload['message']).toBe('third');

				// Markdown stripped (headers/bold/inline-code), newlines kept.
				await fx.adapter.sendText(
					'iMessage;-;engine',
					"## Heading\n**bold** and `code`",
				);
				expect(fx.server.messageTextCalls.at(-1)?.payload['message']).toBe(
					"Heading\nbold and code",
				);

				// Underscores in identifiers survive (source regression parity).
				const identifiers =
					'Use /api_v2 with FEATURE_FLAG_NAME and config_file.json';
				expect(fx.adapter.formatMessage(identifiers)).toBe(identifiers);

				// Oversized paragraph: truncated WITHOUT pagination suffixes and
				// byte-preserved across parts.
				const callsBefore = fx.server.messageTextCalls.length;
				const para = 'x'.repeat(BB_MAX_TEXT_LENGTH + 500);
				const big = await fx.adapter.sendText('iMessage;-;engine', para);
				expect(big.success).toBe(true);
				const parts = fx.server.messageTextCalls
					.slice(callsBefore)
					.map((c) => String(c.payload['message']))
					.filter((m) => /^x+$/.test(m));
				expect(parts.length).toBeGreaterThanOrEqual(2);
				const joined = parts.join('');
				expect(joined.length).toBe(para.length); // zero byte loss
				expect(joined).toBe(para); // byte-exact round trip
				for (const part of parts) {
					expect(part.length).toBeLessThanOrEqual(BB_MAX_TEXT_LENGTH);
					// Pagination suffix REMOVED: no '(i/n)' tail anywhere.
					expect(/\(\d+\/\d+\)$/.test(part)).toBe(false);
				}
			},
		),
	];
}

/**
 * Second delta-row batch: scenarios needing their OWN fixture shapes (small
 * LRU caps, private_api toggles, seeded registries).
 */
function bluebubblesDeltaRowsPart2(): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: BlueBubblesFixture) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(['webhook']),
		run: async () => {
			const fx = makeBlueBubblesFixture();
			try {
				await body(fx);
				return { id, title, pass: true, shapes: new Set(['webhook']) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(['webhook']),
					detail: err instanceof Error ? err.message : String(err),
				};
			} finally {
				fx.dispose();
			}
		},
	});

	return [
		mk(
			'transport.bluebubbles.guid-resolution',
			"bluebubbles: ';' targets pass through raw with zero roundtrips; strict identifier match caches LRU-style (move-to-end, injected smaller cap evicts oldest, misses stay UNCACHED); participant-only matches MUST NOT resolve (#24157 leak guard); create-chat fires ONLY for address-like targets with private_api",
			async () => {
				// Raw GUID passthrough + strict-match cache hit.
				const fx = makeBlueBubblesFixture();
				const direct = await fx.adapter.resolveChatGuid('iMessage;-;direct');
				expect(direct).toBe('iMessage;-;direct');
				expect(fx.server.chatQueryCalls.length).toBe(0);
				fx.server.seedChat({
					guid: 'iMessage;-;user@example.com',
					chatIdentifier: 'user@example.com',
				});
				const first = await fx.adapter.resolveChatGuid('user@example.com');
				expect(first).toBe('iMessage;-;user@example.com');
				await expect(
					fx.adapter.resolveChatGuid('user@example.com'),
				).resolves.toBe(first);
				expect(fx.server.chatQueryCalls.length).toBe(1); // served by cache
				// chatGuid-field fallback (guid absent, chatGuid present).
				fx.server.seedChat({ chatGuid: 'G2', chatIdentifier: 'c2' });
				await expect(fx.adapter.resolveChatGuid('c2')).resolves.toBe('G2');
				fx.dispose();

				// LRU eviction observable with an injected cap-2 cache.
				const lru = makeBlueBubblesFixture({
					config: { guid_cache_size: 2 },
					chats: [
						{ guid: 'G-a', chatIdentifier: 'a' },
						{ guid: 'G-b', chatIdentifier: 'b' },
						{ guid: 'G-c', chatIdentifier: 'c' },
					],
				});
				await expect(lru.adapter.resolveChatGuid('a')).resolves.toBe('G-a');
				await expect(lru.adapter.resolveChatGuid('b')).resolves.toBe('G-b');
				expect(lru.adapter.guidCacheSnapshot()).toEqual(['a', 'b']);
				// Third insertion evicts the OLDEST (popitem(last=False)).
				await expect(lru.adapter.resolveChatGuid('c')).resolves.toBe('G-c');
				expect(lru.adapter.guidCacheSnapshot()).toEqual(['b', 'c']);
				// Hit moves b to the end.
				await expect(lru.adapter.resolveChatGuid('b')).resolves.toBe('G-b');
				expect(lru.adapter.guidCacheSnapshot()).toEqual(['c', 'b']);
				lru.dispose();

				// #24157 leak guard: the contact appears ONLY as a group
				// PARTICIPANT — a DM reply must never select the group GUID.
				const guard = makeBlueBubblesFixture({
					chats: [
						{
							guid: 'iMessage;+;chat0000000000-family-group',
							chatIdentifier: 'chat0000000000',
							participants: [
								{ address: 'user@example.com' },
								{ address: '+15555550100' },
							],
						},
					],
				});
				await expect(
					guard.adapter.resolveChatGuid('user@example.com'),
				).resolves.toBeNull();
				// Unresolved targets are NOT cached (stale-miss companion fix).
				expect(guard.adapter.guidCacheSnapshot()).toEqual([]);
				guard.dispose();

				// Create-chat happy path: private_api + address-like target.
				const creator = makeBlueBubblesFixture({ privateApi: true });
				await expect(creator.connect()).resolves.toBe(true);
				const created = await creator.adapter.sendText(
					'+15551234567',
					'hi there',
				);
				expect(created.success).toBe(true);
				expect(creator.server.chatNewCalls.length).toBe(1);
				const newPayload = creator.server.chatNewCalls[0]?.payload ?? {};
				expect(newPayload['addresses']).toEqual(['+15551234567']);
				expect(newPayload['message']).toBe('hi there');
				expect(String(newPayload['tempGuid'])).toMatch(/^temp-/);
				creator.dispose();

				// private_api OFF ⇒ never guess-create; fail instead.
				const noPrivate = makeBlueBubblesFixture({ privateApi: false });
				await expect(noPrivate.connect()).resolves.toBe(true);
				const refused = await noPrivate.adapter.sendText('+15551234567', 'hi');
				expect(refused.success).toBe(false);
				expect(refused.error).toBe(
					'BlueBubbles chat not found for target: +15551234567',
				);
				expect(noPrivate.server.chatNewCalls.length).toBe(0);

				// Non-address-like target with private_api ON ⇒ also refused.
				const plain = await noPrivate.adapter.sendText('plainchat', 'hi');
				expect(plain.success).toBe(false);
				noPrivate.dispose();
			},
		),
		mk(
			'transport.bluebubbles.reply-enrichment-matrix',
			'bluebubbles: method=private-api + selectedMessageGuid + partIndex=0 attach IFF reply_to AND private_api AND helper_connected; plain sends never carry them (full 2×2 boolean matrix)',
			async () => {
				for (const privateApi of [true, false]) {
					for (const helperConnected of [true, false]) {
						const fx = makeBlueBubblesFixture({
							privateApi,
							helperConnected,
							chats: [{ guid: 'iMessage;-;r', chatIdentifier: 'r' }],
						});
						await expect(fx.connect()).resolves.toBe(true);
						const enrichable = privateApi && helperConnected;

						const replied = await fx.adapter.sendText(
							'iMessage;-;r',
							'body',
							'parent-guid',
						);
						expect(replied.success).toBe(true);
						const enriched = fx.server.messageTextCalls.at(-1)?.payload ?? {};
						if (enrichable) {
							expect(enriched['method']).toBe('private-api');
							expect(enriched['selectedMessageGuid']).toBe('parent-guid');
							expect(enriched['partIndex']).toBe(0);
						} else {
							expect(enriched['method']).toBeUndefined();
							expect(enriched['selectedMessageGuid']).toBeUndefined();
							expect(enriched['partIndex']).toBeUndefined();
						}

						// Plain send NEVER enriches regardless of flags.
						await fx.adapter.sendText('iMessage;-;r', 'plain');
						const plain = fx.server.messageTextCalls.at(-1)?.payload ?? {};
						expect(plain['method']).toBeUndefined();
						expect(plain['selectedMessageGuid']).toBeUndefined();
						expect(plain['partIndex']).toBeUndefined();
						fx.dispose();
					}
				}
			},
		),
		mk(
			'transport.bluebubbles.registration-lifecycle',
			'bluebubbles: fresh connect POSTs the registration with the message-event subset; crash-resilient REUSE skips the POST; disconnect deletes ALL duplicates sparing foreign entries; register URL embeds the quoted password while the log-safe variant masks it; server_url/webhook_path normalize',
			async () => {
				// Fresh registration.
				const fresh = makeBlueBubblesFixture();
				expect(fresh.adapter.webhookUrl).toBe(
					'http://localhost:8645/bluebubbles-webhook',
				);
				const quotedPassword = encodeURIComponent(FIXTURE_BB_PASSWORD).replace(
					/[!'()*]/g,
					(c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,
				);
				expect(fresh.adapter.webhookRegisterUrl).toBe(
					`http://localhost:8645/bluebubbles-webhook?password=${quotedPassword}`,
				);
				// Log-safe variant masks the credential.
				expect(fresh.adapter.webhookRegisterUrlForLog).toBe(
					'http://localhost:8645/bluebubbles-webhook?password=***',
				);
				expect(
					fresh.adapter.webhookRegisterUrlForLog.includes(FIXTURE_BB_PASSWORD),
				).toBe(false);

				await expect(fresh.connect()).resolves.toBe(true);
				expect(fresh.server.registerWebhookCalls.length).toBe(1);
				const reg = fresh.server.registerWebhookCalls[0]?.payload ?? {};
				expect(reg['url']).toBe(fresh.adapter.webhookRegisterUrl);
				expect(reg['events']).toEqual(['new-message', 'updated-message']);
				// Disconnect unregisters.
				await fresh.adapter.disconnect();
				expect(fresh.server.deletedWebhookIds.length).toBe(1);
				fresh.dispose();

				// Crash resilience: existing matching registration reused, no POST.
				const crashed = makeBlueBubblesFixture();
				crashed.server.seedWebhook({ url: crashed.adapter.webhookRegisterUrl });
				await expect(crashed.connect()).resolves.toBe(true);
				expect(crashed.server.registerWebhookCalls.length).toBe(0);
				await crashed.adapter.disconnect();
				crashed.dispose();

				// Duplicates left by prior crashes: disconnect removes ALL of them,
				// leaving unrelated registrations alone.
				const dupes = makeBlueBubblesFixture();
				dupes.server.seedWebhook({ url: dupes.adapter.webhookRegisterUrl });
				dupes.server.seedWebhook({ url: dupes.adapter.webhookRegisterUrl });
				dupes.server.seedWebhook({ url: 'http://other:9999/hook' });
				await expect(dupes.connect()).resolves.toBe(true);
				await dupes.adapter.disconnect();
				expect(dupes.server.deletedWebhookIds.length).toBe(2);
				// The foreign registration survives untouched.
				expect(dupes.server.webhookUrls()).toEqual(['http://other:9999/hook']);
				dupes.dispose();

				// Normalization parity (source tests): scheme-less http:// prefix,
				// trailing slash stripped, leading slash added to paths.
				const normalized = makeBlueBubblesFixture({
					config: {
						server_url: 'localhost:1234/',
						webhook_path: 'bluebubbles-webhook',
					},
				});
				expect(normalized.adapter.serverUrl).toBe('http://localhost:1234');
				expect(normalized.adapter.webhookPath).toBe('/bluebubbles-webhook');
				normalized.dispose();
			},
		),
		mk(
			'transport.bluebubbles.receipts-typing-gates',
			'bluebubbles: typing/read REST calls fire ONLY when private_api AND helper_connected (full 2×2 matrix); ingress schedules a fire-and-forget read receipt when send_read_receipts=true; disabled receipts never fire',
			async () => {
				for (const privateApi of [true, false]) {
					for (const helperConnected of [true, false]) {
						const fx = makeBlueBubblesFixture({ privateApi, helperConnected });
						await expect(fx.connect()).resolves.toBe(true);
						const gated = privateApi && helperConnected;
						await fx.adapter.sendTyping('iMessage;-;t');
						await fx.adapter.stopTyping('iMessage;-;t');
						const read = await fx.adapter.markRead('iMessage;-;t');
						expect(read).toBe(gated);
						expect(fx.server.typingCalls).toEqual(
							gated ? ['iMessage;-;t'] : [],
						);
						expect(fx.server.stopTypingCalls).toEqual(
							gated ? ['iMessage;-;t'] : [],
						);
						expect(fx.server.readCalls).toEqual(gated ? ['iMessage;-;t'] : []);
						fx.dispose();
					}
				}

				// Ingress-driven fire-and-forget receipt (default enabled).
				const fx = makeBlueBubblesFixture({
					chats: [
						{ guid: 'iMessage;-;dm-user', chatIdentifier: 'user@example.com' },
					],
				});
				await expect(fx.connect()).resolves.toBe(true);
				const resp = await fx.postWebhook(fx.messageEvent());
				expect(resp.status).toBe(200);
				expect(fx.adapter.counters.readReceiptsRequested).toBe(1);
				await fx.settle();
				// The inbound chatGuid rides the session id verbatim (';' ⇒ raw
				// passthrough, no roster needed).
				expect(fx.server.readCalls).toEqual(['iMessage;-;user@example.com']);
				fx.dispose();

				// Disabled: dispatch happens, receipt never fires.
				const quiet = makeBlueBubblesFixture({
					config: { send_read_receipts: false },
					chats: [
						{ guid: 'iMessage;-;dm-user', chatIdentifier: 'user@example.com' },
					],
				});
				const quietResp = await quiet.postWebhook(quiet.messageEvent());
				expect(quietResp.status).toBe(200);
				expect(quiet.adapter.counters.dispatched).toBe(1);
				await quiet.settle();
				expect(quiet.server.readCalls).toEqual([]);
				quiet.dispose();

				// Gated off downstream: requested but private_api OFF swallows it.
				const dark = makeBlueBubblesFixture({
					privateApi: false,
					chats: [
						{ guid: 'iMessage;-;dm-user', chatIdentifier: 'user@example.com' },
					],
				});
				await dark.postWebhook(dark.messageEvent());
				await dark.settle();
				expect(dark.server.readCalls).toEqual([]);
				dark.dispose();
			},
		),
		mk(
			'transport.bluebubbles.trust-boundary-complete',
			'bluebubbles: local DEC-017 validator clean on the REAL boundary; every mutation produces its NAMED error; manifest constants byte-exact (caps, tapback maps, event set, VERBATIM vendor wake-word patterns); stateless flag pairing + probe-fed streaming exclusion; smallest-honest idempotency bound',
			async (fx) => {
				const boundary = fx.adapter.trustBoundary;
				expect(validateBlueBubblesTrustBoundary(boundary)).toEqual([]);

				// Mutations produce NAMED errors.
				const mutations: Array<[string, Record<string, unknown>]> = [
					['bbPasswordTokenCompare', { bbPasswordTokenCompare: false }],
					['constantTimeCompare', { constantTimeCompare: false }],
					['bodySizeCapBytes', { bodySizeCapBytes: 0 }],
					['idempotency', { idempotency: undefined }],
					[
						'home-directory confinement',
						{ scriptTransformsConfinedToHome: false },
					],
				];
				for (const [needle, patch] of mutations) {
					const mutated = { ...boundary, ...patch } as typeof boundary;
					const errors = validateBlueBubblesTrustBoundary(mutated);
					expect(errors.length).toBeGreaterThanOrEqual(1);
					expect(errors.some((e) => e.includes(needle))).toBe(true);
				}

				// Declared boundary DATA.
				expect(boundary.signatureSchemes).toEqual([]); // no HMAC on this wire
				expect(boundary.bodySizeCapBytes).toBe(BB_WEBHOOK_MAX_BODY_BYTES);
				expect(BB_WEBHOOK_MAX_BODY_BYTES).toBe(1_048_576);
				expect(boundary.backpressureWindow).toBe('bounded');
				expect(boundary.constantTimeCompare).toBe(true);
				// Smallest HONEST bound: Hermes declares no dedupe machinery —
				// inventing a seen-set would fabricate behavior (proposed DEC).
				expect(boundary.idempotency?.seenSetMaxEntries).toBe(1);

				// Manifest constants (verified against the READ-ONLY source).
				expect(BB_MAX_TEXT_LENGTH).toBe(4000);
				expect(BB_GUID_CACHE_SIZE).toBe(500);
				expect(BB_DEFAULT_WEBHOOK_HOST).toBe('127.0.0.1');
				expect(BB_DEFAULT_WEBHOOK_PORT).toBe(8645);
				expect(BB_DEFAULT_WEBHOOK_PATH).toBe('/bluebubbles-webhook');
				expect([...BB_MESSAGE_EVENTS].sort()).toEqual([
					'message',
					'new-message',
					'updated-message',
				]);
				expect(BB_TAPBACK_ADDED).toEqual({
					2000: 'love',
					2001: 'like',
					2002: 'dislike',
					2003: 'laugh',
					2004: 'emphasize',
					2005: 'question',
				});
				expect(BB_TAPBACK_REMOVED).toEqual({
					3000: 'love',
					3001: 'like',
					3002: 'dislike',
					3003: 'laugh',
					3004: 'emphasize',
					3005: 'question',
				});
				// CLOSED historical mime→ext override maps.
				expect(BB_IMAGE_EXT_OVERRIDES['image/heic']).toBe('.jpg');
				expect(BB_IMAGE_EXT_OVERRIDES['image/tiff']).toBe('.jpg');
				expect(Object.keys(BB_IMAGE_EXT_OVERRIDES).length).toBe(7);
				expect(BB_AUDIO_EXT_OVERRIDES['audio/x-caf']).toBe('.mp3');
				expect(BB_AUDIO_EXT_OVERRIDES['audio/aac']).toBe('.m4a');
				expect(BB_AUDIO_EXT_OVERRIDES['audio/mp4']).toBe('.m4a');
				// VERBATIM vendor wake words (see manifest DATA NOTE / proposed
				// DEC): byte-exact against the Hermes source strings.
				expect([...BB_DEFAULT_MENTION_PATTERNS]).toEqual([
					"(?<![\\w@])@?hermes\\s+agent\\b[:,\\-]?",
					"(?<![\\w@])@?hermes\\b[:,\\-]?",
				]);
				// The streaming exclusion rides the DATUM, and the probe follows.
				expect(BB_SUPPORTS_MESSAGE_EDITING).toBe(false);
				expect(fx.adapter.supportsDraftStreaming()).toBe(false);
				expect(fx.adapter.supportsAsyncDelivery).toBe(false);
				expect(fx.adapter.interactiveResume).toBe(false);
				expect(fx.adapter.splitsLongMessages).toBe(true);
			},
		),
	];
}

describe('conformance suite — bluebubbles census port (shape: webhook)', () => {
	it('applicability is COMPUTED from capability data (streaming family excluded iff SUPPORTS_MESSAGE_EDITING=False)', () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // no edit API ⇒ no draft cursor
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it('passes EVERY applicable shared row against the bluebubbles subject', async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: 'bluebubbles',
			shape: 'webhook',
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it('passes the INHERITED webhook transport rows (reference fixture) over the REAL adapter', async () => {
		const subject = makeSubject() as BlueBubblesSubject;
		const probe = subject.flagsAndTrustProbe();

		const fx = makeBlueBubblesFixture();
		try {
			// HOLD the fixture adapter's turns — the webhook must ack FAST
			// even while the guard's turn handler is parked (bounded window).
			fx.adapter.holdTurns(true);
			const startedAt = Date.now();
			const resp = await fx.postWebhook(fx.messageEvent());
			const elapsed = Date.now() - startedAt;
			expect(resp.status).toBe(200); // acked FAST even with the turn held
			expect(elapsed).toBeLessThan(5_000);
			fx.adapter.holdTurns(false);

			const rows = makeWebhookRows({
				async flagsAndTrust() {
					return probe;
				},
				async boundedWindowAnswer() {
					return {
						answeredWithinWindowMs: elapsed,
						windowCapMs: 5_000,
					};
				},
			});
			const report = await runConformanceSuite({
				subjectName: 'bluebubbles-shape',
				shape: 'webhook',
				rows,
				suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
			});
			if (report.failed > 0) console.error(formatReport(report));
			expect(report.failed).toBe(0);
			expect(report.deferred).toEqual([]);
		} finally {
			fx.dispose();
		}
	});

	it('passes ALL TEN bluebubbles shape-delta rows through the real engine fixture', async () => {
		const rows = [
			...bluebubblesDeltaRows(() => makeBlueBubblesFixture()),
			...bluebubblesDeltaRowsPart2(),
		];
		expect(rows.map((r) => r.id)).toEqual([
			'transport.bluebubbles.auth-token-matrix',
			'transport.bluebubbles.payload-record-variants',
			'transport.bluebubbles.event-filter-chain',
			'transport.bluebubbles.mention-gating',
			'transport.bluebubbles.paragraph-split-pagination',
			'transport.bluebubbles.guid-resolution',
			'transport.bluebubbles.reply-enrichment-matrix',
			'transport.bluebubbles.registration-lifecycle',
			'transport.bluebubbles.receipts-typing-gates',
			'transport.bluebubbles.trust-boundary-complete',
		]);
		expect(rows.length).toBe(10);

		const report = await runConformanceSuite({
			subjectName: 'bluebubbles-deltas',
			shape: 'webhook',
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 60_000);

	it('FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)', async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));

		const subject = makeSubject() as BlueBubblesSubject;
		const probe = subject.flagsAndTrustProbe();
		const transport = makeWebhookRows({
			async flagsAndTrust() {
				return probe;
			},
			async boundedWindowAnswer() {
				return { answeredWithinWindowMs: 12, windowCapMs: 5_000 };
			},
		});
		const deltas = [
			...bluebubblesDeltaRows(() => makeBlueBubblesFixture()),
			...bluebubblesDeltaRowsPart2(),
		];

		const report = await runConformanceSuite({
			subjectName: 'bluebubbles-full',
			shape: 'webhook',
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 60_000);

	it('the gate DETECTS violations: a token-gate-defeating mutant fails ITS OWN named row', async () => {
		// Mutant: the auth-token gate ALWAYS accepts (as if secureCompare were
		// stubbed true) — the auth matrix row must fail BY NAME.
		const rows = bluebubblesDeltaRows(() => {
			const fx = makeBlueBubblesFixture();
			const original = fx.adapter.handleWebhookPost.bind(fx.adapter);
			Object.defineProperty(fx.adapter, 'handleWebhookPost', {
				value: async (input: Parameters<typeof original>[0]) =>
					original({
						...input,
						query: {
							...(input.query ?? {}),
							password: FIXTURE_BB_PASSWORD, // the lie
						},
					}),
			});
			return fx;
		});

		const authRow = rows.find(
			(r) => r.id === 'transport.bluebubbles.auth-token-matrix',
		);
		expect(authRow).toBeDefined();
		const authReport = await runConformanceSuite({
			subjectName: 'mutant-bb-auth',
			shape: 'webhook',
			rows: [authRow as ConformanceRow],
		});
		expect(authReport.failed).toBe(1);
		expect(authReport.rows[0]?.pass).toBe(false);

		// Sanity: the OTHER rows still pass on their own fresh fixtures.
		const others = rows.filter((r) => r.id !== authRow?.id);
		const otherReport = await runConformanceSuite({
			subjectName: 'mutant-bb-others',
			shape: 'webhook',
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 60_000);

	it('LIE-SCAN: flipping the SUPPORTS_MESSAGE_EDITING datum makes the streaming rows RUN and FAIL', async () => {
		// With the datum flipped TRUE the probe reports streaming support and
		// the streaming family is INCLUDED — and fails, because this surface
		// has no native draft lanes to seal.
		const flipped = computeApplicability({ declaredMessageEditing: true });
		expect(flipped.streamsSupported).toBe(true);
		expect(flipped.excludedIds).toEqual([]);

		const all = buildSharedRows({
			makeSubject: (opts) =>
				makeSubject({ ...opts, declaredMessageEditing: true }),
		});
		const streamingRows = all.filter((r) => STREAMING_ROW_IDS.includes(r.id));
		expect(streamingRows.length).toBe(STREAMING_ROW_IDS.length);
		const report = await runConformanceSuite({
			subjectName: 'lie-scan-bb-streaming',
			shape: 'webhook',
			rows: streamingRows as ConformanceRow[],
		});
		expect(report.failed).toBe(STREAMING_ROW_IDS.length);
		for (const id of STREAMING_ROW_IDS) {
			expect(report.rows.find((r) => r.id === id)?.pass).toBe(false);
		}

		// Control: the honest datum keeps them excluded.
		const honest = computeApplicability();
		expect(honest.streamsSupported).toBe(false);
	}, 60_000);
});

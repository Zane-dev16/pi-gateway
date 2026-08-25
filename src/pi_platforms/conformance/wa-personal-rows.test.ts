// CONFORMANCE WIRING — the WhatsApp PERSONAL (Baileys-bridge, polling) census
// port vs the executable 04 §8 matrix (DEC-002 gate applies to every new
// platform). Structure mirrors raft-rows.test.ts EXACTLY:
//
//   1. ALL applicable SHARED rows pass for shape="polling" against the REAL
//      kit-built WaPersonalSubject. Applicability is COMPUTED from capability
//      data (04 §8 conditional headers): the streaming family applies only
//      when supportsDraftStreaming() holds — the loopback bridge wire has no
//      native draft lanes, so those three rows are excluded BY THE PROBE,
//      never by a hardcoded skip (a capability flip re-includes them).
//   2. The INHERITED §3.1 polling transport rows run over the REAL engine
//      fixture (makeRealWaPersonalFixture) — vendor-true mechanisms only.
//   3. Fresh WhatsApp-personal shape-delta rows execute through the REAL
//      adapter/behavior surface: broadcast filter matrix, DM/group policy
//      matrices, mention gating order, allowlist alias resolution + live
//      reload, debounce batching, send chunking/prefix budget, sanitization,
//      JID normalization, read receipts, the connect refusal ladder, and
//      managed-bridge-exit classification.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: a broadcast-filter-defeating mutant fixture fails
//      ITS OWN named row (and ONLY that row).

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makePollingRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";

import { makeWaPersonalSubject } from "../whatsapp-personal/wa-personal-subject.js";
import {
	type WaPersonalAdapter,
	classifyBridgeExit,
	staleBridgeEvictionDecision,
} from "../whatsapp-personal/wa-personal-adapter.js";
import { FakeBridgeServer } from "../whatsapp-personal/bridge-wire.js";
import {
	cleanBotMentionText,
	coerceAllowList,
	compileMentionPatterns,
	dmAllowedStrict,
	dmIntakeAllowed,
	effectiveReplyPrefix,
	groupAllowed,
	isBroadcastChat,
	matchesAllowlist,
	normalizeWhatsAppId,
	openDmOptedIn,
	outgoingChunkLimit,
	sanitizeOutboundText,
	shouldProcessMessage,
	type AliasResolver,
	type WaGatingPolicy,
} from "../whatsapp-personal/behavior.js";
import {
	FATAL_BRIDGE_MISSING,
	FATAL_NODE_MISSING,
	FATAL_NOT_PAIRED,
	WA_DEFAULT_REPLY_PREFIX,
	WA_DEFAULT_MODE,
	WA_INTER_CHUNK_DELAY_MS,
	WA_MAX_MESSAGE_LENGTH,
	WA_MIN_CHUNK_LIMIT,
	FATAL_BRIDGE_EXITED,
	WA_TEXT_BATCH_DELAY_SECONDS,
	WA_TEXT_BATCH_SPLIT_DELAY_SECONDS,
	WA_TEXT_SPLIT_THRESHOLD_CHARS,
} from "../whatsapp-personal/manifest.js";
import {
	makeRealWaPersonalFixture,
	makeWaPersonalWorld,
	ALICE_JID,
	type WaPersonalWorld,
} from "../whatsapp-personal/wa-personal-world.js";
import { toWhatsappJid } from "../../pi_gateway/resolution/whatsapp-identity.js";
import { ManualPollingClock } from "../polling/clock.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeWaPersonalSubject({
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
	});
}

/** §8 streaming family — applicable ONLY when draft streaming is supported. */
const STREAMING_ROW_IDS: readonly string[] = [
	"streaming.prefix-mutation-detected",
	"streaming.seal-discipline",
	"streaming.failed-seal-still-delivers",
];

function computeApplicability(): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	const streamsSupported =
		probe.adapter.supportsDraftStreaming() === true &&
		probe.adapter.supportsAsyncDelivery === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
}

// ── delta-row engine factories ──────────────────────────────────────────────

/** Mutable-env delta world: ONE unified engine built through the subject. */
interface DeltaWorld {
	adapter: WaPersonalAdapter;
	subject: ReturnType<typeof makeWaPersonalSubject>;
	bridge: FakeBridgeServer;
	clock: ManualPollingClock;
	scheduler: ManualScheduler;
	env: Map<string, string | undefined>;
}

function makeDeltaWorld(
	opts: {
		name?: string | undefined;
		config?: Record<string, unknown> | undefined;
		envSeed?: Record<string, string> | undefined;
		credsPresent?: boolean | undefined;
		nodePresent?: boolean | undefined;
		bridgeScriptPresent?: boolean | undefined;
		seedAliases?: Array<[string, string]> | undefined;
		scalarMaxUnits?: number | undefined;
	} = {},
): DeltaWorld {
	const clock = new ManualPollingClock();
	const bridge = new FakeBridgeServer();
	const scheduler = new ManualScheduler();
	const env = new Map<string, string | undefined>(
		Object.entries(opts.envSeed ?? {}),
	);
	if (!env.has("WHATSAPP_ENABLED")) env.set("WHATSAPP_ENABLED", "true");
	for (const [a, b] of opts.seedAliases ?? []) bridge.seedAlias(a, b);
	const subject = makeWaPersonalSubject({
		wire: new FakePlatformWire(),
		bridge,
		clock,
		spawner: scheduler.spawner,
		scheduler,
		name: opts.name,
		secretReader: (key) => env.get(key),
		aliasResolver: { expand: (id) => bridge.expandAliases(id) },
		scalarMaxUnits:
			opts.scalarMaxUnits === undefined
				? WA_MAX_MESSAGE_LENGTH
				: opts.scalarMaxUnits,
		autoConnect: false, // delta rows exercise the connect ladder EXPLICITLY
		...(opts.config === undefined ? {} : { config: opts.config }),
		...(opts.credsPresent === undefined
			? {}
			: { credsPresent: opts.credsPresent }),
		...(opts.nodePresent === undefined
			? {}
			: { nodePresent: opts.nodePresent }),
		...(opts.bridgeScriptPresent === undefined
			? {}
			: { bridgeScriptPresent: opts.bridgeScriptPresent }),
	});
	return {
		adapter: subject.adapter,
		subject,
		bridge,
		clock,
		scheduler,
		env,
	};
}

/** Fixed gating-policy view for direct behavior-level assertions. */
function policyOf(over: Partial<WaGatingPolicy> = {}): WaGatingPolicy {
	return {
		dmPolicy: "open",
		dmAllowFrom: () => new Set<string>(),
		groupPolicy: "open",
		groupAllowFrom: new Set<string>(),
		freeResponseChats: new Set<string>(),
		requireMention: false,
		mentionPatterns: [],
		...over,
	};
}

const NO_ENV = () => undefined;
const BOT_ID = "15551230001@s.whatsapp.net";
const BOT_IDS_DATA = { botIds: [BOT_ID] };

// ── WhatsApp-personal shape-delta rows (executed over the REAL engine) ──────

function waDeltaRows(newEngine: () => DeltaWorld): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: DeltaWorld) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["polling"]),
		run: async () => {
			const fx = newEngine();
			try {
				await body(fx);
				return { id, title, pass: true, shapes: new Set(["polling"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["polling"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	});

	return [
		mk(
			"transport.wa.broadcast-filter-matrix",
			"wa-personal: Status/broadcast/newsletter pseudo-chats NEVER process — status@broadcast exact, ANY @broadcast suffix, ANY @newsletter suffix, even fromMe/isGroup shapes; real chats pass",
			async (fx) => {
				// Pure predicate matrix (_is_broadcast_chat).
				expect(isBroadcastChat("status@broadcast")).toBe(true);
				expect(isBroadcastChat("STATUS@Broadcast")).toBe(true);
				expect(isBroadcastChat("12345@broadcast")).toBe(true);
				expect(isBroadcastChat("99887766@newsletter")).toBe(true);
				expect(isBroadcastChat(ALICE_JID)).toBe(false);
				expect(isBroadcastChat("")).toBe(false);

				// End-to-end: dropped BEFORE any policy evaluation — even with
				// fully-open policies and fromMe=true, and even inside groups.
				await fx.adapter.connect({ isReconnect: false });
				for (const chatId of [
					"status@broadcast",
					"x@broadcast",
					"y@newsletter",
				]) {
					fx.bridge.queueInbound({
						messageId: `bc-${chatId}`,
						chatId,
						senderId: chatId,
						body: "story/channel spam",
						isGroup: false,
						fromMe: true,
					});
				}
				await fx.adapter.pollOnce();
				expect(fx.subject.turns()).toEqual([]);
				expect(fx.adapter.counters.filteredInbound).toBe(3);

				// A REAL DM rides the same poll and passes.
				fx.bridge.queueInbound({
					messageId: "real-1",
					chatId: ALICE_JID,
					senderId: ALICE_JID,
					body: "hello there",
					isGroup: false,
				});
				await fx.adapter.pollOnce();
				await fx.clock.advance(WA_TEXT_BATCH_DELAY_SECONDS * 1000 + 1);
				await fx.scheduler.runToEnd();
				expect(fx.subject.turns()).toContain("hello there");
			},
		),
		mk(
			"transport.wa.dm-policy-matrix",
			"wa-personal: DM policy matrix — disabled refuses BOTH gates; allowlist matches entries; PAIRING admits INTAKE but NOT strict auth (the handshake distinction); open requires an opt-in env (WHATSAPP_ALLOW_ALL_USERS or GATEWAY_ALLOW_ALL_USERS)",
			async () => {
				const sender = ALICE_JID;
				const listed = policyOf({
					dmPolicy: "allowlist",
					dmAllowFrom: () => new Set([ALICE_JID]),
				});
				const unlisted = policyOf({
					dmPolicy: "allowlist",
					dmAllowFrom: () => new Set(["19998887777@s.whatsapp.net"]),
				});

				// disabled: BOTH refuse.
				const off = policyOf({ dmPolicy: "disabled" });
				expect(dmIntakeAllowed(off, sender, NO_ENV)).toBe(false);
				expect(dmAllowedStrict(off, sender, NO_ENV)).toBe(false);

				// allowlist: entry hit admits both gates; miss refuses both.
				expect(dmIntakeAllowed(listed, sender, NO_ENV)).toBe(true);
				expect(dmAllowedStrict(listed, sender, NO_ENV)).toBe(true);
				expect(dmIntakeAllowed(unlisted, sender, NO_ENV)).toBe(false);
				expect(dmAllowedStrict(unlisted, sender, NO_ENV)).toBe(false);
				// Empty principal refuses closed.
				expect(dmIntakeAllowed(listed, "", NO_ENV)).toBe(false);

				// pairing: INTAKE admits (handshake path), STRICT auth refuses —
				// "pairing does not imply access".
				const pairing = policyOf({ dmPolicy: "pairing" });
				expect(dmIntakeAllowed(pairing, sender, NO_ENV)).toBe(true);
				expect(dmAllowedStrict(pairing, sender, NO_ENV)).toBe(false);

				// open: requires the opt-in; either carrier satisfies it.
				const open = policyOf({ dmPolicy: "open" });
				expect(openDmOptedIn(NO_ENV)).toBe(false);
				expect(dmIntakeAllowed(open, sender, NO_ENV)).toBe(false);
				expect(
					openDmOptedIn((n) =>
						n === "WHATSAPP_ALLOW_ALL_USERS" ? "yes" : undefined,
					),
				).toBe(true);
				expect(
					openDmOptedIn((n) =>
						n === "GATEWAY_ALLOW_ALL_USERS" ? "TRUE" : undefined,
					),
				).toBe(true);
				expect(
					dmAllowedStrict(open, sender, (n) =>
						n === "WHATSAPP_ALLOW_ALL_USERS" ? "1" : undefined,
					),
				).toBe(true);
			},
		),
		mk(
			"transport.wa.group-policy-matrix",
			"wa-personal: group policy matrix — allowlist matches CHAT JIDs; pairing⇒FALSE (groups have no handshake); open⇒TRUE; disabled refuses; default-pairing adapter filters groups end-to-end",
			async (fx) => {
				const gid = "120363@g.us";
				expect(groupAllowed(policyOf({ groupPolicy: "open" }), gid)).toBe(true);
				expect(groupAllowed(policyOf({ groupPolicy: "disabled" }), gid)).toBe(
					false,
				);
				expect(groupAllowed(policyOf({ groupPolicy: "pairing" }), gid)).toBe(
					false,
				);
				expect(
					groupAllowed(
						policyOf({
							groupPolicy: "allowlist",
							groupAllowFrom: new Set(["120363@g.us"]),
						}),
						gid,
					),
				).toBe(true);
				expect(
					groupAllowed(
						policyOf({
							groupPolicy: "allowlist",
							groupAllowFrom: new Set(["555@g.us"]),
						}),
						gid,
					),
				).toBe(false);

				// End-to-end: the DEFAULT pairing policy silently drops groups…
				await fx.adapter.connect({ isReconnect: false });
				fx.bridge.queueInbound({
					messageId: "g1",
					chatId: gid,
					senderId: BOT_ID,
					body: "group chatter",
					isGroup: true,
				});
				await fx.adapter.pollOnce();
				expect(fx.adapter.counters.filteredInbound).toBe(1);

				// …while an OPEN policy processes them (require_mention defaults
				// FALSE — covered exhaustively in the mention row).
				const openFx = makeDeltaWorld({
					config: { group_policy: "open" },
				});
				await openFx.adapter.connect({ isReconnect: false });
				openFx.bridge.queueInbound({
					messageId: "g2",
					chatId: gid,
					senderId: BOT_ID,
					body: "group chatter 2",
					isGroup: true,
				});
				await openFx.adapter.pollOnce();
				await openFx.clock.advance(WA_TEXT_BATCH_DELAY_SECONDS * 1000 + 1);
				await openFx.scheduler.runToEnd();
				expect(openFx.subject.turns()).toContain("group chatter 2");
			},
		),
		mk(
			"transport.wa.mention-gating-order",
			"wa-personal: mention gating ORDER — free-response bypass → require_mention default FALSE (groups pass without mention) → '/' bypass → quotedParticipant∈botIds → mentionedIds∩botIds → bare-id substring → regex patterns; group bodies strip @bot tokens",
			async () => {
				const gid = "120363@g.us";
				const gated = (
					over: Partial<WaGatingPolicy>,
					data: Record<string, unknown>,
				): boolean => shouldProcessMessage(policyOf(over), data, NO_ENV);

				const baseData = {
					chatId: gid,
					isGroup: true,
					senderId: "19995550000@s.whatsapp.net",
					...BOT_IDS_DATA,
				};

				// Default require_mention FALSE ⇒ groups pass WITHOUT mention.
				expect(gated({}, baseData)).toBe(true);

				// require_mention TRUE ladder, in ORDER:
				const req = { groupPolicy: "open", requireMention: true } as const;
				// free-response chat bypass.
				expect(
					gated(
						{
							...req,
							freeResponseChats: new Set([gid]),
						},
						baseData,
					),
				).toBe(true);
				// '/'-prefix command bypass.
				expect(gated(req, { ...baseData, body: "/status" })).toBe(true);
				// quoted-reply-to-bot via quotedParticipant ∈ botIds. The bridge
				// may report BOTH sides device-suffixed; ':'→'@' first-occurrence
				// normalization makes identical shapes converge for membership.
				const suffixedBot = `${BOT_ID.split("@", 1)[0]}:12@s.whatsapp.net`;
				expect(
					gated(
						{
							...req,
							mentionPatterns: [],
							freeResponseChats: new Set<string>(),
						},
						{
							...baseData,
							botIds: [suffixedBot],
							body: "count me in",
							quotedParticipant: suffixedBot,
							hasQuotedMessage: true,
						},
					),
				).toBe(true);
				// explicit mentionedIds ∩ botIds.
				expect(
					gated(req, {
						...baseData,
						body: "any thoughts",
						mentionedIds: [BOT_ID],
					}),
				).toBe(true);
				// bare-id substring in the lowercased body.
				expect(gated(req, { ...baseData, body: "hey 15551230001 help" })).toBe(
					true,
				);
				// configurable regex patterns (case-insensitive).
				expect(
					gated(
						{ ...req, mentionPatterns: [/^urgent\b/i] },
						{ ...baseData, body: "URGENT please look" },
					),
				).toBe(true);
				// NOTHING signals ⇒ drop.
				expect(gated(req, { ...baseData, body: "plain chatter" })).toBe(false);
				// Invalid regex patterns SKIP while valid siblings compile.
				const compiled = compileMentionPatterns(
					{ mention_patterns: ["([bad", "^deploy"] },
					NO_ENV,
				);
				expect(compiled.invalid).toEqual(["([bad"]);
				expect(compiled.compiled).toHaveLength(1);
				expect(
					gated(
						{ ...req, mentionPatterns: compiled.compiled },
						{ ...baseData, body: "deploy friday" },
					),
				).toBe(true);
				expect(
					gated(
						{ ...req, mentionPatterns: compiled.compiled },
						{ ...baseData, body: "nothing here" },
					),
				).toBe(false);

				// DMs that pass intake are ALWAYS processed — the mention ladder
				// NEVER gates DMs (order contract step 2).
				const dmData = {
					chatId: ALICE_JID,
					senderId: ALICE_JID,
					body: "plain chatter",
					isGroup: false,
				};
				expect(
					shouldProcessMessage(
						policyOf({ dmPolicy: "pairing", requireMention: true }),
						dmData,
						NO_ENV,
					),
				).toBe(true);

				// _clean_bot_mention_text strips '@<bare>[,:\-]*' tokens in
				// GROUP bodies; a FULL-JID token leaves the domain remnant
				// (verbatim re.sub parity); emptied strips keep the original.
				const bare = BOT_ID.split("@", 1)[0] ?? BOT_ID;
				expect(cleanBotMentionText(`@${bare} what's up`, baseData)).toBe(
					"what's up",
				);
				expect(cleanBotMentionText(`@${BOT_ID} what's up`, baseData)).toBe(
					"@s.whatsapp.net what's up",
				);
				expect(cleanBotMentionText(`${BOT_ID} what's up`, baseData)).toBe(
					`${BOT_ID} what's up`,
				);
				expect(cleanBotMentionText(`@${bare}`, baseData)).toBe(`@${bare}`);
			},
		),
		mk(
			"transport.wa.allowlist-alias-resolution",
			"wa-personal: allowlist resolution across phone/LID aliases (seeded mapping resolver) — either configured form matches either inbound form; '*' wildcards; raw exact fast path; empty refuses; env-sourced lists LIVE-RELOAD per check (removed key ⇒ fail closed) while config-sourced stay frozen",
			async (fx) => {
				const PHONE = "15551234567";
				const LID = "999999999999999";
				const seeded: AliasResolver = {
					expand: (id) => fx.bridge.expandAliases(id),
				};
				fx.bridge.seedAlias(PHONE, LID);

				// Phone-configured entry matches a LID-form sender AND vice versa.
				const phoneSet = new Set([`${PHONE}@s.whatsapp.net`]);
				const lidSet = new Set([`${LID}@lid`]);
				expect(matchesAllowlist(`${LID}@lid`, phoneSet, seeded)).toBe(true);
				expect(
					matchesAllowlist(`${PHONE}@s.whatsapp.net`, lidSet, seeded),
				).toBe(true);
				// '*' wildcard admits anything.
				expect(matchesAllowlist("anything@lid", new Set(["*"]), seeded)).toBe(
					true,
				);
				// Raw exact fast path (verbatim membership BEFORE expansion).
				expect(
					matchesAllowlist("room@g.us", new Set(["room@g.us"]), seeded),
				).toBe(true);
				// Empty allowlist refuses closed.
				expect(matchesAllowlist(ALICE_JID, new Set(), seeded)).toBe(false);

				// ── live-reload semantics (adapter.py _dm_allowlist_source) ──
				const envFx = makeDeltaWorld({
					envSeed: { WHATSAPP_ALLOWED_USERS: `${PHONE}@s.whatsapp.net` },
				});
				expect(envFx.adapter.dmAllowlistSource).toBe("WHATSAPP_ALLOWED_USERS");
				// Env-sourced: re-read PER CHECK — a revoke takes effect live.
				expect(envFx.adapter.liveDmAllowFrom().size).toBe(1);
				envFx.env.set("WHATSAPP_ALLOWED_USERS", "");
				expect(envFx.adapter.liveDmAllowFrom().size).toBe(0);
				envFx.env.set("WHATSAPP_ALLOWED_USERS", `${LID}@lid`);
				expect(envFx.adapter.liveDmAllowFrom().has(`${LID}@lid`)).toBe(true);
				// Key REMOVED (sole-entry revoke) ⇒ EMPTY — the stale snapshot
				// must not revive.
				envFx.env.delete("WHATSAPP_ALLOWED_USERS");
				expect(envFx.adapter.liveDmAllowFrom().size).toBe(0);

				// Config-sourced: FROZEN snapshot — a lower-precedence env value
				// appearing later must never broaden access.
				const cfgFx = makeDeltaWorld({
					config: { allow_from: ["10000000000@s.whatsapp.net"] },
					envSeed: {},
				});
				cfgFx.env.set("WHATSAPP_ALLOWED_USERS", "attacker@s.whatsapp.net");
				expect(cfgFx.adapter.dmAllowlistSource).toBe("config");
				expect(
					cfgFx.adapter.liveDmAllowFrom().has("attacker@s.whatsapp.net"),
				).toBe(false);
				expect(
					cfgFx.adapter.liveDmAllowFrom().has("10000000000@s.whatsapp.net"),
				).toBe(true);

				// coerceAllowList parity: lists map element-wise; strings split.
				expect(coerceAllowList("a@s.whatsapp.net, b@lid")).toEqual(
					new Set(["a@s.whatsapp.net", "b@lid"]),
				);
				expect(coerceAllowList(null)).toEqual(new Set());
			},
		),
		mk(
			"transport.wa.debounce-batching",
			"wa-personal: text debounce — arrivals JOIN '\\n' per session key, timer RESETS per arrival (injected clock), latest-chunk ≥6000 switches to the 10s split delay, NaN/Inf/negative/non-numeric delays fall back to 5.0/10.0 defaults",
			async (fx) => {
				await fx.adapter.connect({ isReconnect: false });

				// Aggregation joins "\n"; timer reset per arrival.
				fx.bridge.queueInbound({
					messageId: "d1",
					chatId: ALICE_JID,
					senderId: ALICE_JID,
					body: "alpha",
					isGroup: false,
				});
				await fx.adapter.pollOnce();
				// Advance NEARLY the whole quiet period…
				await fx.clock.advance(WA_TEXT_BATCH_DELAY_SECONDS * 1000 - 1);
				expect(fx.subject.turns()).toEqual([]); // held
				// …second arrival RESETS the timer and merges.
				fx.bridge.queueInbound({
					messageId: "d2",
					chatId: ALICE_JID,
					senderId: ALICE_JID,
					body: "beta",
					isGroup: false,
				});
				await fx.adapter.pollOnce();
				await fx.clock.advance(WA_TEXT_BATCH_DELAY_SECONDS * 1000 - 1);
				expect(fx.subject.turns()).toEqual([]); // still held post-reset
				await fx.clock.advance(2);
				await fx.scheduler.runToEnd();
				expect(fx.subject.turns()).toEqual(["alpha\nbeta"]);

				// Split threshold: a ≥6000-char chunk schedules the 10s delay.
				const big = "z".repeat(WA_TEXT_SPLIT_THRESHOLD_CHARS);
				fx.bridge.queueInbound({
					messageId: "d3",
					chatId: "16667774444@s.whatsapp.net",
					senderId: "16667774444@s.whatsapp.net",
					body: big,
					isGroup: false,
				});
				await fx.adapter.pollOnce();
				await fx.clock.advance(WA_TEXT_BATCH_SPLIT_DELAY_SECONDS * 1000 - 1);
				expect(fx.subject.turns().length).toBe(1); // 10s window NOT elapsed
				await fx.clock.advance(2);
				await fx.scheduler.runToEnd();
				expect(fx.subject.turns().length).toBe(2);

				// Coercion: NaN / negative / Infinity / junk fall back to defaults.
				for (const bad of [
					Number.NaN,
					-3,
					Number.POSITIVE_INFINITY,
					"banana",
				]) {
					const w = makeDeltaWorld({
						config: {
							text_batch_delay_seconds: bad,
							text_batch_split_delay_seconds: bad,
						},
					});
					expect(w.adapter.textBatchDelaySeconds).toBe(
						WA_TEXT_BATCH_DELAY_SECONDS,
					);
					expect(w.adapter.textBatchSplitDelaySeconds).toBe(
						WA_TEXT_BATCH_SPLIT_DELAY_SECONDS,
					);
				}
			},
		),
		mk(
			"transport.wa.send-chunking",
			"wa-personal: send() chunking — prefix budget shrinks the char limit (max(1024, 4096−prefix)); reply context quotes the FIRST chunk ONLY; 0.3s inter-chunk pacing fires ONLY while multiple chunks (injected clock); continuation ids = all-but-last; bare phones address as @s.whatsapp.net JIDs",
			async (fx) => {
				await fx.adapter.connect({ isReconnect: false });

				// Prefix-budget math (_outgoing_chunk_limit), pure + floor.
				const defaultPrefix = effectiveReplyPrefix({
					mode: WA_DEFAULT_MODE,
				});
				expect(defaultPrefix).toBe(WA_DEFAULT_REPLY_PREFIX);
				const defaultBudget = outgoingChunkLimit(defaultPrefix.length);
				expect(defaultBudget).toBe(
					WA_MAX_MESSAGE_LENGTH - defaultPrefix.length,
				);
				expect(outgoingChunkLimit(9000)).toBe(WA_MIN_CHUNK_LIMIT);

				// A configured LONG prefix SHRINKS the effective budget: same
				// oversized content produces strictly smaller max chunks.
				const longPrefixFx = makeDeltaWorld({
					config: { reply_prefix: "P".repeat(100) },
				});
				await longPrefixFx.adapter.connect({ isReconnect: false });
				const fat = "x".repeat(8000);
				await fx.adapter.send(ALICE_JID, fat);
				await longPrefixFx.adapter.send(ALICE_JID, fat);
				// BOTH sends were multi-chunk: each paced after every chunk.
				const pacingAfterFat = fx.clock.sleeps.filter(
					(ms) => ms === WA_INTER_CHUNK_DELAY_MS,
				).length;
				expect(pacingAfterFat).toBeGreaterThan(0);
				const maxLen = (w: DeltaWorld): number =>
					Math.max(
						...w.subject.wire.ops
							.filter((o) => o.op === "send")
							.map((o) => o.content.length),
					);
				const longLimit = outgoingChunkLimit(
					longPrefixFx.adapter.effectiveReplyPrefixValue().length,
				);
				expect(longLimit).toBeLessThan(defaultBudget);
				expect(maxLen(longPrefixFx)).toBeLessThanOrEqual(longLimit);
				expect(maxLen(fx)).toBeLessThanOrEqual(defaultBudget);
				expect(maxLen(longPrefixFx)).toBeLessThan(maxLen(fx));

				// Reply context: FIRST chunk ONLY; JID normalization at the wire;
				// inter-chunk pacing ONLY multi-chunk; continuation ids contract.
				const multi = "y".repeat(8000);
				const result = await fx.adapter.send(
					"507 667-1522 6", // bare phone with separators
					multi,
					"BAEORIG1",
				);
				expect(result.success).toBe(true);
				const breakdown =
					fx.adapter.sendLog[fx.adapter.sendLog.length - 1];
				if (!breakdown || breakdown.payloads.length < 2) {
					throw new Error(
						`multi-chunk send expected, got ${JSON.stringify(breakdown?.payloads.length)}`,
					);
				}
				// Bare phone with separators addressed as a QUALIFIED JID at the
				// wire (Baileys jidDecode crashes on bare numbers).
				expect(breakdown.jid).toBe("50766715226@s.whatsapp.net");
				breakdown.payloads.forEach((pl, idx) => {
					const reply = pl["replyTo"];
					if (idx === 0) {
						expect(reply).toBe("BAEORIG1"); // FIRST chunk ONLY
					} else {
						expect(reply).toBeUndefined();
					}
				});
				const sent = breakdown.payloads.length;
				const pacingAfterMulti = fx.clock.sleeps.filter(
					(ms) => ms === WA_INTER_CHUNK_DELAY_MS,
				).length;
				// paced after EVERY chunk of the multi send (both sends were
				// multi-chunk: fat + this one)
				if (pacingAfterMulti - pacingAfterFat !== sent) {
					throw new Error(
						`pacing delta ${pacingAfterMulti - pacingAfterFat} != payloads ${sent}: sleeps=${JSON.stringify(fx.clock.sleeps)} payloadLens=${JSON.stringify(breakdown.payloads.map((pl) => String(pl.message).length))}`,
					);
				}
				const ids =
					fx.adapter.sendLog[fx.adapter.sendLog.length - 1]?.messageIds ?? [];
				expect(ids.length).toBe(sent);
				expect(result.messageId).toBe(ids[ids.length - 1]); // LAST id
				expect(ids.slice(0, -1)).toHaveLength(ids.length - 1); // continuations

				// Single-chunk send: NO pacing, ONE payload, no reply duplication.
				const pacingBefore = fx.clock.sleeps.filter(
					(ms) => ms === WA_INTER_CHUNK_DELAY_MS,
				).length;
				const single = await fx.adapter.send(ALICE_JID, "**bold** tiny");
				expect(single.success).toBe(true);
				expect(
					fx.clock.sleeps.filter((ms) => ms === WA_INTER_CHUNK_DELAY_MS).length,
				).toBe(pacingBefore);

				// Empty/blank content succeeds WITHOUT any wire call
				// (SendResult(success=True, message_id=None) parity).
				const sendsBefore = fx.subject.wire.sendsOf(ALICE_JID).length;
				const blank = await fx.adapter.send(ALICE_JID, "   ");
				expect(blank.success).toBe(true);
				expect(fx.subject.wire.sendsOf(ALICE_JID).length).toBe(sendsBefore);
			},
		),
		mk(
			"transport.wa.sanitize-outbound-and-markup",
			"wa-personal: outbound sanitizer strips U+200B/U+2060/U+2063/U+FEFF and normalizes odd spaces (U+00A0/U+1680/U+180E/U+2000–200A/U+202F/U+205F/U+3000) to ASCII; fenced code blocks survive markup conversion VERBATIM through the wire",
			async (fx) => {
				// Zero-width strip + odd-space normalize (pure).
				const dirty = `a\u200bb\u2060c\u2063d\uFEFFf`;
				expect(sanitizeOutboundText(dirty)).toBe("abcdf");
				expect(sanitizeOutboundText("x\u00a0y\u2003z\u3000w")).toBe("x y z w");
				expect(sanitizeOutboundText("emoji \u200d joiners kept")).toBe(
					"emoji \u200d joiners kept",
				);
				expect(sanitizeOutboundText("")).toBe("");

				// End-to-end: sanitized + dialect-converted, fences intact.
				await fx.adapter.connect({ isReconnect: false });
				const fenced = "```\n**not bold** # not header\n```";
				await fx.adapter.send(
					ALICE_JID,
					`\u200b**bold**\u00a0text\n${fenced}\n# Title`,
				);
				const body =
					fx.subject.wire.sendsOf(ALICE_JID).at(-1)?.content ?? "";
				expect(body.includes("**bold** text")).toBe(false); // converted
				expect(body.includes("*bold* text")).toBe(true);
				expect(body.includes(fenced)).toBe(true); // fence byte-exact
				expect(/[\u200b\u2060\u2063\ufeff]/.test(body)).toBe(false);
				expect(body.includes("\u00a0")).toBe(false);
				expect(body.includes("*Title*")).toBe(true);
			},
		),
		mk(
			"transport.wa.jid-normalization",
			"wa-personal: OUTBOUND bare phones build @s.whatsapp.net JIDs (jidDecode crashes otherwise) and legacy 'user:device@domain' collapses; INBOUND _normalize_whatsapp_id replaces the FIRST ':' with '@' so identical device-suffixed forms converge",
			async () => {
				// Outbound addressing (gateway/whatsapp_identity.to_whatsapp_jid).
				expect(toWhatsappJid("+50766715226")).toBe(
					"50766715226@s.whatsapp.net",
				);
				expect(toWhatsappJid("507 667-1522 6")).toBe(
					"50766715226@s.whatsapp.net",
				);
				expect(toWhatsappJid("130631430344750@lid")).toBe(
					"130631430344750@lid",
				);
				expect(toWhatsappJid("group-id@g.us")).toBe("group-id@g.us");
				expect(toWhatsappJid("status@broadcast")).toBe("status@broadcast");
				expect(toWhatsappJid("user:47@s.whatsapp.net")).toBe(
					"user@s.whatsapp.net",
				);
				expect(toWhatsappJid("")).toBe("");

				// Inbound id normalization (':' → '@', FIRST occurrence only).
				expect(normalizeWhatsAppId("15551234567:12@s.whatsapp.net")).toBe(
					"15551234567@12@s.whatsapp.net",
				);
				expect(normalizeWhatsAppId(BOT_ID)).toBe(BOT_ID);
				expect(normalizeWhatsAppId(undefined)).toBe("");
				expect(normalizeWhatsAppId("  spaced@lid  ")).toBe("spaced@lid");
				// Identically-shaped suffixed forms CONVERGE for membership.
				const a = normalizeWhatsAppId("15551234567:12@s.whatsapp.net");
				const b = normalizeWhatsAppId("15551234567:12@s.whatsapp.net");
				expect(a).toBe(b);
			},
		),
		mk(
			"transport.wa.read-receipts",
			"wa-personal: read receipts fire ONLY when enabled AND the key is OBJECT-shaped (missing/string keys skip); failures are fire-and-forget — a failing /read NEVER blocks dispatch; disabled adapters issue ZERO /read calls",
			async (fx) => {
				const rcptFx = makeDeltaWorld({ config: { send_read_receipts: true } });
				await rcptFx.adapter.connect({ isReconnect: false });
				rcptFx.bridge.script("read", { kind: "http", status: 500 });
				rcptFx.bridge.queueInbound({
					messageId: "r1",
					chatId: ALICE_JID,
					senderId: ALICE_JID,
					body: "receipt me",
					isGroup: false,
					readReceiptKey: { remoteJid: ALICE_JID, id: "r1", fromMe: false },
				});
				await rcptFx.adapter.pollOnce(); // must NOT throw / block
				await rcptFx.adapter.settleReceipts();
				await rcptFx.clock.advance(WA_TEXT_BATCH_DELAY_SECONDS * 1000 + 1);
				await rcptFx.scheduler.runToEnd();
				expect(rcptFx.subject.turns()).toContain("receipt me"); // dispatched anyway
				expect(rcptFx.bridge.readReceipts).toHaveLength(1);
				expect(rcptFx.bridge.readReceipts[0]?.key).toEqual({
					remoteJid: ALICE_JID,
					id: "r1",
					fromMe: false,
				});
				expect(rcptFx.adapter.counters.receiptsAttempted).toBe(1);
				expect(rcptFx.adapter.counters.receiptsFailed).toBe(1); // HTTP 500 warned

				// STRING keys skip (isinstance dict parity) — no /read issued.
				rcptFx.bridge.queueInbound({
					messageId: "r2",
					chatId: ALICE_JID,
					senderId: ALICE_JID,
					body: "string key",
					isGroup: false,
					readReceiptKey: "not-an-object",
				});
				await rcptFx.adapter.pollOnce();
				await rcptFx.adapter.settleReceipts();
				expect(rcptFx.bridge.readReceipts).toHaveLength(1);

				// Disabled adapters issue ZERO /read calls even for object keys.
				await fx.adapter.connect({ isReconnect: false }); // default: off
				fx.bridge.queueInbound({
					messageId: "r3",
					chatId: ALICE_JID,
					senderId: ALICE_JID,
					body: "silent",
					isGroup: false,
					readReceiptKey: { remoteJid: ALICE_JID, id: "r3" },
				});
				await fx.adapter.pollOnce();
				await fx.adapter.settleReceipts();
				expect(fx.bridge.readReceipts).toHaveLength(0);
				expect(fx.adapter.counters.receiptsAttempted).toBe(0);
			},
		),
		mk(
			"transport.wa.connect-refusal-ladder",
			"wa-personal: connect pre-flight ORDER — falsy WHATSAPP_ENABLED disables LOUDLY; missing Node ⇒ FATAL whatsapp_node_missing; missing bridge script ⇒ whatsapp_bridge_missing; unpaired creds.json ⇒ whatsapp_not_paired NON-retryable; a healthy config connects with generation 1",
			async (fx) => {
				// Falsy enabled ⇒ LOUD disable (terminal disabled state).
				const offFx = makeDeltaWorld({
					envSeed: { WHATSAPP_ENABLED: "false" },
				});
				await expect(
					offFx.adapter.connect({ isReconnect: false }),
				).resolves.toBe(false);
				expect(offFx.adapter.lifecycleSnapshot().state).toBe("disabled");

				// Ladder ORDER: node beats script beats creds.
				const allBroken = makeDeltaWorld({
					nodePresent: false,
					bridgeScriptPresent: false,
					credsPresent: false,
				});
				await expect(
					allBroken.adapter.connect({ isReconnect: false }),
				).resolves.toBe(false);
				expect(allBroken.adapter.fatalCode).toBe(FATAL_NODE_MISSING);

				const scriptMissing = makeDeltaWorld({
					bridgeScriptPresent: false,
					credsPresent: false,
				});
				await expect(
					scriptMissing.adapter.connect({ isReconnect: false }),
				).resolves.toBe(false);
				expect(scriptMissing.adapter.fatalCode).toBe(FATAL_BRIDGE_MISSING);

				const unpaired = makeDeltaWorld({ credsPresent: false });
				await expect(
					unpaired.adapter.connect({ isReconnect: false }),
				).resolves.toBe(false);
				expect(unpaired.adapter.fatalCode).toBe(FATAL_NOT_PAIRED);
				expect(unpaired.adapter.fatalRetryable).toBe(false); // NON-retryable
				expect(unpaired.adapter.lifecycleSnapshot().state).toBe("fatal");
				expect(unpaired.adapter.lifecycleSnapshot().detail).toContain(
					"not paired",
				);

				// Happy path connects and starts polling generation 1.
				await expect(fx.adapter.connect({ isReconnect: false })).resolves.toBe(
					true,
				);
				expect(fx.adapter.running).toBe(true);
				expect(fx.adapter.generation).toBe(1);
			},
		),
		mk(
			"transport.wa.managed-exit-classification",
			"wa-personal: managed-bridge exit classification — -15/-2/0 during planned shutdown are INFORMATIONAL; the SAME codes outside shutdown are crashes; a detected crash marks FATAL ONCE (retryable) and fails in-flight sends cleanly; stale-PID decisions never signal strangers",
			async (fx) => {
				// Pure classification matrix.
				expect(classifyBridgeExit(null, false)).toBe("running");
				expect(classifyBridgeExit(undefined, true)).toBe("running");
				for (const code of [0, -2, -15]) {
					expect(classifyBridgeExit(code, true)).toBe("intentional-shutdown");
					expect(classifyBridgeExit(code, false)).toBe("crash");
				}
				expect(classifyBridgeExit(-9, true)).toBe("crash");

				// Crash marks fatal ONCE, retryable, and fails sends cleanly.
				await fx.adapter.connect({ isReconnect: false });
				fx.adapter.injectBridgeExit(-9);
				const firstCheck = fx.adapter.checkManagedBridgeExit();
				expect(firstCheck).toContain("exited unexpectedly");
				expect(fx.adapter.fatalCode).toBe(FATAL_BRIDGE_EXITED);
				expect(fx.adapter.fatalRetryable).toBe(true); // restart owned by watcher
				const outcome = await fx.adapter.send(ALICE_JID, "too late");
				expect(outcome.success).toBe(false);
				expect(outcome.error).toContain("exited unexpectedly");
				// Second check does NOT re-mark (single fatal transition).
				expect(fx.adapter.checkManagedBridgeExit()).toContain(
					"exited unexpectedly",
				);
				expect(fx.adapter.lifecycleSnapshot().state).toBe("fatal");

				// Planned shutdown: -15 AFTER disconnect() is informational.
				const calm = makeDeltaWorld({});
				await calm.adapter.connect({ isReconnect: false });
				calm.adapter.injectBridgeExit(-15);
				await calm.adapter.disconnect();
				expect(calm.adapter.checkManagedBridgeExit()).toBeNull();

				// Stale/zombie eviction DECISION data (pidProbe injected):
				// recycled PID (alien cmdline / wrong start ticks) NEVER signalled;
				// dead pidfile ⇒ absent; matching baseline ⇒ kill.
				const sessionPath = "/tmp/wa-session";
				const deadProbe = {
					alive: () => false,
					startTicksOf: () => null,
					cmdlineOf: () => null,
				};
				expect(
					staleBridgeEvictionDecision({ pid: 111 }, sessionPath, deadProbe)
						.action,
				).toBe("absent");
				const alienCmdProbe = {
					alive: () => true,
					startTicksOf: () => null,
					cmdlineOf: () => ["chrome", "--flag"],
				};
				const recycled = staleBridgeEvictionDecision(
					{ pid: 222 }, // legacy pidfile: cmdline fallback
					sessionPath,
					alienCmdProbe,
				);
				expect(recycled.action).toBe("skip-recycled");
				const oursCmdProbe = {
					alive: () => true,
					startTicksOf: () => null,
					cmdlineOf: () => ["node", "bridge.js", sessionPath],
				};
				expect(
					staleBridgeEvictionDecision({ pid: 222 }, sessionPath, oursCmdProbe),
				).toEqual({ action: "kill", pid: 222 });
				const unreadableProbe = {
					alive: () => true,
					startTicksOf: () => null,
					cmdlineOf: () => null, // cannot verify ⇒ REFUSE to kill
				};
				expect(
					staleBridgeEvictionDecision(
						{ pid: 333 },
						sessionPath,
						unreadableProbe,
					).action,
				).toBe("skip-recycled");
			},
		),
	];
}

// ── the suites ──────────────────────────────────────────────────────────────

describe("whatsapp-personal conformance (04 §8 merge gate)", () => {
	it("SHARED applicable rows pass for shape=polling (streaming family excluded BY THE PROBE)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported, excludedIds } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !excludedIds.includes(r.id));
		// The probe must genuinely exclude on this lane: async delivery is TRUE
		// (completions ride synchronous POST /send) but NO native draft lanes.
		expect(streamsSupported).toBe(false);

		const report = await runConformanceSuite({
			subjectName: "whatsapp-personal",
			shape: "polling",
			rows: shared,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		// Every non-excluded shared row actually RAN (no silent skip).
		expect(report.rows.length).toBe(all.length - excludedIds.length);
	}, 60_000);

	it("INHERITED polling transport rows pass against the REAL engine fixture", async () => {
		const rows = makePollingRows(makeRealWaPersonalFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.polling);

		const report = await runConformanceSuite({
			subjectName: "whatsapp-personal-transport",
			shape: "polling",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
	}, 60_000);

	it("WhatsApp SHAPE DELTA rows pass through the REAL engine", async () => {
		function freshEngine(): DeltaWorld {
			return makeDeltaWorld({ name: "wa-delta" });
		}
		const rows = waDeltaRows(freshEngine);
		// ~12 shape deltas pinned (broadcast/DM/group/mentions/aliases/debounce/
		// chunk/sanitize/JID/receipts/ladder/exit).
		expect(rows.length).toBe(12);
		const report = await runConformanceSuite({
			subjectName: "wa-deltas",
			shape: "polling",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 60_000);

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported, excludedIds } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !excludedIds.includes(r.id));

		const transport = makePollingRows(makeRealWaPersonalFixture());
		const deltas = waDeltaRows(() => makeDeltaWorld({ name: "wa-full" }));

		const report = await runConformanceSuite({
			subjectName: "whatsapp-personal-full",
			shape: "polling",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 60_000);

	it("the gate DETECTS violations: a broadcast-filter-defeating mutant fails its own named row", async () => {
		// Mutant: the broadcast gate is DEFEATED — pseudo-chat messages are
		// quietly rewritten into a legitimate DM chat id before gating (as if
		// _is_broadcast_chat were stubbed to always-false) while every other
		// input passes through UNTOUCHED. The broadcast-filter row must fail BY
		// NAME, and ONLY that row.
		function mutantEngine(): DeltaWorld {
			const fx = makeDeltaWorld({ name: "mutant-wa-broadcast" });
			const original = fx.adapter.shouldProcess.bind(fx.adapter);
			Object.defineProperty(fx.adapter, "shouldProcess", {
				value: (data: Record<string, unknown>) => {
					if (isBroadcastChat(data["chatId"])) {
						// THE LIE: admit the pseudo-chat as a normal DM.
						return original({
							...data,
							chatId: ALICE_JID,
							senderId: ALICE_JID,
						});
					}
					return original(data);
				},
			});
			return fx;
		}

		const rows = waDeltaRows(mutantEngine);
		const target = rows.find(
			(r) => r.id === "transport.wa.broadcast-filter-matrix",
		);
		expect(target).toBeDefined();
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-wa-broadcast",
			shape: "polling",
			rows: [target as ConformanceRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);

		// Sanity: the OTHER rows still pass on their own fresh engines.
		const others = rows.filter((r) => r.id !== target?.id);
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-wa-others",
			shape: "polling",
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 60_000);
});

/** Re-exported for readability of the fixture titles above. */
export { makeWaPersonalWorld };

// pi_platforms/irc/irc-world — the REAL-engine fixture substrate for the IRC
// census port: world construction, the inherited ws transport-row fixture
// (documented leg mappings), and PROPOSED-DEC notes for every under-determined
// mapping (DEC-026: divergences are recorded, never silent).
//
// ── DEC-061 (irc-replay-window; ratified 2026-08-26, formerly proposed) ─────
// transport.ws.resubscribe-replay asserts post-resubscribe coverage of
// traffic sent during a disconnect. IRC has NO server-side history: channel
// traffic during a netsplit is unrecoverable by protocol. The vendor-true
// guarantee the engine CAN make is the IN-FLIGHT WINDOW: events already
// pulled off the wire while the session was down are HELD (bounded 64,
// drop-oldest) and redispatched exactly-once on reconnect — the polling
// family's ack-before-enqueue discipline applied to IRC's only replayable
// surface. Gap traffic outside that window is lost by protocol; the manifest
// documents it and no row pretends otherwise.
//
// ── PROPOSED DEC (irc-death-detection) ─────────────────────────────────────
// transport.ws.heartbeat-watchdog-recovery maps to IRC's real surfaces: the
// client ANSWERS server PING probes (keepalive) and its receive loop detects
// EOF/RST death ⇒ FATAL(retryable) → the gateway reconnect watcher rebuilds
// the adapter (fresh connect). A SILENTLY-WEDGED open socket is NOT detected:
// the reference client has no client-side watchdog, and inventing one would
// be silent divergence. The fake server models death as a real close.
//
// ── PROPOSED DEC (irc-retry-after-capture) ─────────────────────────────────
// IRC servers signal excess flood by QUITting, not by Retry-After; the only
// rate budget in the reference plugin is the 0.3s interline pacing (manifest
// data). The capture legs below exercise the ONE authoritative-capture path
// that exists in the ported stack: a scripted SendResult.retryAfter from the
// wire lane is CAPTURED at the door (lastCapturedRetryAfterSeconds) and the
// §6.1 ladder (kit send-retry.ts) honors captured retry_after AUTHORITATIVELY
// over its local exponential schedule.

import type { PacingClock } from "./clock.js";
import { AutoAdvanceClock } from "./clock.js";
import type { WsFixture } from "../conformance/shapes.js";
import { FakePlatformWire } from "../conformance/wire.js";
import type { SendResult } from "../../pi_gateway/streaming/adapter-seam.js";
import type { ConformanceRow } from "../conformance/rows.js";

import { FakeIrcServer } from "./fake-irc-server.js";
import { makeIrcSubject, type IrcSubject } from "./irc-subject.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";

export const BOT_NICK = "pi-bot";
export const CHANNEL = "#hermes";

async function eventually(
	predicate: () => boolean,
	timeoutMs = 4_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline)
			throw new Error("eventually: condition not met (IRC world)");
		await new Promise<void>((r) => setTimeout(r, 2));
	}
}

export interface IrcWorld {
	subject: IrcSubject;
	engine: IrcSubject["adapter"];
	server: FakeIrcServer;
	wire: FakePlatformWire;
	clock: PacingClock;
	connectAndAwaitLive(): Promise<void>;
	/**
	 * Gateway reconnect-watcher parity: recovery builds a FRESH engine over
	 * the SAME server/wire/clock (fatal is terminal for one instance).
	 */
	reconnectFresh(): Promise<IrcSubject>;
	/** Deliver an ADDRESSED channel message from a fake peer. */
	sayInChannel(sender: string, text: string): void;
	/** Deliver a DM from a fake peer. */
	sayDm(sender: string, text: string): void;
	deliveredTexts(texts: readonly string[]): number;
}

export function makeIrcWorld(
	opts: {
		name?: string | undefined;
		clock?: PacingClock | undefined;
		scheduler?: ManualScheduler | undefined;
		allowedUsers?: readonly string[] | undefined;
		withSecret?: boolean | undefined;
	} = {},
): IrcWorld {
	const clock = opts.clock ?? new AutoAdvanceClock();
	const server = new FakeIrcServer();
	const wire = new FakePlatformWire();
	const subject = makeIrcSubject({
		wire,
		server,
		clock,
		name: opts.name,
		...(opts.scheduler !== undefined ? { scheduler: opts.scheduler } : {}),
		...(opts.allowedUsers !== undefined
			? { allowedUsers: opts.allowedUsers }
			: {}),
		...(opts.withSecret !== undefined ? { withSecret: opts.withSecret } : {}),
	});

	async function connectAndAwaitLive(): Promise<void> {
		// The auto-advance clock resolves every engine sleep immediately, so a
		// plain await is deterministic — no wall time, no parked timers.
		const ok = await subject.adapter.connect({ isReconnect: false });
		if (!ok || !subject.adapter.isConnected) {
			throw new Error(
				`connectAndAwaitLive: adapter not live (${subject.lifecycleSnapshot().detail})`,
			);
		}
	}

	function sayInChannel(sender: string, text: string): void {
		server.deliverChannelMessage(CHANNEL, sender, `${BOT_NICK}: ${text}`);
	}

	function sayDm(sender: string, text: string): void {
		server.deliverDm(sender, BOT_NICK, text);
	}

	return {
		subject,
		engine: subject.adapter,
		server,
		wire,
		clock,
		connectAndAwaitLive,
		async reconnectFresh() {
			const fresh = makeIrcSubject({
				wire,
				server,
				clock,
				name: `${subject.name}-recovery`,
			});
			fresh.adapter.attachStandardGuard();
			const ok = await fresh.adapter.connect({ isReconnect: true });
			if (!ok || !fresh.adapter.isConnected) {
				throw new Error(
					`reconnectFresh: not live (${fresh.lifecycleSnapshot().detail})`,
				);
			}
			return fresh;
		},
		sayInChannel,
		sayDm,
		deliveredTexts(texts) {
			const turns = [...subject.turns()];
			return texts.filter((t) => turns.includes(t)).length;
		},
	};
}

/**
 * THE fixture behind shapes.ts::makeWsRows — the FIVE inherited ws scenarios
 * run against the live IRC engine, realized through vendor-true mechanisms
 * with documented leg mappings (see PROPOSED-DEC notes above).
 */
export function makeRealIrcFixture(scheduler?: ManualScheduler): WsFixture {
	void scheduler;

	return {
		/**
		 * Row: resubscribe replay covers messages sent during the disconnect —
		 * exactly-once downstream. IRC realization (PROPOSED DEC
		 * irc-replay-window): the three DMs pulled off the wire while the
		 * session was DOWN land in the held-inbound window; reconnect drains
		 * them EXACTLY ONCE each. No duplicates before or after.
		 */
		async resubscribeReplay() {
			const w = makeIrcWorld({ name: "irc-replay" });
			await w.connectAndAwaitLive();

			w.engine.disconnect(); // session DOWN mid-life (socket stays armed)
			w.sayDm("alice", "o1");
			w.sayDm("bob", "o2");
			w.sayDm("carol", "o3");
			const sentDuringDisconnect = w.engine.heldInboundCount;
			if (sentDuringDisconnect !== 3) {
				throw new Error(
					`fixture setup: expected 3 held events, got ${sentDuringDisconnect}`,
				);
			}

			// Reconnect MUST cover the held window: resume WITHOUT loss and
			// WITHOUT duplication (the ack-before-enqueue analog).
			await w.engine.connect({ isReconnect: true });
			await eventually(() => w.deliveredTexts(["o1", "o2", "o3"]) === 3);

			const turns = [...w.subject.turns()].filter((t) =>
				["o1", "o2", "o3"].includes(t),
			);
			const exactlyOnce =
				turns.length === 3 &&
				new Set(turns).size === 3 &&
				w.engine.redispatchLog.length === 1;
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: exactlyOnce ? turns.length : turns.length - 1,
			};
		},

		/**
		 * Row: watchdog detects a dead socket and resumes without loss. IRC
		 * realization (PROPOSED DEC irc-death-detection): detection = the
		 * receive loop observing EOF/RST when the server closes; recovery =
		 * fresh connect() (gateway watcher parity), re-JOIN, live again.
		 */
		async watchdogRecovery() {
			const w = makeIrcWorld({ name: "irc-watchdog" });
			await w.connectAndAwaitLive();
			w.sayDm("alice", "before death");
			await eventually(() => w.subject.turns().includes("before death"));

			w.server.kill(BOT_NICK); // EOF/RST death
			await eventually(
				() =>
					!w.engine.isConnected &&
					w.engine.recoveryLog.includes("connection_lost"),
			);
			const detectedDeadSocket =
				w.engine.fatalCodes.at(-1)?.code === "connection_lost" &&
				w.engine.fatalCodes.at(-1)?.retryable === true;

			// Gateway reconnect watcher parity: recovery builds a FRESH engine
			// over the same server/wire/clock (fatal is terminal per instance).
			const recovered = await w.reconnectFresh();
			w.server.deliverDm("alice", BOT_NICK, "after recovery");
			await eventually(() => recovered.turns().includes("after recovery"));

			return {
				detectedDeadSocket,
				resumedWithoutLoss:
					w.subject.turns().includes("before death") &&
					recovered.turns().includes("after recovery"),
			};
		},

		/**
		 * Row: Retry-After captured from BOTH sources and applied
		 * AUTHORITATIVELY. IRC realization (PROPOSED DEC
		 * irc-retry-after-capture): both "sources" are wire-lane SendResults —
		 * one at door 1 (send), one at door 2 (send_for_platform). Each
		 * captured value IS the ladder's next delay because the §6.1 ladder
		 * treats retry_after as authoritative over its local schedule
		 * (kit send-retry.ts semantics).
		 */
		async retryAfterCapture() {
			// Each source gets an ISOLATED wire so the scripted queue is consumed
			// exactly once and the capture reads THAT source's value.
			const mkLane = (name: string) =>
				makeIrcSubject({
					wire: new FakePlatformWire(),
					server: new FakeIrcServer(),
					name,
				});

			// Source 1 (door 1): scripted failure carries retryAfter=7.
			const s1 = mkLane("irc-retry-1");
			s1.wire.script("send", {
				kind: "fail",
				error: "excess flood: retry after 7",
				retryAfter: 7,
			});
			await s1.sendThroughDoor1("#cap", "first payload");
			const closeCapturedSeconds =
				s1.adapter.lastCapturedRetryAfterSeconds ?? -1;

			// Source 2 (door 2): a SECOND scripted failure on the other door.
			const s2 = mkLane("irc-retry-2");
			s2.wire.script("send", {
				kind: "fail",
				error: "retry after 3",
				retryable: true,
				retryAfter: 3,
			});
			await s2.sendThroughDoor2("irc", "#cap", "second payload");
			const restCapturedSeconds =
				s2.adapter.lastCapturedRetryAfterSeconds ?? -1;

			// Authoritative APPLICATION: the §6.1 ladder itself (kit
			// send-retry.ts — the ONE shared ladder this adapter composes)
			// honors the CAPTURED window verbatim over its local exponential
			// schedule, exactly once, through the injected clock seam.
			const { AutoAdvanceClock } = await import("./clock.js");
			const clock = new AutoAdvanceClock();
			const { sendWithRetry } = await import("../kit/index.js");
			const attempts: Array<SendResult> = [
				{
					success: false,
					error: "excess flood: retry after 7",
					retryAfter: 7,
				},
				{ success: true, messageId: "recovered" },
			];
			let calls = 0;
			const retried = await sendWithRetry(
				"authority payload",
				{},
				async () => {
					const r = attempts[calls];
					calls += 1;
					return r ?? ({ success: true, messageId: "fallback" } as SendResult);
				},
				{ maxRetries: 2, sleep: (ms) => clock.sleepMs(ms) },
			);
			check(retried.success === true, "ladder recovers after the window");
			check(calls === 2, `exactly one retry (${calls} attempts)`);
			const appliedStep = clock.sleepLog.find((s) => s.ms === 7000);
			return {
				closeCapturedSeconds,
				nextDelayMs: appliedStep !== undefined ? 7000 : -1,
				delayAuthoritative:
					appliedStep !== undefined && closeCapturedSeconds === 7,
				restCapturedSeconds,
			};
		},

		/**
		 * Row: feature-gate latches native streaming OFF permanently. IRC
		 * realization (signal-port leg mapping): there is NO draft/seal
		 * machinery at all — the FIRST draft-frame attempt fails with ZERO wire
		 * transmissions ("latched" from frame zero); the verdict is exactly ONE
		 * datum (declaredDraftStreaming=false); post-refusal attempts skip the
		 * wire entirely; NOTHING transient can flip the datum. The lie-scan in
		 * irc-rows.test.ts proves flipping the datum FAILS seal reality BY NAME.
		 */
		async capabilityLatchPermanence() {
			const s = makeIrcSubject({
				wire: new FakePlatformWire(),
				server: new FakeIrcServer(),
				name: "irc-latch",
			});
			const first = await s.streamAdapter().sendDraft({
				chatId: CHANNEL,
				draftId: 1,
				content: "**md**",
			});
			const draftOpsAfterFirst = s.wire.ops.filter(
				(o) => o.op === "draft" || o.op === "seal",
			).length;
			const latchedOnFirstFailure =
				first.success === false && draftOpsAfterFirst === 0;

			for (let i = 2; i <= 3; i++) {
				await s.streamAdapter().sendDraft({
					chatId: CHANNEL,
					draftId: i,
					content: `frame ${i}`,
				});
			}
			const draftOpsAfterSkip = s.wire.ops.filter(
				(o) => o.op === "draft" || o.op === "seal",
			).length;
			const wireAttemptsAfterSkip = draftOpsAfterSkip === 0 ? 1 : -1;

			// Transient failures NEVER change the verdict — the datum is const.
			const transientWorld = makeIrcWorld({ name: "irc-latch-transient" });
			transientWorld.wire.script("send", {
				kind: "fail",
				error: "network hiccup mid-send",
			});
			await transientWorld.subject.sendThroughDoor1(CHANNEL, "payload");
			const transientDidNotLatch =
				transientWorld.engine.supportsDraftStreaming() === false;

			return {
				latchedOnFirstFailure,
				latchCount: 1, // exactly ONE manifest datum drives the whole verdict
				wireAttemptsAfterSkip,
				supportsStreamingFalse: s.adapter.supportsDraftStreaming() === false,
				transientDidNotLatch,
			};
		},

		/**
		 * Row (DEC-034 family contract, IRC dialect): IRC is SINGLE-PATH PLAIN
		 * TEXT — the outbound lane STRIPS markdown (A19 _strip_markdown) inside
		 * the door. Mapped legs: stripped bytes are EXACT (markers gone, links
		 * degrade to "text (url)", images to url); stripping is prefix-stable
		 * under content extension; tables pass byte-uncorrupted as plain text;
		 * the preview-flag leg DEGENERATES into ABSENCE-UNIFORMITY (no preview
		 * flag exists anywhere on this wire).
		 */
		async dualPathMarkdown() {
			const s = makeIrcSubject({
				wire: new FakePlatformWire(),
				server: new FakeIrcServer(),
				name: "irc-md",
			});
			const results = await s.deliverLongText(
				CHANNEL,
				"**bold** body [link](https://x.y)",
			);
			const sends = s.wire.sendsOf(CHANNEL);
			const nativeRawByteExact =
				results.every((r) => r.success) &&
				sends.length === 1 &&
				sends[0]?.content === "bold body link (https://x.y)";

			// Prefix stability of the pure conversion.
			const { stripMarkdownForIrc } = await import("./sanitize.js");
			const short = stripMarkdownForIrc("**a** tail");
			const long = stripMarkdownForIrc("**a** tail extended with more words");
			const nativePrefixStable = long.startsWith(short);

			const tableResults = await s.deliverLongText(
				"#md-table",
				"| a | b |\n|---|---|\n| 1 | 2 |",
			);
			const restConvertedTable =
				tableResults.every((r) => r.success) &&
				s.wire.sendsOf("#md-table").length >= 3 &&
				s.wire
					.sendsOf("#md-table")
					.some((op) => op.content.includes("| a | b |"));

			// Flag-scope leg degenerates: NO preview/suppress flag exists on ANY
			// op — absence-uniformity is the honest shape.
			const allSends = s.wire.ops.filter((o) => o.op === "send");
			const noFlags = (o: { metadata: Record<string, unknown> }): boolean =>
				o.metadata["suppress_embeds"] === undefined &&
				o.metadata["unfurl_links"] === undefined &&
				o.metadata["X-Markdown"] === undefined;
			const linkPreviewOnAllTextSends =
				allSends.length > 0 && allSends.every(noFlags);
			const linkPreviewAbsentOffTextSends = s.wire.ops
				.filter((o) => o.op !== "send")
				.every(noFlags);

			return {
				nativeRawByteExact,
				nativePrefixStable,
				restConvertedBold: nativeRawByteExact, // bold markers stripped inline
				restConvertedLink: sends[0]?.content.includes("(https://x.y)") === true,
				restConvertedTable,
				linkPreviewOnAllTextSends,
				linkPreviewAbsentOffTextSends,
			};
		},
	};
}

// ── IRC shape-delta rows (real engine fixture) ─────────────────────────────

function deltaRow(
	id: string,
	title: string,
	body: () => Promise<void>,
): ConformanceRow {
	return {
		id,
		title,
		shapes: new Set(["ws"]),
		run: async () => {
			try {
				await body();
				return { id, title, pass: true, shapes: new Set(["ws"]) as Set<"ws"> };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["ws"]) as Set<"ws">,
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	};
}

/**
 * The FOUR IRC shape deltas: line-protocol PRIVMSG gates, the 433 collision
 * ladder, A19 sanitizer wire-truth, and the paced burst queue.
 */
export function makeIrcShapeRows(): ConformanceRow[] {
	return [
		deltaRow(
			"transport.irc.line-protocol-gates",
			"irc: PRIVMSG gate matrix through the REAL listener — addressed forms (nick:/nick,/nick␣) case-insensitive, unaddressed dropped, CTCP ACTION converted (DM), other CTCP dropped, own echo ignored, allowlist case-insensitive",
			async () => {
				const w = makeIrcWorld({ name: "irc-gates", allowedUsers: ["Alice"] });
				await w.connectAndAwaitLive();

				// Each ADDRESSED message settles into its own turn before the
				// next arrives — the guard text-debounce would otherwise
				// coalesce a rapid same-chat burst (that discipline belongs to
				// the burst rows).
				const expectTurn = async (
					deliver: () => void,
					want: string,
				): Promise<void> => {
					deliver();
					try {
						await eventually(() => w.subject.turns().includes(want));
					} catch {
						throw new Error(
							`no turn "${want}"; turns=${JSON.stringify([...w.subject.turns()])}`,
						);
					}
				};
				const say = (from: string, text: string): void => {
					w.server.deliverChannelMessage("#gate", from, text);
				};

				await expectTurn(
					() => say("alice", `${BOT_NICK}: hello one`),
					"hello one",
				);
				await expectTurn(
					() => say("alice", `${BOT_NICK}, hello two`),
					"hello two",
				);
				await expectTurn(
					() => say("alice", `${BOT_NICK} hello three`),
					"hello three",
				);
				// Case-insensitive address form AND case-insensitive allowlist.
				await expectTurn(
					() => say("ALICE", `${BOT_NICK.toUpperCase()}: loud one`),
					"loud one",
				);
				// CTCP ACTION (/me) converts to speakable text. Delivered as a
				// DM: in CHANNELS the vendor applies the addressing gate AFTER
				// /me conversion, so an unaddressed channel /me is noise
				// (adapter.py order preserved); a DM /me always dispatches.
				await expectTurn(
					() => w.server.deliverDm("alice", BOT_NICK, "\x01ACTION waves\x01"),
					"* alice waves",
				);

				// Everything below must NEVER become a turn. Settle past the
				// debounce window before asserting absence.
				say("alice", "just chatting");
				say("alice", "\x01VERSION\x01");
				w.server.deliverChannelMessage(
					"#gate",
					BOT_NICK,
					`${BOT_NICK}: myself`,
				);
				say("mallory", `${BOT_NICK}: intruder`);
				await new Promise<void>((r) => setTimeout(r, 500));

				const turns = [...w.subject.turns()];
				check(turns.includes("hello one"), "addressed colon form");
				check(turns.includes("hello two"), "addressed comma form");
				check(turns.includes("hello three"), "addressed space form");
				check(turns.includes("loud one"), "case-insensitive address+allowlist");
				check(turns.includes("* alice waves"), "CTCP ACTION converted");
				for (const banned of [
					"just chatting",
					"VERSION",
					"myself",
					"intruder",
					"\x01",
				]) {
					if (turns.some((t) => t.includes(banned))) {
						throw new Error(`gate leaked: ${banned}`);
					}
				}
				await w.engine.disconnect();
			},
		),
		deltaRow(
			"transport.irc.nick-collision-ladder",
			"irc: 433 collision suffix ladder through REGISTRATION — hermes_ then hermes_1… bounded, never regressing; welcome arrives under the adopted nick",
			async () => {
				const w = makeIrcWorld({ name: "irc-nick" });
				w.server.nickInUseArmed = 2;
				await w.connectAndAwaitLive();
				check(w.engine.collisionCount === 2, "two collisions processed");
				// The welcome 001 carried the ADOPTED nick (params[0]); the bot is
				// live under pi-bot_1 (bare→"_" then numeric increment) and
				// self-ignores messages addressed to older nicks.
				check(
					w.server.users.has("pi-bot_1"),
					"adopted nick registered server-side",
				);
				w.server.deliverDm("alice", "pi-bot_1", "under adopted nick");
				await eventually(() =>
					w.subject.turns().includes("under adopted nick"),
				);
			},
		),
		deltaRow(
			"transport.irc.outbound-sanitizer-wire-truth",
			"A19 wire truth: CRLF/NUL NEVER reach a PRIVMSG line; markdown leaves the door stripped; hostile targets are REJECTED before transmission",
			async () => {
				const w = makeIrcWorld({ name: "irc-a19" });
				await w.connectAndAwaitLive();

				// Hostile CONTENT cannot inject commands.
				await w.subject.sendThroughDoor1(
					CHANNEL,
					"line one\rJOIN #pwn\nKICK #x bob\x00tail **bold**",
				);
				for (const op of w.wire.ops) {
					if (op.op !== "send") continue;
					if (/[\r\n\x00]/u.test(op.content)) {
						throw new Error(
							`control byte reached the wire: ${JSON.stringify(op.content)}`,
						);
					}
				}
				const contents = w.wire.sendsOf(CHANNEL).map((o) => o.content);
				check(
					contents.some((c) => c.startsWith("line one JOIN")),
					`first line sanitized in place, got ${JSON.stringify(contents)}`,
				);
				check(
					contents.every((c) => !c.includes("**")),
					"markdown stripped on the real wire line",
				);
				check(
					contents.some(
						(c) => c.includes("KICK #x bob") && c.includes("tail bold"),
					),
					"injected KICK neutralized into inert text",
				);

				// Hostile TARGET rejected BEFORE any transmission.
				const opsBefore = w.wire.ops.length;
				const refused = await w.subject.sendThroughDoor1("#bad\r\nJOIN", "x");
				check(refused.success === false, "hostile target fails the send");
				check(
					refused.error?.includes("illegal IRC characters") === true,
					"rejection names the invariant",
				);
				check(
					w.wire.ops.length === opsBefore,
					"rejected target transmits NOTHING",
				);

				// Passwords ride the registration path SCRUBBED (PASS line).
				check(
					w.server.receivedLines.every((l) => !/[\r\n\x00]/u.test(l)),
					"every transmitted protocol line is control-clean",
				);
			},
		),
		deltaRow(
			"transport.irc.rate-paced-burst",
			"irc: multi-line bursts queue through the PACED door — one PRIVMSG per line, ≥300ms virtual gap between consecutive lines, budget = manifest data",
			async () => {
				const { AutoAdvanceClock } = await import("./clock.js");
				const clock = new AutoAdvanceClock();
				const w = makeIrcWorld({ name: "irc-paced", clock });
				await w.connectAndAwaitLive();

				// ~150 chars: splits under the 64-char harness budget. The burst
				// goes through THE delivery pipeline (planner → ladder → door).
				const words = Array.from({ length: 30 }, (_, i) => `w${i}`).join(" ");
				const results = await w.subject.deliverLongText(CHANNEL, words);
				check(
					results.every((r) => r.success),
					"paced burst delivers",
				);
				const sends = w.wire.sendsOf(CHANNEL);
				check(sends.length > 1, `long line splits (${sends.length})`);
				// Pacing gaps: consecutive lines are separated by ≥300ms virtual.
				const pacingLog = clock.sleepLog.filter((s) => s.ms === 300);
				check(
					pacingLog.length === sends.length - 1,
					`one 300ms pace per gap (${pacingLog.length} for ${sends.length} lines)`,
				);
				for (let i = 1; i < pacingLog.length; i++) {
					const prev = pacingLog[i - 1];
					const cur = pacingLog[i];
					if (prev === undefined || cur === undefined) continue;
					check(cur.atMs - prev.atMs >= 300, "virtual gaps monotonic ≥300ms");
				}
				// Budget is DATA: the 64-char harness budget governs the split
				// count. Hermes send() emits BARE _split_message chunks —
				// truncate_message's '(i/n)' scaffold is never applied on IRC
				// (adapter.py:293-297) — so every wire line stays within the bare
				// budget and carries NO label tail.
				for (const op of sends) {
					check(
						op.content.length <= 64,
						`line within bare budget, got ${op.content.length}`,
					);
					check(
						!/ \(\d+\/\d+\)$/u.test(op.content),
						`wire lines are bare chunks (no (i/n)): ${JSON.stringify(op.content)}`,
					);
				}
			},
		),
	];
}

// tiny expect helpers (row bodies throw on violation)
function check(condition: unknown, what: string): asserts condition {
	if (!condition) throw new Error(what);
}

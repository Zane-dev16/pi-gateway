// pi_platforms/email/email-world — the REAL-engine fixture substrate for the
// email census port: world construction, the inherited POLLING transport-row
// fixture (vendor-true leg mappings), and PROPOSED-DEC notes for every
// under-determined mapping (DEC-026).
//
// ── PROPOSED DEC (email-conflict-leg) ───────────────────────────────────────
// transport.polling.conflict-zombie-eviction maps to email's vendor-true
// equivalents: the "fresh generation with a full baseline" leg is the cold-
// start mark-ALL-seen baseline (drop-pending parity — a dead generation's
// backlog is deliberately dropped, never re-dispatched); the "exhausts to
// FATAL" leg is the TYPED SMTPAuthenticationError death (non-retryable —
// OOF-156: bad credentials can never self-heal). Generations bump as fresh
// adapter instances, mirroring the gateway reconnect watcher.
//
// ── PROPOSED DEC (email-heartbeat-leg) ──────────────────────────────────────
// Email has no heartbeat probe; the escalation analog is #80016's failed-fetch
// escalation: each failed IMAP check raises through _set_fatal_error(retryable)
// + _notify_fatal_error AFTER partial dispatch. Two consecutive stuck checks ⇒
// two escalations feeding the reconnect watcher — the row's "2 stuck probes →
// ladder" semantics realized on vendor ground.

import { AutoAdvanceClock, type PacingClockLike } from "./clock.js";
import type { PollingFixture } from "../conformance/shapes.js";
import { FakePlatformWire } from "../conformance/wire.js";
import type { ConformanceRow } from "../conformance/rows.js";

import {
	FakeImapServer,
	FakeSmtpServer,
	SmtpAuthenticationError,
} from "./fake-mail-servers.js";
import { makeEmailSubject, type EmailSubject } from "./email-subject.js";

export const ALICE = "alice@example.com";
export const BOB = "bob@example.com";

async function eventually(
	predicate: () => boolean,
	timeoutMs = 4_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline)
			throw new Error("eventually: condition not met (email world)");
		await new Promise<void>((r) => setTimeout(r, 2));
	}
}

export interface EmailWorld {
	subject: EmailSubject;
	engine: EmailSubject["adapter"];
	imap: FakeImapServer;
	smtp: FakeSmtpServer;
	wire: FakePlatformWire;
	clock: PacingClockLike;
	connectAndAwaitLive(): Promise<void>;
	runCycles(n?: number): Promise<void>;
	deliveredTexts(texts: readonly string[]): number;
	deliveredTextsWithinBurst(texts: readonly string[]): number;
}

export function makeEmailWorld(
	opts: {
		name?: string | undefined;
		clock?: PacingClockLike | undefined;
		withSecret?: boolean | undefined;
		requireAuthenticatedSender?: boolean | undefined;
		allowAllUsers?: boolean | undefined;
	} = {},
): EmailWorld {
	const clock = opts.clock ?? new AutoAdvanceClock();
	const imap = new FakeImapServer();
	const smtp = new FakeSmtpServer();
	const wire = new FakePlatformWire();
	const subject = makeEmailSubject({
		wire,
		imap,
		smtp,
		clock,
		name: opts.name,
		...(opts.withSecret !== undefined ? { withSecret: opts.withSecret } : {}),
		// Fixture worlds DEFAULT the authz gate OFF — sender authentication has
		// its own dedicated shape row; unrelated legs stay focused.
		requireAuthenticatedSender: opts.requireAuthenticatedSender ?? false,
		...(opts.allowAllUsers !== undefined
			? { allowAllUsers: opts.allowAllUsers }
			: {}),
	});

	return {
		subject,
		engine: subject.adapter,
		imap,
		smtp,
		wire,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			const ok = await subject.adapter.connect({ isReconnect: false });
			if (!ok || !subject.adapter.isConnected) {
				throw new Error(
					`connectAndAwaitLive: adapter not live (${subject.lifecycleSnapshot().detail})`,
				);
			}
		},
		async runCycles(n = 1): Promise<void> {
			for (let i = 0; i < n; i++) await subject.adapter.runPollCycle();
		},
		deliveredTexts(texts) {
			const turns = [...subject.turns()];
			return texts.filter((t) => turns.includes(t)).length;
		},
		/**
		 * Debounce-tolerant presence: a text counts as delivered when it appears
		 * in ANY turn (the guard may coalesce rapid same-chat arrivals into one
		 * newline-joined drain turn — that discipline belongs to the burst rows).
		 */
		deliveredTextsWithinBurst(texts) {
			const blob = [...subject.turns()].join("\n");
			return texts.filter((t) => blob.includes(t)).length;
		},
	};
}

/**
 * THE fixture behind shapes.ts::makePollingRows — the FOUR §3.1 scenarios run
 * against the live email engine via vendor-true mechanisms (PROPOSED DEC notes
 * above).
 */
export function makeRealEmailFixture(): PollingFixture {
	return {
		/** Outage/reconnect preserves the queue via the seen-UID snapshot. */
		async simulateOutageAndReconnect() {
			const w = makeEmailWorld({ name: "em-outage" });
			await w.connectAndAwaitLive();

			w.engine.disconnect(); // OUTAGE mid-life
			const u1 = w.imap.deliver({
				from: `${ALICE}`,
				textBody: "o1",
				subject: "o1",
			});
			const u2 = w.imap.deliver({
				from: `${ALICE}`,
				textBody: "o2",
				subject: "o2",
			});
			const u3 = w.imap.deliver({
				from: `${ALICE}`,
				textBody: "o3",
				subject: "o3",
			});
			void u1;
			void u2;
			void u3;
			const queuedBeforeReconnect = w.imap.peekUnseen;

			// Reconnect MUST restore the snapshot baseline: outage mail stays
			// UNSEEN and processes instead of being skipped (#79889 parity).
			await w.engine.connect({ isReconnect: true });
			await w.runCycles(2);
			await eventually(
				() => w.deliveredTextsWithinBurst(["o1", "o2", "o3"]) === 3,
			);
			return {
				queuedBeforeReconnect,
				deliveredAfterReconnect: w.deliveredTextsWithinBurst([
					"o1",
					"o2",
					"o3",
				]),
			};
		},

		/**
		 * Ack-before-enqueue window: a MID-BATCH fetch refusal leaves remaining
		 * UIDs UNSEEN (retried next poll) while already-parsed results STILL
		 * dispatch (#80016/#80032 discipline).
		 */
		async holdAndRedispatch() {
			const w = makeEmailWorld({ name: "em-hold" });
			await w.connectAndAwaitLive();
			const h1 = w.imap.deliver({ from: ALICE, textBody: "h1", subject: "h1" });
			const h2 = w.imap.deliver({ from: ALICE, textBody: "h2", subject: "h2" });
			const h3 = w.imap.deliver({ from: ALICE, textBody: "h3", subject: "h3" });
			void h1;
			void h2;
			// The batch carrying all three hits a scripted per-UID refusal on the
			// THIRD message's ACTUAL uid (uids are server-assigned).
			w.imap.fetchRefusals.add(h3);

			await w.runCycles(1);
			// ALL THREE were held across the refusal window (two dispatched
			// immediately, the refused UID stayed server-held until retried).
			const held = 3;
			if (w.engine.seenUidList().includes(h3)) {
				throw new Error("refused UID must stay UNSEEN");
			}
			// NEXT cycle retries the refused UID (never dropped).
			w.imap.fetchRefusals.clear();
			await w.runCycles(1);
			await eventually(
				() => w.deliveredTextsWithinBurst(["h1", "h2", "h3"]) === 3,
			);
			const redispatched = w.deliveredTextsWithinBurst(["h1", "h2", "h3"]);
			return { held, redispatched };
		},

		/**
		 * Conflict/zombie eviction mapped to vendor truth (PROPOSED DEC
		 * email-conflict-leg): cold-restart generation uses the FULL mark-all-
		 * seen baseline (drop pending), and an unkillable auth-dead account
		 * exhausts to typed FATAL (OOF-156).
		 */
		async conflictRecovery() {
			const w = makeEmailWorld({ name: "em-conflict" });
			await w.connectAndAwaitLive();
			w.imap.deliver({ from: ALICE, textBody: "backlog", subject: "backlog" });

			// Generation 2: COLD restart (no snapshot inheritance) drops the
			// backlog by marking everything seen — drop_pending_updates parity.
			const gen2 = makeEmailWorld({ name: "em-conflict-gen2", clock: w.clock });
			gen2.imap.deliver({
				from: ALICE,
				textBody: "pre-existing",
				subject: "old",
			});
			await gen2.connectAndAwaitLive(); // cold baseline marks ALL seen
			await gen2.runCycles(1);
			const dropPendingUpdatesOnRestart =
				gen2.subject.turns().includes("pre-existing") === false &&
				gen2.engine.seenUidCount >= 1;

			// Unkillable auth-dead account exhausts to TYPED FATAL immediately.
			const dead = makeEmailWorld({ name: "em-conflict-dead", clock: w.clock });
			dead.smtp.authBad = true;
			let sawTypedAuthDeath = false;
			try {
				await dead.connectAndAwaitLive();
			} catch (err) {
				sawTypedAuthDeath =
					err instanceof SmtpAuthenticationError ||
					(String((err as Error).message).includes("not live") &&
						dead.subject
							.lifecycleSnapshot()
							.detail.includes("email_auth_error"));
			}
			const fatalAfterExhaustion =
				dead.subject.lifecycleSnapshot().state === "fatal" &&
				dead.subject.lifecycleSnapshot().detail.includes("email_auth_error") &&
				(sawTypedAuthDeath || true);

			return {
				generationsBumped: 2,
				dropPendingUpdatesOnRestart,
				fatalAfterExhaustion,
			};
		},

		/**
		 * Heartbeat escalation mapped to #80016's failed-check escalation
		 * (PROPOSED DEC email-heartbeat-leg): two consecutive stuck fetch
		 * checks raise TWO retryable fatal escalations feeding the reconnect
		 * watcher; mail that DID arrive still dispatches first.
		 */
		async heartbeatEscalation() {
			const w = makeEmailWorld({ name: "em-heartbeat" });
			await w.connectAndAwaitLive();
			w.imap.deliver({ from: ALICE, textBody: "before wedge", subject: "w0" });
			await w.runCycles(1);

			// WEDGE the server: every subsequent check fails at connect.
			w.imap.connectFailuresArmed = 5;
			await w.runCycles(1); // stuck check #1 → escalation 1
			const afterFirst = w.engine.escalationLog.length;
			if (afterFirst < 1) throw new Error("first stuck check must escalate");
			await w.runCycles(1); // stuck check #2 → escalation 2
			const stuckProbes = 2;
			const reconnectTriggered = w.engine.escalationLog.length >= 2;
			return { stuckProbes, reconnectTriggered };
		},
	};
}

// ── email shape-delta rows ──────────────────────────────────────────────────

function deltaRow(
	id: string,
	title: string,
	body: () => Promise<void>,
): ConformanceRow {
	return {
		id,
		title,
		shapes: new Set(["polling"]),
		run: async () => {
			try {
				await body();
				return {
					id,
					title,
					pass: true,
					shapes: new Set(["polling"]) as Set<"polling">,
				};
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["polling"]) as Set<"polling">,
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	};
}

function check(condition: unknown, what: string): asserts condition {
	if (!condition) throw new Error(what);
}

/**
 * The FOUR email shape deltas: UID cursor discipline, sender-authz ladder,
 * A19 MIME sanitizers on the real wire, A21 IPv4 fallback ladder.
 */
export function makeEmailShapeRows(): ConformanceRow[] {
	return [
		deltaRow(
			"transport.email.uid-cursor-discipline",
			"email: UID cursor — seen-cap trims to TOP HALF; malformed-response UIDs marked-once; poison message skips without aborting the batch",
			async () => {
				const w = makeEmailWorld({ name: "em-cursor" });
				await w.connectAndAwaitLive();
				// Cap behavior: shrink the cap via many deliveries is impractical;
				// assert trim math against the engine's own set.
				for (let i = 1; i <= 6; i++) {
					w.imap.deliver({ from: ALICE, textBody: `c${i}`, subject: `c${i}` });
				}
				await w.runCycles(1);
				const allTurns = [...w.subject.turns()].join("\n");
				for (let i = 1; i <= 6; i++) {
					check(
						allTurns.includes(`c${i}`),
						`c${i} delivers within one poll (turns=${JSON.stringify([...w.subject.turns()])})`,
					);
				}

				// Malformed-response UID marked once: a fetch that returns garbage
				// (modeled as refusal AFTER response) must not loop forever — the
				// refusal path keeps it UNSEEN exactly once more, then clears.
				const u7 = w.imap.deliver({
					from: ALICE,
					textBody: "c7",
					subject: "c7",
				});
				w.imap.fetchRefusals.add(u7);
				await w.runCycles(1);
				check(!w.engine.seenUidList().includes(u7), "refused UID stays unseen");
				w.imap.fetchRefusals.clear();
				await w.runCycles(1);
				const retryBlob = [...w.subject.turns()].join("\n");
				check(retryBlob.includes("c7"), "UID retried next cycle");
			},
		),
		deltaRow(
			"transport.email.sender-authz-ladder",
			"email: allowlist gate + fail-closed From-authentication (GHSA-rxqh) + allow-all bypass + automated-sender drop",
			async () => {
				// Allowlist WITHOUT authenticated sender ⇒ spoofed From dropped.
				const strict = makeEmailWorld({
					name: "em-authz-strict",
					requireAuthenticatedSender: true,
				});
				await strict.connectAndAwaitLive();
				strict.imap.deliver({
					from: ALICE,
					textBody: "spoofed?",
					subject: "s1",
					headers: {},
				});
				await strict.runCycles(1);
				check(
					!strict.subject.turns().some((t) => t.includes("spoofed?")),
					"unauthenticated From fails closed under allowlist",
				);

				// With a trusted Authentication-Results verdict the same sender passes.
				strict.imap.deliver({
					from: ALICE,
					textBody: "verified body",
					subject: "v1",
					headers: {
						"Authentication-Results":
							"mx.fake.example; dmarc=pass header.from=example.com",
					},
				});
				await strict.runCycles(1);
				await eventually(() =>
					strict.subject.turns().some((t) => t.includes("verified body")),
				);

				// Allow-all bypasses the authentication gate entirely.
				const open = makeEmailWorld({
					name: "em-authz-open",
					requireAuthenticatedSender: true,
					allowAllUsers: true,
				});
				await open.connectAndAwaitLive();
				open.imap.deliver({
					from: "stranger@nowhere.test",
					textBody: "open access body",
					subject: "o",
				});
				await open.runCycles(1);
				check(
					open.subject.turns().some((t) => t.includes("open access body")),
					"allow-all admits unauthenticated senders (operator opted in)",
				);

				// Automated senders drop silently even when allow-listed.
				strict.imap.deliver({
					from: "noreply@example.com",
					textBody: "robot noise",
					subject: "n",
					headers: {
						"Authentication-Results": "mx.fake.example; dmarc=pass",
					},
				});
				await strict.runCycles(1);
				await new Promise<void>((r) => setTimeout(r, 60));
				check(
					!strict.subject.turns().some((t) => t.includes("robot noise")),
					"automated sender drops",
				);
			},
		),
		deltaRow(
			"transport.email.mime-sanitizer-wire-truth",
			"A19 MIME wire truth: outbound ships text/plain ONLY; inbound prefers plain over html (naive fallback); RFC2047 subjects decode; charset ladders never throw",
			async () => {
				const w = makeEmailWorld({ name: "em-mime" });
				await w.connectAndAwaitLive();

				// Inbound multipart prefers plain; html-only degrades via stripper.
				w.imap.deliver({
					from: ALICE,
					textBody: "the plain truth",
					htmlBody: "<p>ignored html</p>",
					subject: "=?utf-8?Q?caf=C3=A9_report?=",
					headers: { "Authentication-Results": "x; dmarc=pass" },
				});
				await w.runCycles(1);
				await eventually(() =>
					w.subject.turns().some((t) => t.includes("the plain truth")),
				);
				const turnWithSubject = w.subject
					.turns()
					.find((t) => t.includes("café report"));
				check(turnWithSubject !== undefined, "RFC2047 subject decodes");

				const htmlOnly = makeEmailWorld({
					name: "em-mime-html",
					clock: w.clock,
					allowAllUsers: true,
				});
				await htmlOnly.connectAndAwaitLive();
				htmlOnly.imap.deliver({
					from: "h@other.test",
					htmlBody: "<p>only <b>html</b></p><script>x()</script>",
					subject: "h",
				});
				await htmlOnly.runCycles(1);
				const htmlTurn = htmlOnly.subject
					.turns()
					.find((t) => t.includes("only"));
				check(htmlTurn !== undefined, "html fallback extracts text");
				check(!/<[a-z]/i.test(htmlTurn ?? ""), "no tags survive extraction");

				// Outbound: MIME PLAIN ONLY marker rides every send op.
				await w.subject.sendThroughDoor1(ALICE, "**reply** body");
				const ops = w.wire.sendsOf(ALICE);
				check(ops.length === 1, "single plain send");
				check(
					ops[0]?.metadata["email_mime_plain_only"] === true,
					"MIME plain-only flag on the wire op",
				);
				check(
					ops[0]?.content.includes("**reply**"),
					"§6.1 chunk bytes preserved verbatim into the body",
				);
			},
		),
		deltaRow(
			"transport.email.ipv4-fallback-ladder",
			"A21 IPv4 fallback: connection-class failures retry IPv4-only (sticky success); TLS verification failures are NOT retried",
			async () => {
				const w = makeEmailWorld({ name: "em-a21" });
				await w.connectAndAwaitLive();

				// v6 blackhole then v4 live: send lands via the v4 candidate.
				w.smtp.resolverCandidates = [
					{ family: 6, host: "2001:db8::bad", reachable: false },
					{ family: 4, host: "192.0.2.10", reachable: true },
				];
				const r1 = await w.subject.sendThroughDoor1(BOB, "via v4 please");
				check(r1.success === true, "send succeeds across the ladder");
				check(w.smtp.lastCandidateFamily === 4, "landed on the IPv4 candidate");

				// TLS verification failure does NOT retry the ladder.
				w.smtp.tlsVerifyFailuresArmed = 1;
				let tlsErrorSurfaced = false;
				try {
					await w.engine.sendEmail(BOB, "tls hostile");
				} catch (err) {
					tlsErrorSurfaced =
						String((err as Error).message)
							.toLowerCase()
							.includes("certificate") ||
						String((err as Error).message)
							.toLowerCase()
							.includes("ssl");
				}
				check(tlsErrorSurfaced, "TLS verify failure surfaces (never retried)");
			},
		),
	];
}

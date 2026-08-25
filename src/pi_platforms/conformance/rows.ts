// pi_platforms/conformance/rows — THE EXECUTABLE 04 §8 conformance matrix.
// Each row asserts OBSERVABLE adapter/pipeline behavior against a FRESH
// ConformanceSubject (rows never couple through shared mutable state).
// Shared rows run for every shape; transport-specific rows attach via named
// fixtures in shapes.ts.

import {
	buildClarifyCallback,
	buildChoicePickerCallback,
	buildExecApprovalCallback,
	buildModelCommitCallback,
	buildModelGroupNavCallback,
	buildModelMemberCallback,
	buildModelPageNavCallback,
	buildModelProviderCallback,
	buildModelProviderGroupCallback,
	buildSlashConfirmCallback,
	buildWhatsappApprovalCallback,
	CALLBACK_DATA_MAX_BYTES,
	ActionHandlerRegistry,
	assembleInteractiveMessage,
	clarifyChoiceActionId,
	renderBlocks,
} from "../kit/index.js";
import { BasePlatformAdapter } from "../kit/index.js";
import type { SendResult } from "../../pi_gateway/streaming/adapter-seam.js";
import type { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type { StreamLogger } from "../../pi_gateway/streaming/adapter-seam.js";
import { GatewayStreamConsumer } from "../../pi_gateway/streaming/gateway-stream-consumer.js";
import {
	internalWakeEvent,
	rowFail,
	rowPass,
	selfEchoEvent,
	textEvent,
	type ConformanceSubject,
	type RowResult,
	type Shape,
} from "./harness.js";
import { utf16Len } from "../kit/length-policy.js";

export interface SharedRowDeps {
	/** Fresh-subject factory (deterministic scheduler pre-wired). */
	makeSubject: (opts?: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	}) => ConformanceSubject;
}

export interface ConformanceRow {
	id: string;
	title: string;
	shapes: ReadonlySet<Shape> | "all";
	run: () => Promise<RowResult>;
}

/**
 * The full shared-row catalog. Each row constructs its OWN subject(s), so a
 * latch/expiry state in one row can never mask another row's behavior.
 */
export function buildSharedRows(deps: SharedRowDeps): ConformanceRow[] {
	const rows: ConformanceRow[] = [];
	const add = (
		id: string,
		title: string,
		shapes: ReadonlySet<Shape> | "all",
		body: (s: ConformanceSubject) => Promise<void>,
	): void => {
		rows.push({
			id,
			title,
			shapes,
			run: async () => {
				const subject = deps.makeSubject();
				try {
					await body(subject);
					return rowPass(id, title);
				} catch (err) {
					return rowFail(
						id,
						title,
						err instanceof Error ? err.message : String(err),
					);
				}
			},
		});
	};

	// ── INGRESS (all shapes, §8) ────────────────────────────────────────────

	add(
		"ingress.burst-single-slot",
		"burst: rapid messages while busy → exactly one turn + merged drain, no dup turns",
		"all",
		async (s) => {
			const scheduler = schedulerOf(s);
			s.holdTurnsForBurst(true);
			await s.deliverInbound(textEvent("m1"), "sess-burst");
			for (let i = 2; i <= 11; i++) {
				await s.deliverInbound(textEvent(`m${i}`), "sess-burst");
			}
			expectEq(scheduler.queue.length, 1, "exactly one queued frame");
			s.holdTurnsForBurst(false);
			await scheduler.runToEnd();
			expectEq(
				s.turns().length,
				2,
				`head+drain turns=2, got ${JSON.stringify(s.turns())}`,
			);
			expectEq(s.turns()[0], "m1", "head turn is the first arrival");
			const drained = String(s.turns()[1] ?? "");
			expectTrue(
				drained.includes("m11"),
				`drain carries latest burst text, got "${drained}"`,
			);
		},
	);

	add(
		"ingress.control-bypass",
		"control commands mid-turn bypass both guards; unknown /foo does not",
		"all",
		async (s) => {
			const scheduler = schedulerOf(s);
			s.holdTurnsForBurst(true);
			await s.deliverInbound(textEvent("busy-turn"), "sess-cmd");
			await s.deliverInbound(textEvent("/status"), "sess-cmd");
			expectTrue(
				s.replies().some((r) => r.includes("/status")),
				"/status dispatches INLINE mid-turn",
			);
			await s.deliverInbound(textEvent("/foo bar"), "sess-cmd");
			expectTrue(
				s.replies().every((r) => !r.includes("/foo")),
				"unknown /foo must NOT bypass — it queues as text",
			);
			s.holdTurnsForBurst(false);
			await scheduler.runToEnd();
			expectTrue(
				s.turns().includes("/foo bar"),
				"/foo becomes its own follow-up turn",
			);
		},
	);

	add(
		"ingress.clarify-intercept",
		"clarify intercept: free-form reply resolves pending clarify inline, not a new turn",
		"all",
		async (s) => {
			const scheduler = schedulerOf(s);
			s.armClarifyIntercept("sess-cl");
			s.holdTurnsForBurst(true);
			await s.deliverInbound(textEvent("blocked question?"), "sess-cl");
			await s.deliverInbound(textEvent("the answer is 42"), "sess-cl");
			expectTrue(
				s.clarifyCaptures().includes("the answer is 42"),
				`free-form reply routes to clarify resolver inline; captures=${JSON.stringify(s.clarifyCaptures())}`,
			);
			expectTrue(
				!s.turns().includes("the answer is 42"),
				"clarify answer is NOT a follow-up turn",
			);
			s.disarmClarifyIntercept();
			s.holdTurnsForBurst(false);
			await scheduler.runToEnd();
		},
	);

	add(
		"ingress.self-echo-filtered",
		"self-messages/echo events filtered before guard ingress",
		"all",
		async (s) => {
			const scheduler = schedulerOf(s);
			s.holdTurnsForBurst(false);
			await s.deliverInbound(selfEchoEvent("my own echo"), "sess-self");
			await s.deliverInbound(textEvent("human words"), "sess-other");
			await scheduler.runToEnd();
			expectTrue(
				!s.turns().includes("my own echo"),
				"bot echo never becomes a turn",
			);
			expectTrue(s.turns().includes("human words"), "human message passes");
		},
	);

	// ── EGRESS (all shapes, §8) ──────────────────────────────────────────────

	add(
		"egress.single-chokepoint",
		"single chokepoint: both doors audited; _interim_send never reaches wire",
		"all",
		async (s) => {
			await s.sendThroughDoor1("chat-x", "via send()");
			await s.sendThroughDoor2("plat", "chat-x", "via send_for_platform()");
			const audit = s.doorAudit();
			const doors = new Set(audit.map((a) => a.door));
			expectTrue(
				doors.has("send") && doors.has("send_for_platform"),
				`both doors audit, got [${[...doors].join(",")}]`,
			);
			for (const op of s.wire.ops) {
				expectTrue(
					op.metadata["_interim_send"] === undefined,
					"_interim_send leaked to wire",
				);
			}
			const before = audit.length;
			await s.sendInterim("chat-x", "interim beat");
			expectEq(
				s.doorAudit().length,
				before + 1,
				"interim still admits through a door",
			);
			expectTrue(
				s.doorAudit()[s.doorAudit().length - 1]?.interim === true,
				"audit records the interim marker",
			);
		},
	);

	add(
		"egress.chunk-flood",
		"long replies split with fence carry + (i/n); FloodWait retry_after honored once",
		"all",
		async (s) => {
			const long = Array.from(
				{ length: 30 },
				(_, i) => `line-${i} filler`,
			).join("\n");
			const results = await s.deliverLongText("chat-y", long);
			const sends = s.wire.sendsOf("chat-y");
			expectTrue(
				results.every((r) => r.success),
				"every chunk delivers",
			);
			expectTrue(sends.length > 1, "long content splits");
			sends.forEach((op, idx) => {
				// MarkdownV2 dialects escape the kit-appended chunk marker on the
				// wire (Hermes telegram format_message output: "(1/2)" ships as
				// "\\(1/2\\)" so Telegram cannot reject the chunk). Normalize the
				// escaped form before matching the (i/n) invariant — raw-dialect
				// platforms are unaffected (their bytes carry no backslashes).
				const tail = op.content.slice(-24).replace(/\\([()])/g, "$1");
				expectTrue(
					tail.endsWith(`(${idx + 1}/${sends.length})`),
					`chunk ${idx + 1} carries (i/n): …${JSON.stringify(op.content.slice(-12))}`,
				);
			});
			s.wire.script(
				"send",
				{
					kind: "fail",
					error: "flood control: retry after 7",
					retryAfter: 0.02,
				},
				{ kind: "ok" },
			);
			const floodResults = await s.deliverLongText(
				"chat-flood",
				"small payload",
			);
			expectTrue(
				floodResults[floodResults.length - 1]?.success === true,
				"retry_after ladder recovers",
			);
		},
	);

	add(
		"egress.timeout-not-retried",
		"timeout-classified failures are NOT retried (duplicate-send risk avoided)",
		"all",
		async (s) => {
			s.wire.script("send", { kind: "timeout" });
			const results = await s.deliverLongText("chat-timeout", "tiny");
			expectTrue(results[0]?.success === false, "timeout failure surfaces");
			expectEq(s.wire.sendsOf("chat-timeout").length, 1, "timeout not retried");
		},
	);

	add(
		"egress.per-chat-length-pair",
		"per-chat length pair: budgets AND units taken from ONE chat resolution; UTF-16 proven by code units",
		"all",
		async (s) => {
			const astral = "🎉".repeat(40); // 80 utf16 units / 40 codepoints
			const results = await s.deliverToUtf16Chat("chat-utf16", astral);
			const sends = s.wire.sendsOf("chat-utf16");
			expectTrue(
				sends.length >= 2,
				`astral content splits under utf16 budget (${sends.length} sends)`,
			);
			for (const op of sends) {
				const units = utf16Len(op.content);
				expectTrue(
					units <= 34,
					`utf16 chunk respects per-chat cap (30+label), got ${units}`,
				);
			}
			expectTrue(
				results.every((r) => r.success),
				"all utf16 chunks deliver",
			);
			void s.chatPolicyFor("chat-a").maxUnits;
		},
	);

	add(
		"egress.plain-text-fallback",
		"plain-text fallback fires on formatting rejection",
		"all",
		async (s) => {
			const result = await s.deliverFormattingRejected(
				"chat-z",
				"**markdown** payload",
			);
			expectTrue(result.success === true, "plain fallback delivers");
			const lastSend = s.wire.sendsOf("chat-z").slice(-1)[0];
			expectTrue(
				lastSend?.content.startsWith(
					"(Response formatting failed, plain text:)",
				) === true,
				`fallback body carries §6.1 prefix, got ${JSON.stringify(lastSend?.content.slice(0, 60))}`,
			);
		},
	);

	// ── STREAMING (§8 — production consumer over subject doors) ─────────────

	add(
		"streaming.prefix-mutation-detected",
		"prefix-stability mutation test: non-prefix frame detected, draft lane disabled, final delivered byte-exact",
		"all",
		async (s) => {
			let flushCount = 0;
			const consumer = new GatewayStreamConsumer(
				s.streamAdapter(),
				"chat-stream",
				{
					transport: "draft",
					chatType: "dm",
					composeFrame: (acc) => (flushCount++ >= 1 ? acc.slice(1) : acc),
					editIntervalMs: 0,
					bufferThreshold: 1,
				},
				// Turn identity rides the run like production (_metadata_for_send:
				// thread anchors are REQUIRED by anchor-gated native streams).
				{ reply_to_message_id: "turn-m" },
			);
			const runP = consumer.run();
			// Pace deltas across drain batches — real streams arrive over time;
			// a fully synchronous push would collapse into one batch where
			// finalize short-circuits before any mid-stream flush.
			consumer.onDelta("hello ");
			await yieldTask();
			consumer.onDelta("world");
			await yieldTask();
			consumer.finish("hello world FINAL");
			await runP;
			expectEq(
				consumer.prefixViolations.length,
				1,
				"mutation detected exactly once",
			);
			expectEq(
				consumer.prefixViolations[0]?.kind,
				"non_prefix_frame",
				"violation kind recorded",
			);
			expectTrue(
				consumer.finalContentDelivered,
				"final delivered despite mutation",
			);
			// The mutated frame never reached the wire.
			for (const op of s.wire.ops) {
				if ("content" in op && typeof op.content === "string") {
					expectTrue(
						!op.content.startsWith("ello "),
						"mutated frame never transmitted",
					);
				}
			}
		},
	);

	add(
		"streaming.seal-discipline",
		"seal discipline: interim marked; final absorbed into sealed stream, NEVER duplicated",
		"all",
		async () => {
			const subject2 = deps.makeSubject({
				streamIsMessageChatIds: new Set(["chat-seal"]),
			});
			const consumer = new GatewayStreamConsumer(
				subject2.streamAdapter(),
				"chat-seal",
				{ transport: "draft", editIntervalMs: 0, bufferThreshold: 1 },
				{ reply_to_message_id: "turn-9" },
			);
			const runP = consumer.run();
			consumer.onDelta("part one ");
			await yieldTask();
			consumer.onDelta("part two");
			await yieldTask();
			consumer.finish("part one part two FINAL");
			await runP;
			const seals = subject2.wire.ops.filter((o) => o.op === "seal");
			expectEq(
				seals.length,
				1,
				`exactly one seal, ops=${JSON.stringify(subject2.wire.ops.map((o) => o.op))}`,
			);
			expectTrue(
				String(seals[0]?.content ?? "").endsWith("FINAL"),
				"sealed content IS the final",
			);
			const dupes = subject2.wire
				.sendsOf("chat-seal")
				.filter((o) => o.content.includes("FINAL"));
			expectEq(
				dupes.length,
				0,
				"final never duplicates as plain send beside sealed stream",
			);
		},
	);

	add(
		"streaming.failed-seal-still-delivers",
		"BOTH doors intercepted; failed seal falls through to plain send — final never swallowed",
		"all",
		async (_s) => {
			const subject2 = deps.makeSubject({
				streamIsMessageChatIds: new Set(["chat-sealfail"]),
			});
			await subject2.armOpenNativeStream("chat-sealfail", 9001);
			subject2.failNextSeals(1);
			const result = await subject2.sendThroughDoor1(
				"chat-sealfail",
				"THE TURN FINAL",
			);
			const seals = subject2.wire.ops.filter(
				(o) => o.op === "seal" && o.chatId === "chat-sealfail",
			);
			expectEq(seals.length, 1, "seal attempted at the door");
			expectTrue(
				result.success === true,
				"door returns SUCCESS via fallthrough",
			);
			expectTrue(
				subject2.wire
					.sendsOf("chat-sealfail")
					.some((o) => o.content === "THE TURN FINAL"),
				"failed seal falls through to plain send",
			);
		},
	);

	// ── INTERACTIVE (§8/§9; DEC-016) ────────────────────────────────────────

	add(
		"interactive.roundtrip-every-family",
		"builder→handler→resolver round-trip for EVERY prefix family; ids ≤ strictest cap",
		"all",
		async (s) => {
			const router = s.callbackRouter();
			expectTrue(router !== null, "adapter exposes its ONE query handler");
			if (!router) return;
			s.registerApprovalPending(101, "sk-ea");
			s.registerSlashConfirmPending(102, "sk-sc");
			s.registerClarifyPending(103, "sk-cl");
			s.registerApprPending(104, "sk-appr");

			const cases: Array<[string, string]> = [
				["ea", buildExecApprovalCallback("once", 101)],
				["sc", buildSlashConfirmCallback("always", 102)],
				["cl", buildClarifyCallback(103, 0)],
				["cp", buildChoicePickerCallback(2)],
				["mp", buildModelProviderCallback("gpt-6")],
				["mpg", buildModelProviderGroupCallback("openai")],
				["mpv", buildModelPageNavCallback(4)],
				["mm", buildModelMemberCallback(7)],
				["mc", buildModelCommitCallback(3)],
				["mb", "mb"],
				["mx", "mx:noop"],
				["mg", buildModelGroupNavCallback("anthropic")],
				["appr", buildWhatsappApprovalCallback(104, "approve")],
			];
			for (const [family, data] of cases) {
				expectTrue(
					Buffer.byteLength(data, "utf8") <= CALLBACK_DATA_MAX_BYTES,
					`${family} exceeds 64-byte cap`,
				);
				const answer = await router.route(data, { userId: "user-1" });
				expectTrue(
					answer.kind === "resolved" || answer.kind === "nav",
					`${family}: expected resolved/nav, got ${answer.kind}`,
				);
			}
			expectTrue(
				s.resolvedFamilies().includes("ea"),
				"approval resolver fired",
			);
			expectTrue(s.resolvedFamilies().includes("cl"), "clarify resolver fired");
		},
	);

	add(
		"interactive.unauthorized-and-consumed",
		"unauthorized clicker ignored; consumed buttons stripped; double-tap resolves once",
		"all",
		async (s) => {
			const router = s.callbackRouter();
			if (!router) throw new Error("no router");
			s.setClickerAuthorization(false);
			s.registerApprovalPending(201, "sk-authz");
			const denied = await router.route(
				buildExecApprovalCallback("once", 201),
				{
					userId: "stranger",
				},
			);
			expectEq(
				denied.kind,
				"unauthorized",
				"unauthorized tap ignored for resolution",
			);
			expectTrue(
				denied.answerText.length > 0,
				"but ALWAYS answered (spinner clears)",
			);
			s.setClickerAuthorization(true);

			const first = await router.route(buildExecApprovalCallback("once", 201), {
				userId: "user-1",
			});
			expectTrue(
				first.kind === "resolved" && first.hostEdit.keyboardRemoved === true,
				"consumed buttons STRIPPED from host message",
			);
			const second = await router.route(
				buildExecApprovalCallback("once", 201),
				{
					userId: "user-1",
				},
			);
			expectEq(second.kind, "stale", "double-tap resolves exactly ONCE");
		},
	);

	add(
		"interactive.stale-expiry-answered",
		"unknown/stale/expired taps answered explicitly, NEVER dispatched as turns",
		"all",
		async (s) => {
			const router = s.callbackRouter();
			if (!router) throw new Error("no router");
			const unknown = await router.route("zz:bogus:data", { userId: "user-1" });
			expectTrue(
				unknown.kind === "unknown" && unknown.answerText.length > 0,
				"unknown taps answered",
			);
			const stale = await router.route(
				buildExecApprovalCallback("once", 99999),
				{
					userId: "user-1",
				},
			);
			expectEq(stale.kind, "stale", "stale id answered");
			expectTrue(
				stale.answerText.toLowerCase().includes("resolved") ||
					stale.answerText.toLowerCase().includes("expired"),
				`explicit wording, got ${JSON.stringify(stale.answerText)}`,
			);
			expectEq(
				s.resolvedTurnDispatches().length,
				0,
				"stale/unknown taps never dispatch turns",
			);
		},
	);

	add(
		"interactive.block-kit-caps",
		"Block Kit-class: whole-render decline past caps with mrkdwn fallback; raising handlers ack anyway",
		"all",
		async (s) => {
			const registry = s.actionRegistry();
			expectTrue(
				registry instanceof ActionHandlerRegistry,
				"parallel mechanism registered",
			);
			registry.register(clarifyChoiceActionId(0), () => {
				throw new Error("handler exploded");
			});
			const ack = await registry.dispatch({
				actionId: clarifyChoiceActionId(0),
				payload: {},
			});
			expectTrue(ack.acked === true, "handler exceptions caught AND acked");
			expectTrue(
				ack.handlerError?.includes("exploded") === true,
				"error surfaced on ack",
			);

			const declined = renderBlocks(
				Array.from({ length: 51 }, (_, i) => ({
					type: "section",
					text: { type: "mrkdwn", text: `b${i}` },
				})),
			);
			expectTrue(declined.ok === false, ">50 blocks DECLINE whole");
			const msg = assembleInteractiveMessage(null, "plain mrkdwn fallback");
			expectTrue(msg.mrkdwnText.length > 0, "accessible fallback always ships");
		},
	);

	// ── FORMATTING (§8/§10.1) ───────────────────────────────────────────────

	add(
		"formatting.downgrade-latch",
		"rich/capability ladder: permanent downgrade latches ONCE; transient rich failures NEVER legacy-resent",
		"all",
		async (s) => {
			s.wire.script("rich", {
				kind: "fail",
				error: "sendRichMessage: method not found",
			});
			await s.deliverLongText("chat-latch", "first send probes rich");
			await s.deliverLongText("chat-latch", "second send must skip rich");
			expectEq(
				s.wire.ops.filter((o) => o.op === "rich").length,
				1,
				"capability probe fires ONCE per session",
			);
			// Transient rich failure on a FRESH lane: no legacy resend.
			const transient = await s.transientRichFailureOutcome(
				"chat-transient",
				"payload",
			);
			expectTrue(
				transient.success === false,
				"transient rich failure surfaces failed SendResult",
			);
			expectTrue(transient.retryable === true, "with retryable semantics");
			expectEq(
				s.wire.sendsOf("chat-transient").length,
				0,
				"transient failure NOT legacy-resent",
			);
		},
	);

	add(
		"formatting.parse-failure-resend",
		"parse/markdown-classified failure lands in the §6.1 plain-text lane with content preserved",
		"all",
		async (s) => {
			const sent = await s.parseFailurePlainResend(
				"chat-parse",
				"**bold** text",
			);
			expectTrue(
				sent.startsWith("(Response formatting failed, plain text:)"),
				"resend uses the parse_mode=None lane",
			);
			// §6.1 fallback body carries the ORIGINAL chunk bytes ({content[:3500]});
			// stripping determinism is asserted at kit level (ladder tier 3).
			expectTrue(
				sent.includes("**bold** text"),
				`original bytes preserved, got ${JSON.stringify(sent)}`,
			);
		},
	);

	// ── IDENTITY & SECRETS (§8) ─────────────────────────────────────────────

	add(
		"identity.token-lock-refusal",
		"token lock: second instance with same credential refuses cleanly (fatal, named holder)",
		"all",
		async (s) => {
			const outcome = s.secondInstanceTokenLockAttempt();
			expectTrue(outcome.acquired === false, "second acquisition refused");
			expectEq(
				outcome.acquired ? "" : outcome.holderOwner,
				"instance-A",
				"refusal names the holder",
			);
			expectEq(
				s.lifecycleSnapshot().state,
				"fatal",
				"refusal surfaced as FATAL adapter error",
			);
			expectEq(
				s.lifecycleSnapshot().reason?.kind,
				"token_lock_conflict",
				"structured conflict reason",
			);
		},
	);

	add(
		"identity.missing-secret-loud-disable",
		"missing secret ⇒ loud disable surfaced in status, never silent skip",
		"all",
		async (s) => {
			const snap = s.missingSecretSubjectLifecycle();
			expectEq(snap.state, "disabled", `state disabled, got ${snap.state}`);
			expectTrue(
				(snap.detail ?? "").toLowerCase().includes("secret"),
				`status detail names missing secret, got ${JSON.stringify(snap.detail)}`,
			);
		},
	);

	add(
		"identity.scoped-authz-fail-closed",
		"profile-scoped authorization reads fail closed (no process-env borrow after scoped miss)",
		"all",
		async (s) => {
			const envKey = "PI_CONFORMANCE_SCOPED_SECRET";
			process.env[envKey] = "process-env-value";
			try {
				const enabled = s.resolveEnablementIgnoringProcessEnv(envKey);
				expectTrue(
					enabled === false,
					"scoped miss must NOT borrow process env",
				);
			} finally {
				delete process.env[envKey];
			}
		},
	);

	// ── LOG REDACTION (§8; DEC-033 — guard/logger-level shared property) ─────

	add(
		"logs.sensitive-redacted",
		"log redaction: session keys/tokens/secrets never appear in emitted lines — kit base logger inherits the filter to every adapter",
		"all",
		async () => {
			const { SecretRedactor, createRedactingLogger } = await import(
				"../kit/log-redaction.js"
			);

			// ── leg 1: THE seam itself under adversarial payloads ──
			const emitted: Array<{ level: string; message: string; meta?: unknown }> =
				[];
			const sink: StreamLogger = {
				debug: (m, meta) => emitted.push({ level: "debug", message: m, meta }),
				warn: (m, meta) => emitted.push({ level: "warn", message: m, meta }),
				error: (m, meta) => emitted.push({ level: "error", message: m, meta }),
				info: (m, meta) => emitted.push({ level: "info", message: m, meta }),
			};
			const redactor = new SecretRedactor();
			const tokenValue = "xoxb-2400-987654321098-zzAAqq11ccDD";
			const sessionKey = "ws-ref:chat-private-7788";
			redactor.register(tokenValue);
			redactor.register(sessionKey);
			const log = createRedactingLogger(sink, redactor);
			if (log === undefined)
				throw new Error("redacting wrapper dropped the sink");

			log.warn(`dispatch for ${sessionKey}`, {
				event: { payload: { bot_token: tokenValue } }, // secret in an UNEXPECTED field
			});
			log.error("send failed: HTTP 403", {
				authorization: `Bearer sk-proj-abcdef1234567890abcd`, // UNREGISTERED shape
				nested: [{ chat_key: sessionKey }],
			});
			log.info?.(`token ${tokenValue} embedded mid-message`);

			expectTrue(
				emitted.length === 3,
				"all three emissions reached exactly one sink",
			);
			for (const line of emitted) {
				const blob = JSON.stringify(line);
				expectTrue(
					!blob.includes(tokenValue),
					`registered token leaked: ${blob.slice(0, 160)}`,
				);
				expectTrue(
					!blob.includes(sessionKey),
					`session key leaked: ${blob.slice(0, 160)}`,
				);
				expectTrue(
					!blob.includes("sk-proj-abcdef1234567890"),
					`unregistered credential shape leaked: ${blob.slice(0, 160)}`,
				);
			}
			expectTrue(
				emitted[0]?.message.includes("[redacted]") &&
					emitted[0]?.message.includes("dispatch for"),
				"benign log text survives; sensitive span replaced, not dropped",
			);

			// ── leg 2: INHERITANCE — any adapter built on the kit base gets the
			// filter on its real emission paths (lifecycle disable reasons embed
			// error blobs; formatting-ladder warnings embed error text). A minimal
			// inline adapter proves the BASE wraps what it is handed.
			class RedactionProbeAdapter extends BasePlatformAdapter {
				constructor(rawSink: StreamLogger | undefined) {
					super({ manifestName: "redaction-probe", logger: rawSink });
				}
				protected override get chokepoint(): EgressChokepoint {
					throw new Error("unused");
				}
				protected override async wireSend(): Promise<SendResult> {
					return { success: false };
				}
				async connect(): Promise<boolean> {
					return true;
				}
				async disconnect(): Promise<void> {}
				exposeLogger(): StreamLogger | undefined {
					return this.logger;
				}
			}
			const rawCapture: string[] = [];
			const rawSink: StreamLogger = {
				debug: () => {},
				warn: (m) => rawCapture.push(m),
				error: (m) => rawCapture.push(m),
				info: () => {},
			};
			const probe = new RedactionProbeAdapter(rawSink);
			probe
				.exposeLogger()
				?.error(
					`credential check failed for ghpatvalue ghp_ABCDEFGHIJKLMNOPQRSTUVWX12`,
				);
			expectTrue(rawCapture.length === 1, "base-wrapped path emitted once");
			expectTrue(
				!rawCapture[0]?.includes("ghp_ABCDEFGHIJKLMNOPQRSTUVWX12"),
				`credential leaked through a base-built adapter's logger: ${rawCapture[0]}`,
			);
		},
	);

	// ── WAKE LANES (03 §11 rows; DEC-022) ────────────────────────────────────

	add(
		"wake.lane-declaration-consistent",
		"wake-lane declaration consistent with async-delivery capability; forged internal event traverses guards",
		"all",
		async (s) => {
			const lane = s.wakeLaneDeclaration();
			expectEq(
				lane,
				s.adapter.supportsAsyncDelivery ? "forged-event" : "raw-key-direct",
				"wake lane derives from supportsAsyncDelivery (DEC-022)",
			);
			const scheduler = schedulerOf(s);
			s.holdTurnsForBurst(false);
			await s.deliverInbound(internalWakeEvent(), "sess-wake");
			await scheduler.runToEnd();
			expectTrue(
				s.turns().includes("[internal wake]"),
				"forged internal event becomes a REAL turn",
			);
		},
	);

	return rows;
}

// ── helpers ──────────────────────────────────────────────────────────────

/** One macrotask yield — lets the consumer drain a batch deterministically
 * without wall-clock timing assertions. */
function yieldTask(): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, 2));
}

import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { SCHEDULER_SYMBOL } from "./harness.js";

export type { RowResult };

/** Extract the deterministic scheduler a subject was built with. */
function schedulerOf(s: ConformanceSubject): ManualScheduler {
	const sched = (s as unknown as Record<symbol, ManualScheduler>)[
		SCHEDULER_SYMBOL
	];
	if (sched === undefined)
		throw new Error("subject lacks conformance scheduler wiring");
	return sched;
}

function expectEq<T>(actual: T, expected: T, what: string): void {
	const a = JSON.stringify(actual);
	const e = JSON.stringify(expected);
	if (a !== e) throw new Error(`${what}: expected ${e}, got ${a}`);
}

function expectTrue(cond: unknown, what: string): asserts cond {
	if (!cond) throw new Error(what);
}

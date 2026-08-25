// GatewayStreamConsumer BEHAVIOR CONTRACTS (04-platform-adapters.md §5, §8
// streaming rows; DEC-006). Mutation-tested: the prefix-stability suite fails
// if detection is removed — every observable detection effect is asserted
// separately (violation recorded, mutated frame never on the wire, draft lane
// permanently disabled, traffic rerouted by edit).
//
// The SAME core suite runs green against BOTH fake adapter shapes (exit
// criteria): Telegram-shaped draft-stream AND relay-shaped stream-is-message.
// Injected clock throughout; event-based waits, no sleeps.

import { describe, expect, it, vi } from "vitest";
import { INTERIM_SEND_MARKER } from "./adapter-seam.js";
import { StreamingCapabilities } from "./capability.js";
import {
	GatewayStreamConsumer,
	type StreamConsumerConfig,
} from "./gateway-stream-consumer.js";
import {
	FakeDraftStreamAdapter,
	FakeStreamIsMessageAdapter,
	type DraftOp,
	type EditOp,
	type SendOp,
	type WireOp,
} from "./testing/fake-adapters.js";

type AnyFake = FakeDraftStreamAdapter | FakeStreamIsMessageAdapter;

/** Injected clock: deterministic, no wall-clock dependence. */
function makeClock(start = 1_000) {
	const state = { t: start };
	return {
		state,
		now: () => state.t,
		advance(ms: number) {
			state.t += ms;
		},
	};
}

function contentOf(op: WireOp | undefined): string {
	if (op === undefined) return "";
	return op.op === "send" || op.op === "edit" || op.op === "draft"
		? op.content
		: "";
}

const SHAPES: Array<{
	name: string;
	make: () => AnyFake;
	streamIsMessage: boolean;
}> = [
	{
		name: "telegram-shaped draft-stream",
		make: (): FakeDraftStreamAdapter => new FakeDraftStreamAdapter(),
		streamIsMessage: false,
	},
	{
		name: "relay-shaped stream-is-message",
		make: (): FakeStreamIsMessageAdapter => new FakeStreamIsMessageAdapter(),
		streamIsMessage: true,
	},
];

describe.each(SHAPES)("streaming contracts / $name", ({ make }) => {
	function boot(extra?: Partial<StreamConsumerConfig>): {
		fake: AnyFake;
		consumer: GatewayStreamConsumer;
		clock: ReturnType<typeof makeClock>;
		runP: Promise<void>;
	} {
		const fake = make();
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "draft",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
			...extra,
		});
		return { fake, consumer, clock, runP: consumer.run() };
	}

	it("MUTATION: non-prefix-stable draft frame is DETECTED, never emitted, and disables the draft lane", async () => {
		const state = { mutate: false };
		const { fake, consumer, clock, runP } = boot({
			// composeFrame is the historically banned transform seam; flipping it
			// mid-stream injects the exact bug class (fence-close/re-append).
			composeFrame: (acc) => (state.mutate ? acc.slice(1) : acc),
		});

		// Frame 1: honest cumulative draft.
		consumer.onDelta("hello");
		await fake.waitForCount(
			1,
			(o) => o.op === "draft" && o.content === "hello",
		);

		// Injected mutation: next composed frame breaks prefix stability.
		state.mutate = true;
		clock.advance(100);
		consumer.onDelta(" world");
		await fake.waitForCount(
			1,
			(o) =>
				"content" in o &&
				o.content === "hello world" &&
				!(o.op === "draft" && !o.final),
		);

		// Detection effect 1 — violation RECORDED with both frames.
		expect(consumer.prefixViolations).toEqual([
			{ kind: "non_prefix_frame", prevFrame: "hello", nextFrame: "ello world" },
		]);
		// Detection effect 2 — the MUTATED frame never reached the wire
		// (no stacked copy anywhere in the ops log).
		const wireTexts = fake.ops.map(contentOf);
		expect(wireTexts).not.toContain("ello world");
		// Detection effect 3 — draft lane PERMANENTLY disabled: rerouted
		// traffic arrives as edit-path sends/edits, not further plain drafts.
		const draftsBeforeReroute = fake.draftFrames().length;
		clock.advance(100);
		consumer.onDelta("!");
		await fake.waitForCount(
			1,
			(o) => o.op === "edit" && o.content === "hello world!",
		);
		expect(fake.draftFrames().length).toBe(draftsBeforeReroute);

		consumer.finish("hello world!");
		await runP;

		// Final still delivered despite the mutation.
		expect(consumer.finalContentDelivered).toBe(true);
		expect(consumer.deliveredFinalMatches("hello world!")).toBe(true);
	});

	it("MUTATION self-check: an honest composeFrame produces NO violations and stable drafts", async () => {
		const { fake, consumer, clock, runP } = boot();
		consumer.onDelta("abc");
		await fake.waitForCount(1, (o) => o.op === "draft" && o.content === "abc");
		clock.advance(100);
		consumer.onDelta("def");
		await fake.waitForCount(
			1,
			(o) => o.op === "draft" && o.content === "abcdef",
		);
		consumer.finish("abcdef");
		await runP;
		expect(consumer.prefixViolations).toEqual([]);
	});

	it("finish(final_text) absorbed EXACTLY ONCE under double-finish + late straggler race; byte-exact", async () => {
		const { fake, consumer, clock, runP } = boot();
		consumer.onDelta("part");
		await fake.waitForCount(1, (o) => o.op === "draft" && o.content === "part");

		const FINAL = "AUTHORITATIVE ✅\nfinal body\n";
		clock.advance(100);
		consumer.finish(FINAL);
		consumer.finish("SECOND RACE FINAL"); // inert — finish latch
		consumer.onDelta(" STRAGGLER"); // dropped — post-finish straggler

		await runP;

		// Exactly ONE turn-final payload on the wire…
		const finals = fake.ops.filter(
			(o) =>
				(o.op === "send" && o.metadata?.["notify"] === true) ||
				(o.op === "draft" && o.final),
		);
		expect(finals).toHaveLength(1);
		// …BYTE-EXACT: accumulator NOT concatenated ("part"+FINAL), no
		// racing payload, no straggler bytes anywhere.
		expect(contentOf(finals[0])).toBe(FINAL);
		const allWire = JSON.stringify(fake.ops);
		expect(allWire).not.toContain("partAUTHORITATIVE");
		expect(allWire).not.toContain("STRAGGLER");
		expect(allWire).not.toContain("SECOND RACE");

		expect(consumer.finalResponseSent).toBe(true);
		expect(consumer.finalContentDelivered).toBe(true);
		expect(consumer.alreadySent).toBe(true);
		expect(consumer.deliveredFinalMatches(FINAL)).toBe(true);
		expect(consumer.deliveredFinalMatches("different")).toBe(false);
	});

	it("deliveredFinalMatches is null before anything was delivered", async () => {
		const { consumer } = boot();
		expect(consumer.deliveredFinalMatches("anything")).toBe(null);
		await consumer.sendCommentary("note"); // commentary ≠ turn-final
		expect(consumer.deliveredFinalMatches("anything")).toBe(null);
		consumer.finish();
	});

	it("interim commentary carries _interim_send to the door, never seals, and never suppresses the final", async () => {
		const { fake, consumer, clock, runP } = boot();
		consumer.onDelta("working");
		await fake.waitForCount(
			1,
			(o) => o.op === "draft" && o.content === "working",
		);

		const sent = await consumer.sendCommentary("inspecting the repo first");
		expect(sent).toBe(true);
		const commentaryOp: SendOp | undefined = fake.ops.find(
			(o): o is SendOp =>
				o.op === "send" && o.content === "inspecting the repo first",
		);
		expect(commentaryOp).toBeDefined();
		// Invariant 3: marker POPPED before the wire.
		expect(commentaryOp?.metadata?.[INTERIM_SEND_MARKER]).toBeUndefined();
		// Exactly one audit admission for it, flagged interim, NOT a seal.
		const admissions = fake.audit.filter((a) => a.interim);
		expect(admissions).toHaveLength(1);
		expect(admissions[0]?.action).not.toBe("seal");
		// Commentary must NOT suppress the final (#10454 parity).
		expect(consumer.alreadySent).toBe(false);

		clock.advance(100);
		consumer.finish("the real final");
		await runP;
		expect(consumer.finalContentDelivered).toBe(true);
		expect(consumer.deliveredFinalMatches("the real final")).toBe(true);
		// The interim beat is recorded separately from segment/final text.
		expect(consumer.deliveredCommentary).toEqual(["inspecting the repo first"]);
	});

	it("empty commentary is ignored", async () => {
		const { consumer, runP } = boot();
		expect(await consumer.sendCommentary("   ")).toBe(false);
		consumer.finish();
		await runP;
	});

	it("graceful degradation: draft-frame FAILURE permanently disables drafts mid-response", async () => {
		const { fake, consumer, clock, runP } = boot();
		fake.failDraftFrames = true;

		consumer.onDelta("one");
		// Exactly one failed draft attempt…
		await fake.waitForCount(1, (o) => o.op === "draft" && !o.final);
		// …then the content STILL reaches the wire via a door op (telegram:
		// plain send | relay: the armed stream converts to a message via seal).
		await fake.waitForCount(
			1,
			(o) =>
				"content" in o &&
				o.content === "one" &&
				!(o.op === "draft" && !o.final),
		);
		expect(consumer.prefixViolations).toEqual([]);

		clock.advance(100);
		consumer.onDelta(" two");
		await fake.waitForCount(
			1,
			(o) => "content" in o && o.content === "one two",
		);
		expect(fake.draftFrames()).toHaveLength(1); // ONLY the failed attempt

		consumer.finish("one two");
		await runP;
		expect(consumer.finalContentDelivered).toBe(true);
		expect(consumer.deliveredFinalMatches("one two")).toBe(true);
	});
});

describe.each(SHAPES.filter((s) => !s.streamIsMessage))(
	"telegram-shape segments / $name",
	({ make }) => {
		it("segment break BUMPS the draft id and restarts frames fresh (finding #4)", async () => {
			const fake: AnyFake = make();
			const clock = makeClock();
			const consumer = new GatewayStreamConsumer(fake, "chat-1", {
				transport: "draft",
				editIntervalMs: 50,
				bufferThreshold: 1,
				now: clock.now,
			});
			const runP = consumer.run();

			consumer.onDelta("Alice");
			await fake.waitForCount(
				1,
				(o) => o.op === "draft" && o.content === "Alice",
			);
			consumer.onDelta(null); // tool boundary
			clock.advance(100);
			consumer.onDelta("Bob");
			await fake.waitForCount(2, (o) => o.op === "draft");

			const frames = fake.draftFrames();
			expect(frames.length).toBe(2);
			expect(frames[0]?.draftId).not.toBe(frames[1]?.draftId); // bump
			expect(frames[1]?.content).toBe("Bob"); // fresh segment, NOT "AliceBob"
			expect(consumer.deliveredSegments).toEqual(["Alice"]);
			// Prefix-stability baseline RESET across segments: "Bob" legitimately
			// does not extend "Alice".
			expect(consumer.prefixViolations).toEqual([]);

			consumer.finish("Bob");
			await runP;
		});
	},
);

describe("relay-shaped seal mechanics", () => {
	it("ONE stream per turn: segment boundary emits NOTHING and keeps the draft id (finding #4/#5)", async () => {
		const fake = new FakeStreamIsMessageAdapter();
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "draft",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
		});
		const runP = consumer.run();

		consumer.onDelta("Alice");
		await fake.waitForCount(
			1,
			(o) => o.op === "draft" && o.content === "Alice",
		);
		const firstDraftId = fake.draftFrames()[0]?.draftId;

		consumer.onDelta(null); // boundary: nothing on the wire
		clock.advance(100);
		consumer.onDelta("Bob");
		await fake.waitForCount(2, (o) => o.op === "draft");

		const frames = fake.draftFrames();
		expect(frames[1]?.draftId).toBe(firstDraftId); // NO bump
		// Consumer sends the SEGMENT-local cumulative text; the CONNECTOR's
		// suffix-delta logic appends it to the one stream across boundaries
		// (prefix mismatch → whole-segment append; _reset_segment_state comment,
		// finding #4).
		expect(frames[1]?.content).toBe("Bob");
		// The tool-boundary itself emitted NOTHING.
		expect(frames.length).toBe(2);
		expect(consumer.prefixViolations).toEqual([]);

		consumer.finish("AliceBob");
		await runP;
		const seal = await fake.waitFor(
			(o): o is DraftOp => o.op === "draft" && o.final,
		);
		expect(seal.content).toBe("AliceBob");
		expect(seal.draftId).toBe(firstDraftId);
	});

	it("turn-final send is SEAL-INTERCEPTED into the open stream with the stream's identity", async () => {
		const fake = new FakeStreamIsMessageAdapter();
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "draft",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
		});
		const runP = consumer.run();
		consumer.onDelta("draft body ");
		await fake.waitForCount(1, (o) => o.op === "draft");
		clock.advance(100);
		consumer.finish("draft body sealed");
		await runP;

		const seal = await fake.waitFor(
			(o): o is DraftOp => o.op === "draft" && o.final,
		);
		expect(seal.content).toBe("draft body sealed");
		// No duplicate plain-send of the final beside the seal.
		const dupFinalSends = fake.ops.filter(
			(o) => o.op === "send" && o.content === "draft body sealed",
		);
		expect(dupFinalSends).toHaveLength(0);
		expect(consumer.message_id).toBe(seal.messageId ?? null);
		expect(fake.chokepoint.isOpenDraft("chat-1")).toBe(false);
		expect(fake.chokepoint.isSealedDraft("chat-1")).toBe(true);
	});

	it("FAILED seal still delivers the final BYTE-EXACT via plain send (never swallowed)", async () => {
		const fake = new FakeStreamIsMessageAdapter();
		fake.failSeals = true;
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "draft",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
		});
		const runP = consumer.run();
		consumer.onDelta("abc");
		await fake.waitForCount(1, (o) => o.op === "draft");
		clock.advance(100);
		consumer.finish("THE FINAL 🎯\nkept\n");
		await runP;

		// Seal attempted and failed → NO draft(final:true) op…
		expect(fake.ops.some((o) => o.op === "draft" && o.final)).toBe(false);
		// …but the turn-final went out as an ordinary send, byte-exact.
		const send = await fake.waitFor((o): o is SendOp => o.op === "send");
		expect(send.content).toBe("THE FINAL 🎯\nkept\n");
		// Audit proves the fall-through action.
		const sealAudit = fake.audit.find(
			(a) => a.action === "seal-failed-plain-send",
		);
		expect(sealAudit).toBeDefined();
		expect(sealAudit?.door).toBe("send");
		// Tombstone set BEFORE the attempt: orphaned stream can't re-arm.
		expect(fake.chokepoint.isSealedDraft("chat-1")).toBe(true);
		expect(consumer.finalContentDelivered).toBe(true);
		expect(consumer.deliveredFinalMatches("THE FINAL 🎯\nkept\n")).toBe(true);
	});

	it("reconcile-by-edit BESIDE the sealed stream: later same-turn delivery edits, never second plain send", async () => {
		const fake = new FakeStreamIsMessageAdapter();
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "draft",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
		});
		const runP = consumer.run();
		consumer.onDelta("body");
		await fake.waitForCount(1, (o) => o.op === "draft");
		clock.advance(100);
		consumer.finish("sealed final");
		await runP;
		const seal = await fake.waitFor(
			(o): o is DraftOp => o.op === "draft" && o.final,
		);

		// Delivery-resolver lane delivers beside the sealed stream (finding #7):
		// queued follow-up through door 2 carrying the SAME (identity-less) turn.
		await fake.sendForPlatform("slack", "chat-1", "queued follow-up");
		const followUp = await fake.waitFor(
			(o): o is EditOp => o.op === "edit" && o.content === "queued follow-up",
		);
		// Edited INTO the sealed message identity — never a second plain send.
		expect(followUp.messageId).toBe(seal.messageId);
		expect(followUp.finalize).toBe(true);
		expect(
			fake.ops.some((o) => o.op === "send" && o.content === "queued follow-up"),
		).toBe(false);
		const reconcile = fake.audit.at(-1);
		expect(reconcile?.action).toBe("reconcile-edit");
	});

	it("graceful degradation: draft-frame FAILURE permanently disables drafts mid-response; reroute via edit path", async () => {
		const fake = new FakeStreamIsMessageAdapter();
		fake.failDraftFrames = true;
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "draft",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
		});
		const runP = consumer.run();
		consumer.onDelta("one");
		// Failed draft attempt, then reroute: the armed stream is converted to
		// an editable message by the door seal (relay-shape behavior).
		await fake.waitForCount(1, (o) => o.op === "draft" && !o.final);
		const converted = await fake.waitFor(
			(o): o is DraftOp => o.op === "draft" && o.final,
		);
		expect(converted.content).toBe("one");
		expect(consumer.prefixViolations).toEqual([]);

		clock.advance(100);
		consumer.onDelta(" two");
		const edit = await fake.waitFor(
			(o): o is EditOp => o.op === "edit" && o.content === "one two",
		);
		expect(edit.messageId).toBe(converted.messageId ?? null);
		expect(fake.draftFrames()).toHaveLength(1); // only the failed attempt

		consumer.finish("one two");
		await runP;
		expect(consumer.deliveredFinalMatches("one two")).toBe(true);
	});

	it("REQUIRES_EDIT_FINALIZE forces the redundant finalize edit even when content unchanged", async () => {
		const fake = new FakeStreamIsMessageAdapter();
		fake.failDraftFrames = true; // force edit-based preview path
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "draft",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
			requiresEditFinalize: true,
		});
		const runP = consumer.run();
		consumer.onDelta("same text");
		await fake.waitForCount(
			1,
			(o) =>
				"content" in o &&
				o.content === "same text" &&
				!(o.op === "draft" && !o.final),
		);
		clock.advance(100);
		consumer.finish("same text"); // identical to preview
		await runP;

		const edits = fake.ops.filter((o): o is EditOp => o.op === "edit");
		expect(edits).toHaveLength(1);
		expect(edits[0]?.finalize).toBe(true);
		expect(edits[0]?.content).toBe("same text");
	});

	it("default (no REQUIRES_EDIT_FINALIZE) keeps the fast path: unchanged final does NOT re-edit", async () => {
		const fake = new FakeStreamIsMessageAdapter();
		fake.failDraftFrames = true;
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "draft",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
		});
		const runP = consumer.run();
		consumer.onDelta("same text");
		await fake.waitForCount(
			1,
			(o) =>
				"content" in o &&
				o.content === "same text" &&
				!(o.op === "draft" && !o.final),
		);
		clock.advance(100);
		consumer.finish("same text");
		await runP;

		expect(fake.ops.some((o) => o.op === "edit")).toBe(false);
		expect(consumer.finalContentDelivered).toBe(true);
	});
});

describe("capability discovery integration (latched per chat)", () => {
	it("shared capabilities latch probes across consumers; transport 'edit' never probes", async () => {
		const fake = new FakeDraftStreamAdapter();
		const caps = new StreamingCapabilities(fake);
		const clock = makeClock();

		const c1 = new GatewayStreamConsumer(fake, "chat-9", {
			transport: "auto",
			capabilities: caps,
			now: clock.now,
		});
		c1.finish("x");
		await c1.run();
		expect(fake.supportsProbeCalls).toBe(1);

		const c2 = new GatewayStreamConsumer(fake, "chat-9", {
			transport: "auto",
			capabilities: caps,
			now: clock.now,
		});
		c2.finish("y");
		await c2.run();
		expect(fake.supportsProbeCalls).toBe(1); // LATCHED per chat type

		const c3 = new GatewayStreamConsumer(fake, "chat-9", {
			transport: "edit", // explicit legacy transport
			capabilities: caps,
			now: clock.now,
		});
		c3.finish("z");
		await c3.run();
		expect(fake.supportsProbeCalls).toBe(1); // still no extra probe
	});

	it("unsupported probe ⇒ edit-based path from turn one (auto transport)", async () => {
		const fake = new FakeDraftStreamAdapter();
		fake.supportsDraftStreaming = () => false;
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "c", {
			transport: "auto",
			now: clock.now,
		});
		const runP = consumer.run();
		consumer.onDelta("plain");
		const send = await fake.waitFor((o): o is SendOp => o.op === "send");
		expect(send.content).toBe("plain");
		expect(fake.draftFrames()).toHaveLength(0);
		consumer.finish("plain");
		await runP;
	});
});

// ── se-1: turn-final disposition gate + exact-marker retract ────────────

describe("silence-marker suppression (stream_consumer.py:_suppress_silence_marker)", () => {
	function editPathBoot(extra?: Partial<StreamConsumerConfig>) {
		const fake = new FakeDraftStreamAdapter();
		fake.supportsDraftStreaming = () => false; // force the edit-based path
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(
			fake,
			"chat-1",
			{
				transport: "auto",
				editIntervalMs: 50,
				bufferThreshold: 1,
				now: clock.now,
				...extra,
			},
			undefined,
			"user-msg-1", // initialReplyToId — the turn's threading root
		);
		return { fake, consumer, clock, runP: consumer.run() };
	}

	it("MUTATION: exact-marker authoritative final RETRACTS the streamed preview and delivers NOTHING", async () => {
		const { fake, consumer, runP } = editPathBoot();
		consumer.onDelta("analyzing the repo");
		await fake.waitFor((o): o is SendOp => o.op === "send");
		await vi.waitFor(() => expect(consumer.message_id).not.toBeNull());
		const previewId = consumer.message_id;

		consumer.finish("NO_REPLY", { agentResult: { failed: false } });
		await runP;

		// The preview message was RETRACTED via the best-effort delete path.
		const del = fake.ops.find(
			(o): o is Extract<WireOp, { op: "delete" }> => o.op === "delete",
		);
		expect(del?.messageId).toBe(previewId);
		// Nothing was delivered: flags stay FALSE, recorded final cleared.
		expect(consumer.finalResponseSent).toBe(false);
		expect(consumer.finalContentDelivered).toBe(false);
		expect(consumer.alreadySent).toBe(false);
		expect(consumer.deliveredFinalMatches("NO_REPLY")).toBe(null);
		// No marker text anywhere on the wire beyond the retracted preview.
		expect(
			fake.ops.some((o) => o.op === "send" && o.content === "NO_REPLY"),
		).toBe(false);
		// Disposition observability: gate fired on the interactive matcher.
		expect(consumer.turnDisposition?.deliver).toBe(false);
		expect(consumer.turnDisposition?.reason).toBe("intentional_silence");
	});

	it("cron-lane loose forms suppress through the same seam (per-lane gate parity)", async () => {
		const { fake, consumer, runP } = editPathBoot({ lane: "cron" });
		consumer.onDelta("tick body");
		await fake.waitFor((o): o is SendOp => o.op === "send");
		consumer.finish("[SILENT] no changes detected");
		await runP;
		expect(consumer.turnDisposition?.reason).toBe("autonomous_silence");
		expect(consumer.finalContentDelivered).toBe(false);
	});

	it("substantive finals deliver normally and record a delivering disposition", async () => {
		const { fake, consumer, runP } = editPathBoot();
		consumer.onDelta("working");
		await fake.waitFor((o): o is SendOp => o.op === "send");
		consumer.finish("the real answer", { agentResult: { failed: false } });
		await runP;
		expect(consumer.turnDisposition?.deliver).toBe(true);
		expect(consumer.turnDisposition?.matcher).toBe("none");
		expect(consumer.deliveredFinalMatches("the real answer")).toBe(true);

		// Prose merely mentioning a marker is NOT silence (run.py comment).
		const c2 = editPathBoot();
		c2.consumer.onDelta("I decided to NO_REPLY here because nothing changed");
		c2.consumer.finish("I decided to NO_REPLY here because nothing changed", {
			agentResult: { failed: false },
		});
		await c2.runP;
		expect(c2.consumer.turnDisposition?.deliver).toBe(true);
	});

	it("FAILED turns always deliver their errors even with an exact marker", async () => {
		const { fake, consumer, runP } = editPathBoot();
		consumer.onDelta("turn body");
		await fake.waitFor((o): o is SendOp => o.op === "send");
		consumer.finish("NO_REPLY", { agentResult: { failed: true } });
		await runP;
		expect(consumer.turnDisposition?.deliver).toBe(true);
		expect(consumer.turnDisposition?.reason).toBeNull();
		expect(consumer.finalContentDelivered).toBe(true);
	});
});

// ── se-2: partial-silence-marker hold-back before mid-stream flushes ─────

describe("partial-marker flush hold-back (stream_consumer.py:run)", () => {
	function boot2(extra?: Partial<StreamConsumerConfig>) {
		const fake = new FakeDraftStreamAdapter();
		fake.supportsDraftStreaming = () => false;
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "auto",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
			...extra,
		});
		return { fake, consumer, clock, runP: consumer.run() };
	}

	it("a buffer that could still resolve to a marker NEVER flashes mid-stream; got_done resolves it", async () => {
		const { fake, consumer, clock, runP } = boot2();
		consumer.onDelta("NO");
		clock.advance(1_000); // interval long since elapsed — still held back
		consumer.onDelta("_REPL");
		clock.advance(1_000); // "NO_REPL" still a marker prefix — held back
		expect(fake.ops).toHaveLength(0); // nothing reached the wire

		consumer.finish("NO_REPLY", {
			agentResult: { failed: false },
		}); // got_done resolves the buffer
		await runP;
		// Exact marker ⇒ suppressed; the raw marker never displayed at all.
		expect(
			fake.ops.some((o) => "content" in o && o.content.includes("NO")),
		).toBe(false);
		expect(consumer.finalContentDelivered).toBe(false);
	});

	it("diverged prose resumes normal streaming immediately (never lost)", async () => {
		const { fake, consumer, clock, runP } = boot2();
		consumer.onDelta("No way this is real");
		clock.advance(100);
		await fake.waitForCount(
			1,
			(o) => "content" in o && o.content === "No way this is real",
		);
		consumer.finish("No way this is real");
		await runP;
		expect(consumer.deliveredFinalMatches("No way this is real")).toBe(true);
	});
});

// ── se-4/se-5: expect_edits stamping + routing metadata on edits ────────

describe("preview metadata contracts (_metadata_for_send / _edit_message)", () => {
	it("edit-path preview sends stamp expect_edits:true; finals do NOT", async () => {
		const fake = new FakeDraftStreamAdapter();
		fake.supportsDraftStreaming = () => false;
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(
			fake,
			"chat-1",
			{
				transport: "auto",
				editIntervalMs: 50,
				bufferThreshold: 1,
				now: clock.now,
			},
			{ thread_root: "tr-1" }, // constructor metadata = routing/thread root
		);
		const runP = consumer.run();
		consumer.onDelta("first");
		await fake.waitForCount(1, (o) => "content" in o && o.content === "first");
		clock.advance(100);
		consumer.onDelta(" second");
		await fake.waitFor((o): o is EditOp => o.op === "edit");
		consumer.finish("first second FINAL"); // diverges from the last preview
		await runP;

		const previews = fake.ops.filter(
			(o): o is SendOp => o.op === "send" && o.content === "first",
		);
		expect(previews[0]?.metadata?.["expect_edits"]).toBe(true);
		// Routing metadata rides stream edits when the adapter accepts it.
		const edits = fake.ops.filter((o): o is EditOp => o.op === "edit");
		expect(edits.length).toBeGreaterThan(0);
		for (const e of edits) expect(e.metadata?.["thread_root"]).toBe("tr-1");
		// The turn-final carries notify — and NOT expect_edits.
		const finalizeEdit = edits.at(-1);
		expect(finalizeEdit?.finalize).toBe(true);
		expect(finalizeEdit?.content).toBe("first second FINAL");
		expect(finalizeEdit?.metadata?.["expect_edits"]).toBeUndefined();
	});

	it("draft frames keep their identity metadata WITHOUT expect_edits (drafts are not rich-send previews)", async () => {
		const fake = new FakeDraftStreamAdapter();
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "draft",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
		});
		const runP = consumer.run();
		consumer.onDelta("frame one");
		await fake.waitForCount(1, (o) => o.op === "draft" && !o.final);
		consumer.finish("frame one");
		await runP;
		for (const d of fake.draftFrames()) {
			expect(d.metadata?.["expect_edits"]).toBeUndefined();
		}
	});
});

// ── se-9: length-aware splitting + fallback-final continuation ──────────

describe("overflow splitting + fallback continuation (stream_consumer.py:_truncate_for_stream/_send_fallback_final)", () => {
	function boot9(limit: number, extra?: Partial<StreamConsumerConfig>) {
		const fake = new FakeDraftStreamAdapter();
		fake.supportsDraftStreaming = () => false;
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(
			fake,
			"chat-1",
			{
				transport: "auto",
				editIntervalMs: 0,
				bufferThreshold: 1,
				now: clock.now,
				messageLimit: limit,
				...extra,
			},
			undefined,
			"user-msg-1", // threading root for the sealed-piece reply chain
		);
		return { fake, consumer, clock, runP: consumer.run() };
	}

	function reconstructWire(fake: FakeDraftStreamAdapter): string {
		// Sends append bytes in order (these flows contain no retractions; the
		// finalize path only ever adds NEW sealed pieces beside the active one).
		return fake.ops.map((o) => (o.op === "send" ? o.content : "")).join("");
	}

	it("oversized buffers seal head chunks as fixed messages and keep an ACTIVE tail within the limit", async () => {
		const LIMIT = 24;
		const { fake, consumer, runP } = boot9(LIMIT);
		const payload = "A".repeat(60); // no whitespace ⇒ deterministic hard cuts
		consumer.onDelta(payload);
		await fake.waitForCount(3, (o) => o.op === "send"); // heads + active tail flushed mid-stream
		consumer.finish(payload);
		await runP;

		const sends = fake.ops.filter(
			(o): o is SendOp => o.op === "send" && !o.metadata?.["notify"],
		);
		expect(sends.length).toBe(3); // head + head + active tail — no dupes at got_done
		for (const s of sends) expect(s.content.length).toBeLessThanOrEqual(LIMIT);
		// BYTE-EXACT: every byte landed exactly once across the split delivery.
		expect(reconstructWire(fake)).toBe(payload);
		expect(consumer.turnSplitDelivery).toBe(true);
		expect(consumer.finalContentDelivered).toBe(true);
	});

	it("an oversized FINAL with a live preview seals the first head INTO the preview (finalize edit), then threads the rest", async () => {
		const LIMIT = 16;
		const { fake, consumer, runP } = boot9(LIMIT);
		const finalText = "short-head-|" + "B".repeat(40);
		consumer.onDelta("short-head-");
		await fake.waitForCount(
			1,
			(o) => "content" in o && o.content === "short-head-",
		); // live preview on screen
		consumer.finish(finalText);
		await runP;

		const edits = fake.ops.filter((o): o is EditOp => o.op === "edit");
		expect(edits.length).toBeGreaterThanOrEqual(1);
		expect(edits[0]?.finalize).toBe(true); // sealed head into the live preview
		const sealedHead = edits[0]?.content ?? "";
		expect(sealedHead.length).toBeLessThanOrEqual(LIMIT);
		// The sealed head REPLACES the preview content (same message identity).
		expect(sealedHead.startsWith("short-head-")).toBe(true);
		// Partition: sealedHead + every post-seal send === the full final text.
		// sends[0] is the pre-seal preview, superseded by the finalize edit.
		const postSealSends = fake.ops
			.filter((o): o is SendOp => o.op === "send")
			.slice(1)
			.map((s) => s.content);
		const pieces = [sealedHead, ...postSealSends];
		expect(pieces.join("")).toBe(finalText); // byte-exact, no drops/dups
		for (const piece of pieces) {
			expect(piece.length).toBeLessThanOrEqual(LIMIT);
		}
		expect(consumer.turnSplitDelivery).toBe(true);
		expect(consumer.finalContentDelivered).toBe(true);
	});

	it("MUTATION: progressive edits failing repeatedly fall back to a fresh CONTINUATION send carrying only the unseen tail", async () => {
		const { fake, consumer, clock, runP } = boot9(0, { editIntervalMs: 50 });
		fake.failEdits = true;
		consumer.onDelta("hello");
		const preview = await fake.waitFor((o): o is SendOp => o.op === "send");
		expect(preview.content).toBe("hello");

		// Three consecutive edit failures trip fallback mode (_MAX_FLOOD_STRIKES).
		for (let i = 0; i < 3; i++) {
			clock.advance(100);
			consumer.onDelta(` +${i}`);
			await fake.waitForCount(i + 1, (o) => o.op === "edit");
		}
		const FULL = "hello +0 +1 +2 tail";
		clock.advance(100);
		consumer.finish(FULL);
		await runP;

		// No further doomed edit after fallback armed…
		const edits = fake.ops.filter((o) => o.op === "edit");
		expect(edits.length).toBe(3);
		// …instead the unseen continuation went out as a FRESH send.
		const cont = await fake.waitForCount(
			1,
			(o) => "content" in o && o.content === "+0 +1 +2 tail",
		);
		expect(cont).toHaveLength(1);
		expect(consumer.finalContentDelivered).toBe(true);
		// Reconciliation records the WHOLE answer (prefix shown + continuation).
		expect(consumer.deliveredFinalMatches(FULL)).toBe(true);
	});

	it("continuation that adds nothing new still counts as delivered (visible partial already matches)", async () => {
		const { fake, consumer, clock, runP } = boot9(0, { editIntervalMs: 50 });
		fake.failEdits = true;
		consumer.onDelta("complete answer");
		await fake.waitFor((o): o is SendOp => o.op === "send");
		for (let i = 0; i < 3; i++) {
			clock.advance(100);
			consumer.onDelta(" x");
			await fake.waitForCount(i + 1, (o) => o.op === "edit");
		}
		consumer.finish("complete answer");
		await runP;
		expect(consumer.finalContentDelivered).toBe(true);
		expect(consumer.deliveredFinalMatches("complete answer")).toBe(true);
	});
});

// ── se-12: consumer-side think-block scrubber behind onDelta ──────────

describe("think-block scrubber (stream_consumer.py:_filter_and_accumulate)", () => {
	function bootThink() {
		const fake = new FakeDraftStreamAdapter();
		fake.supportsDraftStreaming = () => false;
		const clock = makeClock();
		const consumer = new GatewayStreamConsumer(fake, "chat-1", {
			transport: "auto",
			editIntervalMs: 50,
			bufferThreshold: 1,
			now: clock.now,
		});
		return { fake, consumer, clock, runP: consumer.run() };
	}

	function allWireContent(fake: FakeDraftStreamAdapter): string {
		return fake.ops.map((o) => ("content" in o ? o.content : "")).join("\n");
	}

	it("MUTATION: reasoning blocks streamed char-by-char NEVER display — not even partially", async () => {
		const { fake, consumer, runP } = bootThink();
		const payload = "<think>secret chain of thought</think>visible answer";
		const run = async (): Promise<void> => {
			for (const ch of payload) {
				consumer.onDelta(ch);
			}
		};
		await run();
		consumer.finish("visible answer");
		await runP;

		const wire = allWireContent(fake);
		expect(wire).not.toContain("think"); // mutation removing the filter leaks the tag itself
		expect(wire).not.toContain("secret"); // reasoning content never displays
		expect(wire).toContain("visible answer");
		expect(consumer.deliveredFinalMatches("visible answer")).toBe(true);
	});

	it("case-insensitive variants are scrubbed too", async () => {
		const { fake, consumer, runP } = bootThink();
		for (const ch of "<THINKING>hidden</THINKING>shown") consumer.onDelta(ch);
		consumer.finish("shown");
		await runP;
		expect(allWireContent(fake)).not.toContain("hidden");
		expect(allWireContent(fake)).toContain("shown");
	});

	it("partial-tag hold-back: a trailing '<thi…' fragment must not display", async () => {
		const { fake, consumer, clock, runP } = bootThink();
		consumer.onDelta("hello <thi");
		clock.advance(100); // flush tick while a partial open tag sits at the tail
		consumer.onDelta("nk>masked</think> world");
		consumer.finish("hello world");
		await runP;

		for (const op of fake.ops) {
			if ("content" in op) {
				expect(op.content).not.toMatch(/</); // no tag fragment ever visible
			}
		}
		expect(allWireContent(fake)).toContain("hello");
		expect(allWireContent(fake)).toContain("world");
		expect(allWireContent(fake)).not.toContain("masked");
	});

	it("orphan close tags with no matching open are stripped along with trailing whitespace", async () => {
		const { fake, consumer, runP } = bootThink();
		consumer.onDelta("text </think> more");
		consumer.finish("text more");
		await runP;
		const wire = allWireContent(fake);
		expect(wire).not.toContain("</think>");
		expect(wire).toContain("text more");
	});

	it("boundary-gated: prose MERELY mentioning a tag displays verbatim (no false positive)", async () => {
		const { fake, consumer, runP } = bootThink();
		consumer.onDelta("the <think> tag is used for notes");
		consumer.finish("the <think> tag is used for notes");
		await runP;
		expect(allWireContent(fake)).toContain("the <think> tag is used for notes");
	});
});

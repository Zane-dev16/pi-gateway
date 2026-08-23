// GatewayStreamConsumer BEHAVIOR CONTRACTS (04-platform-adapters.md §5, §8
// streaming rows; DEC-006). Mutation-tested: the prefix-stability suite fails
// if detection is removed — every observable detection effect is asserted
// separately (violation recorded, mutated frame never on the wire, draft lane
// permanently disabled, traffic rerouted by edit).
//
// The SAME core suite runs green against BOTH fake adapter shapes (exit
// criteria): Telegram-shaped draft-stream AND relay-shaped stream-is-message.
// Injected clock throughout; event-based waits, no sleeps.

import { describe, expect, it } from "vitest";
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
			(o) => o.content === "hello world" && !(o.op === "draft" && !o.final),
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
			(o) => o.content === "one" && !(o.op === "draft" && !o.final),
		);
		expect(consumer.prefixViolations).toEqual([]);

		clock.advance(100);
		consumer.onDelta(" two");
		await fake.waitForCount(1, (o) => o.content === "one two");
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
			(o) => o.content === "same text" && !(o.op === "draft" && !o.final),
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
			(o) => o.content === "same text" && !(o.op === "draft" && !o.final),
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

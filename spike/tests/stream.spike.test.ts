// Spike contract tests — stream consumer proof (04-platform-adapters.md §5, DEC-006).
//
// BEHAVIOR CONTRACTS ONLY: mutation detection, seal-interception at both egress
// doors, exactly-once final absorption, reconcile-by-edit beside sealed streams,
// and failed-seal degradation. Every wait is event-based (FakeRelayAdapter
// waitFor/waitForCount) with loose >=2s bounds; the one timing-sensitive test
// uses an injected clock. No DB/home/state dirs are involved in this spike
// (pure in-memory seams), so there is nothing to isolate in temp paths.
import { describe, expect, it } from "vitest";
import {
	FakeRelayAdapter,
	type DraftOp,
	type EditOp,
	type SendOp,
	type WireOp,
} from "../stream/fake-relay-adapter.js";
import {
	GatewayStreamConsumer,
	type StreamConsumerConfig,
} from "../stream/gateway-stream-consumer.js";

// ── helpers ───────────────────────────────────────────────────────────────

/** Mid-stream cumulative draft frame (not the sealing frame). */
const isFrame = (op: WireOp): op is DraftOp =>
	op.op === "draft" && op.final === false;
/** Sealing draft frame (door-emitted draft final:true). */
const isSeal = (op: WireOp): op is DraftOp =>
	op.op === "draft" && op.final === true;
const isSend = (op: WireOp): op is SendOp => op.op === "send";
const isEdit = (op: WireOp): op is EditOp => op.op === "edit";

function makeTurn(
	adapter: FakeRelayAdapter,
	chatId: string,
	replyTo: string,
	cfg: StreamConsumerConfig = {},
): GatewayStreamConsumer {
	return new GatewayStreamConsumer(
		adapter,
		chatId,
		{ transport: "auto", ...cfg },
		{ reply_to_message_id: replyTo },
	);
}

/** Byte-exact round-trip assertion (UTF-8 identity, not merely string identity). */
function expectByteEqual(actual: string, expected: string): void {
	expect(actual).toBe(expected);
	expect(
		Buffer.compare(Buffer.from(actual, "utf8"), Buffer.from(expected, "utf8")),
	).toBe(0);
}

// ── contract tests ────────────────────────────────────────────────────────

describe("stream consumer spike (04 §5 four invariants + DEC-006)", () => {
	it("happy path: prefix-stable drafts accumulate under ONE stream id per turn; authoritative final rides the seal byte-exactly", async () => {
		const a = new FakeRelayAdapter();
		const c = makeTurn(a, "C1", "T1");
		const running = c.run();

		c.onDelta("Hello ");
		await a.waitForCount(1, isFrame);
		c.onDelta("world 🚀");
		await a.waitForCount(2, isFrame);

		// Segment break (tool boundary): stream-is-message adapters keep ONE
		// stream — no draft_id bump (finding #4 "Alice"), boundary emits nothing.
		c.onDelta(null);
		c.onDelta(" Part two");
		await a.waitForCount(3, isFrame);

		// Consumer-declared authoritative final (post-stream augmentation the
		// accumulator never saw rides INSIDE it — invariant 2 / finding #11).
		const authoritative = "Hello world 🚀 Part two — ✅ verifier footer";
		c.finish(authoritative);
		await running;

		const frames = a.ops.filter(isFrame);
		expect(frames.map((f) => f.content)).toEqual([
			"Hello ",
			"Hello world 🚀",
			" Part two",
		]);
		// Invariant 1: each frame is a string prefix of the next within its
		// segment (append-only deltas; fresh baseline after the break).
		expect("Hello world 🚀".startsWith("Hello ")).toBe(true);
		// One cumulative native stream per turn for stream-is-message adapters.
		expect(new Set(frames.map((f) => f.draftId)).size).toBe(1);

		// Exactly one sealing frame carries the complete final — no corrective
		// send, no duplicate.
		const seals = a.ops.filter(isSeal);
		expect(seals).toHaveLength(1);
		expectByteEqual(seals[0]?.content ?? "", authoritative);
		expect(a.ops.filter(isSend)).toHaveLength(0);
		expect(a.isOpenDraft("C1", { reply_to_message_id: "T1" })).toBe(false);
		expect(a.isSealedDraft("C1", { reply_to_message_id: "T1" })).toBe(true);

		// Runner-read properties after drain (spec §5.2 sketch).
		expect(c.alreadySent).toBe(true);
		expect(c.finalResponseSent).toBe(true);
		expect(c.finalContentDelivered).toBe(true);
		expect(c.message_id).toBe(seals[0]?.messageId ?? null);
		expect(c.deliveredFinalMatches(authoritative)).toBe(true);
		expect(c.deliveredFinalMatches("something else")).toBe(false);
	});

	it("MUTATION: a draft frame mutating emitted prefix content is DETECTED — violation recorded, draft lane permanently disabled, final still delivered", async () => {
		// composeFrame models the banned upstream transforms (fence-closing,
		// cursor suffix — invariant 1: "NEVER mutate draft frames per-tick").
		// Flipping it mid-stream makes the composed frame rewrite history.
		let mutate = false;
		const a = new FakeRelayAdapter();
		const c = makeTurn(a, "C2", "T2", {
			composeFrame: (acc) =>
				mutate ? `MUTATED ${acc.replace("world", "w0rld")}` : acc,
		});
		const running = c.run();

		c.onDelta("Hello ");
		await a.waitForCount(1, isFrame);
		c.onDelta("world");
		await a.waitForCount(2, isFrame);

		mutate = true; // inject the non-prefix-stable frame
		c.onDelta(" more");

		// Observable detection effects, each of which FAILS if the
		// prefix-stability guard is removed:
		//  (a) the violation is recorded with prev/next evidence;
		const seal = await a.waitFor<DraftOp>(isSeal); // degraded preview send → door seal-intercepts
		expect(c.prefixViolations).toEqual([
			{
				kind: "non_prefix_frame",
				prevFrame: "Hello world",
				nextFrame: "MUTATED Hello w0rld more",
			},
		]);
		//  (b) the violating frame NEVER went out over the draft lane — the
		//      draft lane is permanently disabled (graceful degradation, §5)
		//      and traffic rerouted through the edit-based path, whose first
		//      unmarked send the DOOR converted into the seal.
		expect(a.ops.filter(isFrame)).toHaveLength(2); // frozen at the last good frame
		// The violating frame is REFUSED on every lane — detection prevents its
		// emission, so the degraded door-sealed preview carries only clean
		// buffer truth; the authoritative final below repairs the message.
		expect(seal?.content).toBe("Hello world more");

		// The authoritative final repairs everything byte-exactly.
		c.finish("Clean authoritative final");
		await running;

		expect(a.ops.filter(isFrame)).toHaveLength(2); // still no further draft frames
		const edits = a.ops.filter(isEdit);
		expect(edits).toHaveLength(1);
		expect(edits[0]?.messageId).toBe(seal?.messageId);
		expectByteEqual(edits[0]?.content ?? "", "Clean authoritative final");
		expect(c.finalContentDelivered).toBe(true);
	});

	it("interim send beside a SEALED stream reconciles BY EDIT against the interim message id — never a plain second send", async () => {
		const a = new FakeRelayAdapter();
		const md = { reply_to_message_id: "T9" };

		// Seal a stream (arm + turn-final absorbed by the door).
		await a.sendDraft({
			chatId: "C9",
			draftId: 5,
			content: "partial answer…",
			metadata: md,
		});
		const sealed = await a.send("C9", "the sealed answer", undefined, md);
		expect(sealed.success).toBe(true);
		expect(a.isOpenDraft("C9", md)).toBe(false);

		// Interim send #1 beside the sealed stream: its own lane has no
		// editable message yet, so plain send() fires ONCE and records the
		// interim message id ("plain send only when no editable message
		// exists" — invariant 4).
		await a.send("C9", "tail flush one", undefined, {
			...md,
			_interim_send: true,
		});
		const sends = a.ops.filter(isSend);
		expect(sends).toHaveLength(1);

		// Interim send #2 beside the sealed stream: reconciled BY EDIT against
		// the interim message id — a second bubble would duplicate the lane.
		await a.send("C9", "tail flush two", undefined, {
			...md,
			_interim_send: true,
		});
		const edits = a.ops.filter(isEdit);
		expect(edits).toHaveLength(1);
		expect(edits[0]?.messageId).toMatch(/^msg_/);
		expect(edits[0]?.messageId).not.toBe(sealed.messageId); // own lane, not the final
		expect(edits[0]?.content).toBe("tail flush two");
		expect(a.ops.filter(isSend)).toHaveLength(1); // still exactly one send

		// Queued-lane FINAL parity (run.py:_deliver_queued_first_response):
		// an unmarked follow-up beside the sealed stream edits the sealed
		// message id in place instead of plain-sending a duplicate.
		await a.send("C9", "queued follow-up final body", undefined, md);
		const queuedEdits = a.ops.filter(isEdit);
		expect(queuedEdits).toHaveLength(2);
		expect(queuedEdits[1]?.messageId).toBe(sealed.messageId);
		expect(a.ops.filter(isSend)).toHaveLength(1); // never a plain second send

		// Audit trail: the reconcile went through the audited chokepoint.
		expect(a.chokepointAudit.at(-1)?.action).toBe("reconcile-edit");
	});

	it("_interim_send is popped at BOTH egress doors by the single audited chokepoint; interim sends never seal the open stream", async () => {
		const a = new FakeRelayAdapter();
		const c = makeTurn(a, "C4", "T4");
		const running = c.run();
		c.onDelta("answer body");
		await a.waitForCount(1, isFrame); // draft lane armed the open stream

		// Door 1 — send(): consumer commentary declares interim intent.
		expect(await c.sendCommentary("Using the browser tool…")).toBe(true);
		// Door must NOT have sealed the live stream with interim text
		// (invariant 3: sealing orphans the true final into a duplicate).
		expect(a.isOpenDraft("C4", { reply_to_message_id: "T4" })).toBe(true);

		// Door 2 — send_for_platform(): the delivery-resolver lane bypasses
		// send() (finding #7); the interim contract must hold here too.
		await a.sendForPlatform("slack", "C4", "scheduled status blip", undefined, {
			reply_to_message_id: "T4",
			_interim_send: true,
		});
		expect(a.isOpenDraft("C4", { reply_to_message_id: "T4" })).toBe(true);

		// The true turn-final (unmarked) DOES seal — exactly one seal.
		c.finish("the real final answer");
		await running;

		const seals = a.ops.filter(isSeal);
		expect(seals).toHaveLength(1);
		expectByteEqual(seals[0]?.content ?? "", "the real final answer");
		expect(a.isOpenDraft("C4", { reply_to_message_id: "T4" })).toBe(false);

		// The marker never leaks onto the wire — from EITHER door.
		for (const op of a.ops) {
			expect(JSON.stringify(op.metadata ?? {})).not.toContain("_interim_send");
		}

		// Single-audited-chokepoint coverage (DEC-006): EVERY door admission —
		// commentary, resolver send, turn-final — produced exactly one audit
		// entry, spanning BOTH doors. Mid-stream frames bypass doors entirely
		// (draft lane), so admissions == 3.
		expect(a.chokepointAudit.map((e) => e.door)).toEqual([
			"send",
			"send_for_platform",
			"send",
		]);
		expect(a.chokepointAudit.map((e) => e.interim)).toEqual([
			true,
			true,
			false,
		]);
		expect(a.chokepointAudit.map((e) => e.action)).toEqual([
			"plain-send", // door 1: first interim — no editable interim id yet
			"reconcile-edit", // door 2: interim lane deduped BY EDIT
			"seal", // door 1: turn-final absorbed into the stream
		]);
		// Door 2's interim was reconciled against the interim lane's OWN id —
		// never a plain second send, never the final's message.
		const edits = a.ops.filter(isEdit);
		expect(edits).toHaveLength(1);
		expect(edits[0]?.content).toBe("scheduled status blip");
		expect(edits[0]?.platform).toBe("slack");
		expect(edits[0]?.messageId).not.toBe(seals[0]?.messageId);
		expect(a.ops.filter(isSend)).toHaveLength(1); // only the commentary send
	});

	it("finish(final_text) is absorbed EXACTLY once — double finish and a raced late draft frame cannot duplicate or override it", async () => {
		const a = new FakeRelayAdapter();
		const c = makeTurn(a, "C5", "T5");
		const running = c.run();
		c.onDelta("streamed body.");
		await a.waitForCount(1, isFrame);

		const authoritative = "AUTHORITATIVE 🚀 final";
		// Race window: authoritative finish + duplicate finish + late straggler
		// delta land back-to-back. (JS is single-threaded, so the race is
		// modeled at the queue-semantics level: whichever enqueue order wins,
		// the payload must be absorbed exactly once.)
		c.finish(authoritative);
		c.finish("DUPLICATED final must be ignored");
		c.onDelta(" LATE straggler frame");
		await running;

		const seals = a.ops.filter(isSeal);
		expect(seals).toHaveLength(1);
		// Adoption REPLACES the accumulator — the payload is the complete
		// response, never concatenated with the streamed body or the straggler.
		expectByteEqual(seals[0]?.content ?? "", authoritative);
		const wireDump = JSON.stringify(a.ops);
		expect(wireDump).not.toContain("DUPLICATED");
		expect(wireDump).not.toContain("LATE straggler");
		expect(c.deliveredFinalMatches(authoritative)).toBe(true);
		expect(c.deliveredFinalMatches("DUPLICATED final must be ignored")).toBe(
			false,
		);
		expect(c.finalContentDelivered).toBe(true);
	});

	it("failed seal degrades to plain delivery — the turn-final is NEVER swallowed", async () => {
		const a = new FakeRelayAdapter();
		a.failSeals = true; // connector rejects / ack lost after retry
		const c = makeTurn(a, "C6", "T6");
		const running = c.run();
		c.onDelta("pay");
		await a.waitForCount(1, isFrame); // arms the open stream

		c.finish("THE FINAL 💫");
		await running;

		// No sealing frame made it out, but the final arrived as a plain send…
		expect(a.ops.filter(isSeal)).toHaveLength(0);
		const sends = a.ops.filter(isSend);
		expect(sends).toHaveLength(1);
		expectByteEqual(sends[0]?.content ?? "", "THE FINAL 💫");
		// …with the final-send metadata shape (_metadata_for_send parity)…
		expect(sends[0]?.metadata?.["notify"]).toBe(true);
		expect(sends[0]?.metadata?.["reply_to_message_id"]).toBe("T6");
		// …the audit names the degrade path, and the tombstone was written
		// before the attempt (straggler frames can't re-arm the dead stream).
		expect(
			a.chokepointAudit.find((e) => e.action === "seal-failed-plain-send"),
		).toBeDefined();
		expect(a.isSealedDraft("C6", { reply_to_message_id: "T6" })).toBe(true);
		expect(c.finalContentDelivered).toBe(true);

		// Door-level unit: even a forced seal failure reports SUCCESS to the
		// caller — the plain send below it delivered the payload (PR 85796 pt 1).
		const b = new FakeRelayAdapter();
		b.failSeals = true;
		await b.sendDraft({ chatId: "D1", draftId: 7, content: "x" });
		const direct = await b.send("D1", "never swallowed");
		expect(direct.success).toBe(true);
	});

	it("bare finish() keeps legacy behavior; a no-stream turn delivers nothing via the consumer", async () => {
		// Legacy: bare finish() finalizes the accumulator verbatim.
		const a = new FakeRelayAdapter();
		const c = makeTurn(a, "C7", "T7");
		const running = c.run();
		c.onDelta("plain answer");
		await a.waitForCount(1, isFrame);
		c.finish();
		await running;
		const seals = a.ops.filter(isSeal);
		expect(seals).toHaveLength(1);
		expectByteEqual(seals[0]?.content ?? "", "plain answer");
		expect(c.deliveredFinalMatches("plain answer")).toBe(true);

		// No-stream turn (test_stream_final_contract.py:TestFinalAdoptionGuards
		// parity): finish(payload) must NOT move delivery ownership into the
		// consumer — zero platform traffic.
		const b = new FakeRelayAdapter();
		const idle = makeTurn(b, "C8", "T8");
		const idleRun = idle.run();
		idle.finish("the final answer from a non-streaming turn");
		await idleRun;
		expect(b.ops).toHaveLength(0);
		expect(idle.finalResponseSent).toBe(false);
		expect(idle.deliveredFinalMatches("anything")).toBe(null);
	});

	it("mid-stream flush throttling honors editIntervalMs on an INJECTED clock (no wall-clock waits)", async () => {
		let t = 100_000;
		const a = new FakeRelayAdapter();
		const c = makeTurn(a, "C10", "T10", {
			editIntervalMs: 50_000,
			bufferThreshold: 1,
			now: () => t,
		});
		const running = c.run();

		c.onDelta("one");
		await a.waitForCount(1, isFrame);
		c.onDelta("two"); // within the throttle window — must NOT flush
		t += 60_000; // advance the injected clock past the window
		c.onDelta(" two");
		await a.waitForCount(2, isFrame);

		const frames = a.ops.filter(isFrame);
		expect(frames).toHaveLength(2); // 3 deltas, one suppressed by the window
		expect(frames[1]?.content).toBe("onetwo two");
		c.finish();
		await running;
	});
});

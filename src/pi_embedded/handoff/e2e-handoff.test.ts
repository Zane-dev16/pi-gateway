// E2E behavior contracts for DEC-008 (the Phase-5 exit row):
//
//   "handoff claim/re-bind/replay E2E" — a queued handoff replays a FULL
//   multi-turn CLI transcript onto the destination platform through the
//   NORMAL two-guard pipeline. The rig composes the REAL L1 guard, the REAL
//   GatewayAgentRunner over the scripted faux provider, and a fake
//   destination transport; nothing intercepts either guard or the host loop.
//
// Proven here:
//   1. Identity re-bind drives replay: the runner's model request contains
//      every prior CLI turn IN ORDER plus the synthetic handoff notice.
//   2. Reply egress lands on the destination transport.
//   3. Busy destination: the forged event takes the NORMAL busy ladder —
//      merged into the pending slot, never a rival turn; completes after.
//   4. Turn failure inside the pipeline ⇒ failed row with the error payload.

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import type { Context } from "../../pi_agent_core/host.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import { HANDOFF_POLL_INTERVAL_MS, HANDOFF_STARTUP_DELAY_MS } from "./index.js";
import {
	createHandoffHarness,
	fauxAssistantMessage,
	type HandoffHarness,
} from "./testing/harness.js";

let h: HandoffHarness;

beforeEach(async () => {
	h = await createHandoffHarness();
});

afterEach(async () => {
	await h.close();
});

describe("DEC-008 E2E — claim → re-bind → transcript REPLAY", () => {
	it("a queued handoff replays a FULL multi-turn CLI transcript onto the destination through the NORMAL pipeline", async () => {
		// ---- The CLI session exists with three real turns of history. --------
		const cliTurns: Array<[string, string]> = [
			["scan 10.0.0.0/24 for open ssh", "Found 3 hosts: .12 .30 .99"],
			["fingerprint the .12 one", ".12 runs OpenSSH 9.6 on Ubuntu 22.04"],
			["draft a remediation ticket", "Drafted TK-441 with the findings"],
		];
		h.seedCliSession("cli-multi", cliTurns);

		// Capture the WIRE context of the model call (byte-level observation,
		// no interception): the seeded history must all be present.
		let wireUserTexts: string[] = [];
		h.faux.setResponses([
			(context: Context) => {
				wireUserTexts = context.messages
					.filter((m) => m.role === "user")
					.map((m) =>
						typeof m.content === "string"
							? m.content
							: (m.content as Array<{ type: string; text?: string }>)
									.filter((b) => b.type === "text")
									.map((b) => b.text ?? "")
									.join("\n"),
					);
				return fauxAssistantMessage(
					"Picking up from the CLI — we scanned /24, fingerprinted .12 (OpenSSH 9.6), and drafted TK-441. Ready to continue here.",
				);
			},
		]);

		// ---- The CLI writes its pending row. ---------------------------------
		await h.queue.requestHandoff("cli-multi", "telegram");
		expect(h.queue.getHandoffState("cli-multi")?.state).toBe("pending");

		// ---- One watcher tick performs the whole protocol. -------------------
		const report = await h.watcher.tick();
		expect(report).toMatchObject({
			pending: 1,
			claimed: 1,
			completed: 1,
			failed: 0,
		});

		// Terminal state recorded; the poll-blocked CLI sees completed.
		expect(h.queue.getHandoffState("cli-multi")).toEqual({
			state: "completed",
			platform: "telegram",
			error: null,
		});

		// Re-bind: destination key now resolves to the CLI session id.
		const expectedKey = "agent:main:telegram:dm:100:topic-1";
		expect(h.binder.entryOf(expectedKey)?.session_id).toBe("cli-multi");

		// The synthetic event traversed BOTH guards INTO the runner: exactly
		// one turn ran, keyed on the re-bound session.
		expect(h.turns).toHaveLength(1);
		expect(h.turns[0]?.sessionKey).toBe(expectedKey);
		expect(h.turns[0]?.resolvedSessionId).toBe("cli-multi");

		// TRANSCRIPT REPLAY PROOF: every prior CLI turn reached the wire, in
		// order, followed by the synthetic handoff notice as the last user
		// message.
		const notice = h.turns[0]?.text ?? "";
		expect(wireUserTexts.slice(0, 3)).toEqual([
			"scan 10.0.0.0/24 for open ssh",
			"fingerprint the .12 one",
			"draft a remediation ticket",
		]);
		expect(wireUserTexts.at(-1)).toBe(notice);
		expect(notice).toContain("[Session was just handed off from CLI");
		expect(notice).toContain('("cli-mult")'); // title fallback: id[:8]

		// Reply egress landed on the destination transport.
		expect(h.replies).toEqual([
			"Picking up from the CLI — we scanned /24, fingerprinted .12 (OpenSSH 9.6), and drafted TK-441. Ready to continue here.",
		]);
		expect(h.transport.sends[0]).toMatchObject({ chatId: "100" });
	});

	it("watcher start()/loop processes a late-arriving row end-to-end (virtual time)", async () => {
		h.watcher.start();
		await h.clock.advance(HANDOFF_STARTUP_DELAY_MS - 1); // still connecting…
		expect(h.turns).toHaveLength(0);

		// Row arrives AFTER the watcher is up (CLI wrote it just now).
		h.seedCliSession("cli-late", [["hi", "hello"]]);
		await h.queue.requestHandoff("cli-late", "telegram");

		await h.clock.advance(
			HANDOFF_STARTUP_DELAY_MS + HANDOFF_POLL_INTERVAL_MS * 2,
		);
		expect(h.turns).toHaveLength(1);
		expect(h.queue.getHandoffState("cli-late")?.state).toBe("completed");
		await h.watcher.stop();
	});
});

describe("DEC-008 E2E — busy destination takes the NORMAL ladder", () => {
	it("synthetic turn mid-conversation merges into pending; drains after; never a rival turn", async () => {
		await h.close(); // swap the shared rig for a NO-THREAD destination
		h = await createHandoffHarness({ transport: { createThreads: false } });

		// A live conversation is bound to the destination key and MID-TURN.
		const key = KEY_DEST();
		await h.binder.ensureEntry(key, { platform: "telegram" });
		await binderBindToLive(h, key, "dest-old");
		h.seedCliSession("dest-old", []);

		// TWO turns will run: the parked head, then the drained synthetic turn.
		h.faux.setResponses([
			fauxAssistantMessage("head answer"),
			fauxAssistantMessage("confirming handoff"),
		]);

		h.holdTurns(true);
		await h.guard.handleMessage(userEvent(key, "user asks something"), key);
		const headFrameStarted = h.tracker.active > 0;
		expect(headFrameStarted).toBe(true); // head parked mid-turn

		// The handoff arrives while busy.
		h.seedCliSession("cli-busy", [["earlier", "answer"]]);
		await h.queue.requestHandoff("cli-busy", "telegram");

		let tickDone = false;
		const ticking = h.watcher.tick().then((r) => {
			tickDone = true;
			return r;
		});
		// Deterministic wait: the tick chain runs until the forged wake sits
		// in the pending slot.
		for (let i = 0; i < 200 && h.guard.pendingOf(key) === undefined; i++) {
			await new Promise<void>((r) => setTimeout(r, 1));
		}
		expect(tickDone).toBe(false); // dispatch awaits frame settlement

		// The forged wake took the BUSY path: merged into the single pending
		// slot — NOT a second concurrent turn.
		expect(h.guard.pendingOf(key)?.internal).toBe(true);
		expect(h.turns).toHaveLength(1); // only the parked head so far

		// Release the head; the drain boundary serves the synthetic turn.
		h.holdTurns(false);
		const report = await ticking;
		expect(report.completed).toBe(1);
		// §11 serialization invariant: handler sections NEVER overlapped —
		// the drain boundary handed off ownership instead of racing.
		expect(h.tracker.maxHandlerConcurrency).toBe(1);

		// Both turns ran, head first, synthetic second — on the RE-BOUND id.
		expect(h.turns.map((t) => t.text)).toEqual([
			"user asks something",
			expect.stringContaining("[Session was just handed off from CLI"),
		]);
		expect(h.turns[1]?.resolvedSessionId).toBe("cli-busy");
		expect(queueState(h)).toBe("completed");
	});
});

describe("DEC-008 E2E — failure path records the error payload", () => {
	it("model-provider failure inside the pipeline ⇒ failed(+error), CLI-visible", async () => {
		h.seedCliSession("cli-fail", [["q", "a"]]);
		await h.queue.requestHandoff("cli-fail", "telegram");

		// Script the provider to fail the request: the host loop surfaces an
		// error exit; the handler throws; the dispatcher rejects; the watcher
		// records failed with the message.
		h.faux.setResponses([
			() => {
				throw new Error("provider socket reset");
			},
		]);

		const report = await h.watcher.tick();
		expect(report.failed).toBe(1);
		expect(report.failures[0]?.sessionId).toBe("cli-fail");
		const snapshot = h.queue.getHandoffState("cli-fail");
		expect(snapshot?.state).toBe("failed");
		expect(snapshot?.error ?? "").toContain("provider socket reset");

		// Retry is legal after failure (request CAS parity).
		expect(await h.queue.requestHandoff("cli-fail", "telegram")).toBe(true);
	});
});

// -- helpers -----------------------------------------------------------------

function KEY_DEST(): string {
	// No-thread telegram home ⇒ plain DM key (matches harness home chatId 100).
	return "agent:main:telegram:dm:100";
}

function userEvent(sessionKey: string, text: string): IncomingEvent {
	return {
		messageType: "text",
		text,
		source: {
			platform: "telegram",
			chatType: "dm",
			userId: "u1",
			chatId: "100",
		},
		metadata: { gateway_session_key: sessionKey },
	};
}

async function binderBindToLive(
	harness: HandoffHarness,
	key: string,
	sessionId: string,
): Promise<void> {
	await harness.binder.switchSession(key, sessionId);
}

function queueState(harness: HandoffHarness): string | null {
	return harness.queue.getHandoffState("cli-busy")?.state ?? null;
}

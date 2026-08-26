// pi_platforms/photon/photon-send-effect.test.ts — native iMessage effect
// lane behavior contract (adjudication cn-12, Hermes anchor
// plugins/platforms/photon/adapter.py:send_effect):
//
//   - POST /send-effect {spaceId, text, effect} with BOTH fields stripped
//     and validated non-empty BEFORE any sidecar call.
//   - Empty/whitespace input fails WITHOUT touching the wire.
//   - Success carries the sidecar's messageId; failures surface the
//     structured SidecarHttpError classification.

import { describe, expect, it } from "vitest";
import { makePhotonWorld, type PhotonWorld } from "./photon-world.js";

async function liveWorld(name: string): Promise<PhotonWorld> {
	const world = makePhotonWorld({ name });
	await world.connectAndAwaitLive();
	return world;
}

describe("photon /send-effect lane (adapter.py:send_effect parity)", () => {
	it("posts {spaceId, text, effect} with stripped values and returns messageId", async () => {
		const world = await liveWorld("ph-effect-body");

		const result = await world.engine.sidecarSendEffect(
			"+15551234567",
			"  happy birthday  ",
			" confetti ",
		);

		expect(result.success).toBe(true);
		expect(result.messageId).toMatch(/^spc-msg-\d+$/);
		const calls = world.sidecar.callsOf("/send-effect");
		expect(calls).toHaveLength(1);
		expect(calls[0]!.body).toEqual({
			spaceId: "+15551234567",
			text: "happy birthday",
			effect: "confetti",
		});
	});

	it("rejects empty or whitespace-only input WITHOUT any sidecar call", async () => {
		const world = await liveWorld("ph-effect-empty");

		const emptyText = await world.engine.sidecarSendEffect(
			"+15551234567",
			"   ",
			"confetti",
		);
		const emptyEffect = await world.engine.sidecarSendEffect(
			"+15551234567",
			"hello",
			"",
		);

		expect(emptyText.success).toBe(false);
		expect(emptyEffect.success).toBe(false);
		expect(emptyText.error).toBe("text and effect are required");
		expect(emptyEffect.error).toBe("text and effect are required");
		// Validation happens BEFORE the call — the wire stays silent.
		expect(world.sidecar.callsOf("/send-effect")).toHaveLength(0);
	});

	it("surfaces structured sidecar failures (class + retryable marker)", async () => {
		const world = await liveWorld("ph-effect-error");
		world.sidecar.script("/send-effect", {
			kind: "error",
			status: 409,
			error: "space unavailable",
			errorClass: "target_not_allowed",
			retryable: false,
		});

		const result = await world.engine.sidecarSendEffect(
			"+15551234567",
			"hi",
			"slam",
		);

		expect(result.success).toBe(false);
		expect(result.retryable).toBe(false);
		// target_not_allowed carries the canonical user-facing explanation.
		expect(result.error).toContain("/send-effect returned 409");
		expect(result.error).toContain("retryable=false");
	});
});

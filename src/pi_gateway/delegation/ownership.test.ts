// §7.2 ownership decision-table groundwork: the shared user-boundary set and
// the verdict mapping are DATA here, and every store-testable matrix row
// (drop classification recorded correctly, retry churn bounded by the attempt
// cap, deliver acks honestly) is exercised against the real rail. The DB-
// feeding lookups themselves are Phase-5 watcher wiring.

import { rmSync } from "node:fs";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	MAX_DELIVERY_ATTEMPTS,
	classifyCompletionTarget,
	dispositionFor,
	isUserBoundaryEnd,
	USER_BOUNDARY_END_REASONS,
} from "./index.js";
import { openRailHarness, type RailHarness } from "./testing/harness.js";

let h: RailHarness;

beforeEach(async () => {
	h = await openRailHarness();
});

afterEach(async () => {
	await h.close();
	rmSync(h.dir, { recursive: true, force: true });
});

describe("shared boundary set (pre-flight/resolver agreement data)", () => {
	it("matches gateway/run.py:_USER_BOUNDARY_END_REASONS exactly", () => {
		expect([...USER_BOUNDARY_END_REASONS]).toEqual([
			"session_reset",
			"user_exit",
			"session_switch",
			"new_session",
		]);
	});

	it("idle/timeout/lifecycle ends are NOT user boundaries (retarget norm)", () => {
		for (const reason of ["idle", "timeout", "lifecycle_end", "compression"]) {
			expect(isUserBoundaryEnd(reason)).toBe(false);
		}
		expect(isUserBoundaryEnd(null)).toBe(false);
		expect(isUserBoundaryEnd(undefined)).toBe(false);
	});
});

describe("classifyCompletionTarget — §7.2 decision table", () => {
	it("lookup error ⇒ retry (release claim; attempt cap bounds churn)", () => {
		const parent = { endedAt: null, endReason: null };
		expect(classifyCompletionTarget(parent, undefined, true)).toBe("retry");
		expect(classifyCompletionTarget(null, undefined, true)).toBe("retry");
	});

	it("unknown parent row ⇒ terminal (DROP fail-closed; result stays queryable)", () => {
		expect(classifyCompletionTarget(null)).toBe("terminal");
		expect(classifyCompletionTarget(undefined)).toBe("terminal");
	});

	it("live parent ⇒ deliver (pinned session)", () => {
		expect(classifyCompletionTarget({ endedAt: null, endReason: null })).toBe(
			"deliver",
		);
	});

	it("EVERY user-boundary end ⇒ terminal DROP (#55578: never resurrect a closed conversation)", () => {
		for (const reason of USER_BOUNDARY_END_REASONS) {
			expect(
				classifyCompletionTarget({ endedAt: 123, endReason: reason }),
			).toBe("terminal");
		}
	});

	it("non-compression non-boundary ends ⇒ deliver (retarget to chat's CURRENT session)", () => {
		for (const reason of ["idle", "timeout", "lifecycle_end"]) {
			expect(
				classifyCompletionTarget({ endedAt: 123, endReason: reason }),
			).toBe("deliver");
		}
	});

	it("compression lineage: route-owns-lineage — only a VERIFIED LIVE tip delivers", () => {
		const parent = {
			endedAt: 123,
			endReason: "compression",
			parentSessionId: "parent-row",
		};
		expect(classifyCompletionTarget(parent, undefined)).toBe("retry"); // no tip yet
		expect(classifyCompletionTarget(parent, { tipSessionId: null })).toBe(
			"retry",
		); // mid-rotation
		expect(
			classifyCompletionTarget(parent, { tipSessionId: "parent-row" }),
		).toBe("retry"); // self-referential tip ⇒ continuation not visible yet
		const unreadableTip = { tipSessionId: "tip-1" };
		expect(classifyCompletionTarget(parent, unreadableTip)).toBe("retry"); // tip row unreadable (endedAt undefined)
		expect(
			classifyCompletionTarget(parent, { tipSessionId: "tip-1", endedAt: 999 }),
		).toBe("retry"); // tip already ended
		expect(
			classifyCompletionTarget(parent, {
				tipSessionId: "tip-1",
				endedAt: null,
			}),
		).toBe("deliver"); // live continuation
	});

	it("dispositionFor maps verdicts onto the store handshake", () => {
		expect(dispositionFor("terminal")).toBe("drop");
		expect(dispositionFor("retry")).toBe("release");
		expect(dispositionFor("deliver")).toBe("complete-after-inject");
	});
});

describe("store-testable matrix rows (drop classifications recorded correctly)", () => {
	async function seedPending(id: string): Promise<void> {
		await h.rail.recordDispatch({
			delegationId: id,
			originSession: "telegram|chat|5",
			parentSessionId: "parent-x",
			task: { goal: "g" },
		});
		await h.rail.publishCompletion({
			delegationId: id,
			event: { delegation_id: id },
			result: { answer: "payload" },
		});
	}

	it("user-boundary parent ⇒ drop: row lands 'dropped' (NOT delivered), payload queryable, never replayed", async () => {
		await seedPending("dlg-boundary-drop");
		const verdict = classifyCompletionTarget({
			endedAt: 1,
			endReason: "session_reset",
		});
		expect(verdict).toBe("terminal");
		expect(dispositionFor(verdict)).toBe("drop");

		const claim = h.rail.makeClaimId("watcher");
		expect(await h.rail.claimCompletion("dlg-boundary-drop", claim)).toBe(true);
		expect(await h.rail.dropClaim("dlg-boundary-drop", claim)).toBe(true);

		const row = h.rail.row("dlg-boundary-drop");
		expect(row?.delivery_state).toBe("dropped");
		expect(row?.delivered_at).toBeNull(); // the ack stays HONEST
		expect(row?.result_json).toContain("payload");
		const seen: Array<Record<string, unknown>> = [];
		await h.rail.restoreUndelivered((e) => seen.push(e));
		expect(seen).toHaveLength(0); // no boot ever replays it
	});

	it("unknown parent ⇒ same fail-closed drop path; result stays queryable", async () => {
		await seedPending("dlg-unknown-parent");
		expect(classifyCompletionTarget(null)).toBe("terminal");
		const claim = h.rail.makeClaimId("watcher");
		await h.rail.claimCompletion("dlg-unknown-parent", claim);
		expect(await h.rail.dropClaim("dlg-unknown-parent", claim)).toBe(true);
		expect(h.rail.row("dlg-unknown-parent")?.result_json).toContain("payload");
	});

	it("idle-ended parent ⇒ deliver path: retarget then ack delivered AFTER acceptance", async () => {
		await seedPending("dlg-idle-deliver");
		expect(classifyCompletionTarget({ endedAt: 1, endReason: "idle" })).toBe(
			"deliver",
		);
		const claim = h.rail.makeClaimId("watcher");
		expect(await h.rail.claimCompletion("dlg-idle-deliver", claim)).toBe(true);
		// ...adapter accepted the forged turn into the chat's current session...
		expect(await h.rail.completeClaim("dlg-idle-deliver", claim)).toBe(true);
		const row = h.rail.row("dlg-idle-deliver");
		expect(row?.delivery_state).toBe("delivered");
		expect(row?.delivered_at).not.toBeNull();
	});

	it("db-unavailable ⇒ retry path: release returns to pending and the attempt cap bounds the churn", async () => {
		await seedPending("dlg-db-down");
		expect(classifyCompletionTarget(null, undefined, true)).toBe("retry");
		let attempts = 0;
		let converged = false;
		for (let i = 0; i < MAX_DELIVERY_ATTEMPTS + 3; i++) {
			const claim = h.rail.makeClaimId(`retry-${i}`);
			if (!(await h.rail.claimCompletion("dlg-db-down", claim))) break;
			attempts++;
			await h.rail.releaseClaim("dlg-db-down", claim);
			converged = h.rail.deliveryStateOf("dlg-db-down") === "dropped";
		}
		expect(converged).toBe(true); // unroutable rows CONVERGE, never loop forever
		expect(attempts).toBe(MAX_DELIVERY_ATTEMPTS);
		expect(h.rail.row("dlg-db-down")?.result_json).toContain("payload");
	});

	it("compression mid-rotation ⇒ retry now, deliver once the tip exists later", async () => {
		await seedPending("dlg-mid-rotation");
		const before = classifyCompletionTarget(
			{ endedAt: 1, endReason: "compression" },
			{ tipSessionId: null },
		);
		expect(before).toBe("retry");
		const claim = h.rail.makeClaimId("w1");
		await h.rail.claimCompletion("dlg-mid-rotation", claim);
		expect(await h.rail.releaseClaim("dlg-mid-rotation", claim)).toBe(true);
		// Later pass: tip visible and live.
		const after = classifyCompletionTarget(
			{ endedAt: 1, endReason: "compression" },
			{ tipSessionId: "tip-live", endedAt: null },
		);
		expect(after).toBe("deliver");
		const claim2 = h.rail.makeClaimId("w2");
		expect(await h.rail.claimCompletion("dlg-mid-rotation", claim2)).toBe(true);
		expect(await h.rail.completeClaim("dlg-mid-rotation", claim2)).toBe(true);
	});
});

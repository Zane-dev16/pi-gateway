// Behavior contracts for the DELIVERY ENGINE (06 §7.2; gateway/run.py:
// _deliver_completion_notification + _deliver_async_delegation_group ports):
// forged-event shape (DEC-022 push lane), ack-after-acceptance honesty,
// coalescing (#70300), sibling claim exclusion, busy-gate claim hygiene,
// throw→release retry, and exactly-one-winner under racing consumers.

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	openWatcherHarness,
	pendingRow,
	seedCompletion,
	seedRouting,
	seedSession,
	type WatcherHarness,
} from "./testing/harness.js";
import { listPendingCompletions } from "./pending.js";

const KEY = "agent:main:telegram:dm:100";

let h: WatcherHarness;

beforeEach(async () => {
	h = await openWatcherHarness();
	await seedSession(h, "parent", { sessionKey: KEY });
	await seedRouting(h, KEY, "parent");
});

afterEach(async () => {
	await h.close();
});

function pendingOf(
	id: string,
): ReturnType<typeof listPendingCompletions>[number] {
	const row = listPendingCompletions(h.store.db).find(
		(c) => c.delegationId === id,
	);
	if (!row) throw new Error(`no pending completion ${id}`);
	return row;
}

describe("forged event shape (DEC-022 push lane)", () => {
	it("internal event keyed on the dispatch-time routing key with parent metadata", async () => {
		await seedSession(h, "parent", {
			sessionKey: KEY,
			originJson: {
				platform: "slack",
				chatType: "channel",
				chatId: "C42",
				userId: "u9",
			},
		});
		await seedCompletion(h, {
			delegationId: "dlg-shape",
			originSession: KEY,
			parentSessionId: "parent",
			goal: "audit logs",
		});

		const r = await h.engine.deliverGroup([pendingOf("dlg-shape")]);
		expect(r.disposition).toBe("delivered");
		expect(h.dispatcher.events).toHaveLength(1);

		const evt = h.dispatcher.events[0];
		if (!evt) throw new Error("expected exactly one forged event");
		expect(evt.internal).toBe(true); // push lane: traverses BOTH guards
		expect(evt.messageType).toBe("text");
		expect(evt.metadata?.["gateway_session_key"]).toBe(KEY);
		expect(evt.metadata?.["gateway_session_id"]).toBe("parent");
		// Source rebuilt from the TARGET session's stored SessionSource snapshot.
		expect(evt.source).toMatchObject({
			platform: "slack",
			chatType: "channel",
			chatId: "C42",
		});
		// Self-contained re-injection block.
		expect(evt.text).toContain("[ASYNC DELEGATION COMPLETE — dlg-shape]");
		expect(evt.text).toContain("Original goal: audit logs");
		expect(evt.text).toContain("--- RESULT ---");
	});
});

describe("coalescing a same-route fan-out (#70300)", () => {
	it("two completions for one parent enter as ONE consolidated turn; both acked", async () => {
		await seedCompletion(h, {
			delegationId: "dlg-a",
			originSession: KEY,
			parentSessionId: "parent",
			goal: "task A",
			summary: "result A",
		});
		await seedCompletion(h, {
			delegationId: "dlg-b",
			originSession: KEY,
			parentSessionId: "parent",
			goal: "task B",
			summary: "result B",
		});

		const r = await h.engine.deliverGroup([
			pendingOf("dlg-a"),
			pendingOf("dlg-b"),
		]);
		expect(r.disposition).toBe("delivered");
		expect(h.dispatcher.events).toHaveLength(1); // ONE turn, never two
		const text = h.dispatcher.texts()[0] ?? "";
		expect(text).toContain("[IMPORTANT: 2 background subagent delegations");
		expect(text).toContain("result A");
		expect(text).toContain("result B");

		expect(pendingRow(h, "dlg-a")?.delivery_state).toBe("delivered");
		expect(pendingRow(h, "dlg-b")?.delivery_state).toBe("delivered");
	});

	it("a sibling whose claim another consumer holds is EXCLUDED from our text and stays pending", async () => {
		await seedCompletion(h, {
			delegationId: "dlg-mine",
			originSession: KEY,
			parentSessionId: "parent",
			goal: "mine",
			summary: "my result",
		});
		await seedCompletion(h, {
			delegationId: "dlg-theirs",
			originSession: KEY,
			parentSessionId: "parent",
			goal: "theirs",
			summary: "their result",
		});
		// Another consumer wins dlg-theirs' claim before this engine ticks.
		expect(
			await h.rail.claimCompletion(
				"dlg-theirs",
				h.rail.makeClaimId("other-consumer"),
			),
		).toBe(true);

		const r = await h.engine.deliverGroup([
			pendingOf("dlg-mine"),
			pendingOf("dlg-theirs"),
		]);
		expect(r.disposition).toBe("delivered");
		expect(r.excluded).toEqual(["dlg-theirs"]);

		const text = h.dispatcher.texts()[0] ?? "";
		expect(text).toContain("my result");
		expect(text).not.toContain("their result"); // never double-delivered
		expect(text).not.toContain("[IMPORTANT:"); // single member ⇒ plain path

		expect(pendingRow(h, "dlg-mine")?.delivery_state).toBe("delivered");
		expect(pendingRow(h, "dlg-theirs")?.delivery_state).toBe("pending");
	});
});

describe("busy gate — waiting burns NO attempt budget", () => {
	it("mid-turn target: rows left fully untouched (unclaimed, attempts 0)", async () => {
		await seedCompletion(h, {
			delegationId: "dlg-waiting",
			originSession: KEY,
			parentSessionId: "parent",
		});
		h.liveness.busy.add("parent");

		const r = await h.engine.deliverGroup([pendingOf("dlg-waiting")]);
		expect(r.disposition).toBe("busy");
		expect(h.dispatcher.events).toHaveLength(0);

		const row = pendingRow(h, "dlg-waiting");
		expect(row?.delivery_state).toBe("pending");
		expect(row?.delivery_attempts).toBe(0); // never even claimed
		expect(row?.delivery_claim).toBeNull();
	});
});

describe("ack honesty", () => {
	it("dispatcher rejection releases the claim; a later attempt delivers once", async () => {
		await seedCompletion(h, {
			delegationId: "dlg-flaky",
			originSession: KEY,
			parentSessionId: "parent",
		});
		h.dispatcher.failOnceWith(new Error("adapter rejected"));

		const first = await h.engine.deliverGroup([pendingOf("dlg-flaky")]);
		expect(first.disposition).toBe("retry");
		let row = pendingRow(h, "dlg-flaky");
		expect(row?.delivery_state).toBe("pending"); // honest: not delivered
		expect(row?.delivery_claim).toBeNull(); // released for a later consumer
		expect(row?.delivery_attempts).toBe(1); // real budget burned

		const second = await h.engine.deliverGroup([pendingOf("dlg-flaky")]);
		expect(second.disposition).toBe("delivered");
		row = pendingRow(h, "dlg-flaky");
		expect(row?.delivery_state).toBe("delivered");
		expect(row?.delivery_attempts).toBe(2);
		expect(h.dispatcher.events).toHaveLength(1); // delivered EXACTLY once
	});

	it("attempt-cap churn converges: repeated retries terminally drop", async () => {
		await seedCompletion(h, {
			delegationId: "dlg-churn",
			originSession: KEY,
			parentSessionId: "parent",
		});
		for (let i = 0; i < 8; i++) {
			h.dispatcher.failOnceWith(new Error("adapter down"));
			const r = await h.engine.deliverGroup([pendingOf("dlg-churn")]);
			// Every attempt reports retry; the RAIL converges the row.
			expect(r.disposition).toBe("retry");
		}
		// MAX_DELIVERY_ATTEMPTS=8 reached → release converged to terminal dropped.
		const finalRow = pendingRow(h, "dlg-churn");
		expect(finalRow?.delivery_state).toBe("dropped");
		expect(finalRow?.delivery_attempts).toBe(8);
		expect(h.dispatcher.events).toHaveLength(0);
	});
});

describe("exactly-one-winner across consumers", () => {
	it("racing engines over one store deliver one completion exactly once", async () => {
		await seedCompletion(h, {
			delegationId: "dlg-race",
			originSession: KEY,
			parentSessionId: "parent",
		});
		const members = [pendingOf("dlg-race")];
		// Two independent engines (≈ two gateways) race the same backlog.
		const reports = await Promise.all([
			h.engine.deliverGroup(members),
			h.engine.deliverGroup(members),
		]);
		const dispositions = reports.map((r) => r.disposition).sort();
		expect(dispositions).toEqual(["delivered", "owned-elsewhere"]);
		expect(h.dispatcher.events).toHaveLength(1);
		expect(pendingRow(h, "dlg-race")?.delivery_state).toBe("delivered");
	});
});

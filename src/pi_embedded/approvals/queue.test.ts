// Behavior contracts for the per-session FIFO queue and THE one resolution
// primitive (07 §8.1 queue model, §8.4). Count=0 means NOTHING was pending
// and is authoritative; request_id targeting resolves only its entries;
// unregister/cleanup release blocked waits loudly instead of stranding them.

import { describe, expect, it } from "vitest";

import {
	APPROVAL_CHOICES,
	ApprovalEntry,
	ApprovalQueues,
	type ApprovalRequestData,
} from "./queue.js";
import { Gate } from "./testing/manual-clock.js";

const CMD = { command: "rm -rf /tmp/x", description: "hardline delete" };

function entry(
	overrides: Partial<{ command: string; requestId: string }> = {},
) {
	const request: ApprovalRequestData = {
		...CMD,
		description: "delete",
		patternKey: "rm_rf",
		patternKeys: ["rm_rf"],
		command: overrides.command ?? CMD.command,
	};
	if (overrides.requestId !== undefined) {
		request.requestId = overrides.requestId;
	}
	return new ApprovalEntry(request);
}

async function settledWait(wait: Promise<void>): Promise<boolean> {
	const winner = await Promise.race([
		wait.then(() => true),
		new Promise<false>((resolve) => setTimeout(() => resolve(false), 20)),
	]);
	return winner;
}

describe("resolve — THE resolution primitive", () => {
	it("resolves the OLDEST pending entry first (FIFO pop(0))", () => {
		const queues = new ApprovalQueues();
		const first = entry();
		const second = entry();
		queues.enqueue("s", first);
		queues.enqueue("s", second);

		expect(queues.resolve("s", "once")).toBe(1);
		expect(first.result).toBe("once");
		expect(second.result).toBeNull();

		expect(queues.resolve("s", "deny")).toBe(1);
		expect(second.result).toBe("deny");
	});

	it("returns the count resolved — 0 when nothing was pending (authoritative)", () => {
		const queues = new ApprovalQueues();
		expect(queues.resolve("missing-session", "once")).toBe(0);
		const e = entry();
		queues.enqueue("s", e);
		expect(queues.resolve("s", "once")).toBe(1);
		// Double-answer idempotence at the primitive level: the second resolve
		// finds an empty queue and resolves nothing.
		expect(queues.resolve("s", "once")).toBe(0);
		expect(e.result).toBe("once");
	});

	it("request_id targeting resolves ONLY the targeted entries", () => {
		const queues = new ApprovalQueues();
		const a = entry({ requestId: "req-a" });
		const b = entry({ requestId: "req-b", command: "sudo rm x" });
		const c = entry({ requestId: "req-c", command: "mkfs" });
		queues.enqueue("s", a);
		queues.enqueue("s", b);
		queues.enqueue("s", c);

		expect(queues.resolve("s", "session", { requestId: "req-b" })).toBe(1);
		expect(b.result).toBe("session");
		expect(a.result).toBeNull();
		expect(c.result).toBeNull();
		expect(queues.hasBlocking("s")).toBe(true);

		// Unknown request id resolves nothing.
		expect(queues.resolve("s", "once", { requestId: "nope" })).toBe(0);
		expect(queues.hasBlocking("s")).toBe(true);
	});

	it("resolve_all resolves every pending entry at once (/approve all)", () => {
		const queues = new ApprovalQueues();
		const entries = [entry(), entry(), entry()];
		for (const e of entries) queues.enqueue("s", e);
		expect(queues.resolve("s", "always", { resolveAll: true })).toBe(3);
		for (const e of entries) expect(e.result).toBe("always");
		expect(queues.hasBlocking("s")).toBe(false);
	});

	it("relays the deny reason verbatim into the entry", () => {
		const queues = new ApprovalQueues();
		const e = entry();
		queues.enqueue("s", e);
		const reason = "this deletes the staging DB, use pg_dump instead";
		queues.resolve("s", "deny", { reason });
		expect(e.reason).toBe(reason);
	});
});

describe("waiter release paths — never strand a wait", () => {
	it("settle() wakes exactly-once; double settle is a no-op", async () => {
		const e = entry();
		e.result = "deny";
		e.settle();
		await expect(settledWait(e.wait)).resolves.toBe(true);
		e.settle(); // idempotent
		expect(e.result).toBe("deny");
	});

	it("unregisterNotify signals ALL blocked waits WITHOUT a result", async () => {
		const queues = new ApprovalQueues();
		const gate = new Gate();
		queues.registerNotify("s", () => gate.open());
		const e1 = entry();
		const e2 = entry();
		queues.enqueue("s", e1);
		queues.enqueue("s", e2);

		let notified = 0;
		void settledWait(e1.wait).then(() => notified++);
		void settledWait(e2.wait).then(() => notified++);

		queues.unregisterNotify("s");

		await expect(settledWait(e1.wait)).resolves.toBe(true);
		await expect(settledWait(e2.wait)).resolves.toBe(true);
		expect(e1.result).toBeNull(); // no decision injected
		expect(e2.result).toBeNull();
		expect(queues.hasBlocking("s")).toBe(false); // queue torn down
		expect(queues.isNotifyRegistered("s")).toBe(false);
	});

	it("clearSession denies + releases blocked waits IMMEDIATELY", async () => {
		const queues = new ApprovalQueues();
		const e = entry();
		queues.enqueue("s", e);
		queues.clearSession("s");
		expect(e.result).toBe("deny");
		await expect(settledWait(e.wait)).resolves.toBe(true);
		expect(queues.hasBlocking("s")).toBe(false);
	});
});

describe("introspection surfaces", () => {
	it("listApprovals returns replay-safe snapshots (mutating them is inert)", () => {
		const queues = new ApprovalQueues();
		queues.enqueue("s", entry());
		const [snapshot] = queues.listApprovals("s");
		expect(snapshot?.command).toBe(CMD.command);
		snapshot?.patternKeys.push("tampered");
		expect(queues.listApprovals("s")![0]?.patternKeys).toEqual(["rm_rf"]);
	});

	it("oldestPending returns the head snapshot or null", () => {
		const queues = new ApprovalQueues();
		expect(queues.oldestPending("s")).toBeNull();
		queues.enqueue("s", entry({ command: "first" }));
		queues.enqueue("s", entry({ command: "second" }));
		expect(queues.oldestPending("s")?.command).toBe("first");
	});

	it("ack records delivery for api-server replays", () => {
		const queues = new ApprovalQueues();
		queues.enqueue("s", entry({ requestId: "req-1" }));
		expect(queues.ack("s", "req-1")).toBe(true);
		expect(queues.ack("s", "req-other")).toBe(false);
		expect(
			queues.listApprovals("s").every((data) => data.requestId !== undefined),
		).toBe(true);
	});

	it("findIdenticalPending matches (command, patternKeys) EXACTLY for coalescing", () => {
		const queues = new ApprovalQueues();
		const leader = entry();
		queues.enqueue("s", leader);

		expect(queues.findIdenticalPending("s", CMD.command, ["rm_rf"])).toBe(
			leader,
		);
		// Different pattern-key set → NOT identical.
		expect(
			queues.findIdenticalPending("s", CMD.command, ["other_pattern"]),
		).toBeNull();
		// Different command → NOT identical.
		expect(
			queues.findIdenticalPending("s", "sudo reboot", ["rm_rf"]),
		).toBeNull();
		// Different session → invisible.
		expect(
			queues.findIdenticalPending("other", CMD.command, ["rm_rf"]),
		).toBeNull();
	});

	it("entries get uuid-hex request ids when absent", () => {
		const a = new ApprovalEntry({ ...CMD, description: "d" });
		const b = new ApprovalEntry({ ...CMD, description: "d" });
		expect(a.data.requestId).toMatch(/^[0-9a-f]{32}$/);
		expect(b.data.requestId).toMatch(/^[0-9a-f]{32}$/);
		expect(a.data.requestId).not.toBe(b.data.requestId);
	});

	it("choices are closed over the Telegram parity set", () => {
		expect(APPROVAL_CHOICES).toEqual(["once", "session", "always", "deny"]);
	});
});

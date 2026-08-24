// Behavior contracts for the DEC-008 pending-row protocol
// (hermes_state.py handoff block parity): request CAS, atomic claim,
// terminal writes with error payloads, oldest-first listing, and the CLI
// stub-row creation path.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import { ManualClock } from "./testing/manual-clock.js";
import { HandoffQueue } from "./queue.js";

let dir: string;
let store: StateStore;
let queue: HandoffQueue;
const clock = new ManualClock();

beforeEach(async () => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-handoff-queue-"));
	store = await StateStore.open(join(dir, "state.db"));
	queue = new HandoffQueue(store.db, { clock });
});

afterEach(async () => {
	await store.close();
});

async function seedSession(id: string, startedAt?: number): Promise<void> {
	await store.withWrite((db) => {
		db.prepare(
			"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'cli', ?)",
		).run(id, startedAt ?? clock.nowSeconds());
	});
}

describe("HandoffQueue — request_handoff CAS", () => {
	it("NULL → pending on first request; snapshot carries platform", async () => {
		await seedSession("s1");
		expect(await queue.requestHandoff("s1", "telegram")).toBe(true);
		expect(queue.getHandoffState("s1")).toEqual({
			state: "pending",
			platform: "telegram",
			error: null,
		});
	});

	it("refused while pending (already in flight)", async () => {
		await seedSession("s1");
		expect(await queue.requestHandoff("s1", "telegram")).toBe(true);
		expect(await queue.requestHandoff("s1", "slack")).toBe(false);
		expect(queue.getHandoffState("s1")?.platform).toBe("telegram");
	});

	it("retry legal after terminal states; platform+error overwritten", async () => {
		await seedSession("s1");
		await queue.requestHandoff("s1", "telegram");
		await queue.claimHandoff("s1");
		await queue.failHandoff("s1", "boom");
		expect(await queue.requestHandoff("s1", "slack")).toBe(true);
		expect(queue.getHandoffState("s1")).toEqual({
			state: "pending",
			platform: "slack",
			error: null,
		});

		await queue.completeHandoff("s1");
		await queue
			.requestHandoff("s1", "discord")
			.then((ok) => expect(ok).toBe(true));
		expect(queue.getHandoffState("s1")?.platform).toBe("discord");
	});

	it("request on unknown session row returns false (no upsert)", async () => {
		expect(await queue.requestHandoff("ghost", "telegram")).toBe(false);
	});
});

describe("HandoffQueue — claim/complete/fail transitions", () => {
	it("claim wins exactly once: pending→running, second claim loses", async () => {
		await seedSession("s1");
		await queue.requestHandoff("s1", "telegram");
		expect(await queue.claimHandoff("s1")).toBe(true);
		expect(await queue.claimHandoff("s1")).toBe(false); // no longer pending
		expect(queue.getHandoffState("s1")?.state).toBe("running");
	});

	it("claim refused for running/completed/failed/unknown rows", async () => {
		await seedSession("a");
		await seedSession("b");
		await queue.requestHandoff("a", "t");
		// Terminal rows (never claimed here): a claim never resurrects them.
		await queue.completeHandoff("a");
		await queue.requestHandoff("b", "t");
		await queue.failHandoff("b", "x");
		expect(await queue.claimHandoff("ghost")).toBe(false);
		expect(await queue.claimHandoff("a")).toBe(false);
	});

	it("complete clears the error payload; fail records it truncated to 500", async () => {
		await seedSession("s1");
		await queue.requestHandoff("s1", "telegram");
		await queue.failHandoff("s1", "x".repeat(501) + "TAIL");
		const failed = queue.getHandoffState("s1");
		expect(failed?.state).toBe("failed");
		expect(failed?.error).toHaveLength(500); // error[:500] parity
		expect(failed?.error?.endsWith("TAIL")).toBe(false);

		// Retry then complete: error must be NULL again.
		await queue.requestHandoff("s1", "telegram");
		await queue.completeHandoff("s1");
		expect(queue.getHandoffState("s1")).toEqual({
			state: "completed",
			platform: "telegram",
			error: null,
		});
	});
});

describe("HandoffQueue — listPendingHandoffs", () => {
	it("lists ONLY pending rows, oldest first (started_at ASC)", async () => {
		clock.advance(1000);
		await seedSession("old", clock.nowSeconds() - 100);
		await seedSession("mid", clock.nowSeconds() - 50);
		await seedSession("new", clock.nowSeconds());
		await seedSession("done");
		for (const id of ["old", "mid", "new"]) {
			await queue.requestHandoff(id, "telegram");
		}
		await queue.requestHandoff("done", "telegram");
		await queue.completeHandoff("done");

		// A claimed row is invisible to the watcher — exactly-once across a
		// crash: claimed work never re-dispatches.
		await queue.requestHandoff("claimed-but-crashed", "telegram");
		await seedSession("claimed-but-crashed", 1);
		await queue.claimHandoff("claimed-but-crashed");

		expect(queue.listPendingHandoffs().map((r) => r.id)).toEqual([
			"old",
			"mid",
			"new",
		]);
		const first = queue.listPendingHandoffs()[0];
		expect(first?.handoffPlatform).toBe("telegram");
		expect(typeof first?.startedAt).toBe("number");
	});

	it("returns [] when nothing is pending", async () => {
		expect(queue.listPendingHandoffs()).toEqual([]);
	});
});

describe("HandoffQueue — ensureSessionRow (CLI stub path)", () => {
	it("creates a missing cli row once; never clobbers an existing row", async () => {
		expect(await queue.ensureSessionRow("fresh")).toBe(true);
		expect(await queue.ensureSessionRow("fresh")).toBe(false);

		await seedSession("existing");
		await store.withWrite((db) =>
			db.prepare("UPDATE sessions SET title='keep' WHERE id='existing'").run(),
		);
		expect(await queue.ensureSessionRow("existing")).toBe(false);
		expect(
			(
				store.db
					.prepare("SELECT title FROM sessions WHERE id='existing'")
					.get() as { title: string | null }
			).title,
		).toBe("keep");
	});

	it("getHandoffState returns null for unknown rows (no handoff record)", () => {
		expect(queue.getHandoffState("nope")).toBeNull();
	});
});

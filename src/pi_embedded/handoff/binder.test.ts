// Behavior contracts for the switch_session re-bind (gateway/session.py:
// switch_session + hermes_state.py promote_to_session_reset / reopen_session
// parity) — the half of DEC-008 that carries the transcript-replay guarantee.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import { ManualClock } from "./testing/manual-clock.js";
import { RoutingBinder } from "./binder.js";

let dir: string;
let store: StateStore;
let binder: RoutingBinder;
const clock = new ManualClock();

beforeEach(async () => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-handoff-binder-"));
	store = await StateStore.open(join(dir, "state.db"));
	binder = new RoutingBinder(store.db, { clock });
});

afterEach(async () => {
	await store.close();
});

interface SessionRow {
	ended_at: number | null;
	end_reason: string | null;
	model_config: string | null;
	session_key: string | null;
	parent_session_id?: string | null;
}

function sessionRow(id: string): SessionRow | undefined {
	return store.db
		.prepare(
			"SELECT ended_at, end_reason, model_config, session_key FROM sessions WHERE id = ?",
		)
		.get(id) as SessionRow | undefined;
}

async function seedSession(
	id: string,
	opts: Partial<SessionRow> = {},
): Promise<void> {
	await store.withWrite((db) => {
		db.prepare(
			"INSERT OR IGNORE INTO sessions (id, source, started_at, ended_at, end_reason, model_config, session_key) VALUES (?, 'cli', ?, ?, ?, ?, ?)",
		).run(
			id,
			clock.nowSeconds(),
			opts.ended_at ?? null,
			opts.end_reason ?? null,
			opts.model_config ?? null,
			opts.session_key ?? null,
		);
	});
}

describe("RoutingBinder — ensureEntry", () => {
	it("creates an entry when absent; preserves it when present", async () => {
		const first = await binder.ensureEntry("agent:main:telegram:dm:100", {
			origin: "handoff",
			platform: "telegram",
			chat_type: "dm",
			display_name: "Home Chat",
		});
		expect(first.session_id).toMatch(/^pending-/);
		expect(first.platform).toBe("telegram");

		clock.advance(50);
		const second = await binder.ensureEntry("agent:main:telegram:dm:100", {});
		expect(second.session_id).toBe(first.session_id); // adopted, not reset
		expect(second.created_at).toBe(first.created_at);
	});
});

describe("RoutingBinder — switchSession", () => {
	it("returns null when no entry exists (loud caller-side failure)", async () => {
		expect(
			await binder.switchSession("agent:main:telegram:dm:404", "cli-1"),
		).toBeNull();
	});

	it("no-op when already bound to the target (entry untouched)", async () => {
		await binder.ensureEntry("k1", { platform: "telegram" });
		const before = binder.entryOf("k1");
		clock.advance(100);
		const same = await binder.switchSession("k1", before!.session_id);
		expect(same?.updated_at).toBe(before!.updated_at);
	});

	it("re-binds onto the CLI session id, preserving identity fields", async () => {
		await seedSession("cli-1");
		await binder.ensureEntry("agent:main:slack:group:C9", {
			origin: "organic",
			display_name: "War Room",
			platform: "slack",
			chat_type: "group",
		});
		const before = binder.entryOf("agent:main:slack:group:C9")!;
		clock.advance(10);

		const switched = await binder.switchSession(
			"agent:main:slack:group:C9",
			"cli-1",
		);
		expect(switched).not.toBeNull();
		expect(switched!.session_id).toBe("cli-1");
		expect(switched!.created_at).toBeGreaterThan(before.created_at); // fresh
		expect(switched!.origin).toBe("organic"); // identity carried over
		expect(switched!.display_name).toBe("War Room");
		expect(switched!.platform).toBe("slack");
		expect(switched!.chat_type).toBe("group");
		expect(binder.entryOf("agent:main:slack:group:C9")?.session_id).toBe(
			"cli-1",
		);
	});

	it("promotes a LIVE predecessor to end_reason='session_switch'", async () => {
		await seedSession("old-live");
		await seedSession("cli-1");
		await binder.ensureEntry("k", {});
		await binder.switchSession("k", "old-live");
		await binder.switchSession("k", "cli-1");

		const old = sessionRow("old-live");
		expect(old?.ended_at).not.toBeNull();
		expect(old?.end_reason).toBe("session_switch");
	});

	it("promotes ACCIDENTAL ends (agent_close/ws_orphan_reap); explicit boundaries keep first-reason-wins", async () => {
		// Bind each key to a LIVE session first, then let it end accidentally /
		// explicitly BEFORE the handoff switch makes it the predecessor.
		await seedSession("old-live");
		await seedSession("comp-live");
		await seedSession("cli-1");
		await binder.ensureEntry("k1", {});
		await binder.ensureEntry("k2", {});
		await binder.switchSession("k1", "old-live");
		await binder.switchSession("k2", "comp-live");
		await store.withWrite((db) =>
			db
				.prepare(
					"UPDATE sessions SET ended_at = 5, end_reason = 'agent_close' WHERE id = 'old-live'",
				)
				.run(),
		);
		await store.withWrite((db) =>
			db
				.prepare(
					"UPDATE sessions SET ended_at = 6, end_reason = 'compression' WHERE id = 'comp-live'",
				)
				.run(),
		);

		await binder.switchSession("k1", "cli-1");
		expect(sessionRow("old-live")?.end_reason).toBe("session_switch");

		await binder.switchSession("k2", "cli-1");
		expect(sessionRow("comp-live")?.end_reason).toBe("compression"); // preserved
	});

	it("reopens the target CLI row (clears ended_at/end_reason)", async () => {
		await seedSession("cli-1", { ended_at: 7, end_reason: "user_exit" });
		await binder.ensureEntry("k", {});
		await binder.switchSession("k", "cli-1");
		const reopened = sessionRow("cli-1");
		expect(reopened?.ended_at).toBeNull();
		expect(reopened?.end_reason).toBeNull();
	});

	it("stamps markerless legacy reset children with $._reset_from before clearing the boundary", async () => {
		// cli-1 ended at a RESET boundary and has a same-key markerless child.
		await seedSession("cli-1", {
			ended_at: 8,
			end_reason: "session_reset",
			session_key: "agent:main:telegram:dm:77",
		});
		await seedSession("reset-child", {
			session_key: "agent:main:telegram:dm:77",
		});
		await store.withWrite((db) =>
			db
				.prepare(
					"UPDATE sessions SET parent_session_id='cli-1' WHERE id='reset-child'",
				)
				.run(),
		);
		// A child WITH the marker must stay untouched (idempotence).
		await seedSession("marked-child", {
			session_key: "agent:main:telegram:dm:77",
			model_config: '{"_reset_from":"cli-1"}',
		});
		await store.withWrite((db) =>
			db
				.prepare(
					"UPDATE sessions SET parent_session_id='cli-1' WHERE id='marked-child'",
				)
				.run(),
		);

		await binder.ensureEntry("k", {});
		await binder.switchSession("k", "cli-1");

		const stamped = JSON.parse(
			sessionRow("reset-child")?.model_config ?? "{}",
		) as { _reset_from?: string };
		expect(stamped._reset_from).toBe("cli-1");
		const marked = JSON.parse(
			sessionRow("marked-child")?.model_config ?? "{}",
		) as { _reset_from?: string };
		expect(marked._reset_from).toBe("cli-1"); // unchanged value

		// A NON-same-key child is not a reset continuation — never stamped.
		await seedSession("unrelated", { session_key: "other:key" });
		await store.withWrite((db) =>
			db
				.prepare(
					"UPDATE sessions SET parent_session_id='cli-1' WHERE id='unrelated'",
				)
				.run(),
		);
		const unrelatedConfig = sessionRow("unrelated")?.model_config ?? null;
		expect(unrelatedConfig === null ? {} : JSON.parse(unrelatedConfig)).toEqual(
			{},
		);
	});
});

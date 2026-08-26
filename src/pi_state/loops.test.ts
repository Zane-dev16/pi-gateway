// Behavior contracts for the /loop row store (state_meta `loop:<session_id>`).
//
// These pin the PERSISTENCE half of hermes_cli/loops.py against a REAL
// StateStore: byte-format parity of the serialized row, tolerant decode with
// corrupt-row degradation, audit-preserving clear, active-only enumeration
// with LIKE-escape safety, compression-rotation migration (#33618), and the
// StateStore facade surface.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	LOOP_DEFAULT_MAX_TICKS,
	LOOP_META_PREFIX,
	type LoopState,
	clearLoopRow,
	listActiveLoopRows,
	listMetaPrefix,
	loadLoopRow,
	loopMetaKey,
	loopStateFromJson,
	loopStateToJson,
	migrateLoopRowToSession,
	saveLoopRow,
	StateStore,
} from "./index.js";

function sampleState(overrides: Partial<LoopState> = {}): LoopState {
	return {
		prompt: "check the deploy status",
		status: "active",
		mode: "interval",
		intervalSeconds: 300,
		currentDelay: 300,
		times: 0,
		until: "",
		maxTicks: 100,
		ticksFired: 0,
		createdAt: 1_775_000_000,
		lastFiredAt: 0,
		nextDueAt: 1_775_000_300,
		awaitingResponse: false,
		lastResponseDigest: "",
		pausedReason: null,
		lastStopReason: null,
		route: { platform: "telegram", chat_id: "100", chat_type: "dm" },
		...overrides,
	};
}

describe("loop row codec (loops.py LoopState to_json/from_json parity)", () => {
	it("serializes with Hermes snake_case field names — byte-format parity", () => {
		const raw = JSON.parse(loopStateToJson(sampleState())) as Record<
			string,
			unknown
		>;
		expect(Object.keys(raw).sort()).toEqual(
			[
				"prompt",
				"status",
				"mode",
				"interval_seconds",
				"current_delay",
				"times",
				"until",
				"max_ticks",
				"ticks_fired",
				"created_at",
				"last_fired_at",
				"next_due_at",
				"awaiting_response",
				"last_response_digest",
				"paused_reason",
				"last_stop_reason",
				"route",
			].sort(),
		);
		expect(raw["interval_seconds"]).toBe(300);
		expect(raw["next_due_at"]).toBe(1_775_000_300);
		expect(raw["route"]).toEqual({
			platform: "telegram",
			chat_id: "100",
			chat_type: "dm",
		});
	});

	it("round-trips every field through decode", () => {
		const state = sampleState({
			status: "paused",
			mode: "self_paced",
			pausedReason: "user-paused",
			lastStopReason: null,
			awaitingResponse: true,
			lastResponseDigest: "abc123",
			times: 5,
			until: "suite green",
		});
		expect(loopStateFromJson(loopStateToJson(state))).toEqual(state);
	});

	it("decodes a literal Hermes-shaped row verbatim", () => {
		const state = loopStateFromJson(
			JSON.stringify({
				prompt: "/recap",
				status: "active",
				mode: "interval",
				interval_seconds: 600.0,
				current_delay: 600.0,
				times: 3,
				until: "",
				max_ticks: 100,
				ticks_fired: 2,
				created_at: 1751000000.5,
				last_fired_at: 1751000300.25,
				next_due_at: 1751000900.0,
				awaiting_response: false,
				last_response_digest: "",
				paused_reason: null,
				last_stop_reason: null,
				route: { platform: "slack", chat_id: "C1", chat_type: "channel" },
			}),
		);
		expect(state.intervalSeconds).toBe(600);
		expect(state.ticksFired).toBe(2);
		expect(state.times).toBe(3);
		expect(state.route["chat_id"]).toBe("C1");
	});

	it("missing fields fall back to dataclass defaults", () => {
		const state = loopStateFromJson(JSON.stringify({ prompt: "x" }));
		expect(state.status).toBe("active");
		expect(state.mode).toBe("interval");
		expect(state.maxTicks).toBe(LOOP_DEFAULT_MAX_TICKS);
		expect(state.awaitingResponse).toBe(false);
		expect(state.pausedReason).toBeNull();
		expect(state.route).toEqual({});
	});

	it("falsy max_ticks stays 0 (unlimited) — `or 0` chain parity", () => {
		expect(
			loopStateFromJson(JSON.stringify({ prompt: "x", max_ticks: 0 })).maxTicks,
		).toBe(0);
		expect(
			loopStateFromJson(JSON.stringify({ prompt: "x", times: 0 })).times,
		).toBe(0);
	});

	it("Python truthiness drives awaiting_response coercion", () => {
		expect(
			loopStateFromJson(JSON.stringify({ prompt: "x", awaiting_response: 1 }))
				.awaitingResponse,
		).toBe(true);
		expect(
			loopStateFromJson(JSON.stringify({ prompt: "x", awaiting_response: "" }))
				.awaitingResponse,
		).toBe(false);
	});

	it("non-dict route degrades to {}", () => {
		expect(
			loopStateFromJson(JSON.stringify({ prompt: "x", route: ["nope"] })).route,
		).toEqual({});
	});

	it("garbage numerics THROW so corrupt rows degrade to absent upstream", () => {
		expect(() =>
			loopStateFromJson(JSON.stringify({ prompt: "x", next_due_at: "soon" })),
		).toThrow(/next_due_at/);
	});
});

describe("loop row persistence (real StateStore)", () => {
	let dirs: string[] = [];
	let stores: StateStore[] = [];

	async function open(): Promise<StateStore> {
		const dir = mkdtempSync(join(tmpdir(), "pi-gw-loops-store-"));
		dirs.push(dir);
		const store = await StateStore.open(join(dir, "state.db"));
		stores.push(store);
		return store;
	}

	beforeEach(() => {
		dirs = [];
		stores = [];
	});

	afterEach(async () => {
		for (const s of stores.splice(0)) await s.close(false);
		for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
	});

	it("save/load round-trips through state_meta under the loop: prefix", async () => {
		const db = (await open()).db;
		await saveLoopRow(db, "sess-1", sampleState());
		expect(loadLoopRow(db, "sess-1")).toEqual(sampleState());
		const [key, value] = listMetaPrefix(db, LOOP_META_PREFIX)[0]!;
		expect(key).toBe("loop:sess-1");
		expect(JSON.parse(value)).toMatchObject({
			prompt: "check the deploy status",
		});
	});

	it("load misses cleanly: absent row, empty session id, corrupt payload", async () => {
		const db = (await open()).db;
		expect(loadLoopRow(db, "nope")).toBeNull();
		expect(loadLoopRow(db, "")).toBeNull();

		db.prepare("INSERT INTO state_meta (key, value) VALUES (?, ?)").run(
			"loop:broken",
			"{not json",
		);
		expect(loadLoopRow(db, "broken")).toBeNull();
	});

	it("save is a no-op for an empty session id — nothing persisted", async () => {
		const db = (await open()).db;
		await saveLoopRow(db, "", sampleState());
		expect(listActiveLoopRows(db)).toEqual([]);
	});

	it("clear marks status=cleared preserving the row for audit", async () => {
		const db = (await open()).db;
		await saveLoopRow(db, "s", sampleState());
		expect(await clearLoopRow(db, "s")).toBe(true);
		const cleared = loadLoopRow(db, "s");
		expect(cleared?.status).toBe("cleared");
		expect(cleared?.prompt).toBe("check the deploy status");
		// re-clear stays quiet (clear_loop has no already-cleared error arm)
		expect(await clearLoopRow(db, "s")).toBe(true);
	});

	it("clear of an absent row reports false without writing", async () => {
		const db = (await open()).db;
		expect(await clearLoopRow(db, "ghost")).toBe(false);
		expect(loadLoopRow(db, "ghost")).toBeNull();
	});

	it("listActive enumerates ACTIVE rows only as (id, state) pairs", async () => {
		const db = (await open()).db;
		await saveLoopRow(db, "a", sampleState());
		await saveLoopRow(db, "b", sampleState({ status: "paused" }));
		await saveLoopRow(db, "c", sampleState({ status: "done" }));
		await saveLoopRow(db, "d", sampleState({ status: "cleared" }));

		const rows = listActiveLoopRows(db);
		expect(rows.map(([sid]) => sid)).toEqual(["a"]);
		expect(rows[0]?.[1].prompt).toBe("check the deploy status");
	});

	it("listActive skips unparseable rows and empty ids instead of throwing", async () => {
		const db = (await open()).db;
		db.prepare(
			"INSERT INTO state_meta (key, value) VALUES ('loop:bad', '{oops')",
		).run();
		db.prepare(
			"INSERT INTO state_meta (key, value) VALUES ('loop:', '{}')",
		).run();
		await saveLoopRow(db, "good", sampleState());
		expect(listActiveLoopRows(db).map(([sid]) => sid)).toEqual(["good"]);
	});

	it("LIKE wildcards inside session ids never leak across keys", async () => {
		const db = (await open()).db;
		await saveLoopRow(db, "a%b", sampleState({ prompt: "percent" }));
		await saveLoopRow(db, "aXb_c", sampleState({ prompt: "underscore" }));
		await saveLoopRow(db, "ab", sampleState({ prompt: "plain" }));

		const byId = new Map(
			listActiveLoopRows(db).map(([sid, s]) => [sid, s.prompt]),
		);
		expect(byId.get("a%b")).toBe("percent");
		expect(byId.get("aXb_c")).toBe("underscore");
		expect(byId.get("ab")).toBe("plain");
	});

	it("listMetaPrefix with an empty prefix returns []", async () => {
		const db = (await open()).db;
		expect(listMetaPrefix(db, "")).toEqual([]);
	});
});

describe("migrateLoopRowToSession (#33618 rotation carry)", () => {
	let dir: string;
	let store: StateStore;

	beforeEach(async () => {
		dir = mkdtempSync(join(tmpdir(), "pi-gw-loops-mig-"));
		store = await StateStore.open(join(dir, "state.db"));
	});

	afterEach(async () => {
		await store.close(false);
		rmSync(dir, { recursive: true, force: true });
	});

	it("copies the loop onto the child and archives the parent as cleared", async () => {
		await saveLoopRow(store.db, "parent", sampleState());
		expect(
			await migrateLoopRowToSession(store.db, "parent", "child", "compression"),
		).toBe(true);
		expect(loadLoopRow(store.db, "child")?.status).toBe("active");
		expect(loadLoopRow(store.db, "child")?.prompt).toBe(
			"check the deploy status",
		);
		expect(loadLoopRow(store.db, "parent")?.status).toBe("cleared");
		// exactly ONE active row per logical conversation
		expect(listActiveLoopRows(store.db).map(([sid]) => sid)).toEqual(["child"]);
	});

	it("refuses: same id, unknown parent, already-cleared parent, occupied target", async () => {
		expect(await migrateLoopRowToSession(store.db, "p", "p")).toBe(false);
		expect(await migrateLoopRowToSession(store.db, "", "child")).toBe(false);
		expect(await migrateLoopRowToSession(store.db, "ghost", "child")).toBe(
			false,
		);

		await saveLoopRow(store.db, "old", sampleState({ status: "cleared" }));
		expect(await migrateLoopRowToSession(store.db, "old", "child")).toBe(false);

		await saveLoopRow(store.db, "live", sampleState());
		await saveLoopRow(store.db, "taken", sampleState());
		expect(await migrateLoopRowToSession(store.db, "live", "taken")).toBe(
			false,
		);
		// refusal left both untouched
		expect(loadLoopRow(store.db, "live")?.status).toBe("active");
		expect(loadLoopRow(store.db, "taken")?.status).toBe("active");
	});
});

describe("StateStore /loop facade", () => {
	let dir: string;
	let store: StateStore;

	beforeEach(async () => {
		dir = mkdtempSync(join(tmpdir(), "pi-gw-loops-facade-"));
		store = await StateStore.open(join(dir, "state.db"));
	});

	afterEach(async () => {
		await store.close(false);
		rmSync(dir, { recursive: true, force: true });
	});

	it("exposes the full row surface on the store", async () => {
		expect(store.loadLoop("none")).toBeNull();
		await store.saveLoop("s1", sampleState());
		expect(store.loadLoop("s1")?.nextDueAt).toBe(1_775_000_300);
		expect(store.listActiveLoops().map(([sid]) => sid)).toEqual(["s1"]);
		expect(await store.migrateLoopToSession("s1", "s2")).toBe(true);
		expect(store.listActiveLoops().map(([sid]) => sid)).toEqual(["s2"]);
		expect(await store.clearLoop("s2")).toBe(true);
		expect(store.listActiveLoops()).toEqual([]);
	});

	it("meta key helper matches the documented prefix", () => {
		expect(loopMetaKey("abc")).toBe("loop:abc");
		expect(LOOP_META_PREFIX).toBe("loop:");
	});
});

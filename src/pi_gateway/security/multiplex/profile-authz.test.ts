// Behavior contracts for per-profile pairing/authz store instances and the
// adapter-view refusal router (06 §4; 06 §10 rows "Pairing multiplex
// isolation" and "Adapter fallback refusal"). Real per-profile SQLite dbs on
// mkdtemp dirs — no shared mutable state anywhere.

import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Database from "better-sqlite3";

type ProfileDb = Database.Database;

import { isUserAuthorized } from "../authz/index.js";
import { ProfileAdapterViews, ProfileAuthzIsolation } from "./index.js";

let dir: string;
const opened: ProfileDb[] = [];

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-authz-"));
});

afterEach(() => {
	for (const db of opened.splice(0)) {
		try {
			db.close();
		} catch {
			/* already closed */
		}
	}
	rmSync(dir, { recursive: true, force: true });
});

function freshDb(name: string): ProfileDb {
	mkdirSync(join(dir, name), { recursive: true });
	const db = new Database(join(dir, name, "state.db"));
	opened.push(db);
	return db;
}

function makeIsolation(): ProfileAuthzIsolation {
	return new ProfileAuthzIsolation({
		globalDb: freshDb("global"),
		openProfileDb: () => freshDb(`p-${Math.random().toString(36).slice(2)}`),
	});
}

describe("pairing multiplex isolation (profile A approval ≠ profile B admission)", () => {
	it("an approval granted in profile A satisfies A's check only", async () => {
		const iso = makeIsolation();
		const storeA = iso.registerProfile("a");
		const storeB = iso.registerProfile("b");

		const code = await storeA.generateCode("telegram", "42", "Alice");
		expect(code).not.toBeNull();
		expect(await storeA.approveCode("telegram", code as string)).not.toBeNull();

		expect(storeA.isApproved("telegram", "42")).toBe(true);
		expect(storeB.isApproved("telegram", "42")).toBe(false);
		expect(iso.defaultStore().isApproved("telegram", "42")).toBe(false);

		// Through the REAL decision chain with per-profile store selection:
		const source = { platform: "telegram", userId: "42", chatType: "dm" };
		const onA = isUserAuthorized(
			{
				...source,
				profile: "a",
				deliveredViaUpstreamRelay: false,
			},
			{ pairingStoreFor: (s) => iso.pairingStoreFor(s) },
		);
		expect(onA.allowed).toBe(true);
		expect(onA.reasonCode).toBe("pairing_approved");

		const onB = isUserAuthorized(
			{
				...source,
				profile: "b",
				deliveredViaUpstreamRelay: false,
			},
			{ pairingStoreFor: (s) => iso.pairingStoreFor(s) },
		);
		expect(onB.allowed).toBe(false);
		expect(onB.gate).toBe(10);
	});

	it("stores are DISTINCT instances over DISTINCT connections (no shared mutable state)", async () => {
		const iso = makeIsolation();
		const a = iso.registerProfile("a");
		const b = iso.registerProfile("b");
		expect(a).not.toBe(b);

		await a.generateCode("slack", "u1"); // pending row lands ONLY in A's db
		expect(a.listPending("slack")).toHaveLength(1);
		expect(b.listPending("slack")).toHaveLength(0);

		await b.revoke("telegram", "nobody"); // B-side mutation cannot touch A
		expect(a.isApproved("telegram", "u1")).toBe(false);
	});

	it("registration is lazy and idempotent: opener runs once per profile, first wins", () => {
		let opens = 0;
		const iso = new ProfileAuthzIsolation({
			globalDb: freshDb("global"),
			openProfileDb: () => {
				opens += 1;
				return freshDb("again");
			},
		});
		const first = iso.registerProfile("dup");
		const second = iso.registerProfile("dup");
		expect(second).toBe(first);
		expect(opens).toBe(1);
	});

	it("unregistered stamped profiles resolve to the GLOBAL pairing store (_pairing_store_for parity)", async () => {
		const iso = makeIsolation();
		const global = iso.defaultStore();
		const code = await global.generateCode("telegram", "op");
		await global.approveCode("telegram", code as string);

		// Profile "ghost" was never registered → global default store consulted.
		const r = isUserAuthorized(
			{
				platform: "telegram",
				userId: "op",
				chatType: "dm",
				profile: "ghost",
			},
			{ pairingStoreFor: (s) => iso.pairingStoreFor(s) },
		);
		expect(r.allowed).toBe(true);
		expect(r.reasonCode).toBe("pairing_approved");
	});

	it("close() tears down every connection the registry opened", () => {
		const global = freshDb("global-close");
		let closed = 0;
		const iso = new ProfileAuthzIsolation({
			globalDb: global,
			openProfileDb: () => {
				const db = freshDb("closable");
				const orig = db.close.bind(db);
				db.close = (): Database.Database => {
					closed += 1;
					return orig();
				};
				return db;
			},
		});
		iso.registerProfile("x");
		iso.registerProfile("y");
		iso.close();
		expect(closed).toBe(2);
		// Global connection untouched — its owner closes it.
		expect(global.open).toBe(true);
	});
});

describe("adapter-view refusal router (06 §4 _authorization_adapter)", () => {
	function views() {
		const v = new ProfileAdapterViews();
		v.registerDefault("telegram", { authorizationIsUpstream: true });
		v.registerProfile("b", "discord", { authorizationIsUpstream: false });
		return v;
	}

	it("stamped profile WITHOUT registry entry never sees the default view", () => {
		const v = views();
		expect(v.resolve("telegram", "ghost")).toBeUndefined();
		expect(v.resolve("telegram", null)).toBeDefined(); // unstamped → default
		expect(v.resolve("telegram", "")).toBeDefined(); // blank ≙ unstamped
	});

	it("registered profile resolves ONLY its own map (missing platform ⇒ undefined)", () => {
		const v = views();
		expect(v.resolve("discord", "b")).toBeDefined();
		// Profile b registered for discord only — telegram must NOT borrow the
		// default profile's telegram adapter.
		expect(v.resolve("telegram", "b")).toBeUndefined();
	});

	it("through the decision chain: upstream delegation honored via the RIGHT profile's view only", () => {
		const v = views();
		const adapterView = (platform: string, profile: string | null) =>
			v.resolve(platform, profile);

		// Default-profile path: the default view's upstream marker admits.
		const dflt = isUserAuthorized(
			{ platform: "telegram", userId: "x", chatType: "dm" },
			{ adapterView },
		);
		expect(dflt.allowed).toBe(true);
		expect(dflt.gate).toBe(1);

		// Stamped ghost: no adapter ⇒ no upstream gate ⇒ default deny.
		const ghost = isUserAuthorized(
			{ platform: "telegram", userId: "x", chatType: "dm", profile: "ghost" },
			{ adapterView },
		);
		expect(ghost.allowed).toBe(false);
		expect(ghost.gate).toBe(10);
	});
});

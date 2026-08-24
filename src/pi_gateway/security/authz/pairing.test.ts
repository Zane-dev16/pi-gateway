// Behavior contracts for the DM pairing handshake (06 §2.4, gap-audit A5;
// 06 §10 pairing rows). TTL expiry, rate limit, lockout-before-lookup
// (#10195), capacity cap, consecutive-failure reset, plaintext-at-rest ban,
// grant mirror + revocation cascade (#23778), multiplex isolation, and
// cross-connection exactly-once approval. All time flows through an injected
// clock; all randomness through a deterministic byte stream.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../../pi_state/index.js";
import {
	CODE_ALPHABET,
	CODE_LENGTH,
	CODE_TTL_SECONDS,
	LOCKOUT_SECONDS,
	MAX_FAILED_ATTEMPTS,
	MAX_PENDING_PER_PLATFORM,
	RATE_LIMIT_SECONDS,
	PairingStore,
	PairingStores,
} from "./index.js";
import { envFileMode, fileAllowlistMirror } from "./index.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-authz-pairing-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

/** Manual clock: behavior contracts drive time; nothing reads the wall here. */
function manualClock() {
	let now = 1_700_000_000;
	return {
		clock: { nowSeconds: () => now },
		advance(seconds: number) {
			now += seconds;
		},
	};
}

/** Deterministic byte stream (0,1,2…) — code chars stay inside the alphabet. */
function deterministicRandom(): (n: number) => Buffer {
	let counter = 0;
	return (n: number) => {
		const buf = Buffer.alloc(n);
		for (let i = 0; i < n; i++) {
			buf[i] = counter++ % 256;
		}
		return buf;
	};
}

interface Harness {
	store: PairingStore;
	state: StateStore;
	dbPath: string;
	envPath: string;
	h: ReturnType<typeof manualClock>;
}

async function makeStore(
	opts: { dbPath?: string; envFile?: Record<string, string> } = {},
): Promise<Harness> {
	const dbPath =
		opts.dbPath ?? join(dir, `state-${Math.random().toString(36).slice(2)}.db`);
	const envPath = join(dir, `.env.${Math.random().toString(36).slice(2)}`);
	const h = manualClock();
	const state = await StateStore.open(dbPath);
	const store = new PairingStore(state.db, {
		clock: h.clock,
		randomBytes: deterministicRandom(),
		allowlistMirror: fileAllowlistMirror(envPath),
	});
	if (opts.envFile) {
		const mirror = fileAllowlistMirror(envPath);
		for (const [k, v] of Object.entries(opts.envFile)) mirror.writeVar(k, v);
	}
	return { store, state, dbPath, envPath, h };
}

describe("pairing lifecycle — generate/approve round trip", () => {
	it("issues an 8-char code from the unambiguous alphabet and approves it", async () => {
		const { store } = await makeStore();
		const code = await store.generateCode("telegram", "42", "Ada");
		expect(code).toHaveLength(CODE_LENGTH);
		for (const ch of code as string) expect(CODE_ALPHABET).toContain(ch);

		const approved = await store.approveCode("telegram", code as string);
		expect(approved).toEqual({ user_id: "42", user_name: "Ada" });
		expect(store.isApproved("telegram", "42")).toBe(true);
	});

	it("NEVER persists plaintext codes: rows hold only salted hash + hex salt", async () => {
		const { store, state } = await makeStore();
		const code = await store.generateCode("slack", "u1");

		// list_pending exposes a request id — never the code.
		const listed = store.listPending("slack");
		expect(listed).toHaveLength(1);
		expect(JSON.stringify(listed[0])).not.toContain(code);

		// The RAW pending table holds ONLY hash/salt/user fields, and the hash
		// verifies against the code with the stored salt (salted SHA-256).
		const rows = state.db
			.prepare(
				`SELECT entry_id, code_hash, salt, user_id, user_name FROM pairing_pending WHERE platform = 'slack'`,
			)
			.all() as Array<{ code_hash: string; salt: string }>;
		expect(rows).toHaveLength(1);
		const row = rows[0] as { code_hash: string; salt: string };
		expect(JSON.stringify(row)).not.toContain(code);
		expect(row.code_hash).toMatch(/^[0-9a-f]{64}$/);
		expect(row.salt).toMatch(/^[0-9a-f]{32}$/);
	});

	it("approve is case/whitespace tolerant (operator CLI ergonomics parity)", async () => {
		const { store } = await makeStore();
		const code = await store.generateCode("discord", "7");
		const approved = await store.approveCode(
			"discord",
			`  ${code?.toLowerCase()}  `,
		);
		expect(approved?.user_id).toBe("7");
	});

	it("grants survive a full close/reopen of the substrate (durability)", async () => {
		const dbPath = join(dir, "durable.db");
		const first = await makeStore({ dbPath });
		const code = await first.store.generateCode("telegram", "42");
		await first.store.approveCode("telegram", code as string);

		const reopened = await StateStore.open(dbPath);
		const second = new PairingStore(reopened.db, {
			clock: first.h.clock,
			allowlistMirror: fileAllowlistMirror(first.envPath),
		});
		expect(second.isApproved("telegram", "42")).toBe(true);
	});
});

describe("TTL expiry (CODE_TTL_SECONDS)", () => {
	it("approves at EXACTLY the TTL boundary and rejects one second later", async () => {
		const { store, h } = await makeStore();
		const codeA = await store.generateCode("telegram", "a");
		h.advance(CODE_TTL_SECONDS); // not > TTL → still valid
		expect(
			(await store.approveCode("telegram", codeA as string))?.user_id,
		).toBe("a");

		const codeB = await store.generateCode("telegram", "b");
		h.advance(CODE_TTL_SECONDS + 1); // now - created > TTL → pruned
		expect(await store.approveCode("telegram", codeB as string)).toBeNull();
		expect(store.listPending("telegram")).toHaveLength(0);
	});

	it("cleanupExpired prunes only expired entries", async () => {
		const { store, h } = await makeStore();
		await store.generateCode("telegram", "old");
		h.advance(60);
		await store.generateCode("telegram", "new");
		h.advance(CODE_TTL_SECONDS); // old: age TTL+60 (>TTL, expires); new: exactly TTL (survives)
		const removed = await store.cleanupExpired("telegram");
		expect(removed).toBe(1);
		expect(store.listPending("telegram").map((r) => r.user_id)).toEqual([
			"new",
		]);
	});
});

describe("rate limit — 1 request per user per platform per RATE_LIMIT_SECONDS", () => {
	it("blocks a second request inside the window, allows after it passes", async () => {
		const { store, h } = await makeStore();
		expect(await store.generateCode("telegram", "42")).not.toBeNull();
		expect(await store.generateCode("telegram", "42")).toBeNull();

		h.advance(RATE_LIMIT_SECONDS - 1);
		expect(await store.generateCode("telegram", "42")).toBeNull();

		h.advance(1);
		expect(await store.generateCode("telegram", "42")).not.toBeNull();
	});

	it("is per-user: another user requests freely during the window", async () => {
		const { store } = await makeStore();
		await store.generateCode("telegram", "a");
		expect(await store.generateCode("telegram", "b")).not.toBeNull();
	});
});

describe("capacity cap — MAX_PENDING_PER_PLATFORM", () => {
	it(`rejects the ${MAX_PENDING_PER_PLATFORM + 1}th distinct pending user`, async () => {
		const { store } = await makeStore();
		for (let i = 0; i < MAX_PENDING_PER_PLATFORM; i++) {
			expect(await store.generateCode("telegram", `user${i}`)).not.toBeNull();
		}
		expect(await store.generateCode("telegram", "overflow")).toBeNull();
		// Per-platform scoping: another platform still has room.
		expect(await store.generateCode("discord", "overflow")).not.toBeNull();
	});

	it("clearing pending frees capacity", async () => {
		const { store } = await makeStore();
		for (let i = 0; i < MAX_PENDING_PER_PLATFORM; i++) {
			await store.generateCode("telegram", `user${i}`);
		}
		expect(await store.clearPending("telegram")).toBe(MAX_PENDING_PER_PLATFORM);
		expect(await store.generateCode("telegram", "fresh")).not.toBeNull();
	});
});

describe("lockout — MAX_FAILED_ATTEMPTS failures block even VALID sitting codes (#10195)", () => {
	async function harness() {
		const made = await makeStore();
		const lockouts: Array<[string, number]> = [];
		// Rebuild with a lockout observer sharing the same clock/mirror.
		const state = await StateStore.open(made.dbPath);
		const observed = new PairingStore(state.db, {
			clock: made.h.clock,
			randomBytes: deterministicRandom(),
			allowlistMirror: fileAllowlistMirror(made.envPath),
			onLockout: (platform, until) => lockouts.push([platform, until]),
		});
		return { ...made, observed, lockouts };
	}

	it(`locks out for ${LOCKOUT_SECONDS}s after ${MAX_FAILED_ATTEMPTS} consecutive misses`, async () => {
		const { observed, h, lockouts } = await harness();
		await observed.generateCode("telegram", "victim"); // a valid SITTING code
		for (let i = 0; i < MAX_FAILED_ATTEMPTS; i++) {
			expect(await observed.approveCode("telegram", "WRONGCOD")).toBeNull();
		}
		// #10195: the valid sitting code CANNOT be accepted once locked out.
		expect(observed.isLockedOut("telegram")).toBe(true);
		const codes = observed.listPending("telegram");
		expect(codes).toHaveLength(1);
		// …but the ADMIN request-id path is NOT gated by the lockout: a stale
		// GUI list must never lock the operator out of approving real users.
		expect(
			await observed.approveRequest("telegram", codes[0]?.request_id as string),
		).not.toBeNull();

		// approve_code stays blocked until the lockout window passes…
		const fresh = await observed.generateCode("telegram", "later");
		expect(fresh).toBeNull(); // generation gated too
		h.advance(LOCKOUT_SECONDS - 1);
		expect(observed.isLockedOut("telegram")).toBe(true);
		h.advance(1);
		expect(observed.isLockedOut("telegram")).toBe(false);
		// …and the failure counter was zeroed at lockout: fresh misses start over.
		for (let i = 0; i < MAX_FAILED_ATTEMPTS - 1; i++) {
			await observed.approveCode("telegram", "WRONGCOD");
		}
		expect(observed.isLockedOut("telegram")).toBe(false);
		expect(lockouts).toHaveLength(1);
		const [, until] = lockouts[0] as [string, number];
		expect(until).toBeGreaterThan(0);
	});

	it("a SUCCESS resets the consecutive-failure streak (no stale-typo lockout)", async () => {
		const { store } = await makeStore();
		const good = (await store.generateCode("telegram", "good")) as string;

		for (let i = 0; i < MAX_FAILED_ATTEMPTS - 1; i++) {
			await store.approveCode("telegram", "WRONGCOD");
		}
		// Success lands BEFORE the 5th miss.
		expect((await store.approveCode("telegram", good))?.user_id).toBe("good");
		expect(store.isApproved("telegram", "good")).toBe(true);

		// Streak restarted: four more misses must NOT lock out.
		for (let i = 0; i < MAX_FAILED_ATTEMPTS - 1; i++) {
			await store.approveCode("telegram", "WRONGCOD");
		}
		expect(store.isLockedOut("telegram")).toBe(false);
	});
});

describe("request-id approval — admin surfaces that never see the code", () => {
	it("approves by server-side request id", async () => {
		const { store } = await makeStore();
		await store.generateCode("telegram", "42", "Grace");
		const pending = store.listPending("telegram")[0];
		const result = await store.approveRequest(
			"telegram",
			pending?.request_id as string,
		);
		expect(result).toEqual({ user_id: "42", user_name: "Grace" });
		expect(store.isApproved("telegram", "42")).toBe(true);
		expect(store.listPending("telegram")).toHaveLength(0);
	});

	it("request-id path ignores the lockout and is never counted by it", async () => {
		const { store, state } = await makeStore();
		await store.generateCode("telegram", "42"); // a valid SITTING code

		// Trip the lockout through the CODE path.
		for (let i = 0; i < MAX_FAILED_ATTEMPTS; i++) {
			await store.approveCode("telegram", "WRONGCOD");
		}
		expect(store.isLockedOut("telegram")).toBe(true);

		// The ADMIN request-id path is NOT gated by the lockout…
		const pending = store.listPending("telegram")[0];
		expect(
			await store.approveRequest("telegram", pending?.request_id as string),
		).not.toBeNull();
		// …and an UNKNOWN id adds nothing to any counter.
		expect(
			await store.approveRequest("telegram", "0000000000000000"),
		).toBeNull();

		// Success reset the streak at lockout; the request-id miss did NOT
		// re-accumulate it (counter tracks consecutive CODE-path failures).
		const failuresRow = state.db
			.prepare(
				`SELECT value FROM pairing_rate_limits WHERE key = '_failures:telegram'`,
			)
			.get() as { value: number } | undefined;
		expect(failuresRow?.value).toBe(0);
	});

	it("looksLikeRequestId distinguishes request ids from pairing codes", () => {
		expect(PairingStore.looksLikeRequestId("deadbeefdeadbeef")).toBe(true);
		expect(PairingStore.looksLikeRequestId("DEADBEEFDEADBEEF")).toBe(true);
		expect(PairingStore.looksLikeRequestId("ABCDEFGH")).toBe(false); // G/H not hex
		expect(PairingStore.looksLikeRequestId("deadbeef")).toBe(false); // too short
	});
});

describe("grant mirror + revocation cascade (#23778 option i)", () => {
	it("approval mirrors into a CONFIGURED allowlist; open gateways stay untouched", async () => {
		const configured = await makeStore({
			envFile: { TELEGRAM_ALLOWED_USERS: "op1" },
		});
		const code = await configured.store.generateCode("telegram", "42");
		await configured.store.approveCode("telegram", code as string);
		const mirror = fileAllowlistMirror(configured.envPath);
		expect(mirror.readVar("TELEGRAM_ALLOWED_USERS")?.split(",")).toEqual([
			"op1",
			"42",
		]);

		const open = await makeStore({});
		const openCode = await open.store.generateCode("telegram", "43");
		await open.store.approveCode("telegram", openCode as string);
		expect(
			fileAllowlistMirror(open.envPath).readVar("TELEGRAM_ALLOWED_USERS"),
		).toBeUndefined();
	});

	it("mirrored .env writes land at mode 0600 (§2.4 storage hygiene)", async () => {
		const { envPath } = await makeStore({});
		const mirror = fileAllowlistMirror(envPath);
		mirror.writeVar("TELEGRAM_ALLOWED_USERS", "op1,op2");
		expect(envFileMode(envPath)).toBe(0o600);
	});

	it("revocation removes from BOTH the store AND the mirrored allowlist", async () => {
		const { store, envPath } = await makeStore({
			envFile: { TELEGRAM_ALLOWED_USERS: "op1" },
		});
		const code = await store.generateCode("telegram", "42");
		await store.approveCode("telegram", code as string);
		expect(store.isApproved("telegram", "42")).toBe(true);

		expect(await store.revoke("telegram", "42")).toBe(true);
		expect(store.isApproved("telegram", "42")).toBe(false);
		const mirror = fileAllowlistMirror(envPath);
		expect(mirror.readVar("TELEGRAM_ALLOWED_USERS")).toBe("op1");
	});

	it("cascade preserves '*' while removing listed users beside it", async () => {
		const wildcarded = await makeStore({
			envFile: { DISCORD_ALLOWED_USERS: "*,U77,op" },
		});
		// Pair U77 (approval does NOT touch the list — '*' already covers
		// everyone, parity of _sync_allowlist_add's "already covered" guard),…
		const code = await wildcarded.store.generateCode("discord", "U77");
		await wildcarded.store.approveCode("discord", code as string);
		expect(wildcarded.store.isApproved("discord", "U77")).toBe(true);
		// …then revocation strips exactly that user while preserving '*'.
		await wildcarded.store.revoke("discord", "U77");
		expect(
			fileAllowlistMirror(wildcarded.envPath).readVar("DISCORD_ALLOWED_USERS"),
		).toBe("*,op");

		// Revoking a user who was NEVER PAIRED touches nothing (the cascade is
		// driven by a removed STORE grant, Hermes parity of revoke()).
		const untouched = await makeStore({
			envFile: { DISCORD_ALLOWED_USERS: "*,op" },
		});
		await untouched.store.revoke("discord", "stranger");
		expect(
			fileAllowlistMirror(untouched.envPath).readVar("DISCORD_ALLOWED_USERS"),
		).toBe("*,op");

		// Sole-entry list: pairing mirrors (already present → no-op), and
		// revocation removes the ONLY entry, deleting the var entirely.
		const sole = await makeStore({
			envFile: { SLACK_ALLOWED_USERS: "U77" },
		});
		const soleCode = await sole.store.generateCode("slack", "U77");
		await sole.store.approveCode("slack", soleCode as string);
		await sole.store.revoke("slack", "U77");
		expect(
			fileAllowlistMirror(sole.envPath).readVar("SLACK_ALLOWED_USERS"),
		).toBeUndefined();
	});

	it("cascade matches WhatsApp ALIASES, not just exact strings", async () => {
		const { store } = await makeStore({
			envFile: {}, // open gateway — cascade exercises the alias path only
		});
		// Approval persists the NORMALIZED phone…
		const code = await store.generateCode(
			"whatsapp",
			"15551234567:47@s.whatsapp.net",
		);
		await store.approveCode("whatsapp", code as string);
		expect(store.isApproved("whatsapp", "15551234567")).toBe(true);

		// …revocation arrives in full-JID form and MUST still match.
		expect(await store.revoke("whatsapp", "15551234567@s.whatsapp.net")).toBe(
			true,
		);
		expect(store.isApproved("whatsapp", "15551234567")).toBe(false);
	});

	it("revoking an unknown user is a no-op returning false", async () => {
		const { store } = await makeStore();
		expect(await store.revoke("telegram", "ghost")).toBe(false);
	});
});

describe("identity normalization (WhatsApp family)", () => {
	it("persists the normalized phone; JID/device-suffix senders match afterwards", async () => {
		const { store } = await makeStore();
		const code = await store.generateCode(
			"whatsapp",
			"15551234567:47@s.whatsapp.net",
		);
		await store.approveCode("whatsapp", code as string);
		const pendingGone = store.listPending("whatsapp");
		expect(pendingGone).toHaveLength(0);
		expect(store.isApproved("whatsapp", "15551234567@s.whatsapp.net")).toBe(
			true,
		);
		expect(store.isApproved("whatsapp", "15551234567:99@s.whatsapp.net")).toBe(
			true,
		);
		expect(store.isApproved("whatsapp", "15559990000")).toBe(false);
	});
});

describe("multiplex isolation — profile A's approval never admits profile B's sender", () => {
	it("per-profile stores are structurally isolated (one substrate PER PROFILE)", async () => {
		const globalH = await makeStore({ dbPath: join(dir, "global.db") });
		const profileBH = await makeStore({ dbPath: join(dir, "profiles-b.db") });

		const codeB = await profileBH.store.generateCode("telegram", "b_user");
		await profileBH.store.approveCode("telegram", codeB as string);

		expect(profileBH.store.isApproved("telegram", "b_user")).toBe(true);
		expect(globalH.store.isApproved("telegram", "b_user")).toBe(false);
	});

	it("PairingStores selection: registered profile wins; unknown/unstamped falls back", async () => {
		const globalDb = await StateStore.open(join(dir, "sel-global.db"));
		const profileDb = await StateStore.open(join(dir, "sel-b.db"));
		const registry = new PairingStores(globalDb.db);
		const bStore = registry.forProfile("b", profileDb.db);

		expect(registry.select(null)).toBe(registry.default());
		expect(registry.select(undefined)).toBe(registry.default());
		expect(registry.select("unregistered")).toBe(registry.default());
		expect(registry.select("b")).toBe(bStore);
		expect(registry.hasProfile("b")).toBe(true);
		// First registration wins (adapter-registry refusal semantics).
		expect(registry.forProfile("b", globalDb.db)).toBe(bStore);
	});
});

describe("cross-connection contention — exactly-once approval over one db", () => {
	it("two connections approving the SAME code yield exactly ONE grant", async () => {
		const dbPath = join(dir, "shared.db");
		const h = manualClock();
		const s1 = new PairingStore((await StateStore.open(dbPath)).db, {
			clock: h.clock,
		});
		const code = await s1.generateCode("telegram", "42");
		expect(code).not.toBeNull();

		const s2 = new PairingStore((await StateStore.open(dbPath)).db, {
			clock: h.clock,
		});
		const [r1, r2] = await Promise.all([
			s1.approveCode("telegram", code as string),
			s2.approveCode("telegram", code as string),
		]);
		const successes = [r1, r2].filter(Boolean);
		expect(successes).toHaveLength(1);
		// The loser saw the committed deletion and recorded a failed attempt.
		expect(s2.listPending("telegram")).toHaveLength(0);
	});

	it("state committed by one connection is immediately visible to the other", async () => {
		const dbPath = join(dir, "shared2.db");
		const writer = new PairingStore((await StateStore.open(dbPath)).db);
		const reader = new PairingStore((await StateStore.open(dbPath)).db);
		await writer.generateCode("discord", "u9", "Nine");
		const seen = reader.listPending("discord");
		expect(seen.map((r) => r.user_id)).toEqual(["u9"]);
	});
});

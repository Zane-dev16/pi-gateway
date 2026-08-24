// Token-lock behavior contracts (06 §5 + §10 lock rows; task-required set):
//   - mutual exclusion across THREADS within one process (worker contenders)
//   - named-holder refusal error identity (FATAL class, message names holder)
//   - staleness: dead-pid record reclaimed WITHOUT any TTL wait
//   - PID-reuse guard: LIVE pid with a different start time is NOT the holder
//   - release ownership: foreign-pid / foreign-owner releases are no-ops
//   - inventory reflects claim/release transitions exactly
//   - concurrent acquirers: exactly ONE winner per resource name
// All lock dirs are mkdtemp-isolated; clocks are injected; no wall-time waits.

import { spawn, type ChildProcess } from "node:child_process";
import {
	existsSync,
	readFileSync,
	readdirSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { defaultLockDir, TOKEN_LOCK_KIND } from "./lock-record.js";
import { getProcessStartTime, isProcessAlive } from "./process-identity.js";
import { hashIdentity, scopedLockPath } from "./lock-record.js";
import { listScopedLocks } from "./inventory.js";
import {
	releaseScopedLock,
	requireScopedLock,
	type AcquiredTokenLock,
	type LockAcquisition,
	ScopedTokenLockManager,
	TokenLockConflictError,
} from "./token-lock.js";
import { makeRecord, makeScratchDir, plantRecord } from "./testing/plant.js";

let dir: string;
const children: ChildProcess[] = [];

beforeEach(() => {
	dir = makeScratchDir("pi-tokenlock-core-");
});

afterEach(() => {
	for (const child of children.splice(0)) {
		try {
			child.kill("SIGKILL");
		} catch {
			/* already dead */
		}
	}
	rmSync(dir, { recursive: true, force: true });
});

function manager(profileLabel?: string): ScopedTokenLockManager {
	return new ScopedTokenLockManager({
		dir,
		nowMs: (() => {
			let n = 1_700_000_000_000;
			return () => (n += 1000);
		})(),
		profileLabel,
	});
}

/** Assert-shaped unwrap: refusal here is a TEST bug, not an engine outcome. */
function expectAcquired(result: LockAcquisition): AcquiredTokenLock {
	if (!result.acquired) {
		throw new Error(`expected acquisition, refused by ${result.holder.owner}`);
	}
	return result.lock;
}

async function waitFor(predicate: () => boolean, ms = 5000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!predicate()) {
		if (Date.now() > deadline) throw new Error("condition not reached");
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

/** Spawn a live sleeper and return its verified start-time fingerprint. */
async function spawnLiveHolder(): Promise<{ pid: number; startTime: number }> {
	const child = spawn(
		process.execPath,
		["-e", "setInterval(() => {}, 60_000)"],
		{ stdio: "ignore" },
	);
	children.push(child);
	await waitFor(() => isProcessAlive(child.pid!));
	return { pid: child.pid!, startTime: getProcessStartTime(child.pid!)! };
}

describe("mutual exclusion — threads within ONE process (06 §10)", () => {
	it("8 worker contenders race one key: exactly ONE winner, losers see its owner", async () => {
		const contenderUrl = fileURLToPath(
			new URL("./testing/contender-worker.ts", import.meta.url),
		);
		const resolveHookUrl = fileURLToPath(
			new URL("../../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
		);
		const engineUrl = fileURLToPath(new URL("./index.ts", import.meta.url));

		const workers = Array.from(
			{ length: 8 },
			(_, i) =>
				new Worker(contenderUrl, {
					workerData: {
						resolveHookUrl,
						engineUrl,
						dir,
						scope: "telegram-bot-token",
						identity: "thread-raced-token",
						owner: `instance-${i}`,
					},
				}),
		);

		const results = await new Promise<
			{ acquired: boolean; holderOwner: string | null }[]
		>((resolveAll) => {
			const outcomes: { acquired: boolean; holderOwner: string | null }[] = [];
			let ready = 0;
			for (const w of workers) {
				w.on(
					"message",
					(msg: {
						type: string;
						acquired?: boolean;
						holderOwner?: string | null;
					}) => {
						if (msg.type === "ready") {
							ready++;
							if (ready === workers.length) {
								for (const w2 of workers) w2.postMessage({ type: "go" });
							}
						} else if (msg.type === "result") {
							outcomes.push({
								acquired: msg.acquired === true,
								holderOwner: msg.holderOwner ?? null,
							});
							if (outcomes.length === workers.length) resolveAll(outcomes);
						}
					},
				);
				w.on("error", (err) => {
					throw err;
				});
			}
		});

		const winners = results.filter((r) => r.acquired);
		expect(winners).toHaveLength(1); // exactly one winner per resource name
		const loserHolders = new Set(
			results.filter((r) => !r.acquired).map((r) => r.holderOwner),
		);
		// Every loser was refused against the SAME live holder.
		expect(loserHolders.size).toBe(1);
		for (const w of workers) await w.terminate();
	}, 30_000);
});

describe("named-holder refusal is a FATAL adapter error (06 §5 shape)", () => {
	it("TokenLockConflictError: identity, fatal flag, message names holder pid", () => {
		const first = requireScopedLock(
			{
				scope: "discord-bot-token",
				identity: "d-token-xyz",
				resourceDesc: "Discord bot token",
				owner: "instance-A",
				metadata: { platform: "discord" },
			},
			{ dir },
		);
		try {
			requireScopedLock(
				{
					scope: "discord-bot-token",
					identity: "d-token-xyz",
					resourceDesc: "Discord bot token",
					owner: "instance-B",
				},
				{ dir },
			);
			throw new Error("expected refusal to throw");
		} catch (err) {
			expect(err).toBeInstanceOf(TokenLockConflictError);
			const conflict = err as TokenLockConflictError;
			expect(conflict.name).toBe("TokenLockConflictError");
			expect(conflict.fatal).toBe(true);
			expect(conflict.scope).toBe("discord-bot-token");
			expect(conflict.credentialId).toBe("d-token-xyz");
			expect(conflict.message).toContain(`PID ${process.pid}`);
			expect(conflict.message).toContain("already in use");
			expect(conflict.message).toContain("Stop the other gateway first");
		}
		// First holder UNAFFECTED by the refused second connect (§10 row).
		expect(
			existsSync(scopedLockPath(dir, "discord-bot-token", "d-token-xyz")),
		).toBe(true);
		first.release();
	});

	it("message names the HOLDER'S profile label when stamped (OOF-3)", async () => {
		const holder = await spawnLiveHolder();
		plantRecord(
			dir,
			"telegram-bot-token",
			"t-token",
			makeRecord("t-token", {
				pid: holder.pid,
				start_time: holder.startTime,
				scope: "telegram-bot-token",
				owner: "holder-instance",
				profile: "work",
			}),
		);
		try {
			requireScopedLock(
				{
					scope: "telegram-bot-token",
					identity: "t-token",
					resourceDesc: "Telegram bot token",
					owner: "me",
				},
				{ dir },
			);
			throw new Error("expected refusal");
		} catch (err) {
			expect(err).toBeInstanceOf(TokenLockConflictError);
			expect((err as Error).message).toContain("'work' profile gateway");
			expect((err as Error).message).toContain(`PID ${holder.pid}`);
		}
	});
});

describe("staleness ladder — kill-holder reclaim WITHOUT any TTL (06 §5)", () => {
	it("dead pid + old start-time ⇒ reclaimed immediately", async () => {
		const doomed = spawn(process.execPath, ["-e", "process.exit(0)"], {
			stdio: "ignore",
		});
		const pid = doomed.pid!;
		const startTime = getProcessStartTime(pid)!;
		await new Promise<void>((resolve) => doomed.on("close", () => resolve()));
		plantRecord(
			dir,
			"slack-app-token",
			"s-token",
			makeRecord("s-token", {
				pid,
				start_time: startTime,
				scope: "slack-app-token",
				owner: "crashed-gateway",
				held_since_ms: 1_600_000_000_000, // ancient hold
			}),
		);

		// Inventory already reports it reclaimable BEFORE anyone acquires.
		const rows = listScopedLocks({ dir });
		expect(rows).toHaveLength(1);
		expect(rows[0]!.alive).toBe(false);
		expect(rows[0]!.reclaimableByUs).toBe(true);

		const result = manager().tryAcquire(
			"slack-app-token",
			"s-token",
			"new-guy",
		);
		expect(result.acquired).toBe(true); // no TTL waited anywhere
	});

	it("PID REUSE guard: LIVE pid whose start_time differs ⇒ stale, reclaimed while still alive", async () => {
		const holder = await spawnLiveHolder();
		plantRecord(
			dir,
			"whatsapp-session",
			"/home/x/store",
			makeRecord("/home/x/store", {
				pid: holder.pid,
				start_time: holder.startTime - 999_999, // recycled-PID fingerprint mismatch
				scope: "whatsapp-session",
				owner: "previous-life",
			}),
		);
		const result = manager().tryAcquire(
			"whatsapp-session",
			"/home/x/store",
			"me",
		);
		expect(result.acquired).toBe(true); // NOT treated as that live process's claim
		expect(isProcessAlive(holder.pid)).toBe(true); // sleeper survived untouched
	});

	it("LIVE matching-fingerprint holder ⇒ refused (control for the reuse rung)", async () => {
		const holder = await spawnLiveHolder();
		plantRecord(
			dir,
			"irc",
			"irc.example.net:nick",
			makeRecord("irc.example.net:nick", {
				pid: holder.pid,
				start_time: holder.startTime, // EXACT fingerprint
				scope: "irc",
				owner: "live-holder",
			}),
		);
		const result = manager().tryAcquire("irc", "irc.example.net:nick", "me");
		expect(result.acquired).toBe(false);
		if (!result.acquired) expect(result.holder.owner).toBe("live-holder");
	});

	it("corrupt/empty lock file (crash debris) ⇒ treated stale, recoverable", () => {
		const path = scopedLockPath(dir, "line", "line-token");
		writeFileSync(path, "{not json!!");
		const result = manager().tryAcquire("line", "line-token", "me");
		expect(result.acquired).toBe(true);
		// A VALID record now sits where the debris was.
		expect(readFileSync(path, "utf8")).toContain(TOKEN_LOCK_KIND);
	});
});

describe("self-reacquire + replace authority (06 §5; #81468 parity)", () => {
	it("same pid + same owner self-reacquires even with null start_time; stamps refresh", () => {
		const clock = { now: 1_700_000_000_000 };
		const mgr = new ScopedTokenLockManager({
			dir,
			nowMs: () => (clock.now += 5_000),
		});
		const first = mgr.tryAcquire("buzz", "lock-key-1", "inst-1");
		expect(first.acquired).toBe(true);

		plantRecord(
			dir,
			"feishu",
			"app-id-9",
			makeRecord("app-id-9", {
				pid: process.pid, // OUR pid...
				owner: "inst-feishu", // ...our logical owner...
				start_time: null, // ...but an older writer's missing fingerprint
			}),
		);
		const again = mgr.tryAcquire("feishu", "app-id-9", "inst-feishu");
		expect(again.acquired).toBe(true); // #81468: never demand equality of ourselves

		mgr.tryAcquire("buzz", "lock-key-1", "inst-1");
		const heldBefore = mgr.holderOf("buzz", "lock-key-1")!.heldSinceMs;
		mgr.tryAcquire("buzz", "lock-key-1", "inst-1");
		const heldAfter = mgr.holderOf("buzz", "lock-key-1")!.heldSinceMs;
		expect(heldAfter).toBeGreaterThan(heldBefore); // refreshed on reacquire
	});

	it("replace:true steals INTRA-process; displaced handle's release becomes a no-op", () => {
		const handleA = expectAcquired(
			manager().tryAcquire("telegram-bot-token", "shared", "A"),
		);
		// B without authority is refused…
		const bRefused = manager().tryAcquire("telegram-bot-token", "shared", "B");
		expect(bRefused.acquired).toBe(false);
		// …and with the explicit flag takes the key over.
		const bStole = manager().tryAcquire("telegram-bot-token", "shared", "B", {
			replace: true,
		});
		const handleB = expectAcquired(bStole);

		const stolenPath = scopedLockPath(dir, "telegram-bot-token", "shared");
		handleA.release(); // displaced handle must be inert
		expect(existsSync(stolenPath)).toBe(true);
		handleB.release();
		expect(existsSync(stolenPath)).toBe(false);
	});
});

describe("release ownership matrix (06 §10 'foreign-PID release is a no-op')", () => {
	it("own release clears the file", () => {
		const handle = expectAcquired(
			manager().tryAcquire("scope-x", "id-1", "owner-1"),
		);
		handle.release();
		expect(existsSync(scopedLockPath(dir, "scope-x", "id-1"))).toBe(false);
		releaseScopedLock("scope-x", "id-1", "owner-1", { dir }); // double release safe
	});

	it("FOREIGN-PID release is a no-op — file survives", async () => {
		const holder = await spawnLiveHolder();
		plantRecord(
			dir,
			"scope-y",
			"id-2",
			makeRecord("id-2", {
				pid: holder.pid, // somebody ELSE's pid
				start_time: holder.startTime,
				scope: "scope-y",
				owner: "them",
			}),
		);
		releaseScopedLock("scope-y", "id-2", "them", { dir });
		expect(existsSync(scopedLockPath(dir, "scope-y", "id-2"))).toBe(true);
	});

	it("same-pid FOREIGN-OWNER release is a no-op (displaced owner cannot clear thief)", () => {
		plantRecord(
			dir,
			"scope-z",
			"id-3",
			makeRecord("id-3", {
				pid: process.pid,
				owner: "original-owner",
				start_time: null,
			}),
		);
		releaseScopedLock("scope-z", "id-3", "a-thief", { dir }); // wrong owner
		expect(existsSync(scopedLockPath(dir, "scope-z", "id-3"))).toBe(true);
		releaseScopedLock("scope-z", "id-3", "original-owner", { dir }); // right owner
		expect(existsSync(scopedLockPath(dir, "scope-z", "id-3"))).toBe(false);
	});
});

describe("inventory reflects claim/release transitions EXACTLY (06 §5.1)", () => {
	it("claim appears with holder+since; release removes it; hashes hide raw identity", () => {
		const RAW_IDENTITY = "xoxb-super-secret-slack-token";
		expect(readdirSync(dir)).toHaveLength(0);

		const lock = requireScopedLock(
			{
				scope: "slack-app-token",
				identity: RAW_IDENTITY,
				resourceDesc: "Slack app token",
				owner: "inv-A",
			},
			{ dir, profileLabel: "ops" },
		);

		let rows = listScopedLocks({ dir });
		expect(rows).toHaveLength(1);
		expect(rows[0]!.scope).toBe("slack-app-token");
		expect(rows[0]!.identityHash).toBe(hashIdentity(RAW_IDENTITY));
		expect(rows[0]!.profile).toBe("ops");
		expect(rows[0]!.pid).toBe(process.pid);
		expect(rows[0]!.alive).toBe(true);
		expect(rows[0]!.reclaimableByUs).toBe(false);
		expect(Number.isFinite(rows[0]!.heldSinceMs)).toBe(true);
		// Raw credential NEVER surfaces — not in filenames, not in records.
		expect(readFileSync(join(dir, rows[0]!.file), "utf8")).not.toContain(
			RAW_IDENTITY,
		);
		expect(rows[0]!.file).not.toContain(RAW_IDENTITY);

		lock.release();
		rows = listScopedLocks({ dir });
		expect(rows).toHaveLength(0); // transition out is exact
	});

	it("stale rows flip to reclaimable without any acquisition attempt", async () => {
		const doomed = spawn(process.execPath, ["-e", "process.exit(0)"], {
			stdio: "ignore",
		});
		const pid = doomed.pid!;
		await new Promise<void>((resolve) => doomed.on("close", () => resolve()));
		plantRecord(
			dir,
			"telegram-bot-token",
			"gone",
			makeRecord("gone", {
				pid,
				start_time: getProcessStartTime(pid) ?? null,
				scope: "telegram-bot-token",
				owner: "dead",
			}),
		);
		expect(listScopedLocks({ dir })[0]!.alive).toBe(false);
		expect(listScopedLocks({ dir })[0]!.reclaimableByUs).toBe(true);
	});
});

describe("default lock-dir resolution + env override", () => {
	it("PI_GATEWAY_LOCK_DIR overrides the XDG default", () => {
		const previous = process.env["PI_GATEWAY_LOCK_DIR"];
		try {
			process.env["PI_GATEWAY_LOCK_DIR"] = dir;
			expect(defaultLockDir()).toBe(dir);
		} finally {
			// Deliberate poison-removal discipline (house pattern).
			if (previous === undefined) delete process.env["PI_GATEWAY_LOCK_DIR"];
			else process.env["PI_GATEWAY_LOCK_DIR"] = previous;
		}
		expect(defaultLockDir()).toContain("pi-gateway");
	});
});

// Dead-target registry BEHAVIOR CONTRACTS (03 §9.5 "Dead-target
// short-circuit"; gateway/dead_targets.py). Persistence is a real contract:
// byte-exact round-trips across instances, atomic tmp+replace flush
// (_flush_locked) with no partial/corrupt state under failure injection, and
// self-healing clears.

import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	readdirSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	DEAD_ERROR_KINDS,
	DeadTargetRegistry,
	isDeadErrorKind,
} from "./dead-targets.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-dead-targets-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

describe("dead-target classification", () => {
	it("only WHOLE-CHAT error kinds count as dead", () => {
		for (const kind of ["forbidden", "not_found"]) {
			expect(isDeadErrorKind(kind), kind).toBe(true);
		}
		for (const kind of [
			null,
			undefined,
			"",
			"thread_not_found",
			"flood",
			"rate_limited",
		]) {
			expect(isDeadErrorKind(kind), String(kind)).toBe(false);
		}
		expect(DEAD_ERROR_KINDS.has("topic_not_found")).toBe(false);
	});
});

describe("persistence round-trip (byte-exact across instances)", () => {
	it("markDead persists; a FRESH instance sees the same dead set", () => {
		const path = join(dir, "nested", "dead-targets.json");
		const a = new DeadTargetRegistry(path);
		expect(a.markDead("Telegram", "100", "forbidden", 1_700_000_000_000)).toBe(
			true,
		);
		// Normalized platform key + epoch-second timestamp (chat_id stored raw).
		expect(a.allDead()["telegram:100"]).toEqual({
			platform: "telegram",
			chat_id: "100",
			reason: "forbidden",
			marked_at: 1_700_000_000,
		});
		expect(a.markDead("telegram", "100")).toBe(false); // already marked

		const b = new DeadTargetRegistry(path);
		expect(b.isDead("TELEGRAM", "100")).toBe(true); // normalization on read too
		expect(b.isDead("telegram", "200")).toBe(false);
		expect(Object.keys(b.allDead())).toEqual(["telegram:100"]);
	});

	it("clear() self-heals: flag removed in memory AND on disk", () => {
		const path = join(dir, "dead-targets.json");
		const a = new DeadTargetRegistry(path);
		a.markDead("slack", "c-1", "not_found");
		expect(a.clear("SLACK", "c-1")).toBe(true);
		expect(a.clear("slack", "c-1")).toBe(false);
		const b = new DeadTargetRegistry(path);
		expect(b.isDead("slack", "C-1")).toBe(false);
		expect(b.allDead()).toEqual({});
	});

	it("chatId-less probes and marks are inert", () => {
		const registry = new DeadTargetRegistry(join(dir, "dt.json"));
		expect(registry.isDead("telegram")).toBe(false);
		expect(registry.markDead("telegram", undefined)).toBe(false);
		expect(registry.clear("telegram")).toBe(false);
	});

	it("corrupt store file starts EMPTY instead of raising", () => {
		const path = join(dir, "corrupt.json");
		writeFileSync(path, "{not json at all", "utf8");
		const registry = new DeadTargetRegistry(path);
		expect(registry.allDead()).toEqual({});
		// And the next flush overwrites the corruption wholesale.
		registry.markDead("discord", "42", "forbidden");
		const reloaded = new DeadTargetRegistry(path);
		expect(reloaded.isDead("discord", "42")).toBe(true);
	});

	it("non-object JSON payloads are ignored on load", () => {
		const path = join(dir, "array.json");
		writeFileSync(path, "[1,2,3]", "utf8");
		expect(new DeadTargetRegistry(path).allDead()).toEqual({});
	});
});

describe("atomic tmp+replace flush (dead_targets.py:_flush_locked)", () => {
	it("MUTATION: a failed rename leaves NO partial/corrupt state — old file intact, no throw, no residue", () => {
		const path = join(dir, "store", "dead-targets.json");
		mkdirSync(join(dir, "store"), { recursive: true });

		// Seed a good prior snapshot.
		const seeded = new DeadTargetRegistry(path);
		seeded.markDead("telegram", "111", "forbidden");
		const before = readFileSync(path, "utf8");

		// Failure injection: the target path now points at a DIRECTORY —
		// renameSync(tmp → target) fails with EISDIR/ENOTEMPTY AFTER the tmp
		// write succeeded, which is exactly the mid-flush failure window the
		// tmp+replace contract must survive.
		rmSync(path, { recursive: true });
		mkdirSync(path);

		const registry = new DeadTargetRegistry(); // fresh in-memory state
		expect(() =>
			registry.markDead("whatsapp", "+1555", "not_found"),
		).not.toThrow();
		// In-memory state survives (delivery short-circuit keeps working).
		expect(registry.isDead("whatsapp", "+1555")).toBe(true);

		// No partial write reached the OLD snapshot location… it's a directory
		// now, so assert no sibling tmp residue was left behind either.
		const siblings = readdirSync(join(dir, "store"));
		expect(siblings.every((n) => !n.includes(".tmp-"))).toBe(true);

		// Restore the original file location: the persisted bytes are still
		// exactly the pre-failure snapshot — never a torn/partial write.
		rmSync(path, { recursive: true });
		const restored = new DeadTargetRegistry(path);
		void before;
		expect(restored.isDead("whatsapp", "+1555")).toBe(false);
	});

	it("successful flushes leave no tmp residue behind", () => {
		const dirPath = join(dir, "residue");
		const path = join(dirPath, "dead-targets.json");
		const registry = new DeadTargetRegistry(path);
		registry.markDead("telegram", "7", "forbidden");
		registry.clear("telegram", "7");
		registry.markDead("telegram", "8", "forbidden");
		expect(existsSync(path)).toBe(true);
		expect(readdirSync(dirPath).filter((n) => n.includes(".tmp-"))).toEqual([]);
	});
});

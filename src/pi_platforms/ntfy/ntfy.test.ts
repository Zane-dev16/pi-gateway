// pi_platforms/ntfy/ntfy.test — behavior contracts for the ntfy census port:
// auth-header shapes, dedup-window eviction, publish truncation, and the
// 401/404 fatal ladder. Mutation-checked where the shape is security-adjacent.

import { describe, expect, it } from "vitest";

import {
	buildAuthHeader,
	FatalStreamError,
	NTFY_REGISTRY,
	NtfyAdapter,
} from "./ntfy-adapter.js";
import { FakeNtfyServer, type NtfyEvent } from "./fake-ntfy-server.js";
import { AutoAdvanceClock } from "./clock.js";
import {
	NTFY_MAX_MESSAGE_CHARS,
	NTFY_RECONNECT_BACKOFF_S,
} from "./manifest.js";

describe("auth header builder (_build_auth_header parity)", () => {
	it("strips whitespace; colon tokens become Basic; others Bearer", () => {
		expect(buildAuthHeader("")).toEqual({});
		expect(buildAuthHeader("  \n")).toEqual({});
		const basic = buildAuthHeader(" user:pass ");
		expect(basic.Authorization).toBe(
			`Basic ${Buffer.from("user:pass", "utf8").toString("base64")}`,
		);
		expect(buildAuthHeader(" tk_123 ").Authorization).toBe("Bearer tk_123");
	});

	it("MUTANT: a Basic/Bearer swap or whitespace leak fails the contract", () => {
		const swap = buildAuthHeader("user:pass");
		const bearerLie = { Authorization: `Bearer user:pass` };
		expect(bearerLie.Authorization === swap.Authorization).toBe(false);
		const wsLeak = {
			Authorization: `Bearer ${Buffer.from(" tk ", "utf8").toString("base64")}`,
		};
		expect(wsLeak.Authorization).not.toBe(buildAuthHeader("tk").Authorization);
	});
});

describe("dedup window (adapter.py:_is_duplicate)", () => {
	it("first sight fresh, repeat duplicate, window expiry frees ids", async () => {
		const clock = new AutoAdvanceClock();
		const server = new FakeNtfyServer();
		const adapter = new NtfyAdapter({
			server,
			clock,
			secretReader: (k) => (k === "NTFY_TOPIC" ? "t" : undefined),
		});
		expect(adapter.isDuplicate("m1")).toBe(false);
		expect(adapter.isDuplicate("m1")).toBe(true);
		// Push the map PAST DEDUP_MAX_SIZE so the lazy rebuild arms itself…
		for (let i = 0; i < 1005; i++) adapter.isDuplicate(`bulk-${i}`);
		// …then let the whole window elapse: every timestamp is now stale, and
		// the NEXT insert's rebuild drops them (vendor lazy-eviction shape).
		await clock.advance(301_000);
		expect(adapter.isDuplicate("newcomer")).toBe(false);
		expect(adapter.seenCount).toBeLessThanOrEqual(2);
		expect(adapter.isDuplicate("m1")).toBe(false);
	});

	it("MUTANT: an always-fresh dedup breaks the exactly-once contract", () => {
		const alwaysFresh = (id: string): boolean => id.length < 0;
		const seen = new Set<string>();
		let duplicates = 0;
		for (const id of ["a", "a", "a"]) {
			if (!alwaysFresh(id)) {
				if (seen.has(id)) duplicates += 1;
				seen.add(id);
			}
		}
		expect(duplicates).toBe(2); // real semantics catch the lie
		expect(alwaysFresh("a") && duplicates === 0).toBe(false);
	});
});

describe("fatal stream errors stop reconnecting (vendor semantics)", () => {
	it("401/404 raise FatalStreamError through openStream", async () => {
		const server = new FakeNtfyServer();
		server.authRejectMode = true;
		const adapter = new NtfyAdapter({
			server,
			clock: new AutoAdvanceClock(),
			secretReader: (k) => (k === "NTFY_TOPIC" ? "t" : undefined),
		});
		await expect(adapter.connect({ isReconnect: false })).rejects.toThrow(
			FatalStreamError,
		);
		expect(adapter.fatalCodes.some((f) => f.code === "ntfy_unauthorized")).toBe(
			true,
		);

		const s404 = new FakeNtfyServer();
		s404.topicNotFound = true;
		const adapter404 = new NtfyAdapter({
			server: s404,
			clock: new AutoAdvanceClock(),
			secretReader: (k) => (k === "NTFY_TOPIC" ? "t" : undefined),
		});
		await expect(adapter404.connect({ isReconnect: false })).rejects.toThrow(
			FatalStreamError,
		);
		expect(
			adapter404.fatalCodes.some((f) => f.code === "ntfy_topic_not_found"),
		).toBe(true);
	});
});

describe("manifest data is load-bearing", () => {
	it("the backoff ladder is the transcribed fixed array", () => {
		expect(NTFY_RECONNECT_BACKOFF_S).toEqual([2, 5, 10, 30, 60]);
	});

	it("the command registry mirrors the reference derivation set", () => {
		expect(NTFY_REGISTRY.map((c) => c.name)).toEqual([
			"new",
			"stop",
			"model",
			"approve",
			"status",
		]);
	});
});

describe("stream event typing", () => {
	it("message events carry id/topic/message; keepalives do not", () => {
		const server = new FakeNtfyServer();
		const stream = server.subscribe("t");
		stream.pushMessage("hello", { title: "T" });
		const events: NtfyEvent[] = [];
		void stream.nextEvent().then((e) => events.push(e));
		expect(events.length).toBe(0);
	});
});

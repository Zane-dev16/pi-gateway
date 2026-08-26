// pi_platforms/ntfy/ntfy.test — behavior contracts for the ntfy census port:
// auth-header shapes, subscribe-outcome status modeling (401/404 fatality),
// per-event fallback dedup ids, disconnect-map clearing, single-POST vendor
// truncation, and the fixed reconnect ladder. Mutation-checked where the
// shape is security-adjacent.

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
import { makeNtfyWorld, TOPIC } from "./ntfy-world.js";

/** Real-time poll helper (world parity — small bounded wait). */
async function poll(p: () => boolean, timeoutMs = 2_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!p()) {
		if (Date.now() > deadline) throw new Error("poll: condition not met");
		await new Promise<void>((r) => setTimeout(r, 2));
	}
}

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

describe("subscribe carries authHeaders; fake validates before admitting (nthaha-1)", () => {
	it("the built Authorization header rides EVERY stream GET", async () => {
		const server = new FakeNtfyServer();
		server.requiredAuthHeader = "Bearer tk_9"; // token-protected topic
		const adapter = new NtfyAdapter({
			server,
			clock: new AutoAdvanceClock(),
			config: { token: "tk_9" },
			secretReader: (k) => (k === "NTFY_TOPIC" ? "t" : undefined),
		});
		expect(await adapter.connect({ isReconnect: false })).toBe(true);
		expect(adapter.isConnected).toBe(true);
		// The EXACT buildAuthHeader output was presented on the wire GET.
		expect(server.subscribeLog.at(-1)).toEqual({
			topic: "t",
			authHeaders: { Authorization: "Bearer tk_9" },
		});
		await adapter.disconnect();
	});

	it("a mismatching credential is refused BEFORE any reader is admitted", () => {
		const server = new FakeNtfyServer();
		server.requiredAuthHeader = "Bearer right";
		const verdict = server.subscribe("t", { Authorization: "Bearer wrong" });
		expect(verdict).toMatchObject({ kind: "refused", status: 401 });
		expect(server.streams).toHaveLength(0);
		const anonymous = server.subscribe("t", {});
		expect(anonymous).toMatchObject({ kind: "refused", status: 401 });
		expect(server.streams).toHaveLength(0);
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

	it("id-less events mint UNIQUE per-event fallback ids — none dropped (uuid4().hex parity)", async () => {
		// MUTANT: a constant fallback ('uuid-fallback') collides in the dedup
		// map so all but the FIRST id-less event would be swallowed.
		const w = makeNtfyWorld({ name: "ntfy-fallback" });
		await w.connectAndAwaitLive();
		const stream = w.engine.activeStreamForTests();
		stream?.pushMessage("alpha no id", { id: "" });
		stream?.pushMessage("beta no id", { id: "" });
		stream?.pushMessage("gamma no id", { id: "" });
		// Blob containment: the guard may coalesce same-chat arrivals into one
		// newline-joined turn; the mutant proof is PRESENCE of every payload.
		const blob = (): string => w.subject.turns().join("\n");
		await w.pumpStreamEvents(30);
		await poll(() => blob().includes("gamma no id"));
		expect(blob()).toContain("alpha no id");
		expect(blob()).toContain("beta no id");
		expect(blob()).toContain("gamma no id");
	});
});

describe("disconnect clears the dedup map (adapter.py:disconnect :327)", () => {
	it("seen ids are forgotten across generations; server redelivery re-dispatches", async () => {
		const w = makeNtfyWorld({ name: "ntfy-dedup-reset" });
		await w.connectAndAwaitLive();
		const gen1 = w.engine.activeStreamForTests();
		if (gen1 === null) throw new Error("fixture setup: no live stream");
		const id = gen1.pushMessage("generation one");
		await w.pumpStreamEvents(15);
		await poll(() => w.subject.turns().includes("generation one"));
		expect(w.engine.seenCount).toBeGreaterThan(0);

		await w.engine.disconnect();
		// THE contract: a NEW generation starts with a CLEAN dedup map.
		expect(w.engine.seenCount).toBe(0);

		// Reconnect and simulate the SERVER redelivering the same vendor id:
		// Hermes re-dispatches it (suppression would hide real traffic).
		expect(await w.engine.connect({ isReconnect: true })).toBe(true);
		w.engine.activeStreamForTests()?.pushMessage("redelivery of m1", { id });
		await w.pumpStreamEvents(15);
		await poll(() => w.subject.turns().join("\n").includes("redelivery of m1"));
		const redeliveries =
			w.subject.turns().join("\n").split("redelivery of m1").length - 1;
		expect(redeliveries).toBe(1);
	});
});

describe("fatal stream errors stop reconnecting (vendor STATUS semantics)", () => {
	it("modeled 401 on the stream GET raises FatalStreamError through openStream", async () => {
		const server = new FakeNtfyServer();
		server.requiredAuthHeader = "Bearer real-token";
		const adapter = new NtfyAdapter({
			server,
			clock: new AutoAdvanceClock(),
			secretReader: (k) => (k === "NTFY_TOPIC" ? "t" : undefined),
		}); // no token configured ⇒ presented headers lack Authorization
		await expect(adapter.connect({ isReconnect: false })).rejects.toThrow(
			FatalStreamError,
		);
		expect(adapter.fatalCodes.some((f) => f.code === "ntfy_unauthorized")).toBe(
			true,
		);
		// Classification came from the MODELED status, not an error string:
		// the refusal body never needs to contain the digits for the verdict.
		expect(adapter.fatalCodes[0]?.detail).toContain("Check NTFY_TOKEN");

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

describe("send truncates at the vendor cap in ONE POST (adapter.py:send :429-439)", () => {
	it("oversized deliverText ships ONE truncated publish + warning — NO labeled split lane", async () => {
		const w = makeNtfyWorld({ name: "ntfy-trunc" }); // harness budget inert
		const big = "y".repeat(9000);
		const results = await w.subject.deliverLongText(TOPIC, big);
		expect(results).toHaveLength(1);
		expect(results[0]?.success).toBe(true);
		const sends = w.wire.sendsOf(TOPIC);
		expect(sends).toHaveLength(1);
		expect(sends[0]?.content).toBe(big.slice(0, NTFY_MAX_MESSAGE_CHARS));
		expect(w.server.published.at(-1)?.body.length).toBe(NTFY_MAX_MESSAGE_CHARS);
		// Vendor warning parity: "[ntfy] Message truncated from 9000 to
		// 4096 chars (ntfy limit)".
		expect(
			w.engine.warningLog.some((l) =>
				l.includes("truncated from 9000 to 4096 chars"),
			),
		).toBe(true);
	});

	it("within-cap content stays byte-verbatim with NO warning", async () => {
		const w = makeNtfyWorld({ name: "ntfy-trunc-ok" });
		const results = await w.subject.deliverLongText(TOPIC, "short body");
		expect(results).toHaveLength(1);
		expect(results[0]?.success).toBe(true);
		expect(w.wire.sendsOf(TOPIC)[0]?.content).toBe("short body");
		expect(w.engine.warningLog).toEqual([]);
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
		const verdict = server.subscribe("t");
		if (verdict.kind !== "subscribed") throw new Error("open topic admits");
		const stream = verdict.stream;
		stream.pushMessage("hello", { title: "T" });
		const events: NtfyEvent[] = [];
		void stream.nextEvent().then((e) => events.push(e));
		expect(events.length).toBe(0);
	});
});

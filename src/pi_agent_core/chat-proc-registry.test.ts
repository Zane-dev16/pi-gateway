// pi_agent_core/chat-proc-registry.test.ts — behavior contracts for the
// per-chat process table. Fake clients only: no OS processes here. The one
// real-child proof lives in chat-proc-registry.live.test.ts.

import { describe, expect, it } from "vitest";
import { ChatProcRegistry } from "./chat-proc-registry.js";
import type { ChatProcSpawnOpts, ProcClient } from "./chat-proc-registry.js";

class FakeClient implements ProcClient {
	started = false;
	stopped = false;
	startCalls = 0;
	failStart = false;
	failProbe = false;
	sessionId: string;

	constructor(sessionId = "host-session-1") {
		this.sessionId = sessionId;
	}

	async start(): Promise<void> {
		this.startCalls += 1;
		if (this.failStart) throw new Error("boom-start");
		this.started = true;
	}

	async stop(): Promise<void> {
		this.stopped = true;
		this.started = false;
	}

	async getState(): Promise<{ sessionId: string; isStreaming: boolean }> {
		if (this.failProbe) throw new Error("boom-probe");
		if (!this.started) throw new Error("not started");
		return { sessionId: this.sessionId, isStreaming: false };
	}
}

const OPTS: ChatProcSpawnOpts = { homeDir: "/tmp/fake-home" };

function makeRegistry(fakes: FakeClient[], opts?: { now?: () => number }) {
	let calls = 0;
	const registry = new ChatProcRegistry({
		makeClient: () => {
			const fake = fakes[calls];
			calls += 1;
			if (fake === undefined) throw new Error("no fake left");
			return fake;
		},
		...(opts?.now !== undefined ? { now: opts.now } : {}),
	});
	return {
		registry,
		clientCalls: () => calls,
	};
}

describe("ChatProcRegistry spawn", () => {
	it("starts one ready child holding the host session id", async () => {
		const { registry } = makeRegistry([new FakeClient("sess-abc")]);
		const entry = await registry.spawn("chat-1", OPTS);

		expect(entry.chatKey).toBe("chat-1");
		expect(entry.state).toBe("ready");
		expect(entry.generation).toBe(0);
		expect(entry.sessionId).toBe("sess-abc");
		expect(entry.restarts).toBe(0);
		expect(registry.size).toBe(1);
	});

	it("returns the live entry without doubling the child", async () => {
		const { registry, clientCalls } = makeRegistry([new FakeClient()]);
		const first = await registry.spawn("chat-1", OPTS);
		const second = await registry.spawn("chat-1", OPTS);

		expect(second).toBe(first);
		expect(clientCalls()).toBe(1);
	});

	it("keeps the dead entry and rethrows when start fails", async () => {
		const fake = new FakeClient();
		fake.failStart = true;
		const { registry } = makeRegistry([fake]);

		await expect(registry.spawn("chat-1", OPTS)).rejects.toThrow(
			"boom-start",
		);
		const entry = registry.get("chat-1");
		expect(entry?.state).toBe("dead");
		expect(entry?.lastError).toBe("boom-start");
		expect(entry?.sessionId).toBeNull();
	});
});

describe("ChatProcRegistry supervise", () => {
	it("leaves a healthy child alone", async () => {
		const { registry, clientCalls } = makeRegistry([new FakeClient()]);
		await registry.spawn("chat-1", OPTS);

		const report = await registry.supervise("chat-1");

		expect(report.action).toBe("alive");
		expect(report.state).toBe("ready");
		expect(report.generation).toBe(0);
		expect(clientCalls()).toBe(1);
	});

	it("preserves busy across a healthy probe", async () => {
		const { registry } = makeRegistry([new FakeClient()]);
		const entry = await registry.spawn("chat-1", OPTS);
		entry.state = "busy";

		const report = await registry.supervise("chat-1");

		expect(report.action).toBe("alive");
		expect(registry.get("chat-1")?.state).toBe("busy");
	});

	it("restarts a dead child at the next generation", async () => {
		const first = new FakeClient("sess-old");
		const second = new FakeClient("sess-new");
		const { registry } = makeRegistry([first, second]);
		await registry.spawn("chat-1", OPTS);
		first.failProbe = true;

		const report = await registry.supervise("chat-1");

		expect(report.action).toBe("restarted");
		expect(report.generation).toBe(1);
		expect(report.restarts).toBe(1);
		expect(report.state).toBe("ready");
		const entry = registry.get("chat-1");
		expect(entry?.sessionId).toBe("sess-new");
		expect(entry?.client).toBe(second);
		expect(registry.size).toBe(1);
	});

	it("backs off when the replacement dies inside the window", async () => {
		let now = 1_000_000;
		const first = new FakeClient();
		const second = new FakeClient();
		const { registry } = makeRegistry([first, second], { now: () => now });
		await registry.spawn("chat-1", OPTS);
		first.failProbe = true;

		const restarted = await registry.supervise("chat-1");
		expect(restarted.action).toBe("restarted");

		const replacement = registry.get("chat-1");
		(replacement?.client as FakeClient).failProbe = true;
		now += 500;

		const backed = await registry.supervise("chat-1");
		expect(backed.action).toBe("backoff");
		expect(backed.state).toBe("backoff");
		expect(backed.generation).toBe(1);
	});

	it("retries past the backoff window", async () => {
		let now = 1_000_000;
		const first = new FakeClient();
		const flaky = new FakeClient();
		const healthy = new FakeClient();
		const { registry, clientCalls } = makeRegistry([first, flaky, healthy], {
			now: () => now,
		});
		await registry.spawn("chat-1", OPTS);
		first.failProbe = true;
		await registry.supervise("chat-1");
		flaky.failProbe = true;
		now += 500;
		expect((await registry.supervise("chat-1")).action).toBe("backoff");

		now += 60_000;
		const report = await registry.supervise("chat-1");

		expect(report.action).toBe("restarted");
		expect(report.state).toBe("ready");
		expect(report.restarts).toBe(2);
		expect(clientCalls()).toBe(3);
	});

	it("caps replacements at maxRestarts", async () => {
		const initial = new FakeClient();
		const replacement = new FakeClient();
		const queue = [initial, replacement];
		let now = 1_000_000;
		const registry = new ChatProcRegistry({
			makeClient: () => {
				const fake = queue.shift();
				if (fake === undefined) throw new Error("no fake left");
				return fake;
			},
			now: () => now,
			maxRestarts: 1,
		});
		await registry.spawn("chat-1", OPTS);
		initial.failProbe = true;
		expect((await registry.supervise("chat-1")).action).toBe("restarted");

		replacement.failProbe = true;
		now += 60_000;
		const report = await registry.supervise("chat-1");

		expect(report.action).toBe("dead-capped");
		expect(report.state).toBe("dead");
		expect(report.restarts).toBe(1);
	});

	it("reports missing for an unknown key", async () => {
		const { registry } = makeRegistry([]);
		const report = await registry.supervise("ghost");

		expect(report.action).toBe("missing");
	});
});

describe("ChatProcRegistry stop", () => {
	it("stops the child and drops the entry", async () => {
		const fake = new FakeClient();
		const { registry } = makeRegistry([fake]);
		await registry.spawn("chat-1", OPTS);

		expect(await registry.stop("chat-1")).toBe(true);
		expect(fake.stopped).toBe(true);
		expect(registry.size).toBe(0);
		expect(await registry.stop("chat-1")).toBe(false);
	});

	it("drops the entry even when the kill fails", async () => {
		const fake = new FakeClient();
		const { registry } = makeRegistry([fake]);
		await registry.spawn("chat-1", OPTS);
		fake.stop = async () => {
			throw new Error("kill failed");
		};

		await expect(registry.stop("chat-1")).rejects.toThrow("kill failed");
		expect(registry.size).toBe(0);
	});

	it("stopAll empties the table", async () => {
		const { registry } = makeRegistry([new FakeClient(), new FakeClient()]);
		await registry.spawn("chat-1", OPTS);
		await registry.spawn("chat-2", OPTS);

		await registry.stopAll();

		expect(registry.size).toBe(0);
	});
});

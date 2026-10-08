// worker-boot.test.ts — DEC-086 per-chat worker boot path.
//
// Workers boot pi children through ChatProcRegistry and never touch the
// boss-owned lifecycle: no duplicate_guard trip, no gateway_state.json
// stamp, no pid file claim. The layering gate already bans pi_agent_core
// reaching upward into pi_gateway; these contracts pin the runtime effect.

import { existsSync, mkdtempSync, readFileSync, rmSync, readFileSync as readSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ChatProcRegistry } from "../../pi_agent_core/chat-proc-registry.js";
import type { ProcClient } from "../../pi_agent_core/chat-proc-registry.js";
import { getRunningPid } from "./instance-guard.js";
import { writeRuntimeStatus } from "./status-stamp.js";

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-worker-boot-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

class FakeWorker implements ProcClient {
	started = false;
	async start(): Promise<void> {
		this.started = true;
	}
	async stop(): Promise<void> {
		this.started = false;
	}
	async getState(): Promise<{ sessionId: string; isStreaming: boolean }> {
		if (!this.started) throw new Error("not started");
		return { sessionId: "worker-session-1", isStreaming: false };
	}
}

describe("per-chat worker boot never touches boss state", () => {
	it("spawn writes no gateway_state.json and claims no pid file", async () => {
		const registry = new ChatProcRegistry({ makeClient: () => new FakeWorker() });
		const entry = await registry.spawn("chat-1", { homeDir: home });
		expect(entry.state).toBe("ready");
		expect(entry.sessionId).toBe("worker-session-1");
		expect(existsSync(join(home, "gateway_state.json"))).toBe(false);
		expect(existsSync(join(home, "gateway.pid"))).toBe(false);
		expect(getRunningPid(home)).toBe(null);
		await registry.stopAll();
	});

	it("spawn leaves a boss stamp byte-identical", async () => {
		const bossPid = process.pid;
		writeRuntimeStatus(home, { gateway_state: "running" }, { pid: bossPid, home });
		const before = readFileSync(join(home, "gateway_state.json"), "utf8");
		const registry = new ChatProcRegistry({ makeClient: () => new FakeWorker() });
		await registry.spawn("chat-1", { homeDir: home });
		await registry.spawn("chat-2", { homeDir: home });
		const after = readFileSync(join(home, "gateway_state.json"), "utf8");
		expect(after).toBe(before);
		expect(JSON.parse(after).pid).toBe(bossPid);
		expect(existsSync(join(home, "gateway.pid"))).toBe(false);
		await registry.stopAll();
	});

	it("worker boot modules never import the boss status writer", () => {
		const root = join(
			fileURLToPath(new URL(".", import.meta.url)),
			"..",
			"..",
			"pi_agent_core",
		);
		const banned = [
			"status-stamp",
			"writeRuntimeStatus",
			"gateway_state",
			"instance-guard",
			"GatewayLifecycle",
		];
		for (const file of ["chat-proc-registry.ts", "rpc-turn-runner.ts", "host.ts"]) {
			const text = readSync(join(root, file), "utf8");
			for (const needle of banned) {
				expect(text.includes(needle), `${file} must not contain ${needle}`).toBe(false);
			}
		}
	});
});

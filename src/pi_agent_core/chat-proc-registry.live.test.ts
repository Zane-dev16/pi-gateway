// pi_agent_core/chat-proc-registry.live.test.ts — ONE real child, headless.
//
// The default factory spawns a genuine `pi --mode rpc` child on a disposable
// temp home, reads its host session id over the RPC state round-trip, and
// stops it. No Telegram, no model turn, no auth seeding: getState is local.
// The temp home is removed in afterEach. Live ~/.pi is never written.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ChatProcRegistry } from "./chat-proc-registry.js";

let home = "";
let registry: ChatProcRegistry | null = null;

afterEach(async () => {
	if (registry !== null) {
		await registry.stopAll();
		registry = null;
	}
	if (home !== "") {
		rmSync(home, { recursive: true, force: true });
		home = "";
	}
});

describe("ChatProcRegistry live child", () => {
	it(
		"starts and stops one headless child",
		async () => {
			home = mkdtempSync(join(tmpdir(), "pi-chat-proc-live-"));
			registry = new ChatProcRegistry();
			const key = "agent:main:live-probe:dm:chat-1";

			const entry = await registry.spawn(key, { homeDir: home });

			expect(entry.state).toBe("ready");
			expect(entry.sessionId).not.toBeNull();
			expect(entry.sessionId?.length).toBeGreaterThan(0);
			expect(registry.size).toBe(1);

			const report = await registry.supervise(key);
			expect(report.action).toBe("alive");
			expect(report.state).toBe("ready");

			expect(await registry.stop(key)).toBe(true);
			expect(registry.size).toBe(0);
		},
		30_000,
	);
});

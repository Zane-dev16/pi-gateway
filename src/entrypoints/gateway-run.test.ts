// gateway-run.test.ts — the composition-root behavior contracts (structure-7).
//
// Spec: 01-architecture.md §5.3 (entrypoints top the graph; 'pi gateway
// run'), §3.1 stage order, 08-operations.md §1.1–§1.2. Whole-sequence parity
// anchor: gateway/run.py:start_gateway — cron provider bound, watchers bound,
// adapters derived from manifests with missing-secret loud disable, REAL
// drain overlays (obligations flush / notify / lease release), boot
// redelivery, signal wiring. Two-process startup/shutdown contracts live in
// gateway-run.two-process.test.ts.

import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { PluginManifest } from "../pi_platforms/kit/index.js";
import {
	DeliveryLedger,
	readProcessStartTime,
	type DeliveryRequest,
} from "../pi_gateway/obligations/index.js";
import { RETENTION_SECONDS } from "../pi_gateway/obligations/ledger.js";
import {
	readRestartFailureCounts,
	type Logger,
} from "../pi_gateway/lifecycle/shutdown.js";
import { StateStore } from "../pi_state/index.js";
import { structuredHolder } from "../pi_state/leases.js";
import {
	composeGatewayLifecycle,
	runGateway,
	type AdapterConnectSurface,
	type GatewayRunInput,
	type PlatformHosting,
} from "./gateway-run.js";

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-gateway-run-home-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

interface LogCall {
	level: "info" | "warn" | "error";
	message: string;
	meta?: Record<string, unknown> | undefined;
}

function spyLogger(): { log: Logger; calls: LogCall[] } {
	const calls: LogCall[] = [];
	return {
		calls,
		log: {
			info: (m: string, meta?: Record<string, unknown>) => {
				calls.push({ level: "info", message: m, meta });
			},
			warn: (m: string, meta?: Record<string, unknown>) => {
				calls.push({ level: "warn", message: m, meta });
			},
			error: (m: string, meta?: Record<string, unknown>) => {
				calls.push({ level: "error", message: m, meta });
			},
		},
	};
}

const DRIVER_MANIFEST: PluginManifest = {
	name: "driver",
	description: "composition-root driver platform",
	transportShape: "polling",
	requiresEnv: [{ name: "DRIVER_TOKEN" }],
	capabilities: {},
};

interface DriverAdapterEvents {
	connects: number[];
	disconnects: number[];
}

/**
 * Conforming adapter stand-in for the registry boundary (connect/disconnect
 * base contract). Production transports land with hosting; these exercise the
 * DERIVATION + overlay wiring, which is this module's contract.
 */
function driverAdapter(events: DriverAdapterEvents): AdapterConnectSurface {
	let everConnected = false;
	return {
		async connect() {
			events.connects.push(1);
			if (everConnected) return false;
			everConnected = true;
			return true;
		},
		async disconnect() {
			events.disconnects.push(1);
		},
	};
}

function driverHosting(
	events: DriverAdapterEvents,
	factory?: () => unknown,
): PlatformHosting {
	return {
		platform: DRIVER_MANIFEST.name,
		manifest: DRIVER_MANIFEST,
		factory: factory ?? (() => driverAdapter(events)),
	};
}

async function openStore(): Promise<StateStore> {
	return StateStore.open(join(home, "state.db"));
}

function insertLease(
	store: StateStore,
	conversationId: string,
	holder: string,
	expiresInSec: number,
): void {
	const now = Date.now() / 1000;
	store.db
		.prepare(
			"INSERT INTO session_turn_leases (conversation_id, holder, acquired_at, expires_at) VALUES (?, ?, ?, ?)",
		)
		.run(conversationId, holder, now - 10, now + expiresInSec);
}

/** Ghost-owner ledger: rows owned by a provably-dead process stamp. */
function ghostLedger(store: StateStore): DeliveryLedger {
	return new DeliveryLedger(store.db, {
		selfStamp: {
			pid: process.pid,
			startedAt: (readProcessStartTime(process.pid) ?? 0) + 7,
		},
	});
}

describe("composeGatewayLifecycle — production stage entries", () => {
	it("binds cron ticker (stage 7), extensions + reconnect watcher (stage 8) and derived adapter entries (stage 9); startup reaches running with zero service degradations and the adapter connects", async () => {
		const events: DriverAdapterEvents = { connects: [], disconnects: [] };
		const spy = spyLogger();
		const composed = composeGatewayLifecycle({
			home,
			logger: spy.log,
			installSignals: false,
			cron: {
				runner: {
					run: async () => ({ ok: true }),
					interrupt: async () => true,
				},
				intervalSeconds: 3600,
			},
			platforms: [driverHosting(events)],
			secretReader: () => "tok",
		});
		const result = await composed.lifecycle.startup();
		expect(result.ok).toBe(true);

		// Stage 7 really started the ticker: the cron jobs store lives under
		// <home>/cron and holds its jobs-file lock for the service lifetime.
		expect(existsSync(join(home, "cron"))).toBe(true);
		expect(composed.lifecycle.degradedServices).toEqual([]);

		// Stage 9 derivation connected the adapter (manifest gate passed).
		expect(composed.connectedPlatforms()).toEqual(["driver"]);
		expect(events.connects.length).toBe(1);

		// Drain stops ingress through the derived handle → disconnect runs.
		await composed.lifecycle.requestShutdown("planned_stop");
		await vi.waitFor(() => expect(events.disconnects.length).toBe(1));
		const outcome = await composed.lifecycle.waitShutdown();
		expect(outcome.exitCode).toBe(0);
	});

	it("missing required secret ⇒ LOUD adapter DISABLE (never silent), platform stays unconnected", async () => {
		const events: DriverAdapterEvents = { connects: [], disconnects: [] };
		const spy = spyLogger();
		const composed = composeGatewayLifecycle({
			home,
			logger: spy.log,
			platforms: [driverHosting(events)],
			secretReader: () => undefined,
		});
		const result = await composed.lifecycle.startup();
		expect(result.ok).toBe(true); // loud disable degrades, never blocks
		expect(composed.connectedPlatforms()).toEqual([]);
		expect(events.connects.length).toBe(0);
		const disabled = spy.calls.find(
			(c) =>
				c.level === "error" &&
				c.message.includes("driver") &&
				c.message.includes("DISABLED"),
		);
		expect(disabled).toBeDefined();
		expect(disabled?.meta?.reason_code).toBe("adapter_disabled");
		// The loud line carries the missing secret name (08 §1.1 step 7).
		expect(disabled?.message).toContain("DRIVER_TOKEN");
	});

	it("refused connect is RETRYABLE — queued into the failed-platform queue feeding the reconnect watcher", async () => {
		const events: DriverAdapterEvents = { connects: [], disconnects: [] };
		const spy = spyLogger();
		const composed = composeGatewayLifecycle({
			home,
			logger: spy.log,
			platforms: [
				driverHosting(
					events,
					() =>
						({
							connect: () => false,
							disconnect: async () => undefined,
						}) as unknown as AdapterConnectSurface,
				),
			],
			secretReader: () => "tok",
		});
		await composed.lifecycle.startup();
		expect(composed.lifecycle.failedPlatforms.get("driver")).not.toBeNull();
		expect(composed.lifecycle.failedPlatforms.get("driver")?.attempts).toBe(1);
	});
});

describe("production drain overlays", () => {
	it("releases THIS process's turn leases at drain and leaves foreign rows alone", async () => {
		const seed = await openStore();
		insertLease(
			seed,
			"conv-self",
			structuredHolder("turn-lease", process.pid),
			300,
		);
		insertLease(seed, "conv-foreign", "foreign-gateway:pid=999999999", 300);
		seed.close();

		const composed = composeGatewayLifecycle({
			home,
			logger: spyLogger().log,
			installSignals: false,
		});
		await composed.lifecycle.startup();
		const outcome = await composed.lifecycle.requestShutdown("planned_stop");
		expect(outcome.flushesFailed).toBe(false);

		const verify = await openStore();
		const remaining = verify.db
			.prepare("SELECT conversation_id FROM session_turn_leases")
			.all() as Array<{ conversation_id: string }>;
		verify.close();
		expect(remaining.map((r) => r.conversation_id)).toEqual(["conv-foreign"]);
	});

	it("flushes delivery obligations before closeDatabase (retention GC) and counts live sessions into restart-failure state", async () => {
		const seed = await openStore();
		const staleAt = Date.now() / 1000 - RETENTION_SECONDS - 86_400;
		const deadLedger = ghostLedger(seed);
		const staleId = await deadLedger.record(
			{
				sessionKey: "sess-stale",
				platform: "driver",
				chatId: "chat-1",
				content: "old delivered reply",
			},
			{ nowSeconds: staleAt },
		);
		await deadLedger.beginAttempt(staleId, { nowSeconds: staleAt });
		await deadLedger.markDelivered(staleId, { nowSeconds: staleAt });
		insertLease(seed, "conv-live", "holder-live:pid=42", 600);
		seed.close();

		const composed = composeGatewayLifecycle({
			home,
			logger: spyLogger().log,
			installSignals: false,
		});
		await composed.lifecycle.startup();
		await composed.lifecycle.requestShutdown("planned_stop");

		const verify = await openStore();
		const row = verify.db
			.prepare("SELECT COUNT(*) AS n FROM delivery_obligations")
			.get() as { n: number };
		verify.close();
		expect(row.n).toBe(0); // retention-expired delivered row pruned pre-close

		// Live lease at teardown ⇒ #7536 restart-failure counting input.
		const counts = readRestartFailureCounts(home);
		expect(Object.keys(counts)).toContain("conv-live");
	});

	it("notify phase fires the injected transport per live session while adapters are still connected; absent transport warns loudly", async () => {
		const seed = await openStore();
		insertLease(seed, "conv-a", "holder-x:pid=42", 300);
		seed.close();

		const sent: string[] = [];
		const spy = spyLogger();
		const input: GatewayRunInput = {
			home,
			logger: spy.log,
			installSignals: false,
			shutdownNoticeSender: async (key) => {
				sent.push(key);
			},
		};
		const composed = composeGatewayLifecycle(input);
		await composed.lifecycle.startup();
		await composed.lifecycle.requestShutdown("planned_stop");
		expect(sent).toEqual(["conv-a"]);

		// Same shape WITHOUT a transport: loud warning, no fake sends.
		const home2 = mkdtempSync(join(tmpdir(), "pi-gateway-run-home2-"));
		try {
			const seed2 = await StateStore.open(join(home2, "state.db"));
			insertLease(seed2, "conv-b", "holder-y:pid=43", 300);
			seed2.close();
			const spy2 = spyLogger();
			const composed2 = composeGatewayLifecycle({
				home: home2,
				logger: spy2.log,
				installSignals: false,
			});
			await composed2.lifecycle.startup();
			await composed2.lifecycle.requestShutdown("planned_stop");
			const warned = spy2.calls.find(
				(c) =>
					c.level === "warn" &&
					c.message.includes("no notice transport configured"),
			);
			expect(warned).toBeDefined();
			expect(warned?.meta?.count).toBe(1);
		} finally {
			rmSync(home2, { recursive: true, force: true });
		}
	});
});

describe("boot sends — pending-obligation redelivery", () => {
	it("claims dead-owned pending rows for CONNECTED platforms only and drives them through the sender (plain content, crash-ambiguity parity)", async () => {
		const seed = await openStore();
		await ghostLedger(seed).record(
			{
				sessionKey: "sess-redeliver",
				platform: "driver",
				chatId: "chat-9",
				content: "the owed reply",
			},
			{ nowSeconds: Date.now() / 1000 - 30 },
		);
		seed.close();

		const sent: DeliveryRequest[] = [];
		const events: DriverAdapterEvents = { connects: [], disconnects: [] };
		const spy = spyLogger();
		const composed = composeGatewayLifecycle({
			home,
			logger: spy.log,
			platforms: [driverHosting(events)],
			secretReader: () => "tok",
			deliverySender: async (req) => {
				sent.push(req);
				return { ok: true };
			},
		});
		await composed.lifecycle.startup();

		expect(sent).toHaveLength(1);
		expect(sent[0]?.sessionKey).toBe("sess-redeliver");
		expect(sent[0]?.content).toBe("the owed reply"); // pending ⇒ plain, NO marker
		expect(sent[0]?.needsMarker).toBe(false);

		const verify = await openStore();
		const settled = verify.db
			.prepare(
				"SELECT state FROM delivery_obligations WHERE session_key = 'sess-redeliver'",
			)
			.get() as { state: string };
		verify.close();
		expect(settled.state).toBe("delivered");
	});

	it("with no connected adapters the redelivery skips loudly and spends NO attempt", async () => {
		const seed = await openStore();
		await ghostLedger(seed).record(
			{
				sessionKey: "sess-waiting",
				platform: "driver",
				chatId: "chat-1",
				content: "still owed",
			},
			{ nowSeconds: Date.now() / 1000 - 30 },
		);
		seed.close();

		let sends = 0;
		const spy = spyLogger();
		const composed = composeGatewayLifecycle({
			home,
			logger: spy.log,
			installSignals: false,
			deliverySender: async () => {
				sends++;
				return { ok: true };
			},
		});
		await composed.lifecycle.startup();
		expect(sends).toBe(0);
		const skipped = spy.calls.find((c) =>
			c.message.includes("redelivery skipped"),
		);
		expect(skipped).toBeDefined();

		const verify = await openStore();
		const row = verify.db
			.prepare(
				"SELECT attempts, state FROM delivery_obligations WHERE session_key = 'sess-waiting'",
			)
			.get() as { attempts: number; state: string };
		verify.close();
		expect(row.attempts).toBe(0); // budget NOT spent
		expect(row.state).toBe("pending");
	});
});

describe("runGateway — process contract", () => {
	it("a lost runtime-lock race returns exit code 1 without running", async () => {
		// Another live process holds the runtime lock ⇒ stage 5 aborts
		// (runtime_lock_held) and runGateway maps that to exit code 1.
		const { RuntimeLock } = await import(
			"../pi_gateway/lifecycle/instance-guard.js"
		);
		const holder = new RuntimeLock(home, { selfPid: 999999997 });
		const released = false;
		try {
			expect(holder.acquire()).toBe(true);
			const exit = await runGateway({
				home,
				logger: spyLogger().log,
				installSignals: false,
			});
			expect(exit.ran).toBe(false);
			expect(exit.exitCode).toBe(1);
		} finally {
			if (!released) holder.release();
		}
	});
});

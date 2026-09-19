// platform-hosting.test.ts — DEC-072 explicit-list composition contracts.
//
// The extension builds an explicit platform list at boot from the
// PI_GATEWAY_PLATFORMS allowlist; resolveConfiguredPlatforms is that
// mapping, and matrixHosting is the matrix census builder. Contracts here
// run REAL chains (env string → hosted platforms → composed lifecycle →
// stage-9 entries), hermetic in temp homes — no source-regex, no frozen
// lists.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MatrixAdapterCore } from "../pi_platforms/matrix/matrix-adapter.js";
import { TelegramAdapter } from "../pi_platforms/telegram/telegram-adapter.js";
import { MATRIX_MANIFEST } from "../pi_platforms/matrix/manifest.js";
import { TELEGRAM_MANIFEST } from "../pi_platforms/telegram/manifest.js";
import {
	composeGatewayLifecycle,
	type AdapterConnectSurface,
} from "./gateway-run.js";
import type { Logger } from "../pi_gateway/lifecycle/shutdown.js";
import {
	matrixHosting,
	resolveConfiguredPlatforms,
	telegramHosting,
} from "./platform-hosting.js";

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-gateway-platforms-home-"));
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

/** Fast conforming adapter stand-in (no parked long-polls, no timers). */
function stubAdapter(events: {
	connects: number;
	disconnects: number;
}): AdapterConnectSurface {
	return {
		async connect() {
			events.connects += 1;
			return true;
		},
		async disconnect() {
			events.disconnects += 1;
		},
	};
}

describe("resolveConfiguredPlatforms — explicit boot list (DEC-072)", () => {
	it("default empty/unset resolves to zero entries (current behavior)", () => {
		expect(resolveConfiguredPlatforms(undefined, () => "cred")).toEqual([]);
		expect(resolveConfiguredPlatforms("", () => "cred")).toEqual([]);
		expect(resolveConfiguredPlatforms("  ,  ", () => "cred")).toEqual([]);
	});

	it("listed names resolve to hosted platforms paired with their manifests", () => {
		const entries = resolveConfiguredPlatforms("telegram,matrix", () => "cred");
		expect(entries.map((e) => e.platform)).toEqual(["telegram", "matrix"]);
		expect(entries[0]?.manifest).toBe(TELEGRAM_MANIFEST);
		expect(entries[1]?.manifest).toBe(MATRIX_MANIFEST);
	});

	it("names are trimmed, lowercased, deduplicated; unknown names stay absent", () => {
		const entries = resolveConfiguredPlatforms(
			" Matrix ,MATRIX,unknown-platform",
			() => "cred",
		);
		expect(entries.map((e) => e.platform)).toEqual(["matrix"]);
	});

	it("production factories construct the real adapters over the injected secrets", () => {
		const seen: string[] = [];
		const entries = resolveConfiguredPlatforms("telegram,matrix", (name) => {
			seen.push(name);
			return `cred-for-${name}`;
		});
		const telegram = entries[0]?.factory();
		const matrix = entries[1]?.factory();
		expect(telegram).toBeInstanceOf(TelegramAdapter);
		expect(matrix).toBeInstanceOf(MatrixAdapterCore);
		// Both factories route secret reads through the injected reader.
		expect(seen.length).toBeGreaterThan(0);
		for (const adapter of [telegram, matrix]) {
			const surface = adapter as Partial<AdapterConnectSurface>;
			expect(typeof surface.connect).toBe("function");
			expect(typeof surface.disconnect).toBe("function");
		}
	});
});

describe("matrixHosting builder", () => {
	it("pairs MATRIX_MANIFEST with the given factory (telegramHosting pattern)", () => {
		const events = { connects: 0, disconnects: 0 };
		const factory = () => stubAdapter(events);
		const hosting = matrixHosting(factory);
		expect(hosting.platform).toBe("matrix");
		expect(hosting.manifest).toBe(MATRIX_MANIFEST);
		expect(hosting.factory).toBe(factory);
		// No custom register hook — the default ctx.registerPlatform path runs.
		expect(hosting.register).toBeUndefined();
	});
});

describe("explicit-list composition through stage 9", () => {
	it("listed + credentialed platform yields a connected entry; unlisted stays absent", async () => {
		const events = { connects: 0, disconnects: 0 };
		const spy = spyLogger();
		const composed = composeGatewayLifecycle({
			home,
			logger: spy.log,
			installSignals: false,
			platforms: [matrixHosting(() => stubAdapter(events))],
			secretReader: () => "cred",
		});
		const result = await composed.lifecycle.startup();
		expect(result.ok).toBe(true);
		expect(composed.connectedPlatforms()).toEqual(["matrix"]);
		expect(events.connects).toBe(1);
		// telegram was never listed — no entry, no connection, no disable line.
		expect(composed.connectedPlatforms()).not.toContain("telegram");

		await composed.lifecycle.requestShutdown("planned_stop");
		await composed.lifecycle.waitShutdown();
		expect(events.disconnects).toBe(1);
	});

	it("listed but uncredentialed ⇒ LOUD adapter_disabled naming the missing secret per entry", async () => {
		const matrixEvents = { connects: 0, disconnects: 0 };
		const telegramEvents = { connects: 0, disconnects: 0 };
		const spy = spyLogger();
		const composed = composeGatewayLifecycle({
			home,
			logger: spy.log,
			installSignals: false,
			platforms: [
				matrixHosting(() => stubAdapter(matrixEvents)),
				telegramHosting(() => stubAdapter(telegramEvents)),
			],
			secretReader: () => undefined,
		});
		const result = await composed.lifecycle.startup();
		expect(result.ok).toBe(true); // loud disable degrades, never blocks
		expect(composed.connectedPlatforms()).toEqual([]);
		expect(matrixEvents.connects).toBe(0);
		expect(telegramEvents.connects).toBe(0);

		const disables = spy.calls.filter(
			(c) => c.level === "error" && c.meta?.reason_code === "adapter_disabled",
		);
		expect(disables).toHaveLength(2);
		const byPlatform = new Map(
			disables.map((d) => [
				d.message.includes("matrix")
					? "matrix"
					: d.message.includes("telegram")
						? "telegram"
						: d.message,
				d.message,
			]),
		);
		// Each entry's line names ITS missing secret (first requiresEnv miss).
		expect(byPlatform.get("matrix")).toContain("MATRIX_HOMESERVER");
		expect(byPlatform.get("telegram")).toContain("TELEGRAM_BOT_TOKEN");

		await composed.lifecycle.requestShutdown("planned_stop");
		await composed.lifecycle.waitShutdown();
	});

	it("default-empty composition (no platforms key) starts with zero entries", async () => {
		const spy = spyLogger();
		const composed = composeGatewayLifecycle({
			home,
			logger: spy.log,
			installSignals: false,
		});
		const result = await composed.lifecycle.startup();
		expect(result.ok).toBe(true);
		expect(composed.connectedPlatforms()).toEqual([]);
		expect(
			spy.calls.some((c) => c.meta?.reason_code === "adapter_disabled"),
		).toBe(false);

		await composed.lifecycle.requestShutdown("planned_stop");
		await composed.lifecycle.waitShutdown();
	});
});

describe("matrix production factory — real HTTP transport (DEC-073)", () => {
	it("factory adapter sends through the HTTP client with bearer auth (loopback stub)", async () => {
		const puts: Array<{ path: string; auth: string | undefined; body: unknown }> =
			[];
		const { createServer } = await import("node:http");
		const stub = createServer((req, res) => {
			const url = new URL(req.url ?? "/", "http://127.0.0.1");
			let text = "";
			req.on("data", (c: Buffer) => {
				text += c.toString("utf8");
			});
			req.on("end", () => {
				if (req.method === "PUT") {
					puts.push({
						path: url.pathname,
						auth:
							typeof req.headers["authorization"] === "string"
								? req.headers["authorization"]
								: undefined,
						body: text === "" ? null : (JSON.parse(text) as unknown),
					});
				}
				res.writeHead(200, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ event_id: "$factory1" }));
			});
		});
		await new Promise<void>((resolve) => {
			stub.listen(0, "127.0.0.1", () => resolve());
		});
		try {
			const addr = stub.address();
			const port =
				typeof addr === "object" && addr !== null ? String(addr.port) : "1";
			const entries = resolveConfiguredPlatforms("matrix", (name) =>
				name === "MATRIX_HOMESERVER"
					? `http://127.0.0.1:${port}`
					: name === "MATRIX_ACCESS_TOKEN"
						? "tok-factory"
						: undefined,
			);
			expect(entries).toHaveLength(1);
			const adapter = entries[0]?.factory();
			expect(adapter).toBeInstanceOf(MatrixAdapterCore);
			const wired = adapter as unknown as {
				wireTransmitSend: (
					chatId: string,
					content: string,
					metadata: unknown,
				) => Promise<{ success: boolean; messageId?: string }>;
			};
			const result = await wired.wireTransmitSend("!r:x", "factory send", {
				event_content: { msgtype: "m.text", body: "factory send" },
			});
			expect(result.success).toBe(true);
			expect(result.messageId).toBe("$factory1");
			expect(puts).toHaveLength(1);
			expect(puts[0]?.path).toMatch(/\/send\/m\.room\.message\/.+/);
			expect(puts[0]?.auth).toBe("Bearer tok-factory");
			expect(puts[0]?.body).toEqual({ msgtype: "m.text", body: "factory send" });
		} finally {
			await new Promise<void>((resolve) => {
				stub.close(() => resolve());
			});
		}
	});
});

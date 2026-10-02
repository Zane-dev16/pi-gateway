// entrypoints/setup-wiring.test — DEC-081 wiring contracts.
//
// Relation test (kept per policy): every manifest row appears in the setup
// table. Live validator cases drive loopback getMe stubs or no network at
// all. Assertions cover redaction and fail-closed behavior, not transport.

import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { TELEGRAM_MANIFEST } from "../pi_platforms/telegram/manifest.js";
import {
	allSetupSpecs,
	buildTelegramSetupSpec,
	manifestToSetupSpec,
	runTuiSetup,
	validateSetupLive,
	validateTelegramToken,
} from "./setup-wiring.js";

/** Loopback stub answering every request with one canned Bot API envelope. */
async function startGetMeStub(
	status: number,
	json: unknown,
): Promise<{ url: string; close: () => Promise<void> }> {
	const server = createServer((req, res) => {
		const chunks: Buffer[] = [];
		req.on("data", (c: Buffer) => chunks.push(c));
		req.on("end", () => {
			res.writeHead(status, { "Content-Type": "application/json" });
			res.end(JSON.stringify(json));
		});
	});
	await new Promise<void>((resolve) => {
		server.listen(0, "127.0.0.1", () => resolve());
	});
	const addr = server.address();
	const port = typeof addr === "object" && addr !== null ? addr.port : 0;
	return {
		url: `http://127.0.0.1:${String(port)}`,
		close: () =>
			new Promise<void>((resolve) => {
				server.closeAllConnections();
				server.close(() => resolve());
			}),
	};
}

describe("manifest to setup table", () => {
	it("carries every manifest row into the setup spec", () => {
		const spec = manifestToSetupSpec(TELEGRAM_MANIFEST);
		expect(spec.name).toBe(TELEGRAM_MANIFEST.name);
		for (const row of TELEGRAM_MANIFEST.requiresEnv) {
			expect(spec.required.map((v) => v.name)).toContain(row.name);
		}
		for (const row of TELEGRAM_MANIFEST.optionalEnv ?? []) {
			expect((spec.optional ?? []).map((v) => v.name)).toContain(row.name);
		}
	});

	it("telegram spec adds the DM allowlist as optional", () => {
		const spec = buildTelegramSetupSpec();
		expect((spec.optional ?? []).map((v) => v.name)).toContain(
			"TELEGRAM_ALLOWED_USERS",
		);
		expect(allSetupSpecs().map((s) => s.name)).toContain("telegram");
	});
});

describe("live validation", () => {
	it("telegram live success returns ok against a loopback getMe", async () => {
		const stub = await startGetMeStub(200, {
			ok: true,
			result: { id: 1, username: "loopback_bot" },
		});
		try {
			const res = await validateSetupLive(
				"telegram",
				new Map([["TELEGRAM_BOT_TOKEN", "TESTTOKEN"]]),
				{ baseUrl: stub.url },
			);
			expect(res).toEqual({ ok: true });
		} finally {
			await stub.close();
		}
	});

	it("telegram live failure redacts the token", async () => {
		const token = "live-failure-token-k7q2";
		const stub = await startGetMeStub(401, {
			ok: false,
			error_code: 401,
			description: "Unauthorized",
		});
		try {
			const res = await validateSetupLive(
				"telegram",
				new Map([["TELEGRAM_BOT_TOKEN", token]]),
				{ baseUrl: stub.url },
			);
			expect(res.ok).toBe(false);
			if (!res.ok) {
				expect(res.error).not.toContain(token);
			}
		} finally {
			await stub.close();
		}
	});

	it("unreachable origin fails closed without echoing the token", async () => {
		const token = "secret-token-zz9-4351";
		const res = await validateTelegramToken(token, {
			baseUrl: "http://127.0.0.1:1",
		});
		expect(res.ok).toBe(false);
		if (!res.ok) {
			expect(res.error).not.toContain(token);
			expect(res.error).toContain("telegram");
		}
	});

	it("empty token fails with a secret-free message", async () => {
		const res = await validateTelegramToken("   ");
		expect(res).toEqual({ ok: false, error: "bot token is required" });
	});

	it("unknown platforms fail closed without network", async () => {
		const res = await validateSetupLive(
			"signal",
			new Map([["SIGNAL_API_KEY", "signal-value-9"]]),
		);
		expect(res.ok).toBe(false);
		if (!res.ok) {
			expect(res.error).toContain("no live validator");
			expect(res.error).toContain("signal");
			expect(res.error).not.toContain("signal-value-9");
		}
		const empty = await validateSetupLive("signal", new Map());
		expect(empty.ok).toBe(false);
	});
});

describe("TUI run end to end with scripted dialogs", () => {
	let home = "";
	afterEach(() => {
		if (home !== "") rmSync(home, { recursive: true, force: true });
		home = "";
	});

	it("drives prompts through TUI dialogs and writes 0600", async () => {
		home = mkdtempSync(join(tmpdir(), "setup-wiring-"));
		const seen: string[] = [];
		const secret = "tui-dialog-token-77ab";
		const ui = {
			select: async (_t: string, o: string[]) => o[0],
			input: async (_t: string, _p?: string) => secret,
			confirm: async (_t: string, m: string) => {
				seen.push(m);
				return true;
			},
			notify: (m: string) => {
				seen.push(m);
			},
		};
		const res = await runTuiSetup(home, ui, {
			validate: async () => ({ ok: true as const }),
		});
		expect(res.ok).toBe(true);
		const body = readFileSync(join(home, ".env"), "utf8");
		expect(body).toContain(secret);
		expect(statSync(join(home, ".env")).mode & 0o777).toBe(0o600);
		expect(seen.join("\n")).not.toContain(secret);
	});

	it("marks secret prompts with the password flag", async () => {
		home = mkdtempSync(join(tmpdir(), "setup-wiring-mask-"));
		const secret = "mask-me-token-3f9c";
		const seen: string[] = [];
		const inputCalls: {
			title: string;
			opts:
				| { password?: boolean; signal?: AbortSignal; timeout?: number }
				| undefined;
		}[] = [];
		const ui = {
			select: async (_t: string, o: string[]) => o[0],
			input: async (
				t: string,
				_p?: string,
				opts?: { password?: boolean; signal?: AbortSignal; timeout?: number },
			) => {
				inputCalls.push({ title: t, opts });
				if (t.includes("TELEGRAM_BOT_TOKEN")) return secret;
				return "222";
			},
			confirm: async (_t: string, m: string) => {
				seen.push(m);
				return true;
			},
			notify: (m: string) => {
				seen.push(m);
			},
		};
		const res = await runTuiSetup(home, ui, {
			specs: [
				{
					name: "telegram",
					required: [{ name: "TELEGRAM_BOT_TOKEN", password: true as const }],
					optional: [
						{ name: "TELEGRAM_ALLOWED_USERS", optional: true as const },
					],
				},
			],
			validate: async () => ({ ok: true as const }),
		});
		expect(res.ok).toBe(true);
		const tokenCall = inputCalls.find((c) =>
			c.title.includes("TELEGRAM_BOT_TOKEN"),
		);
		const allowCall = inputCalls.find((c) =>
			c.title.includes("TELEGRAM_ALLOWED_USERS"),
		);
		expect(tokenCall?.opts).toEqual({ password: true });
		expect(allowCall?.opts).toBeUndefined();
		expect(seen.join("\n")).not.toContain(secret);
	});
});

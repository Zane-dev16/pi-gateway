// entrypoints/setup-wiring.test — DEC-081 wiring contracts.
//
// Relation test (kept per policy): every manifest row appears in the setup
// table. Live validator test uses an unreachable loopback origin so no real
// network runs; the assertion is the redaction, not the transport.

import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
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

	it("unknown platforms accept non-empty values, reject empties", async () => {
		expect(
			await validateSetupLive("signal", new Map([["SIGNAL_API_KEY", "x"]])),
		).toEqual({ ok: true });
		const bad = await validateSetupLive(
			"signal",
			new Map([["SIGNAL_API_KEY", "  "]]),
		);
		expect(bad.ok).toBe(false);
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
});

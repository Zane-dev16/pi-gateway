// pi_gateway/setup/setup.test — DEC-081 behavior contracts.
//
// Calls runSetupFlow the way the TUI does (scripted IO) and asserts what the
// operator observes: .env bytes, 0600 mode, rotation, and that no secret ever
// reaches a notify/log line.

import {
	mkdtempSync,
	readFileSync,
	rmSync,
	statSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	runSetupFlow,
	type SetupIO,
	type SetupPlatformSpec,
} from "./machine.js";
import { writeSetupVars } from "./writer.js";

const TOKEN = "loopback-token-alpha-9f8e";
const TOKEN_ROTATED = "loopback-token-beta-41cd";
const ALLOW = "111,222";

const TELEGRAM_LIKE: SetupPlatformSpec = {
	name: "telegram",
	required: [{ name: "TELEGRAM_BOT_TOKEN", password: true }],
	optional: [{ name: "TELEGRAM_ALLOWED_USERS", optional: true }],
};

const SECOND_PLATFORM: SetupPlatformSpec = {
	name: "signal",
	required: [{ name: "SIGNAL_API_KEY", password: true }],
};

let home: string;
let lines: string[];

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "setup-home-"));
	lines = [];
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

function scriptedIO(
	answers: Record<string, string>,
	opts: { confirm?: boolean } = {},
): SetupIO {
	return {
		selectPlatform: async (platforms) => platforms[0],
		inputVar: async (_platform, spec) => answers[spec.name] ?? "",
		confirmSave: async () => opts.confirm ?? true,
		notify: (message) => {
			lines.push(message);
		},
	};
}

const loopbackValidator = async (
	platform: string,
	values: ReadonlyMap<string, string>,
) => {
	if (platform === "telegram") {
		return values.get("TELEGRAM_BOT_TOKEN") !== undefined &&
			(values.get("TELEGRAM_BOT_TOKEN") ?? "") !== ""
			? { ok: true as const }
			: { ok: false as const, error: "bot token is required" };
	}
	return { ok: true as const };
};

function loggedSecrets(): string[] {
	return lines.filter((l) => l.includes(TOKEN) || l.includes(TOKEN_ROTATED));
}

describe("setup flow against a loopback validator", () => {
	it("prompts, validates, writes .env at 0600 with no secret in notify/log", async () => {
		const log: string[] = [];
		const res = await runSetupFlow({
			home,
			platforms: [TELEGRAM_LIKE],
			io: scriptedIO({
				TELEGRAM_BOT_TOKEN: TOKEN,
				TELEGRAM_ALLOWED_USERS: ALLOW,
			}),
			validate: loopbackValidator,
			write: writeSetupVars,
			log: (m) => {
				lines.push(m);
				log.push(m);
			},
		});
		expect(res).toEqual({
			ok: true,
			platform: "telegram",
			vars: ["TELEGRAM_BOT_TOKEN", "TELEGRAM_ALLOWED_USERS"],
		});

		const envPath = join(home, ".env");
		const body = readFileSync(envPath, "utf8");
		expect(body).toContain(`TELEGRAM_BOT_TOKEN=${TOKEN}`);
		expect(body).toContain(`TELEGRAM_ALLOWED_USERS=${ALLOW}`);
		expect(statSync(envPath).mode & 0o777).toBe(0o600);
		expect(loggedSecrets()).toEqual([]);
		expect(log.length).toBeGreaterThan(0);
	});

	it("re-run rotates the token and preserves unrelated keys", async () => {
		const envPath = join(home, ".env");
		writeFileSync(envPath, "UNRELATED_KEEP=1\n");
		const io1 = scriptedIO({ TELEGRAM_BOT_TOKEN: TOKEN });
		const r1 = await runSetupFlow({
			home,
			platforms: [TELEGRAM_LIKE],
			io: io1,
			validate: loopbackValidator,
			write: writeSetupVars,
		});
		expect(r1.ok).toBe(true);

		lines = [];
		const r2 = await runSetupFlow({
			home,
			platforms: [TELEGRAM_LIKE],
			io: scriptedIO({ TELEGRAM_BOT_TOKEN: TOKEN_ROTATED }),
			validate: loopbackValidator,
			write: writeSetupVars,
		});
		expect(r2.ok).toBe(true);
		const body = readFileSync(envPath, "utf8");
		expect(body).toContain(`TELEGRAM_BOT_TOKEN=${TOKEN_ROTATED}`);
		expect(body).not.toContain(TOKEN);
		expect(body).toContain("UNRELATED_KEEP=1");
		expect(statSync(envPath).mode & 0o777).toBe(0o600);
		expect(loggedSecrets()).toEqual([]);
	});

	it("validation failure blocks the write and surfaces the error", async () => {
		const failing = async () => ({
			ok: false as const,
			error: "getMe rejected",
		});
		const res = await runSetupFlow({
			home,
			platforms: [TELEGRAM_LIKE],
			io: scriptedIO({ TELEGRAM_BOT_TOKEN: "bad-token-xyz" }),
			validate: failing,
			write: writeSetupVars,
		});
		expect(res).toEqual({
			ok: false,
			platform: "telegram",
			error: "getMe rejected",
		});
		expect(() => readFileSync(join(home, ".env"), "utf8")).toThrow();
		expect(lines.join("\n")).not.toContain("bad-token-xyz");
	});

	it("missing required value fails without writing", async () => {
		const res = await runSetupFlow({
			home,
			platforms: [TELEGRAM_LIKE],
			io: scriptedIO({ TELEGRAM_BOT_TOKEN: "" }),
			validate: loopbackValidator,
			write: writeSetupVars,
		});
		expect(res.ok).toBe(false);
		expect(() => readFileSync(join(home, ".env"), "utf8")).toThrow();
	});

	it("declined confirm cancels without writing", async () => {
		const res = await runSetupFlow({
			home,
			platforms: [TELEGRAM_LIKE],
			io: scriptedIO({ TELEGRAM_BOT_TOKEN: TOKEN }, { confirm: false }),
			validate: loopbackValidator,
			write: writeSetupVars,
		});
		expect(res).toEqual({ ok: false, cancelled: true, platform: "telegram" });
		expect(() => readFileSync(join(home, ".env"), "utf8")).toThrow();
	});

	it("same machine drives a second platform table with no telegram code", async () => {
		const res = await runSetupFlow({
			home,
			platforms: [SECOND_PLATFORM],
			io: scriptedIO({ SIGNAL_API_KEY: "signal-key-7" }),
			validate: loopbackValidator,
			write: writeSetupVars,
		});
		expect(res).toEqual({
			ok: true,
			platform: "signal",
			vars: ["SIGNAL_API_KEY"],
		});
		const body = readFileSync(join(home, ".env"), "utf8");
		expect(body).toContain("SIGNAL_API_KEY=signal-key-7");
		expect(lines.join("\n")).not.toContain("signal-key-7");
	});
});

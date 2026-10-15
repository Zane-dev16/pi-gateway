// entrypoints/setup-cli.test — terminal setup behavior contracts. Drives
// runSetupCommand the way the bin does and asserts files plus stdout
// literally: exit codes, .env bytes, 0600 mode, and that secrets never leak
// into printed lines.

import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { runSetupCommand } from "./setup-cli.js";

const GOOD_TOKEN = `123456:${"a".repeat(35)}`;

let home: string;
let savedEnv: Record<string, string | undefined>;

const TOUCHED = [
	"TELEGRAM_BOT_TOKEN",
	"TELEGRAM_ALLOWED_USERS",
	"TELEGRAM_ALLOW_ALL_USERS",
	"TELEGRAM_HOME_CHANNEL",
	"DISCORD_BOT_TOKEN",
	"PI_GATEWAY_PLATFORMS",
];

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "setup-cli-home-"));
	savedEnv = {};
	for (const key of TOUCHED) savedEnv[key] = process.env[key];
	for (const key of TOUCHED) delete process.env[key];
});

afterEach(() => {
	for (const key of TOUCHED) {
		if (savedEnv[key] === undefined) delete process.env[key];
		else process.env[key] = savedEnv[key] as string;
	}
	rmSync(home, { recursive: true, force: true });
});

function linesOf(result: { output: string[] }): string[] {
	return result.output;
}

describe("--list", () => {
	it("prints every platform with its required vars and writes nothing", async () => {
		const result = await runSetupCommand({ home, argv: ["--list"] });
		expect(result.exitCode).toBe(0);
		expect(linesOf(result)).toContain("telegram — requires TELEGRAM_BOT_TOKEN");
		expect(linesOf(result)).toContain("discord — requires DISCORD_BOT_TOKEN");
		expect(existsSync(join(home, ".env"))).toBe(false);
	});
});

describe("--status", () => {
	it("reports not configured plus missing vars when the file is empty", async () => {
		const result = await runSetupCommand({ home, argv: ["--status"] });
		expect(result.exitCode).toBe(0);
		expect(linesOf(result)).toContain(
			"telegram: not configured (missing: TELEGRAM_BOT_TOKEN)",
		);
		expect(existsSync(join(home, ".env"))).toBe(false);
	});

	it("lets an exported var win for the reading process", async () => {
		process.env["TELEGRAM_BOT_TOKEN"] = GOOD_TOKEN;
		const listed = await runSetupCommand({ home, argv: ["--status"] });
		expect(linesOf(listed)).toContain("telegram: configured");
		delete process.env["TELEGRAM_BOT_TOKEN"];
		const relisted = await runSetupCommand({ home, argv: ["--status"] });
		expect(linesOf(relisted)).toContain(
			"telegram: not configured (missing: TELEGRAM_BOT_TOKEN)",
		);
	});

	it("honors the platform allowlist filter", async () => {
		process.env["PI_GATEWAY_PLATFORMS"] = "telegram";
		const result = await runSetupCommand({ home, argv: ["--list"] });
		expect(result.exitCode).toBe(0);
		expect(linesOf(result)).toHaveLength(1);
		expect(linesOf(result)[0]).toContain("telegram");
	});
});

describe("non-interactive telegram", () => {
	it("writes token plus allowlist plus home channel at 0600", async () => {
		const result = await runSetupCommand(
			{
				home,
				argv: [
					"--platform",
					"telegram",
					"--token",
					GOOD_TOKEN,
					"--allowlist",
					"111,222",
					"--home-channel",
					"111",
					"--yes",
				],
			},
			{ validateTelegram: async () => ({ ok: true }) },
		);
		expect(result.exitCode).toBe(0);
		const envPath = join(home, ".env");
		const body = readFileSync(envPath, "utf8");
		expect(body).toContain(`TELEGRAM_BOT_TOKEN=${GOOD_TOKEN}`);
		expect(body).toContain("TELEGRAM_ALLOWED_USERS=111,222");
		expect(body).toContain("TELEGRAM_HOME_CHANNEL=111");
		expect(statSync(envPath).mode & 0o777).toBe(0o600);
		for (const line of linesOf(result)) {
			expect(line).not.toContain(GOOD_TOKEN);
		}
	});

	it("rejects a missing token with usage and writes nothing", async () => {
		const result = await runSetupCommand(
			{ home, argv: ["--platform", "telegram", "--yes"] },
			{ validateTelegram: async () => ({ ok: true }) },
		);
		expect(result.exitCode).toBe(2);
		expect(existsSync(join(home, ".env"))).toBe(false);
	});

	it("rejects a malformed token with usage and writes nothing", async () => {
		const result = await runSetupCommand(
			{
				home,
				argv: ["--platform", "telegram", "--token", "nope", "--yes"],
			},
			{ validateTelegram: async () => ({ ok: true }) },
		);
		expect(result.exitCode).toBe(2);
		expect(existsSync(join(home, ".env"))).toBe(false);
	});

	it("rejects an empty allowlist without the explicit open flag", async () => {
		const result = await runSetupCommand(
			{ home, argv: ["--platform", "telegram", "--token", GOOD_TOKEN, "--yes"] },
			{ validateTelegram: async () => ({ ok: true }) },
		);
		expect(result.exitCode).toBe(2);
		expect(existsSync(join(home, ".env"))).toBe(false);
	});

	it("writes the allow-all flag when the open flag is passed", async () => {
		const result = await runSetupCommand(
			{
				home,
				argv: [
					"--platform",
					"telegram",
					"--token",
					GOOD_TOKEN,
					"--allow-open",
					"--yes",
				],
			},
			{ validateTelegram: async () => ({ ok: true }) },
		);
		expect(result.exitCode).toBe(0);
		expect(readFileSync(join(home, ".env"), "utf8")).toContain(
			"TELEGRAM_ALLOW_ALL_USERS=true",
		);
	});

	it("fails closed on a live-check error with no partial write", async () => {
		const result = await runSetupCommand(
			{
				home,
				argv: [
					"--platform",
					"telegram",
					"--token",
					GOOD_TOKEN,
					"--allowlist",
					"111",
					"--yes",
				],
			},
			{
				validateTelegram: async () => ({
					ok: false,
					error: "telegram: validation failed (Unauthorized)",
				}),
			},
		);
		expect(result.exitCode).toBe(1);
		expect(existsSync(join(home, ".env"))).toBe(false);
	});

	it("rejects an unknown platform with usage", async () => {
		const result = await runSetupCommand(
			{ home, argv: ["--platform", "nope", "--yes"] },
			{ validateTelegram: async () => ({ ok: true }) },
		);
		expect(result.exitCode).toBe(2);
	});
});

describe("tty rules", () => {
	it("exits 2 without --yes when stdin is not a TTY", async () => {
		const result = await runSetupCommand({ home, argv: [] }, { isTTY: false });
		expect(result.exitCode).toBe(2);
		expect(existsSync(join(home, ".env"))).toBe(false);
	});
});

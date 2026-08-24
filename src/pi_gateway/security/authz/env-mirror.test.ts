// Behavior contracts for the allowlist mirror — byte-exact round-trips
// through the secretscope .env parser (the same parser scoped authz reads
// use), atomic-write shape, and 0600 persistence.

import {
	existsSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	statSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { loadEnvFile } from "../secretscope/index.js";
import { fileAllowlistMirror } from "./index.js";

let dir: string;
let envPath: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-authz-mirror-"));
	envPath = join(dir, ".env");
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

describe("fileAllowlistMirror", () => {
	it("reads undefined on a missing file and creates it on first write at 0600", () => {
		const mirror = fileAllowlistMirror(envPath);
		expect(mirror.readVar("TELEGRAM_ALLOWED_USERS")).toBeUndefined();
		mirror.writeVar("TELEGRAM_ALLOWED_USERS", "op1");
		expect(existsSync(envPath)).toBe(true);
		expect(statSync(envPath).mode & 0o777).toBe(0o600);
		expect(mirror.readVar("TELEGRAM_ALLOWED_USERS")).toBe("op1");
	});

	it("preserves every unrelated line verbatim across writes", () => {
		const mirror = fileAllowlistMirror(envPath);
		const original = [
			"# operator comment — do not lose",
			"",
			"DISCORD_BOT_TOKEN=abc123",
			"export TELEGRAM_ALLOWED_USERS=old1",
			"TELEGRAM_ALLOWED_USERS=old2",
			"EMPTY_LINE_BELOW=yes",
			"",
		].join("\n");
		writeFileSync(envPath, original);

		mirror.writeVar("TELEGRAM_ALLOWED_USERS", "new");

		const text = readFileSync(envPath, "utf8") as string;
		expect(text).toContain("# operator comment — do not lose");
		expect(text).toContain("DISCORD_BOT_TOKEN=abc123");
		expect(text).toContain("EMPTY_LINE_BELOW=yes");
		// Last occurrence wins and earlier duplicates collapse to ONE line.
		const keyLines = text
			.split("\n")
			.filter((l) => l.includes("TELEGRAM_ALLOWED_USERS="));
		expect(keyLines).toHaveLength(1);
		expect(keyLines[0]).toBe("TELEGRAM_ALLOWED_USERS=new");
		// The parsed view agrees.
		const parsed = loadEnvFile(envPath);
		expect(parsed.get("TELEGRAM_ALLOWED_USERS")).toBe("new");
		expect(parsed.get("DISCORD_BOT_TOKEN")).toBe("abc123");
	});

	it("round-trips values that need quoting through the engine's parser", () => {
		const mirror = fileAllowlistMirror(envPath);
		const value = 'has space, "quote" and #hash';
		mirror.writeVar("SIGNAL_ALLOWED_USERS", value);
		expect(loadEnvFile(envPath).get("SIGNAL_ALLOWED_USERS")).toBe(value);
	});

	it("removeVar drops every occurrence and keeps the rest readable", () => {
		writeFileSync(
			envPath,
			[
				"A=1",
				"SLACK_ALLOWED_USERS=x,y",
				"# mid comment",
				"slack_allowed_users_lower=z",
				"SLACK_ALLOWED_USERS=y,z",
			].join("\n"),
		);
		const mirror = fileAllowlistMirror(envPath);
		mirror.removeVar("SLACK_ALLOWED_USERS");
		const text = readFileSync(envPath, "utf8");
		expect(text).not.toContain("SLACK_ALLOWED_USERS=");
		expect(text).toContain("# mid comment");
		expect(text).toContain("slack_allowed_users_lower=z"); // case-sensitive keys
		expect(mirror.readVar("SLACK_ALLOWED_USERS")).toBeUndefined();
		expect(mirror.readVar("A")).toBe("1");
	});

	it("removeVar is a no-op on a missing file (never creates it)", () => {
		fileAllowlistMirror(envPath).removeVar("X");
		expect(existsSync(envPath)).toBe(false);
	});
});

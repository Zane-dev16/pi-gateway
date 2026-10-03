// entrypoints/setup-chat.test — DEC-081 renderer-two behavior contracts.
//
// Drives ChatSetupDriver the way the Telegram /setup conversation will:
// a run() promise plus routed chat messages. Assertions observe what the
// operator sees: .env bytes at 0600, sent lines with names but never
// values, secret-message deletion, cancellation, and no transcript files.

import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	ChatSetupDriver,
	type ChatSetupMessage,
	type ChatSetupTransport,
	type ChatSetupOptions,
} from "./setup-chat.js";
import type { SetupPlatformSpec } from "../pi_gateway/setup/index.js";

const SECRET = "chat-secret-token-9d4e";
const SECRET_ROTATED = "chat-secret-token-77aa";

const TELEGRAM_LIKE: SetupPlatformSpec = {
	name: "telegram",
	required: [{ name: "TELEGRAM_BOT_TOKEN", password: true }],
	optional: [{ name: "TELEGRAM_ALLOWED_USERS", optional: true }],
};

const SECOND: SetupPlatformSpec = {
	name: "signal",
	required: [{ name: "SIGNAL_API_KEY", password: true }],
};

const CHAT = "agent:main:telegram:dm:7";

let home: string;
let driver: ChatSetupDriver;
let sent: string[];
let deleted: string[];
let transport: ChatSetupTransport;

const pump = (): Promise<void> =>
	new Promise<void>((resolve) => {
		setImmediate(resolve);
	});

async function settle(times = 3): Promise<void> {
	for (let i = 0; i < times; i += 1) {
		await pump();
	}
}

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "setup-chat-"));
	driver = new ChatSetupDriver();
	sent = [];
	deleted = [];
	transport = {
		send: (text: string) => {
			sent.push(text);
		},
		deleteMessage: (id: string) => {
			deleted.push(id);
		},
	};
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

function answer(id: string, text: string): boolean {
	return driver.route(CHAT, { id, text } satisfies ChatSetupMessage);
}

function okValidator() {
	return async () => ({ ok: true as const });
}

const chatOpts = (): ChatSetupOptions => ({
	specs: [TELEGRAM_LIKE],
	validate: okValidator(),
});

function sentSecrets(): string[] {
	return sent.filter(
		(l) => l.includes(SECRET) || l.includes(SECRET_ROTATED),
	);
}

describe("chat setup conversation", () => {
	it("prompts over chat, deletes the secret reply, writes .env at 0600", async () => {
		const runP = driver.run(home, CHAT, transport, chatOpts());
		await settle();
		expect(answer("m1", SECRET)).toBe(true);
		await settle();
		expect(answer("m2", "111,222")).toBe(true);
		await settle();
		expect(answer("m3", "yes")).toBe(true);
		const res = await runP;
		expect(res).toEqual({
			ok: true,
			platform: "telegram",
			vars: ["TELEGRAM_BOT_TOKEN", "TELEGRAM_ALLOWED_USERS"],
		});

		const body = readFileSync(join(home, ".env"), "utf8");
		expect(body).toContain(`TELEGRAM_BOT_TOKEN=${SECRET}`);
		expect(statSync(join(home, ".env")).mode & 0o777).toBe(0o600);
		expect(sentSecrets()).toEqual([]);
		expect(deleted).toEqual(["m1"]);
		expect(sent.join("\n")).toContain("TELEGRAM_BOT_TOKEN");
	});

	it("re-prompts on unknown platform, then drives the named machine", async () => {
		const runP = driver.run(home, CHAT, transport, {
			specs: [TELEGRAM_LIKE, SECOND],
			validate: async () => ({ ok: true as const }),
		});
		await settle();
		expect(answer("m1", "nope")).toBe(true);
		await settle();
		expect(sent.join("\n")).toContain("unknown platform");
		expect(answer("m2", "2")).toBe(true);
		await settle();
		expect(answer("m3", "signal-key-1")).toBe(true);
		await settle();
		expect(answer("m4", "yes")).toBe(true);
		const res = await runP;
		expect(res).toEqual({
			ok: true,
			platform: "signal",
			vars: ["SIGNAL_API_KEY"],
		});
		expect(readFileSync(join(home, ".env"), "utf8")).toContain(
			"SIGNAL_API_KEY=signal-key-1",
		);
		expect(sent.join("\n")).not.toContain("signal-key-1");
		expect(deleted).toEqual(["m3"]);
	});

	it("/cancel at the secret prompt ends with no write and no echo", async () => {
		const runP = driver.run(home, CHAT, transport, chatOpts());
		await settle();
		expect(answer("m1", "/cancel")).toBe(true);
		const res = await runP;
		expect(res).toEqual({ ok: false, cancelled: true, platform: "telegram" });
		expect(readdirSync(home)).toEqual([]);
		expect(sentSecrets()).toEqual([]);
		expect(deleted).toEqual([]);
	});

	it("declined confirm cancels without writing", async () => {
		const runP = driver.run(home, CHAT, transport, chatOpts());
		await settle();
		expect(answer("m1", SECRET)).toBe(true);
		await settle();
		expect(answer("m2", "")).toBe(true);
		await settle();
		expect(answer("m3", "no")).toBe(true);
		const res = await runP;
		expect(res).toEqual({ ok: false, cancelled: true, platform: "telegram" });
		expect(readdirSync(home)).toEqual([]);
		expect(sentSecrets()).toEqual([]);
	});

	it("validation failure blocks the write and never echoes the value", async () => {
		const runP = driver.run(home, CHAT, transport, {
			specs: [TELEGRAM_LIKE],
			validate: async () => ({ ok: false as const, error: "getMe rejected" }),
		});
		await settle();
		expect(answer("m1", "bad-token-xyz")).toBe(true);
		await settle();
		expect(answer("m2", "")).toBe(true);
		await settle();
		const res = await runP;
		expect(res).toEqual({
			ok: false,
			platform: "telegram",
			error: "getMe rejected",
		});
		expect(readdirSync(home)).toEqual([]);
		expect(sent.join("\n")).not.toContain("bad-token-xyz");
	});

	it("unrouted text is not consumed; a second run on the same chat refuses", async () => {
		expect(driver.route(CHAT, { id: "stray", text: "hello" })).toBe(false);
		const runP = driver.run(home, CHAT, transport, chatOpts());
		await settle();
		const second = await driver.run(home, CHAT, transport, chatOpts());
		expect(second).toEqual({
			ok: false,
			error: "setup already running for this chat",
		});
		expect(answer("m1", "/cancel")).toBe(true);
		const first = await runP;
		expect(first).toEqual({
			ok: false,
			cancelled: true,
			platform: "telegram",
		});
	});

	it("persists nothing besides .env (no chat transcript on disk)", async () => {
		const runP = driver.run(home, CHAT, transport, chatOpts());
		await settle();
		expect(answer("m1", SECRET_ROTATED)).toBe(true);
		await settle();
		expect(answer("m2", "")).toBe(true);
		await settle();
		expect(answer("m3", "y")).toBe(true);
		const res = await runP;
		expect(res.ok).toBe(true);
		expect(readdirSync(home)).toEqual([".env"]);
		expect(sentSecrets()).toEqual([]);
	});
});

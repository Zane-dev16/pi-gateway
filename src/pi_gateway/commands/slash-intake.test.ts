// Unknown-/foo queueing contract (03 §11) + tokenization parity
// (base.py get_command / get_command_args).

import { describe, expect, it } from "vitest";
import type { CommandDef } from "./command-def.js";
import { CommandRegistry } from "./registry.js";
import {
	classifySlashIntake,
	extractSlashArgs,
	extractSlashToken,
} from "./slash-intake.js";

const row = (name: string, extra: Partial<CommandDef>): CommandDef => ({
	name,
	description: `${name} description`,
	category: "Session",
	...extra,
});

const REGISTRY = new CommandRegistry([
	row("new", { aliases: ["reset"] }),
	row("background", { aliases: ["bg", "btw"], argsHint: "<prompt>" }),
]);

describe("extractSlashToken (base.py:get_command parity)", () => {
	it("first word minus slash, lowercased; @mention stripped", () => {
		expect(extractSlashToken("/New hello world")).toEqual({
			command: "new",
			args: "hello world",
		});
		expect(extractSlashToken("/bg@mybot do stuff")).toEqual({
			command: "bg",
			args: "do stuff",
		});
	});

	it("file-path-like words are NOT commands", () => {
		expect(extractSlashToken("/usr/bin/env --version")).toEqual({
			command: null,
			// base.py:get_command_args gates ONLY on the slash prefix — it still
			// reports the post-first-word text even when get_command is None.
			args: "--version",
		});
		expect(extractSlashToken("plain text question")).toEqual({
			command: null,
			args: "",
		});
	});

	it("bare '/cmd' has empty args", () => {
		const { command, args } = extractSlashToken("/stop");
		expect(command).toBe("stop");
		expect(args).toBe("");
	});

	it("get_command_args repairs iOS smart-dash corruption", () => {
		expect(extractSlashArgs("/new \u2014\u2014flag value")).toBe(
			"--flag value",
		);
		expect(extractSlashArgs("/new \u2013x")).toBe("-x");
	});
});

describe("unknown commands queue as TEXT — byte-stable (07 §1.4/§2)", () => {
	const resolve = (name: string | null | undefined) => REGISTRY.resolve(name);

	it('"/foo bar baz" falls back to text with ORIGINAL bytes untouched', () => {
		const raw = "/foo bar baz";
		const intake = classifySlashIntake(resolve, raw);
		expect(intake).toEqual({
			kind: "text",
			reason: "unknown-command",
			text: raw,
		});
		if (intake.kind === "text") expect(intake.text === raw).toBe(true);
	});

	it("known commands resolve with canonical row + dash-repaired args", () => {
		const intake = classifySlashIntake(resolve, "/reset my session");
		expect(intake.kind).toBe("command");
		if (intake.kind === "command") {
			expect(intake.cmd.name).toBe("new");
			expect(intake.args).toBe("my session");
		}
	});

	it("plain text and control-disabled events never consult the registry", () => {
		const intake = classifySlashIntake(resolve, "what is 2+2?");
		expect(intake).toEqual({
			kind: "text",
			reason: "no-slash",
			text: "what is 2+2?",
		});
		const controlled = classifySlashIntake(resolve, "/new", {
			allowGatewayControl: false,
		});
		expect(controlled).toEqual({
			kind: "text",
			reason: "no-slash",
			text: "/new",
		});
	});

	it("whitespace-prefixed commands still parse (lstrip semantics)", () => {
		const intake = classifySlashIntake(resolve, "   /bg ship it");
		expect(intake.kind).toBe("command");
		if (intake.kind === "command") expect(intake.token).toBe("bg");
	});
});

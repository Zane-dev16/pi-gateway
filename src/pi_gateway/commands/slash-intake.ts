// pi_gateway/commands/slash-intake.ts — command-vs-text classification at
// arrival (07 §2 dispatch flow: "unknown command / plain text → agent turn";
// 03 §11 bypass-completeness row: "unknown `/foo` queues as text").
//
// THE INVARIANT (07 §1.4): a RECOGNIZED slash command is never queued — it
// dispatches (or explicitly rejects). Only plain text queues. An UNKNOWN
// "/foo" is not resolvable and therefore IS plain text: the caller must feed
// the ORIGINAL message bytes into the normal busy ladder untouched.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/platforms/base.py:MessageEvent.is_command / get_command /
//     get_command_args → extractSlashToken / extractSlashArgs
//   hermes_cli/commands.py:resolve_command → CommandRegistry.resolve /
//     classifySlashIntake

import type { CommandDef } from "./command-def.js";

export interface SlashToken {
	command: string | null;
	args: string;
}

/**
 * base.py:get_command — first whitespace word minus the leading `/`,
 * lowercased, "@mention" suffix stripped; file-path-like words ("/" inside
 * the name) are NOT commands. Non-command text yields {command: null}.
 */
export function extractSlashToken(text: string | null | undefined): SlashToken {
	const commandText = (text ?? "").replace(/^\s+/, "");
	if (!commandText.startsWith("/")) return { command: null, args: "" };
	const cut = commandText.search(/\s/);
	const head = cut < 0 ? commandText.slice(1) : commandText.slice(1, cut);
	let raw = head.toLowerCase();
	const at = raw.indexOf("@");
	if (at >= 0) raw = raw.slice(0, at);
	if (raw.includes("/")) raw = ""; // valid command names never contain "/"
	return {
		command: raw.length > 0 ? raw : null,
		args: extractSlashArgs(commandText),
	};
}

/** base.py:get_command_args — everything after the first word; iOS dash repair. */
export function extractSlashArgs(commandText: string): string {
	const idx = commandText.search(/\s/);
	if (idx < 0) return "";
	return commandText
		.slice(idx + 1)
		.replaceAll("\u2014\u2014", "--")
		.replaceAll("\u2014", "--")
		.replaceAll("\u2013", "-");
}

export type SlashIntake =
	| {
			kind: "command";
			/** Token exactly as typed (lowercased, mention-stripped), e.g. "bg". */
			token: string;
			/** Canonical registry row that resolved. */
			cmd: CommandDef;
			/** Arguments after the command word (dash-repaired). */
			args: string;
	  }
	| {
			kind: "text";
			reason: "no-slash" | "unparseable" | "unknown-command";
			/**
			 * The message to treat as PLAIN TEXT — byte-identical to the input.
			 * Queue/dispatch THIS string through the normal turn path.
			 */
			text: string;
	  };

/**
 * Classify one inbound message against the live registry. Unknown commands
 * fall back to TEXT with the original bytes preserved — never an error
 * reply, never a queue entry for a recognized command.
 */
export function classifySlashIntake(
	resolve: (rawName: string | null | undefined) => CommandDef | null,
	text: string,
	options: { allowGatewayControl?: boolean } = {},
): SlashIntake {
	if ((options.allowGatewayControl ?? true) === false) {
		return { kind: "text", reason: "no-slash", text };
	}
	const { command, args } = extractSlashToken(text);
	if (command === null) {
		return {
			kind: "text",
			reason:
				command === null && !text.replace(/^\s+/, "").startsWith("/")
					? "no-slash"
					: "unparseable",
			text,
		};
	}
	const cmd = resolve(command);
	if (cmd === null) {
		return { kind: "text", reason: "unknown-command", text };
	}
	return { kind: "command", token: command, cmd, args };
}

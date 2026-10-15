// pi_gateway/setup/prompts — ordered terminal prompt flows for `pi-gateway
// setup`. Plain readline on stdin/stdout only: no TUI library, no Ink, no
// bubbles, so setup works over SSH and in CI logs. Secrets are never echoed
// and never reach print lines; values return through the result map only.
//
// Ctrl-C discipline: one press cancels the current prompt and returns to the
// picker; a second consecutive press, or Ctrl-C at the picker, raises the
// exit-130 flag. Nothing persists until a platform flow completes, so reruns
// and aborts converge on the same file state.

import { createInterface, type Interface } from "node:readline";
import {
	allowAllVarFor,
	statusOf,
	telegramTokenFormatOk,
	type PlatformSpec,
	type VarReader,
} from "./platforms.js";

export interface AskOptions {
	readonly mask?: boolean | undefined;
	/** Picker questions exit 130 on Ctrl-C; platform prompts cancel to picker. */
	readonly onSigint?: "cancel" | "exit130" | undefined;
}

export class PromptTerminal {
	private readonly rl: Interface;
	private pending:
		| {
				readonly resolve: (value: string | undefined) => void;
				readonly onSigint: "cancel" | "exit130";
			}
		| undefined;
	private armed = false;
	private eof = false;
	private closed = false;
	readonly print: (line: string) => void;

	constructor(
		input: NodeJS.ReadableStream = process.stdin,
		output: NodeJS.WritableStream = process.stdout,
		print?: (line: string) => void,
	) {
		this.print =
			print ??
			((line: string) => {
				output.write(`${line}\n`);
			});
		this.rl = createInterface({ input, output, terminal: true });
		this.rl.on("SIGINT", () => this.handleSigint());
		this.rl.on("close", () => {
			this.closed = true;
			const waiting = this.pending;
			this.pending = undefined;
			if (waiting !== undefined) waiting.resolve(undefined);
		});
	}

	private handleSigint(): void {
		const waiting = this.pending;
		this.pending = undefined;
		if (
			waiting?.onSigint === "exit130" ||
			waiting === undefined ||
			this.armed
		) {
			this.exit130 = true;
			waiting?.resolve(undefined);
			return;
		}
		this.armed = true;
		waiting.resolve(undefined);
	}

	/** Set when Ctrl-C must exit the process with code 130. */
	exit130 = false;

	/** One prompt round. Resolves undefined on Ctrl-C cancel or EOF/close. */
	ask(prompt: string, opts: AskOptions = {}): Promise<string | undefined> {
		if (this.closed || this.eof) return Promise.resolve(undefined);
		const mode = opts.onSigint ?? "cancel";
		return new Promise<string | undefined>((resolve) => {
			this.pending = { resolve, onSigint: mode };
			if (opts.mask === true) {
				const anyRl = this.rl as unknown as {
					_writeToOutput(s: string): void;
					output: NodeJS.WritableStream;
					line: string;
				};
				const original = anyRl._writeToOutput.bind(anyRl);
				const redraw = () => {
					anyRl.output.write(`\x1b[2K\r${prompt}`);
				};
				anyRl._writeToOutput = (chunk: string) => {
					if (this.pending === undefined) return;
					if (chunk === prompt || chunk.endsWith(prompt)) {
						original(chunk);
						return;
					}
					redraw();
				};
				this.rl.question(prompt, (answer) => {
					anyRl._writeToOutput = original;
					this.pending = undefined;
					this.armed = false;
					this.print("");
					resolve(answer);
				});
			} else {
				this.rl.question(prompt, (answer) => {
					this.pending = undefined;
					this.armed = false;
					resolve(answer);
				});
			}
		});
	}

	close(): void {
		try {
			this.rl.close();
		} catch {
			/* close is best-effort */
		}
	}
}

export type LiveCheck = (
	token: string,
) => Promise<{ readonly ok: true } | { readonly ok: false; readonly error: string }>;

export interface FlowDeps {
	readonly term: PromptTerminal;
	readonly read: VarReader;
	readonly validateTelegram: LiveCheck;
	readonly validateDiscord: LiveCheck;
}

export type FlowOutcome =
	| {
			readonly completed: true;
			readonly values: ReadonlyMap<string, string>;
			readonly notes: readonly string[];
	  }
	| { readonly completed: false; readonly reason: "cancelled" | "declined" };

function isYes(raw: string | undefined): boolean {
	const v = (raw ?? "").trim().toLowerCase();
	return v === "y" || v === "yes";
}

/**
 * One platform round in spec order: reconfigure gate, instructions, masked
 * credential prompts, format plus live checks, allowlist nudge, home-channel
 * offer, unauthorized handling. Returns values only; the caller writes once.
 */
export async function runPlatformFlow(
	spec: PlatformSpec,
	deps: FlowDeps,
): Promise<FlowOutcome> {
	const { term, read } = deps;
	const notes: string[] = [];
	const stored = new Map<string, string>();
	for (const v of spec.required) {
		const current = read(v.name);
		if (current !== undefined && current.trim() !== "") {
			stored.set(v.name, current);
		}
	}
	const report = statusOf(spec, read);
	if (report.status !== "not configured") {
		const answer = await term.ask(
			`${spec.name} is already configured. Reconfigure? [y/N] `,
		);
		if (term.exit130) return { completed: false, reason: "cancelled" };
		if (!isYes(answer)) return { completed: false, reason: "declined" };
	}

	term.print(spec.instructions);

	const values = new Map<string, string>();
	for (const v of spec.required) {
		const label = `${v.name}: `;
		let answer = await term.ask(label, {
			...(v.password === true ? { mask: true as const } : {}),
		});
		if (term.exit130) return { completed: false, reason: "cancelled" };
		if (answer === undefined) return { completed: false, reason: "cancelled" };
		if (answer.trim() === "" && stored.get(v.name) !== undefined) {
			values.set(v.name, stored.get(v.name) as string);
			continue;
		}
		if (answer.trim() === "" && stored.get(v.name) === undefined) {
			answer = await term.ask(label, {
				...(v.password === true ? { mask: true as const } : {}),
			});
			if (term.exit130) return { completed: false, reason: "cancelled" };
			if (answer === undefined || answer.trim() === "") {
				return { completed: false, reason: "cancelled" };
			}
		}
		values.set(v.name, (answer as string).trim());
	}

	if (spec.live === "telegram") {
		for (;;) {
			const token = values.get("TELEGRAM_BOT_TOKEN") ?? "";
			if (!telegramTokenFormatOk(token)) {
				term.print("Invalid token format");
				const retry = await term.ask("TELEGRAM_BOT_TOKEN: ", { mask: true });
				if (term.exit130) return { completed: false, reason: "cancelled" };
				if (retry === undefined || retry.trim() === "") {
					return { completed: false, reason: "cancelled" };
				}
				values.set("TELEGRAM_BOT_TOKEN", retry.trim());
				continue;
			}
			const live = await deps.validateTelegram(token);
			if (live.ok) break;
			term.print(live.error);
			const next = await term.ask(
				"Retry live check or save without validation? (retry/save/cancel) [retry] ",
			);
			if (term.exit130) return { completed: false, reason: "cancelled" };
			const choice = (next ?? "").trim().toLowerCase();
			if (choice === "save") {
				notes.push("saved without live validation");
				break;
			}
			if (choice === "cancel") {
				return { completed: false, reason: "cancelled" };
			}
		}
	}

	if (spec.live === "discord") {
		for (;;) {
			const token = values.get("DISCORD_BOT_TOKEN") ?? "";
			const live = await deps.validateDiscord(token);
			if (live.ok) break;
			term.print(live.error);
			const next = await term.ask(
				"Retry live check or save without validation? (retry/save/cancel) [retry] ",
			);
			if (term.exit130) return { completed: false, reason: "cancelled" };
			const choice = (next ?? "").trim().toLowerCase();
			if (choice === "save") {
				notes.push("saved without live validation");
				break;
			}
			if (choice === "cancel") {
				return { completed: false, reason: "cancelled" };
			}
		}
	}

	let allowlist = read(spec.allowlistVar ?? "") ?? "";
	if (spec.allowlistVar !== undefined) {
		if (spec.name === "telegram" && allowlist.trim() === "") {
			term.print(
				"Find your numeric user ID with @userinfobot on Telegram and add it so only you can use the bot.",
			);
		}
		const answer = await term.ask(
			`${spec.allowlistVar} (comma-separated IDs, empty for none): `,
		);
		if (term.exit130) return { completed: false, reason: "cancelled" };
		if (answer === undefined) return { completed: false, reason: "cancelled" };
		allowlist = answer.trim() !== "" ? answer.trim() : allowlist.trim();
		if (allowlist !== "") values.set(spec.allowlistVar, allowlist);
	}

	if (spec.homeChannelVar !== undefined) {
		const storedHome = read(spec.homeChannelVar) ?? "";
		if (allowlist !== "" && storedHome.trim() === "") {
			const first = allowlist.split(",")[0]?.trim() ?? "";
			const answer = await term.ask(
				`${spec.homeChannelVar} [${first}] (empty defers; run /set-home later): `,
			);
			if (term.exit130) return { completed: false, reason: "cancelled" };
			if (answer === undefined) return { completed: false, reason: "cancelled" };
			if (answer.trim() !== "") {
				values.set(spec.homeChannelVar, answer.trim());
			} else if (first !== "") {
				values.set(spec.homeChannelVar, first);
				notes.push("home channel defaulted to the first allowlisted ID");
			} else {
				notes.push("home channel deferred; run /set-home later");
			}
		} else if (storedHome.trim() !== "") {
			const answer = await term.ask(
				`${spec.homeChannelVar} [${storedHome}] (type "clear" to remove, empty keeps): `,
			);
			if (term.exit130) return { completed: false, reason: "cancelled" };
			if (answer === undefined) return { completed: false, reason: "cancelled" };
			const choice = answer.trim().toLowerCase();
			if (choice === "clear" || (choice === "" && spec.name === "discord")) {
				values.set(spec.homeChannelVar, "");
				notes.push("stored home value cleared");
			} else if (choice !== "") {
				values.set(spec.homeChannelVar, answer.trim());
			}
		}
	}

	if (spec.allowlistVar !== undefined && allowlist === "") {
		const answer = await term.ask(
			"No allowlist set. Choose: (1) open access to any user, (2) pairing approval, (3) decline [3]: ",
		);
		if (term.exit130) return { completed: false, reason: "cancelled" };
		const choice = (answer ?? "").trim();
		if (choice === "1") {
			const flag = allowAllVarFor(spec.name);
			if (flag !== undefined) values.set(flag, "true");
			notes.push("open access enabled");
		} else if (choice === "2") {
			notes.push(
				"pairing mode: when a user messages the bot, approve the code shown in the gateway log",
			);
		} else {
			notes.push("allowlist declined; the bot answers nobody until configured");
		}
	}

	return { completed: true, values, notes };
}

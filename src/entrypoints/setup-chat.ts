// entrypoints/setup-chat — DEC-081 renderer two: guided setup as a chat
// conversation (Telegram /setup follow-up).
//
// Data shape first: the SAME runSetupFlow machine drives both surfaces. The
// TUI renderer answers prompts with modal dialogs; this renderer answers
// them with chat messages. The only state is a per-chat pending slot
// (chatId → continuation) — no booleans, no phase flags. The caller passes
// the stored home path, owns routing incoming chat text into route(), and
// owns the transport. Values travel reply-to-writer only, never into sent
// lines; password replies are deleted right after receipt when the transport
// can (Telegram deleteMessage). The driver never writes transcripts — the
// only file touch is the writer's <home>/.env at 0600.

import {
	runSetupFlow,
	writeSetupVars,
	type SetupIO,
	type SetupPlatformSpec,
	type SetupResult,
	type SetupValidator,
	type SetupWriter,
} from "../pi_gateway/setup/index.js";
import { allSetupSpecs, validateSetupLive } from "./setup-wiring.js";

/** One incoming chat message (id enables secret-message deletion). */
export interface ChatSetupMessage {
	readonly id: string;
	readonly text: string;
}

/** Minimal chat surface the driver needs (sender + optional deleter). */
export interface ChatSetupTransport {
	send(text: string): Promise<void> | void;
	deleteMessage?: ((messageId: string) => Promise<void> | void) | undefined;
}

export interface ChatSetupOptions {
	readonly specs?: readonly SetupPlatformSpec[] | undefined;
	readonly validate?: SetupValidator | undefined;
	readonly write?: SetupWriter | undefined;
}

const CANCEL_WORDS = new Set(["/cancel", "cancel"]);

function isCancel(text: string): boolean {
	return CANCEL_WORDS.has(text.trim().toLowerCase());
}

function fireAndForget(send: () => Promise<void> | void): void {
	try {
		const r = send();
		if (r instanceof Promise) r.catch(() => {});
	} catch {
		/* notify is best-effort — the machine already treats it that way */
	}
}

export class ChatSetupDriver {
	private readonly pending = new Map<
		string,
		(msg: ChatSetupMessage | undefined) => void
	>();

	/**
	 * Route one incoming chat message into a pending setup prompt. TRUE
	 * when a prompt consumed it (the caller must not turn-route it).
	 */
	route(chatId: string, msg: ChatSetupMessage): boolean {
		const resolve = this.pending.get(chatId);
		if (resolve === undefined) return false;
		this.pending.delete(chatId);
		resolve(msg);
		return true;
	}

	/** Release a pending prompt without an answer. TRUE when one waited. */
	abort(chatId: string): boolean {
		const resolve = this.pending.get(chatId);
		if (resolve === undefined) return false;
		this.pending.delete(chatId);
		resolve(undefined);
		return true;
	}

	/** Run the setup machine over chat. One run per chat at a time. */
	async run(
		home: string,
		chatId: string,
		transport: ChatSetupTransport,
		opts: ChatSetupOptions = {},
	): Promise<SetupResult> {
		if (this.pending.has(chatId)) {
			return { ok: false, error: "setup already running for this chat" };
		}
		const awaitReply = (): Promise<ChatSetupMessage | undefined> =>
			new Promise<ChatSetupMessage | undefined>((resolve) => {
				this.pending.set(chatId, resolve);
			});
		const io: SetupIO = {
			selectPlatform: async (platforms) => {
				for (;;) {
					await transport.send(
						`setup — choose platform:\n${platforms.map((p, i) => `${String(i + 1)}. ${p}`).join("\n")}\nReply with the number or name (or /cancel).`,
					);
					const msg = await awaitReply();
					if (msg === undefined || isCancel(msg.text)) return undefined;
					const trimmed = msg.text.trim();
					const n = Number(trimmed);
					if (
						Number.isInteger(n) &&
						n >= 1 &&
						n <= platforms.length &&
						platforms[n - 1] !== undefined
					) {
						return platforms[n - 1] as string;
					}
					const named = platforms.find((p) => p === trimmed);
					if (named !== undefined) return named;
					await transport.send(
						`unknown platform ${JSON.stringify(trimmed)} — try again (or /cancel).`,
					);
				}
			},
			inputVar: async (platform, spec) => {
				const secret = spec.password === true;
				await transport.send(
					`setup ${platform} — ${spec.name}\n${spec.description ?? spec.name}\nReply with the value (or /cancel).` +
						(secret
							? "\nThis is a secret: your reply is deleted right after receipt and never echoed."
							: ""),
				);
				const msg = await awaitReply();
				if (msg === undefined || isCancel(msg.text)) return undefined;
				if (secret) {
					try {
						await transport.deleteMessage?.(msg.id);
					} catch {
						/* deletion is best-effort — the value still never echoes */
					}
				}
				return msg.text;
			},
			confirmSave: async (platform, varNames) => {
				await transport.send(
					`setup ${platform} — save\nSave ${String(varNames.length)} vars (${varNames.join(", ")}) to <home>/.env? Reply yes to save (anything else cancels).`,
				);
				const msg = await awaitReply();
				if (msg === undefined) return false;
				const trimmed = msg.text.trim().toLowerCase();
				return trimmed === "yes" || trimmed === "y";
			},
			notify: (message) => {
				fireAndForget(() => transport.send(message));
			},
		};
		return runSetupFlow({
			home,
			platforms: opts.specs ?? allSetupSpecs(),
			io,
			validate: opts.validate ?? validateSetupLive,
			write: opts.write ?? writeSetupVars,
		});
	}
}

// pi_platforms/irc/irc-adapter — THE IRC adapter, ported from the READ-ONLY
// Hermes reference plugin (plugins/platforms/irc/adapter.py) onto the kit
// base. Everything policy-shaped is inherited; this module supplies TRANSPORT
// (the RFC 2812 line protocol over a fake socket) and MANIFEST DATA.
//
// Ported semantics (file:symbol anchors):
//   adapter.py::connect          PASS/NICK/USER → 001 wait (30s) → NickServ
//                                IDENTIFY (+2s settle) → JOIN → mark-connected;
//                                identity lock `server:nick` refuses a second
//                                profile; config-missing is FATAL non-retryable.
//   adapter.py::_handle_line     PING→PONG · 001 adopts server nick · 433
//                                collision suffix ladder · PRIVMSG gates:
//                                self-ignore (case-insensitive) → CTCP ACTION
//                                `/me` conversion → other CTCP dropped →
//                                channel addressing gate (#& channels require
//                                "nick:"/"nick,"/"nick " prefix) → allowlist.
//   adapter.py::send             strip markdown → byte-aware split → one
//                                PRIVMSG per line paced 0.3s apart (excess-flood
//                                budget = manifest data).
//   adapter.py::_receive_loop    connection death ⇒ FATAL(retryable) +
//                                _notify_fatal_error (gateway reconnect watcher).
//
// Divergences are RECORDED, never silent (DEC-026): see PROPOSED-DEC notes in
// irc-world.ts (in-flight-window replay coverage) and in this file at wireSend
// (A19 per-line scrub rides the LIVE send path too — the reference applies it
// only on the standalone sender; the live splitter removes \n via paragraphing
// but not bare \r/\x00).

import {
	ActionHandlerRegistry,
	BasePlatformAdapter,
	CallbackQueryRouter,
	ClarifyPendingStore,
	DELIVERY_FAILED_NOTICE,
	FormattingLadder,
	OneShotPendingStore,
	classifySendError,
	plainTextFallbackBody,
	resolveEnablement,
	sendWithRetry,
	TokenLockManagerSeam,
} from "../kit/index.js";
import { immediateSpawner } from "../../pi_gateway/guards/index.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
	DraftFrameArgs,
	EditOptions,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	TaskSpawner,
	GatewayTask,
} from "../../pi_gateway/guards/index.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ManualClock } from "../persistent-ws/manual-clock.js";
import type { PacingClock } from "./clock.js";

import {
	IRC_INTERLINE_PACING_MS,
	IRC_MAX_MESSAGE_LENGTH_CHARS,
	IRC_NICKSERV_SETTLE_MS,
	IRC_PLUGIN_MANIFEST,
	IRC_REGISTRATION_TIMEOUT_MS,
} from "./manifest.js";
import {
	ctcpActionText,
	extractNick,
	isCtcp,
	isCtcpAction,
	nextNickOnCollision,
	parseIrcMessage,
	safeIrcTarget,
	splitMessageForIrc,
	stripIrcControlChars,
	stripMarkdownForIrc,
} from "./sanitize.js";
import type {
	FakeIrcClientConnection,
	FakeIrcServer,
} from "./fake-irc-server.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const IRC_REGISTRY: CommandRegistry = [
	{
		name: "new",
		aliases: ["reset"],
		busyPolicy: "interrupt_then_dispatch" as const,
		busyHandler: "new",
	},
	{
		name: "stop",
		busyPolicy: "interrupt_then_dispatch" as const,
		busyHandler: "stop",
	},
	{ name: "model", busyPolicy: "reject" as const, busyHandler: "model" },
	{ name: "approve", busyPolicy: "dispatch" },
	{ name: "status", busyPolicy: "dispatch" },
];

/** Held-inbound window cap (polling-family discipline: drop-oldest at 64). */
export const IRC_HELD_INBOUND_MAX = 64;

export interface IrcAdapterConfig {
	server?: string | undefined;
	port?: number | undefined;
	nickname?: string | undefined;
	channel?: string | undefined;
	useTls?: boolean | undefined;
	serverPassword?: string | undefined;
	nickservPassword?: string | undefined;
	allowedUsers?: readonly string[] | undefined;
	maxMessageLength?: number | undefined;
}

export interface IrcAdapterDeps {
	fakeServer: FakeIrcServer;
	clock?: PacingClock | undefined;
	secretReader?: ScopedSecretReader | undefined;
	spawner?: TaskSpawner | undefined;
	manifestName?: string | undefined;
	/** Harness-scale budget override (subjects pass 64 like the references). */
	scalarMaxUnits?: number | undefined;
	config?: IrcAdapterConfig | undefined;
	/** Allowlist override (subject seam); env IRC_ALLOWED_USERS still wins. */
	allowedUsers?: readonly string[] | undefined;
	/** Gateway reconnect-watcher seam (_notify_fatal_error parity). */
	onFatalNotify?: (() => void) | undefined;
	/**
	 * LIE-SCAN datum ONLY (signal-port parity): flips supportsDraftStreaming
	 * so the streaming-family exclusion probe ADMITS rows that then fail
	 * against seal reality. Production construction leaves it false.
	 */
	declaredDraftStreaming?: boolean | undefined;
}

export class IrcAdapter
	extends BasePlatformAdapter
	implements StreamEgressAdapter
{
	readonly pluginManifest = IRC_PLUGIN_MANIFEST;
	readonly fakeServer: FakeIrcServer;
	readonly clock: PacingClock | undefined;

	private readonly cp: EgressChokepoint;
	private readonly secretReader: ScopedSecretReader;
	private readonly spawn: TaskSpawner;
	private readonly deps: IrcAdapterDeps;

	// ── resolved config (env wins over extra — adapter.py __init__) ──
	readonly serverName: string;
	readonly port: number;
	readonly nickname: string;
	readonly channel: string;
	readonly maxMessageLength: number;
	readonly allowedUsersLower: ReadonlySet<string>;
	private readonly serverPassword: string;
	private readonly nickservPassword: string;

	// ── runtime state ──
	private conn: FakeIrcClientConnection | null = null;
	private registered = false;
	/** Event-driven RPL_WELCOME waiter (resolved by _handle_line 001). */
	private registrationWaiter: {
		promise: Promise<void>;
		resolve: () => void;
	} | null = null;
	private currentNick: string;
	private connectingNow = false;
	private connectSession = 0;

	connected = false;
	collisionCount = 0;
	/** Observability: what fed recovery (connect / connection_lost / holds). */
	readonly recoveryLog: string[] = [];
	/** Sizes of completed held-inbound drains, in order. */
	readonly redispatchLog: number[] = [];
	lastCapturedRetryAfterSeconds: number | null = null;
	/** Structured fatal codes, in order (registration_timeout, 433 latches…). */
	readonly fatalCodes: Array<{
		code: string;
		detail: string;
		retryable: boolean;
	}> = [];

	private readonly heldInbound: IncomingEvent[] = [];

	// ── interactive surfaces (kit census posture; no native taps on IRC) ──
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];
	private routerResolved: string[] = [];
	private readonly clarifyArmedSet = new Set<string>();
	private allowAllClickers = true;

	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};
	private holding = false;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	/** Bound by the subject: the shared harness wire PRIVMSG lane. */
	wireTransmitPrivmsg: (
		target: string,
		line: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });
	/** Bound by the subject: reads the latest captured send for a chat. */
	lastSendContentReader: (chatId: string) => string = () => "";
	richScriptedProbe: () => boolean = () => false;
	wireTransmitRich: (
		content: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({
			success: false,
			error: "sendRichMessage: method not found",
		});

	constructor(deps: IrcAdapterDeps) {
		super({
			manifestName: deps.manifestName ?? "irc",
			capabilities: IRC_PLUGIN_MANIFEST.capabilities,
			lengthUnit: "chars", // Python len() parity — code points
			scalarMaxUnits:
				deps.scalarMaxUnits ??
				deps.config?.maxMessageLength ??
				IRC_MAX_MESSAGE_LENGTH_CHARS,
		});
		this.deps = deps;
		this.fakeServer = deps.fakeServer;
		this.clock = deps.clock;
		this.secretReader = deps.secretReader ?? ((name) => process.env[name]);
		this.spawn = deps.spawner ?? immediateSpawner();

		const cfg = deps.config ?? {};
		const env = (k: string): string | undefined => this.secretReader(k);
		this.serverName = env("IRC_SERVER") ?? cfg.server ?? "";
		const rawPort = env("IRC_PORT") ?? cfg.port;
		const parsedPort = Number(rawPort);
		this.port =
			rawPort === undefined || rawPort === "" || !Number.isFinite(parsedPort)
				? 6697
				: parsedPort;
		this.nickname = env("IRC_NICKNAME") ?? cfg.nickname ?? "hermes-bot";
		this.channel = env("IRC_CHANNEL") ?? cfg.channel ?? "";
		this.serverPassword =
			env("IRC_SERVER_PASSWORD") ?? cfg.serverPassword ?? "";
		this.nickservPassword =
			env("IRC_NICKSERV_PASSWORD") ?? cfg.nickservPassword ?? "";
		const allowedRaw = env("IRC_ALLOWED_USERS");
		const allowedSource = allowedRaw
			? [allowedRaw]
			: (deps.allowedUsers ?? ((cfg.allowedUsers ?? []) as readonly string[]));
		const allowedList = (Array.isArray(allowedSource) ? allowedSource : [])
			.flatMap((entry) => String(entry).split(","))
			.map((u) => u.trim().toLowerCase())
			.filter((u) => u.length > 0);
		this.allowedUsersLower = new Set(allowedList);
		const configuredMax = cfg.maxMessageLength;
		this.maxMessageLength =
			configuredMax === undefined
				? IRC_MAX_MESSAGE_LENGTH_CHARS
				: Number(configuredMax);
		this.currentNick = this.nickname;

		// §11 step 3/4: missing required secret ⇒ LOUD disable at construction
		// (Hermes refuses at connect(); the kit expresses the same posture at
		// construction so /status shows the reason instead of a silent skip).
		const enablement = resolveEnablement(
			IRC_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}
		for (const secret of [this.serverPassword, this.nickservPassword]) {
			if (secret.length > 0) this.registerLogSecret(secret);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: (chatId) =>
				this.deps.declaredDraftStreaming === true &&
				this.isMessageChats.has(String(chatId)),
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async () => ({ success: false, error: "Not supported" }),
			transmitSeal: async () => ({ success: false, error: "Not supported" }),
		});

		this.router = new CallbackQueryRouter({
			stores: {
				approvals: this.approvals,
				slashConfirms: this.slashConfirms,
				appr: this.appr,
				clarify: this.clarify,
			},
			authorizer: () => this.allowAllClickers,
			onExecApproval: async (sessionKey) => {
				this.resolvedFamilies.push("ea");
				this.routerResolved.push(`ea:${sessionKey}`);
				return "ok";
			},
			onSlashConfirm: async (sessionKey, _id, choice) => {
				this.resolvedFamilies.push("sc");
				this.routerResolved.push(`sc:${sessionKey}:${choice}`);
				return "ok";
			},
			onClarifyChoice: async (sessionKey, _id, idx) => {
				this.resolvedFamilies.push("cl");
				this.routerResolved.push(`cl:${sessionKey}:${idx}`);
				return `answer-${idx}`;
			},
			onWhatsappApproval: async (sessionKey) => {
				this.resolvedFamilies.push("appr");
				this.routerResolved.push(`appr:${sessionKey}`);
				return "ok";
			},
			onPickerNav: async (parsed) => ({ answerText: `nav:${parsed.family}` }),
		});
	}

	// ── lie-scan probe (DEC-006 METHOD, not a flag) ──
	private readonly isMessageChats = new Set<string>();
	markStreamIsMessage(chatId: string): void {
		this.isMessageChats.add(chatId);
	}
	override supportsDraftStreaming(): boolean {
		return this.deps.declaredDraftStreaming === true;
	}

	get isConnected(): boolean {
		return this.connected;
	}

	get phase(): "idle" | "connecting" | "live" | "fatal" {
		if (this.lifecycle.state === "fatal") return "fatal";
		if (this.connected) return "live";
		if (this.connectingNow) return "connecting";
		return "idle";
	}

	// ══════════════════════════════════════════════════════════════════════
	// Connection lifecycle (adapter.py::connect)
	// ══════════════════════════════════════════════════════════════════════

	override async connect(opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		const session = ++this.connectSession;
		if (!this.serverName || !this.channel) {
			this.logger?.error?.("[irc] server and channel must be configured");
			this.markFatal(
				"config_missing",
				"IRC_SERVER and IRC_CHANNEL must be set",
				false,
			);
			return false;
		}

		// Identity lock (`{server}:{nick}`) — two profiles cannot share an IRC
		// identity (adapter.py acquire_scoped_lock parity).
		try {
			this.acquireCredentialLock(
				this.lockManager,
				"irc-identity",
				this.identityCredentialId(),
				this.manifestName,
			);
		} catch {
			this.logger?.error?.(
				`[irc] ${this.nickname}@${this.fakeServer.address} already in use by another profile`,
			);
			this.markFatal(
				"lock_conflict",
				"IRC identity in use by another profile",
				false,
			);
			return false;
		}

		this.connectingNow = true;
		let conn: FakeIrcClientConnection;
		try {
			conn = this.fakeServer.connect({
				onLine: (line) => {
					void this.handleLine(line);
				},
				onClose: () => {
					this.handleConnectionLost();
				},
			});
		} catch (err) {
			this.connectingNow = false;
			this.logger?.error?.(
				`[irc] failed to connect to ${this.serverName}:${this.port} — ${brief(err)}`,
			);
			this.markFatal("connect_failed", brief(err), true);
			return false;
		}
		this.conn = conn;

		// Registration sequence (PASS → NICK → USER), passwords scrubbed (A19).
		if (this.serverPassword.length > 0) {
			await this.sendRaw(`PASS ${stripIrcControlChars(this.serverPassword)}`);
		}
		await this.sendRaw(`NICK ${this.currentNick}`);
		await this.sendRaw(`USER ${this.nickname} 0 * :Hermes Agent`);

		// Wait for RPL_WELCOME under the injected-clock deadline. Event-driven:
		// the 001 handler resolves the waiter; only the deadline timer can time
		// the wait out (no polling loops — they starve microtask chains).
		this.registrationWaiter = this.makeWaiter();
		const outcome = await Promise.race([
			this.registrationWaiter.promise.then(() => "welcome" as const),
			(async () => {
				await this.sleepMs(IRC_REGISTRATION_TIMEOUT_MS);
				// Let synchronous-delivery microtask chains (433 ladder → second
				// NICK → 001) settle inside an auto-advancing clock before
				// declaring the wait dead.
				for (let i = 0; i < 8; i++) {
					await new Promise<void>((r) => setImmediate(r));
					if (this.registered) break;
				}
				return "timeout" as const;
			})(),
		]);
		this.registrationWaiter = null;
		if (session !== this.connectSession || this.conn !== conn) {
			return false; // superseded by a newer session — do not touch state
		}
		const timedOut = outcome === "timeout" && !this.registered;
		if (timedOut) {
			this.logger?.error?.("[irc] registration timed out");
			await this.disconnect();
			this.markFatal(
				"registration_timeout",
				"IRC server did not send RPL_WELCOME",
				true,
			);
			this.connectingNow = false;
			return false;
		}
		void conn;

		// NickServ identification + fixed settle sleep.
		if (this.nickservPassword.length > 0 && this.conn !== null) {
			await this.sendRaw(
				`PRIVMSG NickServ :IDENTIFY ${stripIrcControlChars(this.nickservPassword)}`,
			);
			await this.sleepMs(IRC_NICKSERV_SETTLE_MS);
		}

		// Join the configured channel.
		if (this.conn !== null) await this.sendRaw(`JOIN ${this.channel}`);

		this.connected = true;
		this.connectingNow = false;
		this.recoveryLog.push(opts.isReconnect ? "reconnected" : "connected");
		this.logger?.info?.(
			`[irc] connected to ${this.serverName}:${this.port} as ${this.currentNick}, joined ${this.channel}`,
		);
		// _mark_connected parity: drain the held-inbound queue.
		this.drainHeldInbound();
		return true;
	}

	override async disconnect(): Promise<void> {
		// State clears SYNCHRONOUSLY — a suspended tail must never clobber a
		// successor session mid-registration.
		this.connected = false;
		this.registered = false;
		this.registrationWaiter = null;
		const conn = this.conn;
		this.conn = null;
		this.connectingNow = false;
		if (conn !== null && !conn.closedByServer) {
			try {
				conn.write("QUIT :Hermes Agent shutting down");
			} catch {
				/* best-effort teardown */
			}
		}
	}

	// ── raw line I/O ──

	private async sendRaw(line: string): Promise<void> {
		const conn = this.conn;
		if (conn === null || conn.closedByServer) return;
		conn.write(line);
	}

	private async handleLine(raw: string): Promise<void> {
		const msg = parseIrcMessage(raw);
		switch (msg.command) {
			case "PING": {
				const payload = msg.params[0] ?? "";
				await this.sendRaw(`PONG :${payload}`);
				return;
			}
			case "001": {
				const welcomed = msg.params[0];
				if (welcomed !== undefined) this.currentNick = welcomed;
				this.registered = true;
				this.registrationWaiter?.resolve();
				return;
			}
			case "433": {
				this.collisionCount += 1;
				this.currentNick = nextNickOnCollision(this.nickname, this.currentNick);
				await this.sendRaw(`NICK ${this.currentNick}`);
				return;
			}
			case "PRIVMSG": {
				await this.handlePrivmsg(msg.prefix, msg.params);
				return;
			}
			case "NICK": {
				const changer = extractNick(msg.prefix).toLowerCase();
				const newNick = msg.params[0];
				if (
					changer === this.currentNick.toLowerCase() &&
					newNick !== undefined
				) {
					this.currentNick = newNick;
				}
				return;
			}
			default:
				return; // other numerics recorded only
		}
	}

	/**
	 * PRIVMSG gates — exact adapter.py::_handle_line order: self-ignore →
	 * CTCP ACTION conversion → other CTCP dropped → channel-vs-DM routing →
	 * channel addressing gate → allowlist (case-insensitive) → dispatch.
	 */
	private async handlePrivmsg(prefix: string, params: string[]): Promise<void> {
		if (params.length < 2) return;
		const senderNick = extractNick(prefix);
		const target = params[0];
		let text = params[1];
		if (target === undefined || text === undefined) return;

		if (senderNick.toLowerCase() === this.currentNick.toLowerCase()) return;

		if (isCtcpAction(text)) {
			text = ctcpActionText(text, senderNick); // /me → "* nick action"
		} else if (isCtcp(text)) {
			return; // every other CTCP payload ignored
		}

		const isChannel = target.startsWith("#") || target.startsWith("&");
		const chatId = isChannel ? target : senderNick;
		const chatType = isChannel ? "group" : "dm";

		if (isChannel) {
			let addressed = false;
			for (const addressForm of [
				`${this.currentNick}:`,
				`${this.currentNick},`,
				`${this.currentNick} `,
			]) {
				if (text.toLowerCase().startsWith(addressForm.toLowerCase())) {
					text = text.slice(addressForm.length).trim();
					addressed = true;
					break;
				}
			}
			if (!addressed) return; // unaddressed channel chatter is noise
		}

		if (
			this.allowedUsersLower.size > 0 &&
			!this.allowedUsersLower.has(senderNick.toLowerCase())
		) {
			return;
		}

		const event: IncomingEvent = {
			messageType: "text",
			text,
			source: {
				platform: "irc",
				chatType: chatType as "group" | "dm",
				userId: senderNick,
				chatId,
				chatName: chatId,
			},
		};
		await this.dispatchOrHold(event, `privmsg:${chatId}:${senderNick}`);
	}

	// ══════════════════════════════════════════════════════════════════════
	// In-flight window: events constructed off the wire while NOT live are
	// HELD (bounded, drop-oldest at IRC_HELD_INBOUND_MAX) and redispatched on
	// reconnect — the polling family's ack-before-enqueue discipline applied
	// to the only replay window IRC actually has (PROPOSED-DEC text lives in
	// irc-world.ts).
	// ══════════════════════════════════════════════════════════════════════

	get heldInboundCount(): number {
		return this.heldInbound.length;
	}

	private async dispatchOrHold(
		event: IncomingEvent,
		where: string,
	): Promise<void> {
		if (!this.connected) {
			this.recoveryLog.push(`hold:${where}`);
			if (this.heldInbound.length >= IRC_HELD_INBOUND_MAX) {
				this.heldInbound.shift(); // drop-oldest overflow policy
			}
			this.heldInbound.push(event);
			return;
		}
		await this.deliverInbound(event, sessionKeyFor(event));
	}

	private drainHeldInbound(): void {
		if (this.heldInbound.length === 0) return;
		const drained = [...this.heldInbound];
		this.heldInbound.length = 0;
		this.redispatchLog.push(drained.length);
		for (const event of drained) {
			this.spawn(async () => {
				await this.deliverInbound(event, sessionKeyFor(event));
			});
		}
	}

	private handleConnectionLost(): void {
		if (this.conn === null) return;
		this.conn = null;
		this.registered = false;
		this.connected = false;
		this.recoveryLog.push("connection_lost");
		this.logger?.warn?.("[irc] connection lost, marking disconnected");
		this.markFatal(
			"connection_lost",
			"IRC connection closed unexpectedly",
			true,
		);
		this.deps.onFatalNotify?.();
	}

	// ══════════════════════════════════════════════════════════════════════
	// Guard wiring + egress doors (reference-fixture inheritance)
	// ══════════════════════════════════════════════════════════════════════

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: IRC_REGISTRY,
				messageHandler: async (event, ctx) => {
					const text = event.text ?? `[${String(event.messageType)}]`;
					const sessionKey = String(
						event.metadata?.["gateway_session_key"] ?? "",
					);
					if (this.clarifyArmedSet.has(sessionKey) && !text.startsWith("/")) {
						this.clarifyCaptures.push(text);
						return null;
					}
					this.turnLog.push(text);
					const isInlineDispatch =
						text.startsWith("/") ||
						(ctx.task.cancelRequested() === false && ctx.task.isDone());
					if (!isInlineDispatch) {
						while (this.holding && !ctx.task.cancelRequested()) {
							await Promise.race([
								this.holdGate.then(() => undefined),
								new Promise<void>((r) => setTimeout(r, 1)),
							]);
						}
					}
					ctx.throwIfCancelled();
					return `reply:${text}`;
				},
				sendReply: async (_chatId, text) => {
					this.replyLog.push(text);
				},
			},
			{
				...(spawner === undefined ? {} : { spawner }),
				hasPendingClarify: (key) => this.clarifyArmedSet.has(key),
			},
		);
	}

	get clarifyArmed(): Set<string> {
		return this.clarifyArmedSet;
	}

	holdTurns(on: boolean): void {
		if (on && !this.holding) {
			this.holdGate = new Promise<void>((resolve) => {
				this.releaseHold = resolve;
			});
		}
		this.holding = on;
		if (!on) this.releaseHold();
	}

	async deliverInbound(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		if (String(event.source?.userId ?? "") === "bot-self") return; // self/echo filter
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * §6.3/A15 per-chat pair: harness utf16-marked chats budget 30 CODE UNITS;
	 * production chats use the manifest char budget.
	 */
	protected override chatDescriptorFor(chatId: string):
		| {
				maxMessageLength?: number | undefined;
				lenUnit?: import("../kit/length-policy.js").LengthUnit | undefined;
		  }
		| undefined {
		if (chatId.includes("utf16")) {
			return { maxMessageLength: 30, lenUnit: "utf16" };
		}
		return undefined;
	}

	/**
	 * THE text-send lane (adapter.py::send): sanitize the TARGET (A19), strip
	 * markdown + split per line INSIDE the door (the vendor splitter IS the
	 * chunker — manifest shape delta), pace one PRIVMSG per line 300ms apart
	 * through the injected clock. Scripted SendResult.retryAfter is CAPTURED
	 * so the §6.1 ladder's next delay IS the captured window (authoritative).
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const target = safeIrcTarget(chatId);
		if (target === null) {
			return {
				success: false,
				error: "chat_id contains illegal IRC characters",
			};
		}
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith("(Response formatting failed, plain text:")
		) {
			return Promise.resolve({
				success: false,
				error: "Bad Request: can't parse entities",
			});
		}
		// ONE logical chunk per door admission (the §6.3 planner split upstream).
		// Markdown NEVER reaches IRC (vendor order: strip at send) — but the
		// §6.1 plain-text fallback lane carries ORIGINAL chunk bytes and is
		// therefore scrubbed WITHOUT re-stripping.
		const isPlainLane = content.startsWith(
			"(Response formatting failed, plain text:",
		);
		const clean = isPlainLane
			? stripIrcControlChars(content)
			: stripIrcControlChars(stripMarkdownForIrc(content));
		// adapter.py:359/:297 parity: _split_message SKIPS blank paragraphs, so a
		// chunk whose payload is entirely whitespace ships in the [""] shape —
		// ONE 'PRIVMSG <target> :' empty-trailing line (the A19 scrub has already
		// turned control bytes into spaces). Short-circuiting success here would
		// silently DROP a message the reference puts on the wire.
		const line = clean.trim().length === 0 ? "" : clean;
		const result = await this.wireTransmitPrivmsg(target, line, {
			...metadata,
			irc_privmsg_target: target,
		});
		this.captureRetryAfter(result.retryAfter);
		return result;
	}

	async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		// adapter.py::send — THE vendor splitter IS the chunker and its chunks go
		// out BARE: truncate_message's '(i/n)' scaffold is NEVER applied on IRC
		// (adapter.py:293-297). No label-width reservation, no per-line suffix —
		// appending one would mutate wire-visible content on every long reply.
		const lines = splitMessageForIrc(
			content,
			chatId,
			policy.maxUnits,
			policy.lenFn,
			true,
		);
		if (
			lines.length === 1 &&
			lines[0] === "" &&
			policy.lenFn(content) > policy.maxUnits
		) {
			// hostile-target / unsplittable shape: let the ladder surface it
			return [await this.deliverViaLadder(chatId, content, metadata)];
		}
		const results: SendResult[] = [];
		for (let i = 0; i < lines.length; i++) {
			// adapter.py::send pacing — 300ms between consecutive PRIVMSG lines
			// (excess-flood budget = manifest data), driven by the clock seam.
			if (i > 0) await this.sleepMs(IRC_INTERLINE_PACING_MS);
			results.push(
				await this.deliverViaLadder(chatId, lines[i] ?? "", metadata),
			);
		}
		return results;
	}

	private ladderInstance: FormattingLadder | null = null;
	private ladderChatId = "";

	private ensureLadder(): FormattingLadder {
		if (this.ladderInstance === null) {
			this.ladderInstance = new FormattingLadder({
				tryRich: (content, md) => this.wireRich(content, md),
				sendConverted: (content, md) =>
					this.wireSend(this.ladderChatId, content, md),
				sendPlain: (content, md) =>
					this.wireSend(this.ladderChatId, content, md),
			});
		}
		return this.ladderInstance;
	}

	private async deliverViaLadder(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		this.ladderChatId = chatId;
		const outcome = await this.ensureLadder().sendText(content, metadata);
		if (outcome.success) return outcome;
		if (outcome.tier === "rich") return outcome; // transient rich: NEVER resent

		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		const networkClassified =
			outcome.retryable === true ||
			failureClass === "connect-timeout" ||
			failureClass === "network" ||
			failureClass === "flood";
		if (networkClassified) {
			this.captureRetryAfter(outcome.retryAfter);
			const clock = this.clock;
			const retried = await sendWithRetry(
				content,
				metadata,
				(c, md) => this.wireSend(chatId, c, md),
				{
					maxRetries: 2,
					...(clock === undefined
						? {}
						: { sleep: (ms: number) => this.sleepMs(ms) }),
				},
			);
			if (retried.success) return retried;
			return this.wireSend(chatId, DELIVERY_FAILED_NOTICE, metadata);
		}
		if (failureClass === "formatting") {
			const { forceFormattingError: _ignored, ...plainMeta } = metadata;
			void _ignored;
			return this.wireSend(
				chatId,
				plainTextFallbackBody(content),
				plainMeta as Metadata,
			);
		}
		return outcome;
	}

	private captureRetryAfter(seconds: number | null | undefined): void {
		if (seconds !== null && seconds !== undefined) {
			this.lastCapturedRetryAfterSeconds = seconds;
		}
	}

	async transientRichOutcome(
		_chatId: string,
		content: string,
	): Promise<SendResult> {
		const ladder = new FormattingLadder({
			tryRich: async () => ({ success: false, error: "socket hang up" }),
			sendConverted: async () => ({
				success: false,
				error: "SHOULD-NOT-HAPPEN",
			}),
			sendPlain: async () => ({ success: false, error: "SHOULD-NOT-HAPPEN" }),
		});
		return ladder.sendText(content, {});
	}

	async parseFailureResendContent(
		chatId: string,
		content: string,
	): Promise<string> {
		await this.deliverText(chatId, content, { forceFormattingError: true });
		return this.lastSendContentReader(chatId);
	}

	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (!this.richScriptedProbe()) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.wireTransmitRich(content, metadata);
	}

	protected override wireEdit(
		_chatId: string,
		_messageId: string,
		_content: string,
		_opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		return Promise.resolve({ success: false, error: "Not supported" });
	}

	protected override wireDraft(_args: DraftFrameArgs): Promise<SendResult> {
		return Promise.resolve({ success: false, error: "Not supported" });
	}

	/** adapter.py::send_typing — IRC has no typing indicator: no-op. */
	sendTyping(_chatId: string): void {}

	/** adapter.py::get_chat_info — #& channels are group chats, nicks are DMs. */
	getChatInfo(chatId: string): { name: string; type: "group" | "dm" } {
		return {
			name: chatId,
			type: chatId.startsWith("#") || chatId.startsWith("&") ? "group" : "dm",
		};
	}

	// ── interactive surfaces + identity probes ──

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	routerAuditResolved(): readonly string[] {
		return this.routerResolved;
	}

	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		const credentialId = this.identityCredentialId();
		if (!this.lockHeld) {
			const first = this.acquireCredentialLock(
				this.lockManager,
				"irc-identity",
				credentialId,
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.acquireCredentialLock(
				this.lockManager,
				"irc-identity",
				credentialId,
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf("irc-identity", credentialId);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}

	buildMissingSecretSibling(): IrcAdapter {
		return new IrcAdapter({
			...this.deps,
			manifestName: `${this.manifestName}-no-secret`,
			secretReader: () => undefined,
		});
	}

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}

	// ── seams ──

	private identityCredentialId(): string {
		return `${this.fakeServer.address}:${this.nickname.toLowerCase()}`;
	}

	private makeWaiter(): {
		promise: Promise<void>;
		resolve: () => void;
	} {
		let resolve!: () => void;
		const promise = new Promise<void>((r) => {
			resolve = r;
		});
		return { promise, resolve };
	}

	private async sleepMs(ms: number): Promise<void> {
		if (this.clock !== undefined) {
			await this.clock.sleepMs(ms);
			return;
		}
		// No injected clock (production wiring): real pacing, capped small so
		// accidental un-clocked tests cannot stall the suite (workspace rule:
		// timing claims always run against an INJECTED clock).
		await new Promise<void>((r) => setTimeout(r, Math.min(ms, 5)));
	}

	private markFatal(code: string, detail: string, retryable: boolean): void {
		this.fatalCodes.push({ code, detail, retryable });
		this.lifecycle.markFatal({
			kind: "config_invalid",
			detail: `[${code}]${retryable ? "(retryable)" : ""} ${detail}`,
		});
	}

	/** Fire-and-forget frame through the spawner seam (drain helper). */
	spawnFrame(run: () => Promise<void>): GatewayTask | null | undefined {
		return this.spawn(async () => {
			await run();
		});
	}
}

// ── helpers ──────────────────────────────────────────────────────────────

function brief(err: unknown): string {
	return String(err instanceof Error ? err.message : err).slice(0, 160);
}

function sessionKeyFor(event: IncomingEvent): string {
	return `irc:${String(event.source?.chatId ?? "?")}`;
}

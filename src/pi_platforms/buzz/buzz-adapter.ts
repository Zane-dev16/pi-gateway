// pi_platforms/buzz/buzz-adapter — THE Buzz community-relay adapter, ported
// from the READ-ONLY Hermes plugin plugins/platforms/buzz/adapter.py onto the
// kit base. Everything policy-shaped is inherited or MANIFEST DATA; this
// module supplies TRANSPORT (the injected-CLI polling plane) and the connect
// ladder.
//
// Shape (DEC-002 polling family):
//   - Buzz does NOT speak Nostr itself: EVERYTHING rides the external `buzz`
//     CLI ("JSON in, JSON out") through an INJECTED executor seam — the port
//     NEVER spawns OS children (cli-wire.ts).
//   - The live NIP-42 WebSocket relay loop is a documented probe-computed
//     exclusion (manifest.ts); its PURE crypto core is fully ported in
//     nostr-auth.ts and contract-tested against reference-computed vectors.
//     The transport-MODE resolution stays source-true; the ws establishment is
//     an injectable seam that defaults to "unavailable" (⇒ auto falls back to
//     poll; pinned websocket-required fails connect — source truth).
//
// Source anchors (adapter.py):
//   __init__ (@~300)          → constructor config/env precedence
//   _resolve_cli_path (@~380) → resolveCliPath pure fn
//   _resolve_private_key      → resolvePrivateKeyEvent (env reader first, then credentials chain)
//   _exec_buzz / _run_cli     → runCli (env-only secret carriage)
//   _cli_error_message        → cliErrorMessage ("<cat>: <msg> (exit N)")
//   _parse_json_list          → parseJsonList (tolerant)
//   connect (@~470)           → connect ladder with _set_fatal_error parity
//   send (@~660)              → wireSend (messages send --channel C --content -)
//   send_image (@~667)        → sendImage (--file lane for local files with
//                               caption on stdin; URL/link-text fallback)
//   _poll_loop/_poll_channel  → pollSweep/pollChannel (manual sweeps; injected clock)
//   _seed_channel             → seedChannel (watermark + seen seeding)
//   _discover_dms (#68871)    → discoverDms (dms list + channels-list fallback)
//   _handle_event             → handleEvent — SEEN-COMMIT PRECEDES DISPATCH (pinned)
//   DM latch block comments   → mayReclassifyAsDm/isDirectMessageEvent/maybeLatchDm
//   _is_mentioned/_strip_mention → isMentioned/stripMention
//   _resolve_user_name        → resolveUserName (negative caching)
//   _trim_seen                → trimSeen (OrderedDict popitem(last=false) = Map FIFO delete)

import {
	BasePlatformAdapter,
	type TokenLockManagerSeam,
	resolveEnablement,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	ActionHandlerRegistry,
} from "../kit/index.js";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { DisableReason } from "../kit/lifecycle-state.js";
import {
	REPLY_TO_METADATA_KEY,
	type Metadata,
	type SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { BuzzCliExecutor, BuzzCliResult } from "./cli-wire.js";
import {
	BUZZ_CHAT_KIND,
	BUZZ_DM_DISCOVERY_EVERY,
	BUZZ_FETCH_LIMIT,
	BUZZ_MIN_POLL_INTERVAL_S,
	BUZZ_DEFAULT_POLL_INTERVAL_S,
	BUZZ_PLUGIN_MANIFEST,
	BUZZ_REQUIRE_MENTION_FALSE_TOKENS,
	BUZZ_SEEN_CAP,
	BUZZ_TRANSPORT_MODES,
	validateBuzzTrustBoundary,
	declareBuzzTrustBoundary,
	type BuzzTrustBoundary,
} from "./manifest.js";
import { hexToNpub, normalizeUserRef } from "./nostr-auth.js";

/** adapter.py per-channel state: chat_type + last_ts watermark + seen OrderedDict. */
export interface BuzzChannelState {
	chatType: "group" | "dm";
	lastTs: number;
	/** Insertion-ordered id set — Map preserves order like OrderedDict. */
	seen: Map<string, null>;
}

export interface BuzzExtraConfig {
	relay_url?: unknown;
	cli_path?: unknown;
	channels?: unknown;
	home_channel?: unknown;
	poll_interval?: unknown;
	require_mention?: unknown;
	transport?: unknown;
	allowed_users?: unknown;
	credentials_file?: unknown;
}

export interface BuzzAdapterOptions {
	config?: BuzzExtraConfig | undefined;
	/**
	 * Scoped env/secret reader (_get_scoped_secret parity). Fail-closed: a
	 * scoped miss NEVER borrows process.env (DEC-003/009).
	 */
	secretReader?: ScopedSecretReader | undefined;
	/** THE CLI plane seam — required for any CLI traffic. */
	executor?: BuzzCliExecutor | undefined;
	/** Credentials-file text reader seam (never real fs in tests). */
	credentialsReader?: ((path: string) => string | undefined) | undefined;
	/** Credentials-dir glob seam (~/.config/buzz/*credentials*.json). */
	credentialsLister?: (() => readonly string[]) | undefined;
	/** PATH/file-existence probes behind resolveCliPath (no real fs). */
	pathProbes?:
		| {
				which?: ((bin: string) => string | undefined) | undefined;
				fileExists?: ((path: string) => boolean) | undefined;
		  }
		| undefined;
	/** Local-image existence probe behind sendImage (no real fs in fixtures). */
	imageFileProbe?: ((path: string) => boolean) | undefined;
	/**
	 * WebSocket establishment seam (probe-computed exclusion of the live loop).
	 * Undefined ⇒ unavailable ⇒ auto falls back to poll; transport=websocket
	 * fails connect exactly like a failed NIP-42 handshake.
	 */
	wsStarter?: (() => Promise<boolean>) | undefined;
	nowMs?: (() => number) | undefined;
	scalarMaxUnits?: number | undefined;
	/** Scoped identity lock (connect acquires relay:pubkey when supplied). */
	lockManager?: TokenLockManagerSeam | undefined;
	lockOwner?: string | undefined;
}

export interface BuzzFaultRecord {
	code: string;
	detail: string;
	retryable: boolean;
}

/** Injected fault points — behavior rows model crashes at EXACT seams. */
export interface BuzzHooks {
	/** Fires AFTER dedupe check, BEFORE the seen-commit (fetched-but-uncommitted window). */
	beforeCommit?:
		| ((channelId: string, event: Record<string, unknown>) => void)
		| undefined;
	/** Fires AFTER the seen-commit, right before dispatch (committed-but-undispatched window). */
	beforeDispatch?: ((event: IncomingEvent) => void) | undefined;
}

function escapeRegExp(text: string): string {
	return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** http(s) refs ride as link text; everything else is a local-path candidate. */
function isHttpUrl(ref: string): boolean {
	return /^https?:\/\//i.test(ref);
}

/** Path.expanduser parity for local media sources. */
function expandUserPath(ref: string): string {
	if (ref === "~") return homedir();
	if (ref.startsWith("~/")) return `${homedir()}/${ref.slice(2)}`;
	return ref;
}

/**
 * adapter.py:_resolve_cli_path — configured path (must exist) → `buzz` on
 * PATH → ~/bin/buzz (must exist) → "" so callers raise config errors.
 */
export function resolveCliPath(
	configured: string,
	probes: {
		which?: ((bin: string) => string | undefined) | undefined;
		fileExists?: ((path: string) => boolean) | undefined;
	},
): string {
	const trimmed = (configured ?? "").trim();
	const fileExists = probes.fileExists ?? (() => false);
	if (trimmed.length > 0) return fileExists(trimmed) ? trimmed : "";
	const found = probes.which?.("buzz");
	if (found !== undefined && found.length > 0) return found;
	return fileExists("~/bin/buzz") ? "~/bin/buzz" : "";
}

/** adapter.py:_cli_error_message — stderr JSON error contract parsing. */
export function cliErrorMessage(stderr: string, returncode: number): string {
	const text = (stderr ?? "").trim();
	try {
		const data: unknown = JSON.parse(text);
		if (
			data !== null &&
			typeof data === "object" &&
			typeof (data as Record<string, unknown>)["message"] === "string" &&
			((data as Record<string, unknown>)["message"] as string).length > 0
		) {
			const category =
				typeof (data as Record<string, unknown>)["error"] === "string"
					? ((data as Record<string, unknown>)["error"] as string)
					: "error";
			return `${category}: ${(data as Record<string, unknown>)["message"] as string} (exit ${returncode})`;
		}
	} catch {
		/* fall back to raw stderr */
	}
	return text.length > 0
		? text
		: `buzz CLI failed with exit code ${returncode}`;
}

/** adapter.py:_parse_json_list — tolerant stdout array-of-objects parse. */
export function parseJsonList(stdout: string): Array<Record<string, unknown>> {
	let data: unknown;
	try {
		data = JSON.parse(stdout.length > 0 ? stdout : "[]");
	} catch {
		return [];
	}
	if (!Array.isArray(data)) return [];
	return data.filter(
		(item): item is Record<string, unknown> =>
			item !== null && typeof item === "object" && !Array.isArray(item),
	);
}

/** Conformance command registry — same five-command shape as sibling adapters. */
const BUZZ_REGISTRY: CommandRegistry = [
	{
		name: "new",
		aliases: ["reset"],
		busyPolicy: "interrupt_then_dispatch",
		busyHandler: "new",
	},
	{ name: "stop", busyPolicy: "interrupt_then_dispatch", busyHandler: "stop" },
	{ name: "model", busyPolicy: "reject", busyHandler: "model" },
	{ name: "approve", busyPolicy: "dispatch" },
	{ name: "status", busyPolicy: "dispatch" },
];

function sessionKeyOf(event: IncomingEvent): string {
	return `buzz:${String(event.source?.chatId ?? "unknown")}`;
}

export class BuzzAdapter extends BasePlatformAdapter {
	readonly pluginManifest = BUZZ_PLUGIN_MANIFEST;
	readonly trustBoundary: BuzzTrustBoundary = declareBuzzTrustBoundary();

	// ── config (__init__ parity) ────────────────────────────────────────────────
	readonly relayUrl: string;
	readonly cliPath: string;
	readonly channels: readonly string[];
	readonly homeChannel: string;
	readonly pollInterval: number;
	readonly requireMention: boolean;
	readonly transportMode: "auto" | "websocket" | "poll";
	readonly allowedPubkeys: ReadonlySet<string>;

	private readonly secretReader: ScopedSecretReader;
	private readonly extra: BuzzExtraConfig;
	private readonly executor: BuzzCliExecutor | undefined;
	private readonly credentialsReader:
		| ((path: string) => string | undefined)
		| undefined;
	private readonly credentialsLister: (() => readonly string[]) | undefined;
	private readonly pathProbes: {
		which?: ((bin: string) => string | undefined) | undefined;
		fileExists?: ((path: string) => boolean) | undefined;
	};
	private readonly imageFileExists: (path: string) => boolean;
	/**
	 * WebSocket establishment seam (probe-computed exclusion of the live loop).
	 * MUTABLE so mode-resolution fixtures can flip availability per connect.
	 * Undefined ⇒ unavailable ⇒ auto falls back to poll; transport=websocket
	 * fails connect exactly like a failed NIP-42 handshake.
	 */
	wsStarter: (() => Promise<boolean>) | undefined;
	private readonly nowFn: () => number;
	private readonly lockManager: TokenLockManagerSeam | undefined;
	private readonly lockOwner: string;

	// ── runtime state ────────────────────────────────────────────────────────────
	private privateKey = "";
	selfPubkey = "";
	selfNpub = "";
	displayName = "";
	connectedOnce = false;
	pollLoopActive = false;
	lastTransportUsed: "poll" | "websocket" | null = null;
	private lockHandle: { release(): void } | null = null;
	readonly channelState = new Map<string, BuzzChannelState>();
	readonly channelNames = new Map<string, string>();
	readonly channelMeta = new Map<string, Record<string, unknown>>();
	private readonly userNames = new Map<string, string>();
	pollCount = 0;
	sweepCount = 0;
	sweepErrors = 0;
	/** Poll-channel CLI failures (rc≠0 — contained, never thrown). */
	pollFailures = 0;
	/** Last contained CLI error text (timeout-ladder row asserts the shape). */
	lastCliError = "";
	/** Contained-sweep error text (exception-class faults). */
	lastSweepError = "";
	readonly fatalEvents: BuzzFaultRecord[] = [];
	hooks: BuzzHooks | undefined;
	/** Built IncomingEvents ENQUEUED for dispatch, in order (row observability). */
	readonly inboundEventLog: IncomingEvent[] = [];

	// Subject-bound egress lanes (mattermost/matrix precedent): when bound,
	// wireSend delegates there instead of the CLI plane (harness capture +
	// formatting-rejection script); unset ⇒ real CLI delivery.
	wireTransmitSend:
		| ((
				chatId: string,
				content: string,
				metadata: Metadata,
		  ) => Promise<SendResult>)
		| undefined;
	wireTransmitRich:
		| ((content: string, metadata: Metadata) => Promise<SendResult>)
		| undefined;
	richScriptedProbe: (() => boolean) | undefined;

	constructor(opts: BuzzAdapterOptions = {}) {
		super({
			manifestName: BUZZ_PLUGIN_MANIFEST.name,
			capabilities: BUZZ_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? 4096,
		});
		this.extra = opts.config ?? {};
		this.secretReader = opts.secretReader ?? (() => undefined);
		this.executor = opts.executor;
		this.credentialsReader = opts.credentialsReader;
		this.credentialsLister = opts.credentialsLister;
		this.pathProbes = opts.pathProbes ?? {};
		this.imageFileExists =
			opts.imageFileProbe ?? ((p) => existsSync(expandUserPath(p)));
		this.wsStarter = opts.wsStarter;
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.lockManager = opts.lockManager;
		this.lockOwner = opts.lockOwner ?? "this-instance";

		const env = (name: string): string =>
			(this.secretReader(name) ?? "").trim();

		// Env overrides config.yaml on every field (adapter.py:__init__).
		this.relayUrl = (
			env("BUZZ_RELAY_URL") || String(this.extra.relay_url ?? "")
		).trim();
		this.cliPath = resolveCliPath(
			env("BUZZ_CLI_PATH") || String(this.extra.cli_path ?? ""),
			this.pathProbes,
		);

		const rawChannels: unknown =
			(env("BUZZ_CHANNELS") || this.extra.channels) ?? [];
		const channelItems = Array.isArray(rawChannels)
			? rawChannels.map((c) => String(c))
			: String(rawChannels).split(",");
		this.channels = channelItems
			.map((c) => c.trim())
			.filter((c) => c.length > 0);

		this.homeChannel = (
			env("BUZZ_HOME_CHANNEL") || String(this.extra.home_channel ?? "")
		).trim();

		let interval = BUZZ_DEFAULT_POLL_INTERVAL_S;
		const rawInterval = env("BUZZ_POLL_INTERVAL") || this.extra.poll_interval;
		if (
			rawInterval !== "" &&
			rawInterval !== undefined &&
			rawInterval !== null
		) {
			const parsed = Number(rawInterval);
			interval = Number.isFinite(parsed)
				? parsed
				: BUZZ_DEFAULT_POLL_INTERVAL_S;
		}
		this.pollInterval = Math.max(BUZZ_MIN_POLL_INTERVAL_S, interval);

		// require_mention: env overrides config; anything not in the false-token
		// set means TRUE (default) — adapter.py:__init__ exact semantics.
		const rmRaw = env("BUZZ_REQUIRE_MENTION");
		const rmValue =
			rmRaw.length > 0 ? rmRaw : String(this.extra.require_mention ?? true);
		this.requireMention = !BUZZ_REQUIRE_MENTION_FALSE_TOKENS.has(
			rmValue.trim().toLowerCase(),
		);

		const transportRaw = (
			env("BUZZ_TRANSPORT") ||
			String(this.extra.transport ?? "auto") ||
			"auto"
		)
			.trim()
			.toLowerCase();
		this.transportMode = (BUZZ_TRANSPORT_MODES as readonly string[]).includes(
			transportRaw,
		)
			? (transportRaw as "auto" | "websocket" | "poll")
			: "auto";

		const rawAllowed: unknown =
			(env("BUZZ_ALLOWED_USERS") || this.extra.allowed_users) ?? [];
		const allowedItems = Array.isArray(rawAllowed)
			? rawAllowed.map((a) => String(a))
			: String(rawAllowed).split(",");
		const normalized = new Set<string>();
		for (const entry of allowedItems) {
			const ref = normalizeUserRef(entry);
			if (ref !== null) normalized.add(ref); // invalid entries DROPPED silently
		}
		this.allowedPubkeys = normalized;

		// §11 step 3/4: missing required secret ⇒ LOUD disable.
		const enablement = resolveEnablement(
			BUZZ_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}
		// DEC-017: incomplete trust boundary is a CONSTRUCTION-TIME error.
		const boundaryErrors = validateBuzzTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // CLI plane has no native stream lanes
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async () => ({ success: false, error: "Not supported" }),
			transmitSeal: async () => ({ success: false, error: "Not supported" }),
		});

		// Kit interactive surface (shared rows drive it; Buzz itself has no
		// button wire — the router is the ONE query handler the kit expects).
		this.router = new CallbackQueryRouter({
			stores: {
				approvals: this.approvals,
				slashConfirms: this.slashConfirms,
				appr: this.appr,
				clarify: this.clarify,
			},
			authorizer: () => this.allowAllClickers,
			onExecApproval: async () => {
				this.resolvedFamilies.push("ea");
				return "ok";
			},
			onSlashConfirm: async () => {
				this.resolvedFamilies.push("sc");
				return "ok";
			},
			onClarifyChoice: async (_k, _id, idx) => {
				this.resolvedFamilies.push("cl");
				return `answer-${idx}`;
			},
			onWhatsappApproval: async () => {
				this.resolvedFamilies.push("appr");
				return "ok";
			},
			onPickerNav: async (parsed) => ({ answerText: `nav:${parsed.family}` }),
		});
	}

	private cp: EgressChokepoint;

	// ── secrets (NEVER logged; redactor registers resolved values) ─────────────

	/**
	 * adapter.py:_resolve_private_key — env reader FIRST, then a credentials
	 * JSON: explicit BUZZ_CREDENTIALS_FILE / extra.credentials_file, else the
	 * ~/.config/buzz glob (injected lister), fields tried in nsec →
	 * private_key_hex → private_key order.
	 */
	resolvePrivateKeyEvent(): string {
		const fromEnv = (this.secretReader("BUZZ_PRIVATE_KEY") ?? "").trim();
		if (fromEnv.length > 0) {
			this.registerLogSecret(fromEnv);
			return fromEnv;
		}
		const configured = (
			(this.secretReader("BUZZ_CREDENTIALS_FILE") ?? "").trim() ||
			String(this.extra.credentials_file ?? "")
		).trim();
		const candidates =
			configured.length > 0
				? [configured]
				: [...(this.credentialsLister?.() ?? [])].sort();
		for (const path of candidates) {
			const text = this.credentialsReader?.(path);
			if (text === undefined) continue;
			let data: unknown;
			try {
				data = JSON.parse(text);
			} catch {
				continue;
			}
			if (data === null || typeof data !== "object" || Array.isArray(data))
				continue;
			for (const field of ["nsec", "private_key_hex", "private_key"]) {
				const value = (data as Record<string, unknown>)[field];
				if (typeof value === "string" && value.trim().length > 0) {
					const key = value.trim();
					this.registerLogSecret(key);
					return key;
				}
			}
		}
		return "";
	}

	// ── buzz-cli plumbing ─────────────────────────────────────────────────────────

	/** adapter.py:_run_cli + _exec_buzz — env carries relay+key, argv NEVER does. */
	runCli(
		args: readonly string[],
		input?: string | undefined,
	): Promise<BuzzCliResult> {
		if (!this.privateKey) this.privateKey = this.resolvePrivateKeyEvent();
		if (this.executor === undefined) {
			return Promise.resolve({
				code: 4,
				stdout: "",
				stderr: JSON.stringify({
					error: "executor_missing",
					message: "no CLI executor wired",
				}),
			});
		}
		return this.executor(args, {
			env: {
				BUZZ_RELAY_URL: this.relayUrl,
				BUZZ_PRIVATE_KEY: this.privateKey,
			},
			...(input === undefined ? {} : { input }),
		});
	}

	// ── connection lifecycle ─────────────────────────────────────────────────────

	/** adapter.py:_set_fatal_error parity (IRC-port precedent for reason shape). */
	private markFatal(code: string, detail: string, retryable: boolean): void {
		this.fatalEvents.push({ code, detail, retryable });
		this.lifecycle.markFatal({
			kind: "config_invalid",
			detail: `[${code}]${retryable ? "(retryable)" : ""} ${detail}`,
		});
	}

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (this.relayUrl.length === 0) {
			this.markFatal("config_missing", "BUZZ_RELAY_URL must be set", false);
			return false;
		}
		if (this.cliPath.length === 0) {
			this.markFatal("cli_missing", "buzz CLI binary not found", false);
			return false;
		}
		this.privateKey = this.resolvePrivateKeyEvent();
		if (this.privateKey.length === 0) {
			this.markFatal("config_missing", "BUZZ_PRIVATE_KEY must be set", false);
			return false;
		}

		// Identity fetch: pubkey drives self-echo suppression; display name
		// drives channel mention gating (adapter.py:connect users get).
		const profileRes = await this.runCli(["users", "get"]);
		if (profileRes.code !== 0) {
			const message = cliErrorMessage(profileRes.stderr, profileRes.code);
			this.markFatal("connect_failed", message, profileRes.code === 2);
			return false;
		}
		const profiles = parseJsonList(profileRes.stdout);
		const first = profiles[0];
		if (
			first === undefined ||
			typeof first["pubkey"] !== "string" ||
			(first["pubkey"] as string).length === 0
		) {
			this.markFatal(
				"connect_failed",
				"buzz users get returned no profile",
				true,
			);
			return false;
		}
		this.selfPubkey = first["pubkey"].toLowerCase();
		this.displayName = String(first["display_name"] ?? "").trim();
		this.selfNpub = hexToNpub(this.selfPubkey) ?? "";

		// Two profiles must not drive the same Buzz identity on one relay
		// (scoped-lock pattern; ImportError parity = no manager ⇒ skip).
		if (this.lockManager !== undefined) {
			const credentialId = `${this.relayUrl}:${this.selfPubkey}`;
			const acquisition = this.lockManager.tryAcquire(
				"buzz",
				credentialId,
				this.lockOwner,
			);
			if (!acquisition.acquired) {
				this.lifecycle.markFatal({
					kind: "token_lock_conflict",
					scope: "buzz",
					credentialId,
					holder: acquisition.holder.owner,
				});
				return false;
			}
			this.lockHandle = acquisition.lock;
		}

		// Watch-set discovery: channels list maps ids→names/meta.
		const listRes = await this.runCli(["channels", "list"]);
		if (listRes.code !== 0) {
			const message = cliErrorMessage(listRes.stderr, listRes.code);
			this.markFatal("connect_failed", message, listRes.code === 2);
			return false;
		}
		const listed = parseJsonList(listRes.stdout);
		for (const ch of listed) {
			if (typeof ch["channel_id"] !== "string") continue;
			const id = ch["channel_id"];
			this.channelMeta.set(id, ch);
			this.channelNames.set(id, String(ch["name"] ?? id));
		}
		const watch =
			this.channels.length > 0
				? [...this.channels]
				: [...this.channelNames.keys()];
		if (watch.length === 0) {
			this.markFatal("config_missing", "no Buzz channels to watch", false);
			return false;
		}

		// Seed high-water marks so a (re)start never replays history.
		for (const channelId of watch) {
			await this.seedChannel(channelId, "group");
		}
		await this.discoverDms(true);

		// Inbound transport: WS preferred (auto/websocket) — the LIVE LOOP is a
		// documented exclusion; the starter seam decides availability. A failed
		// websocket-required connect is FATAL retryable (source truth).
		let transportUsed: "poll" | "websocket" = "poll";
		if (this.transportMode === "auto" || this.transportMode === "websocket") {
			const started =
				this.wsStarter === undefined ? false : await this.wsStarter();
			if (started) {
				transportUsed = "websocket";
			} else if (this.transportMode === "websocket") {
				this.markFatal(
					"ws_auth_failed",
					"Buzz WebSocket transport did not authenticate (transport=websocket)",
					true,
				);
				await this.disconnect();
				return false;
			}
		}
		this.lastTransportUsed = transportUsed;
		if (transportUsed === "poll") this.pollLoopActive = true;
		this.connectedOnce = true;
		this.connectLog.push(`connected:${transportUsed}`);
		return true;
	}

	/** Connect audit trail (rows assert the used transport + fallbacks). */
	readonly connectLog: string[] = [];

	override async disconnect(): Promise<void> {
		this.connectedOnce = false;
		this.pollLoopActive = false;
		if (this.lockHandle !== null) {
			this.lockHandle.release();
			this.lockHandle = null;
		}
		this.channelState.clear();
		this.pollCount = 0;
	}

	// ── sending ───────────────────────────────────────────────────────────────────

	/**
	 * adapter.py:send — ONE command shape for channels AND DMs (a DM target is
	 * just its conversation ref): ["messages","send","--channel",<target>,
	 * "--content","-", …("--reply-to",<target>)]. Content rides stdin.
	 *
	 * adapter.py:send_image --file lane: a local filePath inserts
	 * "--file",<path> BEFORE "--content -" (argv order is the vendor CLI's
	 * contract) while the caption still rides stdin.
	 */
	buildSendArgs(
		chatId: string,
		replyTarget?: string | undefined,
		filePath?: string | undefined,
	): string[] {
		const args = ["messages", "send", "--channel", String(chatId)];
		if (filePath !== undefined && filePath.length > 0) {
			args.push("--file", String(filePath));
		}
		args.push("--content", "-");
		if (replyTarget !== undefined && replyTarget.length > 0) {
			args.push("--reply-to", String(replyTarget));
		}
		return args;
	}

	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		if (content.length === 0) {
			return { success: false, error: "Empty message" };
		}
		if (this.wireTransmitSend !== undefined) {
			return this.wireTransmitSend(chatId, content, metadata);
		}
		// Kit-door convention: reply targets ride METADATA (the chokepoint's
		// transmitSend carries no positional replyTo). thread_id mirrors the
		// reference read; reply_to_message_id is the kit-standard key.
		const rawTarget = metadata["thread_id"] ?? metadata[REPLY_TO_METADATA_KEY];
		const replyTarget =
			typeof rawTarget === "string" && rawTarget.length > 0
				? rawTarget
				: undefined;
		const res = await this.runCli(
			this.buildSendArgs(chatId, replyTarget),
			content,
		);
		if (res.code !== 0) {
			return {
				success: false,
				error: cliErrorMessage(res.stderr, res.code),
				retryable: res.code === 2,
			};
		}
		let data: Record<string, unknown> = {};
		try {
			const parsed: unknown = JSON.parse(
				res.stdout.length > 0 ? res.stdout : "{}",
			);
			if (
				parsed !== null &&
				typeof parsed === "object" &&
				!Array.isArray(parsed)
			) {
				data = parsed as Record<string, unknown>;
			}
		} catch {
			data = {};
		}
		const eventId =
			typeof data["event_id"] === "string" ? data["event_id"] : undefined;
		if (eventId !== undefined) {
			// Belt-and-braces echo suppression: mark own send's id seen.
			this.markSeen(chatId, eventId);
		}
		return {
			success: data["accepted"] !== false,
			...(eventId !== undefined ? { messageId: eventId } : {}),
		};
	}

	// ── image sending (adapter.py:send_image parity) ──────────────────────────

	/** adapter.py:send_image — local files upload via `--file` with the caption
	 * piped on stdin; URLs (and MISSING local files, which fail is_file()) go
	 * as link text because Buzz markdown renders clickable image links.
	 */
	async sendImage(
		chatId: string,
		imageRef: string,
		caption?: string | undefined,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		const local = isHttpUrl(imageRef) ? null : expandUserPath(imageRef);
		if (local !== null && this.imageFileExists(local)) {
			const res = await this.runCli(
				this.buildSendArgs(chatId, replyTo, local),
				caption ?? "",
			);
			if (res.code !== 0) {
				return {
					success: false,
					error: cliErrorMessage(res.stderr, res.code),
					retryable: res.code === 2,
				};
			}
			let data: Record<string, unknown> = {};
			try {
				const parsed: unknown = JSON.parse(
					res.stdout.length > 0 ? res.stdout : "{}",
				);
				if (
					parsed !== null &&
					typeof parsed === "object" &&
					!Array.isArray(parsed)
				) {
					data = parsed as Record<string, unknown>;
				}
			} catch {
				data = {};
			}
			const eventId =
				typeof data["event_id"] === "string" ? data["event_id"] : undefined;
			if (eventId !== undefined) this.markSeen(chatId, eventId);
			return {
				success: data["accepted"] !== false,
				...(eventId !== undefined ? { messageId: eventId } : {}),
			};
		}
		// Link-text fallback: caption ABOVE the URL on its own line.
		const text = caption ? `${caption}\n${imageRef}` : imageRef;
		return this.wireSend(chatId, text, {});
	}

	/** Post-stream image-batch hook (DEC-019 explicit-tag delivery path). */
	async sendMultipleImages(
		chatId: string,
		images: readonly string[],
	): Promise<SendResult[]> {
		const results: SendResult[] = [];
		for (const image of images) {
			results.push(await this.sendImage(chatId, image));
		}
		return results;
	}

	// ── inbound polling ───────────────────────────────────────────────────────────

	/**
	 * adapter.py:_poll_loop body — ONE sweep. Cadence lives with the caller's
	 * injected clock; sweep faults are CONTAINED (loop survives — the
	 * heartbeat-escalation analog asserts survival after repeated timeouts).
	 */
	async pollSweep(): Promise<void> {
		if (!this.pollLoopActive) return;
		this.sweepCount += 1;
		this.pollCount += 1;
		try {
			if (this.pollCount % BUZZ_DM_DISCOVERY_EVERY === 0) {
				await this.discoverDms(false);
			}
			for (const channelId of [...this.channelState.keys()]) {
				await this.pollChannel(channelId);
			}
		} catch (err) {
			this.sweepErrors += 1;
			const message = err instanceof Error ? err.message : String(err);
			this.lastSweepError = message;
			this.logger?.warn?.(`Buzz: poll sweep failed: ${message}`);
		}
	}

	/** adapter.py:_seed_channel — watermark from newest events, history marked seen. */
	async seedChannel(
		channelId: string,
		chatType: "group" | "dm",
	): Promise<void> {
		const state: BuzzChannelState = { chatType, lastTs: 0, seen: new Map() };
		this.channelState.set(channelId, state);
		const res = await this.runCli([
			"messages",
			"get",
			"--channel",
			channelId,
			"--limit",
			String(BUZZ_FETCH_LIMIT),
		]);
		if (res.code !== 0) {
			this.logger?.warn?.(
				`Buzz: could not seed channel ${channelId} — ${cliErrorMessage(res.stderr, res.code)}`,
			);
			// Transient unreadability must not replay history once readable:
			// fall back to "now".
			state.lastTs = Math.floor(this.nowFn() / 1000);
			return;
		}
		for (const event of parseJsonList(res.stdout)) {
			const eventId = event["id"];
			const createdAt = Number(event["created_at"] ?? 0);
			if (typeof eventId === "string" && eventId.length > 0) {
				state.seen.set(eventId, null);
			}
			state.lastTs = Math.max(state.lastTs, createdAt);
			// History is never dispatched but still classifies (DM latch).
			this.maybeLatchDm(channelId, state, event);
		}
		this.trimSeen(state);
	}

	/**
	 * adapter.py:_discover_dms — `dms list` best-effort plus the channels-list
	 * fallback (#68871); fresh mid-run conversations dispatch from their
	 * beginning, startup ones are seeded like channels.
	 */
	async discoverDms(seed: boolean): Promise<void> {
		const dmsRes = await this.runCli(["dms", "list"]);
		if (dmsRes.code === 0) {
			for (const dm of parseJsonList(dmsRes.stdout)) {
				const dmId = String(dm["dm_id"] ?? "");
				if (dmId.length === 0 || this.channelState.has(dmId)) continue;
				if (seed) {
					await this.seedChannel(dmId, "dm");
				} else {
					this.channelState.set(dmId, {
						chatType: "dm",
						lastTs: 0,
						seen: new Map(),
					});
				}
				if (!this.channelNames.has(dmId)) this.channelNames.set(dmId, "DM");
			}
		}

		const listRes = await this.runCli(["channels", "list"]);
		if (listRes.code !== 0) return;
		for (const ch of parseJsonList(listRes.stdout)) {
			const chId = String(ch["channel_id"] ?? "");
			if (chId.length === 0) continue;
			this.channelMeta.set(chId, ch);
			if (!this.channelNames.has(chId)) {
				this.channelNames.set(chId, String(ch["name"] ?? chId));
			}
			if (this.channelState.has(chId) || !this.mayReclassifyAsDm(chId))
				continue;
			if (seed) {
				await this.seedChannel(chId, "group");
			} else {
				this.channelState.set(chId, {
					chatType: "group",
					lastTs: 0,
					seen: new Map(),
				});
			}
		}
	}

	async pollChannel(channelId: string): Promise<void> {
		const state = this.channelState.get(channelId);
		if (state === undefined) return;
		const args = [
			"messages",
			"get",
			"--channel",
			channelId,
			"--limit",
			String(BUZZ_FETCH_LIMIT),
		];
		if (state.lastTs > 0) {
			// Nostr since is INCLUSIVE: same-second events re-fetch and de-duped
			// by id below (adapter.py:_poll_channel comment).
			args.push("--since", String(state.lastTs));
		}
		const res = await this.runCli(args);
		if (res.code !== 0) {
			const message = cliErrorMessage(res.stderr, res.code);
			this.pollFailures += 1;
			this.lastCliError = message;
			this.logger?.debug?.(
				`Buzz: poll of channel ${channelId} failed — ${message}`,
			);
			return;
		}
		for (const event of parseJsonList(res.stdout)) {
			await this.handleEvent(channelId, state, event);
		}
		this.trimSeen(state);
	}

	/**
	 * adapter.py:_handle_event — THE ACK-WINDOW CONTRACT (source-pinned):
	 * the seen-commit happens IMMEDIATELY after the dedupe check and BEFORE
	 * kind filtering and dispatch. Consequences (both behavior-tested):
	 *   - fetched-but-uncommitted events (crash before commit) are REFETCHED by
	 *     the inclusive-since next sweep and dispatch exactly once;
	 *   - committed-but-undispatched events are NEVER redispatched
	 *     (at-most-once downstream) — the opposite ordering of dispatch-first
	 *     adapters; the hooks.beforeCommit fault point models that window.
	 */
	async handleEvent(
		channelId: string,
		state: BuzzChannelState,
		event: Record<string, unknown>,
	): Promise<void> {
		const eventId = String(event["id"] ?? "");
		const createdAt = Number(event["created_at"] ?? 0);
		if (eventId.length === 0 || state.seen.has(eventId)) return;
		this.hooks?.beforeCommit?.(channelId, event);
		state.seen.set(eventId, null); // COMMIT (before dispatch — source truth)
		state.lastTs = Math.max(state.lastTs, createdAt);

		if (Number(event["kind"] ?? 0) !== BUZZ_CHAT_KIND) return;
		const pubkey = String(event["pubkey"] ?? "").toLowerCase();
		const content = event["content"];
		if (
			pubkey.length === 0 ||
			typeof content !== "string" ||
			content.trim().length === 0
		) {
			return;
		}
		// Self-echo suppression: our own messages never dispatch back.
		if (pubkey === this.selfPubkey) return;

		this.maybeLatchDm(channelId, state, event);

		const isDm = state.chatType === "dm";
		// Channels respond only when addressed (unless require_mention=false);
		// DMs ALWAYS dispatch.
		if (!isDm && this.requireMention && !this.isMentioned(content)) return;

		// Adapter-level allow-list: EMPTY SET ⇒ FILTER OFF entirely (source:
		// `if self._allowed_pubkeys and pubkey not in …`). The separate
		// BUZZ_ALLOW_ALL_USERS flag is gateway-central, not adapter machinery.
		if (this.allowedPubkeys.size > 0 && !this.allowedPubkeys.has(pubkey))
			return;

		const dispatchText = this.stripMention(content);
		const outgoing: IncomingEvent = {
			messageType: "text",
			text: dispatchText,
			messageId: eventId,
			source: {
				platform: BUZZ_PLUGIN_MANIFEST.name,
				chatType: isDm ? "dm" : "group",
				userId: pubkey,
				chatId: channelId,
				chatName: this.channelNames.get(channelId) ?? channelId,
			},
		};
		// Committed-but-undispatched fault window (behavior rows model the
		// at-most-once consequence of the source's commit-first ordering).
		this.hooks?.beforeDispatch?.(outgoing);
		await this.dispatchMessage({
			text: dispatchText,
			chatId: channelId,
			chatType: isDm ? "dm" : "group",
			userId: pubkey,
			userName: await this.resolveUserName(pubkey),
			messageId: eventId,
			createdAt,
		});
	}

	// ── DM classification (issue #68871 block comment) ──────────────────────────

	/** Real community metadata rules out DM; absent meta trusts only unconfigured ids. */
	mayReclassifyAsDm(channelId: string): boolean {
		const meta = this.channelMeta.get(channelId);
		if (meta === undefined) return !this.channels.includes(channelId);
		const name = String(meta["name"] ?? "").trim();
		const description = String(meta["description"] ?? "").trim();
		return name === "DM" && description.length === 0;
	}

	/**
	 * p-tagged to self WITHOUT a visible mention ⇒ structural DM addressing
	 * (in a real channel that combination only occurs WITH typed mentions).
	 */
	isDirectMessageEvent(
		channelId: string,
		event: Record<string, unknown>,
	): boolean {
		if (this.selfPubkey.length === 0 || !this.mayReclassifyAsDm(channelId))
			return false;
		if (Number(event["kind"] ?? 0) !== BUZZ_CHAT_KIND) return false;
		const pubkey = String(event["pubkey"] ?? "").toLowerCase();
		if (pubkey.length === 0 || pubkey === this.selfPubkey) return false;
		const tags = event["tags"];
		if (!Array.isArray(tags)) return false;
		const pTaggedToSelf = tags.some(
			(tag) =>
				Array.isArray(tag) &&
				tag.length > 1 &&
				tag[0] === "p" &&
				String(tag[1]).toLowerCase() === this.selfPubkey,
		);
		if (!pTaggedToSelf) return false;
		const content = event["content"];
		return typeof content === "string" && !this.isMentioned(content);
	}

	/** Latch group→dm once ANY direct message is seen; classification sticks. */
	maybeLatchDm(
		channelId: string,
		state: BuzzChannelState,
		event: Record<string, unknown>,
	): void {
		if (state.chatType === "dm" || !this.isDirectMessageEvent(channelId, event))
			return;
		state.chatType = "dm";
		if (!this.channelNames.has(channelId))
			this.channelNames.set(channelId, "DM");
		this.dmLatches.push(channelId);
	}

	/** Conversation ids latched to dm (row observability). */
	readonly dmLatches: string[] = [];

	// ── mention gating ───────────────────────────────────────────────────────────

	/** Addresses this agent via hex pubkey, npub, or display-name word. */
	isMentioned(content: string): boolean {
		const lowered = content.toLowerCase();
		if (this.selfPubkey.length > 0 && lowered.includes(this.selfPubkey))
			return true;
		if (this.selfNpub.length > 0 && lowered.includes(this.selfNpub))
			return true;
		if (this.displayName.length > 0) {
			const pattern = `(?<![\\w])@?${escapeRegExp(this.displayName.toLowerCase())}(?![\\w])`;
			if (new RegExp(pattern).test(lowered)) return true;
		}
		return false;
	}

	/** Strip ONE LEADING mention (case-insensitive) so commands stay clean. */
	stripMention(content: string): string {
		const text = content.trim();
		const candidates: string[] = [];
		if (this.displayName.length > 0)
			candidates.push(escapeRegExp(this.displayName));
		if (this.selfNpub.length > 0) candidates.push(escapeRegExp(this.selfNpub));
		if (this.selfPubkey.length > 0)
			candidates.push(escapeRegExp(this.selfPubkey));
		if (candidates.length === 0) return text;
		const pattern = `^@?(?:${candidates.join("|")})[\\s:,]*`;
		return text.replace(new RegExp(pattern, "i"), "").trim();
	}

	/** Cached pubkey→name resolution with negative caching (amplification guard). */
	async resolveUserName(pubkey: string): Promise<string> {
		const cached = this.userNames.get(pubkey);
		if (cached !== undefined) return cached;
		let name = "";
		const res = await this.runCli(["users", "get", "--pubkey", pubkey]);
		if (res.code === 0) {
			const profiles = parseJsonList(res.stdout);
			if (profiles.length > 0) {
				name = String(profiles[0]?.["display_name"] ?? "").trim();
			}
		}
		if (name.length === 0) {
			name = (hexToNpub(pubkey) ?? pubkey).slice(0, 16);
		}
		this.userNames.set(pubkey, name);
		return name;
	}

	// ── de-dupe bookkeeping ───────────────────────────────────────────────────────

	/** adapter.py:_trim_seen — OrderedDict popitem(last=false) ⇒ FIFO eviction. */
	trimSeen(state: BuzzChannelState): void {
		while (state.seen.size > BUZZ_SEEN_CAP) {
			const oldest = state.seen.keys().next();
			if (oldest.done) break;
			state.seen.delete(oldest.value);
		}
	}

	markSeen(channelId: string, eventId: string): void {
		const state = this.channelState.get(channelId);
		if (state !== undefined) {
			state.seen.set(eventId, null);
			this.trimSeen(state);
		}
	}

	// ── dispatch into the pipeline ─────────────────────────────────────────────────

	private async dispatchMessage(fields: {
		text: string;
		chatId: string;
		chatType: "dm" | "group";
		userId: string;
		userName: string;
		messageId: string;
		createdAt: number;
	}): Promise<void> {
		if (this.guard === null) return; // adapter.py: `if not self._message_handler: return`
		const event: IncomingEvent = {
			messageType: "text",
			text: fields.text,
			messageId: fields.messageId,
			source: {
				platform: BUZZ_PLUGIN_MANIFEST.name,
				chatType: fields.chatType,
				userId: fields.userId,
				chatId: fields.chatId,
				chatName: this.channelNames.get(fields.chatId) ?? fields.chatId,
			},
		};
		// Immutable enqueue-time SNAPSHOT: guard-side burst merging MUTATES event
		// objects after admission; the observability log must not be rewritten
		// by later merges (dispatch = exactly one entry, frozen at this point).
		const snapshot: IncomingEvent = {
			messageType: event.messageType,
			...(event.text === undefined ? {} : { text: event.text }),
			...(event.messageId === undefined ? {} : { messageId: event.messageId }),
			...(event.internal === undefined ? {} : { internal: event.internal }),
			...(event.source === undefined ? {} : { source: { ...event.source } }),
		};
		this.inboundEventLog.push(snapshot);
		await this.deliverInbound(event, sessionKeyOf(event));
		// 👀 seen-reaction after dispatch (best-effort, never blocks flow).
		try {
			await this.runCli([
				"reactions",
				"add",
				"--event",
				fields.messageId,
				"--emoji",
				"👀",
			]);
		} catch {
			/* reaction failures are debug-class */
		}
	}

	// ── guard wiring (reference-fixture inheritance) ──────────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: BUZZ_REGISTRY,
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
					const reply = `reply:${text}`;
					this.replyLog.push(reply);
					return reply;
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

	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	private readonly clarifyArmedSet = new Set<string>();
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	get clarifyArmed(): Set<string> {
		return this.clarifyArmedSet;
	}

	setClarifyIntercept(sessionKey: string, on: boolean): void {
		if (on) this.clarifyArmedSet.add(sessionKey);
		else this.clarifyArmedSet.delete(sessionKey);
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
		// Harness echo-lane convention (shared row contract).
		if (String(event.source?.userId ?? "") === "bot-self") return;
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	// ── egress doors ───────────────────────────────────────────────────────────────

	private allowAllClickers = true;
	readonly router: CallbackQueryRouter;
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly resolvedFamilies: string[] = [];

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	/** Per-chat length descriptor: harness utf16-marked chats return budget AND unit TOGETHER. */
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

	doorAudit() {
		return this.cp.audit;
	}

	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// The CLI plane exposes NO rich endpoint; only a scripted harness lane
		// exists (formatting-ladder row drives the probe-once latch through it).
		if (
			this.wireTransmitRich === undefined ||
			this.richScriptedProbe?.() !== true
		) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.wireTransmitRich(content, metadata);
	}
}

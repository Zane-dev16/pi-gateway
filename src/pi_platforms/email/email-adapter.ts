// pi_platforms/email/email-adapter — THE Email adapter, ported from the
// READ-ONLY Hermes reference plugin (plugins/platforms/email/adapter.py) onto
// the kit base. TRANSPORT (IMAP UID cursor + SMTP MIME plain) and MANIFEST
// DATA live here; policy stays in the kit.
//
// Ported semantics (file:symbol anchors):
//   __init__            env-wins config; seen-UID set cap 2000/trim-top-half;
//                       require_authenticated_sender default ON (fail-closed).
//   connect             missing-config ⇒ FATAL non-retryable; IMAP test
//                       (login+ID+SELECT) marks ALL existing UIDs seen on cold
//                       start, RESTORES the per-account snapshot on reconnect
//                       (#79889 fd-leak parity, mail during outage processed);
//                       SMTP auth failure ⇒ typed FATAL non-retryable
//                       (email_auth_error); other SMTP errors retryable.
//   _fetch_new_messages UID SEARCH UNSEEN → skip seen → per-UID fetch
//                       (refusal leaves UNSEEN for next poll); mark-seen ONCE
//                       a response arrives even if malformed (#80032); poison
//                       message never aborts the batch; partial results ARE
//                       dispatched before failure escalation; snapshot updated
//                       after EVERY poll; RFC 2971 IMAP ID issued after EVERY
//                       login (_send_imap_id — byte-identical vendor identity,
//                       best-effort/swallowed).
//   parseFetchedMessage _extract_attachments over parts: image exts are
//                       magic-byte-checked (non-images SKIPPED), others cache
//                       as documents.
//   _dispatch_message   gates in order: self-drop → automated drop → allowlist
//                       gate (unset + no allow-all ⇒ drop; EMAIL_ALLOW_ALL_USERS
//                       OR GATEWAY_ALLOW_ALL_USERS opt in) → authenticated-
//                       sender gate ONLY when allowlist in effect & allow-all
//                       off (GHSA-rxqh-5572-8m77 fail-closed); attachments
//                       become event mediaUrls/mediaTypes with DOCUMENT-wins
//                       typing (image ⇒ PHOTO only while still TEXT).
//   _send_email         MIMEMultipart with MIMEText PLAIN only; thread context
//                       Re:/In-Reply-To/References; default subject "Hermes
//                       Agent"; RFC 2822 local-time Date; Message-ID hermes-<hex>;
//                       port 465 implicit TLS else STARTTLS; A21 IPv4 ladder.
//
// PROPOSED DEC text lives in email-world.ts (polling-row leg mappings).

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
import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type { StreamEgressAdapter } from "../../pi_gateway/streaming/adapter-seam.js";
import type {
	DraftFrameArgs,
	EditOptions,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type {
	CommandRegistry,
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import { immediateSpawner } from "../../pi_gateway/guards/index.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";

import {
	EMAIL_IMAP_PORT_DEFAULT,
	EMAIL_IMAP_ID_ARGUMENT,
	EMAIL_MAX_BODY_CHARS,
	EMAIL_PLUGIN_MANIFEST,
	EMAIL_POLL_INTERVAL_MS,
	EMAIL_SEEN_UIDS_MAX,
	EMAIL_SMTP_IMPLICIT_TLS_PORT,
	EMAIL_SMTP_PORT_DEFAULT,
} from "./manifest.js";
import {
	extractEmailAddress,
	extractTextBody,
	extractAttachments,
	formatRfc2822Date,
	isAutomatedSender,
	decodeHeaderValue,
	verifySenderAuthentication,
} from "./mime-text.js";
import type { ExtractedAttachment } from "./mime-text.js";
import type { PacingClockLike } from "./clock.js";
import {
	type FakeImapServer,
	type FakeSmtpServer,
	SmtpAuthenticationError,
} from "./fake-mail-servers.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const EMAIL_REGISTRY: CommandRegistry = [
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

export interface EmailAdapterDeps {
	imap: FakeImapServer;
	smtp: FakeSmtpServer;
	clock?: PacingClockLike | undefined;
	secretReader?: ScopedSecretReader | undefined;
	spawner?: TaskSpawner | undefined;
	manifestName?: string | undefined;
	/** Harness-scale budget override (subjects pass 64 like the references). */
	scalarMaxUnits?: number | undefined;
	config?:
		| {
				pollIntervalMs?: number | undefined;
				requireAuthenticatedSender?: boolean | undefined;
				authservId?: string | undefined;
		  }
		| undefined;
	declaredDraftStreaming?: boolean | undefined;
}

interface ParsedMail {
	uid: string;
	senderAddr: string;
	senderName: string;
	subject: string;
	messageId: string;
	inReplyTo: string;
	body: string;
	date: string;
	senderAuthenticated: boolean;
	authReason: string;
	attachments: ExtractedAttachment[];
}

export class EmailAdapter
	extends BasePlatformAdapter
	implements StreamEgressAdapter
{
	readonly pluginManifest = EMAIL_PLUGIN_MANIFEST;
	readonly imap: FakeImapServer;
	readonly smtp: FakeSmtpServer;
	readonly clock: PacingClockLike | undefined;

	private readonly cp: EgressChokepoint;
	private readonly secretReader: ScopedSecretReader;
	private readonly deps: EmailAdapterDeps;

	// ── resolved config ──
	readonly address: string;
	private readonly password: string;
	readonly imapHost: string;
	readonly imapPort: number;
	readonly smtpHost: string;
	readonly smtpPort: number;
	readonly pollIntervalMs: number;
	readonly requireAuthenticatedSender: boolean;
	readonly authservId: string;

	// ── runtime state (adapter.py __init__) ──
	private readonly seenUids = new Set<string>();
	private seenUidsMax = EMAIL_SEEN_UIDS_MAX;
	private lastFetchFailed = false;
	private lastFetchError = "";
	private readonly threadContext = new Map<
		string,
		{ subject: string; messageId: string }
	>();
	private readonly seenSnapshot = new Map<string, Set<string>>();
	private pollLoopRunning = false;
	private pollOnceCount = 0;
	private running = false;

	/** Observability: what fed recovery / escalation. */
	readonly recoveryLog: string[] = [];
	readonly escalationLog: string[] = [];
	lastCapturedRetryAfterSeconds: number | null = null;

	// ── interactive surfaces (kit census posture; no native taps) ──
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

	wireTransmitSend: (
		toAddr: string,
		bodyText: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });
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

	constructor(deps: EmailAdapterDeps) {
		super({
			manifestName: deps.manifestName ?? "email",
			capabilities: EMAIL_PLUGIN_MANIFEST.capabilities,
			lengthUnit: "chars",
			scalarMaxUnits: deps.scalarMaxUnits ?? EMAIL_MAX_BODY_CHARS,
		});
		this.deps = deps;
		this.imap = deps.imap;
		this.smtp = deps.smtp;
		this.clock = deps.clock;
		this.secretReader = deps.secretReader ?? ((name) => process.env[name]);
		this.spawn = deps.spawner ?? immediateSpawner();

		const env = (k: string): string | undefined => this.secretReader(k);
		this.address = (env("EMAIL_ADDRESS") ?? "").trim();
		this.password = env("EMAIL_PASSWORD") ?? "";
		this.imapHost = (env("EMAIL_IMAP_HOST") ?? "").trim();
		const imapPortRaw = Number(env("EMAIL_IMAP_PORT"));
		this.imapPort =
			env("EMAIL_IMAP_PORT") === undefined || !Number.isFinite(imapPortRaw)
				? EMAIL_IMAP_PORT_DEFAULT
				: imapPortRaw;
		this.smtpHost = (env("EMAIL_SMTP_HOST") ?? "").trim();
		const smtpPortRaw = Number(env("EMAIL_SMTP_PORT"));
		this.smtpPort =
			env("EMAIL_SMTP_PORT") === undefined || !Number.isFinite(smtpPortRaw)
				? EMAIL_SMTP_PORT_DEFAULT
				: smtpPortRaw;
		const pollRaw = Number(env("EMAIL_POLL_INTERVAL"));
		this.pollIntervalMs =
			env("EMAIL_POLL_INTERVAL") === undefined || !Number.isFinite(pollRaw)
				? (deps.config?.pollIntervalMs ?? EMAIL_POLL_INTERVAL_MS)
				: pollRaw * 1000;
		if (this.password.length > 0) this.registerLogSecret(this.password);

		// require_authenticated_sender: extra flag > EMAIL_TRUST_FROM_HEADER
		// mirror > DEFAULT ON (GHSA-rxqh fail-closed posture).
		const trustFromHeader = ["true", "1", "yes"].includes(
			(env("EMAIL_TRUST_FROM_HEADER") ?? "").toLowerCase(),
		);
		this.requireAuthenticatedSender =
			deps.config?.requireAuthenticatedSender ??
			(trustFromHeader ? false : true);
		this.authservId = (deps.config?.authservId ?? "").toLowerCase();

		// §11 step 3/4: missing required secret ⇒ LOUD disable at construction.
		const enablement = resolveEnablement(
			EMAIL_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
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

	// ── lie-scan probe (DEC-006 METHOD) ──
	private readonly isMessageChats = new Set<string>();
	markStreamIsMessage(chatId: string): void {
		this.isMessageChats.add(chatId);
	}
	override supportsDraftStreaming(): boolean {
		return this.deps.declaredDraftStreaming === true;
	}

	get isConnected(): boolean {
		return this.running;
	}
	get polledOnce(): boolean {
		return this.pollOnceCount > 0;
	}
	get seenUidCount(): number {
		return this.seenUids.size;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Connection lifecycle (adapter.py::connect)
	// ══════════════════════════════════════════════════════════════════════

	override async connect(opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		const missing: string[] = [];
		if (this.address.length === 0) missing.push("EMAIL_ADDRESS");
		if (this.password.length === 0) missing.push("EMAIL_PASSWORD");
		if (this.imapHost.length === 0) missing.push("EMAIL_IMAP_HOST");
		if (this.smtpHost.length === 0) missing.push("EMAIL_SMTP_HOST");
		if (missing.length > 0) {
			this.logger?.error?.(
				`[email] Not configured — missing ${missing.join(", ")}`,
			);
			this.markFatal(
				"email_missing_configuration",
				`missing ${missing.join(", ")}`,
				false,
			);
			return false;
		}

		// IMAP connection test (handle closed in finally — #79889).
		try {
			this.imap.connectCalls += 1;
			if (this.imap.connectFailuresArmed > 0) {
				this.imap.connectFailuresArmed -= 1;
				throw new Error("unreachable host");
			}
			this.imap.login(this.address, this.password);
			this.sendImapId();
			this.imap.selectInbox();
			const snapshot = this.seenSnapshot.get(this.address);
			if (opts.isReconnect && snapshot !== undefined) {
				// Reconnect within process: restore baseline so outage mail
				// (still UNSEEN) dispatches instead of being skipped.
				for (const uid of snapshot) this.seenUids.add(uid);
				this.trimSeenUids();
				this.logger?.info?.(
					`[email] IMAP reconnect restored ${snapshot.size} seen UIDs`,
				);
			} else {
				// Cold start: mark ALL existing messages seen.
				for (const uid of this.imap.uidSearch("ALL")) this.seenUids.add(uid);
				this.trimSeenUids();
			}
			this.imap.logout();
			this.seenSnapshot.set(this.address, new Set(this.seenUids));
		} catch (err) {
			// Generic IMAP error: kept RETRYABLE deliberately — imaplib raises the
			// same IMAP4.error for bad credentials AND transient NOs (OOF-156).
			this.markFatal(
				"email_imap_connect_error",
				`IMAP connection to ${this.imapHost}:${this.imapPort} failed: ${brief(err)}`,
				true,
			);
			return false;
		}

		// SMTP connection test.
		try {
			this.connectSmtp();
			try {
				this.smtp.login(this.address, this.password);
			} finally {
				void this.smtp;
			}
		} catch (err) {
			if (err instanceof SmtpAuthenticationError) {
				// Typed auth failure can NEVER self-heal: drop out of the queue.
				this.markFatal(
					"email_auth_error",
					`SMTP authentication failed for ${this.address}: ${brief(err)}`,
					false,
				);
			} else {
				this.markFatal(
					"email_smtp_connect_error",
					`SMTP connection to ${this.smtpHost} failed: ${brief(err)}`,
					true,
				);
			}
			return false;
		}

		this.running = true;
		this.recoveryLog.push(opts.isReconnect ? "reconnected" : "connected");
		return true;
	}

	override async disconnect(): Promise<void> {
		// State clears SYNCHRONOUSLY (IRC-session discipline).
		this.running = false;
		this.pollLoopRunning = false;
	}

	/**
	 * The poll loop body — ONE deterministic cycle. The reference loop runs
	 * forever at EMAIL_POLL_INTERVAL; fixtures drive cycles explicitly so all
	 * timing claims stay under the injected clock.
	 */
	async runPollCycle(): Promise<void> {
		if (!this.running && !this.pollLoopRunning) return;
		this.pollOnceCount += 1;
		let messages: ParsedMail[] = [];
		try {
			messages = this.fetchNewMessages();
		} catch (err) {
			this.lastFetchFailed = true;
			this.lastFetchError = brief(err);
		}
		// Dispatch whatever the fetch returned BEFORE escalating a failure:
		// dropping partial results would lose already-processed messages.
		for (const message of messages) {
			await this.dispatchMessage(message);
		}
		if (this.lastFetchFailed) {
			this.lastFetchFailed = false;
			this.escalationLog.push(`email_imap_fetch_failed:${this.lastFetchError}`);
			this.markFatal("email_imap_fetch_failed", this.lastFetchError, true);
		}
	}

	/** Start the interval loop (production wiring; clock-driven). */
	startPollLoop(): void {
		if (this.pollLoopRunning) return;
		this.pollLoopRunning = true;
		const tick = async (): Promise<void> => {
			while (this.pollLoopRunning && this.running) {
				await this.runPollCycle();
				await this.sleepMs(this.pollIntervalMs);
			}
		};
		void tick();
	}

	stopPollLoop(): void {
		this.pollLoopRunning = false;
	}

	// ── _fetch_new_messages (executor-thread parity; sync here) ──

	/**
	 * adapter.py:_send_imap_id — RFC 2971 ID after EVERY login, with the
	 * BYTE-IDENTICAL vendor identity string (163/NetEase refuse every UID
	 * SEARCH/FETCH with ``BYE Unsafe Login`` otherwise). Best-effort: any
	 * rejection is swallowed so non-supporting servers keep working.
	 */
	private sendImapId(): void {
		try {
			this.imap.id(EMAIL_IMAP_ID_ARGUMENT);
		} catch {
			// ID not accepted — never fatal (adapter.py:_send_imap_id).
		}
	}

	private fetchNewMessages(): ParsedMail[] {
		const results: ParsedMail[] = [];
		try {
			this.imap.connectCalls += 1;
			if (this.imap.connectFailuresArmed > 0) {
				throw new Error("IMAP fetch error: unreachable host");
			}
			this.imap.login(this.address, this.password);
			this.sendImapId();
			this.imap.selectInbox();
			const uids = this.imap.uidSearch("UNSEEN");
			for (const uid of uids) {
				if (this.seenUids.has(uid)) continue;
				let mail;
				try {
					mail = this.imap.uidFetchRfc822(uid);
				} catch {
					// Transient per-UID refusal: leave UNSEEN so the NEXT poll
					// retries it (#80016).
					continue;
				}
				// Mark seen ONCE a response arrived — a garbage response skips
				// once instead of retrying forever (#80032 review).
				this.seenUids.add(uid);
				if (this.seenUids.size > this.seenUidsMax) this.trimSeenUids();
				// Per-message poison guard: one bad message never aborts the batch
				// (it is already marked seen — logged UID and move on, #80032).
				try {
					const parsed = this.parseFetchedMessage(mail);
					if (parsed !== null) results.push(parsed);
				} catch (poisonErr) {
					this.logger?.warn?.(
						`[email] poison message UID ${mail.uid} skipped: ${brief(poisonErr)}`,
					);
				}
			}
			this.imap.logout();
		} catch (err) {
			this.lastFetchFailed = true;
			this.lastFetchError = brief(err);
		}
		// Snapshot updated after EVERY poll (mid-outage recreation restores an
		// up-to-date baseline).
		this.seenSnapshot.set(this.address, new Set(this.seenUids));
		return results;
	}

	private parseFetchedMessage(
		mail: ReturnType<FakeImapServer["uidFetchRfc822"]>,
	): ParsedMail | null {
		const senderAddr = extractEmailAddress(mail.fromRaw);
		let senderName = decodeHeaderValue(mail.fromRaw);
		if (senderName.includes("<")) {
			senderName =
				senderName.split("<")[0]?.trim().replace(/^"|"$/g, "") ?? senderName;
		}
		const subject = decodeHeaderValue(mail.headers.Subject ?? "(no subject)");

		// Automated/noreply senders skip BEFORE any processing.
		if (isAutomatedSender(senderAddr, mail.headers)) return null;

		// From-domain authentication verdict consumed at dispatch.
		const arHeaders = Object.entries(mail.headers)
			.filter(([k]) => k.toLowerCase() === "authentication-results")
			.map(([, v]) => v);
		const verdict = verifySenderAuthentication(
			arHeaders,
			senderAddr,
			this.authservId,
		);

		const body = extractTextBody(
			mail.parts.map((p) => ({
				contentType: p.contentType,
				disposition: p.disposition,
				payload: p.payload as Buffer | null,
				charset: p.charset ?? null,
			})),
		);
		const attachments = extractAttachments(
			mail.uid,
			mail.parts.map((p) => ({
				contentType: p.contentType,
				disposition: p.disposition,
				payload: p.payload as Buffer | null,
				...(p.filename !== undefined && p.filename !== null
					? { filename: p.filename }
					: {}),
			})),
		);

		return {
			uid: mail.uid,
			senderAddr,
			senderName,
			subject,
			messageId: mail.messageId,
			inReplyTo: mail.inReplyTo,
			body,
			date: mail.date,
			senderAuthenticated: verdict.authenticated,
			authReason: verdict.reason,
			attachments,
		};
	}

	/** adapter.py:_trim_seen_uids — keep only the TOP HALF by numeric UID. */
	private trimSeenUids(): void {
		if (this.seenUids.size <= this.seenUidsMax) return;
		const sorted = [...this.seenUids].sort((a, b) => Number(a) - Number(b));
		const keep = Math.floor(this.seenUidsMax / 2);
		this.seenUids.clear();
		for (const uid of sorted.slice(-keep)) this.seenUids.add(uid);
	}

	/** Test observability into the trimmed set. */
	seenUidList(): string[] {
		return [...this.seenUids];
	}

	// ── _dispatch_message gates (exact order) ──

	async dispatchMessage(message: ParsedMail): Promise<void> {
		const senderAddr = message.senderAddr;

		// 1. Self-messages never dispatch.
		if (senderAddr === this.address.toLowerCase()) return;
		// 2. Never reply to automated senders.
		if (isAutomatedSender(senderAddr, {})) return;

		// 3. Allowlist gate — unset AND no open-access opt-in drops EVERYONE
		//    (gateway default-deny parity). Open access opts in via EITHER
		//    mirror: EMAIL_ALLOW_ALL_USERS or GATEWAY_ALLOW_ALL_USERS
		//    (adapter.py:_dispatch_message truthy set {true,1,yes}).
		const flagTruthy = (name: string): boolean =>
			["true", "1", "yes"].includes(
				(this.secretReader(name) ?? "").trim().toLowerCase(),
			);
		const allowedRaw = (this.secretReader("EMAIL_ALLOWED_USERS") ?? "").trim();
		const allowAll =
			flagTruthy("EMAIL_ALLOW_ALL_USERS") ||
			flagTruthy("GATEWAY_ALLOW_ALL_USERS");
		if (allowedRaw.length === 0) {
			if (!allowAll) return;
		} else {
			const allowed = allowedRaw
				.split(",")
				.map((a) => a.trim().toLowerCase())
				.filter((a) => a.length > 0);
			if (!allowed.includes(senderAddr.toLowerCase())) return;
		}

		// 4. Authenticated-sender gate: enforced EXACTLY when an allowlist is
		// in effect AND allow-all is off (GHSA-rxqh fail-closed).
		const allowlistInEffect = allowedRaw.length > 0;
		if (
			this.requireAuthenticatedSender &&
			allowlistInEffect &&
			!allowAll &&
			!message.senderAuthenticated
		) {
			this.logger?.warn?.(
				`[email] Dropping unauthenticated From: ${senderAddr} (${message.authReason})`,
			);
			return;
		}

		const subject = message.subject;
		const body = message.body.trim();
		let text = body;
		if (subject.length > 0 && !subject.startsWith("Re:")) {
			text = `[Subject: ${subject}]\n\n${body}`;
		}
		if (text.length === 0) text = "(empty email)";

		// Attachments → media (adapter.py:_dispatch_message): URLs/types ride
		// the house event shape; DOCUMENT wins over PHOTO for mixed sets (an
		// image promotes TEXT only while still TEXT).
		const mediaUrls: string[] = [];
		const mediaTypes: string[] = [];
		let messageType: IncomingEvent["messageType"] = "text";
		for (const att of message.attachments) {
			mediaUrls.push(att.path);
			mediaTypes.push(att.mediaType);
			if (att.kind === "image") {
				if (messageType === "text") messageType = "photo";
			} else if (att.kind === "document") {
				messageType = "document";
			}
		}

		// Thread context for reply threading.
		this.threadContext.set(senderAddr, {
			subject,
			messageId: message.messageId,
		});

		const event: IncomingEvent = {
			messageType,
			text,
			source: {
				platform: "email",
				chatType: "dm",
				userId: senderAddr,
				chatId: senderAddr,
				chatName:
					message.senderName.length > 0 ? message.senderName : senderAddr,
			},
			...(message.inReplyTo.length > 0
				? { replyToMessageId: message.inReplyTo }
				: {}),
			...(mediaUrls.length > 0 ? { mediaUrls } : {}),
			...(mediaTypes.length > 0 ? { mediaTypes } : {}),
		};
		await this.deliverInbound(event, `email:${senderAddr}`);
	}

	// ══════════════════════════════════════════════════════════════════════
	// Guard wiring + egress doors (reference-fixture inheritance)
	// ══════════════════════════════════════════════════════════════════════

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: EMAIL_REGISTRY,
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
		if (String(event.source?.userId ?? "") === "bot-self") return;
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

	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith("(Response formatting failed, plain text:")
		) {
			return Promise.resolve({
				success: false,
				error: "Bad Request: can't parse entities",
			});
		}
		// MIME plain-only outbound (A19): the body ships AS TEXT; the SMTP
		// lane below wraps it in a text/plain part exclusively.
		return this.wireTransmitSend(chatId, content, {
			...metadata,
			email_mime_plain_only: true,
		});
	}

	async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		let chunks = [content];
		if (policy.lenFn(content) > policy.maxUnits) {
			// Gmail-safe budget: split on paragraph boundaries at the policy cap.
			chunks = splitByParagraphs(content, policy.maxUnits, policy.lenFn);
		}
		const total = chunks.length;
		const out: SendResult[] = [];
		for (let i = 0; i < chunks.length; i++) {
			const raw = chunks[i] ?? "";
			const labeled = total > 1 ? `${raw} (${i + 1}/${total})` : raw;
			out.push(await this.deliverViaLadder(chatId, labeled, metadata));
		}
		return out;
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
		if (outcome.tier === "rich") return outcome;

		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		const networkClassified =
			outcome.retryable === true ||
			failureClass === "connect-timeout" ||
			failureClass === "network" ||
			failureClass === "flood";
		if (networkClassified) {
			if (outcome.retryAfter != null)
				this.lastCapturedRetryAfterSeconds = outcome.retryAfter;
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
		_c: string,
		_m: string,
		_x: string,
		_o: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		return Promise.resolve({ success: false, error: "Not supported" });
	}
	protected override wireDraft(_a: DraftFrameArgs): Promise<SendResult> {
		return Promise.resolve({ success: false, error: "Not supported" });
	}
	sendTyping(_chatId: string): void {}

	// ══════════════════════════════════════════════════════════════════════
	// SMTP send lane (_send_email): MIME multipart, text/plain part ONLY;
	// threading headers from thread context; default subject "Hermes Agent";
	// RFC 2822 local-time Date (formatdate localtime=True, clock-pinned);
	// Message-ID hermes-<hex12>@domain; port 465 implicit TLS else STARTTLS;
	// A21 IPv4 fallback ladder on connection-class failures (TLS verification
	// errors NOT retried).
	// ══════════════════════════════════════════════════════════════════════

	private connectSmtp(ipv4Only = false): void {
		this.smtp.openConnection(this.smtpPort, ipv4Only);
		if (this.smtpPort === EMAIL_SMTP_IMPLICIT_TLS_PORT) {
			// implicit TLS — no STARTTLS verb
		} else {
			this.smtp.startTls();
		}
	}

	async sendEmail(
		toAddr: string,
		body: string,
		replyToMsgId?: string | undefined,
	): Promise<string> {
		const ctx = this.threadContext.get(toAddr) ?? {
			subject: "Hermes Agent",
			messageId: "",
		};
		let subject = ctx.subject;
		if (!subject.startsWith("Re:")) subject = `Re: ${subject}`;
		const originalMsgId = replyToMsgId ?? ctx.messageId;
		const msgId = `<hermes-${Math.floor(Math.random() * 0xffffffffffff)
			.toString(16)
			.padStart(12, "0")
			.slice(0, 12)}@${this.messageIdDomain()}>`;

		// Connection ladder (A21): default resolution first; connection-class
		// failures retry IPv4-only. TLS verification errors are NOT retried.
		try {
			this.connectSmtp(false);
		} catch (err) {
			if ((err as { name?: string }).name === "SSLError") throw err;
			this.connectSmtp(true); // IPv4-only retry
		}
		this.smtp.login(this.address, this.password);
		// msg["Date"] = formatdate(localtime=True) — epoch pinned via the clock
		// seam so contracts stay deterministic.
		const nowMs = this.clock !== undefined ? this.clock.nowMs() : Date.now();
		this.smtp.sendMessage({
			from: this.address,
			to: toAddr,
			subject,
			bodyText: body.slice(0, EMAIL_MAX_BODY_CHARS),
			headers: {
				...(originalMsgId !== undefined
					? { "In-Reply-To": originalMsgId, References: originalMsgId }
					: {}),
				Date: formatRfc2822Date(nowMs),
				"Message-ID": msgId,
			},
		});
		return msgId;
	}

	private messageIdDomain(): string {
		if (this.address.includes("@")) {
			return this.address.split("@").pop() || "localhost";
		}
		return "localhost";
	}

	// ── identity probes ──

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}
	routerAuditResolved(): readonly string[] {
		return this.routerResolved;
	}

	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		const credentialId = `account:${this.address.toLowerCase()}`;
		if (!this.lockHeld) {
			const first = this.acquireCredentialLock(
				this.lockManager,
				"email-account",
				credentialId,
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.acquireCredentialLock(
				this.lockManager,
				"email-account",
				credentialId,
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf("email-account", credentialId);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}

	buildMissingSecretSibling(): EmailAdapter {
		return new EmailAdapter({
			...this.deps,
			manifestName: `${this.manifestName}-no-secret`,
			secretReader: () => undefined,
		});
	}

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}

	private markFatal(code: string, detail: string, retryable: boolean): void {
		this.fatalCodes.push({ code, detail, retryable });
		this.lifecycle.markFatal({
			kind: "config_invalid",
			detail: `[${code}]${retryable ? "(retryable)" : ""} ${detail}`,
		});
	}

	readonly fatalCodes: Array<{
		code: string;
		detail: string;
		retryable: boolean;
	}> = [];
	private readonly spawn: TaskSpawner;

	private async sleepMs(ms: number): Promise<void> {
		if (this.clock !== undefined) {
			await this.clock.sleepMs(ms);
			return;
		}
		await new Promise<void>((r) => setTimeout(r, Math.min(ms, 5)));
	}
}

// ── helpers ──────────────────────────────────────────────────────────────

function brief(err: unknown): string {
	return String(err instanceof Error ? err.message : err).slice(0, 160);
}

const CHUNK_RESERVE = 8;

/** Largest prefix of `text` within `limit` units (word-boundary preferred). */
function largestPrefix(
	text: string,
	limit: number,
	lenFn: (s: string) => number,
): number {
	let low = 1;
	let high = text.length;
	let best = 0;
	while (low <= high) {
		const mid = (low + high) >> 1;
		if (lenFn(text.slice(0, mid)) <= limit) {
			best = mid;
			low = mid + 1;
		} else {
			high = mid - 1;
		}
	}
	const cut = best;
	const space = text.lastIndexOf(" ", cut);
	return space > Math.floor(cut / 3) ? space : cut;
}

function splitByParagraphs(
	content: string,
	maxUnits: number,
	lenFn: (s: string) => number,
): string[] {
	const budget = maxUnits - CHUNK_RESERVE;
	const chunks: string[] = [];
	let current = "";
	for (const paragraph of content.split("\n\n")) {
		const candidate =
			current.length === 0 ? paragraph : `${current}\n\n${paragraph}`;
		if (lenFn(candidate) <= budget) {
			current = candidate;
			continue;
		}
		if (current.length > 0) chunks.push(current);
		// Oversized single paragraph: hard-split at unit windows.
		let rest = paragraph;
		while (rest.length > 0 && lenFn(rest) > budget) {
			const cut = largestPrefix(rest, budget, lenFn);
			if (cut === 0) break;
			chunks.push(rest.slice(0, cut).trimEnd());
			rest = rest.slice(cut).trimStart();
		}
		current = rest;
	}
	if (current.length > 0) chunks.push(current);
	return chunks.length > 0 ? chunks : [content];
}

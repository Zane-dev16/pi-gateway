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
//                       as documents; config skip_attachments short-circuits
//                       the whole walk (adapter.py:565/:969).
//   _dispatch_message   gates in order: self-drop → automated drop → allowlist
//                       gate (unset + no allow-all ⇒ drop; EMAIL_ALLOW_ALL_USERS
//                       OR GATEWAY_ALLOW_ALL_USERS opt in) → authenticated-
//                       sender gate ONLY when allowlist in effect & allow-all
//                       off (GHSA-rxqh-5572-8m77 fail-closed); Authentication-
//                       Results instances feed verifySenderAuthentication as an
//                       ORDERED LIST — first-instance trust, optional authserv-id
//                       pin (msg.get_all parity; Record collapse never decides).
//   _send_email         MIMEMultipart with MIMEText PLAIN only (FULL body — the
//                       50000 cap is plugin max_message_length metadata feeding
//                       policy chunking upstream, never an SMTP-lane slice);
//                       thread context Re:/In-Reply-To/References; default
//                       subject "Hermes Agent"; RFC 2822 local-time Date;
//                       Message-ID hermes-<hex>; port 465 implicit TLS else
//                       STARTTLS; A21 IPv4 ladder; smtp.quit() in finally
//                       (best-effort, chased by close) after EVERY send and
//                       connect test.
//   send_document /     MIMEBase('application','octet-stream') base64 parts
//   send_multiple_images attached to the thread-context multipart (local files;
//                       remote URLs link in the body).
//
// PROPOSED DEC text lives in email-world.ts (polling-row leg mappings).

import { existsSync, readFileSync } from "node:fs";
import { basename } from "node:path";

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
	type SentAttachment,
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
				/** adapter.py extra.get("skip_attachments") — operator opt-out that
				 *  short-circuits attachment extraction entirely (:565/:969). */
				skipAttachments?: boolean | undefined;
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
	private readonly skipAttachments: boolean;

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
		// _esecret_int parity (adapter.py:78-86): strip, treat BLANK/whitespace as
		// unset, strict integer parse — a blank-but-present value falls back to the
		// default instead of Number('')===0 driving port-0 connects / 0ms polls.
		this.imapPort = esecretInt(env, "EMAIL_IMAP_PORT", EMAIL_IMAP_PORT_DEFAULT);
		this.smtpHost = (env("EMAIL_SMTP_HOST") ?? "").trim();
		this.smtpPort = esecretInt(env, "EMAIL_SMTP_PORT", EMAIL_SMTP_PORT_DEFAULT);
		const pollRaw = (env("EMAIL_POLL_INTERVAL") ?? "").trim();
		this.pollIntervalMs =
			pollRaw.length > 0 && /^[+-]?\d+$/.test(pollRaw)
				? Number(pollRaw) * 1000
				: (deps.config?.pollIntervalMs ?? EMAIL_POLL_INTERVAL_MS);
		if (this.password.length > 0) this.registerLogSecret(this.password);

		// require_authenticated_sender: extra flag > EMAIL_TRUST_FROM_HEADER
		// mirror > DEFAULT ON (GHSA-rxqh fail-closed posture).
		const trustFromHeader = ["true", "1", "yes"].includes(
			(env("EMAIL_TRUST_FROM_HEADER") ?? "").toLowerCase(),
		);
		this.requireAuthenticatedSender =
			deps.config?.requireAuthenticatedSender ??
			(trustFromHeader ? false : true);
		// adapter.py:591-592 — config extra first, EMAIL_AUTHSERV_ID secret second;
		// env-configured operators keep their injected-header pinning defense.
		this.authservId = (
			deps.config?.authservId ||
			(env("EMAIL_AUTHSERV_ID") ?? "")
		)
			.trim()
			.toLowerCase();
		// adapter.py:565 — operator opt-out: when set, attachment/inline parts are
		// ignored entirely (malware protection / bandwidth savings).
		this.skipAttachments = deps.config?.skipAttachments === true;

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
		let imapHandleOpen = false;
		try {
			this.imap.connectCalls += 1;
			if (this.imap.connectFailuresArmed > 0) {
				this.imap.connectFailuresArmed -= 1;
				throw new Error("unreachable host");
			}
			// TCP handle exists from here on: teardown is due on EVERY path
			// (#79889 — adapter.py connect() finally: _close_imap(imap)).
			imapHandleOpen = true;
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
		} catch (err) {
			// Generic IMAP error: kept RETRYABLE deliberately — imaplib raises the
			// same IMAP4.error for bad credentials AND transient NOs (OOF-156).
			this.markFatal(
				"email_imap_connect_error",
				`IMAP connection to ${this.imapHost}:${this.imapPort} failed: ${brief(err)}`,
				true,
			);
			return false;
		} finally {
			if (imapHandleOpen) {
				// _close_imap parity (adapter.py:115/:744): best-effort teardown
				// OUTSIDE the failing try — an abort raised by logout can never
				// fail an otherwise-successful connection test.
				try {
					this.imap.logout();
				} catch {
					/* eaten — socket death is guaranteed regardless */
				}
			}
		}
		this.seenSnapshot.set(this.address, new Set(this.seenUids));

		// SMTP connection test (_connect_smtp ladder + quit-in-finally).
		try {
			this.connectSmtpWithLadder();
			try {
				this.smtp.login(this.address, this.password);
			} finally {
				this.smtp.quit();
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
		let imapHandleOpen = false;
		try {
			this.imap.connectCalls += 1;
			if (this.imap.connectFailuresArmed > 0) {
				throw new Error("IMAP fetch error: unreachable host");
			}
			// Handle exists from here on — teardown due on EVERY path (#79889).
			imapHandleOpen = true;
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
		} catch (err) {
			this.lastFetchFailed = true;
			this.lastFetchError = brief(err);
		} finally {
			if (imapHandleOpen) {
				// _close_imap parity (adapter.py:115/:919): teardown lives OUTSIDE
				// the failing try with its exceptions eaten — an armed
				// ImapLogoutAbortError can never mark fully-successful fetches as
				// failed and spur the reconnect escalation ladder.
				try {
					this.imap.logout();
				} catch {
					/* eaten — socket death is guaranteed regardless */
				}
			}
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

		// From-domain authentication verdict consumed at dispatch. The A-R
		// headers feed verifySenderAuthentication as an ORDERED LIST (eml-7):
		// msg.get_all preserves duplicates and the receiving server PREPENDS its
		// verdict, so first-instance trust is only representable when every
		// instance survives — a collapsed Record decided with last-wins bytes.
		const arHeaders = mail.headerList
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
		// skip_attachments short-circuit (adapter.py:969): when configured, ALL
		// attachment/inline parts are ignored (malware protection / bandwidth).
		const attachments = this.skipAttachments
			? []
			: extractAttachments(
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
	// FULL body (the 50000 cap lives upstream as plugin max_message_length
	// metadata feeding deliverText policy chunking — never an SMTP-lane slice);
	// threading headers from thread context; default subject "Hermes Agent";
	// RFC 2822 local-time Date (formatdate localtime=True, clock-pinned);
	// Message-ID hermes-<hex12>@domain; port 465 implicit TLS else STARTTLS;
	// A21 IPv4 fallback ladder on connection-class failures (TLS verification
	// errors NOT retried); smtp.quit() best-effort in EVERY lane's finally.
	// ══════════════════════════════════════════════════════════════════════

	/** One SMTP connection (adapter.py:_connect_smtp._connect). */
	private connectSmtp(ipv4Only = false): void {
		const conn = this.smtp.openConnection(this.smtpPort, ipv4Only);
		try {
			if (this.smtpPort === EMAIL_SMTP_IMPLICIT_TLS_PORT) {
				// implicit TLS — no STARTTLS verb
			} else {
				this.smtp.startTls();
			}
		} catch (err) {
			conn.close(); // smtp.close() before raise — never leak the handle
			throw err;
		}
	}

	/**
	 * adapter.py:_connect_smtp — the connection plus its A21 ladder INSIDE:
	 * connection-class failures retry IPv4-only; TLS verification errors are
	 * NOT retried. Shared by the connect test AND every send lane (the ladder
	 * is not a send-only affordance).
	 */
	private connectSmtpWithLadder(): void {
		try {
			this.connectSmtp(false);
		} catch (err) {
			if ((err as { name?: string }).name === "SSLError") throw err;
			this.connectSmtp(true); // IPv4-only retry
		}
	}

	/** Best-effort session teardown (adapter.py send lanes' finally). */
	private quitSmtpBestEffort(): void {
		try {
			this.smtp.quit();
		} catch {
			this.smtp.close(); // socket death guaranteed regardless (#79889)
		}
	}

	private newMessageId(): string {
		return `<hermes-${Math.floor(Math.random() * 0xffffffffffff)
			.toString(16)
			.padStart(12, "0")
			.slice(0, 12)}@${this.messageIdDomain()}>`;
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
		const msgId = this.newMessageId();

		this.connectSmtpWithLadder();
		try {
			this.smtp.login(this.address, this.password);
			// msg["Date"] = formatdate(localtime=True) — epoch pinned via the clock
			// seam so contracts stay deterministic.
			const nowMs = this.clock !== undefined ? this.clock.nowMs() : Date.now();
			this.smtp.sendMessage({
				from: this.address,
				to: toAddr,
				subject,
				// MIMEText(body) carries the FULL body (adapter.py:_send_email) —
				// no SMTP-lane truncation; policy chunking owns the 50000 budget.
				bodyText: body,
				headers: {
					...(originalMsgId.length > 0
						? { "In-Reply-To": originalMsgId, References: originalMsgId }
						: {}),
					Date: formatRfc2822Date(nowMs),
					"Message-ID": msgId,
				},
			});
		} finally {
			this.quitSmtpBestEffort();
		}
		return msgId;
	}

	// ── outbound media lanes (adapter.py send_image / send_document /
	//    send_multiple_images — DEC-019 post-stream delivery surface) ──

	/** adapter.py:send_image — URL linked into the body; plain send. */
	async sendImage(
		chatId: string,
		imageSource: string,
		caption?: string | undefined,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		let text = caption ?? "";
		text += `\n\nImage: ${imageSource}`;
		return this.send(chatId, text.trim(), replyTo);
	}

	/**
	 * adapter.py:send_multiple_images — ONE email carrying every local file as
	 * a MIMEBase('application','octet-stream') base64 part; remote URLs link in
	 * the body. No hard cap — email clients handle dozens of attachments.
	 */
	async sendMultipleImages(
		chatId: string,
		images: readonly string[],
	): Promise<SendResult[]> {
		if (images.length === 0) return [];
		const bodyParts: string[] = [];
		const localPaths: string[] = [];
		for (const source of images) {
			const localPath = unwrapLocalMediaPath(source);
			if (localPath !== null) {
				if (existsSync(localPath)) {
					localPaths.push(localPath);
				} else {
					this.logger?.warn?.(`[email] Skipping missing image: ${localPath}`);
				}
			} else {
				// Remote URLs just get linked in the body (send_image parity).
				bodyParts.push(`Image: ${source}`);
			}
		}
		if (localPaths.length === 0 && bodyParts.length === 0) return [];
		try {
			const messageId = this.sendEmailWithAttachments(
				chatId,
				bodyParts.join("\n\n"),
				localPaths.map((p) => ({ path: p, filename: basename(p) })),
				true,
			);
			return [{ success: true, messageId }];
		} catch {
			// super().send_multiple_images fallback: degrade to per-image
			// send_image links instead of losing the batch.
			const results: SendResult[] = [];
			for (const source of images) {
				results.push(await this.sendImage(chatId, source));
			}
			return results;
		}
	}

	/** adapter.py:send_document — one file attached preserving original bytes. */
	async sendDocument(
		chatId: string,
		filePath: string,
		filename?: string | undefined,
		caption?: string | undefined,
	): Promise<SendResult> {
		try {
			const messageId = this.sendEmailWithAttachments(
				chatId,
				caption ?? "",
				[
					{
						path: filePath,
						filename: filename ?? basename(filePath),
					},
				],
				false,
			);
			return { success: true, messageId };
		} catch (err) {
			return { success: false, error: brief(err) };
		}
	}

	/**
	 * adapter.py:_send_email_with_attachments / _send_email_with_attachment —
	 * thread-context multipart whose files ride octet-stream base64 parts.
	 * `skipUnreadable` mirrors the lane split: the multi-image lane logs and
	 * SKIPS an unreadable file while the document lane fails the whole send.
	 */
	private sendEmailWithAttachments(
		toAddr: string,
		body: string,
		files: ReadonlyArray<{ path: string; filename: string }>,
		skipUnreadable: boolean,
	): string {
		const ctx = this.threadContext.get(toAddr) ?? {
			subject: "Hermes Agent",
			messageId: "",
		};
		let subject = ctx.subject;
		if (!subject.startsWith("Re:")) subject = `Re: ${subject}`;
		const originalMsgId = ctx.messageId;
		const msgId = this.newMessageId();
		const nowMs = this.clock !== undefined ? this.clock.nowMs() : Date.now();

		const attachments: SentAttachment[] = [];
		for (const file of files) {
			try {
				attachments.push({
					contentType: "application/octet-stream",
					filename: file.filename,
					payloadBase64: readFileSync(file.path).toString("base64"),
				});
			} catch (err) {
				if (!skipUnreadable) throw err;
				this.logger?.warn?.(
					`[email] Failed to attach ${file.path}: ${brief(err)}`,
				);
			}
		}

		this.connectSmtpWithLadder();
		try {
			this.smtp.login(this.address, this.password);
			this.smtp.sendMessage({
				from: this.address,
				to: toAddr,
				subject,
				bodyText: body,
				headers: {
					...(originalMsgId.length > 0
						? { "In-Reply-To": originalMsgId, References: originalMsgId }
						: {}),
					Date: formatRfc2822Date(nowMs),
					"Message-ID": msgId,
				},
				attachments,
			});
		} finally {
			this.quitSmtpBestEffort();
		}
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

/**
 * adapter.py:_esecret_int — scope-aware integer env read. Blank/whitespace ⇒
 * default; non-integer text (int() would raise) ⇒ default. Prevents the
 * Number('')===0 trap: port 0 connects, poll interval 0 busy-loops.
 */
function esecretInt(
	read: (name: string) => string | undefined,
	name: string,
	fallback: number,
): number {
	const raw = (read(name) ?? "").trim();
	if (raw.length === 0) return fallback;
	return /^[+-]?\d+$/.test(raw) ? Number(raw) : fallback;
}

/**
 * Media-source classification for the attachment lanes. Hermes run.py wraps
 * local paths as file:// URLs before send_multiple_images; pi's post-stream
 * rescan passes expanded bare paths — BOTH attach. True scheme URLs link in
 * the body (adapter.py remote-URL parity). Returns null for remote sources.
 */
function unwrapLocalMediaPath(source: string): string | null {
	if (source.startsWith("file://")) {
		const raw = source.slice("file://".length);
		try {
			return decodeURIComponent(raw); // urllib.parse.unquote parity
		} catch {
			return raw;
		}
	}
	if (/^[a-zA-Z][a-zA-Z\d+.-]*:\/\//.test(source)) return null; // remote URL
	return source;
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

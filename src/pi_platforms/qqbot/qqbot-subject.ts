// pi_platforms/qqbot/qqbot-subject — the QQBot adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). Shared rows run against the
// REAL kit-built adapter over FakePlatformWire egress capture; the gateway +
// REST planes are exercised through the engine fixture against FakeQQGateway.

import {
	TokenLockManagerSeam,
	resolveEnablement,
	FormattingLadder,
} from "../kit/index.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { CallbackQueryRouter } from "../kit/callback-router.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { QQBotAdapter, type QQRestTransport } from "./qqbot-adapter.js";
import { FakeQQGateway } from "./fake-qq-gateway.js";
import { QQBOT_PLUGIN_MANIFEST } from "./manifest.js";

export interface QQSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	scheduler?: ManualScheduler | undefined;
	gateway?: FakeQQGateway | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	appId?: string | undefined;
	clientSecret?: string | undefined;
	markdownSupport?: boolean | undefined;
}

/**
 * Capture seam: routes the adapter's REST transport into FakePlatformWire so
 * shared egress rows observe chunk/fallback behavior, while ALSO recording
 * vendor-shaped ops on the gateway face.
 */
function makeRestCapture(
	raw: FakePlatformWire,
	gateway: FakeQQGateway,
): QQRestTransport {
	return {
		async request(method, path, body, headers) {
			const bare = stripBase(path);
			if (method === "POST" && /\/messages$/.test(bare)) {
				const meta = {
					...(headers ?? {}),
					rest_path: bare,
				} as unknown as Metadata;
				// Scripted wire behaviors GOVERN the send result (shared rows
				// program failures/timeouts); unscripted sends record and fall
				// through to the fake gateway's REST face.
				if (raw.hasScript("send")) {
					const res = await raw.transmitSend(
						logicalChatIdOf(bare),
						extractSentText(asRecord(body)),
						meta,
					);
					return res.success
						? { status: 200, body: { id: res.messageId ?? "wmsg-scripted" } }
						: {
								status: 400,
								body: { message: res.error ?? "scripted failure" },
							};
				}
				await raw.transmitSend(
					logicalChatIdOf(bare),
					extractSentText(asRecord(body)),
					meta,
				);
			}
			// Delegate ALL calls to the fake gateway's scripted REST face.
			const resp = gateway.handleRest(
				method,
				bare,
				asRecord(body),
				normalizeHeaders(headers),
			);
			return resp;
		},
	};
}

function extractSentText(body: Record<string, unknown> | Buffer): string {
	if (Buffer.isBuffer(body)) return "";
	const md = body["markdown"];
	if (md !== null && typeof md === "object") {
		return String((md as Record<string, unknown>)["content"] ?? "");
	}
	return String(body["content"] ?? "");
}

/** Derive the LOGICAL chat id from a v2 messages path (row observability). */
function logicalChatIdOf(path: string): string {
	const m = /\/v2\/(?:users|groups)\/([^/]+)\/messages/.exec(path);
	if (m !== null) return m[1] as string;
	const g = /\/channels\/([^/]+)\/messages/.exec(path);
	return g !== null ? (g[1] as string) : path;
}

function stripBase(path: string): string {
	return path.startsWith("https://api.sgroup.qq.com")
		? path.slice("https://api.sgroup.qq.com".length)
		: path;
}

function asRecord(
	body: Record<string, unknown> | Buffer,
): Record<string, unknown> {
	return Buffer.isBuffer(body) ? {} : body;
}

function normalizeHeaders(
	headers?: Record<string, string> | undefined,
): Record<string, string> | undefined {
	return headers;
}

/** The qqbot-shaped ConformanceSubject over the REAL adapter. */
export class QQBotSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: QQBotAdapter;
	readonly wire: FakePlatformWire;
	readonly gateway: FakeQQGateway;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: QQSubjectOptions) {
		this.name = opts.name ?? "qqbot";
		this.wire = opts.wire;
		this.gateway = opts.gateway ?? new FakeQQGateway();
		const hasSecret = opts.withSecret !== false;
		const appId = opts.appId ?? (hasSecret ? "fake-app-id" : "");
		const clientSecret = opts.clientSecret ?? (hasSecret ? "fake-secret" : "");
		this.adapter = new QQBotAdapter({
			scalarMaxUnits: 64, // harness-scale budget mirrors reference subjects
			appId,
			clientSecret,
			...(opts.markdownSupport !== undefined
				? { markdownSupport: opts.markdownSupport }
				: {}),
			rest: makeRestCapture(this.wire, this.gateway),
			wsFactory: this.gateway,
			// §10.1 tier-1 rich probe rides the SAME capture wire; unscripted
			// probes answer the capability-error shape WITHOUT a roundtrip.
			richProbe: (content) => this.wire.transmitRich("__rich__", content, {}),
			richHasScript: () => this.wire.hasScript("rich"),
		});
		// Guard ALWAYS attached: with the row-supplied ManualScheduler when one
		// is passed (shared rows), else the immediate production spawner.
		this.adapter.attachStandardGuard(opts.spawner);
		if (opts.scheduler !== undefined) {
			(this as unknown as Record<symbol, unknown>)[SCHEDULER_SYMBOL] =
				opts.scheduler;
		}
	}

	// ── observability ──
	doorAudit() {
		return this.adapter.doorAudit();
	}
	turns(): readonly string[] {
		return this.adapter.turnLog;
	}
	replies(): readonly string[] {
		return this.adapter.replyLog;
	}
	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.adapter.lifecycle.statusSnapshot();
	}

	// ── ingress lane ──
	deliverInbound(event: IncomingEvent, sessionKey: string): Promise<void> {
		return this.adapter.deliverInbound(event, sessionKey);
	}
	holdTurnsForBurst(on: boolean): void {
		this.adapter.holdTurns(on);
	}
	armClarifyIntercept(sessionKey: string): void {
		this.adapter.setClarifyIntercept(sessionKey, true);
	}
	disarmClarifyIntercept(): void {
		this.adapter.clarifyArmed.clear();
	}
	clarifyCaptures(): readonly string[] {
		return this.adapter.clarifyCaptures;
	}

	// ── egress lanes ──
	sendThroughDoor1(
		chatId: string,
		content: string,
		metadata?: Metadata,
	): Promise<SendResult> {
		return this.adapter.send(chatId, content, undefined, metadata);
	}
	sendThroughDoor2(
		platform: string,
		chatId: string,
		content: string,
		metadata?: Metadata,
	): Promise<SendResult> {
		return this.adapter.sendForPlatform(
			platform,
			chatId,
			content,
			undefined,
			metadata,
		);
	}
	sendInterim(chatId: string, content: string): Promise<SendResult> {
		return this.adapter.send(chatId, content, undefined, {
			_interim_send: true,
		} as unknown as Metadata);
	}
	deliverLongText(chatId: string, content: string): Promise<SendResult[]> {
		return this.adapter.deliverText(chatId, content);
	}
	deliverToUtf16Chat(chatId: string, content: string): Promise<SendResult[]> {
		return this.adapter.deliverText(chatId, content);
	}
	async deliverFormattingRejected(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		const results = await this.adapter.deliverText(chatId, content, {
			forceFormattingError: true,
		} as Metadata);
		return results[results.length - 1] ?? { success: false };
	}
	transientRichFailureOutcome(
		_chatId: string,
		content: string,
	): Promise<SendResult> {
		// Fresh ladder lane against a rich endpoint failing TRANSIENTLY:
		// outcome must be a retryable failure and NO legacy send.
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
	async parseFailurePlainResend(
		chatId: string,
		content: string,
	): Promise<string> {
		await this.deliverFormattingRejected(chatId, content);
		const sends = this.wire.sendsOf(chatId);
		return sends[sends.length - 1]?.content ?? "";
	}
	chatPolicyFor(chatId: string): ChatLengthPolicy {
		return this.adapter.chatLengthPolicyForChat(chatId);
	}

	// ── streaming seam (no native lanes on the QQ v2 wire) ──
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // no native stream lanes
	}
	failNextSeals(n: number): void {
		this.wire.script(
			"seal",
			...Array.from({ length: n }, () => ({
				kind: "fail" as const,
				error: "forced seal failure",
			})),
		);
	}

	// ── interactive surfaces ──
	callbackRouter(): CallbackQueryRouter {
		return this.adapter.router;
	}
	actionRegistry(): import("../kit/block-kit.js").ActionHandlerRegistry {
		return this.adapter.actionRegistry;
	}
	registerApprovalPending(id: number, sessionKey: string): void {
		this.adapter.approvals.register(id, sessionKey);
	}
	registerSlashConfirmPending(id: number, sessionKey: string): void {
		this.adapter.slashConfirms.register(id, sessionKey);
	}
	registerClarifyPending(id: number, sessionKey: string): void {
		this.adapter.clarify.register(id, sessionKey);
	}
	registerApprPending(id: number, sessionKey: string): void {
		this.adapter.appr.register(id, sessionKey);
	}
	setClickerAuthorization(allow: boolean): void {
		this.adapter.setClickerAuthorization(allow);
	}
	resolvedFamilies(): readonly string[] {
		return this.adapter.resolvedFamilies;
	}
	resolvedTurnDispatches(): readonly string[] {
		return [];
	}

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.adapter.acquireCredentialLock(
				this.lockManager,
				"qqbot-appid",
				`cred-${this.adapter.appId}`,
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"qqbot-appid",
				`cred-${this.adapter.appId}`,
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"qqbot-appid",
				`cred-${this.adapter.appId}`,
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new QQBotAdapter({
			appId: "",
			clientSecret: "",
			rest: makeRestCapture(this.wire, this.gateway),
			wsFactory: this.gateway,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "qq-scoped-probe",
				description: "",
				transportShape: "ws",
				requiresEnv: [{ name: envKey }],
				capabilities: {},
			},
			() => undefined,
		).enabled;
	}

	// ── DEC-022 declaration ──
	wakeLaneDeclaration(): "forged-event" | "raw-key-direct" {
		return this.adapter.wakeLane;
	}

	// ── manifest probe (wiring assertions) ──
	get manifestName(): string {
		return QQBOT_PLUGIN_MANIFEST.name;
	}
}

export function makeQQSubject(
	opts: QQSubjectOptions & { wire: FakePlatformWire },
): QQBotSubject {
	return new QQBotSubject(opts);
}

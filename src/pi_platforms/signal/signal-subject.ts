// pi_platforms/signal/signal-subject — the Signal adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). The shared rows run against
// the REAL kit-built adapter with FakePlatformWire egress capture (a subject-
// level RPC bridge models the daemon's JSON-RPC plane ON TOP of the shared
// wire); the SSE/health/engine lanes are exercised by signal-engine tests
// against FakeSignalCliServer directly.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
	PLAIN_TEXT_FALLBACK_PREFIX,
	resolveEnablement,
	TokenLockManagerSeam,
} from "../kit/index.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import type { CallbackQueryRouter } from "../kit/callback-router.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { markdownToSignal } from "./signal-format.js";
import { SIGNAL_PLUGIN_MANIFEST } from "./manifest.js";
import {
	jsonRpcBody,
	type RpcOutcome,
	sseData,
	type SignalCliTransport,
	type SignalEventStream,
} from "./signal-wire.js";
import { SignalAdapter } from "./signal-adapter.js";

export interface SignalSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	/**
	 * Lie-scan fixture seam ONLY: flips THE manifest datum that drives the
	 * streaming-exclusion probe so the negative gate can prove a lying
	 * capability claim FAILS the streaming family rows. Never set in production.
	 */
	declaredMessageEditing?: boolean | undefined;
}

/**
 * Subject-level RPC bridge: models the signal-cli JSON-RPC plane on top of
 * the shared harness wire so every user-visible transmission lands in
 * wire.ops. "send" records as a send op whose content is the CONVERTED plain
 * body + style metadata; typing/reaction ops ride dedicated audit lanes so
 * they never pollute send-op accounting. Attachment/contact RPCs refuse
 * loudly (engine tests bind FakeSignalCliServer).
 */
class SubjectRpcBridge implements SignalCliTransport {
	private seqCounter = 0;

	constructor(private readonly raw: FakePlatformWire) {}

	async rpc(
		method: string,
		params: Record<string, unknown>,
		opts?: { id?: string; metadata?: Metadata },
	): Promise<RpcOutcome> {
		void opts?.id;
		if (method === "send") {
			const chatId = addressChatId(params);
			const body = String(params["message"] ?? "");
			const styles = Array.isArray(params["textStyles"])
				? (params["textStyles"] as string[])
				: typeof params["textStyle"] === "string"
					? [String(params["textStyle"])]
					: [];
			const metadata: Metadata = {};
			if (params["groupId"] !== undefined)
				metadata["signal_group_id"] = params["groupId"];
			if (params["recipient"] !== undefined)
				metadata["signal_recipient"] = params["recipient"];
			if (styles.length > 0) metadata["signal_text_styles"] = styles.join(",");
			// Markdown-rendering rejection script — EXACTLY like the reference
			// subjects: a forced formatting error fails unless this IS already
			// the §6.1 plain-text fallback body.
			if (
				opts?.metadata?.["forceFormattingError"] === true &&
				!body.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			) {
				return {
					ok: false,
					error: { code: -32600, message: "can't parse entities" },
				};
			}
			const result = await this.raw.transmitSend(chatId, body, metadata);
			return result.success
				? {
						ok: true,
						result: {
							timestamp: seqTs(++this.seqCounter),
							results: [{ type: "SUCCESS" }],
						},
					}
				: {
						ok: false,
						error: { code: -1, message: result.error ?? "send failed" },
					};
		}
		// Typing / reaction / contact RPCs are NOT user-visible transmissions:
		// they answer OK without touching the harness wire (engine tests bind
		// FakeSignalCliServer for those lanes) so send-op accounting stays clean.
		return { ok: true, result: {} };
	}

	async checkHealth(): Promise<boolean> {
		return true;
	}

	async openEventStream(): Promise<SignalEventStream> {
		throw new Error("event streams require the FakeSignalCliServer fixture");
	}

	// Rich-probe passthrough (§10.1 latch row) — reference-subject parity.
	hasRichScript(): boolean {
		return this.raw.hasScript("rich");
	}

	async transmitRichProbe(chatId: string, content: string): Promise<boolean> {
		const result = await this.raw.transmitRich(chatId, content, {});
		return result.success;
	}
}

function seqTs(n: number): number {
	return 1_700_000_000_000 + n;
}

/** Inverse of the adapter's addressing split (groupId vs recipient list). */
function addressChatId(params: Record<string, unknown>): string {
	if (typeof params["groupId"] === "string")
		return `group:${params["groupId"]}`;
	const recipient = params["recipient"];
	if (Array.isArray(recipient)) return String(recipient[0] ?? "");
	return "";
}

/** The ws-shaped ConformanceSubject over the REAL Signal engine. */
export class SignalSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: SignalAdapter;
	readonly wire: FakePlatformWire;
	readonly bridge: SubjectRpcBridge;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: SignalSubjectOptions) {
		this.name = opts.name ?? "signal-reference";
		this.wire = opts.wire;
		this.bridge = new SubjectRpcBridge(opts.wire);
		this.adapter = new SignalAdapter({
			transport: this.bridge,
			account: "signal-account",
			scalarMaxUnits: 64, // harness-scale budget mirrors the reference subjects
			mediaCacheDir: harnessTmpDir(),
			secretReader: (key) =>
				opts.withSecret === false ? undefined : signalHarnessSecret(key),
			...(opts.declaredMessageEditing !== undefined
				? { declaredMessageEditing: opts.declaredMessageEditing }
				: {}),
		});
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
		} as unknown as Metadata);
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

	// ── streaming seam (probe-computed exclusion: no edits ⇒ no drafts) ──────
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // no native stream lanes on Signal
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
		// The router NEVER dispatches turns for stale/unknown taps.
		return [...this.adapter.resolvedFamilies];
	}

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.adapter.acquireCredentialLock(
				this.lockManager,
				"signal-account",
				"cred-signal-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"signal-account",
				"cred-signal-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"signal-account",
				"cred-signal-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new SignalAdapter({
			transport: new SubjectRpcBridge(new FakePlatformWire()),
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "signal-scoped-probe",
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

	/** Converted body + styles for a content sample (format-lane evidence). */
	formatPreview(content: string): {
		body: string;
		styles: string[];
	} {
		const [body, styles] = markdownToSignal(content);
		return { body, styles };
	}

	pluginManifest() {
		return SIGNAL_PLUGIN_MANIFEST;
	}
}

/** Scoped harness secrets (fail-closed reader shape; never process.env). */
function signalHarnessSecret(name: string): string | undefined {
	switch (name) {
		case "SIGNAL_HTTP_URL":
			return "http://127.0.0.1:8080";
		case "SIGNAL_ACCOUNT":
			return "+15550001111";
		default:
			return undefined;
	}
}

/**
 * One throwaway tmp dir per WORKER PROCESS for inbound media caches (subjects
 * must NEVER write into the repo; mkdtemp isolation).
 */
let harnessTmp: string | undefined;
function harnessTmpDir(): string {
	if (harnessTmp === undefined) {
		harnessTmp = mkdtempSync(join(tmpdir(), "signal-subject-"));
	}
	return harnessTmp;
}

export function makeSignalSubject(
	opts: SignalSubjectOptions & { wire: FakePlatformWire },
): SignalSubject {
	return new SignalSubject(opts);
}

// Re-exported for the wiring suite's mutant construction.
export { sseData, jsonRpcBody };

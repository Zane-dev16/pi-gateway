// pi_platforms/whatsapp-cloud/wa-cloud-subject — the WhatsApp Cloud adapter
// wired as a ConformanceSubject (04 §8 merge-gate wiring). The shared rows run
// against the REAL kit-built adapter with FakePlatformWire egress capture; the
// Graph plane (media/receipts/signature/envelope lanes) is exercised by the
// engine fixture against FakeGraphServer directly.

import { TokenLockManagerSeam, resolveEnablement } from "../kit/index.js";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
import { validateTrustBoundaryManifest } from "../kit/trust.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { WaCloudAdapter } from "./wa-cloud-adapter.js";
import type { GraphResponse } from "./graph-wire.js";

export interface WaCloudSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
}

interface BridgeReceiptRecord {
	body: Record<string, unknown>;
	seq: number;
}

/**
 * The harness bridge: models the Graph /messages edge ON TOP of the shared
 FakePlatformWire so every user-visible transmission lands in wire.ops —
 text/interactive sends record as `send` ops; status:"read" receipts are
 captured SEPARATELY (they are UX polish, not user-visible transmissions) and
 media-plane calls refuse loudly (engine tests bind FakeGraphServer).
 */
class WireBridge {
	private seqCounter = 0;
	readonly receipts: BridgeReceiptRecord[] = [];

	constructor(private readonly raw: FakePlatformWire) {}

	async postMessages(
		body: Record<string, unknown>,
		metadata: Metadata = {},
	): Promise<GraphResponse> {
		if (body["status"] === "read") {
			this.seqCounter += 1;
			this.receipts.push({ body, seq: this.seqCounter });
			return {
				status: 200,
				json: { messages: [{ id: `wamid.receipt.${this.seqCounter}` }] },
			};
		}
		const to = String(body["to"] ?? "");
		const kind = String(body["type"] ?? "text");
		let content: string;
		if (kind === "text") {
			content = String(
				(body["text"] as Record<string, unknown> | undefined)?.["body"] ?? "",
			);
			// Markdown-rendering rejection script — EXACTLY like the reference
			// fixture: a forced formatting error fails unless this IS already the
			// plain-text fallback body (§6.1 lane succeeds on the wire).
			if (
				metadata["forceFormattingError"] === true &&
				!content.startsWith("(Response formatting failed, plain text:")
			) {
				return {
					status: 400,
					json: {
						error: {
							message: "Bad Request: can't parse entities",
							code: 0,
						},
					},
				};
			}
		} else if (kind === "interactive") {
			const inter = body["interactive"] as Record<string, unknown>;
			content = `[interactive:${String(inter?.["type"] ?? "?")}]`;
		} else {
			content = `[${kind}]`;
		}
		const result = await this.raw.transmitSend(to, content, metadata);
		return result.success
			? {
					status: 200,
					json: { messages: [{ id: result.messageId ?? "wamid.bridge" }] },
				}
			: {
					status: 400,
					json: { error: { message: result.error ?? "send failed", code: -1 } },
				};
	}

	receiptBodies(): Array<Record<string, unknown>> {
		return this.receipts.map((r) => r.body);
	}

	// Media plane is NOT wired through the generic harness wire — engine
	// contracts bind FakeGraphServer directly (loud refusal keeps accidental
	// subject-side media use impossible).
	uploadMedia(): Promise<GraphResponse> {
		return Promise.reject(
			new Error("media upload requires the FakeGraphServer fixture"),
		);
	}

	getMediaMetadata(): Promise<GraphResponse> {
		return Promise.reject(
			new Error("media metadata requires the FakeGraphServer fixture"),
		);
	}

	fetchMediaBytes(): Promise<{ status: number; bytes: Buffer }> {
		return Promise.reject(
			new Error("media bytes require the FakeGraphServer fixture"),
		);
	}

	// Rich-probe passthrough (§10.1 latch row): the harness may script a rich
	// behavior; a consumed script means the endpoint was deliberately
	// programmed (reference-fixture parity).
	hasRichScript(): boolean {
		return this.raw.hasScript("rich");
	}

	async transmitRichProbe(
		chatId: string,
		content: string,
	): Promise<GraphResponse> {
		const result = await this.raw.transmitRich(chatId, content, {});
		return result.success
			? { status: 200, json: {} }
			: {
					status: 400,
					json: { error: { message: result.error ?? "rich failed", code: -1 } },
				};
	}
}

/** The WhatsApp-Cloud-shaped ConformanceSubject over the REAL adapter. */
export class WaCloudSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: WaCloudAdapter;
	readonly wire: FakePlatformWire;
	readonly bridge: WireBridge;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: WaCloudSubjectOptions) {
		this.name = opts.name ?? "whatsapp-cloud";
		this.wire = opts.wire;
		this.bridge = new WireBridge(opts.wire);
		this.adapter = new WaCloudAdapter({
			transport: this.bridge,
			scalarMaxUnits: 64, // harness-scale budget mirrors the reference subjects
			whatsappSessionDir: harnessLidSessionDir(), // NEVER the real home
			mediaCacheDir: harnessLidSessionDir(), // inbound media lands in tmp too
			secretReader: (key) =>
				opts.withSecret === false ? undefined : waHarnessSecret(key),
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

	// ── streaming seam (reply-only egress: drafts unsupported by capability) ──
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // no native stream lanes on the Cloud API
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
				"whatsapp-cloud-phone-number-id",
				"cred-wa-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"whatsapp-cloud-phone-number-id",
				"cred-wa-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"whatsapp-cloud-phone-number-id",
				"cred-wa-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new WaCloudAdapter({
			transport: new WireBridge(new FakePlatformWire()),
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "wa-scoped-probe",
				description: "",
				transportShape: "webhook",
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

	// ── webhook-shape probes (inherited transport rows) ──

	flagsAndTrustProbe(): {
		interactiveResumeFalse: boolean;
		supportsAsyncDeliveryFalse: boolean;
		trustBoundaryComplete: boolean;
	} {
		const errors = validateTrustBoundaryManifest(this.adapter.trustBoundary);
		return {
			interactiveResumeFalse: this.adapter.interactiveResume === false,
			supportsAsyncDeliveryFalse: this.adapter.supportsAsyncDelivery === false,
			trustBoundaryComplete: errors.length === 0,
		};
	}
}

/** Scoped harness secrets (fail-closed reader shape; never process.env). */
function waHarnessSecret(name: string): string | undefined {
	switch (name) {
		case "WHATSAPP_CLOUD_PHONE_NUMBER_ID":
			return "wa-phone-id";
		case "WHATSAPP_CLOUD_ACCESS_TOKEN":
			return "wa-access-token";
		case "WHATSAPP_CLOUD_APP_SECRET":
			return "wa-app-secret";
		case "WHATSAPP_CLOUD_VERIFY_TOKEN":
			return "wa-verify-token";
		default:
			return undefined;
	}
}

/**
 * One throwaway LID-mapping dir per WORKER PROCESS: recipient resolution
 * walks the identity module on every send, so subjects must NEVER read the
 * real PI_HOME session dir (mkdtemp isolation). Empty dir ⇒ every alias set
 * is the singleton input — exactly the fresh-install semantics.
 */
let harnessSessionDir: string | undefined;
function harnessLidSessionDir(): string {
	if (harnessSessionDir === undefined) {
		harnessSessionDir = mkdtempSync(join(tmpdir(), "wa-cloud-subject-"));
	}
	return harnessSessionDir;
}

export function makeWaCloudSubject(
	opts: WaCloudSubjectOptions & { wire: FakePlatformWire },
): WaCloudSubject {
	return new WaCloudSubject(opts);
}

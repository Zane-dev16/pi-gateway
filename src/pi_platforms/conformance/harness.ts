// pi_platforms/conformance/harness — the fixture harness an adapter-under-test
// plugs into (04 §8). Rows assert OBSERVABLE adapter/pipeline behavior through
// the ConformanceSubject surface; the harness supplies scripted inbound
// scenarios, egress capture, deterministic scheduling, and fresh-subject
// construction so rows never couple through shared mutable state.

import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { ChokepointAuditEntry } from "../../pi_gateway/streaming/egress-door.js";
import type { CallbackQueryRouter } from "../kit/callback-router.js";
import type { ActionHandlerRegistry } from "../kit/block-kit.js";
import type {
	AdapterStatusSnapshot,
	DisableReason,
} from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { FakePlatformWire } from "./wire.js";

/** Transport shapes (DEC-002). */
export type Shape = "polling" | "ws" | "webhook";

/** Harness-stamped deterministic scheduler for ingress rows. */
export const SCHEDULER_SYMBOL: unique symbol = Symbol("conformance.scheduler");

/**
 * The surface EVERY adapter under test exposes to the suite. Reference-correct
 * implementations pass all shared rows; transport-specific rows extend via
 * named fixtures (shapes.ts).
 */
export interface ConformanceSubject {
	/** Stable display name in reports. */
	readonly name: string;
	/** The kit base instance (capabilities/lifecycle/policy probes). */
	readonly adapter: import("../kit/base-adapter.js").BasePlatformAdapter;
	/** Egress capture — every user-visible transmission, in order. */
	readonly wire: FakePlatformWire;

	// ── observability ──
	doorAudit(): readonly ChokepointAuditEntry[];
	turns(): readonly string[];
	replies(): readonly string[];
	lifecycleSnapshot(): AdapterStatusSnapshot;

	// ── ingress lane ──
	deliverInbound(event: IncomingEvent, sessionKey: string): Promise<void>;
	/** Deterministic hold gate over the guard's turn handlers. */
	holdTurnsForBurst(on: boolean): void;
	/** Lane C predicate arming (§5.3 clarify intercept). */
	armClarifyIntercept(sessionKey: string): void;
	disarmClarifyIntercept(): void;
	clarifyCaptures(): readonly string[];

	// ── egress lanes ──
	sendThroughDoor1(
		chatId: string,
		content: string,
		metadata?: Metadata,
	): Promise<SendResult>;
	sendThroughDoor2(
		logicalPlatform: string,
		chatId: string,
		content: string,
		metadata?: Metadata,
	): Promise<SendResult>;
	/** Interim-lane send (metadata marked `_interim_send`). */
	sendInterim(chatId: string, content: string): Promise<SendResult>;
	deliverLongText(chatId: string, content: string): Promise<SendResult[]>;
	deliverToUtf16Chat(chatId: string, content: string): Promise<SendResult[]>;
	/**
	 * Declared §6.3 chunk-label policy of THIS transport's vendor truth.
	 * Optional — UNDECLARED means "kit-labeled" (chunks ship with the kit
	 * fence-carry + '(i/n)' scaffold). A subject whose reference emits BARE
	 * vendor-splitter chunks declares "vendor-bare" (Hermes irc adapter.py::send:
	 * truncate_message's '(i/n)' is never applied on IRC). Rows branch on this
	 * DECLARED datum — never on platform-name sniffing.
	 */
	chunkLabelStyle?(): "kit-labeled" | "vendor-bare";
	deliverFormattingRejected(
		chatId: string,
		content: string,
	): Promise<SendResult>;
	transientRichFailureOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult>;
	parseFailurePlainResend(chatId: string, content: string): Promise<string>;
	chatPolicyFor(chatId: string): ChatLengthPolicy;

	// ── streaming seam ──
	streamAdapter(): StreamEgressAdapter;
	/** Emit one native draft frame to ARM seal-interception (relay lanes). */
	armOpenNativeStream(chatId: string, draftId: number): Promise<void>;
	failNextSeals(n: number): void;

	// ── interactive surfaces ──
	callbackRouter(): CallbackQueryRouter | null;
	actionRegistry(): ActionHandlerRegistry;
	registerApprovalPending(id: number, sessionKey: string): void;
	registerSlashConfirmPending(id: number, sessionKey: string): void;
	registerClarifyPending(id: number, sessionKey: string): void;
	registerApprPending(id: number, sessionKey: string): void;
	setClickerAuthorization(allow: boolean): void;
	resolvedFamilies(): readonly string[];
	resolvedTurnDispatches(): readonly string[];

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true };
	/** Lifecycle of a SIBLING subject built without required secrets. */
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot;
	/** Scoped enablement resolution that must NOT borrow process env. */
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean;

	// ── DEC-022 declaration ──
	wakeLaneDeclaration(): "forged-event" | "raw-key-direct";
}

export interface RowResult {
	id: string;
	title: string;
	pass: boolean;
	shapes: ReadonlySet<Shape> | "all";
	detail?: string | undefined;
}

export function rowPass(id: string, title: string): RowResult {
	return { id, title, pass: true, shapes: "all" };
}

export function rowFail(id: string, title: string, detail: string): RowResult {
	return { id, title, pass: false, shapes: "all", detail };
}

/**
 * Structured disable-reason constructor for subject implementations.
 * (Re-exported so adapters and rows share one vocabulary.)
 */
export type { DisableReason };

/** Scripted inbound text event builder. */
export function textEvent(t: string, senderId = "user-1"): IncomingEvent {
	return {
		messageType: "text",
		text: t,
		source: {
			platform: "reference",
			chatType: "dm",
			userId: senderId,
			chatId: "chat-1",
		},
	};
}

/** Bot-authored echo event (self-filter row). */
export function selfEchoEvent(t: string): IncomingEvent {
	return textEvent(t, "bot-self");
}

/** DEC-022 push-lane forged internal wake. */
export function internalWakeEvent(): IncomingEvent {
	return {
		messageType: "text",
		text: "[internal wake]",
		internal: true,
		source: {
			platform: "reference",
			chatType: "dm",
			userId: "system",
			chatId: "chat-1",
		},
	};
}

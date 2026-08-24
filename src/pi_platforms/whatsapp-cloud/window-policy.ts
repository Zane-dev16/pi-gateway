// pi_platforms/whatsapp-cloud/window-policy — the 24-hour messaging-window
// classifier. Free-form (session) sends are deliverable only inside 24h of the
// chat's last inbound message; outside it only approved TEMPLATE sends reach
// Meta's wire, so every outbound RECORDS its routing decision (session vs
// template) BEFORE any transport call and free-form egress is refused
// pre-wire with a classified error.
//
// Hermes anchors (READ-ONLY reference; semantics transcribed as DATA):
//   gateway/platforms/whatsapp_cloud.py module docstring @~25: "Phase 5 —
//     24-hour conversation window + template fallback" (the declared class)
//   whatsapp_cloud.py @~672: interactive sends "only work *inside* the
//     24-hour conversation window — … we're always inside the window when
//     they're invoked" (the operational assumption this module makes explicit
//     and auditable instead of implicit)
//
// The decision log is a BEHAVIOR surface: tests mutate the injected clock and
// assert the recorded class flips session→template at exactly the boundary.

import { MESSAGING_WINDOW_MS } from "./manifest.js";

/** Routing class of an outbound send (manifest-declared distinction). */
export type RouteClass = "session" | "template";

export interface WindowDecision {
	chatId: string;
	/** True when a live customer session covers this send. */
	withinWindow: boolean;
	routeClass: RouteClass;
	/** ms since the chat's last inbound message (Infinity when never seen). */
	elapsedMs: number;
	windowMs: number;
	decidedAtMs: number;
	reason: string;
}

const REASON_OPEN = "customer message seen within the 24h window";
const REASON_EXPIRED = `no customer message within ${MESSAGING_WINDOW_MS}ms — template-class required`;
const REASON_NEVER_SEEN =
	"no inbound message ever recorded for this chat — template-class required";

/**
 * Per-chat window tracker. Injected clock only (flake discipline); state is
 * in-memory by design — a restart re-opens sessions on the next inbound
 * message (whatsapp_cloud.py:_last_inbound_wamid_by_chat parity comment:
 * "In-memory only; on gateway restart the next inbound message repopulates").
 */
export class MessagingWindowClassifier {
	private readonly lastInboundMs = new Map<string, number>();
	private readonly decisions = new Map<string, WindowDecision[]>();
	private readonly nowFn: () => number;
	readonly windowMs: number;

	constructor(
		opts: {
			windowMs?: number | undefined;
			nowMs?: (() => number) | undefined;
		} = {},
	) {
		this.windowMs = opts.windowMs ?? MESSAGING_WINDOW_MS;
		this.nowFn = opts.nowMs ?? (() => Date.now());
	}

	/** Record an inbound customer message (opens/extends the session). */
	noteInbound(chatId: string, atMs?: number): void {
		this.lastInboundMs.set(chatId, atMs ?? this.nowFn());
	}

	/** Pure classification — records nothing. */
	classify(chatId: string): WindowDecision {
		const nowMsValue = this.nowFn();
		const last = this.lastInboundMs.get(chatId);
		if (last === undefined) {
			return {
				chatId,
				withinWindow: false,
				routeClass: "template",
				elapsedMs: Number.POSITIVE_INFINITY,
				windowMs: this.windowMs,
				decidedAtMs: nowMsValue,
				reason: REASON_NEVER_SEEN,
			};
		}
		const elapsed = nowMsValue - last;
		const withinWindow = elapsed < this.windowMs; // exact-boundary = closed
		return {
			chatId,
			withinWindow,
			routeClass: withinWindow ? "session" : "template",
			elapsedMs: elapsed,
			windowMs: this.windowMs,
			decidedAtMs: nowMsValue,
			reason: withinWindow ? REASON_OPEN : REASON_EXPIRED,
		};
	}

	/**
	 * Classification FOR AN OUTBOUND SEND — classifies AND appends to the
	 * per-chat decision audit so the session/template routing choice is
	 * always RECORDED, never implicit.
	 */
	decideForSend(chatId: string): WindowDecision {
		const decision = this.classify(chatId);
		const log = this.decisions.get(chatId) ?? [];
		log.push(decision);
		this.decisions.set(chatId, log);
		return decision;
	}

	/** Recorded send decisions for a chat (oldest first). */
	decisionsOf(chatId: string): readonly WindowDecision[] {
		return this.decisions.get(chatId) ?? [];
	}

	/** Raw last-inbound timestamp probe (receipts/window tests). */
	lastInboundOf(chatId: string): number | undefined {
		return this.lastInboundMs.get(chatId);
	}
}

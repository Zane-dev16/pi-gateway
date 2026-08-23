// sender.ts — the narrow delivery seam Phase 3 platform adapters plug into.
//
// The ledger NEVER sends by itself: it owns durable state (what is owed, in
// which state, how many redelivery attempts remain) and hands each claimed
// obligation to exactly one `DeliverySender`. Adapters implement the send;
// the engine classifies the outcome and advances the state machine.
//
// Hermes anchors (READ-ONLY reference; semantics ported):
//   gateway/run.py:_redeliver_pending_obligations — driver composes
//     RECOVERED_MARKER + content for needs_marker rows and treats a thrown
//     send as `error="send failed"`; only SendResult.success marks delivered.
//   gateway/delivery_ledger.py:RECOVERED_MARKER — visible at-least-once
//     marker byte-parity (user-visible text is part of the contract).

/** Visible prefix for redeliveries that might duplicate an already-received message. */
export const RECOVERED_MARKER =
	"♻️ Recovered reply — the gateway restarted during delivery, so this may be a duplicate:\n\n";

/** One claimed obligation handed to a sender. */
export interface DeliveryRequest {
	obligationId: string;
	sessionKey: string;
	platform: string;
	chatId: string;
	threadId: string | null;
	/**
	 * Final text to transmit — ALREADY composed with RECOVERED_MARKER when
	 * `needsMarker` is true (the engine composes, adapters transmit verbatim).
	 */
	content: string;
	/**
	 * True when the previous delivery attempt was ambiguous or definitively
	 * rejected (crash mid-send / post-rejection retry) and the marker above
	 * was prepended. Informational for senders that care why.
	 */
	needsMarker: boolean;
	/** Redelivery attempt number for this send (1-based; 0 = plain first send). */
	attempts: number;
}

/** Classification of one send. `ok:false` is a DEFINITIVE rejection. */
export interface DeliveryOutcome {
	ok: boolean;
	/** Machine-readable rejection reason when ok is false. */
	error?: string;
}

/**
 * The seam. Throw instead of returning to signal an aborted/raised send —
 * the engine converts throws to `{ ok: false, error }` exactly like the
 * Hermes driver converts adapter exceptions to mark_failed("send failed").
 */
export type DeliverySender = (req: DeliveryRequest) => Promise<DeliveryOutcome>;

/** Compose final wire content (marker prepend parity of run.py's driver). */
export function composeDeliveryContent(
	content: string,
	needsMarker: boolean,
): string {
	return needsMarker ? RECOVERED_MARKER + content : content;
}

/** Normalize any throw/rejection into a DeliveryOutcome (parity run.py). */
export function normalizeSendFailure(err: unknown): DeliveryOutcome {
	const message =
		err instanceof Error
			? err.message
			: typeof err === "string" && err.length > 0
				? err
				: "send failed";
	return { ok: false, error: message || "send failed" };
}

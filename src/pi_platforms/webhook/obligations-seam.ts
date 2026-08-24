// pi_platforms/webhook/obligations-seam — the HELD-OPEN half of the C5
// obligation split, landed through the pi_gateway delivery-obligations ledger
// (DEC-007): a reply that outlives its bounded HTTP window is durably RECORDED
// so a later redelivery drive delivers it byte-exactly. Stateless adapters can
// never push inline; the ledger is the at-least-once rail that keeps the work
// from vanishing with the closed connection.

import type {
	DeliveryLedger,
	DeliverySender,
	NewObligation,
} from "../../pi_gateway/obligations/index.js";
import type { HeldOpenSink } from "./http-ingress.js";

export interface LedgerSeamOptions {
	/** Redelivery platform tag (defaults to the adapter manifest name). */
	platform?: string | undefined;
}

export class LedgerObligationSink implements HeldOpenSink {
	private readonly ledger: DeliveryLedger;
	private readonly platform: string;

	constructor(ledger: DeliveryLedger, opts: LedgerSeamOptions = {}) {
		this.ledger = ledger;
		this.platform = opts.platform ?? "webhook";
	}

	/**
	 * Record one held-open reply PENDING (no inline send — there is no lane to
	 * push on). The row redelivers via claimDueRetries/sweepRecoverable once a
	 * sender materializes.
	 */
	async holdOpen(entry: {
		sessionKey: string;
		chatId: string;
		content: string;
		messageRef: string;
		route?: string | undefined;
	}): Promise<{ obligationId: string }> {
		const input: NewObligation = {
			sessionKey: entry.sessionKey,
			platform: this.platform,
			chatId: entry.chatId,
			threadId: null,
			content: entry.content,
			messageRef: entry.messageRef,
		};
		const obligationId = await this.ledger.record(input);
		return { obligationId };
	}

	/**
	 * The redelivery drive: claim due rows for THIS platform and hand each to
	 * `sender`, settling the state machine (delivered/failed with backoff).
	 */
	async driveRedeliveries(sender: DeliverySender): Promise<number> {
		const claimed = await this.ledger.claimDueRetries({
			deliverablePlatforms: new Set([this.platform]),
		});
		if (claimed.length === 0) return 0;
		await this.ledger.driveClaimed(claimed, sender);
		return claimed.length;
	}
}

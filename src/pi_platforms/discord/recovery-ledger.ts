// pi_platforms/discord/recovery-ledger — the A13 Discord recovery ledger
// (missed-message completeness + missed-dispatch sweep admission).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/discord/recovery.py:DiscordRecoveryStore — message rows
//     keyed by message_id with a status machine (discovered/queued/processing/
//     responded/processed/cancelled/failed), replied/outage_response flags,
//     response_message_id, attempts; per-channel cursor high-water marks;
//     30-day retention purge (recovery.py:26,104-114).
//   adapter.py:3072-3084 _discord_message_is_persistently_complete — complete
//     ⇔ status=='responded' AND replied AND NOT outage_response.
//   adapter.py:3085-3104 _discord_message_has_active_claim — queued|processing
//     within a 10-minute window counts as claimed (fail-closed posture).
//   adapter.py:2878-2896 _should_backfill_discord_message — skip own /
//     persistently-complete / actively-claimed / already-answered candidates.
//   adapter.py:2902-2940 + :2849-2860 ping safety — a down-notice or an
//     emoji-only ack NEVER masks a pending request.
//
// Storage seam: this port keeps the ledger IN-PROCESS (injectable state
// object); durability semantics are exercised through the same status
// machine. A durable SQLite backend is a proposed-DEC item in the port report
// (Hermes ships `discord_message_recovery.db`; Pi defers storage choice).

import {
	ACTIVE_CLAIM_WINDOW_SECONDS,
	RECOVERY_RETENTION_DAYS,
	type RecoveryStatus,
} from "./manifest.js";
import type { NowFn } from "./clock.js";

export interface LedgerMessage {
	messageId: string;
	channelId: string;
	threadId?: string | undefined;
	authorId: string;
	status: RecoveryStatus;
	replied: boolean;
	emojiAck: boolean;
	outageResponse: boolean;
	responseMessageId?: string | undefined;
	attempts: number;
	/** Ledger clock (ms) of last transition. */
	updatedAtMs: number;
}

export interface SweepCandidate {
	messageId: string;
	channelId: string;
	text?: string | undefined;
}

export interface LedgerOptions {
	nowMs?: NowFn | undefined;
	retentionDays?: number | undefined;
	activeClaimWindowSeconds?: number | undefined;
}

export class DiscordRecoveryLedger {
	private readonly messages = new Map<string, LedgerMessage>();
	private readonly cursors = new Map<string, string>();
	private readonly nowFn: NowFn;
	private readonly retentionDays: number;
	private readonly activeClaimWindowMs: number;

	constructor(opts: LedgerOptions = {}) {
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.retentionDays = opts.retentionDays ?? RECOVERY_RETENTION_DAYS;
		this.activeClaimWindowMs =
			(opts.activeClaimWindowSeconds ?? ACTIVE_CLAIM_WINDOW_SECONDS) * 1000;
	}

	recordDiscovered(
		messageId: string,
		fields: {
			channelId: string;
			authorId: string;
			threadId?: string | undefined;
			text?: string | undefined;
		},
	): void {
		const existing = this.messages.get(messageId);
		if (existing !== undefined) return; // idempotent discovery
		this.messages.set(messageId, {
			messageId,
			channelId: fields.channelId,
			...(fields.threadId !== undefined ? { threadId: fields.threadId } : {}),
			authorId: fields.authorId,
			status: "discovered",
			replied: false,
			emojiAck: false,
			outageResponse: false,
			attempts: 0,
			updatedAtMs: this.nowFn(),
		});
		if (fields.text !== undefined) this.texts.set(messageId, fields.text);
	}

	private readonly texts = new Map<string, string>();

	markStatus(messageId: string, status: RecoveryStatus): boolean {
		const m = this.messages.get(messageId);
		if (m === undefined) return false;
		m.status = status;
		m.attempts += 1;
		m.updatedAtMs = this.nowFn();
		return true;
	}

	/** Responded + replied parity (`_record_discord_response` :3018-3070). */
	markResponded(messageId: string, responseMessageId: string): boolean {
		const m = this.messages.get(messageId);
		if (m === undefined) return false;
		m.status = "responded";
		m.replied = true;
		m.outageResponse = false;
		m.responseMessageId = responseMessageId;
		m.updatedAtMs = this.nowFn();
		this.advanceCursor(m.channelId, messageId);
		return true;
	}

	markFailed(messageId: string): boolean {
		return this.markStatus(messageId, "failed");
	}

	/** Emoji-only acks are recorded but NEVER count as completion (:2849-2852). */
	markEmojiAck(messageId: string): boolean {
		const m = this.messages.get(messageId);
		if (m === undefined) return false;
		m.emojiAck = true;
		m.updatedAtMs = this.nowFn();
		return true;
	}

	get(messageId: string): LedgerMessage | undefined {
		return structuredClone(this.messages.get(messageId));
	}

	advanceCursor(channelId: string, messageId: string): void {
		this.cursors.set(channelId, messageId);
	}

	cursorFor(channelId: string): string | null {
		return this.cursors.get(channelId) ?? null;
	}

	/**
	 * `_discord_message_is_persistently_complete` parity — an emoji-only ack
	 * is NOT completion (ping safety, A13).
	 */
	isPersistentlyComplete(messageId: string): boolean {
		const m = this.messages.get(messageId);
		if (m === undefined) return false;
		return m.status === "responded" && m.replied && !m.outageResponse;
	}

	/** `_discord_message_has_active_claim` parity — queued|processing in-window. */
	hasActiveClaim(messageId: string): boolean {
		const m = this.messages.get(messageId);
		if (m === undefined) return false;
		if (m.status !== "queued" && m.status !== "processing") return false;
		return this.nowFn() - m.updatedAtMs <= this.activeClaimWindowMs;
	}

	/**
	 * Sweep admission (`_should_backfill_discord_message` order): skip own
	 * bot author, persistently-complete, actively-claimed; oldest-first;
	 * bounded by maxDispatches. `botAuthorId` filters self-messages.
	 */
	candidatesForSweep(opts: {
		botAuthorId: string;
		maxDispatches: number;
	}): SweepCandidate[] {
		const out: SweepCandidate[] = [];
		for (const m of this.messages.values()) {
			if (out.length >= opts.maxDispatches) break;
			if (m.authorId === opts.botAuthorId) continue;
			if (this.isPersistentlyComplete(m.messageId)) continue;
			if (this.hasActiveClaim(m.messageId)) continue;
			out.push({
				messageId: m.messageId,
				channelId: m.channelId,
				...(this.texts.has(m.messageId)
					? { text: this.texts.get(m.messageId) }
					: {}),
			});
		}
		return out;
	}

	/** Retention purge parity (recovery.py:104-114) — cutoff by ledger clock. */
	purgeExpired(): number {
		const cutoff = this.nowFn() - this.retentionDays * 86_400_000;
		let purged = 0;
		for (const [id, m] of this.messages) {
			if (m.updatedAtMs < cutoff) {
				this.messages.delete(id);
				this.texts.delete(id);
				purged += 1;
			}
		}
		return purged;
	}

	get size(): number {
		return this.messages.size;
	}
}

/**
 * Down-notice content guard (`_is_down_notice_content` :2856-2860): such bot
 * posts never mask a pending request during sweep eligibility.
 */
export function isDownNoticeContent(text: string): boolean {
	return /\b(hermes|the agent|agent|the gateway|gateway|pi)\s+(is|was|appears to be|seems to be)\s+(down|offline|unavailable|not running)\b/i.test(
		text,
	);
}
